#include "replication/replication_node.h"

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <future>
#include <sstream>

#include "common/env.h"
#include "common/logger.h"

namespace kvdb {

std::string ReplicationNode::Stats::ToString() const {
  std::ostringstream ss;
  ss << "writes_replicated=" << writes_replicated << " writes_applied=" << writes_applied
     << " heartbeats_sent=" << heartbeats_sent << " heartbeats_received=" << heartbeats_received
     << " promotions=" << promotions;
  return ss.str();
}

ReplicationNode::ReplicationNode(ReplicationConfig config,
                                 std::unique_ptr<ReplicationTransport> transport,
                                 ApplyFn apply_fn)
    : config_(std::move(config)), transport_(std::move(transport)), apply_fn_(std::move(apply_fn)) {}

ReplicationNode::~ReplicationNode() { Stop(); }

Status ReplicationNode::Start() {
  KVDB_RETURN_IF_ERROR(log_.Open(config_.data_dir, config_.sync_log));

  uint64_t epoch = log_.epoch();
  uint64_t primary_id = log_.primary_id();
  if (epoch == 0) {
    // First boot: trust the config.
    epoch = 1;
    primary_id = config_.primary_id;
    KVDB_RETURN_IF_ERROR(log_.SaveEpoch(epoch, primary_id));
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    epoch_ = epoch;
    primary_id_ = primary_id;
    role_.store(primary_id_ == config_.node_id ? NodeRole::kPrimary : NodeRole::kBackup,
               std::memory_order_release);
  }
  last_heartbeat_ms_.store(static_cast<int64_t>(NowMillis()), std::memory_order_release);
  known_primary_last_seq_.store(log_.last_seq(), std::memory_order_release);

  running_.store(true, std::memory_order_release);
  bg_thread_ = std::thread([this] { BackgroundLoop(); });
  LOG_INFO("replication node %" PRIu64 " started: role=%s epoch=%" PRIu64 " primary=%" PRIu64
           " last_seq=%" PRIu64,
           config_.node_id, RoleName(role_.load()), epoch_, primary_id_, log_.last_seq());
  return Status::OK();
}

void ReplicationNode::Stop() {
  if (!running_.exchange(false)) return;
  {
    std::lock_guard<std::mutex> lk(stop_mu_);
    stop_cv_.notify_all();
  }
  if (bg_thread_.joinable()) bg_thread_.join();
}

bool ReplicationNode::IsPrimary() const { return role_.load(std::memory_order_acquire) == NodeRole::kPrimary; }

uint64_t ReplicationNode::Epoch() const {
  std::lock_guard<std::mutex> lk(mu_);
  return epoch_;
}

uint64_t ReplicationNode::PrimaryId() const {
  std::lock_guard<std::mutex> lk(mu_);
  return primary_id_;
}

std::string ReplicationNode::PrimaryAddressLocked() const {
  auto it = config_.peers.find(primary_id_);
  return it == config_.peers.end() ? "" : it->second;
}

std::string ReplicationNode::PrimaryAddress() const {
  std::lock_guard<std::mutex> lk(mu_);
  return PrimaryAddressLocked();
}

ReplicationNode::Stats ReplicationNode::GetStats() const {
  std::lock_guard<std::mutex> lk(stats_mu_);
  return stats_;
}

void ReplicationNode::AdoptEpochLocked(uint64_t new_epoch, uint64_t new_primary_id) {
  log_.SaveEpoch(new_epoch, new_primary_id);
  epoch_ = new_epoch;
  primary_id_ = new_primary_id;
  role_.store(NodeRole::kBackup, std::memory_order_release);
}

void ReplicationNode::StepDownIfStale(uint64_t seen_epoch, uint64_t seen_primary_id) {
  std::lock_guard<std::mutex> lk(mu_);
  if (seen_epoch <= epoch_) return;
  LOG_WARN("node %" PRIu64 " stepping down: saw epoch %" PRIu64 " > local %" PRIu64,
           config_.node_id, seen_epoch, epoch_);
  AdoptEpochLocked(seen_epoch, seen_primary_id);
  last_heartbeat_ms_.store(static_cast<int64_t>(NowMillis()), std::memory_order_release);
}

Status ReplicationNode::Replicate(const std::string& data, uint64_t* seq) {
  uint64_t my_epoch, my_primary_id;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (role_.load(std::memory_order_acquire) != NodeRole::kPrimary) {
      return Status::NotLeader(PrimaryAddressLocked());
    }
    my_epoch = epoch_;
    my_primary_id = primary_id_;
  }

  const uint64_t s = log_.last_seq() + 1;
  KVDB_RETURN_IF_ERROR(log_.Append(s, data));
  KVDB_RETURN_IF_ERROR(apply_fn_(data));
  if (seq != nullptr) *seq = s;
  {
    std::lock_guard<std::mutex> lk(stats_mu_);
    stats_.writes_replicated++;
    stats_.writes_applied++;
  }

  auto acked = std::make_shared<std::pair<std::mutex, std::condition_variable>>();
  auto ack_count = std::make_shared<std::atomic<size_t>>(0);
  size_t backup_count = 0;
  for (const auto& [id, addr] : config_.peers) {
    if (id == config_.node_id) continue;
    backup_count++;
    ReplicateReq req;
    req.epoch = my_epoch;
    req.primary_id = my_primary_id;
    req.seq = s;
    req.data = data;
    transport_->SendReplicate(id, req, [this, acked, ack_count, my_epoch](Status rs, ReplicateResp resp) {
      if (rs.ok() && resp.success && resp.epoch == my_epoch) {
        ack_count->fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lk(acked->first);
        acked->second.notify_all();
      } else if (rs.ok() && resp.epoch > my_epoch) {
        StepDownIfStale(resp.epoch, resp.primary_id);
      }
    });
  }
  (void)backup_count;

  if (config_.min_acks > 0) {
    std::unique_lock<std::mutex> lk(acked->first);
    acked->second.wait_for(lk, std::chrono::milliseconds(config_.rpc_timeout_ms),
                           [&] { return ack_count->load(std::memory_order_relaxed) >= config_.min_acks; });
  }
  return Status::OK();
}

void ReplicationNode::HandleReplicate(const ReplicateReq& req, ReplicateResp* resp) {
  if (!running_.load(std::memory_order_acquire)) {
    resp->success = false;
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (req.epoch < epoch_) {
      resp->epoch = epoch_;
      resp->success = false;
      resp->backup_id = config_.node_id;
      resp->applied_seq = log_.last_seq();
      resp->primary_id = primary_id_;
      return;
    }
    if (req.epoch > epoch_) AdoptEpochLocked(req.epoch, req.primary_id);
  }
  last_heartbeat_ms_.store(static_cast<int64_t>(NowMillis()), std::memory_order_release);
  known_primary_last_seq_.store(
      std::max(known_primary_last_seq_.load(std::memory_order_relaxed), req.seq),
      std::memory_order_relaxed);

  const uint64_t local_last = log_.last_seq();
  bool applied = false;
  if (req.seq == local_last + 1) {
    Status s = log_.Append(req.seq, req.data);
    if (s.ok()) s = apply_fn_(req.data);
    applied = s.ok();
    if (applied) {
      std::lock_guard<std::mutex> lk(stats_mu_);
      stats_.writes_applied++;
    }
  }

  std::lock_guard<std::mutex> lk(mu_);
  resp->epoch = epoch_;
  resp->success = applied;
  resp->backup_id = config_.node_id;
  resp->applied_seq = applied ? req.seq : local_last;
  resp->primary_id = primary_id_;
}

void ReplicationNode::HandleHeartbeat(const HeartbeatReq& req, HeartbeatResp* resp) {
  if (!running_.load(std::memory_order_acquire)) {
    resp->ok = false;
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (req.epoch < epoch_) {
      resp->epoch = epoch_;
      resp->ok = false;
      resp->backup_id = config_.node_id;
      resp->applied_seq = log_.last_seq();
      resp->primary_id = primary_id_;
      return;
    }
    if (req.epoch > epoch_) AdoptEpochLocked(req.epoch, req.primary_id);
  }
  last_heartbeat_ms_.store(static_cast<int64_t>(NowMillis()), std::memory_order_release);
  known_primary_last_seq_.store(
      std::max(known_primary_last_seq_.load(std::memory_order_relaxed), req.primary_last_seq),
      std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lk(stats_mu_);
    stats_.heartbeats_received++;
  }

  std::lock_guard<std::mutex> lk(mu_);
  resp->epoch = epoch_;
  resp->ok = true;
  resp->backup_id = config_.node_id;
  resp->applied_seq = log_.last_seq();
  resp->primary_id = primary_id_;
}

void ReplicationNode::HandleCatchUp(const CatchUpReq& req, CatchUpResp* resp) {
  if (!running_.load(std::memory_order_acquire)) {
    resp->ok = false;
    return;
  }
  (void)req;
  resp->epoch = log_.epoch();
  resp->ok = true;
  log_.Range(req.from_seq, 256, &resp->entries);
}

Status ReplicationNode::BlockingCatchUp() {
  uint64_t primary_id;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (role_.load(std::memory_order_acquire) == NodeRole::kPrimary) return Status::OK();
    primary_id = primary_id_;
  }
  if (primary_id == config_.node_id || primary_id == 0) return Status::OK();

  CatchUpReq req;
  req.epoch = log_.epoch();
  req.from_seq = log_.last_seq() + 1;

  auto prom = std::make_shared<std::promise<std::pair<Status, CatchUpResp>>>();
  auto fut = prom->get_future();
  transport_->SendCatchUp(primary_id, req,
                          [prom](Status s, CatchUpResp r) { prom->set_value({s, std::move(r)}); });
  if (fut.wait_for(std::chrono::milliseconds(config_.rpc_timeout_ms)) != std::future_status::ready) {
    return Status::Timeout("catch-up request timed out");
  }
  auto [s, resp] = fut.get();
  KVDB_RETURN_IF_ERROR(s);

  for (const auto& [seq, data] : resp.entries) {
    if (seq != log_.last_seq() + 1) break;
    KVDB_RETURN_IF_ERROR(log_.Append(seq, data));
    KVDB_RETURN_IF_ERROR(apply_fn_(data));
    std::lock_guard<std::mutex> lk(stats_mu_);
    stats_.writes_applied++;
  }
  return Status::OK();
}

bool ReplicationNode::IsDesignatedBackupLocked() const {
  uint64_t lowest = UINT64_MAX;
  for (const auto& [id, addr] : config_.peers) {
    (void)addr;
    if (id == primary_id_) continue;
    lowest = std::min(lowest, id);
  }
  return lowest == config_.node_id;
}

void ReplicationNode::PromoteSelf() {
  std::lock_guard<std::mutex> lk(mu_);
  if (role_.load(std::memory_order_acquire) == NodeRole::kPrimary) return;
  const uint64_t new_epoch = epoch_ + 1;
  log_.SaveEpoch(new_epoch, config_.node_id);
  epoch_ = new_epoch;
  primary_id_ = config_.node_id;
  role_.store(NodeRole::kPrimary, std::memory_order_release);
  {
    std::lock_guard<std::mutex> slk(stats_mu_);
    stats_.promotions++;
  }
  LOG_WARN("node %" PRIu64 " promoting itself to primary at epoch %" PRIu64, config_.node_id,
           new_epoch);
}

void ReplicationNode::SendHeartbeatsToBackups() {
  uint64_t my_epoch, my_primary_id, my_last_seq;
  {
    std::lock_guard<std::mutex> lk(mu_);
    my_epoch = epoch_;
    my_primary_id = primary_id_;
  }
  my_last_seq = log_.last_seq();

  for (const auto& [id, addr] : config_.peers) {
    if (id == config_.node_id) continue;
    HeartbeatReq req;
    req.epoch = my_epoch;
    req.primary_id = my_primary_id;
    req.primary_last_seq = my_last_seq;
    transport_->SendHeartbeat(id, req, [this, my_epoch](Status rs, HeartbeatResp resp) {
      if (rs.ok() && resp.epoch > my_epoch) StepDownIfStale(resp.epoch, resp.primary_id);
    });
  }
  std::lock_guard<std::mutex> lk(stats_mu_);
  stats_.heartbeats_sent++;
}

void ReplicationNode::BackgroundLoop() {
  while (running_.load(std::memory_order_acquire)) {
    {
      std::unique_lock<std::mutex> lk(stop_mu_);
      stop_cv_.wait_for(lk, std::chrono::milliseconds(config_.heartbeat_interval_ms),
                        [this] { return !running_.load(std::memory_order_acquire); });
    }
    if (!running_.load(std::memory_order_acquire)) return;
    if (paused_.load(std::memory_order_acquire)) continue;

    if (role_.load(std::memory_order_acquire) == NodeRole::kPrimary) {
      SendHeartbeatsToBackups();
      continue;
    }

    if (log_.last_seq() < known_primary_last_seq_.load(std::memory_order_relaxed)) {
      Status s = BlockingCatchUp();
      if (!s.ok()) LOG_WARN("catch-up failed: %s", s.ToString().c_str());
    }

    const int64_t elapsed = static_cast<int64_t>(NowMillis()) - last_heartbeat_ms_.load(std::memory_order_acquire);
    if (elapsed > config_.heartbeat_timeout_ms) {
      bool designated;
      {
        std::lock_guard<std::mutex> lk(mu_);
        designated = IsDesignatedBackupLocked();
      }
      if (designated) PromoteSelf();
    }
  }
}

}  // namespace kvdb
