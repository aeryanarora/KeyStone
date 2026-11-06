#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "replication/replication_log.h"
#include "replication/replication_transport.h"
#include "replication/replication_types.h"

namespace kvdb {

// One primary, N backups. The primary appends every write to its own log and
// applies it locally, then ships it to backups. There is no election and no
// quorum: on heartbeat timeout, a single fixed "designated backup" (the
// lowest node id among the peers other than the presumed-dead primary)
// promotes itself, bumping a monotonic epoch that fences off the old
// primary. This handles crash-and-restart cleanly. It does NOT handle a
// network partition safely -- an isolated old primary has no way to learn
// it's been replaced and keeps accepting writes on its side of the
// partition. Solving that requires a majority/quorum rule, which is what
// Raft adds on top of this.
class ReplicationNode {
 public:
  using ApplyFn = std::function<Status(const std::string& data)>;

  struct Stats {
    uint64_t writes_replicated = 0;
    uint64_t writes_applied = 0;
    uint64_t heartbeats_sent = 0;
    uint64_t heartbeats_received = 0;
    uint64_t promotions = 0;
    std::string ToString() const;
  };

  ReplicationNode(ReplicationConfig config, std::unique_ptr<ReplicationTransport> transport,
                  ApplyFn apply_fn);
  ~ReplicationNode();

  Status Start();
  void Stop();

  bool IsPrimary() const;
  NodeRole role() const { return role_.load(std::memory_order_acquire); }
  uint64_t Epoch() const;
  uint64_t PrimaryId() const;
  std::string PrimaryAddress() const;
  uint64_t LastSeq() const { return log_.last_seq(); }
  Stats GetStats() const;

  // Primary-only: replicate one write. Applies locally, then fans out to
  // backups; if config.min_acks > 0, waits (best-effort, up to rpc_timeout_ms)
  // for that many backups to ack before returning.
  Status Replicate(const std::string& data, uint64_t* seq);

  void HandleReplicate(const ReplicateReq& req, ReplicateResp* resp);
  void HandleHeartbeat(const HeartbeatReq& req, HeartbeatResp* resp);
  void HandleCatchUp(const CatchUpReq& req, CatchUpResp* resp);

  void PauseHeartbeats() { paused_.store(true, std::memory_order_release); }
  void ResumeHeartbeats() { paused_.store(false, std::memory_order_release); }

  // Destroys the transport (joining/draining any in-flight dispatch work)
  // independently of the rest of this node's state. Only meant for ordered
  // teardown of a whole cluster of nodes sharing an in-process fake network:
  // callers must ensure every node's outbound dispatch work is drained this
  // way *before* any node's core state (log_, mu_, ...) starts being
  // destroyed, or a peer's in-flight callback can end up touching freed
  // memory. The real TCP transport has no equivalent need -- a socket to a
  // torn-down peer just errors out, it doesn't dangle a raw pointer.
  void DrainTransport() { transport_.reset(); }

  ReplicationTransport* transport() { return transport_.get(); }

 private:
  void BackgroundLoop();
  void SendHeartbeatsToBackups();
  void PromoteSelf();
  bool IsDesignatedBackupLocked() const;
  void AdoptEpochLocked(uint64_t new_epoch, uint64_t new_primary_id);
  void StepDownIfStale(uint64_t seen_epoch, uint64_t seen_primary_id);
  Status BlockingCatchUp();
  std::string PrimaryAddressLocked() const;

  ReplicationConfig config_;
  ReplicationLog log_;
  std::unique_ptr<ReplicationTransport> transport_;
  ApplyFn apply_fn_;

  mutable std::mutex mu_;
  std::atomic<NodeRole> role_{NodeRole::kBackup};
  uint64_t epoch_ = 0;
  uint64_t primary_id_ = 0;

  std::atomic<int64_t> last_heartbeat_ms_{0};
  std::atomic<uint64_t> known_primary_last_seq_{0};
  std::atomic<bool> running_{false};
  std::atomic<bool> paused_{false};
  std::thread bg_thread_;
  std::condition_variable stop_cv_;
  std::mutex stop_mu_;

  mutable std::mutex stats_mu_;
  Stats stats_;
};

}  // namespace kvdb
