#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/env.h"
#include "concurrency/thread_pool.h"
#include "replication/replication_node.h"

namespace kvdb {
namespace testutil {

// Applies "key=value" strings; the apply_fn ReplicationNode expects.
class SimpleMap {
 public:
  Status Apply(const std::string& data) {
    std::lock_guard<std::mutex> lk(mu_);
    auto pos = data.find('=');
    if (pos != std::string::npos) map_[data.substr(0, pos)] = data.substr(pos + 1);
    applied_.push_back(data);
    return Status::OK();
  }
  std::string Get(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    return it == map_.end() ? "" : it->second;
  }
  size_t NumApplied() const {
    std::lock_guard<std::mutex> lk(mu_);
    return applied_.size();
  }
  size_t MapSize() const {
    std::lock_guard<std::mutex> lk(mu_);
    return map_.size();
  }

 private:
  mutable std::mutex mu_;
  std::map<std::string, std::string> map_;
  std::vector<std::string> applied_;
};

// Registry letting a FakeReplTransport look up any peer ReplicationNode by
// id, delivering RPCs via direct in-process calls instead of real sockets.
class FakeNetwork {
 public:
  void Register(uint64_t id, ReplicationNode* node) {
    std::lock_guard<std::mutex> lk(mu_);
    nodes_[id] = node;
  }
  ReplicationNode* Get(uint64_t id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : it->second;
  }
  void SetIsolated(uint64_t id, bool isolated) {
    std::lock_guard<std::mutex> lk(mu_);
    if (isolated) {
      isolated_.insert(id);
    } else {
      isolated_.erase(id);
    }
  }
  bool CanReach(uint64_t from, uint64_t to) {
    std::lock_guard<std::mutex> lk(mu_);
    return !isolated_.count(from) && !isolated_.count(to);
  }

 private:
  std::mutex mu_;
  std::map<uint64_t, ReplicationNode*> nodes_;
  std::set<uint64_t> isolated_;
};

class FakeReplTransport : public ReplicationTransport {
 public:
  FakeReplTransport(uint64_t self_id, std::shared_ptr<FakeNetwork> net)
      : self_id_(self_id), net_(std::move(net)) {}

  void SendReplicate(uint64_t peer_id, const ReplicateReq& req, ReplicateCallback cb) override {
    Dispatch([this, peer_id, req, cb] {
      ReplicationNode* peer = net_->Get(peer_id);
      if (peer == nullptr || !net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      ReplicateResp resp;
      peer->HandleReplicate(req, &resp);
      cb(Status::OK(), resp);
    });
  }

  void SendHeartbeat(uint64_t peer_id, const HeartbeatReq& req, HeartbeatCallback cb) override {
    Dispatch([this, peer_id, req, cb] {
      ReplicationNode* peer = net_->Get(peer_id);
      if (peer == nullptr || !net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      HeartbeatResp resp;
      peer->HandleHeartbeat(req, &resp);
      cb(Status::OK(), resp);
    });
  }

  void SendCatchUp(uint64_t peer_id, const CatchUpReq& req, CatchUpCallback cb) override {
    Dispatch([this, peer_id, req, cb] {
      ReplicationNode* peer = net_->Get(peer_id);
      if (peer == nullptr || !net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      CatchUpResp resp;
      peer->HandleCatchUp(req, &resp);
      cb(Status::OK(), resp);
    });
  }

 private:
  void Dispatch(std::function<void()> fn) { pool_.Submit(std::move(fn)); }

  uint64_t self_id_;
  std::shared_ptr<FakeNetwork> net_;
  // Declared last so it's destroyed *first*: ~ThreadPool blocks until every
  // queued/in-flight task finishes, and those tasks read self_id_/net_ above
  // via `this`, so those members must still be alive when that happens. A
  // raw detached std::thread per call (the tempting simpler option) can't
  // give this guarantee -- nothing joins it, so it can still be mid-flight,
  // touching a peer's freed memory, well after this transport is gone.
  ThreadPool pool_{2, 0, "fake-repl-tx"};
};

struct TestCluster {
  std::shared_ptr<FakeNetwork> network = std::make_shared<FakeNetwork>();
  std::vector<std::unique_ptr<SimpleMap>> maps;
  std::vector<std::unique_ptr<ReplicationNode>> nodes;

  ReplicationConfig MakeConfig(uint64_t id, size_t n, const std::string& base_dir) {
    ReplicationConfig cfg;
    cfg.node_id = id;
    for (uint64_t i = 1; i <= n; i++) cfg.peers[i] = "fake://" + std::to_string(i);
    cfg.primary_id = 1;
    cfg.data_dir = base_dir + "/node" + std::to_string(id);
    cfg.heartbeat_interval_ms = 30;
    cfg.heartbeat_timeout_ms = 120;
    cfg.rpc_timeout_ms = 500;
    cfg.sync_log = false;
    return cfg;
  }

  void Start(size_t n, const std::string& base_dir) {
    for (uint64_t id = 1; id <= n; id++) {
      auto m = std::make_unique<SimpleMap>();
      auto transport = std::make_unique<FakeReplTransport>(id, network);
      SimpleMap* map_ptr = m.get();
      auto node = std::make_unique<ReplicationNode>(
          MakeConfig(id, n, base_dir), std::move(transport),
          [map_ptr](const std::string& data) { return map_ptr->Apply(data); });
      node->Start();
      network->Register(id, node.get());
      maps.push_back(std::move(m));
      nodes.push_back(std::move(node));
    }
  }

  ~TestCluster() {
    // Two explicit passes, both run while every node object is still fully
    // alive: (1) stop every background loop so no new outbound RPCs get
    // queued, (2) drain every node's outbound dispatch queue so no in-flight
    // callback can later touch a peer whose memory the (implicit,
    // sequential) vector destructor has already freed. See
    // ReplicationNode::DrainTransport for why this two-pass order matters.
    for (auto& n : nodes) {
      if (n) n->Stop();
    }
    for (auto& n : nodes) {
      if (n) n->DrainTransport();
    }
  }

  ReplicationNode* FindPrimary(int timeout_ms = 5000) {
    uint64_t deadline = NowMillis() + static_cast<uint64_t>(timeout_ms);
    while (NowMillis() < deadline) {
      for (auto& n : nodes) {
        if (n->IsPrimary()) return n.get();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return nullptr;
  }
};

}  // namespace testutil
}  // namespace kvdb
