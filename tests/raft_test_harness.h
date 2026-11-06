#pragma once

// Shared test harness for Raft unit/integration tests: an in-process fake
// transport plus a small in-memory KV state machine, so elections and
// replication can be tested deterministically (no real sockets, no timing
// flakiness beyond the randomized election timers Raft itself relies on) and
// so injected partitions/latency are exact rather than best-effort.

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/env.h"
#include "raft/raft_node.h"
#include "raft/state_machine.h"

namespace kvdb {
namespace testutil {

// Simple deterministic state machine: applies "key=value" strings, supports
// snapshot/restore, used to verify Raft's replication + compaction behavior
// without pulling in the full LSM engine.
class MapStateMachine : public StateMachine {
 public:
  Status Apply(uint64_t index, const std::string& data, std::string* result) override {
    std::lock_guard<std::mutex> lk(mu_);
    if (result != nullptr) result->clear();
    auto pos = data.find('=');
    if (pos != std::string::npos) map_[data.substr(0, pos)] = data.substr(pos + 1);
    applied_.push_back(data);
    last_applied_ = index;
    return Status::OK();
  }
  Status Snapshot(uint64_t up_to_index, std::string* out) override {
    std::lock_guard<std::mutex> lk(mu_);
    out->clear();
    for (const auto& [k, v] : map_) *out += k + "=" + v + "\n";
    snapshot_up_to_ = up_to_index;
    return Status::OK();
  }
  Status Restore(uint64_t up_to_index, const std::string& data) override {
    std::lock_guard<std::mutex> lk(mu_);
    map_.clear();
    size_t pos = 0;
    while (pos < data.size()) {
      size_t nl = data.find('\n', pos);
      if (nl == std::string::npos) break;
      std::string line = data.substr(pos, nl - pos);
      auto eq = line.find('=');
      if (eq != std::string::npos) map_[line.substr(0, eq)] = line.substr(eq + 1);
      pos = nl + 1;
    }
    last_applied_ = up_to_index;
    return Status::OK();
  }
  uint64_t LastAppliedIndex() const override {
    std::lock_guard<std::mutex> lk(mu_);
    return last_applied_;
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
  uint64_t last_applied_ = 0;
  uint64_t snapshot_up_to_ = 0;
};

// Registry that lets a FakeTransport look up any peer RaftNode by id, so RPCs
// can be delivered by direct in-process calls instead of real sockets.
class FakeNetwork {
 public:
  void Register(uint64_t id, RaftNode* node) {
    std::lock_guard<std::mutex> lk(mu_);
    nodes_[id] = node;
  }
  void Unregister(uint64_t id) {
    std::lock_guard<std::mutex> lk(mu_);
    nodes_.erase(id);
  }
  RaftNode* Get(uint64_t id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : it->second;
  }

  void SetPartitioned(uint64_t a, uint64_t b, bool partitioned) {
    std::lock_guard<std::mutex> lk(mu_);
    if (partitioned) {
      partitions_.insert({a, b});
      partitions_.insert({b, a});
    } else {
      partitions_.erase({a, b});
      partitions_.erase({b, a});
    }
  }
  // Fully isolates one node from all others (simulates that node crashing at
  // the network level without stopping its ticker/apply threads).
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
    if (isolated_.count(from) || isolated_.count(to)) return false;
    return partitions_.find({from, to}) == partitions_.end();
  }

 private:
  std::mutex mu_;
  std::map<uint64_t, RaftNode*> nodes_;
  std::set<std::pair<uint64_t, uint64_t>> partitions_;
  std::set<uint64_t> isolated_;
};

// In-process RaftTransport: delivers RPCs via direct calls into the target
// RaftNode's Handle* methods, dispatched on a small thread pool to preserve
// asynchrony (callbacks must not fire on the caller's own stack, matching the
// real transport's contract and avoiding self-deadlock in RaftNode).
class FakeTransport : public RaftTransport {
 public:
  FakeTransport(uint64_t self_id, std::shared_ptr<FakeNetwork> net)
      : self_id_(self_id), net_(std::move(net)) {}

  void SendRequestVote(uint64_t peer_id, const RequestVoteReq& req, VoteCallback cb) override {
    Dispatch([this, peer_id, req, cb] {
      RaftNode* peer = net_->Get(peer_id);
      if (peer == nullptr || !net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      RequestVoteResp resp;
      peer->HandleRequestVote(req, &resp);
      if (!net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});  // partitioned mid-flight
        return;
      }
      cb(Status::OK(), resp);
    });
  }

  void SendAppendEntries(uint64_t peer_id, const AppendEntriesReq& req,
                         AppendCallback cb) override {
    Dispatch([this, peer_id, req, cb] {
      RaftNode* peer = net_->Get(peer_id);
      if (peer == nullptr || !net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      AppendEntriesResp resp;
      peer->HandleAppendEntries(req, &resp);
      if (!net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      cb(Status::OK(), resp);
    });
  }

  void SendInstallSnapshot(uint64_t peer_id, const InstallSnapshotReq& req,
                           SnapshotCallback cb) override {
    Dispatch([this, peer_id, req, cb] {
      RaftNode* peer = net_->Get(peer_id);
      if (peer == nullptr || !net_->CanReach(self_id_, peer_id)) {
        cb(Status::NetworkError("unreachable"), {});
        return;
      }
      InstallSnapshotResp resp;
      peer->HandleInstallSnapshot(req, &resp);
      cb(Status::OK(), resp);
    });
  }

 private:
  void Dispatch(std::function<void()> fn) {
    std::thread([fn = std::move(fn)] { fn(); }).detach();
  }

  uint64_t self_id_;
  std::shared_ptr<FakeNetwork> net_;
};

// A small cluster of RaftNodes wired together via FakeNetwork, for election
// and replication tests that need real timing behaviour but not real sockets.
struct TestCluster {
  std::shared_ptr<FakeNetwork> network = std::make_shared<FakeNetwork>();
  std::vector<std::unique_ptr<MapStateMachine>> state_machines;
  std::vector<std::unique_ptr<RaftNode>> nodes;
  std::vector<std::string> data_dirs;

  RaftConfig MakeConfig(uint64_t id, size_t n, const std::string& base_dir) {
    RaftConfig cfg;
    cfg.node_id = id;
    for (uint64_t i = 1; i <= n; i++) cfg.peers[i] = "fake://" + std::to_string(i);
    cfg.data_dir = base_dir + "/node" + std::to_string(id);
    cfg.election_timeout_min_ms = 100;
    cfg.election_timeout_max_ms = 200;
    cfg.heartbeat_interval_ms = 30;
    cfg.rpc_timeout_ms = 500;
    cfg.sync_log = false;  // keep unit tests fast; durability is tested at the WAL layer
    cfg.snapshot_threshold_entries = 1000000;  // disabled unless a test wants it
    return cfg;
  }

  void Start(size_t n, const std::string& base_dir) {
    for (uint64_t id = 1; id <= n; id++) {
      auto sm = std::make_unique<MapStateMachine>();
      auto transport = std::make_unique<FakeTransport>(id, network);
      auto node = std::make_unique<RaftNode>(MakeConfig(id, n, base_dir), std::move(transport),
                                             sm.get());
      network->Register(id, node.get());
      node->Start();
      state_machines.push_back(std::move(sm));
      nodes.push_back(std::move(node));
    }
  }

  ~TestCluster() {
    for (auto& n : nodes) {
      if (n) n->Stop();
    }
  }

  RaftNode* FindLeader(int timeout_ms = 5000) {
    uint64_t deadline = NowMillis() + static_cast<uint64_t>(timeout_ms);
    while (NowMillis() < deadline) {
      for (auto& n : nodes) {
        if (n->IsLeader()) return n.get();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return nullptr;
  }

  int CountLeadersAtTerm(uint64_t term) {
    int count = 0;
    for (auto& n : nodes) {
      if (n->IsLeader() && n->CurrentTerm() == term) count++;
    }
    return count;
  }
};

}  // namespace testutil
}  // namespace kvdb
