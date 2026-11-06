#include <gtest/gtest.h>

#include "raft_test_harness.h"
#include "test_util.h"

using namespace kvdb;
using kvdb::testutil::TestCluster;

namespace {
RaftConfig SnapshotConfig(uint64_t id, size_t n, const std::string& base_dir,
                          uint64_t threshold) {
  RaftConfig cfg;
  cfg.node_id = id;
  for (uint64_t i = 1; i <= n; i++) cfg.peers[i] = "fake://" + std::to_string(i);
  cfg.data_dir = base_dir + "/node" + std::to_string(id);
  cfg.election_timeout_min_ms = 100;
  cfg.election_timeout_max_ms = 200;
  cfg.heartbeat_interval_ms = 30;
  cfg.rpc_timeout_ms = 500;
  cfg.sync_log = false;
  cfg.snapshot_threshold_entries = threshold;
  return cfg;
}
}  // namespace

TEST(RaftSnapshot, SnapshotTakenAfterThreshold) {
  testutil::TempDir dir("snapshot_taken");
  auto network = std::make_shared<kvdb::testutil::FakeNetwork>();
  auto sm = std::make_unique<kvdb::testutil::MapStateMachine>();
  auto transport = std::make_unique<kvdb::testutil::FakeTransport>(1, network);
  RaftNode node(SnapshotConfig(1, 1, dir.path(), /*threshold=*/20), std::move(transport), sm.get());
  network->Register(1, &node);
  ASSERT_TRUE(node.Start().ok());

  // Single-node clusters still self-elect via the normal ticker/election-timeout
  // path rather than becoming leader synchronously inside Start(); wait for it.
  uint64_t leader_deadline = NowMillis() + 2000;
  while (NowMillis() < leader_deadline && !node.IsLeader()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(node.IsLeader());

  for (int i = 0; i < 50; i++) {
    uint64_t index = 0;
    ASSERT_TRUE(node.Propose("k" + std::to_string(i) + "=v", &index).ok());
  }
  uint64_t deadline = NowMillis() + 3000;
  while (NowMillis() < deadline && node.GetStats().snapshots_taken == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_GT(node.GetStats().snapshots_taken, 0u);
  node.Stop();
}

// A follower that misses entries the leader has already compacted away must
// be brought up to date via InstallSnapshot rather than left stuck forever.
TEST(RaftSnapshot, LaggingFollowerCatchesUpViaInstallSnapshot) {
  testutil::TempDir dir("snapshot_install");
  TestCluster cluster;
  // Low threshold so the leader compacts its log quickly, well before the
  // isolated follower could catch up with a normal AppendEntries replay.
  auto network = cluster.network;
  const size_t n = 3;
  const uint64_t threshold = 15;
  for (uint64_t id = 1; id <= n; id++) {
    auto sm = std::make_unique<kvdb::testutil::MapStateMachine>();
    auto transport = std::make_unique<kvdb::testutil::FakeTransport>(id, network);
    auto node =
        std::make_unique<RaftNode>(SnapshotConfig(id, n, dir.path(), threshold),
                                   std::move(transport), sm.get());
    network->Register(id, node.get());
    ASSERT_TRUE(node->Start().ok());
    cluster.state_machines.push_back(std::move(sm));
    cluster.nodes.push_back(std::move(node));
  }

  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  RaftNode* victim = nullptr;
  for (auto& n2 : cluster.nodes) {
    if (n2.get() != leader) {
      victim = n2.get();
      break;
    }
  }
  ASSERT_NE(victim, nullptr);

  network->SetIsolated(victim->node_id(), true);

  // Propose enough entries to blow well past the snapshot threshold while the
  // victim is cut off, forcing the leader to compact its log.
  for (int i = 0; i < 100; i++) {
    uint64_t index = 0;
    ASSERT_TRUE(leader->Propose("k" + std::to_string(i) + "=v", &index).ok());
  }
  uint64_t snap_deadline = NowMillis() + 3000;
  while (NowMillis() < snap_deadline && leader->GetStats().snapshots_taken == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GT(leader->GetStats().snapshots_taken, 0u);

  network->SetIsolated(victim->node_id(), false);  // rejoin

  uint64_t deadline = NowMillis() + 5000;
  bool caught_up = false;
  while (NowMillis() < deadline) {
    if (victim->LastApplied() >= leader->CommitIndex() && leader->CommitIndex() > 0) {
      caught_up = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(caught_up);
  EXPECT_GT(victim->GetStats().snapshots_installed, 0u);

  // And the state machine content itself must match, not just the index.
  for (int i = 0; i < 100; i += 17) {
    std::string key = "k" + std::to_string(i);
    EXPECT_EQ(cluster.state_machines[static_cast<size_t>(victim->node_id() - 1)]->Get(key), "v");
  }
}
