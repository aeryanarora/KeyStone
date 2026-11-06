#include <gtest/gtest.h>

#include "raft_test_harness.h"
#include "test_util.h"

using namespace kvdb;
using kvdb::testutil::TestCluster;

TEST(RaftReplication, ProposedEntryReplicatesToAllNodes) {
  testutil::TempDir dir("replication_basic");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  uint64_t index = 0;
  ASSERT_TRUE(leader->Propose("k1=v1", &index).ok());

  // All nodes must eventually apply this entry to their state machine.
  uint64_t deadline = NowMillis() + 3000;
  bool all_applied = false;
  while (NowMillis() < deadline) {
    all_applied = true;
    for (auto& sm : cluster.state_machines) {
      if (sm->Get("k1") != "v1") {
        all_applied = false;
        break;
      }
    }
    if (all_applied) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(all_applied);
}

TEST(RaftReplication, MultipleSequentialProposals) {
  testutil::TempDir dir("replication_sequence");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  for (int i = 0; i < 50; i++) {
    uint64_t index = 0;
    Status s = leader->Propose("key" + std::to_string(i) + "=val" + std::to_string(i), &index);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  uint64_t deadline = NowMillis() + 5000;
  bool ok = false;
  while (NowMillis() < deadline) {
    ok = true;
    for (auto& sm : cluster.state_machines) {
      if (sm->MapSize() != 50) {
        ok = false;
        break;
      }
    }
    if (ok) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(ok);
}

TEST(RaftReplication, ProposeOnFollowerFailsWithNotLeader) {
  testutil::TempDir dir("replication_not_leader");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  for (auto& n : cluster.nodes) {
    if (n.get() != leader) {
      uint64_t index = 0;
      Status s = n->Propose("x=y", &index);
      EXPECT_TRUE(s.IsNotLeader());
    }
  }
}

TEST(RaftReplication, MinorityPartitionCannotCommit) {
  testutil::TempDir dir("replication_minority");
  TestCluster cluster;
  cluster.Start(5, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  // Isolate the leader down to a minority (itself + at most one other): it
  // can no longer reach a quorum and must not be able to commit new entries.
  for (auto& n : cluster.nodes) {
    if (n->node_id() != leader->node_id()) {
      cluster.network->SetIsolated(n->node_id(), true);
      break;  // isolate exactly one follower is not enough; isolate the rest below
    }
  }
  // Actually isolate the leader itself from everyone to guarantee minority.
  cluster.network->SetIsolated(leader->node_id(), true);

  uint64_t index = 0;
  Status s = leader->Propose("orphan=entry", &index, /*timeout_ms=*/800);
  EXPECT_FALSE(s.ok());  // times out or steps down; either way, must not silently succeed
}

TEST(RaftReplication, ReadIndexSucceedsOnLeaderWithQuorum) {
  testutil::TempDir dir("replication_readindex");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);
  Status s = leader->ReadIndex(2000);
  EXPECT_TRUE(s.ok());
}

TEST(RaftReplication, ReadIndexFailsOnFollower) {
  testutil::TempDir dir("replication_readindex_follower");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);
  for (auto& n : cluster.nodes) {
    if (n.get() != leader) {
      EXPECT_TRUE(n->ReadIndex(500).IsNotLeader());
    }
  }
}

TEST(RaftReplication, CatchUpAfterTemporaryPartitionHeals) {
  testutil::TempDir dir("replication_catchup");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  RaftNode* victim = nullptr;
  for (auto& n : cluster.nodes) {
    if (n.get() != leader) {
      victim = n.get();
      break;
    }
  }
  ASSERT_NE(victim, nullptr);

  cluster.network->SetIsolated(victim->node_id(), true);
  for (int i = 0; i < 20; i++) {
    uint64_t index = 0;
    ASSERT_TRUE(leader->Propose("k" + std::to_string(i) + "=v", &index).ok());
  }
  cluster.network->SetIsolated(victim->node_id(), false);  // heal

  // The isolated node must catch up via normal replication (it wasn't gone
  // long enough to need a snapshot).
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
}
