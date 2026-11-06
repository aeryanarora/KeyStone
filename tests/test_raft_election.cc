#include <gtest/gtest.h>

#include "raft_test_harness.h"
#include "test_util.h"

using namespace kvdb;
using kvdb::testutil::TestCluster;

TEST(RaftElection, SingleNodeBecomesLeaderImmediately) {
  testutil::TempDir dir("election_single");
  TestCluster cluster;
  cluster.Start(1, dir.path());
  RaftNode* leader = cluster.FindLeader(2000);
  ASSERT_NE(leader, nullptr);
  EXPECT_EQ(leader->node_id(), 1u);
}

TEST(RaftElection, ThreeNodeClusterElectsExactlyOneLeader) {
  testutil::TempDir dir("election_three");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);

  int leader_count = 0;
  for (auto& n : cluster.nodes) {
    if (n->IsLeader()) leader_count++;
  }
  EXPECT_EQ(leader_count, 1);
}

TEST(RaftElection, FiveNodeClusterElectsLeader) {
  testutil::TempDir dir("election_five");
  TestCluster cluster;
  cluster.Start(5, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);
}

TEST(RaftElection, AllNodesAgreeOnSameLeaderAndTerm) {
  testutil::TempDir dir("election_agree");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));  // let heartbeats settle followers

  uint64_t term = leader->CurrentTerm();
  uint64_t leader_id = leader->node_id();
  for (auto& n : cluster.nodes) {
    EXPECT_EQ(n->CurrentTerm(), term);
    EXPECT_EQ(n->LeaderId(), leader_id);
  }
}

TEST(RaftElection, LeaderFailureTriggersReElection) {
  testutil::TempDir dir("election_failover");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  RaftNode* leader1 = cluster.FindLeader(5000);
  ASSERT_NE(leader1, nullptr);
  uint64_t old_leader_id = leader1->node_id();
  uint64_t old_term = leader1->CurrentTerm();

  cluster.network->SetIsolated(old_leader_id, true);  // simulate a crash

  // Poll for a NEW leader (different id) to emerge among the survivors.
  RaftNode* new_leader = nullptr;
  uint64_t deadline = NowMillis() + 5000;
  while (NowMillis() < deadline) {
    for (auto& n : cluster.nodes) {
      if (n->node_id() != old_leader_id && n->IsLeader()) {
        new_leader = n.get();
        break;
      }
    }
    if (new_leader) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_NE(new_leader, nullptr);
  EXPECT_GT(new_leader->CurrentTerm(), old_term);
}

TEST(RaftElection, NoDoubleLeaderInSameTermDuringPartition) {
  testutil::TempDir dir("election_split_brain");
  TestCluster cluster;
  cluster.Start(5, dir.path());
  RaftNode* leader = cluster.FindLeader(5000);
  ASSERT_NE(leader, nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Repeatedly check invariant over a period that spans several election
  // timeouts and an injected partition, since split-brain is a safety
  // property that must hold at every instant, not just eventually.
  uint64_t deadline = NowMillis() + 2000;
  bool partitioned = false;
  while (NowMillis() < deadline) {
    if (!partitioned) {
      cluster.network->SetIsolated(leader->node_id(), true);
      partitioned = true;
    }
    for (auto& n : cluster.nodes) {
      // At most one leader per term, cluster-wide, at every observation point.
      if (n->IsLeader()) {
        int count_at_term = cluster.CountLeadersAtTerm(n->CurrentTerm());
        EXPECT_LE(count_at_term, 1);
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}
