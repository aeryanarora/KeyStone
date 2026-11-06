#include <gtest/gtest.h>

#include "replication_test_harness.h"
#include "test_util.h"

using namespace kvdb;
using kvdb::testutil::TestCluster;

TEST(ReplicationNode, WritesReplicateToAllBackups) {
  testutil::TempDir dir("repl_write");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  ReplicationNode* primary = cluster.FindPrimary(3000);
  ASSERT_NE(primary, nullptr);

  uint64_t seq = 0;
  ASSERT_TRUE(primary->Replicate("k1=v1", &seq).ok());
  EXPECT_EQ(seq, 1u);

  uint64_t deadline = NowMillis() + 3000;
  bool all_applied = false;
  while (NowMillis() < deadline) {
    all_applied = true;
    for (auto& m : cluster.maps) {
      if (m->Get("k1") != "v1") all_applied = false;
    }
    if (all_applied) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(all_applied);
}

TEST(ReplicationNode, OnlyPrimaryAcceptsWrites) {
  testutil::TempDir dir("repl_only_primary");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(3000), nullptr);

  int rejects = 0;
  for (auto& n : cluster.nodes) {
    if (!n->IsPrimary()) {
      uint64_t seq = 0;
      EXPECT_TRUE(n->Replicate("x=1", &seq).IsNotLeader());
      rejects++;
    }
  }
  EXPECT_EQ(rejects, 2);
}

TEST(ReplicationNode, BackupPromotesAfterPrimaryGoesSilent) {
  testutil::TempDir dir("repl_promote");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  ReplicationNode* primary = cluster.FindPrimary(3000);
  ASSERT_NE(primary, nullptr);
  const uint64_t old_epoch = primary->Epoch();

  cluster.network->SetIsolated(1, true);  // node 1 is always the initial primary here

  uint64_t deadline = NowMillis() + 3000;
  ReplicationNode* new_primary = nullptr;
  while (NowMillis() < deadline) {
    for (auto& n : cluster.nodes) {
      if (n->IsPrimary() && n->Epoch() > old_epoch) {
        new_primary = n.get();
        break;
      }
    }
    if (new_primary) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_NE(new_primary, nullptr);

  uint64_t seq = 0;
  EXPECT_TRUE(new_primary->Replicate("after=promotion", &seq).ok());
}

TEST(ReplicationNode, IsolatedOldPrimaryStepsDownWhenReconnected) {
  testutil::TempDir dir("repl_stepdown");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  ReplicationNode* primary = cluster.FindPrimary(3000);
  ASSERT_NE(primary, nullptr);
  ReplicationNode* old_primary = primary;

  cluster.network->SetIsolated(1, true);
  uint64_t deadline = NowMillis() + 3000;
  ReplicationNode* new_primary = nullptr;
  while (NowMillis() < deadline) {
    for (auto& n : cluster.nodes) {
      if (n->IsPrimary() && n.get() != old_primary) new_primary = n.get();
    }
    if (new_primary) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_NE(new_primary, nullptr);

  // Reconnect the old primary; the next replicate/heartbeat exchange should
  // fence it off once it observes the new, higher epoch.
  cluster.network->SetIsolated(1, false);
  uint64_t seq = 0;
  new_primary->Replicate("post-partition", &seq);

  deadline = NowMillis() + 3000;
  bool stepped_down = false;
  while (NowMillis() < deadline) {
    if (!old_primary->IsPrimary()) {
      stepped_down = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_TRUE(stepped_down);
}

TEST(ReplicationNode, LaggingBackupCatchesUp) {
  testutil::TempDir dir("repl_catchup");
  TestCluster cluster;
  cluster.Start(3, dir.path());
  ReplicationNode* primary = cluster.FindPrimary(3000);
  ASSERT_NE(primary, nullptr);

  size_t lagging_idx = cluster.nodes.size();
  for (size_t i = 0; i < cluster.nodes.size(); i++) {
    if (cluster.nodes[i].get() != primary) {
      lagging_idx = i;
      break;
    }
  }
  ASSERT_LT(lagging_idx, cluster.nodes.size());
  uint64_t lagging_id = lagging_idx + 1;

  cluster.network->SetIsolated(lagging_id, true);
  for (int i = 0; i < 5; i++) {
    uint64_t seq = 0;
    ASSERT_TRUE(primary->Replicate("k" + std::to_string(i) + "=v" + std::to_string(i), &seq).ok());
  }
  cluster.network->SetIsolated(lagging_id, false);

  uint64_t deadline = NowMillis() + 3000;
  bool caught_up = false;
  while (NowMillis() < deadline) {
    if (cluster.maps[lagging_idx]->Get("k4") == "v4") {
      caught_up = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }
  EXPECT_TRUE(caught_up);
}
