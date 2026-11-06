// Automated fault-injection tests: process crashes, primary crashes, backup
// crashes, restarts, and recovery from WAL/replication logs, all driven
// against real KvServer/TCP/replication/LSM stacks.
#include <gtest/gtest.h>

#include "cluster_test_harness.h"

using namespace kvdb;
using kvdb::testutil::RealCluster;

TEST(FaultInjection, KillPrimaryClusterPromotesAndStaysAvailable) {
  testutil::TempDir dir("fault_kill_primary");
  RealCluster cluster;
  cluster.Start(3, dir.path(), /*sync_writes=*/true);
  KvServer* primary = cluster.FindPrimary(5000);
  ASSERT_NE(primary, nullptr);

  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  ASSERT_TRUE(client.Put("before-crash", "value").ok());

  size_t primary_idx = 0;
  for (size_t i = 0; i < cluster.size(); i++) {
    if (cluster.at(i) == primary) primary_idx = i;
  }
  cluster.KillNode(primary_idx);

  uint64_t deadline = NowMillis() + 6000;
  KvServer* new_primary = nullptr;
  while (NowMillis() < deadline) {
    for (size_t i = 0; i < cluster.size(); i++) {
      if (i != primary_idx && cluster.at(i) && cluster.at(i)->repl() &&
          cluster.at(i)->repl()->IsPrimary()) {
        new_primary = cluster.at(i);
        break;
      }
    }
    if (new_primary) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_NE(new_primary, nullptr);

  KvClient client2;
  ASSERT_TRUE(client2.Connect(cluster.Endpoints()).ok());
  ASSERT_TRUE(client2.Put("after-crash", "still-works").ok());
  std::string value;
  ASSERT_TRUE(client2.Get("before-crash", &value).ok());
  EXPECT_EQ(value, "value");
  ASSERT_TRUE(client2.Get("after-crash", &value).ok());
  EXPECT_EQ(value, "still-works");
}

TEST(FaultInjection, KillBackupDoesNotAffectAvailability) {
  testutil::TempDir dir("fault_kill_backup");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  KvServer* primary = cluster.FindPrimary(5000);
  ASSERT_NE(primary, nullptr);

  size_t backup_idx = cluster.size();
  for (size_t i = 0; i < cluster.size(); i++) {
    if (cluster.at(i) != primary) {
      backup_idx = i;
      break;
    }
  }
  ASSERT_LT(backup_idx, cluster.size());
  cluster.KillNode(backup_idx);

  // With one backup down, the primary is still up: writes must keep
  // succeeding uninterrupted, and the primary should not change.
  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(client.Put("k" + std::to_string(i), "v").ok());
  }
  EXPECT_TRUE(primary->repl()->IsPrimary());
}

TEST(FaultInjection, RestartedBackupRecoversAndCatchesUp) {
  testutil::TempDir dir("fault_restart_backup");
  RealCluster cluster;
  cluster.Start(3, dir.path(), /*sync_writes=*/true);
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  size_t victim_idx = 1;
  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  ASSERT_TRUE(client.Put("pre-restart", "v1").ok());

  cluster.KillNode(victim_idx);
  ASSERT_TRUE(client.Put("during-downtime", "v2").ok());

  ASSERT_TRUE(cluster.RestartNode(victim_idx).ok());

  uint64_t deadline = NowMillis() + 5000;
  std::string value;
  bool caught_up = false;
  while (NowMillis() < deadline) {
    KvClient direct;
    if (direct.Connect(cluster.EndpointFor(victim_idx)).ok()) {
      Status s = direct.Get("during-downtime", &value, ReadConsistency::kStale);
      if (s.ok() && value == "v2") {
        caught_up = true;
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(caught_up);
}

TEST(FaultInjection, ProcessCrashDuringWritesRecoversAllAcknowledgedWrites) {
  // Standalone (no replication) node: exercises the storage layer's own
  // crash recovery guarantee independent of replication.
  testutil::TempDir dir("fault_process_crash");
  ServerConfig cfg;
  cfg.node_id = 1;
  cfg.bind_addr = "127.0.0.1";
  cfg.port = static_cast<uint16_t>(testutil::FreeTcpPort());
  cfg.standalone = true;
  cfg.storage.data_dir = dir.path();
  cfg.storage.sync_writes = true;
  cfg.io_threads = 1;
  cfg.worker_threads = 4;

  int acknowledged = 0;
  {
    KvServer server(cfg);
    ASSERT_TRUE(server.Start().ok());
    KvClient client;
    ASSERT_TRUE(client.Connect("127.0.0.1:" + std::to_string(cfg.port)).ok());
    for (int i = 0; i < 100; i++) {
      if (client.Put("key" + std::to_string(i), "value" + std::to_string(i)).ok()) {
        acknowledged++;
      }
    }
    server.store()->SimulateCrash();
  }

  KvServer server2(cfg);
  ASSERT_TRUE(server2.Start().ok());
  KvClient client2;
  ASSERT_TRUE(client2.Connect("127.0.0.1:" + std::to_string(cfg.port)).ok());
  int recovered = 0;
  for (int i = 0; i < 100; i++) {
    std::string value;
    if (client2.Get("key" + std::to_string(i), &value).ok() &&
        value == "value" + std::to_string(i)) {
      recovered++;
    }
  }
  EXPECT_EQ(recovered, acknowledged);
  server2.Stop();
}

TEST(FaultInjection, RollingRestartOfAllNodesPreservesData) {
  testutil::TempDir dir("fault_rolling_restart");
  RealCluster cluster;
  cluster.Start(3, dir.path(), /*sync_writes=*/true);
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  for (int i = 0; i < 30; i++) {
    ASSERT_TRUE(client.Put("rolling" + std::to_string(i), "v" + std::to_string(i)).ok());
  }

  for (size_t i = 0; i < cluster.size(); i++) {
    cluster.KillNode(i);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_TRUE(cluster.RestartNode(i).ok());
    ASSERT_NE(cluster.FindPrimary(5000), nullptr);
  }

  KvClient verifier;
  ASSERT_TRUE(verifier.Connect(cluster.Endpoints()).ok());
  for (int i = 0; i < 30; i++) {
    std::string value;
    ASSERT_TRUE(verifier.Get("rolling" + std::to_string(i), &value,
                             ReadConsistency::kLinearizable)
                   .ok());
    EXPECT_EQ(value, "v" + std::to_string(i));
  }
}

TEST(FaultInjection, RepeatedPrimaryCrashesEventuallyStabilize) {
  // Stress the promotion path itself: kill the primary repeatedly and
  // confirm the cluster keeps recovering rather than getting stuck.
  testutil::TempDir dir("fault_repeated_primary_crash");
  RealCluster cluster;
  cluster.Start(5, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  for (int round = 0; round < 3; round++) {
    KvServer* primary = cluster.FindPrimary(5000);
    ASSERT_NE(primary, nullptr) << "round " << round;
    size_t idx = cluster.size();
    for (size_t i = 0; i < cluster.size(); i++) {
      if (cluster.at(i) == primary) idx = i;
    }
    ASSERT_LT(idx, cluster.size());
    cluster.KillNode(idx);

    KvServer* next = cluster.FindPrimary(6000);
    ASSERT_NE(next, nullptr) << "no primary promoted in round " << round;

    ASSERT_TRUE(cluster.RestartNode(idx).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  EXPECT_TRUE(client.Put("final-check", "ok").ok());
}
