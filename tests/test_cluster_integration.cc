#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "cluster_test_harness.h"

using namespace kvdb;
using kvdb::testutil::RealCluster;

TEST(ClusterIntegration, WriteThenReadBackThroughClient) {
  testutil::TempDir dir("cluster_write_read");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  ASSERT_TRUE(client.Put("foo", "bar").ok());
  std::string value;
  ASSERT_TRUE(client.Get("foo", &value).ok());
  EXPECT_EQ(value, "bar");
}

TEST(ClusterIntegration, WriteToBackupRedirectsToPrimary) {
  testutil::TempDir dir("cluster_redirect");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  // Connect directly to node 0, whichever role it holds; CallWithRetry must
  // find the primary via the NotLeader hint regardless.
  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.EndpointFor(0)).ok());
  ASSERT_TRUE(client.Put("k", "v").ok());
  std::string value;
  ASSERT_TRUE(client.Get("k", &value).ok());
  EXPECT_EQ(value, "v");
}

TEST(ClusterIntegration, LinearizableReadFromBackupSeesLatestWrite) {
  testutil::TempDir dir("cluster_linearizable");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  KvServer* primary = cluster.FindPrimary(5000);
  ASSERT_NE(primary, nullptr);

  KvClient writer;
  ASSERT_TRUE(writer.Connect(cluster.Endpoints()).ok());
  ASSERT_TRUE(writer.Put("linear-key", "linear-value").ok());

  for (size_t i = 0; i < cluster.size(); i++) {
    KvClient reader;
    ASSERT_TRUE(reader.Connect(cluster.EndpointFor(i)).ok());
    std::string value;
    Status s = reader.Get("linear-key", &value, ReadConsistency::kLinearizable);
    ASSERT_TRUE(s.ok()) << "node " << i << ": " << s.ToString();
    EXPECT_EQ(value, "linear-value");
  }
}

TEST(ClusterIntegration, BatchWriteIsAtomic) {
  testutil::TempDir dir("cluster_batch");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  std::vector<BatchOp> ops;
  ops.push_back({false, "a", "1"});
  ops.push_back({false, "b", "2"});
  ops.push_back({true, "c", ""});
  ASSERT_TRUE(client.Batch(ops).ok());

  std::string value;
  ASSERT_TRUE(client.Get("a", &value).ok());
  EXPECT_EQ(value, "1");
  ASSERT_TRUE(client.Get("b", &value).ok());
  EXPECT_EQ(value, "2");
  EXPECT_TRUE(client.Get("c", &value).IsNotFound());
}

TEST(ClusterIntegration, IncrementIsAtomicAcrossManyClients) {
  testutil::TempDir dir("cluster_incr");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  const int kClients = 5;
  const int kPerClient = 20;
  std::vector<std::thread> threads;
  for (int t = 0; t < kClients; t++) {
    threads.emplace_back([&cluster] {
      KvClient client;
      if (!client.Connect(cluster.Endpoints()).ok()) return;
      for (int i = 0; i < kPerClient; i++) {
        int64_t result = 0;
        client.Incr("shared-counter", 1, &result);
      }
    });
  }
  for (auto& t : threads) t.join();

  KvClient reader;
  ASSERT_TRUE(reader.Connect(cluster.Endpoints()).ok());
  std::string value;
  ASSERT_TRUE(reader.Get("shared-counter", &value, ReadConsistency::kLinearizable).ok());
  EXPECT_EQ(std::stoll(value), kClients * kPerClient);
}

// Companion to IncrementIsAtomicAcrossManyClients for the other conditional
// primitive. Many clients race to CAS the same key from the same expected
// value; the primary must serialize them (see write_mu_ in kv_server.cc) so
// exactly one observes swapped=true.
TEST(ClusterIntegration, CasHasExactlyOneWinnerAcrossManyClients) {
  testutil::TempDir dir("cluster_cas");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  {
    KvClient seed;
    ASSERT_TRUE(seed.Connect(cluster.Endpoints()).ok());
    ASSERT_TRUE(seed.Put("cas-key", "start").ok());
  }

  const int kClients = 8;
  std::atomic<int> winners{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kClients; t++) {
    threads.emplace_back([&cluster, &winners, t] {
      KvClient client;
      if (!client.Connect(cluster.Endpoints()).ok()) return;
      bool swapped = false;
      // Every client swaps from the same expected value to a distinct one.
      if (client.Cas("cas-key", "start", "winner-" + std::to_string(t), false, &swapped).ok() &&
          swapped) {
        winners.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();

  EXPECT_EQ(winners.load(), 1) << "exactly one CAS from the same expected value may succeed";

  // And the surviving value must be one of the winners', not a mix.
  KvClient reader;
  ASSERT_TRUE(reader.Connect(cluster.Endpoints()).ok());
  std::string value;
  ASSERT_TRUE(reader.Get("cas-key", &value, ReadConsistency::kLinearizable).ok());
  EXPECT_EQ(value.rfind("winner-", 0), 0u) << "unexpected surviving value: " << value;
}

// CAS with expect_absent is the create-if-not-exists primitive; it has the
// same race shape, so it gets the same one-winner guarantee.
TEST(ClusterIntegration, CasExpectAbsentHasExactlyOneWinner) {
  testutil::TempDir dir("cluster_cas_absent");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  const int kClients = 8;
  std::atomic<int> winners{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kClients; t++) {
    threads.emplace_back([&cluster, &winners, t] {
      KvClient client;
      if (!client.Connect(cluster.Endpoints()).ok()) return;
      bool swapped = false;
      if (client.Cas("create-once", "", "by-" + std::to_string(t), true, &swapped).ok() && swapped) {
        winners.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();

  EXPECT_EQ(winners.load(), 1) << "exactly one expect-absent CAS may create the key";
}

TEST(ClusterIntegration, ScanReturnsConsistentRange) {
  testutil::TempDir dir("cluster_scan");
  RealCluster cluster;
  cluster.Start(3, dir.path());
  ASSERT_NE(cluster.FindPrimary(5000), nullptr);

  KvClient client;
  ASSERT_TRUE(client.Connect(cluster.Endpoints()).ok());
  for (int i = 0; i < 50; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "s%03d", i);
    ASSERT_TRUE(client.Put(buf, std::to_string(i)).ok());
  }
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(client.Scan("s010", "s020", 0, &rows).ok());
  EXPECT_EQ(rows.size(), 10u);
}
