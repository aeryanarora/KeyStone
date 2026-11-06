#include <gtest/gtest.h>

#include "replication/replication_log.h"
#include "test_util.h"

using namespace kvdb;

TEST(ReplicationLog, AppendAndReadBack) {
  testutil::TempDir dir("repl_log_basic");
  ReplicationLog log;
  ASSERT_TRUE(log.Open(dir.path(), true).ok());

  ASSERT_TRUE(log.Append(1, "a=1").ok());
  ASSERT_TRUE(log.Append(2, "b=2").ok());
  EXPECT_EQ(log.last_seq(), 2u);

  std::string data;
  ASSERT_TRUE(log.EntryAt(1, &data).ok());
  EXPECT_EQ(data, "a=1");
  ASSERT_TRUE(log.EntryAt(2, &data).ok());
  EXPECT_EQ(data, "b=2");
  EXPECT_TRUE(log.EntryAt(3, &data).IsNotFound());
}

TEST(ReplicationLog, RejectsOutOfOrderAppend) {
  testutil::TempDir dir("repl_log_order");
  ReplicationLog log;
  ASSERT_TRUE(log.Open(dir.path(), true).ok());
  ASSERT_TRUE(log.Append(1, "a=1").ok());
  EXPECT_FALSE(log.Append(3, "c=3").ok());
  EXPECT_TRUE(log.Append(2, "b=2").ok());
}

TEST(ReplicationLog, RangeReturnsRequestedWindow) {
  testutil::TempDir dir("repl_log_range");
  ReplicationLog log;
  ASSERT_TRUE(log.Open(dir.path(), true).ok());
  for (uint64_t i = 1; i <= 10; i++) {
    ASSERT_TRUE(log.Append(i, "k" + std::to_string(i)).ok());
  }
  std::vector<std::pair<uint64_t, std::string>> out;
  ASSERT_TRUE(log.Range(5, 3, &out).ok());
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out[0].first, 5u);
  EXPECT_EQ(out[0].second, "k5");
  EXPECT_EQ(out[2].first, 7u);
}

TEST(ReplicationLog, EpochPersistsAcrossReopen) {
  testutil::TempDir dir("repl_log_epoch");
  {
    ReplicationLog log;
    ASSERT_TRUE(log.Open(dir.path(), true).ok());
    ASSERT_TRUE(log.SaveEpoch(7, 3).ok());
  }
  ReplicationLog log2;
  ASSERT_TRUE(log2.Open(dir.path(), true).ok());
  EXPECT_EQ(log2.epoch(), 7u);
  EXPECT_EQ(log2.primary_id(), 3u);
}

TEST(ReplicationLog, EntriesSurviveReopen) {
  testutil::TempDir dir("repl_log_reopen");
  {
    ReplicationLog log;
    ASSERT_TRUE(log.Open(dir.path(), true).ok());
    for (uint64_t i = 1; i <= 5; i++) ASSERT_TRUE(log.Append(i, "v" + std::to_string(i)).ok());
  }
  ReplicationLog log2;
  ASSERT_TRUE(log2.Open(dir.path(), true).ok());
  EXPECT_EQ(log2.last_seq(), 5u);
  std::string data;
  ASSERT_TRUE(log2.EntryAt(3, &data).ok());
  EXPECT_EQ(data, "v3");
}
