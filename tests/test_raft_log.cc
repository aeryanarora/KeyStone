#include <gtest/gtest.h>

#include "raft/raft_log.h"
#include "test_util.h"

using namespace kvdb;

TEST(RaftLog, AppendAndRetrieve) {
  testutil::TempDir dir("raftlog_basic");
  RaftLog log;
  ASSERT_TRUE(log.Open(dir.path(), false).ok());

  LogEntry e1{1, 1, EntryType::kData, "first"};
  LogEntry e2{1, 2, EntryType::kData, "second"};
  ASSERT_TRUE(log.Append({e1, e2}).ok());

  EXPECT_EQ(log.last_index(), 2u);
  EXPECT_EQ(log.last_term(), 1u);

  LogEntry got;
  ASSERT_TRUE(log.EntryAt(1, &got).ok());
  EXPECT_EQ(got.data, "first");
  ASSERT_TRUE(log.EntryAt(2, &got).ok());
  EXPECT_EQ(got.data, "second");
}

TEST(RaftLog, HardStatePersistsAcrossReopen) {
  testutil::TempDir dir("raftlog_hardstate");
  {
    RaftLog log;
    ASSERT_TRUE(log.Open(dir.path(), false).ok());
    ASSERT_TRUE(log.SaveHardState(5, 3).ok());
  }
  RaftLog log2;
  ASSERT_TRUE(log2.Open(dir.path(), false).ok());
  EXPECT_EQ(log2.current_term(), 5u);
  EXPECT_EQ(log2.voted_for(), 3u);
}

TEST(RaftLog, EntriesPersistAcrossReopen) {
  testutil::TempDir dir("raftlog_persist");
  {
    RaftLog log;
    ASSERT_TRUE(log.Open(dir.path(), true).ok());
    for (int i = 1; i <= 10; i++) {
      LogEntry e{1, static_cast<uint64_t>(i), EntryType::kData, "entry" + std::to_string(i)};
      ASSERT_TRUE(log.Append({e}).ok());
    }
  }
  RaftLog log2;
  ASSERT_TRUE(log2.Open(dir.path(), true).ok());
  EXPECT_EQ(log2.last_index(), 10u);
  LogEntry got;
  ASSERT_TRUE(log2.EntryAt(5, &got).ok());
  EXPECT_EQ(got.data, "entry5");
}

TEST(RaftLog, TruncateSuffixDropsConflictingEntries) {
  testutil::TempDir dir("raftlog_truncsuffix");
  RaftLog log;
  ASSERT_TRUE(log.Open(dir.path(), false).ok());
  for (int i = 1; i <= 5; i++) {
    LogEntry e{1, static_cast<uint64_t>(i), EntryType::kData, "v" + std::to_string(i)};
    ASSERT_TRUE(log.Append({e}).ok());
  }
  ASSERT_TRUE(log.TruncateSuffix(3).ok());  // drop entries at index >= 3
  EXPECT_EQ(log.last_index(), 2u);
  LogEntry got;
  EXPECT_FALSE(log.EntryAt(3, &got).ok());
  ASSERT_TRUE(log.EntryAt(2, &got).ok());
  EXPECT_EQ(got.data, "v2");
}

TEST(RaftLog, TruncatePrefixCompactsOldEntries) {
  testutil::TempDir dir("raftlog_truncprefix");
  RaftLog log;
  ASSERT_TRUE(log.Open(dir.path(), false).ok());
  for (int i = 1; i <= 10; i++) {
    LogEntry e{1, static_cast<uint64_t>(i), EntryType::kData, "v" + std::to_string(i)};
    ASSERT_TRUE(log.Append({e}).ok());
  }
  ASSERT_TRUE(log.SaveSnapshot(5, 1, "snapshot-data").ok());
  EXPECT_EQ(log.first_index(), 6u);
  LogEntry got;
  EXPECT_FALSE(log.EntryAt(5, &got).ok());
  ASSERT_TRUE(log.EntryAt(6, &got).ok());
  EXPECT_EQ(got.data, "v6");
}

TEST(RaftLog, RangeRespectsMaxEntriesAndBytes) {
  testutil::TempDir dir("raftlog_range");
  RaftLog log;
  ASSERT_TRUE(log.Open(dir.path(), false).ok());
  for (int i = 1; i <= 20; i++) {
    LogEntry e{1, static_cast<uint64_t>(i), EntryType::kData, "payload"};
    ASSERT_TRUE(log.Append({e}).ok());
  }
  std::vector<LogEntry> out;
  ASSERT_TRUE(log.Range(1, 20, 5, 1 << 20, &out).ok());
  EXPECT_EQ(out.size(), 5u);
  EXPECT_EQ(out.front().index, 1u);
  EXPECT_EQ(out.back().index, 5u);
}

TEST(RaftLog, SnapshotRoundTrip) {
  testutil::TempDir dir("raftlog_snapshot");
  RaftLog log;
  ASSERT_TRUE(log.Open(dir.path(), false).ok());
  ASSERT_TRUE(log.SaveSnapshot(10, 2, "the-snapshot-bytes").ok());
  EXPECT_TRUE(log.HasSnapshot());
  EXPECT_EQ(log.snapshot_index(), 10u);
  EXPECT_EQ(log.snapshot_term(), 2u);
  std::string data;
  ASSERT_TRUE(log.LoadSnapshot(&data).ok());
  EXPECT_EQ(data, "the-snapshot-bytes");
}

TEST(RaftLog, TruncatedTailToleratedOnReopen) {
  testutil::TempDir dir("raftlog_torn");
  {
    RaftLog log;
    ASSERT_TRUE(log.Open(dir.path(), true).ok());
    for (int i = 1; i <= 5; i++) {
      LogEntry e{1, static_cast<uint64_t>(i), EntryType::kData, "v" + std::to_string(i)};
      ASSERT_TRUE(log.Append({e}).ok());
    }
  }
  // Simulate a crash mid-append by truncating the raw log file.
  std::string path = dir.path() + "/raft.log";
  std::string data;
  ASSERT_TRUE(fileutil::ReadFileToString(path, &data).ok());
  data.resize(data.size() - 3);
  ASSERT_TRUE(fileutil::WriteStringToFileAtomic(path, data).ok());

  RaftLog log2;
  ASSERT_TRUE(log2.Open(dir.path(), true).ok());  // must not fail to open
  // At least the earlier, complete entries must survive.
  EXPECT_GE(log2.last_index(), 1u);
}
