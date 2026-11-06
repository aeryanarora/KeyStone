#include <gtest/gtest.h>

#include "common/file_util.h"
#include "storage/wal.h"
#include "test_util.h"

using namespace kvdb;

TEST(Wal, WriteAndReadSingleRecord) {
  testutil::TempDir dir("wal_single");
  std::string path = fileutil::JoinPath(dir.path(), "test.wal");
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path).ok());
    ASSERT_TRUE(w.AddRecord("hello world").ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  WalReader r;
  ASSERT_TRUE(r.Open(path).ok());
  std::string record;
  ASSERT_TRUE(r.ReadRecord(&record).ok());
  EXPECT_EQ(record, "hello world");
  EXPECT_TRUE(r.ReadRecord(&record).IsNotFound());
}

TEST(Wal, MultipleRecordsPreserveOrder) {
  testutil::TempDir dir("wal_multi");
  std::string path = fileutil::JoinPath(dir.path(), "test.wal");
  std::vector<std::string> records = {"one", "two", "three", "", std::string(500, 'z')};
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path).ok());
    for (const auto& r : records) ASSERT_TRUE(w.AddRecord(r).ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  WalReader r;
  ASSERT_TRUE(r.Open(path).ok());
  for (const auto& expected : records) {
    std::string got;
    ASSERT_TRUE(r.ReadRecord(&got).ok());
    EXPECT_EQ(got, expected);
  }
  std::string trailing;
  EXPECT_TRUE(r.ReadRecord(&trailing).IsNotFound());
}

TEST(Wal, RecordSpanningMultipleBlocks) {
  testutil::TempDir dir("wal_span");
  std::string path = fileutil::JoinPath(dir.path(), "test.wal");
  // Bigger than one 32KB block, forcing FIRST/MIDDLE/LAST fragmentation.
  std::string big(100000, 'A');
  for (size_t i = 0; i < big.size(); i++) big[i] = static_cast<char>('A' + (i % 26));
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path).ok());
    ASSERT_TRUE(w.AddRecord("small before").ok());
    ASSERT_TRUE(w.AddRecord(big).ok());
    ASSERT_TRUE(w.AddRecord("small after").ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  WalReader r;
  ASSERT_TRUE(r.Open(path).ok());
  std::string got;
  ASSERT_TRUE(r.ReadRecord(&got).ok());
  EXPECT_EQ(got, "small before");
  ASSERT_TRUE(r.ReadRecord(&got).ok());
  EXPECT_EQ(got, big);
  ASSERT_TRUE(r.ReadRecord(&got).ok());
  EXPECT_EQ(got, "small after");
}

TEST(Wal, AppendAcrossReopen) {
  testutil::TempDir dir("wal_append");
  std::string path = fileutil::JoinPath(dir.path(), "test.wal");
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path).ok());
    ASSERT_TRUE(w.AddRecord("first").ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path, /*append=*/true).ok());
    ASSERT_TRUE(w.AddRecord("second").ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  WalReader r;
  ASSERT_TRUE(r.Open(path).ok());
  std::string got;
  ASSERT_TRUE(r.ReadRecord(&got).ok());
  EXPECT_EQ(got, "first");
  ASSERT_TRUE(r.ReadRecord(&got).ok());
  EXPECT_EQ(got, "second");
}

// The core crash-safety property: a WAL truncated mid-record must be readable
// up to the last complete record, and the reader must report the failure
// (not silently skip or fabricate data) rather than crash or hang.
TEST(Wal, TruncatedTailIsDetected) {
  testutil::TempDir dir("wal_trunc");
  std::string path = fileutil::JoinPath(dir.path(), "test.wal");
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path).ok());
    ASSERT_TRUE(w.AddRecord("complete record one").ok());
    ASSERT_TRUE(w.AddRecord("complete record two").ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  uint64_t full_size;
  ASSERT_TRUE(fileutil::GetFileSize(path, &full_size).ok());

  // Truncate away the last several bytes to simulate a torn write.
  std::string data;
  ASSERT_TRUE(fileutil::ReadFileToString(path, &data).ok());
  data.resize(data.size() - 5);
  ASSERT_TRUE(fileutil::WriteStringToFileAtomic(path, data).ok());

  WalReader r;
  ASSERT_TRUE(r.Open(path).ok());
  std::string got;
  ASSERT_TRUE(r.ReadRecord(&got).ok());
  EXPECT_EQ(got, "complete record one");
  // Second record's tail is torn: must not silently succeed.
  Status s = r.ReadRecord(&got);
  EXPECT_FALSE(s.ok());
}

TEST(Wal, CorruptedChecksumDetected) {
  testutil::TempDir dir("wal_crc");
  std::string path = fileutil::JoinPath(dir.path(), "test.wal");
  {
    WalWriter w;
    ASSERT_TRUE(w.Open(path).ok());
    ASSERT_TRUE(w.AddRecord("integrity matters").ok());
    ASSERT_TRUE(w.Sync().ok());
  }
  std::string data;
  ASSERT_TRUE(fileutil::ReadFileToString(path, &data).ok());
  // Flip a byte in the middle of the record payload (past the 7-byte header).
  ASSERT_GT(data.size(), 10u);
  data[10] = static_cast<char>(data[10] ^ 0xFF);
  ASSERT_TRUE(fileutil::WriteStringToFileAtomic(path, data).ok());

  WalReader r;
  ASSERT_TRUE(r.Open(path).ok());
  std::string got;
  EXPECT_TRUE(r.ReadRecord(&got).IsCorruption());
}
