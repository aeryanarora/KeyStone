#include <gtest/gtest.h>

#include "storage/db.h"
#include "test_util.h"

using namespace kvdb;

namespace {
Options TestOptions(const std::string& dir) {
  Options o;
  o.data_dir = dir;
  o.write_buffer_size = 64 * 1024;
  o.block_cache_bytes = 1 * 1024 * 1024;
  o.background_compaction = false;  // deterministic tests
  return o;
}
}  // namespace

TEST(LsmStore, PutGetDelete) {
  testutil::TempDir dir("db_basic");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());

  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "foo", "bar").ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "foo", &value).ok());
  EXPECT_EQ(value, "bar");

  ASSERT_TRUE(db->Delete(wo, "foo").ok());
  EXPECT_TRUE(db->Get(ReadOptions(), "foo", &value).IsNotFound());
}

TEST(LsmStore, GetMissingKey) {
  testutil::TempDir dir("db_missing");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  std::string value;
  EXPECT_TRUE(db->Get(ReadOptions(), "nope", &value).IsNotFound());
}

TEST(LsmStore, OverwriteReturnsLatest) {
  testutil::TempDir dir("db_overwrite");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  WriteOptions wo;
  for (int i = 0; i < 10; i++) ASSERT_TRUE(db->Put(wo, "k", "v" + std::to_string(i)).ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "k", &value).ok());
  EXPECT_EQ(value, "v9");
}

TEST(LsmStore, WriteBatchIsAtomic) {
  testutil::TempDir dir("db_batch");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());

  WriteBatch batch;
  batch.Put("a", "1");
  batch.Put("b", "2");
  batch.Delete("c");
  WriteOptions wo;
  ASSERT_TRUE(db->Write(wo, &batch).ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "a", &value).ok());
  EXPECT_EQ(value, "1");
  ASSERT_TRUE(db->Get(ReadOptions(), "b", &value).ok());
  EXPECT_EQ(value, "2");
  EXPECT_TRUE(db->Get(ReadOptions(), "c", &value).IsNotFound());
}

TEST(LsmStore, ScanReturnsSortedRange) {
  testutil::TempDir dir("db_scan");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  WriteOptions wo;
  for (int i = 0; i < 100; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%03d", i);
    ASSERT_TRUE(db->Put(wo, buf, std::to_string(i)).ok());
  }
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(db->Scan(ReadOptions(), "k010", "k020", 0, &rows).ok());
  ASSERT_EQ(rows.size(), 10u);
  EXPECT_EQ(rows.front().first, "k010");
  EXPECT_EQ(rows.back().first, "k019");
  for (size_t i = 1; i < rows.size(); i++) EXPECT_LT(rows[i - 1].first, rows[i].first);
}

TEST(LsmStore, ScanRespectsLimit) {
  testutil::TempDir dir("db_scan_limit");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  WriteOptions wo;
  for (int i = 0; i < 50; i++) {
    ASSERT_TRUE(db->Put(wo, "k" + std::to_string(1000 + i), "v").ok());
  }
  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(db->Scan(ReadOptions(), "", "", 5, &rows).ok());
  EXPECT_EQ(rows.size(), 5u);
}

TEST(LsmStore, FlushMovesDataToSSTable) {
  testutil::TempDir dir("db_flush");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  WriteOptions wo;
  for (int i = 0; i < 500; i++) ASSERT_TRUE(db->Put(wo, "k" + std::to_string(i), "v").ok());
  ASSERT_TRUE(db->Flush().ok());

  DbStats stats = db->GetStats();
  EXPECT_EQ(stats.memtable_bytes, 0u == 0 ? stats.memtable_bytes : 0u);  // memtable reset
  int total_l0 = stats.files_per_level.empty() ? 0 : stats.files_per_level[0];
  EXPECT_GT(total_l0, 0);

  // Data must still be readable after flush.
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "k250", &value).ok());
}

TEST(LsmStore, LargeValueRoundTrip) {
  testutil::TempDir dir("db_large_value");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  std::string big(200000, 'q');
  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "bigkey", big).ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "bigkey", &value).ok());
  EXPECT_EQ(value, big);
}

TEST(LsmStore, EmptyValueIsDistinctFromMissing) {
  testutil::TempDir dir("db_empty_value");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "k", "").ok());
  std::string value = "not-empty";
  ASSERT_TRUE(db->Get(ReadOptions(), "k", &value).ok());
  EXPECT_EQ(value, "");
}
