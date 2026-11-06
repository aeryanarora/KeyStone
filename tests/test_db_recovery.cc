#include <gtest/gtest.h>

#include "storage/db.h"
#include "test_util.h"

using namespace kvdb;

namespace {
Options TestOptions(const std::string& dir) {
  Options o;
  o.data_dir = dir;
  o.write_buffer_size = 1 * 1024 * 1024;  // big enough that flush doesn't race the test
  o.background_compaction = false;
  o.sync_writes = true;  // durability guarantee is what we're testing
  return o;
}
}  // namespace

TEST(Recovery, CleanCloseThenReopenPreservesData) {
  testutil::TempDir dir("recovery_clean");
  Options opts = TestOptions(dir.path());
  {
    std::unique_ptr<LsmStore> db;
    ASSERT_TRUE(LsmStore::Open(opts, &db).ok());
    WriteOptions wo;
    for (int i = 0; i < 100; i++) ASSERT_TRUE(db->Put(wo, "k" + std::to_string(i), "v" + std::to_string(i)).ok());
    ASSERT_TRUE(db->Close().ok());
  }
  std::unique_ptr<LsmStore> db2;
  ASSERT_TRUE(LsmStore::Open(opts, &db2).ok());
  std::string value;
  ASSERT_TRUE(db2->Get(ReadOptions(), "k50", &value).ok());
  EXPECT_EQ(value, "v50");
}

TEST(Recovery, SimulatedCrashReplaysWal) {
  testutil::TempDir dir("recovery_crash");
  Options opts = TestOptions(dir.path());
  {
    std::unique_ptr<LsmStore> db;
    ASSERT_TRUE(LsmStore::Open(opts, &db).ok());
    WriteOptions wo;
    for (int i = 0; i < 300; i++) {
      ASSERT_TRUE(db->Put(wo, "key" + std::to_string(i), "value" + std::to_string(i)).ok());
    }
    db->SimulateCrash();  // no clean shutdown: exercises WAL replay on reopen
  }
  std::unique_ptr<LsmStore> db2;
  ASSERT_TRUE(LsmStore::Open(opts, &db2).ok());
  for (int i = 0; i < 300; i++) {
    std::string value;
    ASSERT_TRUE(db2->Get(ReadOptions(), "key" + std::to_string(i), &value).ok())
        << "missing key" << i;
    EXPECT_EQ(value, "value" + std::to_string(i));
  }
}

TEST(Recovery, CrashAfterDeletesPreservesTombstones) {
  testutil::TempDir dir("recovery_tombstone");
  Options opts = TestOptions(dir.path());
  {
    std::unique_ptr<LsmStore> db;
    ASSERT_TRUE(LsmStore::Open(opts, &db).ok());
    WriteOptions wo;
    for (int i = 0; i < 50; i++) ASSERT_TRUE(db->Put(wo, "k" + std::to_string(i), "v").ok());
    for (int i = 0; i < 50; i += 2) ASSERT_TRUE(db->Delete(wo, "k" + std::to_string(i)).ok());
    db->SimulateCrash();
  }
  std::unique_ptr<LsmStore> db2;
  ASSERT_TRUE(LsmStore::Open(opts, &db2).ok());
  for (int i = 0; i < 50; i++) {
    std::string value;
    Status s = db2->Get(ReadOptions(), "k" + std::to_string(i), &value);
    if (i % 2 == 0) {
      EXPECT_TRUE(s.IsNotFound()) << "k" << i;
    } else {
      EXPECT_TRUE(s.ok()) << "k" << i;
    }
  }
}

TEST(Recovery, MultipleCrashCycles) {
  testutil::TempDir dir("recovery_multi_cycle");
  Options opts = TestOptions(dir.path());
  int total_written = 0;
  for (int cycle = 0; cycle < 5; cycle++) {
    std::unique_ptr<LsmStore> db;
    ASSERT_TRUE(LsmStore::Open(opts, &db).ok());
    WriteOptions wo;
    for (int i = 0; i < 20; i++) {
      std::string key = "cycle" + std::to_string(cycle) + "_k" + std::to_string(i);
      ASSERT_TRUE(db->Put(wo, key, "v").ok());
      total_written++;
    }
    db->SimulateCrash();
  }
  std::unique_ptr<LsmStore> final_db;
  ASSERT_TRUE(LsmStore::Open(opts, &final_db).ok());
  int found = 0;
  for (int cycle = 0; cycle < 5; cycle++) {
    for (int i = 0; i < 20; i++) {
      std::string key = "cycle" + std::to_string(cycle) + "_k" + std::to_string(i);
      std::string value;
      if (final_db->Get(ReadOptions(), key, &value).ok()) found++;
    }
  }
  EXPECT_EQ(found, total_written);
}

TEST(Recovery, FlushedDataSurvivesCrashOfNewWrites) {
  testutil::TempDir dir("recovery_flush_then_crash");
  Options opts = TestOptions(dir.path());
  {
    std::unique_ptr<LsmStore> db;
    ASSERT_TRUE(LsmStore::Open(opts, &db).ok());
    WriteOptions wo;
    for (int i = 0; i < 30; i++) ASSERT_TRUE(db->Put(wo, "flushed" + std::to_string(i), "v").ok());
    ASSERT_TRUE(db->Flush().ok());  // durably on disk as an SSTable
    for (int i = 0; i < 30; i++) ASSERT_TRUE(db->Put(wo, "unflushed" + std::to_string(i), "v").ok());
    db->SimulateCrash();  // only the unflushed batch relies on WAL replay
  }
  std::unique_ptr<LsmStore> db2;
  ASSERT_TRUE(LsmStore::Open(opts, &db2).ok());
  std::string value;
  for (int i = 0; i < 30; i++) {
    EXPECT_TRUE(db2->Get(ReadOptions(), "flushed" + std::to_string(i), &value).ok());
    EXPECT_TRUE(db2->Get(ReadOptions(), "unflushed" + std::to_string(i), &value).ok());
  }
}
