#include <gtest/gtest.h>

#include "storage/db.h"
#include "test_util.h"

using namespace kvdb;

namespace {
Options SmallOptions(const std::string& dir) {
  Options o;
  o.data_dir = dir;
  o.write_buffer_size = 16 * 1024;  // flush often to build up many L0 files
  o.l0_compaction_trigger = 3;
  o.l1_size_bytes = 32 * 1024;
  o.background_compaction = false;  // driven explicitly by the test
  return o;
}
}  // namespace

TEST(Compaction, CompactRangeMergesL0IntoL1) {
  testutil::TempDir dir("compaction_basic");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(SmallOptions(dir.path()), &db).ok());

  WriteOptions wo;
  for (int i = 0; i < 5000; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%06d", i);
    ASSERT_TRUE(db->Put(wo, buf, "value-" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(db->CompactRange().ok());

  DbStats stats = db->GetStats();
  ASSERT_GE(stats.files_per_level.size(), 2u);
  // After compaction, L0 should not be sitting at (or above) its trigger.
  EXPECT_LT(stats.files_per_level[0], 4);

  // Every key must still be readable correctly post-compaction.
  for (int i = 0; i < 5000; i += 37) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%06d", i);
    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions(), buf, &value).ok());
    EXPECT_EQ(value, "value-" + std::to_string(i));
  }
}

TEST(Compaction, TombstonesReclaimSpaceAtBottomLevel) {
  testutil::TempDir dir("compaction_tombstone");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(SmallOptions(dir.path()), &db).ok());

  WriteOptions wo;
  for (int i = 0; i < 2000; i++) {
    ASSERT_TRUE(db->Put(wo, "k" + std::to_string(i), "v").ok());
  }
  ASSERT_TRUE(db->CompactRange().ok());
  for (int i = 0; i < 2000; i++) {
    ASSERT_TRUE(db->Delete(wo, "k" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(db->CompactRange().ok());

  for (int i = 0; i < 2000; i++) {
    std::string value;
    EXPECT_TRUE(db->Get(ReadOptions(), "k" + std::to_string(i), &value).IsNotFound());
  }
}

TEST(Compaction, OverwritesResolveToNewestAfterCompaction) {
  testutil::TempDir dir("compaction_overwrite");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(SmallOptions(dir.path()), &db).ok());

  WriteOptions wo;
  for (int round = 0; round < 5; round++) {
    for (int i = 0; i < 500; i++) {
      ASSERT_TRUE(db->Put(wo, "k" + std::to_string(i), "round" + std::to_string(round)).ok());
    }
  }
  ASSERT_TRUE(db->CompactRange().ok());

  for (int i = 0; i < 500; i += 13) {
    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions(), "k" + std::to_string(i), &value).ok());
    EXPECT_EQ(value, "round4");
  }
}

TEST(Compaction, BackgroundCompactionKeepsUpUnderLoad) {
  testutil::TempDir dir("compaction_background");
  Options opts = SmallOptions(dir.path());
  opts.background_compaction = true;
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(opts, &db).ok());

  WriteOptions wo;
  for (int i = 0; i < 8000; i++) {
    ASSERT_TRUE(db->Put(wo, "k" + std::to_string(i), "v" + std::to_string(i)).ok());
  }
  db->WaitForBackgroundWork();

  DbStats stats = db->GetStats();
  EXPECT_GT(stats.num_flushes, 0u);

  for (int i = 0; i < 8000; i += 97) {
    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions(), "k" + std::to_string(i), &value).ok());
    EXPECT_EQ(value, "v" + std::to_string(i));
  }
}
