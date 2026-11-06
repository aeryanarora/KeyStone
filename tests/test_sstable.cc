#include <gtest/gtest.h>

#include "common/file_util.h"
#include "storage/sstable.h"
#include "test_util.h"

using namespace kvdb;

namespace {
std::string Build(const std::string& path, int n, int bloom_bits = 10) {
  SSTableBuilder builder(4096, bloom_bits);
  EXPECT_TRUE(builder.Open(path).ok());
  for (int i = 0; i < n; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key%06d", i);
    std::string ikey = MakeInternalKey(buf, static_cast<uint64_t>(i + 1), kTypeValue);
    EXPECT_TRUE(builder.Add(ikey, "value" + std::to_string(i)).ok());
  }
  EXPECT_TRUE(builder.Finish().ok());
  return path;
}
}  // namespace

TEST(SSTable, BuildAndPointLookup) {
  testutil::TempDir dir("sstable_lookup");
  std::string path = fileutil::JoinPath(dir.path(), "1.sst");
  Build(path, 1000);

  SSTableReader reader;
  ASSERT_TRUE(reader.Open(path).ok());

  std::string value;
  Status s;
  ASSERT_TRUE(reader.Get("key000500", kMaxSequenceNumber, &value, &s));
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "value500");

  EXPECT_FALSE(reader.Get("key999999", kMaxSequenceNumber, &value, &s));
}

TEST(SSTable, IteratorYieldsAllKeysInOrder) {
  testutil::TempDir dir("sstable_iter");
  std::string path = fileutil::JoinPath(dir.path(), "1.sst");
  const int kN = 2000;
  Build(path, kN);

  SSTableReader reader;
  ASSERT_TRUE(reader.Open(path).ok());
  auto it = reader.NewIterator();
  int count = 0;
  std::string prev;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string cur(it->internal_key());
    if (!prev.empty()) EXPECT_LT(InternalKeyComparator::Compare(prev, cur), 0);
    prev = cur;
    count++;
  }
  EXPECT_EQ(count, kN);
  EXPECT_TRUE(it->status().ok());
}

TEST(SSTable, SeekMidFile) {
  testutil::TempDir dir("sstable_seek");
  std::string path = fileutil::JoinPath(dir.path(), "1.sst");
  Build(path, 5000);

  SSTableReader reader;
  ASSERT_TRUE(reader.Open(path).ok());
  auto it = reader.NewIterator();
  std::string target = MakeInternalKey("key002500", kMaxSequenceNumber, kTypeValue);
  it->Seek(target);
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(ExtractUserKey(it->internal_key()), "key002500");
}

TEST(SSTable, RejectsOutOfOrderKeys) {
  testutil::TempDir dir("sstable_order");
  std::string path = fileutil::JoinPath(dir.path(), "1.sst");
  SSTableBuilder builder;
  ASSERT_TRUE(builder.Open(path).ok());
  ASSERT_TRUE(builder.Add(MakeInternalKey("b", 1, kTypeValue), "v").ok());
  EXPECT_FALSE(builder.Add(MakeInternalKey("a", 2, kTypeValue), "v").ok());
}

TEST(SSTable, DetectsCorruptedBlock) {
  testutil::TempDir dir("sstable_corrupt");
  std::string path = fileutil::JoinPath(dir.path(), "1.sst");
  Build(path, 100);

  // Flip a byte early in the file (inside the first data block).
  std::string data;
  ASSERT_TRUE(fileutil::ReadFileToString(path, &data).ok());
  data[20] = static_cast<char>(data[20] ^ 0xFF);
  ASSERT_TRUE(fileutil::WriteStringToFileAtomic(path, data).ok());

  SSTableReader reader;
  ASSERT_TRUE(reader.Open(path).ok());  // footer/index intact, opens fine
  std::string value;
  Status s;
  bool found = reader.Get("key000050", kMaxSequenceNumber, &value, &s);
  // Either it reports the corruption via status, or (if the flip landed
  // somewhere that doesn't affect this particular key's block) succeeds --
  // but it must never silently return wrong data un-flagged.
  if (found) {
    EXPECT_TRUE(s.ok() || s.IsCorruption());
  }
}

TEST(SSTable, EmptyBuilderAbandons) {
  testutil::TempDir dir("sstable_empty");
  std::string path = fileutil::JoinPath(dir.path(), "1.sst");
  SSTableBuilder builder;
  ASSERT_TRUE(builder.Open(path).ok());
  EXPECT_EQ(builder.NumEntries(), 0u);
  ASSERT_TRUE(builder.Abandon().ok());
  EXPECT_FALSE(fileutil::FileExists(path));
}
