#include <gtest/gtest.h>

#include "storage/memtable.h"
#include "storage/write_batch.h"

using namespace kvdb;

TEST(WriteBatch, EmptyBatchHasZeroCount) {
  WriteBatch b;
  EXPECT_EQ(b.Count(), 0u);
  EXPECT_TRUE(b.empty());
}

TEST(WriteBatch, PutAndDeleteIncrementCount) {
  WriteBatch b;
  b.Put("a", "1");
  b.Delete("b");
  b.Put("c", "3");
  EXPECT_EQ(b.Count(), 3u);
}

namespace {
struct RecordingHandler : public WriteBatch::Handler {
  std::vector<std::string> ops;
  void Put(std::string_view key, std::string_view value, SequenceNumber seq) override {
    ops.push_back("PUT " + std::string(key) + "=" + std::string(value) + "@" +
                  std::to_string(seq));
  }
  void Delete(std::string_view key, SequenceNumber seq) override {
    ops.push_back("DEL " + std::string(key) + "@" + std::to_string(seq));
  }
};
}  // namespace

TEST(WriteBatch, IterateAssignsSequentialSequenceNumbers) {
  WriteBatch b;
  b.SetSequence(100);
  b.Put("a", "1");
  b.Delete("b");
  b.Put("c", "3");

  RecordingHandler h;
  ASSERT_TRUE(b.Iterate(&h).ok());
  ASSERT_EQ(h.ops.size(), 3u);
  EXPECT_EQ(h.ops[0], "PUT a=1@100");
  EXPECT_EQ(h.ops[1], "DEL b@101");
  EXPECT_EQ(h.ops[2], "PUT c=3@102");
}

TEST(WriteBatch, AppendMergesTwoBatches) {
  WriteBatch a, b;
  a.Put("x", "1");
  b.Put("y", "2");
  b.Delete("z");
  a.Append(b);
  EXPECT_EQ(a.Count(), 3u);
}

TEST(WriteBatch, InsertIntoMemTable) {
  WriteBatch b;
  b.SetSequence(1);
  b.Put("k1", "v1");
  b.Put("k2", "v2");
  b.Delete("k1");  // deletes k1 within the same batch (seq 1 then seq 2)

  MemTable mem;
  ASSERT_TRUE(b.InsertInto(&mem).ok());

  std::string value;
  Status s;
  ASSERT_TRUE(mem.Get("k1", 10, &value, &s));
  EXPECT_TRUE(s.IsNotFound());
  ASSERT_TRUE(mem.Get("k2", 10, &value, &s));
  EXPECT_EQ(value, "v2");
}

TEST(WriteBatch, SetContentsRejectsTooShort) {
  WriteBatch b;
  EXPECT_FALSE(b.SetContents("x").ok());
}

TEST(WriteBatch, ContentsRoundTripsThroughSetContents) {
  WriteBatch a;
  a.SetSequence(7);
  a.Put("foo", "bar");
  WriteBatch b;
  ASSERT_TRUE(b.SetContents(a.Contents()).ok());
  EXPECT_EQ(b.Count(), 1u);
  EXPECT_EQ(b.Sequence(), 7u);
}
