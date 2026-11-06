#include <gtest/gtest.h>

#include "storage/memtable.h"

using namespace kvdb;

TEST(MemTable, PutThenGet) {
  MemTable mem;
  mem.Add(1, kTypeValue, "foo", "bar");
  std::string value;
  Status s;
  ASSERT_TRUE(mem.Get("foo", 1, &value, &s));
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "bar");
}

TEST(MemTable, GetMissingKeyReturnsFalse) {
  MemTable mem;
  mem.Add(1, kTypeValue, "foo", "bar");
  std::string value;
  Status s;
  EXPECT_FALSE(mem.Get("nope", 1, &value, &s));
}

TEST(MemTable, DeleteShadowsOlderValue) {
  MemTable mem;
  mem.Add(1, kTypeValue, "foo", "bar");
  mem.Add(2, kTypeDeletion, "foo", "");
  std::string value;
  Status s;
  ASSERT_TRUE(mem.Get("foo", 2, &value, &s));
  EXPECT_TRUE(s.IsNotFound());
}

TEST(MemTable, SnapshotIsolation) {
  MemTable mem;
  mem.Add(1, kTypeValue, "foo", "v1");
  mem.Add(5, kTypeValue, "foo", "v5");
  mem.Add(10, kTypeValue, "foo", "v10");

  std::string value;
  Status s;
  ASSERT_TRUE(mem.Get("foo", 1, &value, &s));
  EXPECT_EQ(value, "v1");
  ASSERT_TRUE(mem.Get("foo", 5, &value, &s));
  EXPECT_EQ(value, "v5");
  ASSERT_TRUE(mem.Get("foo", 7, &value, &s));
  EXPECT_EQ(value, "v5");  // newest version <= snapshot
  ASSERT_TRUE(mem.Get("foo", 100, &value, &s));
  EXPECT_EQ(value, "v10");
}

TEST(MemTable, SnapshotBeforeFirstWriteMisses) {
  MemTable mem;
  mem.Add(10, kTypeValue, "foo", "v10");
  std::string value;
  Status s;
  EXPECT_FALSE(mem.Get("foo", 5, &value, &s));
}

TEST(MemTable, IteratorYieldsSortedInternalKeys) {
  MemTable mem;
  mem.Add(1, kTypeValue, "b", "2");
  mem.Add(2, kTypeValue, "a", "1");
  mem.Add(3, kTypeValue, "c", "3");

  auto it = mem.NewIterator();
  std::vector<std::string> user_keys;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    user_keys.push_back(std::string(ExtractUserKey(it->internal_key())));
  }
  ASSERT_EQ(user_keys.size(), 3u);
  EXPECT_EQ(user_keys[0], "a");
  EXPECT_EQ(user_keys[1], "b");
  EXPECT_EQ(user_keys[2], "c");
}

TEST(MemTable, MultipleVersionsOfSameKeyOrderedNewestFirst) {
  MemTable mem;
  mem.Add(1, kTypeValue, "k", "v1");
  mem.Add(2, kTypeValue, "k", "v2");

  auto it = mem.NewIterator();
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(ExtractSequence(it->internal_key()), 2u);
  it->Next();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(ExtractSequence(it->internal_key()), 1u);
}

TEST(MemTable, MemoryUsageGrows) {
  MemTable mem;
  size_t initial = mem.ApproximateMemoryUsage();
  for (int i = 0; i < 1000; i++) {
    mem.Add(static_cast<uint64_t>(i + 1), kTypeValue, "key" + std::to_string(i),
            std::string(100, 'x'));
  }
  EXPECT_GT(mem.ApproximateMemoryUsage(), initial);
  EXPECT_EQ(mem.NumEntries(), 1000u);
}
