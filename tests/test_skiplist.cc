#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <thread>

#include "common/arena.h"
#include "common/env.h"
#include "storage/skiplist.h"

using namespace kvdb;

namespace {
struct IntComparator {
  int operator()(const int64_t* a, const int64_t* b) const {
    return (*a < *b) ? -1 : (*a > *b) ? 1 : 0;
  }
};
using IntSkipList = SkipList<const int64_t*, IntComparator>;

const int64_t* MakeKey(Arena* arena, int64_t v) {
  auto* p = reinterpret_cast<int64_t*>(arena->AllocateAligned(sizeof(int64_t)));
  *p = v;
  return p;
}
}  // namespace

TEST(SkipList, EmptyListInvalidIterator) {
  Arena arena;
  IntSkipList list(IntComparator(), &arena);
  IntSkipList::Iterator it(&list);
  it.SeekToFirst();
  EXPECT_FALSE(it.Valid());
}

TEST(SkipList, InsertAndIterateSorted) {
  Arena arena;
  IntSkipList list(IntComparator(), &arena);
  std::vector<int64_t> values = {5, 1, 9, 3, 7, 2, 8, 4, 6, 0};
  for (auto v : values) list.Insert(MakeKey(&arena, v));

  std::vector<int64_t> seen;
  IntSkipList::Iterator it(&list);
  for (it.SeekToFirst(); it.Valid(); it.Next()) seen.push_back(*it.key());

  std::vector<int64_t> expected = values;
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(seen, expected);
}

TEST(SkipList, SeekFindsGreaterOrEqual) {
  Arena arena;
  IntSkipList list(IntComparator(), &arena);
  for (int64_t v : {10, 20, 30, 40, 50}) list.Insert(MakeKey(&arena, v));

  IntSkipList::Iterator it(&list);
  it.Seek(MakeKey(&arena, 25));
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(*it.key(), 30);

  it.Seek(MakeKey(&arena, 30));
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(*it.key(), 30);

  it.Seek(MakeKey(&arena, 1000));
  EXPECT_FALSE(it.Valid());
}

TEST(SkipList, ContainsWorks) {
  Arena arena;
  IntSkipList list(IntComparator(), &arena);
  for (int64_t v : {1, 2, 3}) list.Insert(MakeKey(&arena, v));
  int64_t two = 2, five = 5;
  EXPECT_TRUE(list.Contains(&two));
  EXPECT_FALSE(list.Contains(&five));
}

TEST(SkipList, ManyInsertsPreserveOrderAndCount) {
  Arena arena;
  IntSkipList list(IntComparator(), &arena);
  const int kN = 10000;
  Random rnd(42);
  std::set<int64_t> unique_values;
  while (static_cast<int>(unique_values.size()) < kN) {
    unique_values.insert(static_cast<int64_t>(rnd.Uniform(1000000)));
  }
  for (auto v : unique_values) list.Insert(MakeKey(&arena, v));
  EXPECT_EQ(list.ApproximateNumEntries(), unique_values.size());

  IntSkipList::Iterator it(&list);
  auto expected_it = unique_values.begin();
  size_t count = 0;
  for (it.SeekToFirst(); it.Valid(); it.Next(), ++expected_it, ++count) {
    ASSERT_NE(expected_it, unique_values.end());
    EXPECT_EQ(*it.key(), *expected_it);
  }
  EXPECT_EQ(count, unique_values.size());
}

// Concurrent-read safety: one writer inserts strictly increasing keys while
// several readers iterate concurrently. Because keys only increase, a reader
// that observes a node is guaranteed to see a fully-published, valid one --
// this is exactly the contract SkipList promises.
TEST(SkipList, ConcurrentReadsWhileWriting) {
  Arena arena;
  IntSkipList list(IntComparator(), &arena);
  std::atomic<int64_t> next_to_insert{0};
  std::atomic<bool> stop{false};
  const int64_t kTotal = 20000;

  std::thread writer([&] {
    for (int64_t i = 0; i < kTotal; i++) {
      list.Insert(MakeKey(&arena, i));
      next_to_insert.store(i, std::memory_order_release);
    }
    stop.store(true, std::memory_order_release);
  });

  std::vector<std::thread> readers;
  std::atomic<bool> saw_out_of_order{false};
  for (int r = 0; r < 4; r++) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        IntSkipList::Iterator it(&list);
        int64_t prev = -1;
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
          if (*it.key() <= prev) saw_out_of_order.store(true);
          prev = *it.key();
        }
      }
    });
  }
  writer.join();
  for (auto& t : readers) t.join();
  EXPECT_FALSE(saw_out_of_order.load());
  EXPECT_EQ(list.ApproximateNumEntries(), static_cast<size_t>(kTotal));
}
