#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "concurrency/transaction.h"
#include "storage/db.h"
#include "test_util.h"

using namespace kvdb;

namespace {
Options TestOptions(const std::string& dir) {
  Options o;
  o.data_dir = dir;
  o.background_compaction = false;
  return o;
}
}  // namespace

TEST(Transaction, ReadYourOwnWrites) {
  testutil::TempDir dir("txn_ryow");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  auto txn = mgr.Begin();
  txn->Put("k", "v1");
  std::string value;
  ASSERT_TRUE(txn->Get("k", &value).ok());
  EXPECT_EQ(value, "v1");
  ASSERT_TRUE(txn->Commit().ok());
}

TEST(Transaction, CommitIsVisibleAfterwards) {
  testutil::TempDir dir("txn_commit_visible");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  auto txn = mgr.Begin();
  txn->Put("a", "1");
  txn->Put("b", "2");
  ASSERT_TRUE(txn->Commit().ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "a", &value).ok());
  EXPECT_EQ(value, "1");
  ASSERT_TRUE(db->Get(ReadOptions(), "b", &value).ok());
  EXPECT_EQ(value, "2");
}

TEST(Transaction, RollbackDiscardsWrites) {
  testutil::TempDir dir("txn_rollback");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  auto txn = mgr.Begin();
  txn->Put("k", "v1");
  txn->Rollback();

  std::string value;
  EXPECT_TRUE(db->Get(ReadOptions(), "k", &value).IsNotFound());
}

TEST(Transaction, ConcurrentWriteConflictAborts) {
  testutil::TempDir dir("txn_conflict");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "shared", "initial").ok());

  auto txn1 = mgr.Begin();
  std::string v;
  ASSERT_TRUE(txn1->Get("shared", &v).ok());  // establishes read-set version

  // A concurrent, independent writer commits first.
  auto txn2 = mgr.Begin();
  txn2->Get("shared", &v);
  txn2->Put("shared", "from-txn2");
  ASSERT_TRUE(txn2->Commit().ok());

  // txn1's validation must now see the key has moved on and abort.
  txn1->Put("shared", "from-txn1");
  Status s = txn1->Commit();
  EXPECT_TRUE(s.IsAborted());

  std::string final_value;
  ASSERT_TRUE(db->Get(ReadOptions(), "shared", &final_value).ok());
  EXPECT_EQ(final_value, "from-txn2");
}

TEST(Transaction, CompareAndSwapSucceedsWhenExpectedMatches) {
  testutil::TempDir dir("txn_cas_ok");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "k", "old").ok());
  bool swapped = false;
  ASSERT_TRUE(mgr.CompareAndSwap("k", "old", "new", false, &swapped).ok());
  EXPECT_TRUE(swapped);
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "k", &value).ok());
  EXPECT_EQ(value, "new");
}

TEST(Transaction, CompareAndSwapFailsWhenExpectedMismatches) {
  testutil::TempDir dir("txn_cas_fail");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "k", "actual").ok());
  bool swapped = true;
  ASSERT_TRUE(mgr.CompareAndSwap("k", "wrong-expectation", "new", false, &swapped).ok());
  EXPECT_FALSE(swapped);
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "k", &value).ok());
  EXPECT_EQ(value, "actual");
}

TEST(Transaction, CompareAndSwapExpectAbsent) {
  testutil::TempDir dir("txn_cas_absent");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  bool swapped = false;
  ASSERT_TRUE(mgr.CompareAndSwap("new-key", "", "first-value", true, &swapped).ok());
  EXPECT_TRUE(swapped);

  swapped = false;
  ASSERT_TRUE(mgr.CompareAndSwap("new-key", "", "second-value", true, &swapped).ok());
  EXPECT_FALSE(swapped);  // key now exists, expect_absent precondition fails
}

TEST(Transaction, IncrementAccumulatesAcrossManyThreads) {
  testutil::TempDir dir("txn_incr_concurrent");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  const int kThreads = 8;
  const int kPerThread = 100;
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&mgr] {
      for (int i = 0; i < kPerThread; i++) {
        int64_t result = 0;
        Status s = mgr.Increment("counter", 1, &result);
        ASSERT_TRUE(s.ok());
      }
    });
  }
  for (auto& t : threads) t.join();

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions(), "counter", &value).ok());
  EXPECT_EQ(std::stoll(value), kThreads * kPerThread);
}

TEST(Transaction, ReadOnlyTransactionNeverAborts) {
  testutil::TempDir dir("txn_readonly");
  std::unique_ptr<LsmStore> db;
  ASSERT_TRUE(LsmStore::Open(TestOptions(dir.path()), &db).ok());
  TransactionManager mgr(db.get());

  WriteOptions wo;
  ASSERT_TRUE(db->Put(wo, "k", "v").ok());

  auto txn = mgr.Begin();
  std::string v;
  txn->Get("k", &v);
  // No writes were staged; committing a read-only transaction is trivially valid
  // even if the key changes concurrently.
  ASSERT_TRUE(db->Put(wo, "k", "changed").ok());
  EXPECT_TRUE(txn->Commit().ok());
}
