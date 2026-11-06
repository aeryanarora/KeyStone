#include <gtest/gtest.h>

#include <atomic>
#include <chrono>

#include "concurrency/thread_pool.h"

using namespace kvdb;

TEST(ThreadPool, RunsAllSubmittedTasks) {
  ThreadPool pool(4, 0, "test");
  std::atomic<int> counter{0};
  const int kN = 1000;
  for (int i = 0; i < kN; i++) {
    ASSERT_TRUE(pool.Submit([&counter] { counter.fetch_add(1); }));
  }
  pool.WaitIdle();
  EXPECT_EQ(counter.load(), kN);
  EXPECT_EQ(pool.tasks_completed(), static_cast<uint64_t>(kN));
}

TEST(ThreadPool, BoundedQueueRejectsWhenFull) {
  ThreadPool pool(1, 2, "test");
  std::atomic<bool> release{false};
  // Occupy the single worker so the queue actually backs up.
  pool.Submit([&release] {
    while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  bool ok1 = pool.Submit([] {});
  bool ok2 = pool.Submit([] {});
  bool ok3 = pool.Submit([] {});  // queue capacity is 2; this should be rejected
  EXPECT_TRUE(ok1);
  EXPECT_TRUE(ok2);
  EXPECT_FALSE(ok3);
  EXPECT_GT(pool.tasks_rejected(), 0u);

  release.store(true);
  pool.WaitIdle();
}

TEST(ThreadPool, SubmitWithResultReturnsValue) {
  ThreadPool pool(2, 0, "test");
  auto fut = pool.SubmitWithResult([] { return 21 * 2; });
  EXPECT_EQ(fut.get(), 42);
}

TEST(ThreadPool, ShutdownStopsAcceptingWork) {
  ThreadPool pool(2, 0, "test");
  pool.Shutdown();
  EXPECT_FALSE(pool.Submit([] {}));
}

TEST(ThreadPool, ConcurrentSubmittersAllComplete) {
  ThreadPool pool(8, 0, "test");
  std::atomic<int> counter{0};
  std::vector<std::thread> submitters;
  for (int t = 0; t < 8; t++) {
    submitters.emplace_back([&pool, &counter] {
      for (int i = 0; i < 200; i++) {
        pool.SubmitBlocking([&counter] { counter.fetch_add(1); });
      }
    });
  }
  for (auto& t : submitters) t.join();
  pool.WaitIdle();
  EXPECT_EQ(counter.load(), 8 * 200);
}
