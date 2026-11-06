#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace kvdb {

// Fixed-size worker pool used to execute client RPCs off the I/O threads.
//
// Design notes:
//  * A bounded queue (max_queue_size) provides backpressure: without it a burst
//    of clients turns into unbounded memory growth and multi-second latencies.
//  * Workers block on a condition variable rather than spinning, so an idle
//    server costs no CPU -- which matters when the benchmark measures CPU usage.
class ThreadPool {
 public:
  // max_queue_size == 0 means unbounded.
  explicit ThreadPool(size_t num_threads = 0, size_t max_queue_size = 0,
                      std::string name = "pool");
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  // Returns false if the queue is full or the pool is shutting down.
  bool Submit(std::function<void()> task);

  // Blocking variant: waits for queue space instead of rejecting.
  bool SubmitBlocking(std::function<void()> task);

  template <typename F>
  auto SubmitWithResult(F&& f) -> std::future<decltype(f())> {
    using R = decltype(f());
    auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));
    std::future<R> fut = task->get_future();
    SubmitBlocking([task]() { (*task)(); });
    return fut;
  }

  // Waits until every queued and in-flight task has finished.
  void WaitIdle();
  void Shutdown();

  size_t num_threads() const { return workers_.size(); }
  size_t QueueDepth() const;
  uint64_t tasks_completed() const { return completed_.load(std::memory_order_relaxed); }
  uint64_t tasks_rejected() const { return rejected_.load(std::memory_order_relaxed); }
  uint64_t peak_queue_depth() const { return peak_depth_.load(std::memory_order_relaxed); }

 private:
  void WorkerLoop(size_t index);

  mutable std::mutex mu_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::condition_variable idle_cv_;
  std::queue<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  size_t max_queue_size_;
  size_t active_ = 0;
  bool shutdown_ = false;
  std::string name_;

  std::atomic<uint64_t> completed_{0};
  std::atomic<uint64_t> rejected_{0};
  std::atomic<uint64_t> peak_depth_{0};
};

}  // namespace kvdb
