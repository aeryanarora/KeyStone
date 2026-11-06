#include "concurrency/thread_pool.h"

#include <pthread.h>

#include "common/logger.h"

namespace kvdb {

ThreadPool::ThreadPool(size_t num_threads, size_t max_queue_size, std::string name)
    : max_queue_size_(max_queue_size), name_(std::move(name)) {
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;
  }
  workers_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    workers_.emplace_back([this, i] { WorkerLoop(i); });
  }
  LOG_DEBUG("thread pool '%s' started with %zu workers", name_.c_str(), num_threads);
}

ThreadPool::~ThreadPool() { Shutdown(); }

void ThreadPool::WorkerLoop(size_t index) {
  // Named threads make `perf`, `top -H` and gdb output readable.
  std::string tname = name_ + "-" + std::to_string(index);
  pthread_setname_np(pthread_self(), tname.substr(0, 15).c_str());

  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lk(mu_);
      not_empty_.wait(lk, [this] { return shutdown_ || !queue_.empty(); });
      if (shutdown_ && queue_.empty()) return;
      task = std::move(queue_.front());
      queue_.pop();
      active_++;
      not_full_.notify_one();
    }

    task();

    {
      std::lock_guard<std::mutex> lk(mu_);
      active_--;
      completed_.fetch_add(1, std::memory_order_relaxed);
      if (queue_.empty() && active_ == 0) idle_cv_.notify_all();
    }
  }
}

bool ThreadPool::Submit(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (shutdown_) return false;
    if (max_queue_size_ > 0 && queue_.size() >= max_queue_size_) {
      rejected_.fetch_add(1, std::memory_order_relaxed);
      return false;  // backpressure: caller should shed load, not buffer it
    }
    queue_.push(std::move(task));
    auto depth = static_cast<uint64_t>(queue_.size());
    uint64_t prev = peak_depth_.load(std::memory_order_relaxed);
    while (depth > prev &&
           !peak_depth_.compare_exchange_weak(prev, depth, std::memory_order_relaxed)) {
    }
  }
  not_empty_.notify_one();
  return true;
}

bool ThreadPool::SubmitBlocking(std::function<void()> task) {
  {
    std::unique_lock<std::mutex> lk(mu_);
    if (max_queue_size_ > 0) {
      not_full_.wait(lk, [this] { return shutdown_ || queue_.size() < max_queue_size_; });
    }
    if (shutdown_) return false;
    queue_.push(std::move(task));
  }
  not_empty_.notify_one();
  return true;
}

void ThreadPool::WaitIdle() {
  std::unique_lock<std::mutex> lk(mu_);
  idle_cv_.wait(lk, [this] { return queue_.empty() && active_ == 0; });
}

void ThreadPool::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (shutdown_) return;
    shutdown_ = true;
  }
  not_empty_.notify_all();
  not_full_.notify_all();
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
  workers_.clear();
}

size_t ThreadPool::QueueDepth() const {
  std::lock_guard<std::mutex> lk(mu_);
  return queue_.size();
}

}  // namespace kvdb
