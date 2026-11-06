#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"

namespace kvdb {

// A single-threaded epoll reactor. One EventLoop owns a set of file
// descriptors and is the only thread allowed to touch them; anything else that
// wants to act on a descriptor posts a closure via RunInLoop().
//
// This is the alternative to thread-per-connection: with 64+ concurrent
// benchmark clients, a handful of loop threads keeps context switching and
// stack memory flat instead of scaling linearly with connection count.
class EventLoop {
 public:
  using EventCallback = std::function<void(uint32_t events)>;

  EventLoop();
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  Status Init();

  Status AddFd(int fd, uint32_t events, EventCallback cb);
  Status UpdateFd(int fd, uint32_t events);
  Status RemoveFd(int fd);

  // Runs `fn` on the loop thread. Executes inline if already on that thread,
  // otherwise queues it and wakes the loop through an eventfd.
  void RunInLoop(std::function<void()> fn);

  void Loop();   // blocks until Stop()
  void Stop();

  // Starts Loop() on its own thread.
  void StartThread(const std::string& name);
  void JoinThread();

  bool InLoopThread() const { return std::this_thread::get_id() == thread_id_; }
  uint64_t iterations() const { return iterations_.load(std::memory_order_relaxed); }

 private:
  void Wakeup();
  void DrainWakeup();
  void RunPendingFunctors();

  int epoll_fd_ = -1;
  int wakeup_fd_ = -1;
  std::atomic<bool> running_{false};
  std::thread thread_;
  std::thread::id thread_id_;

  std::mutex mu_;
  std::vector<std::function<void()>> pending_;
  std::unordered_map<int, EventCallback> handlers_;  // loop-thread only
  std::atomic<uint64_t> iterations_{0};
};

}  // namespace kvdb
