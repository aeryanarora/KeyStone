#include "net/event_loop.h"

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/logger.h"

namespace kvdb {

EventLoop::EventLoop() = default;

EventLoop::~EventLoop() {
  Stop();
  JoinThread();
  if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
  if (epoll_fd_ >= 0) ::close(epoll_fd_);
}

Status EventLoop::Init() {
  epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) return Status::IOError(std::string("epoll_create1: ") + std::strerror(errno));

  wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakeup_fd_ < 0) return Status::IOError(std::string("eventfd: ") + std::strerror(errno));

  return AddFd(wakeup_fd_, EPOLLIN, [this](uint32_t) { DrainWakeup(); });
}

Status EventLoop::AddFd(int fd, uint32_t events, EventCallback cb) {
  epoll_event ev{};
  ev.events = events;
  ev.data.fd = fd;
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
    return Status::IOError(std::string("epoll_ctl ADD: ") + std::strerror(errno));
  }
  handlers_[fd] = std::move(cb);
  return Status::OK();
}

Status EventLoop::UpdateFd(int fd, uint32_t events) {
  epoll_event ev{};
  ev.events = events;
  ev.data.fd = fd;
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
    return Status::IOError(std::string("epoll_ctl MOD: ") + std::strerror(errno));
  }
  return Status::OK();
}

Status EventLoop::RemoveFd(int fd) {
  ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
  handlers_.erase(fd);
  return Status::OK();
}

void EventLoop::RunInLoop(std::function<void()> fn) {
  if (InLoopThread() && running_.load(std::memory_order_acquire)) {
    fn();
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    pending_.push_back(std::move(fn));
  }
  Wakeup();
}

void EventLoop::Wakeup() {
  uint64_t one = 1;
  ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
  (void)n;
}

void EventLoop::DrainWakeup() {
  uint64_t buf = 0;
  while (::read(wakeup_fd_, &buf, sizeof(buf)) > 0) {
  }
}

void EventLoop::RunPendingFunctors() {
  std::vector<std::function<void()>> fns;
  {
    std::lock_guard<std::mutex> lk(mu_);
    fns.swap(pending_);
  }
  for (auto& fn : fns) fn();
}

void EventLoop::Loop() {
  thread_id_ = std::this_thread::get_id();
  running_.store(true, std::memory_order_release);

  std::vector<epoll_event> events(256);
  while (running_.load(std::memory_order_acquire)) {
    int n = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), 50);
    if (n < 0) {
      if (errno == EINTR) continue;
      LOG_ERROR("epoll_wait failed: %s", std::strerror(errno));
      break;
    }
    iterations_.fetch_add(1, std::memory_order_relaxed);

    for (int i = 0; i < n; i++) {
      const int fd = events[static_cast<size_t>(i)].data.fd;
      auto it = handlers_.find(fd);
      // The handler may close and unregister other descriptors, so re-check
      // rather than caching the iterator across the callback.
      if (it != handlers_.end()) {
        EventCallback cb = it->second;
        cb(events[static_cast<size_t>(i)].events);
      }
    }
    RunPendingFunctors();

    // Grow the event array if we consistently fill it (many active clients).
    if (n == static_cast<int>(events.size()) && events.size() < 4096) {
      events.resize(events.size() * 2);
    }
  }
  running_.store(false, std::memory_order_release);
  RunPendingFunctors();
}

void EventLoop::Stop() {
  if (!running_.load(std::memory_order_acquire)) return;
  running_.store(false, std::memory_order_release);
  Wakeup();
}

void EventLoop::StartThread(const std::string& name) {
  thread_ = std::thread([this, name] {
    pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
    Loop();
  });
  // Wait until the loop is actually running so RunInLoop() from another thread
  // can't be lost between construction and Loop().
  while (!running_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

void EventLoop::JoinThread() {
  if (thread_.joinable()) thread_.join();
}

}  // namespace kvdb
