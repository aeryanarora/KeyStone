#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "concurrency/thread_pool.h"
#include "net/codec.h"
#include "net/event_loop.h"

namespace kvdb {

class TcpServer;

// One client connection. Reads are driven by the owning EventLoop; writes may
// be posted from any thread (a worker finishing an RPC), which is why the
// outgoing buffer is mutex-protected and the loop is woken explicitly.
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
 public:
  TcpConnection(int fd, EventLoop* loop, TcpServer* server, std::string peer);
  ~TcpConnection();

  // Thread-safe. Queues a complete frame for delivery.
  void Send(std::string frame);
  void Close();

  int fd() const { return fd_; }
  const std::string& peer() const { return peer_; }
  bool closed() const { return closed_.load(std::memory_order_acquire); }

 private:
  friend class TcpServer;

  void HandleEvents(uint32_t events);
  void HandleRead();
  void HandleWrite();
  void FlushOutput();  // loop thread only

  int fd_;
  EventLoop* loop_;
  TcpServer* server_;
  std::string peer_;

  std::string in_buf_;   // loop thread only
  std::mutex out_mu_;
  std::string out_buf_;
  bool want_write_ = false;
  std::atomic<bool> closed_{false};
};

using ConnectionPtr = std::shared_ptr<TcpConnection>;

// Multi-reactor TCP server: one acceptor loop plus N I/O loops, with request
// execution handed to a thread pool so that slow disk work (an fsync in the
// write path) never blocks the event loops.
//
//   accept loop -> io loop (round robin) -> parse frame -> thread pool -> handler
//                                                                   |
//                                             conn->Send() <--------+
class TcpServer {
 public:
  // Invoked on a thread-pool worker. The handler owns responding via conn->Send().
  using MessageHandler =
      std::function<void(const ConnectionPtr& conn, const RequestHeader& header,
                         std::string_view body)>;

  TcpServer(std::string bind_addr, uint16_t port, size_t io_threads, size_t worker_threads,
            size_t max_queue = 4096);
  ~TcpServer();

  void SetMessageHandler(MessageHandler h) { handler_ = std::move(h); }

  Status Start();
  void Stop();

  uint16_t port() const { return port_; }
  size_t num_connections() const { return num_connections_.load(std::memory_order_relaxed); }
  uint64_t requests_received() const { return requests_.load(std::memory_order_relaxed); }
  uint64_t bytes_in() const { return bytes_in_.load(std::memory_order_relaxed); }
  uint64_t bytes_out() const { return bytes_out_.load(std::memory_order_relaxed); }
  ThreadPool* worker_pool() { return workers_.get(); }

 private:
  friend class TcpConnection;

  void HandleAccept();
  void RemoveConnection(const ConnectionPtr& conn);
  void DispatchFrame(const ConnectionPtr& conn, RequestHeader header, std::string body);

  std::string bind_addr_;
  uint16_t port_;
  int listen_fd_ = -1;

  std::unique_ptr<EventLoop> accept_loop_;
  std::vector<std::unique_ptr<EventLoop>> io_loops_;
  std::unique_ptr<ThreadPool> workers_;
  size_t next_loop_ = 0;

  std::mutex conn_mu_;
  std::unordered_map<int, ConnectionPtr> connections_;

  MessageHandler handler_;
  std::atomic<bool> running_{false};
  std::atomic<size_t> num_connections_{0};
  std::atomic<uint64_t> requests_{0};
  std::atomic<uint64_t> bytes_in_{0};
  std::atomic<uint64_t> bytes_out_{0};
};

// Helpers shared with the client.
Status SetNonBlocking(int fd);
Status SetTcpNoDelay(int fd);

}  // namespace kvdb
