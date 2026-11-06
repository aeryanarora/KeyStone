#include "net/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/logger.h"

namespace kvdb {

Status SetNonBlocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return Status::IOError(std::string("fcntl O_NONBLOCK: ") + std::strerror(errno));
  }
  return Status::OK();
}

Status SetTcpNoDelay(int fd) {
  int one = 1;
  // Disabling Nagle matters a lot here: request/response traffic is small and
  // synchronous, and a 40ms delayed-ACK interaction would dominate p99.
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
    return Status::IOError(std::string("TCP_NODELAY: ") + std::strerror(errno));
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// TcpConnection
// ---------------------------------------------------------------------------
TcpConnection::TcpConnection(int fd, EventLoop* loop, TcpServer* server, std::string peer)
    : fd_(fd), loop_(loop), server_(server), peer_(std::move(peer)) {}

TcpConnection::~TcpConnection() {
  if (fd_ >= 0) ::close(fd_);
}

void TcpConnection::HandleEvents(uint32_t events) {
  if (events & (EPOLLHUP | EPOLLERR)) {
    Close();
    return;
  }
  if (events & EPOLLIN) HandleRead();
  if (!closed() && (events & EPOLLOUT)) HandleWrite();
}

void TcpConnection::HandleRead() {
  char buf[64 * 1024];
  while (true) {
    ssize_t n = ::read(fd_, buf, sizeof(buf));
    if (n > 0) {
      in_buf_.append(buf, static_cast<size_t>(n));
      server_->bytes_in_.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);
      continue;
    }
    if (n == 0) {  // peer closed
      Close();
      return;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
    if (errno == EINTR) continue;
    Close();
    return;
  }

  // Drain every complete frame currently buffered.
  size_t consumed = 0;
  while (true) {
    std::string_view view(in_buf_.data() + consumed, in_buf_.size() - consumed);
    bool error = false;
    size_t frame_size = codec::PeekFrameSize(view, &error);
    if (error) {
      LOG_WARN("protocol error from %s, closing", peer_.c_str());
      Close();
      return;
    }
    if (frame_size == 0) break;  // need more bytes

    std::string_view payload(view.data() + codec::kFrameLenSize,
                             frame_size - codec::kFrameLenSize);
    RequestHeader header;
    std::string_view body;
    Status s = codec::DecodeHeader(payload, &header, &body);
    if (!s.ok()) {
      LOG_WARN("bad frame header from %s: %s", peer_.c_str(), s.ToString().c_str());
      Close();
      return;
    }
    server_->DispatchFrame(shared_from_this(), header, std::string(body));
    consumed += frame_size;
  }
  if (consumed > 0) in_buf_.erase(0, consumed);
}

void TcpConnection::Send(std::string frame) {
  if (closed()) return;
  {
    std::lock_guard<std::mutex> lk(out_mu_);
    out_buf_.append(frame);
  }
  // Writing must happen on the loop thread so epoll registration stays
  // single-threaded; hop over there.
  auto self = shared_from_this();
  loop_->RunInLoop([self] {
    if (!self->closed()) self->FlushOutput();
  });
}

void TcpConnection::FlushOutput() {
  std::lock_guard<std::mutex> lk(out_mu_);
  size_t written = 0;
  while (written < out_buf_.size()) {
    ssize_t n = ::write(fd_, out_buf_.data() + written, out_buf_.size() - written);
    if (n > 0) {
      written += static_cast<size_t>(n);
      server_->bytes_out_.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;  // socket buffer full
    if (n < 0 && errno == EINTR) continue;
    closed_.store(true, std::memory_order_release);
    return;
  }
  if (written > 0) out_buf_.erase(0, written);

  // Only arm EPOLLOUT while there is a backlog; leaving it armed would spin
  // the loop on a permanently writable socket.
  const bool need_write = !out_buf_.empty();
  if (need_write != want_write_) {
    want_write_ = need_write;
    loop_->UpdateFd(fd_, EPOLLIN | (need_write ? EPOLLOUT : 0u));
  }
}

void TcpConnection::HandleWrite() { FlushOutput(); }

void TcpConnection::Close() {
  bool expected = false;
  if (!closed_.compare_exchange_strong(expected, true)) return;
  auto self = shared_from_this();
  loop_->RunInLoop([self] {
    self->loop_->RemoveFd(self->fd_);
    self->server_->RemoveConnection(self);
  });
}

// ---------------------------------------------------------------------------
// TcpServer
// ---------------------------------------------------------------------------
TcpServer::TcpServer(std::string bind_addr, uint16_t port, size_t io_threads,
                     size_t worker_threads, size_t max_queue)
    : bind_addr_(std::move(bind_addr)), port_(port) {
  if (io_threads == 0) io_threads = 1;
  accept_loop_ = std::make_unique<EventLoop>();
  for (size_t i = 0; i < io_threads; i++) io_loops_.push_back(std::make_unique<EventLoop>());
  workers_ = std::make_unique<ThreadPool>(worker_threads, max_queue, "kvdb-rpc");
}

TcpServer::~TcpServer() { Stop(); }

Status TcpServer::Start() {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (listen_fd_ < 0) return Status::IOError(std::string("socket: ") + std::strerror(errno));

  int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_);
  if (bind_addr_.empty() || bind_addr_ == "0.0.0.0") {
    addr.sin_addr.s_addr = INADDR_ANY;
  } else if (::inet_pton(AF_INET, bind_addr_.c_str(), &addr.sin_addr) != 1) {
    return Status::InvalidArgument("bad bind address: " + bind_addr_);
  }
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    return Status::IOError("bind " + bind_addr_ + ":" + std::to_string(port_) + ": " +
                           std::strerror(errno));
  }
  if (::listen(listen_fd_, 1024) != 0) {
    return Status::IOError(std::string("listen: ") + std::strerror(errno));
  }
  // Resolve the actual port when the caller asked for an ephemeral one (port 0),
  // which is how tests avoid fixed-port collisions.
  if (port_ == 0) {
    sockaddr_in actual{};
    socklen_t len = sizeof(actual);
    if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&actual), &len) == 0) {
      port_ = ntohs(actual.sin_port);
    }
  }
  KVDB_RETURN_IF_ERROR(SetNonBlocking(listen_fd_));

  KVDB_RETURN_IF_ERROR(accept_loop_->Init());
  for (auto& loop : io_loops_) KVDB_RETURN_IF_ERROR(loop->Init());

  KVDB_RETURN_IF_ERROR(
      accept_loop_->AddFd(listen_fd_, EPOLLIN, [this](uint32_t) { HandleAccept(); }));

  running_.store(true, std::memory_order_release);
  for (size_t i = 0; i < io_loops_.size(); i++) {
    io_loops_[i]->StartThread("kvdb-io-" + std::to_string(i));
  }
  accept_loop_->StartThread("kvdb-accept");

  LOG_INFO("listening on %s:%u (%zu io threads, %zu workers)", bind_addr_.c_str(), port_,
           io_loops_.size(), workers_->num_threads());
  return Status::OK();
}

void TcpServer::HandleAccept() {
  while (true) {
    sockaddr_in peer{};
    socklen_t len = sizeof(peer);
    int fd = ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &len, SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;
      if (errno == EINTR) continue;
      LOG_WARN("accept failed: %s", std::strerror(errno));
      return;
    }
    SetNonBlocking(fd);
    SetTcpNoDelay(fd);

    char ip[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
    std::string peer_str = std::string(ip) + ":" + std::to_string(ntohs(peer.sin_port));

    // Round-robin across I/O loops keeps connection load even without any
    // cross-thread coordination.
    EventLoop* loop = io_loops_[next_loop_ % io_loops_.size()].get();
    next_loop_++;

    auto conn = std::make_shared<TcpConnection>(fd, loop, this, std::move(peer_str));
    {
      std::lock_guard<std::mutex> lk(conn_mu_);
      connections_[fd] = conn;
    }
    num_connections_.fetch_add(1, std::memory_order_relaxed);

    loop->RunInLoop([loop, conn] {
      loop->AddFd(conn->fd(), EPOLLIN, [conn](uint32_t ev) { conn->HandleEvents(ev); });
    });
  }
}

void TcpServer::RemoveConnection(const ConnectionPtr& conn) {
  std::lock_guard<std::mutex> lk(conn_mu_);
  if (connections_.erase(conn->fd()) > 0) {
    num_connections_.fetch_sub(1, std::memory_order_relaxed);
  }
}

void TcpServer::DispatchFrame(const ConnectionPtr& conn, RequestHeader header, std::string body) {
  requests_.fetch_add(1, std::memory_order_relaxed);
  if (!handler_) return;

  auto task = [this, conn, header, body = std::move(body)]() mutable {
    handler_(conn, header, body);
  };
  // Reject rather than queue without bound: a full queue means the server is
  // already saturated, and admitting more work only inflates tail latency.
  if (!workers_->Submit(std::move(task))) {
    WriteResponse resp;
    resp.status_code = static_cast<int32_t>(Status::Code::kUnavailable);
    resp.error = "server overloaded";
    conn->Send(codec::BuildFrame(MsgType::kPutResp, header.request_id, resp));
  }
}

void TcpServer::Stop() {
  if (!running_.exchange(false)) return;
  accept_loop_->Stop();
  accept_loop_->JoinThread();
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  {
    std::lock_guard<std::mutex> lk(conn_mu_);
    connections_.clear();
  }
  for (auto& loop : io_loops_) {
    loop->Stop();
    loop->JoinThread();
  }
  workers_->Shutdown();
}

}  // namespace kvdb
