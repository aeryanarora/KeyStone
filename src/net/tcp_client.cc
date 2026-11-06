#include "net/tcp_client.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "common/coding.h"
#include "common/logger.h"
#include "net/tcp_server.h"  // SetTcpNoDelay

namespace kvdb {

TcpClient::~TcpClient() { Disconnect(); }

Status TcpClient::ParseAddress(const std::string& address, std::string* host, uint16_t* port) {
  const size_t colon = address.rfind(':');
  if (colon == std::string::npos) return Status::InvalidArgument("address must be host:port");
  *host = address.substr(0, colon);
  const int p = std::atoi(address.c_str() + colon + 1);
  if (p <= 0 || p > 65535) return Status::InvalidArgument("bad port in " + address);
  *port = static_cast<uint16_t>(p);
  if (host->empty()) *host = "127.0.0.1";
  return Status::OK();
}

Status TcpClient::SetTimeouts(int timeout_ms) {
  if (timeout_ms == current_timeout_ms_) return Status::OK();
  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  current_timeout_ms_ = timeout_ms;
  return Status::OK();
}

Status TcpClient::Connect(const std::string& address, int timeout_ms) {
  Disconnect();

  std::string host;
  uint16_t port = 0;
  KVDB_RETURN_IF_ERROR(ParseAddress(address, &host, &port));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    // Fall back to DNS for hostnames (docker-compose service names).
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) {
      return Status::NetworkError("cannot resolve host: " + host);
    }
    addr.sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
    ::freeaddrinfo(res);
  }

  fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd_ < 0) return Status::NetworkError(std::string("socket: ") + std::strerror(errno));

  current_timeout_ms_ = -1;
  SetTimeouts(timeout_ms);

  if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    Status s = Status::NetworkError("connect " + address + ": " + std::strerror(errno));
    ::close(fd_);
    fd_ = -1;
    return s;
  }
  SetTcpNoDelay(fd_);
  address_ = address;
  return Status::OK();
}

void TcpClient::Disconnect() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

Status TcpClient::ReadFully(char* dst, size_t n, int timeout_ms) {
  size_t done = 0;
  while (done < n) {
    ssize_t r = ::read(fd_, dst + done, n - done);
    if (r > 0) {
      done += static_cast<size_t>(r);
      continue;
    }
    if (r == 0) return Status::NetworkError("connection closed by peer");
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return Status::Timeout("read timed out after " + std::to_string(timeout_ms) + "ms");
    }
    return Status::NetworkError(std::string("read: ") + std::strerror(errno));
  }
  return Status::OK();
}

Status TcpClient::Call(const std::string& request_frame, RequestHeader* resp_header,
                       std::string* resp_body, int timeout_ms) {
  if (fd_ < 0) return Status::NetworkError("not connected");
  SetTimeouts(timeout_ms);

  size_t written = 0;
  while (written < request_frame.size()) {
    ssize_t n = ::write(fd_, request_frame.data() + written, request_frame.size() - written);
    if (n > 0) {
      written += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return Status::Timeout("write timeout");
    return Status::NetworkError(std::string("write: ") + std::strerror(errno));
  }

  char len_buf[codec::kFrameLenSize];
  KVDB_RETURN_IF_ERROR(ReadFully(len_buf, sizeof(len_buf), timeout_ms));
  const uint32_t payload_len = DecodeFixed32(len_buf);
  if (payload_len < 9 || payload_len > codec::kMaxFrameSize) {
    return Status::Corruption("bad response frame length");
  }

  std::string payload;
  payload.resize(payload_len);
  KVDB_RETURN_IF_ERROR(ReadFully(payload.data(), payload_len, timeout_ms));

  std::string_view rest;
  KVDB_RETURN_IF_ERROR(codec::DecodeHeader(payload, resp_header, &rest));
  resp_body->assign(rest.data(), rest.size());
  return Status::OK();
}

}  // namespace kvdb
