#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "common/status.h"
#include "net/codec.h"

namespace kvdb {

// Synchronous request/response client over the framed protocol.
//
// Blocking rather than event-driven on purpose: each benchmark client thread
// and each Raft peer replicator owns its own connection, so there is nothing to
// multiplex, and a blocking socket gives far cleaner latency attribution than
// bouncing through a shared reactor.
class TcpClient {
 public:
  TcpClient() = default;
  ~TcpClient();
  TcpClient(const TcpClient&) = delete;
  TcpClient& operator=(const TcpClient&) = delete;

  // `address` is "host:port".
  Status Connect(const std::string& address, int timeout_ms = 3000);
  void Disconnect();
  bool connected() const { return fd_ >= 0; }
  const std::string& address() const { return address_; }

  // Sends one frame and waits for one response frame.
  Status Call(const std::string& request_frame, RequestHeader* resp_header,
              std::string* resp_body, int timeout_ms = 5000);

  // Typed convenience wrapper.
  template <typename Req, typename Resp>
  Status Call(MsgType req_type, const Req& req, Resp* resp, int timeout_ms = 5000) {
    const uint64_t id = ++next_request_id_;
    std::string frame = codec::BuildFrame(req_type, id, req);
    RequestHeader h;
    std::string body;
    KVDB_RETURN_IF_ERROR(Call(frame, &h, &body, timeout_ms));
    return codec::Decode(body, resp);
  }

  static Status ParseAddress(const std::string& address, std::string* host, uint16_t* port);

 private:
  Status ReadFully(char* dst, size_t n, int timeout_ms);
  Status SetTimeouts(int timeout_ms);

  int fd_ = -1;
  std::string address_;
  uint64_t next_request_id_ = 0;
  int current_timeout_ms_ = -1;
};

}  // namespace kvdb
