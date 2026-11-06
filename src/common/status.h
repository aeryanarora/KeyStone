// Copyright (c) 2026 distributed-kv authors. MIT License.
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace kvdb {

// A lightweight error carrier used across every layer of the system.
// Deliberately allocation-free on the (overwhelmingly common) OK path.
class Status {
 public:
  enum class Code : uint8_t {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kIOError = 3,
    kInvalidArgument = 4,
    kNotSupported = 5,
    kNotLeader = 6,
    kTimeout = 7,
    kUnavailable = 8,
    kAborted = 9,       // transaction conflict
    kNetworkError = 10,
    kShutdown = 11,
  };

  Status() = default;

  static Status OK() { return Status(); }
  static Status NotFound(std::string_view m = "") { return Status(Code::kNotFound, m); }
  static Status Corruption(std::string_view m = "") { return Status(Code::kCorruption, m); }
  static Status IOError(std::string_view m = "") { return Status(Code::kIOError, m); }
  static Status InvalidArgument(std::string_view m = "") { return Status(Code::kInvalidArgument, m); }
  static Status NotSupported(std::string_view m = "") { return Status(Code::kNotSupported, m); }
  static Status Timeout(std::string_view m = "") { return Status(Code::kTimeout, m); }
  static Status Unavailable(std::string_view m = "") { return Status(Code::kUnavailable, m); }
  static Status Aborted(std::string_view m = "") { return Status(Code::kAborted, m); }
  static Status NetworkError(std::string_view m = "") { return Status(Code::kNetworkError, m); }
  static Status Shutdown(std::string_view m = "") { return Status(Code::kShutdown, m); }

  // Carries an optional hint about which node the client should retry against.
  static Status NotLeader(std::string_view leader_hint = "") {
    Status s(Code::kNotLeader, "not leader");
    s.leader_hint_ = std::string(leader_hint);
    return s;
  }

  bool ok() const { return code_ == Code::kOk; }
  bool IsNotFound() const { return code_ == Code::kNotFound; }
  bool IsNotLeader() const { return code_ == Code::kNotLeader; }
  bool IsCorruption() const { return code_ == Code::kCorruption; }
  bool IsTimeout() const { return code_ == Code::kTimeout; }
  bool IsAborted() const { return code_ == Code::kAborted; }

  Code code() const { return code_; }
  const std::string& message() const { return msg_; }
  const std::string& leader_hint() const { return leader_hint_; }
  void set_leader_hint(std::string h) { leader_hint_ = std::move(h); }

  std::string ToString() const {
    std::string out(CodeName(code_));
    if (!msg_.empty()) {
      out += ": ";
      out += msg_;
    }
    if (!leader_hint_.empty()) {
      out += " (leader=" + leader_hint_ + ")";
    }
    return out;
  }

  static const char* CodeName(Code c) {
    switch (c) {
      case Code::kOk: return "OK";
      case Code::kNotFound: return "NotFound";
      case Code::kCorruption: return "Corruption";
      case Code::kIOError: return "IOError";
      case Code::kInvalidArgument: return "InvalidArgument";
      case Code::kNotSupported: return "NotSupported";
      case Code::kNotLeader: return "NotLeader";
      case Code::kTimeout: return "Timeout";
      case Code::kUnavailable: return "Unavailable";
      case Code::kAborted: return "Aborted";
      case Code::kNetworkError: return "NetworkError";
      case Code::kShutdown: return "Shutdown";
    }
    return "Unknown";
  }

 private:
  Status(Code c, std::string_view m) : code_(c), msg_(m) {}

  Code code_ = Code::kOk;
  std::string msg_;
  std::string leader_hint_;
};

#define KVDB_RETURN_IF_ERROR(expr)            \
  do {                                        \
    ::kvdb::Status _kvdb_s = (expr);          \
    if (!_kvdb_s.ok()) return _kvdb_s;        \
  } while (0)

}  // namespace kvdb
