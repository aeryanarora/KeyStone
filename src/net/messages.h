#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace kvdb {

// Every RPC travels over the same framed binary protocol:
//   | u32 frame_len | u8 msg_type | u64 request_id | body ... |
enum class MsgType : uint8_t {
  kInvalid = 0,

  kGetReq = 1,
  kGetResp = 2,
  kPutReq = 3,
  kPutResp = 4,
  kDeleteReq = 5,
  kDeleteResp = 6,
  kScanReq = 7,
  kScanResp = 8,
  kBatchReq = 9,
  kBatchResp = 10,
  kCasReq = 11,
  kCasResp = 12,
  kIncrReq = 13,
  kIncrResp = 14,
  kStatusReq = 15,
  kStatusResp = 16,

  // Primary-backup replication
  kReplicateReq = 32,
  kReplicateResp = 33,
  kHeartbeatReq = 34,
  kHeartbeatResp = 35,
  kCatchUpReq = 36,
  kCatchUpResp = 37,

  kAdminReq = 48,
  kAdminResp = 49,
};

// kLinearizable and kPrimaryLocal both require reading from the primary; the
// only difference from kStale is whether a non-primary redirects instead of
// answering from its own (possibly-lagging) copy.
enum class ReadConsistency : uint8_t {
  kLinearizable = 0,
  kPrimaryLocal = 1,
  kStale = 2,
};

struct RequestHeader {
  MsgType type = MsgType::kInvalid;
  uint64_t request_id = 0;
};

// ---- Client requests/responses ----
struct GetRequest {
  std::string key;
  ReadConsistency consistency = ReadConsistency::kLinearizable;
};

struct GetResponse {
  int32_t status_code = 0;
  std::string error;
  std::string primary_hint;
  bool found = false;
  std::string value;
};

struct PutRequest {
  std::string key;
  std::string value;
  bool sync = false;
};

struct DeleteRequest {
  std::string key;
  bool sync = false;
};

struct WriteResponse {
  int32_t status_code = 0;
  std::string error;
  std::string primary_hint;
  uint64_t seq = 0;
};

struct ScanRequest {
  std::string start_key;
  std::string end_key;
  uint32_t limit = 0;
  ReadConsistency consistency = ReadConsistency::kStale;
};

struct ScanResponse {
  int32_t status_code = 0;
  std::string error;
  std::string primary_hint;
  std::vector<std::pair<std::string, std::string>> entries;
};

struct BatchOp {
  bool is_delete = false;
  std::string key;
  std::string value;
};

struct BatchRequest {
  std::vector<BatchOp> ops;
  bool sync = false;
};

struct CasRequest {
  std::string key;
  std::string expected;
  std::string new_value;
  bool expect_absent = false;
  bool sync = false;
};

struct CasResponse {
  int32_t status_code = 0;
  std::string error;
  std::string primary_hint;
  bool swapped = false;
};

struct IncrRequest {
  std::string key;
  int64_t delta = 1;
  bool sync = false;
};

struct IncrResponse {
  int32_t status_code = 0;
  std::string error;
  std::string primary_hint;
  int64_t value = 0;
};

struct StatusRequest {
  bool verbose = false;
};

struct StatusResponse {
  int32_t status_code = 0;
  std::string error;
  uint64_t node_id = 0;
  std::string role;  // "primary" | "backup" | "standalone"
  uint64_t epoch = 0;
  uint64_t primary_id = 0;
  std::string primary_address;
  uint64_t last_seq = 0;
  std::string storage_stats;
  std::string repl_stats;
};

// ---- Replication RPCs ----
struct ReplicateReq {
  uint64_t epoch = 0;
  uint64_t primary_id = 0;
  uint64_t seq = 0;
  std::string data;
};

struct ReplicateResp {
  uint64_t epoch = 0;
  bool success = false;
  uint64_t backup_id = 0;
  uint64_t applied_seq = 0;
  uint64_t primary_id = 0;  // the backup's own view, so a stale primary learns who's current
};

struct HeartbeatReq {
  uint64_t epoch = 0;
  uint64_t primary_id = 0;
  uint64_t primary_last_seq = 0;
};

struct HeartbeatResp {
  uint64_t epoch = 0;
  bool ok = false;
  uint64_t backup_id = 0;
  uint64_t applied_seq = 0;
  uint64_t primary_id = 0;
};

struct CatchUpReq {
  uint64_t epoch = 0;
  uint64_t from_seq = 0;  // inclusive
};

struct CatchUpResp {
  uint64_t epoch = 0;
  bool ok = false;
  std::vector<std::pair<uint64_t, std::string>> entries;  // (seq, data)
};

// ---- Admin (fault injection / test control) ----
struct AdminRequest {
  std::string command;  // "crash", "pause", "resume", "compact", "flush", "stats"
  std::string arg;
};

struct AdminResponse {
  int32_t status_code = 0;
  std::string error;
  std::string result;
};

}  // namespace kvdb

