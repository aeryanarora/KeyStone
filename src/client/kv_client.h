#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "net/tcp_client.h"

namespace kvdb {

// High-level client: knows about cluster membership and automatically
// retries against the current leader when a request comes back NotLeader,
// which is the common case right after a failover.
class KvClient {
 public:
  KvClient() = default;

  // `endpoints` may be a single "host:port" or a comma-separated list for a
  // cluster; the client discovers the leader by asking around.
  Status Connect(const std::string& endpoints);

  Status Get(std::string_view key, std::string* value,
            ReadConsistency c = ReadConsistency::kLinearizable);
  Status Put(std::string_view key, std::string_view value, bool sync = false);
  Status Delete(std::string_view key, bool sync = false);
  Status Scan(std::string_view start, std::string_view end, size_t limit,
             std::vector<std::pair<std::string, std::string>>* out,
             ReadConsistency c = ReadConsistency::kStale);
  Status Batch(const std::vector<BatchOp>& ops, bool sync = false);
  Status Cas(std::string_view key, std::string_view expected, std::string_view new_value,
            bool expect_absent, bool* swapped, bool sync = false);
  Status Incr(std::string_view key, int64_t delta, int64_t* result, bool sync = false);
  Status GetStatus(StatusResponse* out, bool verbose = false);
  Status Admin(const std::string& command, const std::string& arg, AdminResponse* out);

  const std::string& current_leader_hint() const { return leader_hint_; }

 private:
  template <typename Req, typename Resp>
  Status CallWithRetry(MsgType type, const Req& req, Resp* resp, int max_retries = 3);

  std::vector<std::string> endpoints_;
  std::string leader_hint_;  // last known leader address, tried first
  std::unique_ptr<TcpClient> client_;
  std::string connected_to_;
};

}  // namespace kvdb
