#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "concurrency/transaction.h"
#include "net/tcp_server.h"
#include "replication/replication_node.h"
#include "storage/db.h"

namespace kvdb {

struct ServerConfig {
  uint64_t node_id = 1;
  std::string bind_addr = "0.0.0.0";
  uint16_t port = 7000;
  std::map<uint64_t, std::string> peers;  // includes self; empty/size-1 => standalone mode

  Options storage;
  ReplicationConfig repl;

  size_t io_threads = 2;
  size_t worker_threads = 8;
  size_t max_queue = 8192;

  // Standalone mode: serves the LSM store directly, no replication.
  bool standalone = false;
};

// Wires together storage, (optional) replication, and the network layer into
// one running node.
class KvServer {
 public:
  explicit KvServer(ServerConfig config);
  ~KvServer();

  Status Start();
  void Stop();

  bool running() const { return running_.load(std::memory_order_acquire); }
  uint16_t port() const { return tcp_server_ ? tcp_server_->port() : config_.port; }
  LsmStore* store() { return store_.get(); }
  ReplicationNode* repl() { return repl_.get(); }
  const ServerConfig& config() const { return config_; }

  // ---- direct (in-process) API, used by tests and the standalone CLI ----
  Status Get(std::string_view key, std::string* value, ReadConsistency c = ReadConsistency::kLinearizable);
  Status Put(std::string_view key, std::string_view value, bool sync = false);
  Status Delete(std::string_view key, bool sync = false);
  Status Batch(const std::vector<BatchOp>& ops, bool sync = false);
  Status Scan(std::string_view start, std::string_view end, size_t limit,
             std::vector<std::pair<std::string, std::string>>* out);
  Status Cas(std::string_view key, std::string_view expected, std::string_view new_value,
            bool expect_absent, bool* swapped, bool sync = false);
  Status Incr(std::string_view key, int64_t delta, int64_t* result, bool sync = false);

  StatusResponse GetStatus(bool verbose) const;

  // ---- fault injection / admin ----
  Status HandleAdmin(const AdminRequest& req, AdminResponse* resp);

 private:
  void OnMessage(const ConnectionPtr& conn, const RequestHeader& header, std::string_view body);
  Status ReplicateBatch(WriteBatch* batch);
  Status EnsureReadable(ReadConsistency c);
  std::string SelfPrimaryHint() const;

  ServerConfig config_;
  std::unique_ptr<LsmStore> store_;
  std::unique_ptr<ReplicationNode> repl_;
  std::unique_ptr<TransactionManager> txn_mgr_;
  std::unique_ptr<TcpServer> tcp_server_;
  std::mutex write_mu_;  // serializes all primary-side writes; see ReplicateBatch/Cas/Incr

  std::atomic<bool> running_{false};
  std::atomic<bool> paused_{false};
};

}  // namespace kvdb
