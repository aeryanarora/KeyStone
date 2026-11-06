#pragma once

#include <cstdint>
#include <map>
#include <string>

namespace kvdb {

enum class NodeRole : uint8_t { kPrimary = 0, kBackup = 1 };

inline const char* RoleName(NodeRole r) {
  return r == NodeRole::kPrimary ? "primary" : "backup";
}

struct ReplicationConfig {
  uint64_t node_id = 1;
  uint64_t primary_id = 1;  // who starts as primary, before any promotion
  std::map<uint64_t, std::string> peers;  // every node including self
  std::string data_dir = "./repl";

  int heartbeat_interval_ms = 100;
  int heartbeat_timeout_ms = 500;  // a backup promotes itself after this much silence
  int rpc_timeout_ms = 1000;
  bool sync_log = true;

  // 0 = async: primary applies + replies to the client immediately, ships to
  // backups in the background. N > 0: primary waits for N backup acks first.
  size_t min_acks = 0;
};

}  // namespace kvdb
