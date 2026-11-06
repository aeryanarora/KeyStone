// kvdb-server: standalone or primary-backup replicated node entry point.
//
//   kvdb-server --node-id=1 --port=7001 --data-dir=/tmp/node1 \
//               --peers=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//
// Omit --peers (or pass a single entry) to run standalone, with no
// replication.
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <sstream>

#include "common/flags.h"
#include "common/logger.h"
#include "server/kv_server.h"

namespace {
std::atomic<bool> g_stop{false};
void HandleSignal(int) { g_stop.store(true); }

std::map<uint64_t, std::string> ParsePeers(const std::string& spec) {
  // "1=host:port,2=host:port,..."
  std::map<uint64_t, std::string> out;
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    auto eq = item.find('=');
    if (eq == std::string::npos) continue;
    uint64_t id = std::strtoull(item.substr(0, eq).c_str(), nullptr, 10);
    out[id] = item.substr(eq + 1);
  }
  return out;
}
}  // namespace

int main(int argc, char** argv) {
  kvdb::Flags flags(argc, argv);

  if (flags.Has("help")) {
    printf(
        "kvdb-server --node-id=N --port=P --data-dir=DIR [--peers=1=host:port,2=host:port,...]\n"
        "  --sync-writes=true|false     fsync WAL on every write (default false)\n"
        "  --sync-repl-log=true|false   fsync replication log on every append (default true)\n"
        "  --min-acks=N                 backup acks primary waits for before replying (default 0)\n"
        "  --write-buffer-mb=N          memtable flush threshold (default 4)\n"
        "  --block-cache-mb=N           SSTable block cache size (default 8)\n"
        "  --worker-threads=N           RPC worker pool size (default 8)\n"
        "  --log-level=debug|info|warn|error\n");
    return 0;
  }

  std::string level = flags.Get("log-level", "info");
  kvdb::LogLevel lvl = kvdb::LogLevel::kInfo;
  if (level == "debug") lvl = kvdb::LogLevel::kDebug;
  else if (level == "warn") lvl = kvdb::LogLevel::kWarn;
  else if (level == "error") lvl = kvdb::LogLevel::kError;
  kvdb::Logger::Instance().SetLevel(lvl);

  kvdb::ServerConfig cfg;
  cfg.node_id = static_cast<uint64_t>(flags.GetInt("node-id", 1));
  cfg.bind_addr = flags.Get("bind", "0.0.0.0");
  cfg.port = static_cast<uint16_t>(flags.GetInt("port", 7000));
  cfg.storage.data_dir = flags.Get("data-dir", "./kvdb-data");
  cfg.storage.sync_writes = flags.GetBool("sync-writes", false);
  cfg.storage.write_buffer_size = static_cast<size_t>(flags.GetInt("write-buffer-mb", 4)) * 1024 * 1024;
  cfg.storage.block_cache_bytes = static_cast<size_t>(flags.GetInt("block-cache-mb", 8)) * 1024 * 1024;
  cfg.storage.enable_group_commit = flags.GetBool("group-commit", true);
  cfg.io_threads = static_cast<size_t>(flags.GetInt("io-threads", 2));
  cfg.worker_threads = static_cast<size_t>(flags.GetInt("worker-threads", 8));

  std::string peers_spec = flags.Get("peers", "");
  if (!peers_spec.empty()) {
    cfg.peers = ParsePeers(peers_spec);
    cfg.standalone = false;
  } else {
    cfg.standalone = true;
  }
  cfg.repl.data_dir = flags.Get("repl-dir", cfg.storage.data_dir + "/repl");
  cfg.repl.sync_log = flags.GetBool("sync-repl-log", true);
  cfg.repl.heartbeat_interval_ms = flags.GetInt("heartbeat-ms", 100);
  cfg.repl.heartbeat_timeout_ms = flags.GetInt("heartbeat-timeout-ms", 500);
  cfg.repl.min_acks = static_cast<size_t>(flags.GetInt("min-acks", 0));

  kvdb::KvServer server(cfg);
  kvdb::Status s = server.Start();
  if (!s.ok()) {
    fprintf(stderr, "failed to start: %s\n", s.ToString().c_str());
    return 1;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
  while (!g_stop.load() && server.running()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  LOG_INFO("shutting down node %" PRIu64, cfg.node_id);
  server.Stop();
  return 0;
}
