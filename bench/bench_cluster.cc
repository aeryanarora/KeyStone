// kvdb-bench-cluster: throughput/latency load generator.
//
//   kvdb-bench-cluster --endpoints=127.0.0.1:7001,127.0.0.1:7002,...
//   kvdb-bench-cluster --embedded-nodes=3 --embedded-min-acks=0
//
// Either point it at a running cluster, or let it spin up an in-process
// cluster itself (handy for comparing node counts / sync modes without
// juggling separate server processes). Sweeps a list of concurrency levels,
// running each for a fixed duration, and reports throughput plus p50/p95/p99
// latency.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "client/kv_client.h"
#include "common/env.h"
#include "common/flags.h"
#include "server/kv_server.h"

using namespace kvdb;

namespace {

std::vector<int> ParseIntList(const std::string& csv, int fallback) {
  std::vector<int> out;
  if (csv.empty()) {
    out.push_back(fallback);
    return out;
  }
  size_t pos = 0;
  while (pos < csv.size()) {
    size_t comma = csv.find(',', pos);
    std::string tok = csv.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
    if (!tok.empty()) out.push_back(std::atoi(tok.c_str()));
    if (comma == std::string::npos) break;
    pos = comma + 1;
  }
  if (out.empty()) out.push_back(fallback);
  return out;
}

enum class WorkloadMode { kPut, kGet, kMixed, kIncr };

WorkloadMode ParseMode(const std::string& s) {
  if (s == "get") return WorkloadMode::kGet;
  if (s == "mixed") return WorkloadMode::kMixed;
  if (s == "incr") return WorkloadMode::kIncr;
  return WorkloadMode::kPut;
}

struct RunResult {
  uint64_t ops = 0;
  uint64_t errors = 0;
  double elapsed_s = 0.0;
  Histogram latency_us;

  double ThroughputOpsPerSec() const { return elapsed_s > 0 ? static_cast<double>(ops) / elapsed_s : 0.0; }
};

// Runs one (concurrency, duration) trial and returns aggregate stats.
RunResult RunTrial(const std::string& endpoints, int concurrency, int duration_ms,
                   WorkloadMode mode, size_t value_size, size_t key_space, bool sync,
                   ReadConsistency read_consistency) {
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_errors{0};
  std::vector<Histogram> per_thread_hist(static_cast<size_t>(concurrency));
  std::vector<std::thread> threads;

  const std::string value(value_size, 'v');

  for (int t = 0; t < concurrency; t++) {
    threads.emplace_back([&, t] {
      KvClient client;
      if (!client.Connect(endpoints).ok()) return;
      std::mt19937_64 rng(static_cast<uint64_t>(t) * 7919 + 13);
      uint64_t local_ops = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        char keybuf[32];
        snprintf(keybuf, sizeof(keybuf), "bench-key-%016llu",
                static_cast<unsigned long long>(rng() % key_space));
        std::string key(keybuf);

        uint64_t start_us = NowMicros();
        Status s;
        WorkloadMode op_mode = mode;
        if (mode == WorkloadMode::kMixed) {
          op_mode = (rng() % 10 < 8) ? WorkloadMode::kGet : WorkloadMode::kPut;
        }
        switch (op_mode) {
          case WorkloadMode::kPut:
            s = client.Put(key, value, sync);
            break;
          case WorkloadMode::kGet: {
            std::string out;
            s = client.Get(key, &out, read_consistency);
            if (s.IsNotFound()) s = Status::OK();
            break;
          }
          case WorkloadMode::kIncr: {
            int64_t result = 0;
            s = client.Incr("bench-counter", 1, &result, sync);
            break;
          }
          case WorkloadMode::kMixed:
            break;  // unreachable, resolved above
        }
        uint64_t elapsed_us = NowMicros() - start_us;

        per_thread_hist[static_cast<size_t>(t)].Add(static_cast<double>(elapsed_us));
        local_ops++;
        if (!s.ok()) total_errors.fetch_add(1, std::memory_order_relaxed);
      }
      total_ops.fetch_add(local_ops, std::memory_order_relaxed);
    });
  }

  uint64_t start = NowMillis();
  std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
  stop.store(true, std::memory_order_relaxed);
  for (auto& th : threads) th.join();
  uint64_t elapsed_ms = NowMillis() - start;

  RunResult result;
  result.ops = total_ops.load();
  result.errors = total_errors.load();
  result.elapsed_s = static_cast<double>(elapsed_ms) / 1000.0;
  for (auto& h : per_thread_hist) result.latency_us.Merge(h);
  return result;
}

void PrintHeader() {
  printf("%-6s %10s %12s %10s %10s %10s %10s\n", "conc", "ops/sec", "errors", "p50(us)",
        "p95(us)", "p99(us)", "max(us)");
  printf("%s\n", std::string(72, '-').c_str());
}

void PrintRow(int concurrency, RunResult& r) {
  printf("%-6d %10.1f %12" PRIu64 " %10.1f %10.1f %10.1f %10.1f\n", concurrency,
        r.ThroughputOpsPerSec(), r.errors, r.latency_us.Percentile(50), r.latency_us.Percentile(95),
        r.latency_us.Percentile(99), r.latency_us.max());
}

// Spins up an in-process cluster of `n` nodes, all replicating to each
// other, on ephemeral ports. Returns the comma-separated endpoint list.
struct EmbeddedCluster {
  std::vector<std::unique_ptr<KvServer>> servers;
  std::string base_dir;
  std::string endpoints;

  ~EmbeddedCluster() {
    for (auto& s : servers) {
      if (s) s->Stop();
    }
  }
};

int FreeTcpPortLocal() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = 0;
  ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  int port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

std::unique_ptr<EmbeddedCluster> StartEmbeddedCluster(int n, size_t min_acks, bool sync_repl) {
  auto cluster = std::make_unique<EmbeddedCluster>();
  cluster->base_dir = "/tmp/kvdb_bench_cluster_" + std::to_string(::getpid());
  fileutil::DeleteDirRecursive(cluster->base_dir);

  std::vector<int> ports;
  for (int i = 0; i < n; i++) ports.push_back(FreeTcpPortLocal());
  std::map<uint64_t, std::string> peers;
  for (int i = 0; i < n; i++) peers[static_cast<uint64_t>(i + 1)] = "127.0.0.1:" + std::to_string(ports[static_cast<size_t>(i)]);

  for (int i = 0; i < n; i++) {
    ServerConfig cfg;
    cfg.node_id = static_cast<uint64_t>(i + 1);
    cfg.bind_addr = "127.0.0.1";
    cfg.port = static_cast<uint16_t>(ports[static_cast<size_t>(i)]);
    cfg.peers = peers;
    cfg.standalone = (n <= 1);
    cfg.storage.data_dir = cluster->base_dir + "/node" + std::to_string(i + 1);
    cfg.storage.background_compaction = true;
    cfg.io_threads = 2;
    cfg.worker_threads = 8;
    cfg.repl.data_dir = cfg.storage.data_dir + "/repl";
    cfg.repl.min_acks = min_acks;
    cfg.repl.sync_log = sync_repl;

    auto server = std::make_unique<KvServer>(cfg);
    Status s = server->Start();
    if (!s.ok()) {
      fprintf(stderr, "failed to start embedded node %d: %s\n", i + 1, s.ToString().c_str());
      return nullptr;
    }
    if (!cluster->endpoints.empty()) cluster->endpoints += ",";
    cluster->endpoints += "127.0.0.1:" + std::to_string(ports[static_cast<size_t>(i)]);
    cluster->servers.push_back(std::move(server));
  }

  // Give the initial primary a moment to be ready.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  return cluster;
}

}  // namespace

int main(int argc, char** argv) {
  Flags flags(argc, argv);

  if (flags.Has("help")) {
    printf(
        "kvdb-bench-cluster [--endpoints=host:port,...] [--embedded-nodes=N]\n"
        "  --embedded-nodes=N        spin up N nodes in-process instead of --endpoints\n"
        "  --embedded-min-acks=N     backup acks the embedded primary waits for (default 0)\n"
        "  --embedded-sync-repl=bool fsync the embedded replication log (default true)\n"
        "  --concurrency=1,2,4,8     comma-separated client counts to sweep (default 1,4,16)\n"
        "  --duration-ms=N           per-concurrency-level trial duration (default 2000)\n"
        "  --mode=put|get|mixed|incr workload (default put)\n"
        "  --value-size=N            bytes per value (default 100)\n"
        "  --key-space=N             distinct keys touched (default 10000)\n"
        "  --sync=bool               request synchronous durability per write (default false)\n"
        "  --read-consistency=linearizable|primary-local|stale (default linearizable)\n");
    return 0;
  }

  std::unique_ptr<EmbeddedCluster> embedded;
  std::string endpoints = flags.Get("endpoints", "");
  if (endpoints.empty()) {
    int n = flags.GetInt("embedded-nodes", 3);
    size_t min_acks = static_cast<size_t>(flags.GetInt("embedded-min-acks", 0));
    bool sync_repl = flags.GetBool("embedded-sync-repl", true);
    embedded = StartEmbeddedCluster(n, min_acks, sync_repl);
    if (!embedded) return 1;
    endpoints = embedded->endpoints;
    printf("embedded cluster: %d node(s) at %s\n", n, endpoints.c_str());
  }

  std::vector<int> concurrency_levels = ParseIntList(flags.Get("concurrency", "1,4,16"), 4);
  int duration_ms = flags.GetInt("duration-ms", 2000);
  WorkloadMode mode = ParseMode(flags.Get("mode", "put"));
  size_t value_size = static_cast<size_t>(flags.GetInt("value-size", 100));
  size_t key_space = static_cast<size_t>(flags.GetInt("key-space", 10000));
  bool sync = flags.GetBool("sync", false);

  std::string rc_str = flags.Get("read-consistency", "linearizable");
  ReadConsistency rc = ReadConsistency::kLinearizable;
  if (rc_str == "primary-local") rc = ReadConsistency::kPrimaryLocal;
  if (rc_str == "stale") rc = ReadConsistency::kStale;

  printf("mode=%s value_size=%zu key_space=%zu sync=%s duration_ms=%d\n", flags.Get("mode", "put").c_str(),
        value_size, key_space, sync ? "true" : "false", duration_ms);
  PrintHeader();
  for (int c : concurrency_levels) {
    RunResult r = RunTrial(endpoints, c, duration_ms, mode, value_size, key_space, sync, rc);
    PrintRow(c, r);
  }
  return 0;
}
