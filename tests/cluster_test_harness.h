#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "client/kv_client.h"
#include "server/kv_server.h"
#include "test_util.h"

namespace kvdb {
namespace testutil {

class RealCluster {
 public:
  void Start(size_t n, const std::string& base_dir, bool sync_writes = false) {
    std::vector<int> ports;
    for (size_t i = 0; i < n; i++) ports.push_back(FreeTcpPort());

    std::map<uint64_t, std::string> peers;
    for (size_t i = 0; i < n; i++) peers[i + 1] = "127.0.0.1:" + std::to_string(ports[i]);

    for (size_t i = 0; i < n; i++) {
      ServerConfig cfg;
      cfg.node_id = i + 1;
      cfg.bind_addr = "127.0.0.1";
      cfg.port = static_cast<uint16_t>(ports[i]);
      cfg.peers = peers;
      cfg.standalone = false;
      cfg.storage.data_dir = base_dir + "/node" + std::to_string(i + 1);
      cfg.storage.sync_writes = sync_writes;
      cfg.storage.background_compaction = true;
      cfg.io_threads = 1;
      cfg.worker_threads = 4;
      cfg.repl.data_dir = cfg.storage.data_dir + "/repl";
      cfg.repl.heartbeat_interval_ms = 50;
      cfg.repl.heartbeat_timeout_ms = 200;
      cfg.repl.sync_log = sync_writes;
      cfg.repl.rpc_timeout_ms = 800;

      auto server = std::make_unique<KvServer>(cfg);
      Status s = server->Start();
      if (!s.ok()) {
        ADD_FAILURE() << "failed to start node " << (i + 1) << ": " << s.ToString();
        continue;
      }
      configs_.push_back(cfg);
      servers_.push_back(std::move(server));
    }
  }

  ~RealCluster() {
    for (auto& s : servers_) {
      if (s) s->Stop();
    }
  }

  std::string Endpoints() const {
    std::string out;
    for (const auto& cfg : configs_) {
      if (!out.empty()) out += ",";
      out += cfg.bind_addr + ":" + std::to_string(cfg.port);
    }
    return out;
  }

  std::string EndpointFor(size_t idx) const {
    return configs_[idx].bind_addr + ":" + std::to_string(configs_[idx].port);
  }

  KvServer* FindPrimary(int timeout_ms = 5000) {
    uint64_t deadline = NowMillis() + static_cast<uint64_t>(timeout_ms);
    while (NowMillis() < deadline) {
      for (auto& s : servers_) {
        if (s && s->repl() && s->repl()->IsPrimary()) return s.get();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    return nullptr;
  }

  void KillNode(size_t idx) {
    if (idx < servers_.size() && servers_[idx]) {
      servers_[idx]->Stop();
      servers_[idx].reset();
    }
  }

  Status RestartNode(size_t idx) {
    if (idx >= configs_.size()) return Status::InvalidArgument("bad index");
    auto server = std::make_unique<KvServer>(configs_[idx]);
    KVDB_RETURN_IF_ERROR(server->Start());
    servers_[idx] = std::move(server);
    return Status::OK();
  }

  size_t size() const { return servers_.size(); }
  KvServer* at(size_t idx) { return servers_[idx].get(); }

 private:
  std::vector<ServerConfig> configs_;
  std::vector<std::unique_ptr<KvServer>> servers_;
};

}  // namespace testutil
}  // namespace kvdb
