#include "server/kv_server.h"

#include <cinttypes>
#include <cstdlib>

#include "common/logger.h"
#include "replication/replication_transport.h"

namespace kvdb {

KvServer::KvServer(ServerConfig config) : config_(std::move(config)) {}

KvServer::~KvServer() { Stop(); }

Status KvServer::Start() {
  KVDB_RETURN_IF_ERROR(LsmStore::Open(config_.storage, &store_));
  txn_mgr_ = std::make_unique<TransactionManager>(store_.get());

  const bool use_repl = !config_.standalone && config_.peers.size() >= 1;
  if (use_repl) {
    std::map<uint64_t, std::string> peer_only;
    for (const auto& [id, addr] : config_.peers) {
      if (id != config_.node_id) peer_only[id] = addr;
    }
    auto transport = std::make_unique<TcpReplicationTransport>(peer_only, config_.repl.rpc_timeout_ms);

    config_.repl.node_id = config_.node_id;
    config_.repl.peers = config_.peers;
    if (config_.repl.data_dir == ReplicationConfig().data_dir) {
      config_.repl.data_dir = config_.storage.data_dir + "/repl";
    }
    // The initial primary is always the lowest node id in the cluster, same
    // rule a promotion uses -- one deterministic "who's in charge" rule.
    uint64_t lowest = UINT64_MAX;
    for (const auto& [id, addr] : config_.peers) {
      (void)addr;
      lowest = std::min(lowest, id);
    }
    config_.repl.primary_id = lowest;

    auto* store_ptr = store_.get();
    repl_ = std::make_unique<ReplicationNode>(
        config_.repl, std::move(transport), [store_ptr](const std::string& data) -> Status {
          WriteBatch batch;
          KVDB_RETURN_IF_ERROR(batch.SetContents(data));
          WriteOptions wopts;
          wopts.sync = false;  // durability already comes from the replication log's own fsync
          return store_ptr->Write(wopts, &batch);
        });
    KVDB_RETURN_IF_ERROR(repl_->Start());
  }

  tcp_server_ = std::make_unique<TcpServer>(config_.bind_addr, config_.port, config_.io_threads,
                                            config_.worker_threads, config_.max_queue);
  tcp_server_->SetMessageHandler(
      [this](const ConnectionPtr& c, const RequestHeader& h, std::string_view b) {
        OnMessage(c, h, b);
      });
  KVDB_RETURN_IF_ERROR(tcp_server_->Start());

  running_.store(true, std::memory_order_release);
  LOG_INFO("kvdb node %" PRIu64 " ready on port %u (replication=%s)", config_.node_id, port(),
           use_repl ? "on" : "off");
  return Status::OK();
}

void KvServer::Stop() {
  if (!running_.exchange(false)) return;
  if (tcp_server_) tcp_server_->Stop();
  if (repl_) repl_->Stop();
  if (store_) store_->Close();
}

// ---------------------------------------------------------------------------
// Direct API
// ---------------------------------------------------------------------------
std::string KvServer::SelfPrimaryHint() const { return repl_ ? repl_->PrimaryAddress() : ""; }

Status KvServer::EnsureReadable(ReadConsistency c) {
  if (!repl_) return Status::OK();
  switch (c) {
    case ReadConsistency::kStale:
      return Status::OK();
    case ReadConsistency::kPrimaryLocal:
    case ReadConsistency::kLinearizable:
      if (!repl_->IsPrimary()) return Status::NotLeader(SelfPrimaryHint());
      return Status::OK();
  }
  return Status::OK();
}

Status KvServer::ReplicateBatch(WriteBatch* batch) {
  if (!repl_) {
    WriteOptions wopts;
    wopts.sync = config_.storage.sync_writes;
    return store_->Write(wopts, batch);
  }
  std::lock_guard<std::mutex> lk(write_mu_);
  uint64_t seq = 0;
  return repl_->Replicate(batch->Contents(), &seq);
}

Status KvServer::Get(std::string_view key, std::string* value, ReadConsistency c) {
  KVDB_RETURN_IF_ERROR(EnsureReadable(c));
  return store_->Get(ReadOptions(), key, value);
}

Status KvServer::Put(std::string_view key, std::string_view value, bool sync) {
  WriteBatch b;
  b.Put(key, value);
  if (repl_) return ReplicateBatch(&b);
  WriteOptions wopts;
  wopts.sync = sync;
  return store_->Write(wopts, &b);
}

Status KvServer::Delete(std::string_view key, bool sync) {
  WriteBatch b;
  b.Delete(key);
  if (repl_) return ReplicateBatch(&b);
  WriteOptions wopts;
  wopts.sync = sync;
  return store_->Write(wopts, &b);
}

Status KvServer::Batch(const std::vector<BatchOp>& ops, bool sync) {
  WriteBatch b;
  for (const auto& op : ops) {
    if (op.is_delete) {
      b.Delete(op.key);
    } else {
      b.Put(op.key, op.value);
    }
  }
  if (repl_) return ReplicateBatch(&b);
  WriteOptions wopts;
  wopts.sync = sync;
  return store_->Write(wopts, &b);
}

Status KvServer::Scan(std::string_view start, std::string_view end, size_t limit,
                      std::vector<std::pair<std::string, std::string>>* out) {
  return store_->Scan(ReadOptions(), start, end, limit, out);
}

Status KvServer::Cas(std::string_view key, std::string_view expected, std::string_view new_value,
                     bool expect_absent, bool* swapped, bool sync) {
  WriteOptions wopts;
  wopts.sync = sync;
  if (!repl_) {
    return txn_mgr_->CompareAndSwap(key, expected, new_value, expect_absent, swapped, wopts);
  }
  if (!repl_->IsPrimary()) return Status::NotLeader(SelfPrimaryHint());

  // There's only ever one primary accepting writes, so serializing every
  // conditional op behind one mutex (read, decide, replicate) is enough to
  // make it atomic -- no concurrent primary can ever race it.
  std::lock_guard<std::mutex> lk(write_mu_);
  std::string current;
  Status s = store_->Get(ReadOptions(), key, &current);
  const bool absent = s.IsNotFound();
  if (!s.ok() && !absent) return s;

  *swapped = false;
  const bool matches = expect_absent ? absent : (!absent && current == expected);
  if (!matches) return Status::OK();

  WriteBatch b;
  b.Put(key, new_value);
  uint64_t seq = 0;
  s = repl_->Replicate(b.Contents(), &seq);
  if (s.ok()) *swapped = true;
  return s;
}

Status KvServer::Incr(std::string_view key, int64_t delta, int64_t* result, bool sync) {
  WriteOptions wopts;
  wopts.sync = sync;
  if (!repl_) return txn_mgr_->Increment(key, delta, result, wopts);
  if (!repl_->IsPrimary()) return Status::NotLeader(SelfPrimaryHint());

  std::lock_guard<std::mutex> lk(write_mu_);
  std::string current;
  Status s = store_->Get(ReadOptions(), key, &current);
  int64_t value = 0;
  if (s.ok()) {
    char* end = nullptr;
    value = std::strtoll(current.c_str(), &end, 10);
    if (end == current.c_str() || *end != '\0') return Status::InvalidArgument("not an integer");
  } else if (!s.IsNotFound()) {
    return s;
  }
  value += delta;

  WriteBatch b;
  b.Put(key, std::to_string(value));
  uint64_t seq = 0;
  s = repl_->Replicate(b.Contents(), &seq);
  if (s.ok()) *result = value;
  return s;
}

StatusResponse KvServer::GetStatus(bool verbose) const {
  StatusResponse resp;
  resp.status_code = 0;
  resp.node_id = config_.node_id;
  if (repl_) {
    resp.role = RoleName(repl_->role());
    resp.epoch = repl_->Epoch();
    resp.primary_id = repl_->PrimaryId();
    resp.primary_address = repl_->PrimaryAddress();
    resp.last_seq = repl_->LastSeq();
    if (verbose) resp.repl_stats = repl_->GetStats().ToString();
  } else {
    resp.role = "standalone";
  }
  if (verbose && store_) resp.storage_stats = store_->GetStats().ToString();
  return resp;
}

// ---------------------------------------------------------------------------
// Admin / fault injection
// ---------------------------------------------------------------------------
Status KvServer::HandleAdmin(const AdminRequest& req, AdminResponse* resp) {
  resp->status_code = 0;
  if (req.command == "pause") {
    paused_.store(true, std::memory_order_release);
    if (repl_) repl_->PauseHeartbeats();
    resp->result = "paused";
  } else if (req.command == "resume") {
    paused_.store(false, std::memory_order_release);
    if (repl_) repl_->ResumeHeartbeats();
    resp->result = "resumed";
  } else if (req.command == "crash") {
    resp->result = "crashing";
    std::thread([this] {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      if (store_) store_->SimulateCrash();
      _Exit(137);
    }).detach();
  } else if (req.command == "flush") {
    Status s = store_->Flush();
    if (!s.ok()) {
      resp->status_code = codec::StatusToCode(s);
      resp->error = s.ToString();
    } else {
      resp->result = "flushed";
    }
  } else if (req.command == "compact") {
    Status s = store_->CompactRange();
    if (!s.ok()) {
      resp->status_code = codec::StatusToCode(s);
      resp->error = s.ToString();
    } else {
      resp->result = "compacted";
    }
  } else if (req.command == "partition") {
    // arg = "<peer_id>:on" or "<peer_id>:off"
    if (repl_) {
      auto colon = req.arg.find(':');
      if (colon != std::string::npos) {
        uint64_t peer = std::strtoull(req.arg.substr(0, colon).c_str(), nullptr, 10);
        bool on = req.arg.substr(colon + 1) == "on";
        repl_->transport()->SetPartitioned(peer, on);
        resp->result = "ok";
      }
    }
  } else if (req.command == "stats") {
    resp->result = GetStatus(true).storage_stats + " | " + GetStatus(true).repl_stats;
  } else {
    resp->status_code = static_cast<int32_t>(Status::Code::kNotSupported);
    resp->error = "unknown admin command: " + req.command;
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// RPC dispatch
// ---------------------------------------------------------------------------
namespace {
template <typename Resp>
void FillWriteStatus(Resp* resp, const Status& s, const std::string& primary_hint) {
  resp->status_code = codec::StatusToCode(s);
  resp->error = s.ok() ? "" : s.ToString();
  resp->primary_hint = s.IsNotLeader() ? (s.leader_hint().empty() ? primary_hint : s.leader_hint())
                                       : "";
}
}  // namespace

void KvServer::OnMessage(const ConnectionPtr& conn, const RequestHeader& header,
                         std::string_view body) {
  if (paused_.load(std::memory_order_acquire) && header.type != MsgType::kAdminReq) {
    return;
  }

  switch (header.type) {
    case MsgType::kGetReq: {
      GetRequest req;
      GetResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = Get(req.key, &resp.value, req.consistency);
        resp.found = s.ok();
        FillWriteStatus(&resp, s, SelfPrimaryHint());
        if (s.IsNotFound()) resp.status_code = 0;
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kGetResp, header.request_id, resp));
      break;
    }
    case MsgType::kPutReq: {
      PutRequest req;
      WriteResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = Put(req.key, req.value, req.sync);
        FillWriteStatus(&resp, s, SelfPrimaryHint());
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kPutResp, header.request_id, resp));
      break;
    }
    case MsgType::kDeleteReq: {
      DeleteRequest req;
      WriteResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = Delete(req.key, req.sync);
        FillWriteStatus(&resp, s, SelfPrimaryHint());
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kDeleteResp, header.request_id, resp));
      break;
    }
    case MsgType::kScanReq: {
      ScanRequest req;
      ScanResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = EnsureReadable(req.consistency);
        if (s.ok()) s = Scan(req.start_key, req.end_key, req.limit, &resp.entries);
        FillWriteStatus(&resp, s, SelfPrimaryHint());
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kScanResp, header.request_id, resp));
      break;
    }
    case MsgType::kBatchReq: {
      BatchRequest req;
      WriteResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = Batch(req.ops, req.sync);
        FillWriteStatus(&resp, s, SelfPrimaryHint());
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kBatchResp, header.request_id, resp));
      break;
    }
    case MsgType::kCasReq: {
      CasRequest req;
      CasResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = Cas(req.key, req.expected, req.new_value, req.expect_absent, &resp.swapped,
                       req.sync);
        FillWriteStatus(&resp, s, SelfPrimaryHint());
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kCasResp, header.request_id, resp));
      break;
    }
    case MsgType::kIncrReq: {
      IncrRequest req;
      IncrResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        Status s = Incr(req.key, req.delta, &resp.value, req.sync);
        FillWriteStatus(&resp, s, SelfPrimaryHint());
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kIncrResp, header.request_id, resp));
      break;
    }
    case MsgType::kStatusReq: {
      StatusRequest req;
      codec::Decode(body, &req);
      StatusResponse resp = GetStatus(req.verbose);
      conn->Send(codec::BuildFrame(MsgType::kStatusResp, header.request_id, resp));
      break;
    }
    case MsgType::kReplicateReq: {
      if (!repl_) break;
      ReplicateReq req;
      ReplicateResp resp;
      if (codec::Decode(body, &req).ok()) repl_->HandleReplicate(req, &resp);
      conn->Send(codec::BuildFrame(MsgType::kReplicateResp, header.request_id, resp));
      break;
    }
    case MsgType::kHeartbeatReq: {
      if (!repl_) break;
      HeartbeatReq req;
      HeartbeatResp resp;
      if (codec::Decode(body, &req).ok()) repl_->HandleHeartbeat(req, &resp);
      conn->Send(codec::BuildFrame(MsgType::kHeartbeatResp, header.request_id, resp));
      break;
    }
    case MsgType::kCatchUpReq: {
      if (!repl_) break;
      CatchUpReq req;
      CatchUpResp resp;
      if (codec::Decode(body, &req).ok()) repl_->HandleCatchUp(req, &resp);
      conn->Send(codec::BuildFrame(MsgType::kCatchUpResp, header.request_id, resp));
      break;
    }
    case MsgType::kAdminReq: {
      AdminRequest req;
      AdminResponse resp;
      Status ds = codec::Decode(body, &req);
      if (ds.ok()) {
        HandleAdmin(req, &resp);
      } else {
        resp.status_code = codec::StatusToCode(ds);
        resp.error = ds.ToString();
      }
      conn->Send(codec::BuildFrame(MsgType::kAdminResp, header.request_id, resp));
      break;
    }
    default:
      LOG_WARN("unhandled message type %d from %s", static_cast<int>(header.type),
               conn->peer().c_str());
      break;
  }
}

}  // namespace kvdb
