#include "replication/replication_transport.h"

#include "concurrency/thread_pool.h"
#include "net/tcp_client.h"

namespace kvdb {

struct TcpReplicationTransport::PeerConn {
  std::string address;
  std::mutex mu;
  std::unique_ptr<TcpClient> client;
  bool partitioned = false;
};

TcpReplicationTransport::TcpReplicationTransport(std::map<uint64_t, std::string> peer_addresses,
                                                 int rpc_timeout_ms)
    : addresses_(std::move(peer_addresses)), rpc_timeout_ms_(rpc_timeout_ms) {
  pool_ = new ThreadPool(4, 4096, "repl-tx");
}

TcpReplicationTransport::~TcpReplicationTransport() { delete pool_; }

TcpReplicationTransport::PeerConn* TcpReplicationTransport::GetOrCreate(uint64_t peer_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = conns_.find(peer_id);
  if (it != conns_.end()) return it->second.get();
  auto addr_it = addresses_.find(peer_id);
  if (addr_it == addresses_.end()) return nullptr;
  auto pc = std::make_unique<PeerConn>();
  pc->address = addr_it->second;
  pc->client = std::make_unique<TcpClient>();
  PeerConn* raw = pc.get();
  conns_[peer_id] = std::move(pc);
  return raw;
}

void TcpReplicationTransport::SetPartitioned(uint64_t peer_id, bool partitioned) {
  PeerConn* pc = GetOrCreate(peer_id);
  if (pc == nullptr) return;
  std::lock_guard<std::mutex> lk(pc->mu);
  pc->partitioned = partitioned;
  if (partitioned && pc->client) pc->client->Disconnect();
}

namespace {
template <typename Req, typename Resp, typename Cb>
void SendGeneric(TcpReplicationTransport::PeerConn* pc, ThreadPool* pool, MsgType req_type, Req req,
                 int timeout_ms, Cb cb) {
  if (pc == nullptr) {
    pool->Submit([cb] { cb(Status::NetworkError("unknown peer"), Resp{}); });
    return;
  }
  pool->Submit([pc, req_type, req = std::move(req), timeout_ms, cb]() mutable {
    std::lock_guard<std::mutex> lk(pc->mu);
    if (pc->partitioned) {
      cb(Status::NetworkError("peer partitioned (test injection)"), Resp{});
      return;
    }
    if (!pc->client->connected()) {
      Status cs = pc->client->Connect(pc->address, timeout_ms);
      if (!cs.ok()) {
        cb(cs, Resp{});
        return;
      }
    }
    Resp resp;
    Status s = pc->client->Call(req_type, req, &resp, timeout_ms);
    if (!s.ok()) pc->client->Disconnect();
    cb(s, resp);
  });
}
}  // namespace

void TcpReplicationTransport::SendReplicate(uint64_t peer_id, const ReplicateReq& req,
                                            ReplicateCallback cb) {
  SendGeneric<ReplicateReq, ReplicateResp>(GetOrCreate(peer_id), pool_, MsgType::kReplicateReq,
                                           req, rpc_timeout_ms_, std::move(cb));
}

void TcpReplicationTransport::SendHeartbeat(uint64_t peer_id, const HeartbeatReq& req,
                                            HeartbeatCallback cb) {
  SendGeneric<HeartbeatReq, HeartbeatResp>(GetOrCreate(peer_id), pool_, MsgType::kHeartbeatReq,
                                           req, rpc_timeout_ms_, std::move(cb));
}

void TcpReplicationTransport::SendCatchUp(uint64_t peer_id, const CatchUpReq& req,
                                          CatchUpCallback cb) {
  SendGeneric<CatchUpReq, CatchUpResp>(GetOrCreate(peer_id), pool_, MsgType::kCatchUpReq, req,
                                       rpc_timeout_ms_, std::move(cb));
}

}  // namespace kvdb
