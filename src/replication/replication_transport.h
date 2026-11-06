#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "net/messages.h"

namespace kvdb {

// Abstracts "send a replication RPC to peer X" so ReplicationNode can be
// unit-tested with an in-process fake transport and run for real over
// TcpClient/TcpServer with no change to replication_node.cc.
class ReplicationTransport {
 public:
  virtual ~ReplicationTransport() = default;

  using ReplicateCallback = std::function<void(Status, ReplicateResp)>;
  using HeartbeatCallback = std::function<void(Status, HeartbeatResp)>;
  using CatchUpCallback = std::function<void(Status, CatchUpResp)>;

  virtual void SendReplicate(uint64_t peer_id, const ReplicateReq& req, ReplicateCallback cb) = 0;
  virtual void SendHeartbeat(uint64_t peer_id, const HeartbeatReq& req, HeartbeatCallback cb) = 0;
  virtual void SendCatchUp(uint64_t peer_id, const CatchUpReq& req, CatchUpCallback cb) = 0;

  virtual void SetPartitioned(uint64_t peer_id, bool partitioned) {}
};

class TcpReplicationTransport : public ReplicationTransport {
 public:
  TcpReplicationTransport(std::map<uint64_t, std::string> peer_addresses, int rpc_timeout_ms);
  ~TcpReplicationTransport() override;

  void SendReplicate(uint64_t peer_id, const ReplicateReq& req, ReplicateCallback cb) override;
  void SendHeartbeat(uint64_t peer_id, const HeartbeatReq& req, HeartbeatCallback cb) override;
  void SendCatchUp(uint64_t peer_id, const CatchUpReq& req, CatchUpCallback cb) override;

  void SetPartitioned(uint64_t peer_id, bool partitioned) override;

  struct PeerConn;

 private:
  PeerConn* GetOrCreate(uint64_t peer_id);

  std::map<uint64_t, std::string> addresses_;
  int rpc_timeout_ms_;
  std::mutex mu_;
  std::map<uint64_t, std::unique_ptr<PeerConn>> conns_;
  class ThreadPool* pool_;
};

}  // namespace kvdb
