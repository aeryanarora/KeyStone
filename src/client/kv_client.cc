#include "client/kv_client.h"

#include <chrono>
#include <sstream>
#include <thread>

#include "net/codec.h"

namespace kvdb {

Status KvClient::Connect(const std::string& endpoints) {
  endpoints_.clear();
  std::stringstream ss(endpoints);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (!item.empty()) endpoints_.push_back(item);
  }
  if (endpoints_.empty()) return Status::InvalidArgument("no endpoints given");

  client_ = std::make_unique<TcpClient>();
  Status last;
  for (const auto& ep : endpoints_) {
    last = client_->Connect(ep);
    if (last.ok()) {
      connected_to_ = ep;
      return Status::OK();
    }
  }
  return last;
}

template <typename Req, typename Resp>
Status KvClient::CallWithRetry(MsgType type, const Req& req, Resp* resp, int max_retries) {
  if (!client_) return Status::InvalidArgument("not connected");

  std::vector<std::string> candidates;
  if (!leader_hint_.empty()) candidates.push_back(leader_hint_);
  for (const auto& ep : endpoints_) {
    if (ep != leader_hint_) candidates.push_back(ep);
  }

  Status last = Status::Unavailable("no endpoints tried");
  for (int attempt = 0; attempt <= max_retries; attempt++) {
    // Index-based on purpose: a NotLeader reply inserts a fresh candidate at
    // the front below, and mutating `candidates` mid-iteration would
    // invalidate a range-for's iterators out from under it (undefined
    // behavior) -- an index into the vector stays valid across insert().
    size_t i = 0;
    size_t hint_jumps = 0;  // bounds a pathological server that keeps redirecting
    while (i < candidates.size()) {
      const std::string& ep = candidates[i];
      if (connected_to_ != ep || !client_->connected()) {
        Status cs = client_->Connect(ep, 2000);
        if (!cs.ok()) {
          last = cs;
          i++;
          continue;
        }
        connected_to_ = ep;
      }
      Status s = client_->Call(type, req, resp, 5000);
      if (!s.ok()) {
        last = s;
        client_->Disconnect();
        i++;
        continue;
      }
      last = codec::CodeToStatus(resp->status_code, resp->error, resp->primary_hint);
      if (last.IsNotLeader() && !last.leader_hint().empty() &&
          hint_jumps++ < endpoints_.size() + 1) {
        leader_hint_ = last.leader_hint();
        candidates.insert(candidates.begin(), leader_hint_);
        i = 0;  // retry immediately against the newly-inserted hint at the front
        continue;
      }
      return last;  // OK, or a non-retryable application error
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
  }
  return last;
}

Status KvClient::Get(std::string_view key, std::string* value, ReadConsistency c) {
  GetRequest req;
  req.key = std::string(key);
  req.consistency = c;
  GetResponse resp;
  Status s = CallWithRetry(MsgType::kGetReq, req, &resp);
  if (!s.ok()) return s;
  if (!resp.found) return Status::NotFound();
  *value = resp.value;
  return Status::OK();
}

Status KvClient::Put(std::string_view key, std::string_view value, bool sync) {
  PutRequest req;
  req.key = std::string(key);
  req.value = std::string(value);
  req.sync = sync;
  WriteResponse resp;
  return CallWithRetry(MsgType::kPutReq, req, &resp);
}

Status KvClient::Delete(std::string_view key, bool sync) {
  DeleteRequest req;
  req.key = std::string(key);
  req.sync = sync;
  WriteResponse resp;
  return CallWithRetry(MsgType::kDeleteReq, req, &resp);
}

Status KvClient::Scan(std::string_view start, std::string_view end, size_t limit,
                      std::vector<std::pair<std::string, std::string>>* out, ReadConsistency c) {
  ScanRequest req;
  req.start_key = std::string(start);
  req.end_key = std::string(end);
  req.limit = static_cast<uint32_t>(limit);
  req.consistency = c;
  ScanResponse resp;
  Status s = CallWithRetry(MsgType::kScanReq, req, &resp);
  if (!s.ok()) return s;
  *out = std::move(resp.entries);
  return Status::OK();
}

Status KvClient::Batch(const std::vector<BatchOp>& ops, bool sync) {
  BatchRequest req;
  req.ops = ops;
  req.sync = sync;
  WriteResponse resp;
  return CallWithRetry(MsgType::kBatchReq, req, &resp);
}

Status KvClient::Cas(std::string_view key, std::string_view expected, std::string_view new_value,
                     bool expect_absent, bool* swapped, bool sync) {
  CasRequest req;
  req.key = std::string(key);
  req.expected = std::string(expected);
  req.new_value = std::string(new_value);
  req.expect_absent = expect_absent;
  req.sync = sync;
  CasResponse resp;
  Status s = CallWithRetry(MsgType::kCasReq, req, &resp);
  if (!s.ok()) return s;
  *swapped = resp.swapped;
  return Status::OK();
}

Status KvClient::Incr(std::string_view key, int64_t delta, int64_t* result, bool sync) {
  IncrRequest req;
  req.key = std::string(key);
  req.delta = delta;
  req.sync = sync;
  IncrResponse resp;
  Status s = CallWithRetry(MsgType::kIncrReq, req, &resp);
  if (!s.ok()) return s;
  *result = resp.value;
  return Status::OK();
}

Status KvClient::GetStatus(StatusResponse* out, bool verbose) {
  StatusRequest req;
  req.verbose = verbose;
  if (!client_ || !client_->connected()) {
    if (!endpoints_.empty()) client_->Connect(endpoints_.front());
  }
  return client_->Call(MsgType::kStatusReq, req, out);
}

Status KvClient::Admin(const std::string& command, const std::string& arg, AdminResponse* out) {
  AdminRequest req;
  req.command = command;
  req.arg = arg;
  if (!client_) return Status::InvalidArgument("not connected");
  return client_->Call(MsgType::kAdminReq, req, out);
}

}  // namespace kvdb
