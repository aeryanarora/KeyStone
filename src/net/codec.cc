#include "net/codec.h"

#include "common/coding.h"

namespace kvdb {
namespace codec {

void StartFrame(std::string* out, const RequestHeader& header) {
  out->clear();
  out->append(kFrameLenSize, '\0');
  out->push_back(static_cast<char>(header.type));
  PutFixed64(out, header.request_id);
}

void FinishFrame(std::string* out) {
  const auto payload_len = static_cast<uint32_t>(out->size() - kFrameLenSize);
  EncodeFixed32(out->data(), payload_len);
}

size_t PeekFrameSize(std::string_view buffer, bool* error) {
  *error = false;
  if (buffer.size() < kFrameLenSize) return 0;
  const uint32_t payload_len = DecodeFixed32(buffer.data());
  if (payload_len > kMaxFrameSize || payload_len < 9) {
    *error = true;
    return 0;
  }
  const size_t total = kFrameLenSize + payload_len;
  if (buffer.size() < total) return 0;
  return total;
}

Status DecodeHeader(std::string_view payload, RequestHeader* header, std::string_view* rest) {
  if (payload.size() < 9) return Status::Corruption("frame too short");
  header->type = static_cast<MsgType>(static_cast<uint8_t>(payload[0]));
  payload.remove_prefix(1);
  if (!GetFixed64(&payload, &header->request_id)) return Status::Corruption("bad request id");
  *rest = payload;
  return Status::OK();
}

int32_t StatusToCode(const Status& s) { return static_cast<int32_t>(s.code()); }

Status CodeToStatus(int32_t code, const std::string& msg, const std::string& primary_hint) {
  auto c = static_cast<Status::Code>(code);
  switch (c) {
    case Status::Code::kOk: return Status::OK();
    case Status::Code::kNotFound: return Status::NotFound(msg);
    case Status::Code::kCorruption: return Status::Corruption(msg);
    case Status::Code::kIOError: return Status::IOError(msg);
    case Status::Code::kInvalidArgument: return Status::InvalidArgument(msg);
    case Status::Code::kNotSupported: return Status::NotSupported(msg);
    case Status::Code::kNotLeader: return Status::NotLeader(primary_hint);
    case Status::Code::kTimeout: return Status::Timeout(msg);
    case Status::Code::kUnavailable: return Status::Unavailable(msg);
    case Status::Code::kAborted: return Status::Aborted(msg);
    case Status::Code::kNetworkError: return Status::NetworkError(msg);
    case Status::Code::kShutdown: return Status::Shutdown(msg);
  }
  return Status::Corruption("unknown status code");
}

namespace {
inline void PutBool(std::string* out, bool b) { out->push_back(b ? 1 : 0); }
inline bool GetBool(std::string_view* in, bool* b) {
  if (in->empty()) return false;
  *b = (*in)[0] != 0;
  in->remove_prefix(1);
  return true;
}
inline void PutI64(std::string* out, int64_t v) { PutFixed64(out, static_cast<uint64_t>(v)); }
inline bool GetI64(std::string_view* in, int64_t* v) {
  uint64_t u = 0;
  if (!GetFixed64(in, &u)) return false;
  *v = static_cast<int64_t>(u);
  return true;
}
inline void PutU8(std::string* out, uint8_t v) { out->push_back(static_cast<char>(v)); }
inline bool GetU8(std::string_view* in, uint8_t* v) {
  if (in->empty()) return false;
  *v = static_cast<uint8_t>((*in)[0]);
  in->remove_prefix(1);
  return true;
}
#define TRY(expr) \
  if (!(expr)) return Status::Corruption("truncated message body")
}  // namespace

static void PutRespHead(std::string* o, int32_t code, const std::string& err,
                        const std::string& hint) {
  PutFixed32(o, static_cast<uint32_t>(code));
  PutLengthPrefixed(o, err);
  PutLengthPrefixed(o, hint);
}
static bool GetRespHead(std::string_view* in, int32_t* code, std::string* err, std::string* hint) {
  uint32_t c = 0;
  if (!GetFixed32(in, &c)) return false;
  *code = static_cast<int32_t>(c);
  return GetLengthPrefixedString(in, err) && GetLengthPrefixedString(in, hint);
}

void Encode(std::string* o, const GetRequest& m) {
  PutLengthPrefixed(o, m.key);
  PutU8(o, static_cast<uint8_t>(m.consistency));
}
Status Decode(std::string_view in, GetRequest* m) {
  uint8_t c = 0;
  TRY(GetLengthPrefixedString(&in, &m->key));
  TRY(GetU8(&in, &c));
  m->consistency = static_cast<ReadConsistency>(c);
  return Status::OK();
}

void Encode(std::string* o, const GetResponse& m) {
  PutRespHead(o, m.status_code, m.error, m.primary_hint);
  PutBool(o, m.found);
  PutLengthPrefixed(o, m.value);
}
Status Decode(std::string_view in, GetResponse* m) {
  TRY(GetRespHead(&in, &m->status_code, &m->error, &m->primary_hint));
  TRY(GetBool(&in, &m->found));
  TRY(GetLengthPrefixedString(&in, &m->value));
  return Status::OK();
}

void Encode(std::string* o, const PutRequest& m) {
  PutLengthPrefixed(o, m.key);
  PutLengthPrefixed(o, m.value);
  PutBool(o, m.sync);
}
Status Decode(std::string_view in, PutRequest* m) {
  TRY(GetLengthPrefixedString(&in, &m->key));
  TRY(GetLengthPrefixedString(&in, &m->value));
  TRY(GetBool(&in, &m->sync));
  return Status::OK();
}

void Encode(std::string* o, const DeleteRequest& m) {
  PutLengthPrefixed(o, m.key);
  PutBool(o, m.sync);
}
Status Decode(std::string_view in, DeleteRequest* m) {
  TRY(GetLengthPrefixedString(&in, &m->key));
  TRY(GetBool(&in, &m->sync));
  return Status::OK();
}

void Encode(std::string* o, const WriteResponse& m) {
  PutRespHead(o, m.status_code, m.error, m.primary_hint);
  PutFixed64(o, m.seq);
}
Status Decode(std::string_view in, WriteResponse* m) {
  TRY(GetRespHead(&in, &m->status_code, &m->error, &m->primary_hint));
  TRY(GetFixed64(&in, &m->seq));
  return Status::OK();
}

void Encode(std::string* o, const ScanRequest& m) {
  PutLengthPrefixed(o, m.start_key);
  PutLengthPrefixed(o, m.end_key);
  PutFixed32(o, m.limit);
  PutU8(o, static_cast<uint8_t>(m.consistency));
}
Status Decode(std::string_view in, ScanRequest* m) {
  uint8_t c = 0;
  TRY(GetLengthPrefixedString(&in, &m->start_key));
  TRY(GetLengthPrefixedString(&in, &m->end_key));
  TRY(GetFixed32(&in, &m->limit));
  TRY(GetU8(&in, &c));
  m->consistency = static_cast<ReadConsistency>(c);
  return Status::OK();
}

void Encode(std::string* o, const ScanResponse& m) {
  PutRespHead(o, m.status_code, m.error, m.primary_hint);
  PutFixed32(o, static_cast<uint32_t>(m.entries.size()));
  for (const auto& [k, v] : m.entries) {
    PutLengthPrefixed(o, k);
    PutLengthPrefixed(o, v);
  }
}
Status Decode(std::string_view in, ScanResponse* m) {
  TRY(GetRespHead(&in, &m->status_code, &m->error, &m->primary_hint));
  uint32_t n = 0;
  TRY(GetFixed32(&in, &n));
  m->entries.clear();
  m->entries.reserve(n);
  for (uint32_t i = 0; i < n; i++) {
    std::string k, v;
    TRY(GetLengthPrefixedString(&in, &k));
    TRY(GetLengthPrefixedString(&in, &v));
    m->entries.emplace_back(std::move(k), std::move(v));
  }
  return Status::OK();
}

void Encode(std::string* o, const BatchRequest& m) {
  PutFixed32(o, static_cast<uint32_t>(m.ops.size()));
  for (const auto& op : m.ops) {
    PutBool(o, op.is_delete);
    PutLengthPrefixed(o, op.key);
    PutLengthPrefixed(o, op.value);
  }
  PutBool(o, m.sync);
}
Status Decode(std::string_view in, BatchRequest* m) {
  uint32_t n = 0;
  TRY(GetFixed32(&in, &n));
  m->ops.clear();
  m->ops.reserve(n);
  for (uint32_t i = 0; i < n; i++) {
    BatchOp op;
    TRY(GetBool(&in, &op.is_delete));
    TRY(GetLengthPrefixedString(&in, &op.key));
    TRY(GetLengthPrefixedString(&in, &op.value));
    m->ops.push_back(std::move(op));
  }
  TRY(GetBool(&in, &m->sync));
  return Status::OK();
}

void Encode(std::string* o, const CasRequest& m) {
  PutLengthPrefixed(o, m.key);
  PutLengthPrefixed(o, m.expected);
  PutLengthPrefixed(o, m.new_value);
  PutBool(o, m.expect_absent);
  PutBool(o, m.sync);
}
Status Decode(std::string_view in, CasRequest* m) {
  TRY(GetLengthPrefixedString(&in, &m->key));
  TRY(GetLengthPrefixedString(&in, &m->expected));
  TRY(GetLengthPrefixedString(&in, &m->new_value));
  TRY(GetBool(&in, &m->expect_absent));
  TRY(GetBool(&in, &m->sync));
  return Status::OK();
}

void Encode(std::string* o, const CasResponse& m) {
  PutRespHead(o, m.status_code, m.error, m.primary_hint);
  PutBool(o, m.swapped);
}
Status Decode(std::string_view in, CasResponse* m) {
  TRY(GetRespHead(&in, &m->status_code, &m->error, &m->primary_hint));
  TRY(GetBool(&in, &m->swapped));
  return Status::OK();
}

void Encode(std::string* o, const IncrRequest& m) {
  PutLengthPrefixed(o, m.key);
  PutI64(o, m.delta);
  PutBool(o, m.sync);
}
Status Decode(std::string_view in, IncrRequest* m) {
  TRY(GetLengthPrefixedString(&in, &m->key));
  TRY(GetI64(&in, &m->delta));
  TRY(GetBool(&in, &m->sync));
  return Status::OK();
}

void Encode(std::string* o, const IncrResponse& m) {
  PutRespHead(o, m.status_code, m.error, m.primary_hint);
  PutI64(o, m.value);
}
Status Decode(std::string_view in, IncrResponse* m) {
  TRY(GetRespHead(&in, &m->status_code, &m->error, &m->primary_hint));
  TRY(GetI64(&in, &m->value));
  return Status::OK();
}

void Encode(std::string* o, const StatusRequest& m) { PutBool(o, m.verbose); }
Status Decode(std::string_view in, StatusRequest* m) {
  TRY(GetBool(&in, &m->verbose));
  return Status::OK();
}

void Encode(std::string* o, const StatusResponse& m) {
  PutFixed32(o, static_cast<uint32_t>(m.status_code));
  PutLengthPrefixed(o, m.error);
  PutFixed64(o, m.node_id);
  PutLengthPrefixed(o, m.role);
  PutFixed64(o, m.epoch);
  PutFixed64(o, m.primary_id);
  PutLengthPrefixed(o, m.primary_address);
  PutFixed64(o, m.last_seq);
  PutLengthPrefixed(o, m.storage_stats);
  PutLengthPrefixed(o, m.repl_stats);
}
Status Decode(std::string_view in, StatusResponse* m) {
  uint32_t c = 0;
  TRY(GetFixed32(&in, &c));
  m->status_code = static_cast<int32_t>(c);
  TRY(GetLengthPrefixedString(&in, &m->error));
  TRY(GetFixed64(&in, &m->node_id));
  TRY(GetLengthPrefixedString(&in, &m->role));
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetFixed64(&in, &m->primary_id));
  TRY(GetLengthPrefixedString(&in, &m->primary_address));
  TRY(GetFixed64(&in, &m->last_seq));
  TRY(GetLengthPrefixedString(&in, &m->storage_stats));
  TRY(GetLengthPrefixedString(&in, &m->repl_stats));
  return Status::OK();
}

void Encode(std::string* o, const ReplicateReq& m) {
  PutFixed64(o, m.epoch);
  PutFixed64(o, m.primary_id);
  PutFixed64(o, m.seq);
  PutLengthPrefixed(o, m.data);
}
Status Decode(std::string_view in, ReplicateReq* m) {
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetFixed64(&in, &m->primary_id));
  TRY(GetFixed64(&in, &m->seq));
  TRY(GetLengthPrefixedString(&in, &m->data));
  return Status::OK();
}

void Encode(std::string* o, const ReplicateResp& m) {
  PutFixed64(o, m.epoch);
  PutBool(o, m.success);
  PutFixed64(o, m.backup_id);
  PutFixed64(o, m.applied_seq);
  PutFixed64(o, m.primary_id);
}
Status Decode(std::string_view in, ReplicateResp* m) {
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetBool(&in, &m->success));
  TRY(GetFixed64(&in, &m->backup_id));
  TRY(GetFixed64(&in, &m->applied_seq));
  TRY(GetFixed64(&in, &m->primary_id));
  return Status::OK();
}

void Encode(std::string* o, const HeartbeatReq& m) {
  PutFixed64(o, m.epoch);
  PutFixed64(o, m.primary_id);
  PutFixed64(o, m.primary_last_seq);
}
Status Decode(std::string_view in, HeartbeatReq* m) {
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetFixed64(&in, &m->primary_id));
  TRY(GetFixed64(&in, &m->primary_last_seq));
  return Status::OK();
}

void Encode(std::string* o, const HeartbeatResp& m) {
  PutFixed64(o, m.epoch);
  PutBool(o, m.ok);
  PutFixed64(o, m.backup_id);
  PutFixed64(o, m.applied_seq);
  PutFixed64(o, m.primary_id);
}
Status Decode(std::string_view in, HeartbeatResp* m) {
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetBool(&in, &m->ok));
  TRY(GetFixed64(&in, &m->backup_id));
  TRY(GetFixed64(&in, &m->applied_seq));
  TRY(GetFixed64(&in, &m->primary_id));
  return Status::OK();
}

void Encode(std::string* o, const CatchUpReq& m) {
  PutFixed64(o, m.epoch);
  PutFixed64(o, m.from_seq);
}
Status Decode(std::string_view in, CatchUpReq* m) {
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetFixed64(&in, &m->from_seq));
  return Status::OK();
}

void Encode(std::string* o, const CatchUpResp& m) {
  PutFixed64(o, m.epoch);
  PutBool(o, m.ok);
  PutFixed32(o, static_cast<uint32_t>(m.entries.size()));
  for (const auto& [seq, data] : m.entries) {
    PutFixed64(o, seq);
    PutLengthPrefixed(o, data);
  }
}
Status Decode(std::string_view in, CatchUpResp* m) {
  TRY(GetFixed64(&in, &m->epoch));
  TRY(GetBool(&in, &m->ok));
  uint32_t n = 0;
  TRY(GetFixed32(&in, &n));
  m->entries.clear();
  m->entries.reserve(n);
  for (uint32_t i = 0; i < n; i++) {
    uint64_t seq = 0;
    std::string data;
    TRY(GetFixed64(&in, &seq));
    TRY(GetLengthPrefixedString(&in, &data));
    m->entries.emplace_back(seq, std::move(data));
  }
  return Status::OK();
}

void Encode(std::string* o, const AdminRequest& m) {
  PutLengthPrefixed(o, m.command);
  PutLengthPrefixed(o, m.arg);
}
Status Decode(std::string_view in, AdminRequest* m) {
  TRY(GetLengthPrefixedString(&in, &m->command));
  TRY(GetLengthPrefixedString(&in, &m->arg));
  return Status::OK();
}

void Encode(std::string* o, const AdminResponse& m) {
  PutFixed32(o, static_cast<uint32_t>(m.status_code));
  PutLengthPrefixed(o, m.error);
  PutLengthPrefixed(o, m.result);
}
Status Decode(std::string_view in, AdminResponse* m) {
  uint32_t c = 0;
  TRY(GetFixed32(&in, &c));
  m->status_code = static_cast<int32_t>(c);
  TRY(GetLengthPrefixedString(&in, &m->error));
  TRY(GetLengthPrefixedString(&in, &m->result));
  return Status::OK();
}

}  // namespace codec
}  // namespace kvdb
