#pragma once

#include <string>
#include <string_view>

#include "common/status.h"
#include "net/messages.h"

namespace kvdb {
namespace codec {

static constexpr uint32_t kMaxFrameSize = 64u * 1024 * 1024;
static constexpr size_t kFrameLenSize = 4;

void StartFrame(std::string* out, const RequestHeader& header);
void FinishFrame(std::string* out);

size_t PeekFrameSize(std::string_view buffer, bool* error);
Status DecodeHeader(std::string_view frame_payload, RequestHeader* header, std::string_view* rest);

int32_t StatusToCode(const Status& s);
Status CodeToStatus(int32_t code, const std::string& msg, const std::string& primary_hint);

#define KVDB_DECLARE_CODEC(Type)                                     \
  void Encode(std::string* out, const Type& msg);                    \
  Status Decode(std::string_view in, Type* msg);

KVDB_DECLARE_CODEC(GetRequest)
KVDB_DECLARE_CODEC(GetResponse)
KVDB_DECLARE_CODEC(PutRequest)
KVDB_DECLARE_CODEC(DeleteRequest)
KVDB_DECLARE_CODEC(WriteResponse)
KVDB_DECLARE_CODEC(ScanRequest)
KVDB_DECLARE_CODEC(ScanResponse)
KVDB_DECLARE_CODEC(BatchRequest)
KVDB_DECLARE_CODEC(CasRequest)
KVDB_DECLARE_CODEC(CasResponse)
KVDB_DECLARE_CODEC(IncrRequest)
KVDB_DECLARE_CODEC(IncrResponse)
KVDB_DECLARE_CODEC(StatusRequest)
KVDB_DECLARE_CODEC(StatusResponse)
KVDB_DECLARE_CODEC(ReplicateReq)
KVDB_DECLARE_CODEC(ReplicateResp)
KVDB_DECLARE_CODEC(HeartbeatReq)
KVDB_DECLARE_CODEC(HeartbeatResp)
KVDB_DECLARE_CODEC(CatchUpReq)
KVDB_DECLARE_CODEC(CatchUpResp)
KVDB_DECLARE_CODEC(AdminRequest)
KVDB_DECLARE_CODEC(AdminResponse)

#undef KVDB_DECLARE_CODEC

template <typename T>
std::string BuildFrame(MsgType type, uint64_t request_id, const T& msg) {
  std::string out;
  RequestHeader h;
  h.type = type;
  h.request_id = request_id;
  StartFrame(&out, h);
  Encode(&out, msg);
  FinishFrame(&out);
  return out;
}

}  // namespace codec
}  // namespace kvdb
