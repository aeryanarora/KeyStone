#include <gtest/gtest.h>

#include "net/codec.h"

using namespace kvdb;

TEST(Codec, FrameRoundTrip) {
  PutRequest req;
  req.key = "hello";
  req.value = "world";
  req.sync = true;

  std::string frame = codec::BuildFrame(MsgType::kPutReq, 42, req);
  bool error = false;
  size_t sz = codec::PeekFrameSize(frame, &error);
  ASSERT_FALSE(error);
  EXPECT_EQ(sz, frame.size());

  RequestHeader header;
  std::string_view body;
  ASSERT_TRUE(codec::DecodeHeader(
                  std::string_view(frame.data() + codec::kFrameLenSize,
                                   frame.size() - codec::kFrameLenSize),
                  &header, &body)
                 .ok());
  EXPECT_EQ(header.type, MsgType::kPutReq);
  EXPECT_EQ(header.request_id, 42u);

  PutRequest decoded;
  ASSERT_TRUE(codec::Decode(body, &decoded).ok());
  EXPECT_EQ(decoded.key, "hello");
  EXPECT_EQ(decoded.value, "world");
  EXPECT_TRUE(decoded.sync);
}

TEST(Codec, PeekFrameSizeIncompleteReturnsZero) {
  std::string partial = "\x05\x00";  // only 2 of the 4 length-prefix bytes
  bool error = false;
  EXPECT_EQ(codec::PeekFrameSize(partial, &error), 0u);
  EXPECT_FALSE(error);
}

TEST(Codec, PeekFrameSizeOversizedIsError) {
  std::string s;
  uint32_t huge = codec::kMaxFrameSize + 1;
  s.append(reinterpret_cast<char*>(&huge), 4);
  bool error = false;
  codec::PeekFrameSize(s, &error);
  EXPECT_TRUE(error);
}

TEST(Codec, GetResponseRoundTrip) {
  GetResponse resp;
  resp.status_code = 0;
  resp.found = true;
  resp.value = "some-value";
  std::string encoded;
  codec::Encode(&encoded, resp);

  GetResponse decoded;
  ASSERT_TRUE(codec::Decode(encoded, &decoded).ok());
  EXPECT_EQ(decoded.found, true);
  EXPECT_EQ(decoded.value, "some-value");
}

TEST(Codec, ScanResponseRoundTripWithManyEntries) {
  ScanResponse resp;
  for (int i = 0; i < 100; i++) {
    resp.entries.emplace_back("key" + std::to_string(i), "value" + std::to_string(i));
  }
  std::string encoded;
  codec::Encode(&encoded, resp);

  ScanResponse decoded;
  ASSERT_TRUE(codec::Decode(encoded, &decoded).ok());
  ASSERT_EQ(decoded.entries.size(), 100u);
  EXPECT_EQ(decoded.entries[50].first, "key50");
  EXPECT_EQ(decoded.entries[50].second, "value50");
}

TEST(Codec, ReplicateReqRoundTrip) {
  ReplicateReq req;
  req.epoch = 5;
  req.primary_id = 2;
  req.seq = 11;
  req.data = "\x01k=v";
  std::string encoded;
  codec::Encode(&encoded, req);

  ReplicateReq decoded;
  ASSERT_TRUE(codec::Decode(encoded, &decoded).ok());
  EXPECT_EQ(decoded.epoch, 5u);
  EXPECT_EQ(decoded.primary_id, 2u);
  EXPECT_EQ(decoded.seq, 11u);
  EXPECT_EQ(decoded.data, "\x01k=v");
}

TEST(Codec, HeartbeatRoundTrip) {
  HeartbeatReq req;
  req.epoch = 3;
  req.primary_id = 1;
  req.primary_last_seq = 42;
  std::string encoded;
  codec::Encode(&encoded, req);

  HeartbeatReq decoded;
  ASSERT_TRUE(codec::Decode(encoded, &decoded).ok());
  EXPECT_EQ(decoded.epoch, 3u);
  EXPECT_EQ(decoded.primary_last_seq, 42u);

  HeartbeatResp resp;
  resp.epoch = 3;
  resp.ok = true;
  resp.backup_id = 2;
  resp.applied_seq = 42;
  resp.primary_id = 1;
  std::string encoded_resp;
  codec::Encode(&encoded_resp, resp);

  HeartbeatResp decoded_resp;
  ASSERT_TRUE(codec::Decode(encoded_resp, &decoded_resp).ok());
  EXPECT_TRUE(decoded_resp.ok);
  EXPECT_EQ(decoded_resp.applied_seq, 42u);
}

TEST(Codec, CatchUpRoundTripWithManyEntries) {
  CatchUpResp resp;
  resp.epoch = 1;
  resp.ok = true;
  for (uint64_t i = 0; i < 20; i++) resp.entries.emplace_back(i + 1, "data" + std::to_string(i));
  std::string encoded;
  codec::Encode(&encoded, resp);

  CatchUpResp decoded;
  ASSERT_TRUE(codec::Decode(encoded, &decoded).ok());
  ASSERT_EQ(decoded.entries.size(), 20u);
  EXPECT_EQ(decoded.entries[5].first, 6u);
  EXPECT_EQ(decoded.entries[5].second, "data5");
}

TEST(Codec, MalformedBodyReturnsCorruption) {
  GetRequest decoded;
  EXPECT_FALSE(codec::Decode(std::string_view("\xff\xff"), &decoded).ok());
}

TEST(Codec, StatusCodeRoundTrip) {
  Status original = Status::NotLeader("127.0.0.1:9000");
  int32_t code = codec::StatusToCode(original);
  Status restored = codec::CodeToStatus(code, original.message(), original.leader_hint());
  EXPECT_TRUE(restored.IsNotLeader());
  EXPECT_EQ(restored.leader_hint(), "127.0.0.1:9000");
}
