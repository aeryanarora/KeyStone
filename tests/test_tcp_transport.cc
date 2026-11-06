#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "net/tcp_client.h"
#include "net/tcp_server.h"

using namespace kvdb;

namespace {
// Minimal echo-style server: replies to a PutReq with a WriteResponse whose
// error field echoes the request's key, so the test can assert round-trip
// correctness through the whole framing + dispatch + threadpool pipeline.
class TestServer {
 public:
  TestServer() : server_("127.0.0.1", 0, 1, 2) {
    server_.SetMessageHandler([](const ConnectionPtr& conn, const RequestHeader& header,
                                 std::string_view body) {
      PutRequest req;
      WriteResponse resp;
      if (codec::Decode(body, &req).ok()) {
        resp.status_code = 0;
        resp.error = req.key;  // echo for verification
      } else {
        resp.status_code = 1;
      }
      conn->Send(codec::BuildFrame(MsgType::kPutResp, header.request_id, resp));
    });
  }
  Status Start() { return server_.Start(); }
  uint16_t port() const { return server_.port(); }
  TcpServer server_;
};
}  // namespace

TEST(TcpTransport, SingleRequestResponse) {
  TestServer server;
  ASSERT_TRUE(server.Start().ok());

  TcpClient client;
  ASSERT_TRUE(client.Connect("127.0.0.1:" + std::to_string(server.port())).ok());

  PutRequest req;
  req.key = "hello";
  req.value = "world";
  WriteResponse resp;
  ASSERT_TRUE(client.Call(MsgType::kPutReq, req, &resp).ok());
  EXPECT_EQ(resp.status_code, 0);
  EXPECT_EQ(resp.error, "hello");
}

TEST(TcpTransport, ManySequentialRequestsOnOneConnection) {
  TestServer server;
  ASSERT_TRUE(server.Start().ok());
  TcpClient client;
  ASSERT_TRUE(client.Connect("127.0.0.1:" + std::to_string(server.port())).ok());

  for (int i = 0; i < 200; i++) {
    PutRequest req;
    req.key = "key" + std::to_string(i);
    WriteResponse resp;
    ASSERT_TRUE(client.Call(MsgType::kPutReq, req, &resp).ok());
    EXPECT_EQ(resp.error, req.key);
  }
}

TEST(TcpTransport, ManyConcurrentClients) {
  TestServer server;
  ASSERT_TRUE(server.Start().ok());
  const int kClients = 20;
  const int kRequestsPerClient = 50;
  std::atomic<int> successes{0};

  std::vector<std::thread> threads;
  for (int c = 0; c < kClients; c++) {
    threads.emplace_back([&server, &successes, c] {
      TcpClient client;
      if (!client.Connect("127.0.0.1:" + std::to_string(server.port())).ok()) return;
      for (int i = 0; i < kRequestsPerClient; i++) {
        PutRequest req;
        req.key = "c" + std::to_string(c) + "_" + std::to_string(i);
        WriteResponse resp;
        if (client.Call(MsgType::kPutReq, req, &resp).ok() && resp.error == req.key) {
          successes.fetch_add(1);
        }
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(successes.load(), kClients * kRequestsPerClient);
}

TEST(TcpTransport, ClientReconnectsAfterDisconnect) {
  TestServer server;
  ASSERT_TRUE(server.Start().ok());
  TcpClient client;
  ASSERT_TRUE(client.Connect("127.0.0.1:" + std::to_string(server.port())).ok());
  client.Disconnect();
  ASSERT_TRUE(client.Connect("127.0.0.1:" + std::to_string(server.port())).ok());

  PutRequest req;
  req.key = "after-reconnect";
  WriteResponse resp;
  ASSERT_TRUE(client.Call(MsgType::kPutReq, req, &resp).ok());
  EXPECT_EQ(resp.error, "after-reconnect");
}

TEST(TcpTransport, ConnectToClosedPortFails) {
  TcpClient client;
  // Nothing listens on this high, essentially-guaranteed-free port.
  Status s = client.Connect("127.0.0.1:1", 500);
  EXPECT_FALSE(s.ok());
}
