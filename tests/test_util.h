#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <string>

#include "common/file_util.h"

namespace kvdb {
namespace testutil {

// Unique per-test scratch directory under /tmp, cleaned up on destruction so
// tests can run in parallel without colliding and don't leave junk behind.
class TempDir {
 public:
  explicit TempDir(const std::string& prefix) {
    path_ = "/tmp/kvdb_test_" + prefix + "_" + std::to_string(::getpid()) + "_" +
            std::to_string(reinterpret_cast<uintptr_t>(this));
    fileutil::DeleteDirRecursive(path_);
    fileutil::CreateDirRecursive(path_);
  }
  ~TempDir() { fileutil::DeleteDirRecursive(path_); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

inline int FreeTcpPort() {
  // Port 0 lets the OS pick; the server reports the actual bound port after
  // Start(), but some callers (peer maps) need a port before construction.
  // Bind a throwaway socket to reserve one likely-free ephemeral port.
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

}  // namespace testutil
}  // namespace kvdb
