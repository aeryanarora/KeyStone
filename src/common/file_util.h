#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace kvdb {

// Thin, testable wrappers over POSIX file APIs. Everything that must survive a
// crash goes through Fsync{File,Dir} explicitly -- durability is never implicit.
namespace fileutil {

bool FileExists(const std::string& path);
Status CreateDirRecursive(const std::string& path);
Status ListDir(const std::string& path, std::vector<std::string>* out);
Status DeleteFile(const std::string& path);
Status DeleteDirRecursive(const std::string& path);
Status GetFileSize(const std::string& path, uint64_t* size);
Status ReadFileToString(const std::string& path, std::string* out);
// Writes to `path`.tmp then rename()s: readers observe either the old or the
// new contents, never a partial write.
Status WriteStringToFileAtomic(const std::string& path, const std::string& data,
                               bool sync_dir = true);
Status FsyncFile(int fd);
Status FsyncDir(const std::string& dir);
Status RenameFile(const std::string& from, const std::string& to);

std::string JoinPath(const std::string& a, const std::string& b);
std::string DirName(const std::string& path);
std::string BaseName(const std::string& path);

}  // namespace fileutil

// Buffered append-only writer with explicit Flush/Sync separation.
class WritableFile {
 public:
  WritableFile() = default;
  ~WritableFile();
  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  Status Open(const std::string& path, bool append = false);
  Status Append(const char* data, size_t n);
  Status Append(const std::string& s) { return Append(s.data(), s.size()); }
  Status Flush();  // buffer -> kernel
  Status Sync();   // kernel -> disk
  Status Close();

  uint64_t size() const { return size_; }
  const std::string& path() const { return path_; }
  bool is_open() const { return fd_ >= 0; }

 private:
  int fd_ = -1;
  std::string path_;
  std::string buf_;
  uint64_t size_ = 0;
  static constexpr size_t kBufferSize = 64 * 1024;
};

// Random-access reader used by the SSTable reader.
class RandomAccessFile {
 public:
  RandomAccessFile() = default;
  ~RandomAccessFile();
  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  Status Open(const std::string& path);
  // Reads exactly n bytes at offset into *result (resized to n).
  Status Read(uint64_t offset, size_t n, std::string* result) const;
  Status Close();
  uint64_t size() const { return size_; }

 private:
  int fd_ = -1;
  uint64_t size_ = 0;
  std::string path_;
};

// Sequential reader used by WAL / Raft log replay.
class SequentialFile {
 public:
  SequentialFile() = default;
  ~SequentialFile();
  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(const SequentialFile&) = delete;

  Status Open(const std::string& path);
  // Reads up to n bytes; short read (result->size() < n) signals EOF.
  Status Read(size_t n, std::string* result);
  Status Skip(uint64_t n);
  uint64_t offset() const { return offset_; }
  uint64_t size() const { return size_; }

 private:
  int fd_ = -1;
  uint64_t offset_ = 0;
  uint64_t size_ = 0;
};

}  // namespace kvdb
