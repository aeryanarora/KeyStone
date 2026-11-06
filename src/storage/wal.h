#pragma once

#include <functional>
#include <memory>
#include <string>

#include "common/file_util.h"
#include "common/status.h"

namespace kvdb {

// Write-ahead log with LevelDB-style block framing.
//
// The file is a sequence of 32 KiB blocks; records are chopped across block
// boundaries with FIRST/MIDDLE/LAST fragments so that a scanner can always
// resynchronise at the next block after corruption.
//
//   record header := crc32c(masked, 4B) | length (2B) | type (1B)
//
// A crash can only ever truncate the tail; the CRC catches the partially
// written record and recovery stops cleanly there.
namespace walfmt {
enum RecordType : uint8_t {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
};
static constexpr int kMaxRecordType = kLastType;
static constexpr size_t kBlockSize = 32768;
static constexpr size_t kHeaderSize = 4 + 2 + 1;
}  // namespace walfmt

class WalWriter {
 public:
  WalWriter() = default;
  ~WalWriter();

  Status Open(const std::string& path, bool append = false);
  Status AddRecord(std::string_view data);
  Status Flush();
  Status Sync();
  Status Close();

  uint64_t FileSize() const { return file_ ? file_->size() : 0; }
  const std::string& path() const { return path_; }
  bool is_open() const { return file_ != nullptr; }

 private:
  Status EmitPhysicalRecord(walfmt::RecordType t, const char* ptr, size_t length);

  std::unique_ptr<WritableFile> file_;
  std::string path_;
  size_t block_offset_ = 0;
};

class WalReader {
 public:
  WalReader() = default;

  Status Open(const std::string& path);

  // Reads the next logical record. Returns:
  //   OK           -> *record valid
  //   NotFound     -> clean end of file
  //   Corruption   -> truncated/corrupt tail (caller decides tolerance)
  Status ReadRecord(std::string* record);

  uint64_t last_good_offset() const { return last_good_offset_; }

 private:
  Status ReadPhysicalRecord(std::string_view* result, walfmt::RecordType* type);

  SequentialFile file_;
  std::string backing_store_;
  std::string_view buffer_;
  bool eof_ = false;
  uint64_t last_good_offset_ = 0;
  std::string scratch_;
};

}  // namespace kvdb
