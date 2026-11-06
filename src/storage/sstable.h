#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/cache.h"
#include "common/file_util.h"
#include "common/status.h"
#include "storage/bloom.h"
#include "storage/format.h"

namespace kvdb {

// On-disk sorted table. Immutable once written, which is what lets readers run
// completely lock-free while compaction rewrites other files underneath.
//
//   +-----------------+
//   | data block 0    |  entries: varint(klen) key varint(vlen) value
//   | ...             |  each block followed by a 4-byte masked CRC32C
//   | data block N    |
//   +-----------------+
//   | filter block    |  bloom filter over the user keys in this file
//   +-----------------+
//   | index block     |  (last_key_of_block, offset, size) per data block
//   +-----------------+
//   | footer (40B)    |  index handle | filter handle | magic
//   +-----------------+
namespace sstfmt {
static constexpr uint64_t kMagic = 0x6b76646273737400ull;  // "kvdbsst\0"
static constexpr size_t kFooterSize = 40;
static constexpr size_t kBlockTrailerSize = 4;  // CRC32C
}  // namespace sstfmt

struct BlockHandle {
  uint64_t offset = 0;
  uint64_t size = 0;
};

// Metadata the LSM keeps in the manifest for every live SSTable.
struct FileMetaData {
  uint64_t number = 0;
  int level = 0;
  uint64_t file_size = 0;
  std::string smallest;  // smallest internal key
  std::string largest;   // largest internal key
  SequenceNumber smallest_seq = 0;
  SequenceNumber largest_seq = 0;
  uint64_t num_entries = 0;
};

class SSTableBuilder {
 public:
  explicit SSTableBuilder(size_t block_size = 4096, int bloom_bits_per_key = 10);
  ~SSTableBuilder();

  Status Open(const std::string& path);
  // REQUIRES: keys are added in strictly increasing internal-key order.
  Status Add(std::string_view internal_key, std::string_view value);
  Status Finish();
  Status Abandon();

  uint64_t NumEntries() const { return num_entries_; }
  uint64_t FileSize() const { return offset_; }
  const std::string& smallest_key() const { return smallest_; }
  const std::string& largest_key() const { return largest_; }
  SequenceNumber smallest_seq() const { return smallest_seq_; }
  SequenceNumber largest_seq() const { return largest_seq_; }

 private:
  Status FlushDataBlock();
  Status WriteRawBlock(const std::string& contents, BlockHandle* handle);

  size_t block_size_;
  BloomFilter bloom_;
  std::unique_ptr<WritableFile> file_;
  std::string path_;
  std::string block_buf_;
  std::string index_buf_;
  std::vector<std::string> filter_keys_;
  std::string last_key_;
  std::string smallest_;
  std::string largest_;
  SequenceNumber smallest_seq_ = kMaxSequenceNumber;
  SequenceNumber largest_seq_ = 0;
  uint64_t offset_ = 0;
  uint64_t num_entries_ = 0;
  bool closed_ = false;
};

class SSTableReader {
 public:
  SSTableReader() = default;

  // `cache` may be null (no block caching).
  Status Open(const std::string& path, BlockCache* cache = nullptr, uint64_t file_number = 0);

  // Point lookup for the newest version of `user_key` visible at `snapshot`.
  // Returns true if this table resolved the key (value or tombstone).
  bool Get(std::string_view user_key, SequenceNumber snapshot, std::string* value, Status* s,
           SequenceNumber* seq = nullptr) const;

  uint64_t file_size() const { return file_.size(); }
  const std::string& path() const { return path_; }

  // Forward iterator over all internal keys in the file, in sorted order.
  class Iterator {
   public:
    explicit Iterator(const SSTableReader* table) : table_(table) {}

    bool Valid() const { return valid_; }
    void SeekToFirst();
    void Seek(std::string_view internal_key);
    void Next();
    std::string_view internal_key() const { return key_; }
    std::string_view value() const { return value_; }
    const Status& status() const { return status_; }

   private:
    bool LoadBlock(size_t index);
    bool ParseCurrentEntry();

    const SSTableReader* table_;
    size_t block_index_ = 0;
    std::string block_data_;
    size_t pos_ = 0;
    std::string_view key_;
    std::string_view value_;
    bool valid_ = false;
    Status status_;
  };

  std::unique_ptr<Iterator> NewIterator() const { return std::make_unique<Iterator>(this); }

  size_t num_data_blocks() const { return index_.size(); }

 private:
  friend class Iterator;

  struct IndexEntry {
    std::string last_key;  // largest internal key in the block
    BlockHandle handle;
  };

  Status ReadBlock(const BlockHandle& handle, std::string* out) const;

  RandomAccessFile file_;
  std::string path_;
  std::vector<IndexEntry> index_;
  std::string filter_;
  BlockCache* cache_ = nullptr;
  uint64_t file_number_ = 0;
};

}  // namespace kvdb
