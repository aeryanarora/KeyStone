#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/cache.h"
#include "common/status.h"
#include "storage/iterator.h"
#include "storage/memtable.h"
#include "storage/options.h"
#include "storage/sstable.h"
#include "storage/wal.h"
#include "storage/write_batch.h"

namespace kvdb {

struct DbStats {
  uint64_t num_writes = 0;
  uint64_t num_reads = 0;
  uint64_t num_batches = 0;       // WAL appends (< num_writes when grouped)
  uint64_t num_flushes = 0;
  uint64_t num_compactions = 0;
  uint64_t bytes_written = 0;
  uint64_t bytes_read = 0;
  uint64_t bytes_compacted = 0;
  uint64_t wal_syncs = 0;
  uint64_t write_stalls = 0;
  uint64_t memtable_bytes = 0;
  uint64_t immutable_bytes = 0;
  uint64_t last_sequence = 0;
  double cache_hit_rate = 0.0;
  std::vector<int> files_per_level;
  std::vector<uint64_t> bytes_per_level;

  std::string ToString() const;
};

// A single SSTable plus its metadata. Held by shared_ptr so that compaction can
// retire a file while readers are still using it: the file is only unlinked
// once the last reference drops.
struct TableEntry {
  FileMetaData meta;
  std::shared_ptr<SSTableReader> reader;
};

// An immutable snapshot of the level structure. Installing a compaction result
// is a single pointer swap, so readers never block on compaction.
struct Version {
  std::vector<std::vector<std::shared_ptr<TableEntry>>> levels;
  uint64_t TotalBytes(int level) const;
  size_t NumFiles(int level) const {
    return (level < static_cast<int>(levels.size())) ? levels[static_cast<size_t>(level)].size() : 0;
  }
};

// LSM-tree storage engine.
//
//   Put/Delete -> WAL (durability) -> MemTable (in-memory, sorted)
//                                       |
//                                    flush -> L0 SSTable
//                                       |
//                                  compaction -> L1..Ln
//
// Thread safety: all public methods are safe to call concurrently.
class LsmStore {
 public:
  static Status Open(const Options& options, std::unique_ptr<LsmStore>* store);

  ~LsmStore();
  LsmStore(const LsmStore&) = delete;
  LsmStore& operator=(const LsmStore&) = delete;

  Status Put(const WriteOptions& opts, std::string_view key, std::string_view value);
  Status Delete(const WriteOptions& opts, std::string_view key);
  Status Write(const WriteOptions& opts, WriteBatch* batch);

  Status Get(const ReadOptions& opts, std::string_view key, std::string* value);
  // Also reports the sequence number of the version that answered the read;
  // optimistic transactions use this for conflict validation.
  Status GetWithSeq(const ReadOptions& opts, std::string_view key, std::string* value,
                    SequenceNumber* seq);

  // Ordered range scan over [start, end). An empty `end` means "to the end".
  Status Scan(const ReadOptions& opts, std::string_view start, std::string_view end, size_t limit,
              std::vector<std::pair<std::string, std::string>>* out);

  SequenceNumber LastSequence() const { return last_sequence_.load(std::memory_order_acquire); }

  // Forces the active memtable out to an L0 SSTable and waits for it.
  Status Flush();
  // Forces compaction of every level until no level exceeds its budget.
  Status CompactRange();
  // Blocks until no background flush/compaction is in flight.
  void WaitForBackgroundWork();

  DbStats GetStats() const;
  const Options& options() const { return options_; }

  Status Close();

  // Test hook: simulates a crash by dropping in-memory state without a clean
  // shutdown (no final flush, no manifest rewrite).
  void SimulateCrash();

 private:
  explicit LsmStore(const Options& options);

  // --- lifecycle ---
  Status Recover();
  Status LoadManifest(bool* found);
  Status WriteManifest();  // caller holds mu_
  Status ReplayWal(uint64_t log_number, uint64_t* max_sequence);
  Status RollNewWal();     // caller holds mu_

  // --- write path ---
  struct Writer {
    WriteBatch* batch = nullptr;
    bool sync = false;
    bool done = false;
    Status status;
    std::condition_variable cv;
  };
  Status WriteImpl(const WriteOptions& opts, WriteBatch* batch);
  Status MakeRoomForWrite(std::unique_lock<std::mutex>& lk);
  WriteBatch* BuildGroupBatch(Writer* leader, Writer** last_writer);

  // --- background work ---
  void BackgroundLoop();
  void MaybeScheduleWork();  // caller holds mu_
  Status FlushImmutable(std::unique_lock<std::mutex>& lk);
  Status BuildTableFromMemTable(const std::shared_ptr<const MemTable>& mem, FileMetaData* meta);
  bool NeedsCompaction() const;  // caller holds mu_
  Status DoCompaction(std::unique_lock<std::mutex>& lk);
  int PickCompactionLevel() const;  // caller holds mu_
  void RemoveObsoleteFiles();       // caller holds mu_
  uint64_t MaxBytesForLevel(int level) const;

  // --- helpers ---
  std::string WalPath(uint64_t number) const;
  std::string TablePath(uint64_t number) const;
  std::string ManifestPath() const;
  Status OpenTable(const FileMetaData& meta, std::shared_ptr<TableEntry>* out);
  std::shared_ptr<Version> CurrentVersion() const;
  Status GetFromVersion(const std::shared_ptr<Version>& v, std::string_view key,
                        SequenceNumber snapshot, std::string* value, SequenceNumber* seq,
                        bool* found);
  std::unique_ptr<InternalIterator> NewInternalIterator(
      const std::shared_ptr<Version>& v, const std::shared_ptr<const MemTable>& mem,
      const std::shared_ptr<const MemTable>& imm) const;

  Options options_;
  std::unique_ptr<BlockCache> block_cache_;

  mutable std::mutex mu_;
  std::condition_variable bg_cv_;       // signals background work available/done
  std::deque<Writer*> writers_;         // group-commit queue

  std::shared_ptr<MemTable> mem_;
  std::shared_ptr<MemTable> imm_;       // being flushed
  std::shared_ptr<Version> current_;

  std::unique_ptr<WalWriter> wal_;
  uint64_t log_number_ = 0;
  uint64_t next_file_number_ = 1;
  std::atomic<SequenceNumber> last_sequence_{0};

  std::vector<std::shared_ptr<TableEntry>> obsolete_;
  std::vector<std::string> compact_pointer_;  // per level: resume key for round-robin

  std::thread bg_thread_;
  bool bg_work_scheduled_ = false;
  bool bg_running_ = false;
  bool shutting_down_ = false;
  bool crashed_ = false;
  Status bg_error_;

  // Statistics (plain counters under mu_ where cheap, atomics on hot paths).
  std::atomic<uint64_t> stat_writes_{0};
  std::atomic<uint64_t> stat_reads_{0};
  std::atomic<uint64_t> stat_batches_{0};
  std::atomic<uint64_t> stat_flushes_{0};
  std::atomic<uint64_t> stat_compactions_{0};
  std::atomic<uint64_t> stat_bytes_written_{0};
  std::atomic<uint64_t> stat_bytes_read_{0};
  std::atomic<uint64_t> stat_bytes_compacted_{0};
  std::atomic<uint64_t> stat_wal_syncs_{0};
  std::atomic<uint64_t> stat_write_stalls_{0};
};

}  // namespace kvdb
