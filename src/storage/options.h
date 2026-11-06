#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kvdb {

struct Options {
  std::string data_dir = "./kvdb-data";

  // Flush the memtable once it reaches this size.
  size_t write_buffer_size = 4ull * 1024 * 1024;

  // Target size for SSTable data blocks.
  size_t block_size = 4096;

  // Bloom filter bits per key. 10 bits ~= 1% false positive rate.
  int bloom_bits_per_key = 10;

  // Total size of the (sharded) SSTable block cache. 0 disables caching.
  size_t block_cache_bytes = 8ull * 1024 * 1024;

  // Compact L0 into L1 once this many L0 files have accumulated. L0 files have
  // overlapping key ranges, so every extra file costs every read.
  int l0_compaction_trigger = 4;

  // Level i is allowed to hold l1_size_bytes * level_size_multiplier^(i-1).
  uint64_t l1_size_bytes = 10ull * 1024 * 1024;
  int level_size_multiplier = 10;
  int max_levels = 7;

  // Run flush/compaction on a background thread. Turning this off makes tests
  // deterministic.
  bool background_compaction = true;

  // fsync the WAL on every write. The single biggest throughput/durability
  // knob in the system; the benchmark suite sweeps both settings.
  bool sync_writes = false;

  // Group commit: a writer that finds the write lock busy joins a queue, and
  // the current leader merges the queued batches into one WAL append + fsync.
  bool enable_group_commit = true;
  size_t max_group_commit_batch = 64;

  // Create the data directory if it does not exist.
  bool create_if_missing = true;

  // Fail Open() if the data directory already contains a database.
  bool error_if_exists = false;

  // Tolerate a corrupt/truncated WAL tail (the normal case after a crash)
  // instead of refusing to open.
  bool tolerate_wal_tail_corruption = true;
};

struct WriteOptions {
  bool sync = false;  // overridden by Options::sync_writes when true
};

struct ReadOptions {
  // 0 means "read the latest committed sequence".
  uint64_t snapshot = 0;
  bool fill_cache = true;
};

}  // namespace kvdb
