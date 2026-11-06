#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/wal.h"

namespace kvdb {

// Durable, append-only log of (seq, data) entries plus the node's current
// (epoch, primary_id) hard state. Reuses the WAL's block-framed CRC storage.
// No compaction/snapshotting in this version: the log grows without bound,
// which is fine for a learning project but is the first thing to fix before
// this could run for a long time in production.
class ReplicationLog {
 public:
  Status Open(const std::string& dir, bool sync_on_append);

  uint64_t last_seq() const;
  Status EntryAt(uint64_t seq, std::string* data) const;
  Status Append(uint64_t seq, const std::string& data);
  Status Range(uint64_t from_seq, size_t max_entries,
              std::vector<std::pair<uint64_t, std::string>>* out) const;

  Status SaveEpoch(uint64_t epoch, uint64_t primary_id);
  uint64_t epoch() const;
  uint64_t primary_id() const;

 private:
  Status LoadEpoch();
  std::string LogPath() const { return dir_ + "/repl.log"; }
  std::string EpochPath() const { return dir_ + "/repl.epoch"; }

  mutable std::mutex mu_;
  std::string dir_;
  bool sync_on_append_ = true;
  std::vector<std::string> entries_;  // entries_[i] is seq i+1
  std::unique_ptr<WalWriter> writer_;
  uint64_t epoch_ = 0;
  uint64_t primary_id_ = 0;
};

}  // namespace kvdb
