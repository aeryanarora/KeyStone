#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/db.h"

namespace kvdb {

// Optimistic multi-key transactions over the LSM store.
//
// Why optimistic? Writes already funnel through a single group-commit queue, so
// pessimistic key locks would add a second contention point for very little
// benefit. Instead a transaction:
//   1. reads freely at a fixed snapshot, recording (key -> version) for each read;
//   2. buffers writes locally;
//   3. at commit, takes the commit mutex, re-checks every read key's version,
//      and applies the buffered writes as one atomic WriteBatch.
//
// Validation is what gives serializable behaviour: if any key we read changed
// between snapshot and commit, the transaction aborts and the caller retries.
// The commit mutex is held only for the validation + enqueue, never for user
// think-time, so read-heavy workloads scale.
class TransactionManager;

class Transaction {
 public:
  ~Transaction();
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  // Reads observe the transaction's own uncommitted writes first, then the
  // snapshot taken when the transaction began.
  Status Get(std::string_view key, std::string* value);
  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);

  // Records a read-set entry without returning data. Useful to assert
  // "this key must not change" (e.g. a guard key).
  Status Lock(std::string_view key);

  // Returns Aborted if another writer touched something we read.
  Status Commit();
  void Rollback();

  SequenceNumber snapshot() const { return snapshot_; }
  size_t num_reads() const { return read_set_.size(); }
  size_t num_writes() const { return write_set_.size(); }

 private:
  friend class TransactionManager;
  Transaction(TransactionManager* mgr, LsmStore* store, SequenceNumber snapshot,
              const WriteOptions& wopts);

  struct WriteOp {
    std::string value;
    bool is_delete = false;
  };

  // Version fingerprint of a key at read time. `exists` distinguishes
  // "absent" from "present with version v" so that phantom inserts on a key we
  // read as missing are also caught.
  struct ReadVersion {
    bool exists = false;
    std::string value_digest;
  };

  TransactionManager* mgr_;
  LsmStore* store_;
  SequenceNumber snapshot_;
  WriteOptions wopts_;
  std::unordered_map<std::string, ReadVersion> read_set_;
  std::unordered_map<std::string, WriteOp> write_set_;
  bool finished_ = false;
};

class TransactionManager {
 public:
  explicit TransactionManager(LsmStore* store) : store_(store) {}

  std::unique_ptr<Transaction> Begin(const WriteOptions& wopts = WriteOptions());

  // ---- Single-key atomic primitives, all built on the same machinery ----

  // Sets `key` to `new_value` only if its current value equals `expected`.
  // An empty `expected` means "only if the key does not exist".
  Status CompareAndSwap(std::string_view key, std::string_view expected,
                        std::string_view new_value, bool expect_absent, bool* swapped,
                        const WriteOptions& wopts = WriteOptions());

  // Atomically adds `delta` to the integer stored at `key` (absent == 0).
  Status Increment(std::string_view key, int64_t delta, int64_t* result,
                   const WriteOptions& wopts = WriteOptions());

  uint64_t commits() const { return commits_; }
  uint64_t aborts() const { return aborts_; }
  uint64_t retries() const { return retries_; }

 private:
  friend class Transaction;

  Status CommitInternal(Transaction* txn);

  LsmStore* store_;
  std::mutex commit_mu_;  // serialises validation+apply, nothing else
  std::atomic<uint64_t> commits_{0};
  std::atomic<uint64_t> aborts_{0};
  std::atomic<uint64_t> retries_{0};

  static constexpr int kMaxRetries = 100;
};

}  // namespace kvdb
