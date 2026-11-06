#include "concurrency/transaction.h"

#include <cstdlib>

#include "common/logger.h"

namespace kvdb {

Transaction::Transaction(TransactionManager* mgr, LsmStore* store, SequenceNumber snapshot,
                         const WriteOptions& wopts)
    : mgr_(mgr), store_(store), snapshot_(snapshot), wopts_(wopts) {}

Transaction::~Transaction() {
  if (!finished_) Rollback();
}

Status Transaction::Get(std::string_view key, std::string* value) {
  const std::string k(key);

  // Read-your-own-writes.
  auto w = write_set_.find(k);
  if (w != write_set_.end()) {
    if (w->second.is_delete) return Status::NotFound();
    *value = w->second.value;
    return Status::OK();
  }

  ReadOptions ropts;
  ropts.snapshot = snapshot_;
  SequenceNumber seq = 0;
  Status s = store_->GetWithSeq(ropts, key, value, &seq);

  if (read_set_.find(k) == read_set_.end()) {
    ReadVersion rv;
    rv.exists = s.ok();
    rv.value_digest = std::to_string(seq);
    read_set_.emplace(k, std::move(rv));
  }
  return s;
}

Status Transaction::Lock(std::string_view key) {
  std::string ignored;
  Status s = Get(key, &ignored);
  if (s.IsNotFound()) return Status::OK();
  return s;
}

void Transaction::Put(std::string_view key, std::string_view value) {
  WriteOp op;
  op.value.assign(value.data(), value.size());
  op.is_delete = false;
  write_set_[std::string(key)] = std::move(op);
}

void Transaction::Delete(std::string_view key) {
  WriteOp op;
  op.is_delete = true;
  write_set_[std::string(key)] = std::move(op);
}

Status Transaction::Commit() {
  if (finished_) return Status::InvalidArgument("transaction already finished");
  finished_ = true;
  return mgr_->CommitInternal(this);
}

void Transaction::Rollback() {
  finished_ = true;
  write_set_.clear();
  read_set_.clear();
}

// ---------------------------------------------------------------------------

std::unique_ptr<Transaction> TransactionManager::Begin(const WriteOptions& wopts) {
  return std::unique_ptr<Transaction>(
      new Transaction(this, store_, store_->LastSequence(), wopts));
}

Status TransactionManager::CommitInternal(Transaction* txn) {
  if (txn->write_set_.empty()) {
    // Read-only transactions are trivially serializable at their snapshot.
    commits_.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }

  std::lock_guard<std::mutex> lk(commit_mu_);

  // Validation: every key we read must still have the version we saw.
  ReadOptions latest;  // snapshot 0 == read the newest committed state
  for (const auto& [key, rv] : txn->read_set_) {
    std::string value;
    SequenceNumber seq = 0;
    Status s = store_->GetWithSeq(latest, key, &value, &seq);
    const bool exists_now = s.ok();
    if (!s.ok() && !s.IsNotFound()) return s;
    if (exists_now != rv.exists || std::to_string(seq) != rv.value_digest) {
      aborts_.fetch_add(1, std::memory_order_relaxed);
      return Status::Aborted("write-write conflict on key: " + key);
    }
  }

  WriteBatch batch;
  for (const auto& [key, op] : txn->write_set_) {
    if (op.is_delete) {
      batch.Delete(key);
    } else {
      batch.Put(key, op.value);
    }
  }

  Status s = store_->Write(txn->wopts_, &batch);
  if (s.ok()) commits_.fetch_add(1, std::memory_order_relaxed);
  return s;
}

Status TransactionManager::CompareAndSwap(std::string_view key, std::string_view expected,
                                          std::string_view new_value, bool expect_absent,
                                          bool* swapped, const WriteOptions& wopts) {
  *swapped = false;
  for (int attempt = 0; attempt < kMaxRetries; attempt++) {
    auto txn = Begin(wopts);
    std::string current;
    Status s = txn->Get(key, &current);
    if (!s.ok() && !s.IsNotFound()) return s;

    const bool absent = s.IsNotFound();
    if (expect_absent) {
      if (!absent) {
        txn->Rollback();
        return Status::OK();  // precondition failed; *swapped stays false
      }
    } else {
      if (absent || current != expected) {
        txn->Rollback();
        return Status::OK();
      }
    }

    txn->Put(key, new_value);
    s = txn->Commit();
    if (s.ok()) {
      *swapped = true;
      return Status::OK();
    }
    if (!s.IsAborted()) return s;
    retries_.fetch_add(1, std::memory_order_relaxed);  // conflict: try again
  }
  return Status::Aborted("compare-and-swap exceeded retry limit");
}

Status TransactionManager::Increment(std::string_view key, int64_t delta, int64_t* result,
                                     const WriteOptions& wopts) {
  for (int attempt = 0; attempt < kMaxRetries; attempt++) {
    auto txn = Begin(wopts);
    std::string current;
    Status s = txn->Get(key, &current);
    if (!s.ok() && !s.IsNotFound()) return s;

    int64_t value = 0;
    if (s.ok()) {
      char* end = nullptr;
      value = std::strtoll(current.c_str(), &end, 10);
      if (end == current.c_str() || *end != '\0') {
        return Status::InvalidArgument("value at key is not an integer");
      }
    }
    value += delta;

    txn->Put(key, std::to_string(value));
    s = txn->Commit();
    if (s.ok()) {
      *result = value;
      return Status::OK();
    }
    if (!s.IsAborted()) return s;
    retries_.fetch_add(1, std::memory_order_relaxed);
  }
  return Status::Aborted("increment exceeded retry limit");
}

}  // namespace kvdb
