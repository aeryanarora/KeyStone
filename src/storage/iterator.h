#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/format.h"
#include "storage/memtable.h"
#include "storage/sstable.h"

namespace kvdb {

// Uniform view over "a sorted stream of internal keys", implemented by the
// memtable, by SSTables, and by the merging iterator itself.
class InternalIterator {
 public:
  virtual ~InternalIterator() = default;
  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void Seek(std::string_view internal_key) = 0;
  virtual void Next() = 0;
  virtual std::string_view internal_key() const = 0;
  virtual std::string_view value() const = 0;
  virtual Status status() const { return Status::OK(); }
};

class MemTableIteratorWrapper : public InternalIterator {
 public:
  // Keeps the memtable alive for as long as the iterator exists.
  explicit MemTableIteratorWrapper(std::shared_ptr<const MemTable> mem)
      : mem_(std::move(mem)), it_(mem_->NewIterator()) {}

  bool Valid() const override { return it_->Valid(); }
  void SeekToFirst() override { it_->SeekToFirst(); }
  void Seek(std::string_view k) override { it_->Seek(k); }
  void Next() override { it_->Next(); }
  std::string_view internal_key() const override { return it_->internal_key(); }
  std::string_view value() const override { return it_->value(); }

 private:
  std::shared_ptr<const MemTable> mem_;
  std::unique_ptr<MemTable::Iterator> it_;
};

class SSTableIteratorWrapper : public InternalIterator {
 public:
  explicit SSTableIteratorWrapper(std::shared_ptr<const SSTableReader> table)
      : table_(std::move(table)), it_(table_->NewIterator()) {}

  bool Valid() const override { return it_->Valid(); }
  void SeekToFirst() override { it_->SeekToFirst(); }
  void Seek(std::string_view k) override { it_->Seek(k); }
  void Next() override { it_->Next(); }
  std::string_view internal_key() const override { return it_->internal_key(); }
  std::string_view value() const override { return it_->value(); }
  Status status() const override { return it_->status(); }

 private:
  std::shared_ptr<const SSTableReader> table_;
  std::unique_ptr<SSTableReader::Iterator> it_;
};

// N-way merge over child iterators. Children are ordered so that, for equal
// internal keys, the one from the newer source wins -- but since internal keys
// embed a unique sequence number, ties can only happen across duplicate data,
// and the smaller-index child is preferred deterministically.
//
// A linear scan over children beats a heap here: N is the number of levels
// plus a couple of memtables (typically < 10), so the constant factor of a
// heap dominates.
class MergingIterator : public InternalIterator {
 public:
  explicit MergingIterator(std::vector<std::unique_ptr<InternalIterator>> children)
      : children_(std::move(children)) {}

  bool Valid() const override { return current_ >= 0; }

  void SeekToFirst() override {
    for (auto& c : children_) c->SeekToFirst();
    FindSmallest();
  }

  void Seek(std::string_view k) override {
    for (auto& c : children_) c->Seek(k);
    FindSmallest();
  }

  void Next() override {
    if (current_ < 0) return;
    children_[static_cast<size_t>(current_)]->Next();
    FindSmallest();
  }

  std::string_view internal_key() const override {
    return children_[static_cast<size_t>(current_)]->internal_key();
  }
  std::string_view value() const override {
    return children_[static_cast<size_t>(current_)]->value();
  }

  Status status() const override {
    for (const auto& c : children_) {
      Status s = c->status();
      if (!s.ok()) return s;
    }
    return Status::OK();
  }

 private:
  void FindSmallest() {
    int smallest = -1;
    for (size_t i = 0; i < children_.size(); i++) {
      if (!children_[i]->Valid()) continue;
      if (smallest < 0 ||
          InternalKeyComparator::Compare(children_[i]->internal_key(),
                                         children_[static_cast<size_t>(smallest)]->internal_key()) <
              0) {
        smallest = static_cast<int>(i);
      }
    }
    current_ = smallest;
  }

  std::vector<std::unique_ptr<InternalIterator>> children_;
  int current_ = -1;
};

}  // namespace kvdb
