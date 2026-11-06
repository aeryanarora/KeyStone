#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include "common/arena.h"
#include "common/coding.h"
#include "common/status.h"
#include "storage/format.h"
#include "storage/skiplist.h"

namespace kvdb {

// Arena entry layout: varint32(klen) internal_key[klen] varint32(vlen) value[vlen]
// where internal_key = user_key || fixed64(seq<<8|type).
std::string_view DecodeEntryKey(const char* entry);
std::string_view DecodeEntryValue(const char* entry);

// Skiplist keys are raw pointers into the arena; this adaptor decodes them
// before delegating to the ordinary internal-key ordering.
struct MemTableKeyComparator {
  int operator()(const char* a, const char* b) const {
    return InternalKeyComparator::Compare(DecodeEntryKey(a), DecodeEntryKey(b));
  }
};

// In-memory write buffer -- the "M" in LSM. Entries live in a skiplist ordered
// by internal key, so a flush is a single sequential scan that emits an
// already-sorted SSTable.
//
// Threading: Add() must be called under the DB write lock (single writer);
// Get() and iteration are lock-free and safe concurrently with Add().
class MemTable {
 public:
  MemTable();
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void Add(SequenceNumber seq, ValueType type, std::string_view key, std::string_view value);

  // Returns true if `key` was resolved by this memtable at or below `snapshot`.
  // *s is OK with *value filled for a live value, NotFound for a tombstone.
  // *seq (optional) receives the sequence number of the version that answered
  // the lookup -- optimistic transactions use it for conflict validation.
  bool Get(std::string_view key, SequenceNumber snapshot, std::string* value, Status* s,
           SequenceNumber* seq = nullptr) const;

  size_t ApproximateMemoryUsage() const { return arena_.MemoryUsage(); }
  size_t NumEntries() const { return table_.ApproximateNumEntries(); }
  bool empty() const { return NumEntries() == 0; }

  SequenceNumber smallest_seq() const { return smallest_seq_; }
  SequenceNumber largest_seq() const { return largest_seq_; }

  using Table = SkipList<const char*, MemTableKeyComparator>;

  // Iterates internal keys in sorted order (newest version of a key first).
  class Iterator {
   public:
    explicit Iterator(const MemTable* mem) : iter_(&mem->table_) {}
    bool Valid() const { return iter_.Valid(); }
    void SeekToFirst() { iter_.SeekToFirst(); }
    void Seek(std::string_view internal_key);
    void Next() { iter_.Next(); }
    std::string_view internal_key() const { return DecodeEntryKey(iter_.key()); }
    std::string_view value() const { return DecodeEntryValue(iter_.key()); }

   private:
    Table::Iterator iter_;
    std::string seek_buf_;
  };

  std::unique_ptr<Iterator> NewIterator() const { return std::make_unique<Iterator>(this); }

 private:
  friend class Iterator;

  Arena arena_;
  Table table_;
  SequenceNumber smallest_seq_ = kMaxSequenceNumber;
  SequenceNumber largest_seq_ = 0;
};

}  // namespace kvdb
