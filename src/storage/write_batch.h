#pragma once

#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/format.h"

namespace kvdb {

class MemTable;

// A WriteBatch is the atomic unit everywhere in the system: it is what the WAL
// records, what Raft replicates, and what the MemTable applies. Either all of
// its mutations become visible or none do.
//
// Wire format:
//   | seq (fixed64) | count (fixed32) | record* |
//   record := kTypeValue    varstring(key) varstring(value)
//           | kTypeDeletion varstring(key)
class WriteBatch {
 public:
  WriteBatch() { Clear(); }

  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);
  void Clear();

  uint32_t Count() const;
  void SetCount(uint32_t n);
  SequenceNumber Sequence() const;
  void SetSequence(SequenceNumber seq);

  size_t ApproximateSize() const { return rep_.size(); }
  bool empty() const { return Count() == 0; }

  const std::string& Contents() const { return rep_; }
  std::string& MutableContents() { return rep_; }
  Status SetContents(std::string_view contents);

  // Appends every record of `src` to this batch (used for group commit).
  void Append(const WriteBatch& src);

  // Visitor over the records, in insertion order.
  class Handler {
   public:
    virtual ~Handler() = default;
    virtual void Put(std::string_view key, std::string_view value, SequenceNumber seq) = 0;
    virtual void Delete(std::string_view key, SequenceNumber seq) = 0;
  };

  Status Iterate(Handler* handler) const;
  Status InsertInto(MemTable* memtable) const;

  static constexpr size_t kHeaderSize = 12;

 private:
  std::string rep_;
};

}  // namespace kvdb
