#include "storage/write_batch.h"

#include "common/coding.h"
#include "storage/memtable.h"

namespace kvdb {

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeaderSize, '\0');
}

uint32_t WriteBatch::Count() const {
  if (rep_.size() < kHeaderSize) return 0;
  return DecodeFixed32(rep_.data() + 8);
}

void WriteBatch::SetCount(uint32_t n) { EncodeFixed32(rep_.data() + 8, n); }

SequenceNumber WriteBatch::Sequence() const {
  if (rep_.size() < kHeaderSize) return 0;
  return DecodeFixed64(rep_.data());
}

void WriteBatch::SetSequence(SequenceNumber seq) { EncodeFixed64(rep_.data(), seq); }

void WriteBatch::Put(std::string_view key, std::string_view value) {
  SetCount(Count() + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixed(&rep_, key);
  PutLengthPrefixed(&rep_, value);
}

void WriteBatch::Delete(std::string_view key) {
  SetCount(Count() + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixed(&rep_, key);
}

Status WriteBatch::SetContents(std::string_view contents) {
  if (contents.size() < kHeaderSize) return Status::Corruption("write batch too small");
  rep_.assign(contents.data(), contents.size());
  return Status::OK();
}

void WriteBatch::Append(const WriteBatch& src) {
  SetCount(Count() + src.Count());
  rep_.append(src.rep_.data() + kHeaderSize, src.rep_.size() - kHeaderSize);
}

Status WriteBatch::Iterate(Handler* handler) const {
  if (rep_.size() < kHeaderSize) return Status::Corruption("malformed write batch");
  std::string_view input(rep_.data() + kHeaderSize, rep_.size() - kHeaderSize);
  SequenceNumber seq = Sequence();
  uint32_t found = 0;
  while (!input.empty()) {
    found++;
    auto tag = static_cast<ValueType>(static_cast<uint8_t>(input[0]));
    input.remove_prefix(1);
    std::string_view key, value;
    switch (tag) {
      case kTypeValue:
        if (!GetLengthPrefixed(&input, &key) || !GetLengthPrefixed(&input, &value)) {
          return Status::Corruption("bad Put record in write batch");
        }
        handler->Put(key, value, seq);
        break;
      case kTypeDeletion:
        if (!GetLengthPrefixed(&input, &key)) {
          return Status::Corruption("bad Delete record in write batch");
        }
        handler->Delete(key, seq);
        break;
      default:
        return Status::Corruption("unknown record tag in write batch");
    }
    // Each mutation in a batch gets its own sequence number so that reads
    // inside/around a batch see a consistent total order.
    seq++;
  }
  if (found != Count()) return Status::Corruption("write batch count mismatch");
  return Status::OK();
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  explicit MemTableInserter(MemTable* mem) : mem_(mem) {}
  void Put(std::string_view key, std::string_view value, SequenceNumber seq) override {
    mem_->Add(seq, kTypeValue, key, value);
  }
  void Delete(std::string_view key, SequenceNumber seq) override {
    mem_->Add(seq, kTypeDeletion, key, {});
  }

 private:
  MemTable* mem_;
};
}  // namespace

Status WriteBatch::InsertInto(MemTable* memtable) const {
  MemTableInserter inserter(memtable);
  return Iterate(&inserter);
}

}  // namespace kvdb
