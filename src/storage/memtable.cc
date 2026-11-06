#include "storage/memtable.h"

#include <algorithm>

namespace kvdb {

std::string_view DecodeEntryKey(const char* entry) {
  uint32_t len = 0;
  const char* p = GetVarint32Ptr(entry, entry + 5, &len);
  return std::string_view(p, len);
}

std::string_view DecodeEntryValue(const char* entry) {
  uint32_t klen = 0;
  const char* p = GetVarint32Ptr(entry, entry + 5, &klen);
  p += klen;
  uint32_t vlen = 0;
  p = GetVarint32Ptr(p, p + 5, &vlen);
  return std::string_view(p, vlen);
}

MemTable::MemTable() : table_(MemTableKeyComparator(), &arena_) {}

void MemTable::Add(SequenceNumber seq, ValueType type, std::string_view key,
                   std::string_view value) {
  const size_t key_size = key.size();
  const size_t internal_key_size = key_size + 8;
  const size_t val_size = value.size();
  const size_t encoded_len = static_cast<size_t>(VarintLength(internal_key_size)) +
                             internal_key_size + static_cast<size_t>(VarintLength(val_size)) +
                             val_size;

  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, static_cast<uint32_t>(internal_key_size));
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, PackSequenceAndType(seq, type));
  p += 8;
  p = EncodeVarint32(p, static_cast<uint32_t>(val_size));
  if (val_size > 0) std::memcpy(p, value.data(), val_size);

  table_.Insert(buf);
  smallest_seq_ = std::min(smallest_seq_, seq);
  largest_seq_ = std::max(largest_seq_, seq);
}

bool MemTable::Get(std::string_view key, SequenceNumber snapshot, std::string* value, Status* s,
                   SequenceNumber* seq) const {
  // Seek to (key, snapshot): thanks to descending sequence ordering the first
  // hit is the newest version visible to this snapshot.
  std::string lookup_entry;
  std::string internal = MakeInternalKey(key, snapshot, kTypeValue);
  PutVarint32(&lookup_entry, static_cast<uint32_t>(internal.size()));
  lookup_entry.append(internal);
  PutVarint32(&lookup_entry, 0);

  Table::Iterator iter(&table_);
  iter.Seek(lookup_entry.data());
  if (!iter.Valid()) return false;

  std::string_view found_key = DecodeEntryKey(iter.key());
  if (ExtractUserKey(found_key) != key) return false;
  if (seq != nullptr) *seq = ExtractSequence(found_key);

  switch (ExtractValueType(found_key)) {
    case kTypeValue: {
      std::string_view v = DecodeEntryValue(iter.key());
      value->assign(v.data(), v.size());
      *s = Status::OK();
      return true;
    }
    case kTypeDeletion:
      *s = Status::NotFound();
      return true;
  }
  return false;
}

void MemTable::Iterator::Seek(std::string_view internal_key) {
  seek_buf_.clear();
  PutVarint32(&seek_buf_, static_cast<uint32_t>(internal_key.size()));
  seek_buf_.append(internal_key.data(), internal_key.size());
  PutVarint32(&seek_buf_, 0);
  iter_.Seek(seek_buf_.data());
}

}  // namespace kvdb
