#include "storage/sstable.h"

#include <algorithm>
#include <cstring>

#include "common/coding.h"
#include "common/crc32c.h"
#include "common/logger.h"

namespace kvdb {

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------
SSTableBuilder::SSTableBuilder(size_t block_size, int bloom_bits_per_key)
    : block_size_(block_size), bloom_(bloom_bits_per_key) {}

SSTableBuilder::~SSTableBuilder() {
  if (!closed_ && file_) Abandon();
}

Status SSTableBuilder::Open(const std::string& path) {
  file_ = std::make_unique<WritableFile>();
  KVDB_RETURN_IF_ERROR(file_->Open(path));
  path_ = path;
  return Status::OK();
}

Status SSTableBuilder::Add(std::string_view internal_key, std::string_view value) {
  if (!file_) return Status::IOError("builder not open");
  if (!last_key_.empty() &&
      InternalKeyComparator::Compare(last_key_, internal_key) >= 0) {
    return Status::InvalidArgument("sstable keys must be strictly increasing");
  }

  if (num_entries_ == 0) smallest_.assign(internal_key.data(), internal_key.size());
  largest_.assign(internal_key.data(), internal_key.size());
  SequenceNumber seq = ExtractSequence(internal_key);
  smallest_seq_ = std::min(smallest_seq_, seq);
  largest_seq_ = std::max(largest_seq_, seq);

  PutVarint32(&block_buf_, static_cast<uint32_t>(internal_key.size()));
  block_buf_.append(internal_key.data(), internal_key.size());
  PutVarint32(&block_buf_, static_cast<uint32_t>(value.size()));
  block_buf_.append(value.data(), value.size());

  std::string_view user_key = ExtractUserKey(internal_key);
  filter_keys_.emplace_back(user_key);
  last_key_.assign(internal_key.data(), internal_key.size());
  num_entries_++;

  if (block_buf_.size() >= block_size_) return FlushDataBlock();
  return Status::OK();
}

Status SSTableBuilder::WriteRawBlock(const std::string& contents, BlockHandle* handle) {
  handle->offset = offset_;
  handle->size = contents.size();
  KVDB_RETURN_IF_ERROR(file_->Append(contents));
  char trailer[sstfmt::kBlockTrailerSize];
  uint32_t crc = crc32c::Value(contents.data(), contents.size());
  EncodeFixed32(trailer, crc32c::Mask(crc));
  KVDB_RETURN_IF_ERROR(file_->Append(trailer, sizeof(trailer)));
  offset_ += contents.size() + sstfmt::kBlockTrailerSize;
  return Status::OK();
}

Status SSTableBuilder::FlushDataBlock() {
  if (block_buf_.empty()) return Status::OK();
  BlockHandle handle;
  KVDB_RETURN_IF_ERROR(WriteRawBlock(block_buf_, &handle));
  // One index entry per block: (largest key in block, handle).
  PutVarint32(&index_buf_, static_cast<uint32_t>(last_key_.size()));
  index_buf_.append(last_key_);
  PutVarint64(&index_buf_, handle.offset);
  PutVarint64(&index_buf_, handle.size);
  block_buf_.clear();
  return Status::OK();
}

Status SSTableBuilder::Finish() {
  if (!file_) return Status::IOError("builder not open");
  KVDB_RETURN_IF_ERROR(FlushDataBlock());

  std::string filter_block;
  bloom_.CreateFilter(filter_keys_, &filter_block);
  BlockHandle filter_handle;
  KVDB_RETURN_IF_ERROR(WriteRawBlock(filter_block, &filter_handle));

  BlockHandle index_handle;
  KVDB_RETURN_IF_ERROR(WriteRawBlock(index_buf_, &index_handle));

  std::string footer;
  PutFixed64(&footer, index_handle.offset);
  PutFixed64(&footer, index_handle.size);
  PutFixed64(&footer, filter_handle.offset);
  PutFixed64(&footer, filter_handle.size);
  PutFixed64(&footer, sstfmt::kMagic);
  KVDB_RETURN_IF_ERROR(file_->Append(footer));
  offset_ += footer.size();

  KVDB_RETURN_IF_ERROR(file_->Sync());  // SSTables must be durable before the
  KVDB_RETURN_IF_ERROR(file_->Close()); // manifest can reference them
  closed_ = true;
  return Status::OK();
}

Status SSTableBuilder::Abandon() {
  closed_ = true;
  if (file_) {
    file_->Close();
    file_.reset();
    fileutil::DeleteFile(path_);
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------
Status SSTableReader::Open(const std::string& path, BlockCache* cache, uint64_t file_number) {
  KVDB_RETURN_IF_ERROR(file_.Open(path));
  path_ = path;
  cache_ = cache;
  file_number_ = file_number;

  if (file_.size() < sstfmt::kFooterSize) return Status::Corruption("sstable too short: " + path);

  std::string footer;
  KVDB_RETURN_IF_ERROR(
      file_.Read(file_.size() - sstfmt::kFooterSize, sstfmt::kFooterSize, &footer));
  std::string_view fv(footer);
  BlockHandle index_handle, filter_handle;
  uint64_t magic = 0;
  if (!GetFixed64(&fv, &index_handle.offset) || !GetFixed64(&fv, &index_handle.size) ||
      !GetFixed64(&fv, &filter_handle.offset) || !GetFixed64(&fv, &filter_handle.size) ||
      !GetFixed64(&fv, &magic)) {
    return Status::Corruption("bad sstable footer: " + path);
  }
  if (magic != sstfmt::kMagic) return Status::Corruption("bad sstable magic: " + path);

  KVDB_RETURN_IF_ERROR(ReadBlock(filter_handle, &filter_));

  std::string index_block;
  KVDB_RETURN_IF_ERROR(ReadBlock(index_handle, &index_block));
  std::string_view iv(index_block);
  while (!iv.empty()) {
    uint32_t klen = 0;
    if (!GetVarint32(&iv, &klen) || iv.size() < klen) {
      return Status::Corruption("bad sstable index: " + path);
    }
    IndexEntry e;
    e.last_key.assign(iv.data(), klen);
    iv.remove_prefix(klen);
    if (!GetVarint64(&iv, &e.handle.offset) || !GetVarint64(&iv, &e.handle.size)) {
      return Status::Corruption("bad sstable index handle: " + path);
    }
    index_.push_back(std::move(e));
  }
  return Status::OK();
}

Status SSTableReader::ReadBlock(const BlockHandle& handle, std::string* out) const {
  std::string cache_key;
  if (cache_ != nullptr) {
    cache_key.reserve(16);
    PutFixed64(&cache_key, file_number_);
    PutFixed64(&cache_key, handle.offset);
    if (cache_->Lookup(cache_key, out)) return Status::OK();
  }

  std::string raw;
  KVDB_RETURN_IF_ERROR(
      file_.Read(handle.offset, handle.size + sstfmt::kBlockTrailerSize, &raw));
  uint32_t expected = crc32c::Unmask(DecodeFixed32(raw.data() + handle.size));
  uint32_t actual = crc32c::Value(raw.data(), handle.size);
  if (expected != actual) return Status::Corruption("sstable block checksum: " + path_);

  out->assign(raw.data(), handle.size);
  if (cache_ != nullptr) cache_->Insert(cache_key, *out);
  return Status::OK();
}

bool SSTableReader::Get(std::string_view user_key, SequenceNumber snapshot, std::string* value,
                        Status* s, SequenceNumber* seq) const {
  // Bloom filter first: this is the fast path that avoids touching disk for
  // the (common) case where the key simply isn't in this file.
  if (!filter_.empty() && !BloomFilter::KeyMayMatch(user_key, filter_)) return false;

  const std::string target = MakeInternalKey(user_key, snapshot, kTypeValue);

  // Find the first block whose largest key >= target.
  auto it = std::lower_bound(index_.begin(), index_.end(), target,
                             [](const IndexEntry& e, const std::string& t) {
                               return InternalKeyComparator::Compare(e.last_key, t) < 0;
                             });
  if (it == index_.end()) return false;

  std::string block;
  Status rs = ReadBlock(it->handle, &block);
  if (!rs.ok()) {
    *s = rs;
    return true;  // surface the I/O error rather than silently missing data
  }

  std::string_view bv(block);
  while (!bv.empty()) {
    uint32_t klen = 0, vlen = 0;
    if (!GetVarint32(&bv, &klen) || bv.size() < klen) break;
    std::string_view ikey(bv.data(), klen);
    bv.remove_prefix(klen);
    if (!GetVarint32(&bv, &vlen) || bv.size() < vlen) break;
    std::string_view val(bv.data(), vlen);
    bv.remove_prefix(vlen);

    int cmp = InternalKeyComparator::Compare(ikey, target);
    if (cmp < 0) continue;  // older/smaller than what we're looking for
    if (ExtractUserKey(ikey) != user_key) return false;  // walked past the key

    if (seq != nullptr) *seq = ExtractSequence(ikey);
    if (ExtractValueType(ikey) == kTypeValue) {
      value->assign(val.data(), val.size());
      *s = Status::OK();
    } else {
      *s = Status::NotFound();
    }
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Reader::Iterator
// ---------------------------------------------------------------------------
bool SSTableReader::Iterator::LoadBlock(size_t index) {
  if (index >= table_->index_.size()) {
    valid_ = false;
    return false;
  }
  status_ = table_->ReadBlock(table_->index_[index].handle, &block_data_);
  if (!status_.ok()) {
    valid_ = false;
    return false;
  }
  block_index_ = index;
  pos_ = 0;
  return true;
}

bool SSTableReader::Iterator::ParseCurrentEntry() {
  while (true) {
    if (pos_ >= block_data_.size()) {
      if (!LoadBlock(block_index_ + 1)) return false;
      continue;
    }
    std::string_view bv(block_data_.data() + pos_, block_data_.size() - pos_);
    const char* start = bv.data();
    uint32_t klen = 0, vlen = 0;
    if (!GetVarint32(&bv, &klen) || bv.size() < klen) {
      status_ = Status::Corruption("sstable entry truncated");
      valid_ = false;
      return false;
    }
    key_ = std::string_view(bv.data(), klen);
    bv.remove_prefix(klen);
    if (!GetVarint32(&bv, &vlen) || bv.size() < vlen) {
      status_ = Status::Corruption("sstable value truncated");
      valid_ = false;
      return false;
    }
    value_ = std::string_view(bv.data(), vlen);
    bv.remove_prefix(vlen);
    pos_ += static_cast<size_t>(bv.data() - start);
    valid_ = true;
    return true;
  }
}

void SSTableReader::Iterator::SeekToFirst() {
  if (!LoadBlock(0)) {
    valid_ = false;
    return;
  }
  ParseCurrentEntry();
}

void SSTableReader::Iterator::Seek(std::string_view internal_key) {
  const auto& idx = table_->index_;
  auto it = std::lower_bound(idx.begin(), idx.end(), internal_key,
                             [](const IndexEntry& e, std::string_view t) {
                               return InternalKeyComparator::Compare(e.last_key, t) < 0;
                             });
  if (it == idx.end()) {
    valid_ = false;
    return;
  }
  if (!LoadBlock(static_cast<size_t>(it - idx.begin()))) return;
  while (ParseCurrentEntry()) {
    if (InternalKeyComparator::Compare(key_, internal_key) >= 0) return;
  }
}

void SSTableReader::Iterator::Next() {
  if (!valid_) return;
  ParseCurrentEntry();
}

}  // namespace kvdb
