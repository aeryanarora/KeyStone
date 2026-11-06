#include "storage/wal.h"

#include <cstring>

#include "common/coding.h"
#include "common/crc32c.h"
#include "common/logger.h"

namespace kvdb {

using namespace walfmt;

WalWriter::~WalWriter() { Close(); }

Status WalWriter::Open(const std::string& path, bool append) {
  file_ = std::make_unique<WritableFile>();
  KVDB_RETURN_IF_ERROR(file_->Open(path, append));
  path_ = path;
  // When appending we must resume at the same intra-block offset, otherwise
  // block boundaries stop lining up and recovery desynchronises.
  block_offset_ = append ? static_cast<size_t>(file_->size() % kBlockSize) : 0;
  return Status::OK();
}

Status WalWriter::AddRecord(std::string_view data) {
  if (!file_) return Status::IOError("wal not open");
  const char* ptr = data.data();
  size_t left = data.size();

  bool begin = true;
  Status s;
  do {
    const size_t leftover = kBlockSize - block_offset_;
    if (leftover < kHeaderSize) {
      if (leftover > 0) {
        static const char kPad[kHeaderSize] = {0, 0, 0, 0, 0, 0, 0};
        KVDB_RETURN_IF_ERROR(file_->Append(kPad, leftover));
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    const bool end = (left == fragment_length);

    RecordType type;
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status WalWriter::EmitPhysicalRecord(RecordType t, const char* ptr, size_t length) {
  char header[kHeaderSize];
  header[4] = static_cast<char>(length & 0xff);
  header[5] = static_cast<char>(length >> 8);
  header[6] = static_cast<char>(t);

  uint32_t crc = crc32c::Extend(0, reinterpret_cast<const char*>(&header[6]), 1);
  crc = crc32c::Extend(crc, ptr, length);
  EncodeFixed32(header, crc32c::Mask(crc));

  KVDB_RETURN_IF_ERROR(file_->Append(header, kHeaderSize));
  KVDB_RETURN_IF_ERROR(file_->Append(ptr, length));
  block_offset_ += kHeaderSize + length;
  return Status::OK();
}

Status WalWriter::Flush() { return file_ ? file_->Flush() : Status::OK(); }

Status WalWriter::Sync() { return file_ ? file_->Sync() : Status::OK(); }

Status WalWriter::Close() {
  if (!file_) return Status::OK();
  Status s = file_->Close();
  file_.reset();
  return s;
}

// ---------------------------------------------------------------------------

Status WalReader::Open(const std::string& path) {
  KVDB_RETURN_IF_ERROR(file_.Open(path));
  buffer_ = {};
  backing_store_.clear();
  eof_ = false;
  last_good_offset_ = 0;
  return Status::OK();
}

Status WalReader::ReadPhysicalRecord(std::string_view* result, RecordType* type) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      if (eof_) {
        buffer_ = {};
        return Status::NotFound("eof");
      }
      buffer_ = {};
      Status s = file_.Read(kBlockSize, &backing_store_);
      if (!s.ok()) return s;
      buffer_ = std::string_view(backing_store_);
      if (backing_store_.size() < kBlockSize) eof_ = true;
      if (backing_store_.empty()) return Status::NotFound("eof");
      continue;
    }

    const char* header = buffer_.data();
    const auto a = static_cast<uint32_t>(static_cast<uint8_t>(header[4]));
    const auto b = static_cast<uint32_t>(static_cast<uint8_t>(header[5]));
    const auto t = static_cast<uint8_t>(header[6]);
    const uint32_t length = a | (b << 8);

    if (t == kZeroType && length == 0) {
      // Zero padding at the tail of a block: skip to the next block.
      buffer_ = {};
      continue;
    }

    if (kHeaderSize + length > buffer_.size()) {
      buffer_ = {};
      if (!eof_) return Status::Corruption("bad record length");
      return Status::NotFound("eof");  // truncated tail after a crash
    }

    uint32_t expected = crc32c::Unmask(DecodeFixed32(header));
    uint32_t actual = crc32c::Extend(0, header + 6, 1);
    actual = crc32c::Extend(actual, header + kHeaderSize, length);
    if (actual != expected) {
      buffer_ = {};
      return Status::Corruption("wal checksum mismatch");
    }

    *result = std::string_view(header + kHeaderSize, length);
    *type = static_cast<RecordType>(t);
    buffer_.remove_prefix(kHeaderSize + length);
    return Status::OK();
  }
}

Status WalReader::ReadRecord(std::string* record) {
  record->clear();
  bool in_fragmented = false;
  while (true) {
    std::string_view fragment;
    RecordType type;
    Status s = ReadPhysicalRecord(&fragment, &type);
    if (!s.ok()) {
      if (s.IsNotFound() && in_fragmented) {
        return Status::Corruption("wal ends mid-record");
      }
      return s;
    }

    switch (type) {
      case kFullType:
        record->assign(fragment.data(), fragment.size());
        last_good_offset_ = file_.offset();
        return Status::OK();
      case kFirstType:
        record->assign(fragment.data(), fragment.size());
        in_fragmented = true;
        break;
      case kMiddleType:
        if (!in_fragmented) return Status::Corruption("MIDDLE fragment without FIRST");
        record->append(fragment.data(), fragment.size());
        break;
      case kLastType:
        if (!in_fragmented) return Status::Corruption("LAST fragment without FIRST");
        record->append(fragment.data(), fragment.size());
        last_good_offset_ = file_.offset();
        return Status::OK();
      default:
        return Status::Corruption("unknown wal record type");
    }
  }
}

}  // namespace kvdb
