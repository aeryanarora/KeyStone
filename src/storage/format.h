#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/coding.h"

namespace kvdb {

// Monotonically increasing version stamp assigned to every mutation. Snapshot
// reads are just "the newest entry with seq <= my_snapshot".
using SequenceNumber = uint64_t;

static constexpr SequenceNumber kMaxSequenceNumber = (1ull << 56) - 1;

enum ValueType : uint8_t {
  kTypeDeletion = 0x0,  // tombstone
  kTypeValue = 0x1,
};

// Trailer packs (seq << 8 | type) so that, for identical user keys, a plain
// descending numeric comparison puts newer versions first.
inline uint64_t PackSequenceAndType(SequenceNumber seq, ValueType t) {
  return (seq << 8) | static_cast<uint64_t>(t);
}
inline void UnpackSequenceAndType(uint64_t packed, SequenceNumber* seq, ValueType* t) {
  *seq = packed >> 8;
  *t = static_cast<ValueType>(packed & 0xff);
}

// Internal key layout: | user_key bytes | 8-byte fixed trailer |
inline void AppendInternalKey(std::string* dst, std::string_view user_key,
                              SequenceNumber seq, ValueType t) {
  dst->append(user_key.data(), user_key.size());
  PutFixed64(dst, PackSequenceAndType(seq, t));
}

inline std::string MakeInternalKey(std::string_view user_key, SequenceNumber seq, ValueType t) {
  std::string s;
  s.reserve(user_key.size() + 8);
  AppendInternalKey(&s, user_key, seq, t);
  return s;
}

inline std::string_view ExtractUserKey(std::string_view internal_key) {
  if (internal_key.size() < 8) return {};
  return std::string_view(internal_key.data(), internal_key.size() - 8);
}

inline uint64_t ExtractTrailer(std::string_view internal_key) {
  if (internal_key.size() < 8) return 0;
  return DecodeFixed64(internal_key.data() + internal_key.size() - 8);
}

inline SequenceNumber ExtractSequence(std::string_view internal_key) {
  return ExtractTrailer(internal_key) >> 8;
}

inline ValueType ExtractValueType(std::string_view internal_key) {
  return static_cast<ValueType>(ExtractTrailer(internal_key) & 0xff);
}

// Orders by user key ascending, then by sequence number DESCENDING so a seek to
// (key, snapshot_seq) lands directly on the newest visible version.
struct InternalKeyComparator {
  int operator()(std::string_view a, std::string_view b) const { return Compare(a, b); }

  static int Compare(std::string_view a, std::string_view b) {
    std::string_view ua = ExtractUserKey(a);
    std::string_view ub = ExtractUserKey(b);
    int r = ua.compare(ub);
    if (r != 0) return r < 0 ? -1 : 1;
    uint64_t ta = ExtractTrailer(a);
    uint64_t tb = ExtractTrailer(b);
    if (ta > tb) return -1;  // larger sequence sorts first
    if (ta < tb) return 1;
    return 0;
  }
};

}  // namespace kvdb
