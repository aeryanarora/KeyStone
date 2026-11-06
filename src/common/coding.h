#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace kvdb {

// ---------------------------------------------------------------------------
// Fixed-width little-endian encoding.
// ---------------------------------------------------------------------------
inline void EncodeFixed32(char* dst, uint32_t v) { std::memcpy(dst, &v, sizeof(v)); }
inline void EncodeFixed64(char* dst, uint64_t v) { std::memcpy(dst, &v, sizeof(v)); }

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}
inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}

inline void PutFixed32(std::string* dst, uint32_t v) {
  char buf[4];
  EncodeFixed32(buf, v);
  dst->append(buf, 4);
}
inline void PutFixed64(std::string* dst, uint64_t v) {
  char buf[8];
  EncodeFixed64(buf, v);
  dst->append(buf, 8);
}

// ---------------------------------------------------------------------------
// Varint encoding (LEB128-style, 7 bits per byte).
// ---------------------------------------------------------------------------
inline char* EncodeVarint32(char* dst, uint32_t v) {
  auto* p = reinterpret_cast<uint8_t*>(dst);
  while (v >= 0x80) {
    *p++ = static_cast<uint8_t>(v) | 0x80;
    v >>= 7;
  }
  *p++ = static_cast<uint8_t>(v);
  return reinterpret_cast<char*>(p);
}

inline char* EncodeVarint64(char* dst, uint64_t v) {
  auto* p = reinterpret_cast<uint8_t*>(dst);
  while (v >= 0x80) {
    *p++ = static_cast<uint8_t>(v) | 0x80;
    v >>= 7;
  }
  *p++ = static_cast<uint8_t>(v);
  return reinterpret_cast<char*>(p);
}

inline void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* end = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(end - buf));
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* end = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(end - buf));
}

// Returns nullptr on failure (truncated / malformed input).
inline const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    auto byte = static_cast<uint32_t>(static_cast<uint8_t>(*p));
    p++;
    if (byte & 0x80) {
      result |= ((byte & 0x7f) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

inline const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    auto byte = static_cast<uint64_t>(static_cast<uint8_t>(*p));
    p++;
    if (byte & 0x80) {
      result |= ((byte & 0x7f) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

inline bool GetVarint32(std::string_view* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) return false;
  *input = std::string_view(q, static_cast<size_t>(limit - q));
  return true;
}

inline bool GetVarint64(std::string_view* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) return false;
  *input = std::string_view(q, static_cast<size_t>(limit - q));
  return true;
}

inline bool GetFixed32(std::string_view* input, uint32_t* value) {
  if (input->size() < 4) return false;
  *value = DecodeFixed32(input->data());
  input->remove_prefix(4);
  return true;
}

inline bool GetFixed64(std::string_view* input, uint64_t* value) {
  if (input->size() < 8) return false;
  *value = DecodeFixed64(input->data());
  input->remove_prefix(8);
  return true;
}

// ---------------------------------------------------------------------------
// Length-prefixed blobs.
// ---------------------------------------------------------------------------
inline void PutLengthPrefixed(std::string* dst, std::string_view value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline bool GetLengthPrefixed(std::string_view* input, std::string_view* result) {
  uint32_t len = 0;
  if (!GetVarint32(input, &len)) return false;
  if (input->size() < len) return false;
  *result = std::string_view(input->data(), len);
  input->remove_prefix(len);
  return true;
}

inline bool GetLengthPrefixedString(std::string_view* input, std::string* result) {
  std::string_view v;
  if (!GetLengthPrefixed(input, &v)) return false;
  result->assign(v.data(), v.size());
  return true;
}

inline int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 0x80) {
    v >>= 7;
    len++;
  }
  return len;
}

}  // namespace kvdb
