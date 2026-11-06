#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace kvdb {
namespace crc32c {

// Software CRC-32C (Castagnoli, poly 0x1EDC6F41 reflected as 0x82F63B78).
// Used to detect torn/corrupt WAL records and SSTable blocks.
uint32_t Extend(uint32_t crc, const char* data, size_t n);

inline uint32_t Value(const char* data, size_t n) { return Extend(0, data, n); }
inline uint32_t Value(std::string_view s) { return Extend(0, s.data(), s.size()); }

// Masking prevents a stored CRC from being mistaken for data that happens to
// contain its own checksum (same trick LevelDB/RocksDB use).
static const uint32_t kMaskDelta = 0xa282ead8ul;

inline uint32_t Mask(uint32_t crc) { return ((crc >> 15) | (crc << 17)) + kMaskDelta; }

inline uint32_t Unmask(uint32_t masked) {
  uint32_t rot = masked - kMaskDelta;
  return ((rot >> 17) | (rot << 15));
}

}  // namespace crc32c
}  // namespace kvdb
