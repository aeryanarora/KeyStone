#include "common/crc32c.h"

#include <array>

namespace kvdb {
namespace crc32c {
namespace {

constexpr uint32_t kPoly = 0x82f63b78u;

struct Table {
  std::array<uint32_t, 256> t{};
  constexpr Table() {
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t crc = i;
      for (int j = 0; j < 8; j++) {
        crc = (crc & 1) ? (crc >> 1) ^ kPoly : (crc >> 1);
      }
      t[i] = crc;
    }
  }
};

constexpr Table kTable{};

}  // namespace

uint32_t Extend(uint32_t crc, const char* data, size_t n) {
  uint32_t c = crc ^ 0xffffffffu;
  const auto* p = reinterpret_cast<const uint8_t*>(data);
  for (size_t i = 0; i < n; i++) {
    c = kTable.t[(c ^ p[i]) & 0xff] ^ (c >> 8);
  }
  return c ^ 0xffffffffu;
}

}  // namespace crc32c
}  // namespace kvdb
