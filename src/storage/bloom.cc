#include "storage/bloom.h"

#include <cstring>

#include "common/coding.h"

namespace kvdb {

uint32_t BloomFilter::Hash(std::string_view key, uint32_t seed) {
  // MurmurHash-like mixing; cheap and good enough for filter purposes.
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* data = key.data();
  size_t n = key.size();
  uint32_t h = seed ^ (static_cast<uint32_t>(n) * m);

  while (n >= 4) {
    uint32_t w = DecodeFixed32(data);
    data += 4;
    n -= 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  switch (n) {
    case 3:
      h += static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 16;
      [[fallthrough]];
    case 2:
      h += static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 8;
      [[fallthrough]];
    case 1:
      h += static_cast<uint32_t>(static_cast<uint8_t>(data[0]));
      h *= m;
      h ^= (h >> r);
      break;
    default:
      break;
  }
  return h;
}

void BloomFilter::CreateFilter(const std::vector<std::string>& keys, std::string* dst) const {
  size_t bits = keys.size() * static_cast<size_t>(bits_per_key_);
  if (bits < 64) bits = 64;  // tiny filters have absurd false-positive rates
  const size_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  const size_t init_size = dst->size();
  dst->resize(init_size + bytes, '\0');
  dst->push_back(static_cast<char>(k_));
  char* array = dst->data() + init_size;

  for (const auto& key : keys) {
    uint32_t h = Hash(key, 0xbc9f1d34);
    const uint32_t delta = (h >> 17) | (h << 15);  // rotate right 17 bits
    for (size_t j = 0; j < k_; j++) {
      const uint32_t bitpos = h % bits;
      array[bitpos / 8] |= static_cast<char>(1 << (bitpos % 8));
      h += delta;
    }
  }
}

bool BloomFilter::KeyMayMatch(std::string_view key, std::string_view filter) {
  const size_t len = filter.size();
  if (len < 2) return true;  // no/short filter: be conservative

  const char* array = filter.data();
  const size_t bits = (len - 1) * 8;
  const auto k = static_cast<size_t>(static_cast<uint8_t>(filter[len - 1]));
  if (k > 30) return true;  // reserved encoding for future filter policies

  uint32_t h = Hash(key, 0xbc9f1d34);
  const uint32_t delta = (h >> 17) | (h << 15);
  for (size_t j = 0; j < k; j++) {
    const uint32_t bitpos = h % bits;
    if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
    h += delta;
  }
  return true;
}

}  // namespace kvdb
