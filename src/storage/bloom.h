#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kvdb {

// Bloom filter stored in each SSTable. A negative answer is authoritative, so
// point lookups can skip the (potentially disk-resident) index+data blocks for
// tables that certainly don't hold the key -- the single biggest win for reads
// in an LSM tree with many levels.
//
// Uses the double-hashing scheme from Kirsch-Mitzenmacher: g_i(x) = h1 + i*h2.
class BloomFilter {
 public:
  explicit BloomFilter(int bits_per_key = 10) : bits_per_key_(bits_per_key) {
    // k = ln(2) * m/n, clamped to a sane range.
    k_ = static_cast<size_t>(static_cast<double>(bits_per_key) * 0.69);
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  // Builds a filter over `keys` and appends it to *dst.
  // Layout: | bit array | k (1 byte) |
  void CreateFilter(const std::vector<std::string>& keys, std::string* dst) const;

  // `filter` is the encoded blob produced by CreateFilter.
  static bool KeyMayMatch(std::string_view key, std::string_view filter);

  static uint32_t Hash(std::string_view key, uint32_t seed);

  int bits_per_key() const { return bits_per_key_; }

 private:
  int bits_per_key_;
  size_t k_;
};

}  // namespace kvdb
