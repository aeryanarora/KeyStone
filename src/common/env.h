#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace kvdb {

// Monotonic clock: never goes backwards, safe for election timers.
inline uint64_t NowMicros() {
  auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}
inline uint64_t NowMillis() { return NowMicros() / 1000; }

inline uint64_t WallTimeMicros() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

// Fast xorshift PRNG; per-thread instances avoid contention in benchmarks.
class Random {
 public:
  explicit Random(uint64_t seed = 0) {
    if (seed == 0) {
      std::random_device rd;
      seed = (static_cast<uint64_t>(rd()) << 32) ^ rd() ^ NowMicros();
    }
    state_ = seed | 1;
  }

  uint64_t Next() {
    uint64_t x = state_;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    state_ = x;
    return x;
  }

  uint32_t Uniform(uint32_t n) { return n == 0 ? 0 : static_cast<uint32_t>(Next() % n); }
  bool OneIn(uint32_t n) { return Uniform(n) == 0; }
  double NextDouble() { return static_cast<double>(Next() >> 11) / 9007199254740992.0; }

  // Inclusive range [lo, hi].
  uint64_t InRange(uint64_t lo, uint64_t hi) {
    if (hi <= lo) return lo;
    return lo + Next() % (hi - lo + 1);
  }

  std::string RandomString(size_t len) {
    static const char kChars[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; i++) s.push_back(kChars[Uniform(sizeof(kChars) - 1)]);
    return s;
  }

 private:
  uint64_t state_;
};

// Latency recorder for the load generator. Stores raw samples so percentiles
// are exact rather than bucket-approximated (memory is cheap at these volumes).
class Histogram {
 public:
  void Add(double micros) {
    samples_.push_back(micros);
    sum_ += micros;
    min_ = std::min(min_, micros);
    max_ = std::max(max_, micros);
  }

  void Merge(const Histogram& other) {
    samples_.insert(samples_.end(), other.samples_.begin(), other.samples_.end());
    sum_ += other.sum_;
    min_ = std::min(min_, other.min_);
    max_ = std::max(max_, other.max_);
    sorted_ = false;
  }

  size_t count() const { return samples_.size(); }
  double mean() const { return samples_.empty() ? 0.0 : sum_ / static_cast<double>(samples_.size()); }
  double min() const { return samples_.empty() ? 0.0 : min_; }
  double max() const { return samples_.empty() ? 0.0 : max_; }

  double Percentile(double p) {
    if (samples_.empty()) return 0.0;
    EnsureSorted();
    double rank = (p / 100.0) * static_cast<double>(samples_.size() - 1);
    auto lo = static_cast<size_t>(std::floor(rank));
    auto hi = static_cast<size_t>(std::ceil(rank));
    double frac = rank - static_cast<double>(lo);
    return samples_[lo] * (1.0 - frac) + samples_[hi] * frac;
  }

  double p50() { return Percentile(50); }
  double p95() { return Percentile(95); }
  double p99() { return Percentile(99); }
  double p999() { return Percentile(99.9); }

  void Clear() {
    samples_.clear();
    sum_ = 0;
    min_ = 1e300;
    max_ = 0;
    sorted_ = false;
  }

 private:
  void EnsureSorted() {
    if (!sorted_) {
      std::sort(samples_.begin(), samples_.end());
      sorted_ = true;
    }
  }

  std::vector<double> samples_;
  double sum_ = 0;
  double min_ = 1e300;
  double max_ = 0;
  bool sorted_ = false;
};

}  // namespace kvdb
