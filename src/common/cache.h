#pragma once

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kvdb {

// Sharded LRU cache used for SSTable data blocks.
//
// Sharding by key hash is what makes this usable under load: with N shards the
// probability that two random lookups contend on the same mutex is 1/N, which
// turns the cache from a global serialisation point into a scalable one.
class BlockCache {
 public:
  explicit BlockCache(size_t capacity_bytes, size_t num_shards = 16)
      : capacity_(capacity_bytes), num_shards_(num_shards == 0 ? 1 : num_shards) {
    shards_.reserve(num_shards_);
    for (size_t i = 0; i < num_shards_; i++) {
      shards_.push_back(std::make_unique<Shard>(capacity_ / num_shards_));
    }
  }

  bool Lookup(const std::string& key, std::string* value) {
    if (capacity_ == 0) return false;
    bool hit = GetShard(key)->Lookup(key, value);
    (hit ? hits_ : misses_).fetch_add(1, std::memory_order_relaxed);
    return hit;
  }

  void Insert(const std::string& key, std::string value) {
    if (capacity_ == 0) return;
    GetShard(key)->Insert(key, std::move(value));
  }

  void Erase(const std::string& key) {
    if (capacity_ == 0) return;
    GetShard(key)->Erase(key);
  }

  uint64_t hits() const { return hits_.load(std::memory_order_relaxed); }
  uint64_t misses() const { return misses_.load(std::memory_order_relaxed); }
  double hit_rate() const {
    uint64_t h = hits(), m = misses();
    return (h + m == 0) ? 0.0 : static_cast<double>(h) / static_cast<double>(h + m);
  }
  size_t capacity() const { return capacity_; }

  size_t TotalUsage() const {
    size_t total = 0;
    for (const auto& s : shards_) total += s->usage();
    return total;
  }

 private:
  struct Shard {
    explicit Shard(size_t cap) : capacity(cap) {}

    bool Lookup(const std::string& key, std::string* value) {
      std::lock_guard<std::mutex> lk(mu);
      auto it = table.find(key);
      if (it == table.end()) return false;
      lru.splice(lru.begin(), lru, it->second);  // touch: move to MRU end
      *value = it->second->second;
      return true;
    }

    void Insert(const std::string& key, std::string value) {
      std::lock_guard<std::mutex> lk(mu);
      auto it = table.find(key);
      if (it != table.end()) {
        usage_bytes -= it->second->second.size();
        it->second->second = std::move(value);
        usage_bytes += it->second->second.size();
        lru.splice(lru.begin(), lru, it->second);
        return;
      }
      usage_bytes += value.size() + key.size();
      lru.emplace_front(key, std::move(value));
      table[key] = lru.begin();
      while (usage_bytes > capacity && !lru.empty()) {
        auto& victim = lru.back();
        usage_bytes -= victim.second.size() + victim.first.size();
        table.erase(victim.first);
        lru.pop_back();
      }
    }

    void Erase(const std::string& key) {
      std::lock_guard<std::mutex> lk(mu);
      auto it = table.find(key);
      if (it == table.end()) return;
      usage_bytes -= it->second->second.size() + it->second->first.size();
      lru.erase(it->second);
      table.erase(it);
    }

    size_t usage() const {
      std::lock_guard<std::mutex> lk(mu);
      return usage_bytes;
    }

    mutable std::mutex mu;
    size_t capacity;
    size_t usage_bytes = 0;
    std::list<std::pair<std::string, std::string>> lru;  // front = most recent
    std::unordered_map<std::string, decltype(lru.begin())> table;
  };

  Shard* GetShard(const std::string& key) {
    size_t h = std::hash<std::string>{}(key);
    return shards_[h % num_shards_].get();
  }

  size_t capacity_;
  size_t num_shards_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> misses_{0};
};

}  // namespace kvdb
