#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace kvdb {

// Bump allocator backing the MemTable skiplist. Individual nodes are never
// freed; the whole arena dies with the MemTable, which is exactly the lifetime
// we want and removes per-node malloc/free from the write path.
class Arena {
 public:
  Arena() = default;
  ~Arena() {
    for (char* b : blocks_) delete[] b;
  }
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  char* Allocate(size_t bytes) {
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
      char* result = alloc_ptr_;
      alloc_ptr_ += bytes;
      alloc_bytes_remaining_ -= bytes;
      return result;
    }
    return AllocateFallback(bytes);
  }

  char* AllocateAligned(size_t bytes) {
    constexpr size_t kAlign = alignof(std::max_align_t);
    size_t mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (kAlign - 1);
    size_t slop = (mod == 0) ? 0 : kAlign - mod;
    size_t needed = bytes + slop;
    if (needed <= alloc_bytes_remaining_) {
      char* result = alloc_ptr_ + slop;
      alloc_ptr_ += needed;
      alloc_bytes_remaining_ -= needed;
      return result;
    }
    return AllocateFallback(bytes);  // fresh blocks are already aligned
  }

  size_t MemoryUsage() const { return memory_usage_.load(std::memory_order_relaxed); }

 private:
  char* AllocateFallback(size_t bytes) {
    if (bytes > kBlockSize / 4) {
      // Large request: give it its own block so we don't waste the remainder.
      return AllocateNewBlock(bytes);
    }
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }

  char* AllocateNewBlock(size_t block_bytes) {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
  }

  static constexpr size_t kBlockSize = 4096;

  char* alloc_ptr_ = nullptr;
  size_t alloc_bytes_remaining_ = 0;
  std::vector<char*> blocks_;
  std::atomic<size_t> memory_usage_{0};
};

}  // namespace kvdb
