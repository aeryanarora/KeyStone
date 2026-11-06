#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>

#include "common/arena.h"
#include "common/env.h"

namespace kvdb {

// Concurrency contract (same as LevelDB's): writes must be externally
// serialized, reads require no locking at all. Correctness comes from
// publishing each node with a release-store and reading next pointers with
// acquire-loads, so a reader either doesn't see a node or sees it fully built.
//
// Key is a trivially-copyable handle (we use `const char*` into an Arena).
// Comparator must provide `int operator()(Key, Key) const`.
template <typename Key, class Comparator>
class SkipList {
 public:
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // REQUIRES: no key equal to `key` is currently in the list.
  void Insert(const Key& key);
  bool Contains(const Key& key) const;
  size_t ApproximateNumEntries() const { return count_.load(std::memory_order_relaxed); }

  class Iterator {
   public:
    explicit Iterator(const SkipList* list) : list_(list), node_(nullptr) {}

    bool Valid() const { return node_ != nullptr; }
    const Key& key() const {
      assert(Valid());
      return node_->key;
    }
    void Next() {
      assert(Valid());
      node_ = node_->Next(0);
    }
    void Prev() {
      assert(Valid());
      node_ = list_->FindLessThan(node_->key);
      if (node_ == list_->head_) node_ = nullptr;
    }
    void Seek(const Key& target) { node_ = list_->FindGreaterOrEqual(target, nullptr); }
    void SeekToFirst() { node_ = list_->head_->Next(0); }
    void SeekToLast() {
      node_ = list_->FindLast();
      if (node_ == list_->head_) node_ = nullptr;
    }

   private:
    const SkipList* list_;
    typename SkipList::Node* node_;
  };

 private:
  static constexpr int kMaxHeight = 12;

  struct Node {
    explicit Node(const Key& k) : key(k) {}
    Key const key;

    Node* Next(int n) { return next_[n].load(std::memory_order_acquire); }
    void SetNext(int n, Node* x) { next_[n].store(x, std::memory_order_release); }
    // Only safe before the node is linked into the list.
    Node* NoBarrier_Next(int n) { return next_[n].load(std::memory_order_relaxed); }
    void NoBarrier_SetNext(int n, Node* x) { next_[n].store(x, std::memory_order_relaxed); }

   private:
    std::atomic<Node*> next_[1];  // flexible array; real length = node height
  };

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return compare_(a, b) == 0; }
  bool KeyIsAfterNode(const Key& key, Node* n) const {
    return (n != nullptr) && (compare_(n->key, key) < 0);
  }

  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;
  Node* FindLessThan(const Key& key) const;
  Node* FindLast() const;

  Comparator const compare_;
  Arena* const arena_;
  Node* const head_;
  std::atomic<int> max_height_;
  std::atomic<size_t> count_{0};
  mutable Random rnd_;
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(const Key& key,
                                                                            int height) {
  char* mem = arena_->AllocateAligned(sizeof(Node) +
                                      sizeof(std::atomic<Node*>) * static_cast<size_t>(height - 1));
  Node* n = new (mem) Node(key);
  for (int i = 0; i < height; i++) n->NoBarrier_SetNext(i, nullptr);
  return n;
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp), arena_(arena), head_(NewNode(Key{}, kMaxHeight)), max_height_(1), rnd_(0) {}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) height++;
  return height;
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(
    const Key& key, Node** prev) const {
  Node* x = head_;
  int level = max_height_.load(std::memory_order_relaxed) - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) return next;
      level--;
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLessThan(
    const Key& key) const {
  Node* x = head_;
  int level = max_height_.load(std::memory_order_relaxed) - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next != nullptr && compare_(next->key, key) < 0) {
      x = next;
    } else {
      if (level == 0) return x;
      level--;
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast() const {
  Node* x = head_;
  int level = max_height_.load(std::memory_order_relaxed) - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) return x;
      level--;
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);
  (void)x;  // duplicate internal keys are impossible: sequence numbers differ

  int height = RandomHeight();
  int cur_max = max_height_.load(std::memory_order_relaxed);
  if (height > cur_max) {
    for (int i = cur_max; i < height; i++) prev[i] = head_;
    // Readers racing here observe either the old or the new height; both are
    // valid because the new levels still terminate in nullptr.
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);  // release-store publishes the node
  }
  count_.fetch_add(1, std::memory_order_relaxed);
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  return x != nullptr && Equal(key, x->key);
}

}  // namespace kvdb
