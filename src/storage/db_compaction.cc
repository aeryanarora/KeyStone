// Read path, flush, and compaction for LsmStore.
#include <algorithm>
#include <cinttypes>

#include "common/coding.h"
#include "common/env.h"
#include "common/logger.h"
#include "storage/db.h"

namespace kvdb {

namespace {
constexpr uint64_t kTargetFileSize = 2ull * 1024 * 1024;
}

std::shared_ptr<Version> LsmStore::CurrentVersion() const {
  std::lock_guard<std::mutex> lk(mu_);
  return current_;
}

uint64_t LsmStore::MaxBytesForLevel(int level) const {
  if (level <= 1) return options_.l1_size_bytes;
  uint64_t result = options_.l1_size_bytes;
  for (int i = 1; i < level; i++) result *= static_cast<uint64_t>(options_.level_size_multiplier);
  return result;
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------
Status LsmStore::GetFromVersion(const std::shared_ptr<Version>& v, std::string_view key,
                                SequenceNumber snapshot, std::string* value, SequenceNumber* seq,
                                bool* found) {
  *found = false;
  for (size_t lvl = 0; lvl < v->levels.size(); lvl++) {
    const auto& files = v->levels[lvl];
    if (files.empty()) continue;

    if (lvl == 0) {
      // L0 files overlap; they are stored newest-first, so the first hit wins.
      for (const auto& f : files) {
        Status s;
        if (f->reader->Get(key, snapshot, value, &s, seq)) {
          if (s.IsCorruption() || s.code() == Status::Code::kIOError) return s;
          *found = true;
          return s;
        }
      }
      continue;
    }

    // Levels >= 1 hold disjoint ranges: binary search for the one candidate.
    auto it = std::lower_bound(files.begin(), files.end(), key,
                               [](const std::shared_ptr<TableEntry>& f, std::string_view k) {
                                 return ExtractUserKey(f->meta.largest) < k;
                               });
    if (it == files.end()) continue;
    if (key < ExtractUserKey((*it)->meta.smallest)) continue;

    Status s;
    if ((*it)->reader->Get(key, snapshot, value, &s, seq)) {
      if (s.IsCorruption() || s.code() == Status::Code::kIOError) return s;
      *found = true;
      return s;
    }
  }
  return Status::NotFound();
}

Status LsmStore::GetWithSeq(const ReadOptions& opts, std::string_view key, std::string* value,
                            SequenceNumber* out_seq) {
  stat_reads_.fetch_add(1, std::memory_order_relaxed);

  std::shared_ptr<MemTable> mem, imm;
  std::shared_ptr<Version> version;
  SequenceNumber snapshot;
  {
    std::lock_guard<std::mutex> lk(mu_);
    mem = mem_;
    imm = imm_;
    version = current_;
    snapshot = (opts.snapshot != 0) ? opts.snapshot : last_sequence_.load(std::memory_order_acquire);
  }
  // Everything below runs without the DB lock: memtables and SSTables are
  // either immutable or safe for concurrent readers.

  if (out_seq != nullptr) *out_seq = 0;

  Status s;
  if (mem->Get(key, snapshot, value, &s, out_seq)) {
    if (s.ok()) stat_bytes_read_.fetch_add(value->size(), std::memory_order_relaxed);
    return s;
  }
  if (imm != nullptr && imm->Get(key, snapshot, value, &s, out_seq)) {
    if (s.ok()) stat_bytes_read_.fetch_add(value->size(), std::memory_order_relaxed);
    return s;
  }

  bool found = false;
  s = GetFromVersion(version, key, snapshot, value, out_seq, &found);
  if (found && s.ok()) stat_bytes_read_.fetch_add(value->size(), std::memory_order_relaxed);
  if (!found) return Status::NotFound();
  return s;
}

Status LsmStore::Get(const ReadOptions& opts, std::string_view key, std::string* value) {
  return GetWithSeq(opts, key, value, nullptr);
}

std::unique_ptr<InternalIterator> LsmStore::NewInternalIterator(
    const std::shared_ptr<Version>& v, const std::shared_ptr<const MemTable>& mem,
    const std::shared_ptr<const MemTable>& imm) const {
  std::vector<std::unique_ptr<InternalIterator>> children;
  children.push_back(std::make_unique<MemTableIteratorWrapper>(mem));
  if (imm != nullptr) children.push_back(std::make_unique<MemTableIteratorWrapper>(imm));
  for (const auto& level : v->levels) {
    for (const auto& f : level) {
      children.push_back(std::make_unique<SSTableIteratorWrapper>(f->reader));
    }
  }
  return std::make_unique<MergingIterator>(std::move(children));
}

Status LsmStore::Scan(const ReadOptions& opts, std::string_view start, std::string_view end,
                      size_t limit, std::vector<std::pair<std::string, std::string>>* out) {
  out->clear();

  std::shared_ptr<MemTable> mem, imm;
  std::shared_ptr<Version> version;
  SequenceNumber snapshot;
  {
    std::lock_guard<std::mutex> lk(mu_);
    mem = mem_;
    imm = imm_;
    version = current_;
    snapshot = (opts.snapshot != 0) ? opts.snapshot : last_sequence_.load(std::memory_order_acquire);
  }

  auto iter = NewInternalIterator(version, mem, imm);
  const std::string seek_key = MakeInternalKey(start, kMaxSequenceNumber, kTypeValue);
  iter->Seek(seek_key);

  std::string last_user_key;
  bool have_last = false;
  while (iter->Valid()) {
    std::string_view ikey = iter->internal_key();
    std::string_view ukey = ExtractUserKey(ikey);

    if (!end.empty() && ukey >= end) break;

    if (have_last && ukey == last_user_key) {
      iter->Next();  // older version of a key we already resolved
      continue;
    }

    if (ExtractSequence(ikey) > snapshot) {
      iter->Next();  // not visible in this snapshot; try an older version
      continue;
    }

    last_user_key.assign(ukey.data(), ukey.size());
    have_last = true;

    if (ExtractValueType(ikey) == kTypeValue) {
      std::string_view v = iter->value();
      out->emplace_back(last_user_key, std::string(v));
      if (limit > 0 && out->size() >= limit) break;
    }
    iter->Next();
  }
  stat_reads_.fetch_add(1, std::memory_order_relaxed);
  return iter->status();
}

// ---------------------------------------------------------------------------
// Background scheduling
// ---------------------------------------------------------------------------
void LsmStore::MaybeScheduleWork() {
  if (bg_work_scheduled_ || shutting_down_ || !bg_running_) return;
  if (imm_ == nullptr && !NeedsCompaction()) return;
  bg_work_scheduled_ = true;
  bg_cv_.notify_all();
}

bool LsmStore::NeedsCompaction() const { return PickCompactionLevel() >= 0; }

int LsmStore::PickCompactionLevel() const {
  int best_level = -1;
  double best_score = 1.0;
  for (int lvl = 0; lvl + 1 < static_cast<int>(current_->levels.size()); lvl++) {
    double score;
    if (lvl == 0) {
      // L0 is scored by file count, not bytes: each L0 file must be consulted
      // on every read that misses the memtable.
      score = static_cast<double>(current_->NumFiles(0)) /
              static_cast<double>(options_.l0_compaction_trigger);
    } else {
      score = static_cast<double>(current_->TotalBytes(lvl)) /
              static_cast<double>(MaxBytesForLevel(lvl));
    }
    if (score >= best_score) {
      best_score = score;
      best_level = lvl;
    }
  }
  return best_level;
}

void LsmStore::BackgroundLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (true) {
    while (!shutting_down_ && !bg_work_scheduled_) {
      bg_cv_.wait_for(lk, std::chrono::milliseconds(200));
      if (imm_ != nullptr || NeedsCompaction()) bg_work_scheduled_ = true;
    }
    if (shutting_down_) break;

    Status s;
    if (imm_ != nullptr) {
      s = FlushImmutable(lk);
    } else if (NeedsCompaction()) {
      s = DoCompaction(lk);
    }
    if (!s.ok()) {
      LOG_ERROR("background work failed: %s", s.ToString().c_str());
      bg_error_ = s;
    }

    RemoveObsoleteFiles();
    bg_work_scheduled_ = (imm_ != nullptr || NeedsCompaction());
    bg_cv_.notify_all();
  }
  bg_work_scheduled_ = false;
  bg_cv_.notify_all();
}

void LsmStore::WaitForBackgroundWork() {
  std::unique_lock<std::mutex> lk(mu_);
  while (bg_running_ && !shutting_down_ && (bg_work_scheduled_ || imm_ != nullptr)) {
    bg_cv_.wait_for(lk, std::chrono::milliseconds(20));
  }
}

// ---------------------------------------------------------------------------
// Flush: memtable -> L0 SSTable
// ---------------------------------------------------------------------------
Status LsmStore::BuildTableFromMemTable(const std::shared_ptr<const MemTable>& mem,
                                        FileMetaData* meta) {
  meta->number = next_file_number_++;
  meta->level = 0;
  const std::string path = TablePath(meta->number);

  SSTableBuilder builder(options_.block_size, options_.bloom_bits_per_key);
  KVDB_RETURN_IF_ERROR(builder.Open(path));

  auto it = mem->NewIterator();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    KVDB_RETURN_IF_ERROR(builder.Add(it->internal_key(), it->value()));
  }

  if (builder.NumEntries() == 0) {
    builder.Abandon();
    meta->num_entries = 0;
    return Status::OK();
  }

  KVDB_RETURN_IF_ERROR(builder.Finish());
  meta->file_size = builder.FileSize();
  meta->smallest = builder.smallest_key();
  meta->largest = builder.largest_key();
  meta->smallest_seq = builder.smallest_seq();
  meta->largest_seq = builder.largest_seq();
  meta->num_entries = builder.NumEntries();
  return Status::OK();
}

Status LsmStore::FlushImmutable(std::unique_lock<std::mutex>& lk) {
  if (imm_ == nullptr) return Status::OK();
  auto mem = std::const_pointer_cast<const MemTable>(imm_);
  const uint64_t start_us = NowMicros();

  FileMetaData meta;
  meta.number = next_file_number_++;
  meta.level = 0;
  const std::string path = TablePath(meta.number);

  // Table construction is pure I/O over an immutable memtable: do it without
  // the DB lock so writers keep making progress.
  lk.unlock();
  SSTableBuilder builder(options_.block_size, options_.bloom_bits_per_key);
  Status s = builder.Open(path);
  if (s.ok()) {
    auto it = mem->NewIterator();
    for (it->SeekToFirst(); it->Valid() && s.ok(); it->Next()) {
      s = builder.Add(it->internal_key(), it->value());
    }
  }
  bool empty = builder.NumEntries() == 0;
  if (s.ok()) {
    s = empty ? builder.Abandon() : builder.Finish();
  }
  if (s.ok() && !empty) {
    meta.file_size = builder.FileSize();
    meta.smallest = builder.smallest_key();
    meta.largest = builder.largest_key();
    meta.smallest_seq = builder.smallest_seq();
    meta.largest_seq = builder.largest_seq();
    meta.num_entries = builder.NumEntries();
  }
  std::shared_ptr<TableEntry> entry;
  if (s.ok() && !empty) s = OpenTable(meta, &entry);
  lk.lock();

  if (!s.ok()) return s;

  if (!empty) {
    auto next = std::make_shared<Version>(*current_);
    next->levels[0].insert(next->levels[0].begin(), std::move(entry));  // newest first
    current_ = std::move(next);
  }
  imm_ = nullptr;
  Status ms = WriteManifest();
  if (!ms.ok()) return ms;

  stat_flushes_.fetch_add(1, std::memory_order_relaxed);
  LOG_DEBUG("flushed memtable -> %s (%" PRIu64 " entries, %" PRIu64 " bytes) in %" PRIu64 "us",
            fileutil::BaseName(path).c_str(), meta.num_entries, meta.file_size,
            NowMicros() - start_us);
  bg_cv_.notify_all();
  return Status::OK();
}

Status LsmStore::Flush() {
  std::unique_lock<std::mutex> lk(mu_);
  if (mem_->empty() && imm_ == nullptr) return Status::OK();

  while (imm_ != nullptr) {
    if (!bg_running_) {
      KVDB_RETURN_IF_ERROR(FlushImmutable(lk));
    } else {
      MaybeScheduleWork();
      bg_cv_.wait_for(lk, std::chrono::milliseconds(20));
    }
  }
  if (!mem_->empty()) {
    imm_ = mem_;
    mem_ = std::make_shared<MemTable>();
    KVDB_RETURN_IF_ERROR(RollNewWal());
    if (!bg_running_) {
      KVDB_RETURN_IF_ERROR(FlushImmutable(lk));
    } else {
      MaybeScheduleWork();
      while (imm_ != nullptr && !shutting_down_) {
        bg_cv_.wait_for(lk, std::chrono::milliseconds(20));
      }
    }
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Compaction: level N + overlapping level N+1 -> new level N+1 files
// ---------------------------------------------------------------------------
Status LsmStore::DoCompaction(std::unique_lock<std::mutex>& lk) {
  const int level = PickCompactionLevel();
  if (level < 0) return Status::OK();
  const auto next_level = static_cast<size_t>(level + 1);

  auto version = current_;
  std::vector<std::shared_ptr<TableEntry>> inputs;

  if (level == 0) {
    // L0 files overlap arbitrarily, so all of them go in together.
    inputs = version->levels[0];
  } else {
    const auto& files = version->levels[static_cast<size_t>(level)];
    if (files.empty()) return Status::OK();
    // Round-robin across the key space so compaction work spreads evenly.
    const std::string& ptr = compact_pointer_[static_cast<size_t>(level)];
    size_t pick = 0;
    if (!ptr.empty()) {
      for (size_t i = 0; i < files.size(); i++) {
        if (InternalKeyComparator::Compare(files[i]->meta.largest, ptr) > 0) {
          pick = i;
          break;
        }
      }
    }
    inputs.push_back(files[pick]);
  }
  if (inputs.empty()) return Status::OK();

  // Expand with every file in level+1 whose range overlaps the inputs.
  std::string smallest = inputs[0]->meta.smallest;
  std::string largest = inputs[0]->meta.largest;
  for (const auto& f : inputs) {
    if (InternalKeyComparator::Compare(f->meta.smallest, smallest) < 0) smallest = f->meta.smallest;
    if (InternalKeyComparator::Compare(f->meta.largest, largest) > 0) largest = f->meta.largest;
  }
  std::string_view smallest_user = ExtractUserKey(smallest);
  std::string_view largest_user = ExtractUserKey(largest);

  std::vector<std::shared_ptr<TableEntry>> overlaps;
  if (next_level < version->levels.size()) {
    for (const auto& f : version->levels[next_level]) {
      if (ExtractUserKey(f->meta.largest) < smallest_user) continue;
      if (ExtractUserKey(f->meta.smallest) > largest_user) continue;
      overlaps.push_back(f);
    }
  }

  // Tombstones can only be dropped when nothing below could still hold an
  // older value for that key.
  bool bottom_level = true;
  for (size_t lvl = next_level + 1; lvl < version->levels.size(); lvl++) {
    if (!version->levels[lvl].empty()) {
      bottom_level = false;
      break;
    }
  }

  std::vector<std::shared_ptr<TableEntry>> all_inputs = inputs;
  all_inputs.insert(all_inputs.end(), overlaps.begin(), overlaps.end());

  uint64_t first_output_number = next_file_number_;
  const uint64_t start_us = NowMicros();
  const SequenceNumber oldest_snapshot = 0;  // no long-lived snapshots yet
  (void)oldest_snapshot;

  // Merge outside the lock: inputs are immutable.
  lk.unlock();

  std::vector<std::unique_ptr<InternalIterator>> children;
  children.reserve(all_inputs.size());
  for (const auto& f : all_inputs) {
    children.push_back(std::make_unique<SSTableIteratorWrapper>(f->reader));
  }
  MergingIterator merged(std::move(children));

  std::vector<FileMetaData> outputs;
  std::unique_ptr<SSTableBuilder> builder;
  uint64_t file_number = first_output_number;
  Status s;
  std::string last_user_key;
  bool have_last = false;
  uint64_t dropped = 0, kept = 0, bytes_in = 0;
  for (const auto& f : all_inputs) bytes_in += f->meta.file_size;

  for (merged.SeekToFirst(); merged.Valid() && s.ok(); merged.Next()) {
    std::string_view ikey = merged.internal_key();
    std::string_view ukey = ExtractUserKey(ikey);

    if (have_last && ukey == last_user_key) {
      dropped++;  // shadowed by a newer version already emitted
      continue;
    }
    last_user_key.assign(ukey.data(), ukey.size());
    have_last = true;

    if (ExtractValueType(ikey) == kTypeDeletion && bottom_level) {
      dropped++;  // tombstone with nothing left to hide: reclaim the space
      continue;
    }

    if (!builder) {
      builder = std::make_unique<SSTableBuilder>(options_.block_size, options_.bloom_bits_per_key);
      s = builder->Open(TablePath(file_number));
      if (!s.ok()) break;
    }
    s = builder->Add(ikey, merged.value());
    kept++;

    if (s.ok() && builder->FileSize() >= kTargetFileSize) {
      s = builder->Finish();
      if (s.ok()) {
        FileMetaData meta;
        meta.number = file_number;
        meta.level = static_cast<int>(next_level);
        meta.file_size = builder->FileSize();
        meta.smallest = builder->smallest_key();
        meta.largest = builder->largest_key();
        meta.smallest_seq = builder->smallest_seq();
        meta.largest_seq = builder->largest_seq();
        meta.num_entries = builder->NumEntries();
        outputs.push_back(std::move(meta));
      }
      builder.reset();
      file_number++;
    }
  }
  if (s.ok()) s = merged.status();

  if (s.ok() && builder) {
    s = builder->Finish();
    if (s.ok()) {
      FileMetaData meta;
      meta.number = file_number;
      meta.level = static_cast<int>(next_level);
      meta.file_size = builder->FileSize();
      meta.smallest = builder->smallest_key();
      meta.largest = builder->largest_key();
      meta.smallest_seq = builder->smallest_seq();
      meta.largest_seq = builder->largest_seq();
      meta.num_entries = builder->NumEntries();
      outputs.push_back(std::move(meta));
    }
    builder.reset();
    file_number++;
  }

  std::vector<std::shared_ptr<TableEntry>> new_entries;
  if (s.ok()) {
    for (const auto& meta : outputs) {
      std::shared_ptr<TableEntry> entry;
      s = OpenTable(meta, &entry);
      if (!s.ok()) break;
      new_entries.push_back(std::move(entry));
    }
  }

  lk.lock();
  next_file_number_ = std::max(next_file_number_, file_number);

  if (!s.ok()) {
    for (const auto& meta : outputs) fileutil::DeleteFile(TablePath(meta.number));
    return s;
  }

  // Install the new version: drop the inputs, add the outputs.
  auto next = std::make_shared<Version>(*current_);
  if (next->levels.size() <= next_level) next->levels.resize(next_level + 1);

  auto remove_from = [&](size_t lvl, const std::vector<std::shared_ptr<TableEntry>>& drop) {
    auto& files = next->levels[lvl];
    for (const auto& d : drop) {
      files.erase(std::remove_if(files.begin(), files.end(),
                                 [&](const std::shared_ptr<TableEntry>& f) {
                                   return f->meta.number == d->meta.number;
                                 }),
                  files.end());
      obsolete_.push_back(d);
    }
  };
  remove_from(static_cast<size_t>(level), inputs);
  remove_from(next_level, overlaps);

  for (auto& e : new_entries) next->levels[next_level].push_back(std::move(e));
  std::sort(next->levels[next_level].begin(), next->levels[next_level].end(),
            [](const auto& a, const auto& b) {
              return InternalKeyComparator::Compare(a->meta.smallest, b->meta.smallest) < 0;
            });

  current_ = std::move(next);
  compact_pointer_[static_cast<size_t>(level)] = largest;
  KVDB_RETURN_IF_ERROR(WriteManifest());

  stat_compactions_.fetch_add(1, std::memory_order_relaxed);
  stat_bytes_compacted_.fetch_add(bytes_in, std::memory_order_relaxed);
  LOG_DEBUG("compacted L%d(%zu files) + L%zu(%zu files) -> %zu files, kept=%" PRIu64
            " dropped=%" PRIu64 " in %" PRIu64 "us",
            level, inputs.size(), next_level, overlaps.size(), outputs.size(), kept, dropped,
            NowMicros() - start_us);
  bg_cv_.notify_all();
  return Status::OK();
}

Status LsmStore::CompactRange() {
  KVDB_RETURN_IF_ERROR(Flush());
  std::unique_lock<std::mutex> lk(mu_);
  int guard = 0;
  while (NeedsCompaction() && guard++ < 100) {
    if (!bg_running_) {
      KVDB_RETURN_IF_ERROR(DoCompaction(lk));
      RemoveObsoleteFiles();
    } else {
      MaybeScheduleWork();
      bg_cv_.wait_for(lk, std::chrono::milliseconds(20));
    }
  }
  return Status::OK();
}

void LsmStore::RemoveObsoleteFiles() {
  // A retired table is only unlinked once no Version and no in-flight reader
  // still holds a reference to it.
  auto it = obsolete_.begin();
  while (it != obsolete_.end()) {
    if (it->use_count() == 1 && (*it)->reader.use_count() == 1) {
      const std::string path = TablePath((*it)->meta.number);
      (*it)->reader.reset();
      fileutil::DeleteFile(path);
      it = obsolete_.erase(it);
    } else {
      ++it;
    }
  }
}

// ---------------------------------------------------------------------------
DbStats LsmStore::GetStats() const {
  DbStats st;
  st.num_writes = stat_writes_.load(std::memory_order_relaxed);
  st.num_reads = stat_reads_.load(std::memory_order_relaxed);
  st.num_batches = stat_batches_.load(std::memory_order_relaxed);
  st.num_flushes = stat_flushes_.load(std::memory_order_relaxed);
  st.num_compactions = stat_compactions_.load(std::memory_order_relaxed);
  st.bytes_written = stat_bytes_written_.load(std::memory_order_relaxed);
  st.bytes_read = stat_bytes_read_.load(std::memory_order_relaxed);
  st.bytes_compacted = stat_bytes_compacted_.load(std::memory_order_relaxed);
  st.wal_syncs = stat_wal_syncs_.load(std::memory_order_relaxed);
  st.write_stalls = stat_write_stalls_.load(std::memory_order_relaxed);
  st.last_sequence = last_sequence_.load(std::memory_order_acquire);
  st.cache_hit_rate = block_cache_->hit_rate();

  std::lock_guard<std::mutex> lk(mu_);
  st.memtable_bytes = mem_->ApproximateMemoryUsage();
  st.immutable_bytes = (imm_ != nullptr) ? imm_->ApproximateMemoryUsage() : 0;
  for (size_t lvl = 0; lvl < current_->levels.size(); lvl++) {
    st.files_per_level.push_back(static_cast<int>(current_->levels[lvl].size()));
    st.bytes_per_level.push_back(current_->TotalBytes(static_cast<int>(lvl)));
  }
  return st;
}

}  // namespace kvdb
