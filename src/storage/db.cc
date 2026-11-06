#include "storage/db.h"

#include <algorithm>
#include <cinttypes>
#include <sstream>

#include "common/coding.h"
#include "common/crc32c.h"
#include "common/env.h"
#include "common/logger.h"

namespace kvdb {

namespace {
constexpr uint64_t kManifestMagic = 0x6b76646d616e0001ull;
constexpr uint64_t kTargetFileSize = 2ull * 1024 * 1024;
}  // namespace

uint64_t Version::TotalBytes(int level) const {
  if (level >= static_cast<int>(levels.size())) return 0;
  uint64_t total = 0;
  for (const auto& f : levels[static_cast<size_t>(level)]) total += f->meta.file_size;
  return total;
}

std::string DbStats::ToString() const {
  std::ostringstream os;
  os << "writes=" << num_writes << " reads=" << num_reads << " wal_batches=" << num_batches
     << " flushes=" << num_flushes << " compactions=" << num_compactions
     << " bytes_written=" << bytes_written << " bytes_compacted=" << bytes_compacted
     << " wal_syncs=" << wal_syncs << " stalls=" << write_stalls
     << " memtable_bytes=" << memtable_bytes << " last_seq=" << last_sequence
     << " cache_hit_rate=" << cache_hit_rate << " levels=[";
  for (size_t i = 0; i < files_per_level.size(); i++) {
    if (i) os << ",";
    os << files_per_level[i] << ":" << bytes_per_level[i];
  }
  os << "]";
  return os.str();
}

// ---------------------------------------------------------------------------
// Construction / open / close
// ---------------------------------------------------------------------------
LsmStore::LsmStore(const Options& options) : options_(options) {
  block_cache_ = std::make_unique<BlockCache>(options_.block_cache_bytes);
  current_ = std::make_shared<Version>();
  current_->levels.resize(static_cast<size_t>(options_.max_levels));
  compact_pointer_.resize(static_cast<size_t>(options_.max_levels));
  mem_ = std::make_shared<MemTable>();
}

LsmStore::~LsmStore() { Close(); }

Status LsmStore::Open(const Options& options, std::unique_ptr<LsmStore>* store) {
  auto db = std::unique_ptr<LsmStore>(new LsmStore(options));

  if (options.create_if_missing) {
    KVDB_RETURN_IF_ERROR(fileutil::CreateDirRecursive(options.data_dir));
  } else if (!fileutil::FileExists(options.data_dir)) {
    return Status::InvalidArgument("data dir does not exist: " + options.data_dir);
  }
  if (options.error_if_exists && fileutil::FileExists(db->ManifestPath())) {
    return Status::InvalidArgument("database already exists: " + options.data_dir);
  }

  KVDB_RETURN_IF_ERROR(db->Recover());

  if (options.background_compaction) {
    db->bg_running_ = true;
    db->bg_thread_ = std::thread([raw = db.get()] { raw->BackgroundLoop(); });
  }

  *store = std::move(db);
  return Status::OK();
}

Status LsmStore::Close() {
  {
    std::unique_lock<std::mutex> lk(mu_);
    if (shutting_down_) return Status::OK();
    shutting_down_ = true;
    bg_cv_.notify_all();
  }
  if (bg_thread_.joinable()) bg_thread_.join();

  std::unique_lock<std::mutex> lk(mu_);
  if (!crashed_) {
    if (wal_) {
      wal_->Sync();
      wal_->Close();
    }
    // Flushing on close is an optimisation, not a correctness requirement:
    // whatever stays in the memtable is still in the WAL.
  }
  obsolete_.clear();
  return Status::OK();
}

void LsmStore::SimulateCrash() {
  {
    std::unique_lock<std::mutex> lk(mu_);
    crashed_ = true;
    shutting_down_ = true;
    bg_cv_.notify_all();
  }
  if (bg_thread_.joinable()) bg_thread_.join();
  // Deliberately do NOT sync or close the WAL: whatever the OS has not
  // persisted is lost, exactly as in a real crash.
}

std::string LsmStore::WalPath(uint64_t n) const {
  return fileutil::JoinPath(options_.data_dir, StringPrintf("%06" PRIu64 ".wal", n));
}
std::string LsmStore::TablePath(uint64_t n) const {
  return fileutil::JoinPath(options_.data_dir, StringPrintf("%06" PRIu64 ".sst", n));
}
std::string LsmStore::ManifestPath() const {
  return fileutil::JoinPath(options_.data_dir, "MANIFEST");
}

// ---------------------------------------------------------------------------
// Manifest: the authoritative list of live SSTables.
// Rewritten atomically (tmp + rename + fsync) on every version change.
// ---------------------------------------------------------------------------
Status LsmStore::WriteManifest() {
  std::string body;
  PutFixed64(&body, kManifestMagic);
  PutFixed64(&body, next_file_number_);
  PutFixed64(&body, last_sequence_.load(std::memory_order_acquire));
  PutFixed64(&body, log_number_);

  uint32_t num_files = 0;
  for (const auto& level : current_->levels) num_files += static_cast<uint32_t>(level.size());
  PutFixed32(&body, num_files);

  for (size_t lvl = 0; lvl < current_->levels.size(); lvl++) {
    for (const auto& f : current_->levels[lvl]) {
      PutFixed32(&body, static_cast<uint32_t>(lvl));
      PutFixed64(&body, f->meta.number);
      PutFixed64(&body, f->meta.file_size);
      PutLengthPrefixed(&body, f->meta.smallest);
      PutLengthPrefixed(&body, f->meta.largest);
      PutFixed64(&body, f->meta.smallest_seq);
      PutFixed64(&body, f->meta.largest_seq);
      PutFixed64(&body, f->meta.num_entries);
    }
  }

  std::string out;
  PutFixed32(&out, crc32c::Mask(crc32c::Value(body)));
  out.append(body);
  return fileutil::WriteStringToFileAtomic(ManifestPath(), out);
}

Status LsmStore::LoadManifest(bool* found) {
  *found = false;
  const std::string path = ManifestPath();
  if (!fileutil::FileExists(path)) return Status::OK();

  std::string data;
  KVDB_RETURN_IF_ERROR(fileutil::ReadFileToString(path, &data));
  if (data.size() < 4) return Status::Corruption("manifest truncated");

  std::string_view in(data);
  uint32_t stored_crc = 0;
  GetFixed32(&in, &stored_crc);
  if (crc32c::Unmask(stored_crc) != crc32c::Value(in)) {
    return Status::Corruption("manifest checksum mismatch");
  }

  uint64_t magic = 0, last_seq = 0;
  if (!GetFixed64(&in, &magic) || magic != kManifestMagic) {
    return Status::Corruption("bad manifest magic");
  }
  if (!GetFixed64(&in, &next_file_number_) || !GetFixed64(&in, &last_seq) ||
      !GetFixed64(&in, &log_number_)) {
    return Status::Corruption("bad manifest header");
  }
  last_sequence_.store(last_seq, std::memory_order_release);

  uint32_t num_files = 0;
  if (!GetFixed32(&in, &num_files)) return Status::Corruption("bad manifest file count");

  for (uint32_t i = 0; i < num_files; i++) {
    uint32_t level = 0;
    FileMetaData meta;
    if (!GetFixed32(&in, &level) || !GetFixed64(&in, &meta.number) ||
        !GetFixed64(&in, &meta.file_size) || !GetLengthPrefixedString(&in, &meta.smallest) ||
        !GetLengthPrefixedString(&in, &meta.largest) || !GetFixed64(&in, &meta.smallest_seq) ||
        !GetFixed64(&in, &meta.largest_seq) || !GetFixed64(&in, &meta.num_entries)) {
      return Status::Corruption("bad manifest file record");
    }
    meta.level = static_cast<int>(level);
    std::shared_ptr<TableEntry> entry;
    KVDB_RETURN_IF_ERROR(OpenTable(meta, &entry));
    if (meta.level >= static_cast<int>(current_->levels.size())) {
      current_->levels.resize(static_cast<size_t>(meta.level) + 1);
    }
    current_->levels[static_cast<size_t>(meta.level)].push_back(std::move(entry));
  }

  // L0 files may overlap, so keep them newest-first; deeper levels are disjoint
  // and are kept sorted by key range for binary search.
  for (size_t lvl = 0; lvl < current_->levels.size(); lvl++) {
    auto& files = current_->levels[lvl];
    if (lvl == 0) {
      std::sort(files.begin(), files.end(),
                [](const auto& a, const auto& b) { return a->meta.number > b->meta.number; });
    } else {
      std::sort(files.begin(), files.end(), [](const auto& a, const auto& b) {
        return InternalKeyComparator::Compare(a->meta.smallest, b->meta.smallest) < 0;
      });
    }
  }

  *found = true;
  return Status::OK();
}

Status LsmStore::OpenTable(const FileMetaData& meta, std::shared_ptr<TableEntry>* out) {
  auto entry = std::make_shared<TableEntry>();
  entry->meta = meta;
  entry->reader = std::make_shared<SSTableReader>();
  KVDB_RETURN_IF_ERROR(entry->reader->Open(TablePath(meta.number), block_cache_.get(), meta.number));
  *out = std::move(entry);
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------
Status LsmStore::Recover() {
  std::unique_lock<std::mutex> lk(mu_);

  bool found_manifest = false;
  KVDB_RETURN_IF_ERROR(LoadManifest(&found_manifest));

  // Collect WAL files at or after the manifest's log number. Anything older is
  // already captured in an SSTable.
  std::vector<std::string> entries;
  KVDB_RETURN_IF_ERROR(fileutil::ListDir(options_.data_dir, &entries));
  std::vector<uint64_t> logs;
  for (const auto& name : entries) {
    if (name.size() > 4 && name.compare(name.size() - 4, 4, ".wal") == 0) {
      uint64_t num = std::strtoull(name.c_str(), nullptr, 10);
      if (num >= log_number_) logs.push_back(num);
      next_file_number_ = std::max(next_file_number_, num + 1);
    } else if (name.size() > 4 && name.compare(name.size() - 4, 4, ".sst") == 0) {
      uint64_t num = std::strtoull(name.c_str(), nullptr, 10);
      next_file_number_ = std::max(next_file_number_, num + 1);
    }
  }
  std::sort(logs.begin(), logs.end());

  uint64_t max_sequence = last_sequence_.load(std::memory_order_acquire);
  for (uint64_t log : logs) {
    uint64_t seq = 0;
    Status s = ReplayWal(log, &seq);
    if (!s.ok()) return s;
    max_sequence = std::max(max_sequence, seq);
  }
  last_sequence_.store(max_sequence, std::memory_order_release);

  if (!mem_->empty()) {
    LOG_INFO("recovered %zu entries from WAL into memtable (seq=%" PRIu64 ")", mem_->NumEntries(),
             max_sequence);

    // The recovered data currently exists ONLY in this in-memory memtable.
    // It must be made durable again -- via an SSTable -- before we delete the
    // WAL files it came from, or a second crash before the next flush would
    // silently lose it. Flushing synchronously here (recovery runs before the
    // background thread starts) is the simplest way to guarantee that.
    FileMetaData meta;
    auto frozen = std::const_pointer_cast<const MemTable>(mem_);
    KVDB_RETURN_IF_ERROR(BuildTableFromMemTable(frozen, &meta));
    if (meta.num_entries > 0) {
      std::shared_ptr<TableEntry> entry;
      KVDB_RETURN_IF_ERROR(OpenTable(meta, &entry));
      current_->levels[0].insert(current_->levels[0].begin(), std::move(entry));
    }
    mem_ = std::make_shared<MemTable>();
  }

  KVDB_RETURN_IF_ERROR(RollNewWal());
  KVDB_RETURN_IF_ERROR(WriteManifest());

  // Only now -- after the recovered data is durable in an SSTable and the
  // manifest referencing it has been written -- are the source WALs redundant.
  for (uint64_t log : logs) {
    if (log != log_number_) fileutil::DeleteFile(WalPath(log));
  }
  return Status::OK();
}

Status LsmStore::ReplayWal(uint64_t log_number, uint64_t* max_sequence) {
  const std::string path = WalPath(log_number);
  if (!fileutil::FileExists(path)) return Status::OK();

  WalReader reader;
  KVDB_RETURN_IF_ERROR(reader.Open(path));

  std::string record;
  uint64_t records = 0;
  while (true) {
    Status s = reader.ReadRecord(&record);
    if (s.IsNotFound()) break;  // clean EOF
    if (!s.ok()) {
      if (options_.tolerate_wal_tail_corruption) {
        // Expected after a crash mid-append: the last record was torn. Drop it
        // and everything after; those writes were never acknowledged.
        LOG_WARN("wal %s: %s at offset %" PRIu64 " - truncating replay", path.c_str(),
                 s.ToString().c_str(), reader.last_good_offset());
        break;
      }
      return s;
    }

    WriteBatch batch;
    KVDB_RETURN_IF_ERROR(batch.SetContents(record));
    KVDB_RETURN_IF_ERROR(batch.InsertInto(mem_.get()));
    *max_sequence = std::max(*max_sequence, batch.Sequence() + batch.Count() - 1);
    records++;

    // Don't let a huge WAL blow up memory during recovery.
    if (mem_->ApproximateMemoryUsage() >= options_.write_buffer_size) {
      FileMetaData meta;
      auto frozen = std::const_pointer_cast<const MemTable>(mem_);
      KVDB_RETURN_IF_ERROR(BuildTableFromMemTable(frozen, &meta));
      if (meta.num_entries > 0) {
        std::shared_ptr<TableEntry> entry;
        KVDB_RETURN_IF_ERROR(OpenTable(meta, &entry));
        current_->levels[0].insert(current_->levels[0].begin(), std::move(entry));
      }
      mem_ = std::make_shared<MemTable>();
    }
  }
  LOG_INFO("replayed %" PRIu64 " records from %s", records, path.c_str());
  return Status::OK();
}

Status LsmStore::RollNewWal() {
  if (wal_) {
    wal_->Sync();
    wal_->Close();
  }
  log_number_ = next_file_number_++;
  wal_ = std::make_unique<WalWriter>();
  KVDB_RETURN_IF_ERROR(wal_->Open(WalPath(log_number_)));
  return fileutil::FsyncDir(options_.data_dir);
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------
Status LsmStore::Put(const WriteOptions& opts, std::string_view key, std::string_view value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opts, &batch);
}

Status LsmStore::Delete(const WriteOptions& opts, std::string_view key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opts, &batch);
}

Status LsmStore::Write(const WriteOptions& opts, WriteBatch* batch) { return WriteImpl(opts, batch); }

WriteBatch* LsmStore::BuildGroupBatch(Writer* leader, Writer** last_writer) {
  // Called with mu_ held. Merges the queued batches behind `leader` into one
  // WAL append, amortising the fsync across all of them.
  *last_writer = leader;
  if (!options_.enable_group_commit) return leader->batch;

  static thread_local WriteBatch merged;
  size_t merged_count = 0;
  bool started = false;

  auto it = writers_.begin();
  ++it;  // skip the leader
  for (; it != writers_.end(); ++it) {
    Writer* w = *it;
    if (w->sync && !leader->sync) break;  // don't silently downgrade durability
    if (merged_count >= options_.max_group_commit_batch) break;
    if (!started) {
      merged.Clear();
      merged.Append(*leader->batch);
      started = true;
    }
    merged.Append(*w->batch);
    merged_count++;
    *last_writer = w;
  }
  return started ? &merged : leader->batch;
}

Status LsmStore::WriteImpl(const WriteOptions& opts, WriteBatch* batch) {
  if (batch == nullptr || batch->empty()) return Status::InvalidArgument("empty write batch");

  Writer w;
  w.batch = batch;
  w.sync = opts.sync || options_.sync_writes;

  std::unique_lock<std::mutex> lk(mu_);
  if (shutting_down_) return Status::Shutdown("store is closing");
  writers_.push_back(&w);
  // Wait until we're at the head: exactly one writer at a time owns the WAL
  // and memtable, which is what makes the memtable's single-writer skiplist
  // contract hold.
  while (!w.done && &w != writers_.front()) {
    w.cv.wait(lk);
  }
  if (w.done) return w.status;

  Status status = MakeRoomForWrite(lk);

  Writer* last_writer = &w;
  if (status.ok()) {
    WriteBatch* group = BuildGroupBatch(&w, &last_writer);
    const uint64_t seq = last_sequence_.load(std::memory_order_acquire) + 1;
    group->SetSequence(seq);
    const uint32_t count = group->Count();

    // WAL append happens outside mu_ so concurrent readers aren't blocked by
    // disk I/O; the writers_ queue already guarantees single-writer access.
    WalWriter* wal = wal_.get();
    const bool sync = w.sync;
    auto mem = mem_;
    lk.unlock();

    status = wal->AddRecord(group->Contents());
    if (status.ok()) {
      status = wal->Flush();
      if (status.ok() && sync) {
        status = wal->Sync();
        stat_wal_syncs_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    if (status.ok()) {
      status = group->InsertInto(mem.get());
      stat_bytes_written_.fetch_add(group->ApproximateSize(), std::memory_order_relaxed);
      stat_writes_.fetch_add(count, std::memory_order_relaxed);
      stat_batches_.fetch_add(1, std::memory_order_relaxed);
    }

    lk.lock();
    if (status.ok()) {
      // Publishing the sequence is what makes the writes visible to readers.
      last_sequence_.store(seq + count - 1, std::memory_order_release);
    }
  }

  // Retire everyone whose batch we just committed.
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.notify_one();
    }
    if (ready == last_writer) break;
  }
  if (!writers_.empty()) writers_.front()->cv.notify_one();

  return status;
}

Status LsmStore::MakeRoomForWrite(std::unique_lock<std::mutex>& lk) {
  while (true) {
    if (!bg_error_.ok()) return bg_error_;

    if (mem_->ApproximateMemoryUsage() < options_.write_buffer_size) return Status::OK();

    if (imm_ != nullptr) {
      // Previous flush still running: stall rather than run memory unbounded.
      stat_write_stalls_.fetch_add(1, std::memory_order_relaxed);
      if (!options_.background_compaction) {
        Status s = FlushImmutable(lk);
        if (!s.ok()) return s;
        continue;
      }
      MaybeScheduleWork();
      bg_cv_.wait_for(lk, std::chrono::milliseconds(50));
      continue;
    }

    // Freeze the memtable and start a new one plus a fresh WAL.
    imm_ = mem_;
    mem_ = std::make_shared<MemTable>();
    Status s = RollNewWal();
    if (!s.ok()) return s;

    if (options_.background_compaction) {
      MaybeScheduleWork();
    } else {
      s = FlushImmutable(lk);
      if (!s.ok()) return s;
    }
    return Status::OK();
  }
}

}  // namespace kvdb
