#include "replication/replication_log.h"

#include "common/coding.h"
#include "common/file_util.h"
#include "common/logger.h"

namespace kvdb {

Status ReplicationLog::Open(const std::string& dir, bool sync_on_append) {
  dir_ = dir;
  sync_on_append_ = sync_on_append;
  KVDB_RETURN_IF_ERROR(fileutil::CreateDirRecursive(dir_));
  KVDB_RETURN_IF_ERROR(LoadEpoch());

  entries_.clear();
  if (fileutil::FileExists(LogPath())) {
    WalReader reader;
    KVDB_RETURN_IF_ERROR(reader.Open(LogPath()));
    std::string record;
    while (true) {
      Status s = reader.ReadRecord(&record);
      if (s.IsNotFound()) break;
      if (!s.ok()) {
        LOG_WARN("replication log tail corrupt (%s); truncating", s.ToString().c_str());
        break;
      }
      entries_.push_back(record);
    }
  }

  writer_ = std::make_unique<WalWriter>();
  KVDB_RETURN_IF_ERROR(writer_->Open(LogPath(), /*append=*/true));
  LOG_INFO("replication log opened: %zu entries, epoch=%llu primary=%llu", entries_.size(),
           static_cast<unsigned long long>(epoch_), static_cast<unsigned long long>(primary_id_));
  return Status::OK();
}

uint64_t ReplicationLog::last_seq() const {
  std::lock_guard<std::mutex> lk(mu_);
  return entries_.size();
}

Status ReplicationLog::EntryAt(uint64_t seq, std::string* data) const {
  std::lock_guard<std::mutex> lk(mu_);
  if (seq == 0 || seq > entries_.size()) return Status::NotFound("seq out of range");
  *data = entries_[seq - 1];
  return Status::OK();
}

Status ReplicationLog::Append(uint64_t seq, const std::string& data) {
  std::lock_guard<std::mutex> lk(mu_);
  if (seq != entries_.size() + 1) return Status::InvalidArgument("out-of-order append");
  KVDB_RETURN_IF_ERROR(writer_->AddRecord(data));
  KVDB_RETURN_IF_ERROR(writer_->Flush());
  if (sync_on_append_) KVDB_RETURN_IF_ERROR(writer_->Sync());
  entries_.push_back(data);
  return Status::OK();
}

Status ReplicationLog::Range(uint64_t from_seq, size_t max_entries,
                             std::vector<std::pair<uint64_t, std::string>>* out) const {
  std::lock_guard<std::mutex> lk(mu_);
  out->clear();
  if (from_seq == 0) from_seq = 1;
  for (uint64_t seq = from_seq; seq <= entries_.size() && out->size() < max_entries; seq++) {
    out->emplace_back(seq, entries_[seq - 1]);
  }
  return Status::OK();
}

Status ReplicationLog::LoadEpoch() {
  if (!fileutil::FileExists(EpochPath())) return Status::OK();
  std::string data;
  KVDB_RETURN_IF_ERROR(fileutil::ReadFileToString(EpochPath(), &data));
  std::string_view in(data);
  if (!GetFixed64(&in, &epoch_) || !GetFixed64(&in, &primary_id_)) {
    return Status::Corruption("bad replication epoch state");
  }
  return Status::OK();
}

Status ReplicationLog::SaveEpoch(uint64_t epoch, uint64_t primary_id) {
  std::lock_guard<std::mutex> lk(mu_);
  std::string out;
  PutFixed64(&out, epoch);
  PutFixed64(&out, primary_id);
  KVDB_RETURN_IF_ERROR(fileutil::WriteStringToFileAtomic(EpochPath(), out));
  epoch_ = epoch;
  primary_id_ = primary_id;
  return Status::OK();
}

uint64_t ReplicationLog::epoch() const {
  std::lock_guard<std::mutex> lk(mu_);
  return epoch_;
}

uint64_t ReplicationLog::primary_id() const {
  std::lock_guard<std::mutex> lk(mu_);
  return primary_id_;
}

}  // namespace kvdb
