#include "common/file_util.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace kvdb {
namespace fileutil {

static Status Errno(const std::string& what) {
  return Status::IOError(what + ": " + std::strerror(errno));
}

bool FileExists(const std::string& path) { return ::access(path.c_str(), F_OK) == 0; }

Status CreateDirRecursive(const std::string& path) {
  if (path.empty()) return Status::InvalidArgument("empty path");
  std::string cur;
  size_t i = 0;
  if (path[0] == '/') {
    cur = "/";
    i = 1;
  }
  while (i <= path.size()) {
    size_t next = path.find('/', i);
    if (next == std::string::npos) next = path.size();
    std::string comp = path.substr(i, next - i);
    if (!comp.empty()) {
      if (cur.empty()) {
        cur = comp;
      } else if (cur == "/") {
        cur += comp;
      } else {
        cur += "/" + comp;
      }
      if (::mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST) {
        return Errno("mkdir " + cur);
      }
    }
    if (next == path.size()) break;
    i = next + 1;
  }
  return Status::OK();
}

Status ListDir(const std::string& path, std::vector<std::string>* out) {
  out->clear();
  DIR* d = ::opendir(path.c_str());
  if (d == nullptr) return Errno("opendir " + path);
  struct dirent* e;
  while ((e = ::readdir(d)) != nullptr) {
    std::string name(e->d_name);
    if (name == "." || name == "..") continue;
    out->push_back(std::move(name));
  }
  ::closedir(d);
  return Status::OK();
}

Status DeleteFile(const std::string& path) {
  if (::unlink(path.c_str()) != 0) return Errno("unlink " + path);
  return Status::OK();
}

Status DeleteDirRecursive(const std::string& path) {
  std::vector<std::string> entries;
  if (!FileExists(path)) return Status::OK();
  Status s = ListDir(path, &entries);
  if (!s.ok()) return s;
  for (const auto& e : entries) {
    std::string full = JoinPath(path, e);
    struct stat st {};
    if (::lstat(full.c_str(), &st) != 0) continue;
    if (S_ISDIR(st.st_mode)) {
      s = DeleteDirRecursive(full);
    } else {
      s = DeleteFile(full);
    }
    if (!s.ok()) return s;
  }
  if (::rmdir(path.c_str()) != 0) return Errno("rmdir " + path);
  return Status::OK();
}

Status GetFileSize(const std::string& path, uint64_t* size) {
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0) return Errno("stat " + path);
  *size = static_cast<uint64_t>(st.st_size);
  return Status::OK();
}

Status ReadFileToString(const std::string& path, std::string* out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Errno("open " + path);
  out->clear();
  char buf[65536];
  ssize_t n;
  while ((n = ::read(fd, buf, sizeof(buf))) > 0) out->append(buf, static_cast<size_t>(n));
  if (n < 0) {
    Status s = Errno("read " + path);
    ::close(fd);
    return s;
  }
  ::close(fd);
  return Status::OK();
}

Status FsyncFile(int fd) {
  if (::fsync(fd) != 0) return Errno("fsync");
  return Status::OK();
}

Status FsyncDir(const std::string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) return Errno("open dir " + dir);
  Status s = FsyncFile(fd);
  ::close(fd);
  return s;
}

Status RenameFile(const std::string& from, const std::string& to) {
  if (::rename(from.c_str(), to.c_str()) != 0) return Errno("rename " + from);
  return Status::OK();
}

Status WriteStringToFileAtomic(const std::string& path, const std::string& data, bool sync_dir) {
  const std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Errno("open " + tmp);
  size_t written = 0;
  while (written < data.size()) {
    ssize_t n = ::write(fd, data.data() + written, data.size() - written);
    if (n < 0) {
      if (errno == EINTR) continue;
      Status s = Errno("write " + tmp);
      ::close(fd);
      return s;
    }
    written += static_cast<size_t>(n);
  }
  Status s = FsyncFile(fd);
  ::close(fd);
  if (!s.ok()) return s;
  KVDB_RETURN_IF_ERROR(RenameFile(tmp, path));
  if (sync_dir) return FsyncDir(DirName(path));
  return Status::OK();
}

std::string JoinPath(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  if (b.empty()) return a;
  if (a.back() == '/') return a + b;
  return a + "/" + b;
}

std::string DirName(const std::string& path) {
  size_t pos = path.find_last_of('/');
  if (pos == std::string::npos) return ".";
  if (pos == 0) return "/";
  return path.substr(0, pos);
}

std::string BaseName(const std::string& path) {
  size_t pos = path.find_last_of('/');
  if (pos == std::string::npos) return path;
  return path.substr(pos + 1);
}

}  // namespace fileutil

// ---------------------------------------------------------------------------
// WritableFile
// ---------------------------------------------------------------------------
WritableFile::~WritableFile() { Close(); }

Status WritableFile::Open(const std::string& path, bool append) {
  Close();
  int flags = O_WRONLY | O_CREAT | (append ? O_APPEND : O_TRUNC);
  fd_ = ::open(path.c_str(), flags, 0644);
  if (fd_ < 0) return Status::IOError("open " + path + ": " + std::strerror(errno));
  path_ = path;
  buf_.clear();
  size_ = 0;
  if (append) {
    struct stat st {};
    if (::fstat(fd_, &st) == 0) size_ = static_cast<uint64_t>(st.st_size);
  }
  return Status::OK();
}

Status WritableFile::Append(const char* data, size_t n) {
  if (fd_ < 0) return Status::IOError("file not open");
  size_ += n;
  buf_.append(data, n);
  if (buf_.size() >= kBufferSize) return Flush();
  return Status::OK();
}

Status WritableFile::Flush() {
  if (fd_ < 0) return Status::IOError("file not open");
  size_t written = 0;
  while (written < buf_.size()) {
    ssize_t n = ::write(fd_, buf_.data() + written, buf_.size() - written);
    if (n < 0) {
      if (errno == EINTR) continue;
      return Status::IOError("write " + path_ + ": " + std::strerror(errno));
    }
    written += static_cast<size_t>(n);
  }
  buf_.clear();
  return Status::OK();
}

Status WritableFile::Sync() {
  KVDB_RETURN_IF_ERROR(Flush());
  return fileutil::FsyncFile(fd_);
}

Status WritableFile::Close() {
  if (fd_ < 0) return Status::OK();
  Status s = Flush();
  if (::close(fd_) != 0 && s.ok()) {
    s = Status::IOError("close " + path_ + ": " + std::strerror(errno));
  }
  fd_ = -1;
  return s;
}

// ---------------------------------------------------------------------------
// RandomAccessFile
// ---------------------------------------------------------------------------
RandomAccessFile::~RandomAccessFile() { Close(); }

Status RandomAccessFile::Open(const std::string& path) {
  Close();
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) return Status::IOError("open " + path + ": " + std::strerror(errno));
  struct stat st {};
  if (::fstat(fd_, &st) != 0) {
    ::close(fd_);
    fd_ = -1;
    return Status::IOError("fstat " + path);
  }
  size_ = static_cast<uint64_t>(st.st_size);
  path_ = path;
  return Status::OK();
}

Status RandomAccessFile::Read(uint64_t offset, size_t n, std::string* result) const {
  if (fd_ < 0) return Status::IOError("file not open");
  result->resize(n);
  size_t done = 0;
  while (done < n) {
    ssize_t r = ::pread(fd_, result->data() + done, n - done,
                        static_cast<off_t>(offset + done));
    if (r < 0) {
      if (errno == EINTR) continue;
      return Status::IOError("pread " + path_ + ": " + std::strerror(errno));
    }
    if (r == 0) return Status::Corruption("short read on " + path_);
    done += static_cast<size_t>(r);
  }
  return Status::OK();
}

Status RandomAccessFile::Close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// SequentialFile
// ---------------------------------------------------------------------------
SequentialFile::~SequentialFile() {
  if (fd_ >= 0) ::close(fd_);
}

Status SequentialFile::Open(const std::string& path) {
  if (fd_ >= 0) ::close(fd_);
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) return Status::IOError("open " + path + ": " + std::strerror(errno));
  struct stat st {};
  if (::fstat(fd_, &st) == 0) size_ = static_cast<uint64_t>(st.st_size);
  offset_ = 0;
  return Status::OK();
}

Status SequentialFile::Read(size_t n, std::string* result) {
  if (fd_ < 0) return Status::IOError("file not open");
  result->resize(n);
  size_t done = 0;
  while (done < n) {
    ssize_t r = ::read(fd_, result->data() + done, n - done);
    if (r < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(std::string("read: ") + std::strerror(errno));
    }
    if (r == 0) break;  // EOF
    done += static_cast<size_t>(r);
  }
  result->resize(done);
  offset_ += done;
  return Status::OK();
}

Status SequentialFile::Skip(uint64_t n) {
  if (::lseek(fd_, static_cast<off_t>(n), SEEK_CUR) == static_cast<off_t>(-1)) {
    return Status::IOError(std::string("lseek: ") + std::strerror(errno));
  }
  offset_ += n;
  return Status::OK();
}

}  // namespace kvdb
