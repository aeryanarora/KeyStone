#include "common/logger.h"

#include <cstdarg>
#include <cstring>
#include <ctime>
#include <vector>

namespace kvdb {

Logger& Logger::Instance() {
  static Logger instance;
  return instance;
}

Logger::~Logger() {
  if (out_ != nullptr) std::fclose(out_);
}

void Logger::SetPrefix(std::string p) {
  std::lock_guard<std::mutex> lk(mu_);
  prefix_ = std::move(p);
}

bool Logger::SetOutputFile(const std::string& path) {
  std::lock_guard<std::mutex> lk(mu_);
  if (out_ != nullptr) {
    std::fclose(out_);
    out_ = nullptr;
  }
  if (path.empty()) return true;
  out_ = std::fopen(path.c_str(), "a");
  return out_ != nullptr;
}

static const char* LevelName(LogLevel l) {
  switch (l) {
    case LogLevel::kDebug: return "DEBUG";
    case LogLevel::kInfo: return "INFO ";
    case LogLevel::kWarn: return "WARN ";
    case LogLevel::kError: return "ERROR";
    default: return "?????";
  }
}

void Logger::Write(LogLevel l, const char* file, int line, const std::string& msg) {
  timespec ts{};
  clock_gettime(CLOCK_REALTIME, &ts);
  tm tm_buf{};
  localtime_r(&ts.tv_sec, &tm_buf);
  char timebuf[32];
  std::strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tm_buf);

  const char* base = std::strrchr(file, '/');
  base = (base == nullptr) ? file : base + 1;

  std::lock_guard<std::mutex> lk(mu_);
  std::FILE* dst = (out_ != nullptr) ? out_ : stderr;
  std::fprintf(dst, "%s.%03ld %s %s%s:%d] %s\n", timebuf, ts.tv_nsec / 1000000,
               LevelName(l), prefix_.c_str(), base, line, msg.c_str());
  std::fflush(dst);
}

std::string StringPrintf(const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  va_list ap2;
  va_copy(ap2, ap);
  int n = std::vsnprintf(nullptr, 0, fmt, ap);
  va_end(ap);
  if (n <= 0) {
    va_end(ap2);
    return {};
  }
  std::vector<char> buf(static_cast<size_t>(n) + 1);
  std::vsnprintf(buf.data(), buf.size(), fmt, ap2);
  va_end(ap2);
  return std::string(buf.data(), static_cast<size_t>(n));
}

}  // namespace kvdb
