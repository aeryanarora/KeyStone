#pragma once

#include <cstdio>
#include <mutex>
#include <string>
#include <utility>

namespace kvdb {

enum class LogLevel : int { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3, kOff = 4 };

class Logger {
 public:
  static Logger& Instance();

  void SetLevel(LogLevel l) { level_ = l; }
  LogLevel level() const { return level_; }
  // Prefix each line, e.g. "[node-2] ". Useful when several nodes share a tty.
  void SetPrefix(std::string p);
  // Redirect output to a file (nullptr restores stderr).
  bool SetOutputFile(const std::string& path);

  void Write(LogLevel l, const char* file, int line, const std::string& msg);

 private:
  Logger() = default;
  ~Logger();

  LogLevel level_ = LogLevel::kInfo;
  std::string prefix_;
  std::mutex mu_;
  std::FILE* out_ = nullptr;  // nullptr -> stderr
};

std::string StringPrintf(const char* fmt, ...) __attribute__((format(printf, 1, 2)));

}  // namespace kvdb

#define KVDB_LOG(lvl_, ...)                                                    \
  do {                                                                         \
    if (static_cast<int>(lvl_) >=                                              \
        static_cast<int>(::kvdb::Logger::Instance().level())) {                \
      ::kvdb::Logger::Instance().Write((lvl_), __FILE__, __LINE__,             \
                                       ::kvdb::StringPrintf(__VA_ARGS__));     \
    }                                                                          \
  } while (0)

#define LOG_DEBUG(...) KVDB_LOG(::kvdb::LogLevel::kDebug, __VA_ARGS__)
#define LOG_INFO(...) KVDB_LOG(::kvdb::LogLevel::kInfo, __VA_ARGS__)
#define LOG_WARN(...) KVDB_LOG(::kvdb::LogLevel::kWarn, __VA_ARGS__)
#define LOG_ERROR(...) KVDB_LOG(::kvdb::LogLevel::kError, __VA_ARGS__)
