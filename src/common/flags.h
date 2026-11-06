#pragma once

#include <cstdlib>
#include <map>
#include <string>
#include <vector>

namespace kvdb {

// Minimal `--flag value` / `--flag=value` parser. Not meant to compete with a
// real flags library -- just enough for the server/CLI/bench binaries without
// pulling in another dependency.
class Flags {
 public:
  Flags(int argc, char** argv) {
    for (int i = 1; i < argc; i++) {
      std::string arg(argv[i]);
      if (arg.rfind("--", 0) != 0) {
        positional_.push_back(arg);
        continue;
      }
      arg = arg.substr(2);
      auto eq = arg.find('=');
      if (eq != std::string::npos) {
        values_[arg.substr(0, eq)] = arg.substr(eq + 1);
      } else if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
        values_[arg] = argv[++i];
      } else {
        values_[arg] = "true";
      }
    }
  }

  std::string Get(const std::string& name, const std::string& def = "") const {
    auto it = values_.find(name);
    return it == values_.end() ? def : it->second;
  }
  int GetInt(const std::string& name, int def) const {
    auto it = values_.find(name);
    return it == values_.end() ? def : std::atoi(it->second.c_str());
  }
  double GetDouble(const std::string& name, double def) const {
    auto it = values_.find(name);
    return it == values_.end() ? def : std::atof(it->second.c_str());
  }
  bool GetBool(const std::string& name, bool def) const {
    auto it = values_.find(name);
    if (it == values_.end()) return def;
    return it->second == "true" || it->second == "1" || it->second == "yes";
  }
  bool Has(const std::string& name) const { return values_.count(name) > 0; }
  const std::vector<std::string>& positional() const { return positional_; }

 private:
  std::map<std::string, std::string> values_;
  std::vector<std::string> positional_;
};

}  // namespace kvdb
