// kvdb-cli: interactive/scriptable client.
//
//   kvdb-cli --endpoints=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 get foo
//   kvdb-cli --endpoints=127.0.0.1:7001 put foo bar
//   echo -e "put a 1\nget a" | kvdb-cli --endpoints=127.0.0.1:7001
#include <cstdio>
#include <iostream>
#include <sstream>

#include "client/kv_client.h"
#include "common/flags.h"

using namespace kvdb;

namespace {

void PrintStatus(const Status& s) {
  if (s.ok()) {
    printf("OK\n");
  } else if (s.IsNotFound()) {
    printf("(not found)\n");
  } else {
    printf("ERROR: %s\n", s.ToString().c_str());
  }
}

int RunCommand(KvClient* client, const std::vector<std::string>& args) {
  if (args.empty()) return 0;
  const std::string& cmd = args[0];

  if (cmd == "get" && args.size() >= 2) {
    std::string value;
    Status s = client->Get(args[1], &value);
    if (s.ok()) {
      printf("%s\n", value.c_str());
    } else {
      PrintStatus(s);
    }
  } else if (cmd == "put" && args.size() >= 3) {
    PrintStatus(client->Put(args[1], args[2]));
  } else if (cmd == "delete" && args.size() >= 2) {
    PrintStatus(client->Delete(args[1]));
  } else if (cmd == "scan" && args.size() >= 2) {
    std::vector<std::pair<std::string, std::string>> rows;
    std::string end = args.size() >= 3 ? args[2] : "";
    Status s = client->Scan(args[1], end, 0, &rows);
    if (!s.ok()) {
      PrintStatus(s);
    } else {
      for (const auto& [k, v] : rows) printf("%s\t%s\n", k.c_str(), v.c_str());
      printf("(%zu rows)\n", rows.size());
    }
  } else if (cmd == "incr" && args.size() >= 2) {
    int64_t delta = args.size() >= 3 ? std::atoll(args[2].c_str()) : 1;
    int64_t result = 0;
    Status s = client->Incr(args[1], delta, &result);
    if (s.ok()) {
      printf("%lld\n", static_cast<long long>(result));
    } else {
      PrintStatus(s);
    }
  } else if (cmd == "cas" && args.size() >= 4) {
    bool swapped = false;
    Status s = client->Cas(args[1], args[2], args[3], false, &swapped);
    if (!s.ok()) {
      PrintStatus(s);
    } else {
      printf("%s\n", swapped ? "swapped" : "unchanged");
    }
  } else if (cmd == "status") {
    StatusResponse resp;
    Status s = client->GetStatus(&resp, true);
    if (!s.ok()) {
      PrintStatus(s);
    } else {
      printf("node_id=%llu role=%s epoch=%llu primary_id=%llu primary_addr=%s last_seq=%llu\n",
             static_cast<unsigned long long>(resp.node_id), resp.role.c_str(),
             static_cast<unsigned long long>(resp.epoch),
             static_cast<unsigned long long>(resp.primary_id), resp.primary_address.c_str(),
             static_cast<unsigned long long>(resp.last_seq));
      if (!resp.storage_stats.empty()) printf("storage: %s\n", resp.storage_stats.c_str());
      if (!resp.repl_stats.empty()) printf("repl: %s\n", resp.repl_stats.c_str());
    }
  } else if (cmd == "admin" && args.size() >= 2) {
    AdminResponse resp;
    Status s = client->Admin(args[1], args.size() >= 3 ? args[2] : "", &resp);
    if (!s.ok()) {
      PrintStatus(s);
    } else if (resp.status_code != 0) {
      printf("ERROR: %s\n", resp.error.c_str());
    } else {
      printf("%s\n", resp.result.c_str());
    }
  } else {
    fprintf(stderr,
            "unknown command: %s\nusage: get|put|delete|scan|incr|cas|status|admin ...\n",
            cmd.c_str());
    return 1;
  }
  return 0;
}

std::vector<std::string> Tokenize(const std::string& line) {
  std::vector<std::string> out;
  std::istringstream iss(line);
  std::string tok;
  while (iss >> tok) out.push_back(tok);
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  Flags flags(argc, argv);
  std::string endpoints = flags.Get("endpoints", flags.Get("endpoint", "127.0.0.1:7000"));

  KvClient client;
  Status s = client.Connect(endpoints);
  if (!s.ok()) {
    fprintf(stderr, "failed to connect to %s: %s\n", endpoints.c_str(), s.ToString().c_str());
    return 1;
  }

  const auto& positional = flags.positional();
  if (!positional.empty()) {
    return RunCommand(&client, positional);
  }

  // Interactive / piped mode.
  std::string line;
  while (std::getline(std::cin, line)) {
    auto args = Tokenize(line);
    if (args.empty()) continue;
    if (args[0] == "quit" || args[0] == "exit") break;
    RunCommand(&client, args);
  }
  return 0;
}
