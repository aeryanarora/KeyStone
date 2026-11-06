#include <benchmark/benchmark.h>

#include <random>

#include "storage/db.h"

using namespace kvdb;

namespace {

std::string TmpDir(const std::string& tag) {
  return "/tmp/kvdb_bench_" + tag + "_" + std::to_string(::getpid());
}

std::string MakeValue(size_t size) { return std::string(size, 'x'); }

std::unique_ptr<LsmStore> OpenStore(const std::string& tag, bool sync_writes) {
  fileutil::DeleteDirRecursive(TmpDir(tag));
  Options opts;
  opts.data_dir = TmpDir(tag);
  opts.sync_writes = sync_writes;
  opts.background_compaction = true;
  std::unique_ptr<LsmStore> store;
  Status s = LsmStore::Open(opts, &store);
  if (!s.ok()) throw std::runtime_error(s.ToString());
  return store;
}

}  // namespace

static void BM_SequentialPut(benchmark::State& state) {
  const size_t value_size = static_cast<size_t>(state.range(0));
  auto store = OpenStore("seq_put_" + std::to_string(value_size), false);
  std::string value = MakeValue(value_size);
  WriteOptions wopts;
  uint64_t i = 0;
  for (auto _ : state) {
    char key[32];
    snprintf(key, sizeof(key), "key%016llu", static_cast<unsigned long long>(i++));
    benchmark::DoNotOptimize(store->Put(wopts, key, value));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(value_size));
}
BENCHMARK(BM_SequentialPut)->Arg(16)->Arg(256)->Arg(4096);

static void BM_RandomPut(benchmark::State& state) {
  const size_t value_size = static_cast<size_t>(state.range(0));
  auto store = OpenStore("rand_put_" + std::to_string(value_size), false);
  std::string value = MakeValue(value_size);
  WriteOptions wopts;
  std::mt19937_64 rng(42);
  for (auto _ : state) {
    char key[32];
    snprintf(key, sizeof(key), "key%016llu", static_cast<unsigned long long>(rng() % 1000000));
    benchmark::DoNotOptimize(store->Put(wopts, key, value));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(value_size));
}
BENCHMARK(BM_RandomPut)->Arg(16)->Arg(256)->Arg(4096);

static void BM_SyncPut(benchmark::State& state) {
  auto store = OpenStore("sync_put", true);
  std::string value = MakeValue(256);
  WriteOptions wopts;
  wopts.sync = true;
  uint64_t i = 0;
  for (auto _ : state) {
    char key[32];
    snprintf(key, sizeof(key), "key%016llu", static_cast<unsigned long long>(i++));
    benchmark::DoNotOptimize(store->Put(wopts, key, value));
  }
}
BENCHMARK(BM_SyncPut);

static void BM_GetHit(benchmark::State& state) {
  auto store = OpenStore("get_hit", false);
  std::string value = MakeValue(256);
  WriteOptions wopts;
  const int kKeys = 10000;
  for (int i = 0; i < kKeys; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key%016d", i);
    store->Put(wopts, key, value);
  }
  ReadOptions ropts;
  std::mt19937_64 rng(7);
  std::string out;
  for (auto _ : state) {
    char key[32];
    snprintf(key, sizeof(key), "key%016llu", static_cast<unsigned long long>(rng() % kKeys));
    benchmark::DoNotOptimize(store->Get(ropts, key, &out));
  }
}
BENCHMARK(BM_GetHit);

static void BM_GetMiss(benchmark::State& state) {
  auto store = OpenStore("get_miss", false);
  ReadOptions ropts;
  std::string out;
  uint64_t i = 0;
  for (auto _ : state) {
    char key[32];
    snprintf(key, sizeof(key), "missing%016llu", static_cast<unsigned long long>(i++));
    benchmark::DoNotOptimize(store->Get(ropts, key, &out));
  }
}
BENCHMARK(BM_GetMiss);

static void BM_ScanRange(benchmark::State& state) {
  const size_t range_size = static_cast<size_t>(state.range(0));
  auto store = OpenStore("scan_" + std::to_string(range_size), false);
  std::string value = MakeValue(64);
  WriteOptions wopts;
  const int kKeys = 20000;
  for (int i = 0; i < kKeys; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key%016d", i);
    store->Put(wopts, key, value);
  }
  ReadOptions ropts;
  std::vector<std::pair<std::string, std::string>> out;
  for (auto _ : state) {
    char start[32], end[32];
    snprintf(start, sizeof(start), "key%016d", 5000);
    snprintf(end, sizeof(end), "key%016zu", 5000 + range_size);
    benchmark::DoNotOptimize(store->Scan(ropts, start, end, 0, &out));
  }
}
BENCHMARK(BM_ScanRange)->Arg(10)->Arg(100)->Arg(1000);

static void BM_WriteBatch(benchmark::State& state) {
  const int batch_size = static_cast<int>(state.range(0));
  auto store = OpenStore("batch_" + std::to_string(batch_size), false);
  std::string value = MakeValue(64);
  WriteOptions wopts;
  uint64_t base = 0;
  for (auto _ : state) {
    WriteBatch b;
    for (int i = 0; i < batch_size; i++) {
      char key[32];
      snprintf(key, sizeof(key), "key%016llu", static_cast<unsigned long long>(base + i));
      b.Put(key, value);
    }
    base += static_cast<uint64_t>(batch_size);
    benchmark::DoNotOptimize(store->Write(wopts, &b));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * batch_size);
}
BENCHMARK(BM_WriteBatch)->Arg(1)->Arg(10)->Arg(100);

BENCHMARK_MAIN();
