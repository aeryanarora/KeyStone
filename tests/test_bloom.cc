#include <gtest/gtest.h>

#include "storage/bloom.h"

using namespace kvdb;

TEST(Bloom, NoFalseNegatives) {
  BloomFilter bloom(10);
  std::vector<std::string> keys;
  for (int i = 0; i < 5000; i++) keys.push_back("key-" + std::to_string(i));

  std::string filter;
  bloom.CreateFilter(keys, &filter);

  for (const auto& k : keys) {
    EXPECT_TRUE(BloomFilter::KeyMayMatch(k, filter)) << "false negative for " << k;
  }
}

TEST(Bloom, LowFalsePositiveRate) {
  BloomFilter bloom(10);
  std::vector<std::string> keys;
  for (int i = 0; i < 10000; i++) keys.push_back("present-" + std::to_string(i));
  std::string filter;
  bloom.CreateFilter(keys, &filter);

  int false_positives = 0;
  const int kQueries = 10000;
  for (int i = 0; i < kQueries; i++) {
    std::string absent = "absent-" + std::to_string(i);
    if (BloomFilter::KeyMayMatch(absent, filter)) false_positives++;
  }
  double rate = static_cast<double>(false_positives) / kQueries;
  // 10 bits/key should give roughly 1% FP rate; allow generous slack.
  EXPECT_LT(rate, 0.05) << "false positive rate too high: " << rate;
}

TEST(Bloom, EmptyFilterIsConservative) {
  std::string empty_filter;
  // An empty/absent filter must never produce a false negative -- callers
  // treat "no filter" as "can't rule this out".
  EXPECT_TRUE(BloomFilter::KeyMayMatch("anything", empty_filter));
}

TEST(Bloom, SingleKey) {
  BloomFilter bloom(10);
  std::string filter;
  bloom.CreateFilter({"only-key"}, &filter);
  EXPECT_TRUE(BloomFilter::KeyMayMatch("only-key", filter));
}
