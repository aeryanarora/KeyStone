#include <gtest/gtest.h>

#include "common/coding.h"
#include "common/crc32c.h"

using namespace kvdb;

TEST(Coding, FixedRoundTrip) {
  std::string s;
  PutFixed32(&s, 0x12345678);
  PutFixed64(&s, 0x1122334455667788ull);
  std::string_view v(s);
  uint32_t a;
  uint64_t b;
  ASSERT_TRUE(GetFixed32(&v, &a));
  ASSERT_TRUE(GetFixed64(&v, &b));
  EXPECT_EQ(a, 0x12345678u);
  EXPECT_EQ(b, 0x1122334455667788ull);
  EXPECT_TRUE(v.empty());
}

TEST(Coding, VarintRoundTripSmallAndLarge) {
  std::vector<uint64_t> values = {0, 1, 127, 128, 16383, 16384, 1ull << 32, ~0ull};
  std::string s;
  for (auto v : values) PutVarint64(&s, v);
  std::string_view view(s);
  for (auto expected : values) {
    uint64_t got;
    ASSERT_TRUE(GetVarint64(&view, &got));
    EXPECT_EQ(got, expected);
  }
  EXPECT_TRUE(view.empty());
}

TEST(Coding, Varint32Truncated) {
  std::string s;
  PutVarint32(&s, 300);
  s.resize(0);  // simulate truncation
  std::string_view v(s);
  uint32_t out;
  EXPECT_FALSE(GetVarint32(&v, &out));
}

TEST(Coding, LengthPrefixedRoundTrip) {
  std::string s;
  PutLengthPrefixed(&s, "hello");
  PutLengthPrefixed(&s, "");
  PutLengthPrefixed(&s, std::string(1000, 'x'));
  std::string_view v(s);
  std::string_view a, b, c;
  ASSERT_TRUE(GetLengthPrefixed(&v, &a));
  ASSERT_TRUE(GetLengthPrefixed(&v, &b));
  ASSERT_TRUE(GetLengthPrefixed(&v, &c));
  EXPECT_EQ(a, "hello");
  EXPECT_EQ(b, "");
  EXPECT_EQ(c, std::string(1000, 'x'));
}

TEST(Coding, LengthPrefixedRejectsShortInput) {
  std::string s;
  PutVarint32(&s, 100);  // claims 100 bytes but there are none
  std::string_view v(s);
  std::string_view out;
  EXPECT_FALSE(GetLengthPrefixed(&v, &out));
}

TEST(Crc32c, KnownVectorsDiffer) {
  uint32_t a = crc32c::Value("hello", 5);
  uint32_t b = crc32c::Value("hellp", 5);
  EXPECT_NE(a, b);
}

TEST(Crc32c, MaskUnmaskRoundTrip) {
  uint32_t crc = crc32c::Value("some data", 9);
  uint32_t masked = crc32c::Mask(crc);
  EXPECT_NE(masked, crc);
  EXPECT_EQ(crc32c::Unmask(masked), crc);
}

TEST(Crc32c, ExtendMatchesWholeValue) {
  std::string a = "hello ";
  std::string b = "world";
  uint32_t incremental = crc32c::Extend(crc32c::Value(a), b.data(), b.size());
  uint32_t whole = crc32c::Value(a + b);
  EXPECT_EQ(incremental, whole);
}
