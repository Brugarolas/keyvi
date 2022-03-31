//
// keyvi - A key value store.
//
// Copyright 2015 Hendrik Muhs<hendrik.muhs@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

/*
 * index_reader_test.cpp
 *
 *  Created on: Jan 13, 2017
 *      Author: hendrik
 */
#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include <boost/filesystem.hpp>
#include <boost/test/unit_test.hpp>

#include "keyvi/index/read_only_index.h"
#include "keyvi/testing/index_mock.h"
#include "morton-nd/mortonND_LUT.h"

namespace keyvi {
namespace index {
BOOST_AUTO_TEST_SUITE(ReadOnlyIndexTests)

BOOST_AUTO_TEST_CASE(basicindex) {
  testing::IndexMock index;

  std::vector<std::pair<std::string, std::string>> test_data = {
      {"abc", "{a:1}"}, {"abbc", "{b:2}"}, {"abbcd", "{c:3}"}, {"abcde", "{a:1}"}, {"abdd", "{b:2}"},
  };

  index.AddSegment(&test_data);

  std::vector<std::pair<std::string, std::string>> test_data_2 = {
      {"abbcd", "{c:6}"}, {"babc", "{a:1}"}, {"babbc", "{b:2}"}, {"babcde", "{a:1}"}, {"babdd", "{b:2}"},
  };

  index.AddSegment(&test_data_2);

  ReadOnlyIndex reader(index.GetIndexFolder(), {{"refresh_interval", "400"}});

  BOOST_CHECK(reader.Contains("abc"));
  BOOST_CHECK(reader.Contains("babdd"));
  BOOST_CHECK(!reader.Contains("ab"));
  BOOST_CHECK(!reader.Contains("bbc"));
  BOOST_CHECK(!reader.Contains(""));
  BOOST_CHECK_EQUAL(reader["abc"].GetValueAsString(), "\"{a:1}\"");

  BOOST_CHECK(reader[""].IsEmpty());
  BOOST_CHECK(reader["ab"].IsEmpty());

  // test priority, last one should be returned
  BOOST_CHECK_EQUAL(reader["abbcd"].GetValueAsString(), "\"{c:6}\"");

  std::vector<std::pair<std::string, std::string>> test_data_3 = {
      {"abbcd", "{c:8}"}, {"cabc", "{a:1}"}, {"cabbc", "{b:2}"}, {"cabcde", "{a:1}"}, {"cabdd", "{b:2}"},
  };

  // sleep for 1s to ensure modification is visible
  std::this_thread::sleep_for(std::chrono::seconds(1));

  index.AddSegment(&test_data_3);
  BOOST_CHECK(reader.Contains("abc"));

  BOOST_CHECK_EQUAL(reader["abbcd"].GetValueAsString(), "\"{c:6}\"");

  // force reload
  reader.Reload();
  BOOST_CHECK(reader.Contains("abc"));

  BOOST_CHECK_EQUAL(reader["abbcd"].GetValueAsString(), "\"{c:8}\"");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::vector<std::pair<std::string, std::string>> test_data_4 = {{"abbcd", "{c:10}"}};
  index.AddSegment(&test_data_4);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  BOOST_CHECK_EQUAL(reader["abbcd"].GetValueAsString(), "\"{c:10}\"");

  std::vector<std::pair<std::string, std::string>> test_data_5 = {{"abbcd", "{c:12}"}};
  index.AddSegment(&test_data_5);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  BOOST_CHECK(reader.Contains("abc"));

  BOOST_CHECK_EQUAL(reader["abbcd"].GetValueAsString(), "\"{c:12}\"");
}

BOOST_AUTO_TEST_CASE(indexwithdeletedkeys) {
  testing::IndexMock index;

  std::vector<std::pair<std::string, std::string>> test_data = {
      {"cdefg", "{t:1}"}, {"键", "{b:2}"}, {"核心价值", "{c:3}"}, {"商店", "{a:1}"}, {"störe", "{b:2}"},
  };

  index.AddSegment(&test_data);

  std::vector<std::pair<std::string, std::string>> test_data_2 = {
      {"متجر", "{c:6}"},   {"مفتاح", "{a:1}"}, {"מַפְתֵחַ", "{b:2}"},
      {"babcde", "{a:1}"}, {"商店", "{b:2}"},  {"störe", "{t:44}"},
  };

  index.AddSegment(&test_data_2);

  ReadOnlyIndex reader(index.GetIndexFolder(), {{"refresh_interval", "600"}});

  BOOST_CHECK(reader.Contains("cdefg"));
  BOOST_CHECK(reader.Contains("مفتاح"));
  BOOST_CHECK(reader.Contains("核心价值"));
  BOOST_CHECK(reader.Contains("商店"));
  BOOST_CHECK(!reader.Contains(""));
  BOOST_CHECK(!reader.Contains("תֵ"));

  BOOST_CHECK_EQUAL(reader["מַפְתֵחַ"].GetValueAsString(), "\"{b:2}\"");

  BOOST_CHECK(reader[""].IsEmpty());
  BOOST_CHECK(reader["ab"].IsEmpty());

  // test priority, last one should be returned
  BOOST_CHECK_EQUAL(reader["商店"].GetValueAsString(), "\"{b:2}\"");
  index.AddDeletedKeys({"מַפְתֵחַ", "商店"}, 1);
  reader.Reload();

  BOOST_CHECK(reader.Contains("cdefg"));
  BOOST_CHECK(reader.Contains("störe"));
  BOOST_CHECK(reader.Contains("مفتاح"));
  BOOST_CHECK(reader.Contains("核心价值"));
  BOOST_CHECK(!reader.Contains("商店"));
  BOOST_CHECK(!reader.Contains("מַפְתֵחַ"));

  index.AddDeletedKeys({"störe", "商店"}, 0);
  reader.Reload();
  BOOST_CHECK(reader.Contains("störe"));
  index.AddDeletedKeys({"מַפְתֵחַ", "商店", "störe", "商店"}, 1);
  reader.Reload();
  BOOST_CHECK(!reader.Contains("störe"));
}

void testFuzzyMatching(ReadOnlyIndex* reader, const std::string& query, const size_t max_edit_distance,
                       const size_t minimum_exact_prefix, const std::vector<std::string>& expected_matches,
                       const std::vector<std::string>& expected_values) {
  auto expected_matches_it = expected_matches.begin();
  auto expected_values_it = expected_values.begin();

  BOOST_CHECK_EQUAL(expected_matches.size(), expected_values.size());

  auto matcher = reader->GetFuzzy(query, max_edit_distance, minimum_exact_prefix);
  for (auto m : matcher) {
    BOOST_REQUIRE(expected_matches_it != expected_matches.end());
    BOOST_CHECK_EQUAL(*expected_matches_it++, m.GetMatchedString());
    BOOST_CHECK_EQUAL(*expected_values_it++, m.GetValueAsString());
  }
  BOOST_CHECK(expected_matches_it == expected_matches.end());
}

BOOST_AUTO_TEST_CASE(fuzzyMatching) {
  testing::IndexMock index;

  std::vector<std::pair<std::string, std::string>> test_data = {{"abc", "{a:1}"},   {"abbc", "{b:2}"},
                                                                {"abbcd", "{c:3}"}, {"abcde", "{a:1}"},
                                                                {"abdd", "{b:3}"},  {"bbdd", "{f:2}"}};
  index.AddSegment(&test_data);
  std::vector<std::pair<std::string, std::string>> test_data_2 = {
      {"abbcd", "{c:6}"}, {"abcde", "{x:1}"},  {"babc", "{a:1}"},
      {"babbc", "{b:2}"}, {"babcde", "{a:1}"}, {"babdd", "{g:2}"},
  };

  index.AddSegment(&test_data_2);
  ReadOnlyIndex reader_1(index.GetIndexFolder(), {{"refresh_interval", "400"}});
  testFuzzyMatching(&reader_1, "babdd", 0, 5, {"babdd"}, {"\"{g:2}\""});
  testFuzzyMatching(&reader_1, "babdd", 0, 4, {"babdd"}, {"\"{g:2}\""});

  BOOST_CHECK_EQUAL(reader_1["abbc"].GetValueAsString(), "\"{b:2}\"");
  testFuzzyMatching(&reader_1, "abbc", 0, 2, {"abbc"}, {"\"{b:2}\""});
  testFuzzyMatching(&reader_1, "abc", 0, 2, {"abc"}, {"\"{a:1}\""});

  testFuzzyMatching(&reader_1, "abbc", 1, 2, {"abbc", "abbcd", "abc"}, {"\"{b:2}\"", "\"{c:6}\"", "\"{a:1}\""});
  testFuzzyMatching(&reader_1, "cde", 2, 3, {}, {});
  testFuzzyMatching(&reader_1, "babbc", 0, 0, {"babbc"}, {"\"{b:2}\""});
  testFuzzyMatching(&reader_1, "babbc", 0, 3, {"babbc"}, {"\"{b:2}\""});
  testFuzzyMatching(&reader_1, "babbc", 3, 10, {}, {});
  testFuzzyMatching(&reader_1, "abbc", 4, 1, {"abbc", "abbcd", "abc", "abcde", "abdd"},
                    {"\"{b:2}\"", "\"{c:6}\"", "\"{a:1}\"", "\"{x:1}\"", "\"{b:3}\""});

  index.AddDeletedKeys({"abbcd", "abcde", "babbc"}, 1);
  index.AddDeletedKeys({"abbcd", "bbdd"}, 0);

  ReadOnlyIndex reader_2(index.GetIndexFolder(), {{"refresh_interval", "400"}});

  testFuzzyMatching(&reader_2, "abbc", 0, 2, {"abbc"}, {"\"{b:2}\""});
  testFuzzyMatching(&reader_2, "abbc", 1, 2, {"abbc", "abc"}, {"\"{b:2}\"", "\"{a:1}\""});
  testFuzzyMatching(&reader_2, "abbc", 2, 2, {"abbc", "abc", "abdd"}, {"\"{b:2}\"", "\"{a:1}\"", "\"{b:3}\""});

  testFuzzyMatching(&reader_2, "bbdd", 1, 2, {}, {});
  testFuzzyMatching(&reader_2, "bbdd", 2, 1, {"babdd"}, {"\"{g:2}\""});

  testFuzzyMatching(&reader_2, "babbc", 0, 0, {}, {});
  testFuzzyMatching(&reader_2, "babbc", 0, 3, {}, {});
  testFuzzyMatching(&reader_2, "babbc", 2, 3, {"babc", "babdd"}, {"\"{a:1}\"", "\"{g:2}\""});

  testFuzzyMatching(&reader_2, "cde", 2, 3, {}, {});
  testFuzzyMatching(&reader_2, "abbc", 4, 4, {"abbc"}, {"\"{b:2}\""});
  testFuzzyMatching(&reader_2, "abbc", 4, 1, {"abbc", "abc", "abdd"}, {"\"{b:2}\"", "\"{a:1}\"", "\"{b:3}\""});
}

BOOST_AUTO_TEST_CASE(fuzzyMatchingExactPrefix) {
  testing::IndexMock index;

  std::vector<std::pair<std::string, std::string>> test_data = {{"a", "{a:1}"}, {"bc", "{b:2}"}};
  index.AddSegment(&test_data);
  std::vector<std::pair<std::string, std::string>> test_data_2 = {{"apple", "{c:6}"}, {"cde", "{x:1}"}};
  index.AddSegment(&test_data_2);
  ReadOnlyIndex reader_1(index.GetIndexFolder(), {{"refresh_interval", "400"}});

  testFuzzyMatching(&reader_1, "app", 0, 1, {}, {});
  testFuzzyMatching(&reader_1, "ap", 1, 1, {"a"}, {"\"{a:1}\""});
  index.AddDeletedKeys({"a"}, 0);
  testFuzzyMatching(&reader_1, "ap", 1, 1, {"a"}, {"\"{a:1}\""});
}

void testNearMatching(ReadOnlyIndex* reader, const std::string& query, const size_t minimum_exact_prefix,
                      const bool greedy, const std::vector<std::string>& expected_matches,
                      const std::vector<std::string>& expected_values) {
  BOOST_CHECK_EQUAL(expected_matches.size(), expected_values.size());
  auto expected_matches_it = expected_matches.begin();
  auto expected_values_it = expected_values.begin();

  auto matcher = reader->GetNear(query, minimum_exact_prefix, greedy);
  for (auto m : matcher) {
    BOOST_REQUIRE(expected_matches_it != expected_matches.end());
    BOOST_CHECK_EQUAL(*expected_matches_it++, m.GetMatchedString());
    BOOST_CHECK_EQUAL(*expected_values_it++, m.GetValueAsString());
  }
  BOOST_CHECK(expected_matches_it == expected_matches.end());
}

BOOST_AUTO_TEST_CASE(nearMatching) {
  testing::IndexMock index;

  std::vector<std::pair<std::string, std::string>> test_data = {{"pizzeria:u281z7hfvzq9", "pizzeria in Munich 1"},
                                                                {"pizzeria:u0vu7uqfyqkg", "pizzeria in Mainz"},
                                                                {"pizzeria:u281wu8bmmzq", "pizzeria in Munich 2"}};

  index.AddSegment(&test_data);
  std::vector<std::pair<std::string, std::string>> test_data_2 = {{"pizzeria:u33db8mmzj1t", "pizzeria in Berlin"},
                                                                  {"pizzeria:u0yjjd65eqy0", "pizzeria in Frankfurt"},
                                                                  {"pizzeria:u28db8mmzj1t", "pizzeria in Munich 3"},
                                                                  {"pizzeria:u0vu7uqfyqkg", "pizzeria near Mainz"},
                                                                  {"pizzeria:u2817uqfyqkg", "pizzeria in Munich 4"}};

  index.AddSegment(&test_data_2);
  ReadOnlyIndex reader_1(index.GetIndexFolder(), {{"refresh_interval", "400"}});
  testNearMatching(&reader_1, "pizzeria:u281wu88kekq", 12, false, {"pizzeria:u281wu8bmmzq"},
                   {"\"pizzeria in Munich 2\""});
  // exact match in 1 segment
  testNearMatching(&reader_1, "pizzeria:u281wu8bmmzq", 21, false, {"pizzeria:u281wu8bmmzq"},
                   {"\"pizzeria in Munich 2\""});
  // exact match in 2 segments
  testNearMatching(&reader_1, "pizzeria:u0vu7uqfyqkg", 21, false, {"pizzeria:u0vu7uqfyqkg"},
                   {"\"pizzeria near Mainz\""});

  // near match, that should match in in both segments, but de-dedupped and returned from the 2nd segment
  testNearMatching(&reader_1, "pizzeria:u0vu7u8bmmzq", 14, false, {"pizzeria:u0vu7uqfyqkg"},
                   {"\"pizzeria near Mainz\""});

  // match greedy but respecting the geohash prefix u28
  testNearMatching(
      &reader_1, "pizzeria:u281wu88kekq", 12, true,
      {"pizzeria:u281wu8bmmzq", "pizzeria:u2817uqfyqkg", "pizzeria:u281z7hfvzq9", "pizzeria:u28db8mmzj1t"},
      {"\"pizzeria in Munich 2\"", "\"pizzeria in Munich 4\"", "\"pizzeria in Munich 1\"", "\"pizzeria in Munich 3\""});

  index.AddDeletedKeys({"pizzeria:u28db8mmzj1t", "pizzeria:u0vu7uqfyqkg"}, 1);
  index.AddDeletedKeys({"pizzeria:u281wu8bmmzq"}, 0);

  ReadOnlyIndex reader_2(index.GetIndexFolder(), {{"refresh_interval", "400"}});
  testNearMatching(&reader_2, "pizzeria:u281wu88kekq", 12, false, {"pizzeria:u2817uqfyqkg", "pizzeria:u281z7hfvzq9"},
                   {"\"pizzeria in Munich 4\"", "\"pizzeria in Munich 1\""});
  testNearMatching(&reader_2, "pizzeria:u281wu8bmmzq", 21, false, {}, {});
  // exact match in 2 segments
  testNearMatching(&reader_2, "pizzeria:u0vu7uqfyqkg", 21, false, {}, {});

  // near match, that should match in in both segments, but de-dedupped and returned from the 1st segment
  testNearMatching(&reader_2, "pizzeria:u0vu7u8bmmzq", 14, false, {}, {});

  // match greedy but respecting the geohash prefix u28
  testNearMatching(&reader_2, "pizzeria:u281wu88kekq", 12, true, {"pizzeria:u2817uqfyqkg", "pizzeria:u281z7hfvzq9"},
                   {"\"pizzeria in Munich 4\"", "\"pizzeria in Munich 1\""});
}

BOOST_AUTO_TEST_CASE(nearMatching3) {
  testing::IndexMock index;

  constexpr auto MortonND_2D_64 = mortonnd::MortonNDLutEncoder<2, 32, 8>();
  constexpr auto MortonND_2D_64_DC = mortonnd::MortonNDLutDecoder<2, 32, 8>();

  double lat = 46.24710003845394;
  double lon = 13.57959995046258;

  uint64_t mapped_lat = static_cast<std::uint64_t>(((lat + 90.0) / 180) * (1L << 32));
  uint64_t mapped_lon = static_cast<std::uint64_t>(((lon + 180) / 360) * (1L << 32));

  uint64_t encoded_lat_lon = __builtin_bswap64(MortonND_2D_64.Encode(mapped_lat, mapped_lon));
  std::vector<std::pair<std::string, std::string>> test_data;
  std::string key(reinterpret_cast<const char*>(&encoded_lat_lon), 8);
  test_data.emplace_back(key, "kobarid");

  index.AddSegment(&test_data);

  // test
  double lat2 = 46.248561576323794;
  double lon2 = 13.586155688082544;

  uint64_t mapped_lat2 = static_cast<std::uint64_t>(((lat2 + 90.0) / 180) * (1L << 32));
  uint64_t mapped_lon2 = static_cast<std::uint64_t>(((lon2 + 180) / 360) * (1L << 32));

  uint64_t encoded_lat_lon2 = __builtin_bswap64(MortonND_2D_64.Encode(mapped_lat2, mapped_lon2));
  {
    uint64_t mapped_latd;
    uint64_t mapped_lond;
    uint64_t encoded_lat_lond = *(reinterpret_cast<const uint64_t*>(key.c_str()));

    std::tie(mapped_latd, mapped_lond) = MortonND_2D_64_DC.Decode(__builtin_bswap64(encoded_lat_lond));

    double latd = ((static_cast<double>(mapped_latd) / (1L << 32)) * 180.0) - 90.0;
    double lond = ((static_cast<double>(mapped_lond) / (1L << 32)) * 360.0) - 180.0;
    std::cout << "lat: " << latd << " lon: " << lond << std::endl;
  }

  std::string query(reinterpret_cast<const char*>(&encoded_lat_lon2), 8);
  ReadOnlyIndex reader(index.GetIndexFolder(), {{"refresh_interval", "400"}});

  auto matcher = reader.GetNear(query, 2, false);
  for (auto m : matcher) {
    std::cout << m.GetValueAsString() << std::endl;
    uint64_t decoded_lat_lon = *(reinterpret_cast<const uint64_t*>(m.GetMatchedString().c_str()));

    uint64_t mapped_latd;
    uint64_t mapped_lond;

    std::tie(mapped_latd, mapped_lond) = MortonND_2D_64_DC.Decode(__builtin_bswap64(decoded_lat_lon));

    double latd = ((static_cast<double>(mapped_latd) / (1L << 32)) * 180.0) - 90.0;
    double lond = ((static_cast<double>(mapped_lond) / (1L << 32)) * 360.0) - 180.0;
    std::cout << "lat: " << latd << " lon: " << lond << std::endl;
    std::cout << "score: " << m.GetScore() << std::endl;
  }
}
/*
BOOST_AUTO_TEST_CASE(nearMatching2) {
  testing::IndexMock index;

  constexpr auto MortonND_8D_64 = mortonnd::MortonNDLutEncoder<8, 8, 4>();

  std::vector<std::pair<std::string, std::string>> test_data;
  for (size_t i = 0; i < 100; ++i) {
    uint64_t encoding = MortonND_8D_64.Encode(
        i * (std::numeric_limits<int64_t>::max() / 100), i * (std::numeric_limits<int64_t>::max() / 100),
        i * (std::numeric_limits<int64_t>::max() / 100), i * (std::numeric_limits<int64_t>::max() / 100),
        i * (std::numeric_limits<int64_t>::max() / 100), i * (std::numeric_limits<int64_t>::max() / 100),
        i * (std::numeric_limits<int64_t>::max() / 100), i * (std::numeric_limits<int64_t>::max() / 100));
    std::string key(reinterpret_cast<const char*>(&encoding), 8);
    test_data.emplace_back(key, std::to_string(i));
  }
  index.AddSegment(&test_data);

  uint64_t encoding =
      MortonND_8D_64.Encode(std::numeric_limits<int64_t>::max() / 2, std::numeric_limits<int64_t>::max() / 2,
                            std::numeric_limits<int64_t>::max() / 2, std::numeric_limits<int64_t>::max() / 2,
                            std::numeric_limits<int64_t>::max() / 2, std::numeric_limits<int64_t>::max() / 2,
                            std::numeric_limits<int64_t>::max() / 2, std::numeric_limits<int64_t>::max() / 2);
  std::string query(reinterpret_cast<const char*>(&encoding), 8);
  ReadOnlyIndex reader(index.GetIndexFolder(), {{"refresh_interval", "400"}});

  auto matcher = reader.GetNear(query, 0, false);
  for (auto m : matcher) {
    std::cout << m.GetValueAsString() << std::endl;
  }
}*/
BOOST_AUTO_TEST_SUITE_END()

} /* namespace index */
} /* namespace keyvi */
