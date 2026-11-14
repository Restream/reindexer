#include <gtest/gtest.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <vector>
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"

namespace reindexer_tests {

using namespace std::string_view_literals;

TEST(ExtendibleHashMapTest, InsertionAfterJoiningParts) try {
	using Map = reindexer::extendible_hash_map<tsl::sparse_map<uint64_t, uint64_t>, reindexer::extendible_hash_map_rebalance_params<128, 2>,
											   uint64_t, uint64_t>;
	constexpr size_t kKeysPerPart = 40;
	std::array<std::vector<uint64_t>, 4> keys;
	size_t filledGroups = 0;
	for (uint64_t key = 0; filledGroups < keys.size(); ++key) {
		// Select keys for each directory bucket using the map's SplitMix64 selector.
		uint64_t hash = std::hash<uint64_t>{}(key);
		hash ^= hash >> 30;
		hash *= 0xbf58476d1ce4e5b9ULL;
		hash ^= hash >> 27;
		hash *= 0x94d049bb133111ebULL;
		hash ^= hash >> 31;
		auto& group = keys[hash & (keys.size() - 1)];
		if (group.size() < kKeysPerPart + 1) {
			group.push_back(key);
			filledGroups += group.size() == kKeysPerPart + 1;
		}
	}

	enum class [[nodiscard]] InsertMode { Insert, FindOrInsert, Subscript };
	for (const auto mode : {InsertMode::Insert, InsertMode::FindOrInsert, InsertMode::Subscript}) {
		SCOPED_TRACE(static_cast<int>(mode));
		Map map;
		for (size_t i = 0; i < kKeysPerPart; ++i) {
			for (const auto& group : keys) {
				ASSERT_TRUE(map.insert(group[i], group[i]).second);
			}
		}
		ASSERT_EQ(map.num_parts(), 4);

		const std::array<size_t, 4> remaining = {2, 16, kKeysPerPart, kKeysPerPart};
		for (size_t part = 0; part < keys.size(); ++part) {
			for (size_t i = remaining[part]; i < kKeysPerPart; ++i) {
				ASSERT_EQ(map.erase(keys[part][i]), 1);
			}
		}

		// Cross the removal threshold in a large part, leaving the small parts unmerged.
		const auto temporaryKey = keys[2].back();
		for (size_t i = 0; i <= Map::kNumRemovesToJoinMinParts; ++i) {
			ASSERT_TRUE(map.insert(temporaryKey, temporaryKey).second);
			ASSERT_EQ(map.erase(temporaryKey), 1);
		}
		ASSERT_EQ(map.num_parts(), 4);

		// Insertion merges part 0 into part 1 and moves part 3 into the old slot 0.
		const auto key = keys[0].back();
		constexpr uint64_t kValue = 123456;
		const auto sizeBefore = map.size();
		switch (mode) {
			case InsertMode::Insert: {
				auto [it, inserted] = map.insert(key, kValue);
				ASSERT_TRUE(inserted);
				EXPECT_EQ(it->first, key);
				EXPECT_EQ(it->second, kValue);
				break;
			}
			case InsertMode::FindOrInsert: {
				auto [it, inserted] = map.find_or_insert(key, [key] { return key; }, kValue);
				ASSERT_TRUE(inserted);
				EXPECT_EQ(it->first, key);
				EXPECT_EQ(it->second, kValue);
				break;
			}
			case InsertMode::Subscript:
				map[key] = kValue;
				break;
		}
		ASSERT_EQ(map.num_parts(), 3);
		EXPECT_EQ(map.size(), sizeBefore + 1);
		const auto it = map.find(key);
		ASSERT_NE(it, map.end());
		EXPECT_EQ(it->second, kValue);
		for (size_t part = 0; part < keys.size(); ++part) {
			for (size_t i = 0; i < remaining[part]; ++i) {
				const auto found = map.find(keys[part][i]);
				ASSERT_NE(found, map.end());
				EXPECT_EQ(found->second, keys[part][i]);
			}
		}
		EXPECT_EQ(map.erase(key), 1);
		EXPECT_EQ(map.size(), sizeBefore);
	}
}
CATCH_AND_ASSERT

#ifndef REINDEX_WITH_TSAN

using TestStringMapType = reindexer::extendible_hash_map<
	tsl::sparse_map<reindexer::key_string, size_t, reindexer::hash_key_string, reindexer::equal_key_string,
					std::allocator<std::pair<reindexer::key_string, size_t>>, tsl::sh::power_of_two_growth_policy<2>,
					tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>,
	reindexer::str_map_rebalance_params, reindexer::key_string, size_t, reindexer::hash_key_string, reindexer::equal_key_string>;

TEST(ExtendibleHashMapTest, TestingStringHashMapOperations) try {
	const size_t kNumBucketsInPart = reindexer::str_map_rebalance_params::kNumBucketsInPart;
	TestStringMapType map;
	for (size_t i = 0; i < kNumBucketsInPart; ++i) {
		auto str = reindexer::key_string("str" + std::to_string(i));
		auto [it, inserted] = map.insert(str, i);
		EXPECT_TRUE(inserted);
		EXPECT_EQ(it->first, str);
		EXPECT_EQ(it->second, i);
	}

	ASSERT_EQ(map.num_parts(), 2);
	for (size_t i = 0; i < kNumBucketsInPart; i += 100) {
		ASSERT_EQ(map.find(reindexer::key_string("str" + std::to_string(i))) == map.end(), false);
	}

	for (size_t i = kNumBucketsInPart; i < 5 * kNumBucketsInPart; ++i) {
		auto str = reindexer::key_string("str" + std::to_string(i));
		auto [it, inserted] = map.insert(str, i);
		EXPECT_TRUE(inserted);
		EXPECT_EQ(it->first, str);
		EXPECT_EQ(it->second, i);
	}

	long long sumTime = 0;
	long long maxTime = 0;

	for (size_t i = 5 * kNumBucketsInPart; i < 6 * kNumBucketsInPart; ++i) {
		auto str = reindexer::key_string("str" + std::to_string(i));

		auto startTime = std::chrono::high_resolution_clock::now();
		auto [it, inserted] = map.insert({reindexer::key_string("str" + std::to_string(i)), i});
		auto endTime = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
		sumTime += duration.count();
		maxTime = std::max<long long>(maxTime, duration.count());

		EXPECT_TRUE(inserted);
		EXPECT_EQ(it->first, str);
		EXPECT_EQ(it->second, i);
	}

	TestCout() << "Max time " << maxTime << " microseconds" << std::endl;
	TestCout() << "Avg time " << sumTime / kNumBucketsInPart << " microseconds" << std::endl;

	ASSERT_EQ(map.num_parts(), 16);

	for (size_t i = kNumBucketsInPart / 10; i < 6 * kNumBucketsInPart; ++i) {
		ASSERT_EQ(map.erase(reindexer::key_string("str" + std::to_string(i))), 1);
	}

	ASSERT_EQ(map.num_parts(), 1);

	for (size_t i = 0; i < kNumBucketsInPart / 10; ++i) {
		ASSERT_EQ(map.find(reindexer::key_string("str" + std::to_string(i))) == map.end(), false);
	}

	for (size_t i = kNumBucketsInPart / 10; i < kNumBucketsInPart / 5; ++i) {
		ASSERT_EQ(map.find(reindexer::key_string("str" + std::to_string(i))) == map.end(), true);
	}
}
CATCH_AND_ASSERT

using TestPayloadMapType = reindexer::extendible_hash_map<
	tsl::sparse_map<reindexer::PayloadValue, size_t, reindexer::hash_composite, reindexer::equal_composite,
					std::allocator<std::pair<reindexer::PayloadValue, size_t>>, tsl::sh::power_of_two_growth_policy<2>,
					tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>,
	reindexer::payload_map_rebalance_params, reindexer::PayloadValue, size_t, reindexer::hash_composite, reindexer::equal_composite>;

TEST(ExtendibleHashMapTest, TestingPayloadHashMapOperations) try {
	const size_t kNumBucketsInPart = reindexer::payload_map_rebalance_params::kNumBucketsInPart;

	reindexer::PayloadTypeImpl pt("pt");
	reindexer::PayloadFieldType ft1(reindexer::KeyValueType::String{}, "str1", {}, reindexer::IsArray_False);
	reindexer::PayloadFieldType ft2(reindexer::KeyValueType::String{}, "str2", {}, reindexer::IsArray_False);
	reindexer::PayloadFieldType ft3(reindexer::KeyValueType::String{}, "str3", {}, reindexer::IsArray_False);
	reindexer::PayloadFieldType ft4(reindexer::KeyValueType::Int64{}, "int1", {}, reindexer::IsArray_False);
	reindexer::PayloadFieldType ft5(reindexer::KeyValueType::Double{}, "double1", {}, reindexer::IsArray_False);
	pt.Add(ft1);
	pt.Add(ft2);
	pt.Add(ft3);
	pt.Add(ft4);
	pt.Add(ft5);

	reindexer::FieldsSet f;
	f.push_back(0);
	f.push_back(1);
	f.push_back(2);
	f.push_back(3);
	f.push_back(4);

	TestPayloadMapType map(reindexer::hash_composite(pt, f), reindexer::equal_composite(pt, f));

	std::vector<reindexer::key_string> strs;
	strs.reserve(6 * kNumBucketsInPart);
	for (size_t i = 0; i < 6 * kNumBucketsInPart; ++i) {
		strs.emplace_back("str" + std::to_string(i));
	}
	std::vector<reindexer::PayloadValue> pvs;
	for (size_t i = 0; i < 6 * kNumBucketsInPart; ++i) {
		pvs.emplace_back(100);
		reindexer::PayloadIface<reindexer::PayloadValue> plIface(pt, pvs.back());
		plIface.Set(0, reindexer::Variant(strs[i]));
		plIface.Set(1, reindexer::Variant(strs[i]));
		plIface.Set(2, reindexer::Variant(strs[i]));
		plIface.Set(3, reindexer::Variant(100));
		plIface.Set(4, reindexer::Variant(100.5));
	}

	for (size_t i = 0; i < kNumBucketsInPart; ++i) {
		map[pvs[i]] = i;
	}

	ASSERT_EQ(map.num_parts(), 2);
	for (size_t i = 0; i < kNumBucketsInPart; i += 100) {
		ASSERT_EQ(map[pvs[i]], i);
	}

	for (size_t i = kNumBucketsInPart; i < 5 * kNumBucketsInPart; ++i) {
		map[pvs[i]] = i;
	}

	long long sumTime = 0;
	long long maxTime = 0;

	for (size_t i = 5 * kNumBucketsInPart; i < 6 * kNumBucketsInPart; ++i) {
		auto startTime = std::chrono::high_resolution_clock::now();
		map[pvs[i]] = i;
		auto endTime = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
		sumTime += duration.count();
		maxTime = std::max<long long>(maxTime, duration.count());
	}

	TestCout() << "Max time " << maxTime << " microseconds" << std::endl;
	TestCout() << "Avg time " << sumTime / kNumBucketsInPart << " microseconds" << std::endl;

	ASSERT_EQ(map.num_parts(), 16);

	for (size_t i = kNumBucketsInPart / 10; i < 6 * kNumBucketsInPart; ++i) {
		ASSERT_EQ(map.erase(pvs[i]), 1);
	}

	ASSERT_EQ(map.num_parts(), 1);

	for (size_t i = 0; i < kNumBucketsInPart / 10; ++i) {
		ASSERT_EQ(map.find(pvs[i]) == map.end(), false);
	}

	for (size_t i = kNumBucketsInPart / 10; i < kNumBucketsInPart / 5; ++i) {
		ASSERT_EQ(map.find(pvs[i]) == map.end(), true);
	}
}
CATCH_AND_ASSERT

#endif	// REINDEX_WITH_TSAN

}  // namespace reindexer_tests
