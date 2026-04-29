#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include "core/index/payload_map.h"
#include "core/index/string_map.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"

using namespace std::string_view_literals;

#ifndef REINDEX_WITH_TSAN

using TestStringMapType =
	extendible_hash_map<tsl::sparse_map<reindexer::key_string, size_t, reindexer::hash_key_string, reindexer::equal_key_string,
										std::allocator<std::pair<reindexer::key_string, size_t>>, tsl::sh::power_of_two_growth_policy<2>,
										tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>,
						reindexer::str_map_rebalance_params, reindexer::key_string, size_t, reindexer::hash_key_string,
						reindexer::equal_key_string>;

TEST(ExtendibleHashMapTest, TestingStringHahMapOperations) try {
	const size_t kNumBucketsInPart = reindexer::str_map_rebalance_params::kNumBucketsInPart;
	TestStringMapType map;
	for (size_t i = 0; i < kNumBucketsInPart; ++i) {
		map.insert({reindexer::key_string("str" + std::to_string(i)), i});
	}

	ASSERT_EQ(map.num_parts(), 2);
	for (size_t i = 0; i < kNumBucketsInPart; i += 100) {
		ASSERT_EQ(map.find(reindexer::key_string("str" + std::to_string(i))) == map.end(), false);
	}

	for (size_t i = kNumBucketsInPart; i < 5 * kNumBucketsInPart; ++i) {
		map.insert({reindexer::key_string("str" + std::to_string(i)), i});
	}

	long long sumTime = 0;
	long long maxTime = 0;

	for (size_t i = 5 * kNumBucketsInPart; i < 6 * kNumBucketsInPart; ++i) {
		auto startTime = std::chrono::high_resolution_clock::now();
		map.insert({reindexer::key_string("str" + std::to_string(i)), i});
		auto endTime = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
		sumTime += duration.count();
		maxTime = std::max<long long>(maxTime, duration.count());
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

using TestPayloadMapType =
	extendible_hash_map<tsl::sparse_map<reindexer::PayloadValue, size_t, reindexer::hash_composite, reindexer::equal_composite,
										std::allocator<std::pair<reindexer::PayloadValue, size_t>>, tsl::sh::power_of_two_growth_policy<2>,
										tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>,
						reindexer::payload_map_rebalance_params, reindexer::PayloadValue, size_t, reindexer::hash_composite,
						reindexer::equal_composite>;

TEST(ExtendibleHashMapTest, TestingPayloadHahMapOperations) try {
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