#pragma once

#include <stdarg.h>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"

using std::string;
using std::vector;
using std::unique_ptr;

using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::p_string;

namespace internal {

template <typename T>
struct to_array_helper {
	static VariantArray to_array(const vector<T>& vec) {
		VariantArray krs;
		krs.reserve(vec.size());
		for (auto& value : vec) krs.push_back(Variant{value});
		return krs;
	}
};

template <>
struct to_array_helper<string> {
	static VariantArray to_array(const vector<string>& vec) {
		VariantArray krs;
		krs.reserve(vec.size());
		for (auto& value : vec) krs.push_back(Variant{p_string(value.c_str())});
		return krs;
	}
};

}  // namespace internal

template <typename T>
VariantArray toArray(const vector<T>& vec) {
	return internal::to_array_helper<T>::to_array(vec);
}

template <typename T = int>
T random(T from, T to) {
	thread_local static std::mt19937 gen(std::random_device{}());

	using dist_type =
		typename std::conditional<std::is_integral<T>::value, std::uniform_int_distribution<T>, std::uniform_real_distribution<T> >::type;

	thread_local static dist_type dist;

	return dist(gen, typename dist_type::param_type{from, to});
}

template <typename T>
vector<T> randomNumArray(int count, int start, int region) {
	vector<T> result;
	for (int i = 0; i < count; i++) result.emplace_back(random<T>(start, start + region));
	return result;
}

string FormatString(const char* msg, va_list args);
string FormatString(const char* msg, ...);
string HumanReadableNumber(size_t number, bool si, const string& unitLabel = "");
