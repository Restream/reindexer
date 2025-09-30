#pragma once

#include <stdarg.h>
#include <random>
#include <string>
#include <vector>

#include <span>
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"

using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::p_string;

namespace reindexer {
class Reindexer;
}  // namespace reindexer

namespace internal {

template <typename T>
struct [[nodiscard]] to_array_helper {
	static VariantArray to_array(const std::vector<T>& vec) {
		VariantArray krs;
		krs.reserve(vec.size());
		for (auto& value : vec) {
			krs.push_back(Variant{value});
		}
		return krs;
	}
};

template <>
struct [[nodiscard]] to_array_helper<std::string> {
	static VariantArray to_array(const std::vector<std::string>& vec) {
		VariantArray krs;
		krs.reserve(vec.size());
		for (auto& value : vec) {
			krs.push_back(Variant{p_string(value.c_str())});
		}
		return krs;
	}
};

}  // namespace internal

static inline std::string randString(size_t size) {
	constexpr static std::string_view ch{"qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM"};
	std::string ret(size, ' ');
	for (size_t i = 0; i < size; ++i) {
		ret[i] = ch[rand() % ch.size()];
	}
	return ret;
}

template <size_t L>
std::span<const bool> randBoolArray() {
	static bool ret[L];
	for (size_t i = 0; i < L; ++i) {
		ret[i] = rand() % 2;
	}
	return ret;
}

template <size_t L>
std::span<const int> randIntArray() {
	static int ret[L];
	for (size_t i = 0; i < L; ++i) {
		ret[i] = rand();
	}
	return ret;
}

template <size_t L>
std::span<const int64_t> randInt64Array() {
	static int64_t ret[L];
	for (size_t i = 0; i < L; ++i) {
		ret[i] = rand();
	}
	return ret;
}

template <size_t L>
std::span<const double> randDoubleArray() {
	static double ret[L];
	for (size_t i = 0; i < L; ++i) {
		ret[i] = double(rand()) / (rand() + 1);
	}
	return ret;
}

template <size_t L>
std::span<const std::string> randStringArray() {
	static std::string ret[L];
	for (size_t i = 0; i < L; ++i) {
		ret[i] = randString(L);
	}
	return ret;
}

template <typename T>
VariantArray toArray(const std::vector<T>& vec) {
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
std::vector<T> randomNumArray(int count, int start, int region) {
	std::vector<T> result;
	result.reserve(count);
	for (int i = 0; i < count; i++) {
		result.emplace_back(random<T>(start, start + region));
	}
	return result;
}

std::string FormatString(const char* msg, va_list args);
std::string FormatString(const char* msg, ...);
std::string HumanReadableNumber(size_t number, bool si, const std::string& unitLabel = "");

std::shared_ptr<reindexer::Reindexer> InitBenchDB(std::string_view dbDir);
