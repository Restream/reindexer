#pragma once

#include <gtest/gtest.h>
#include <array>
#include <functional>
#include <random>
#include <string>
#include <string_view>
#include "core/enums.h"
#include "core/keyvalue/uuid.h"
#include "core/type_consts.h"
#include "estl/forward_like.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests_tools {

/// Makes random SQL LIKE pattern that matches the given string.
std::string makeLikePattern(std::string_view utf8Str);

/// Converts SQL LIKE pattern to regular expression in ECMAScript grammar.
std::string sqlLikePattern2ECMAScript(std::string pattern);

std::string randStrUuid();
reindexer::Uuid randUuid();
reindexer::Uuid nilUuid();
reindexer::VariantArray randUuidArray(size_t min, size_t max);
reindexer::VariantArray randStrUuidArray(size_t min, size_t max);
reindexer::VariantArray randHeterogeneousUuidArray(size_t min, size_t max);

struct [[nodiscard]] MinMaxArgs {
	size_t min;
	size_t max;
};
MinMaxArgs minMaxArgs(CondType cond, size_t max);

template <typename T>
T randBin(long long min, long long max) noexcept {
	assertrx(min < max);
	const long long divider = (1ull << (rand() % 10));
	min *= divider;
	max *= divider;
	return static_cast<T>((rand() % (max - min)) + min) / static_cast<T>(divider);
}

reindexer::Point randPoint(long long range);

template <size_t Dim>
void rndFloatVector(std::array<float, Dim>& buf) {
	static thread_local std::random_device rd;
	static thread_local std::mt19937 gen(rd());
	static thread_local std::normal_distribution<> nd(0, 0.25);

	for (float& v : buf) {
		v = nd(gen);
	}
}

#define CATCH_AND_ASSERT                           \
	catch (const std::exception& err) {            \
		ASSERT_TRUE(false) << err.what();          \
	}                                              \
	catch (...) {                                  \
		ASSERT_TRUE(false) << "Unknown exception"; \
	}

const gason::JsonNode& findJsonField(const gason::JsonNode& json, std::string_view fieldName);

template <typename Cont>
auto&& randOneOf(Cont&& cont) {
	assertrx(!std::empty(cont));
	auto it = std::begin(cont);
	std::advance(it, rand() % std::size(cont));
	return reindexer::forward_like<Cont>(*it);
}

template <typename T1, typename T2, typename... Ts>
T1 randOneOf(T1 v1, T2 v2, Ts... vs) {
	return std::move(randOneOf(std::initializer_list<T1>{std::move(v1), std::move(v2), std::move(vs)...}));
}

inline std::function<void()> exceptionWrapper(std::function<void()>&& func) {
	// NOLINTNEXTLINE(rx-perf-lambda-to-std-function-allocation)
	return [f = std::move(func)] {	// NOLINT(*.NewDeleteLeaks) False positive
		try {
			f();
		}
		CATCH_AND_ASSERT
	};
}

reindexer::VectorMetric randMetric() noexcept;

#define ASSERT_JSON_CONTAIN_FIELD(json, fieldName) ASSERT_FALSE(reindexer_tests_tools::findJsonField(json, fieldName).empty()) << fieldName;

#define ASSERT_JSON_NOT_CONTAIN_FIELD(json, fieldName) \
	ASSERT_TRUE(reindexer_tests_tools::findJsonField(json, fieldName).empty()) << fieldName;

#define ASSERT_JSON_FIELD_ABSENT_OR_IS_NULL(json, fieldName)                                         \
	if (const auto& node = reindexer_tests_tools::findJsonField(json, fieldName); !node.isEmpty()) { \
		ASSERT_EQ(node.value.getTag(), gason::JsonTag::JSON_NULL);                                   \
	}

#define ASSERT_JSON_IS_NULL(json, fieldName)                                        \
	const auto tag = json.getTag();                                                 \
	ASSERT_TRUE(tag == gason::JsonTag::JSON_NULL || tag == gason::JsonTag::ARRAY)   \
		<< "fieldName: " << fieldName << "; tag: " << gason::JsonTagToTypeStr(tag); \
	if (tag == gason::JsonTag::ARRAY) {                                             \
		ASSERT_EQ(gason::begin(json), gason::end(json));                            \
	}

#define ASSERT_JSON_FIELD_IS_NULL(json, fieldName)                                                   \
	if (const auto& node = reindexer_tests_tools::findJsonField(json, fieldName); !node.isEmpty()) { \
		ASSERT_JSON_IS_NULL(node.value, fieldName);                                                  \
	}

#define ASSERT_JSON_FIELD_INT_EQ(json, fieldName, expectedVal)                          \
	{                                                                                   \
		const auto field = reindexer_tests_tools::findJsonField(json, fieldName);       \
		const auto tag = field.value.getTag();                                          \
		ASSERT_TRUE(tag == gason::JsonTag::DOUBLE || tag == gason::JsonTag::NUMBER)     \
			<< "fieldName: " << fieldName << "; tag: " << gason::JsonTagToTypeStr(tag); \
		ASSERT_EQ(field.value.toNumber(), expectedVal) << fieldName;                    \
	}

#define ASSERT_JSON_FIELD_FLOAT_EQ(json, fieldName, expectedVal)                        \
	{                                                                                   \
		const auto field = reindexer_tests_tools::findJsonField(json, fieldName);       \
		const auto tag = field.value.getTag();                                          \
		ASSERT_TRUE(tag == gason::JsonTag::DOUBLE || tag == gason::JsonTag::NUMBER)     \
			<< "fieldName: " << fieldName << "; tag: " << gason::JsonTagToTypeStr(tag); \
		ASSERT_EQ(field.value.toDouble(), expectedVal) << fieldName;                    \
	}

#define ASSERT_JSON_ARRAY_EQ(json, fieldName, expectedVal)                                                      \
	if (expectedVal.empty()) {                                                                                  \
		ASSERT_JSON_IS_NULL(json.value, fieldName);                                                             \
	} else {                                                                                                    \
		ASSERT_TRUE(json.isArray()) << gason::JsonTagToTypeStr(json.value.getTag());                            \
		auto expectedIt = expectedVal.begin();                                                                  \
		const auto expectedEnd = expectedVal.end();                                                             \
		auto it = gason::begin(json.value);                                                                     \
		const auto end = gason::end(json.value);                                                                \
		for (; it != end && expectedIt != expectedEnd; ++it, ++expectedIt) {                                    \
			ASSERT_EQ(it->As<std::remove_cv_t<std::remove_reference_t<decltype(*expectedIt)>>>(), *expectedIt); \
		}                                                                                                       \
		ASSERT_EQ(it, end);                                                                                     \
		ASSERT_EQ(expectedIt, expectedEnd);                                                                     \
	}

#define ASSERT_JSON_FIELD_ARRAY_EQ(json, fieldName, expectedVal)                  \
	if (expectedVal.empty()) {                                                    \
		ASSERT_JSON_FIELD_IS_NULL(json, fieldName);                               \
	} else {                                                                      \
		const auto& node = reindexer_tests_tools::findJsonField(json, fieldName); \
		ASSERT_JSON_ARRAY_EQ(node, fieldName, expectedVal);                       \
	}

}  // namespace reindexer_tests_tools
