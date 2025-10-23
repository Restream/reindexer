#pragma once

#include <gtest/gtest.h>
#include <string>
#include "core/keyvalue/uuid.h"
#include "estl/forward_like.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"

static constexpr std::string_view hexChars = "0123456789aAbBcCdDeEfF";
static constexpr std::string_view nilUUID = "00000000-0000-0000-0000-000000000000";
static constexpr unsigned uuidDelimPositions[] = {8, 13, 18, 23};

inline bool isUuidDelimPos(unsigned i) noexcept {
	return std::find(std::begin(uuidDelimPositions), std::end(uuidDelimPositions), i) != std::end(uuidDelimPositions);
}

inline std::string randStrUuid() {
	if (rand() % 1000 == 0) {
		return std::string{nilUUID};
	}
	std::string strUuid;
	strUuid.reserve(reindexer::Uuid::kStrFormLen);
	for (size_t i = 0; i < reindexer::Uuid::kStrFormLen; ++i) {
		if (isUuidDelimPos(i)) {
			strUuid.push_back('-');
		} else if (i == 19) {
			strUuid.push_back(hexChars[8 + rand() % (hexChars.size() - 8)]);
		} else {
			strUuid.push_back(hexChars[rand() % hexChars.size()]);
		}
	}
	return strUuid;
}

inline reindexer::Uuid randUuid() { return reindexer::Uuid{randStrUuid()}; }
inline reindexer::Uuid nilUuid() { return reindexer::Uuid{nilUUID}; }

template <typename Fn>
reindexer::VariantArray randUuidArrayImpl(Fn fillFn, size_t min, size_t max) {
	assert(min <= max);
	reindexer::VariantArray ret;
	const size_t count = min == max ? min : min + rand() % (max - min);
	ret.reserve(count);
	for (size_t i = 0; i < count; ++i) {
		fillFn(ret);
	}
	return ret;
}

inline reindexer::VariantArray randUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl([](auto& v) { v.emplace_back(randUuid()); }, min, max);
}

inline reindexer::VariantArray randStrUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl([](auto& v) { v.emplace_back(randStrUuid()); }, min, max);
}

inline reindexer::VariantArray randHeterogeneousUuidArray(size_t min, size_t max) {
	return randUuidArrayImpl(
		[](auto& v) {
			if (rand() % 2) {
				v.emplace_back(randStrUuid());
			} else {
				v.emplace_back(randUuid());
			}
		},
		min, max);
}

inline auto minMaxArgs(CondType cond, size_t max) {
	struct {
		size_t min;
		size_t max;
	} res;
	switch (cond) {
		case CondEq:
		case CondSet:
		case CondAllSet:
			res.min = 0;
			res.max = max;
			break;
		case CondLike:
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			res.min = res.max = 1;
			break;
		case CondRange:
			res.min = res.max = 2;
			break;
		case CondAny:
		case CondEmpty:
			res.min = res.max = 0;
			break;
		case CondDWithin:
		case CondKnn:
			assert(0);
	}
	return res;
}

template <typename T>
T randBin(long long min, long long max) noexcept {
	assertrx(min < max);
	const long long divider = (1ull << (rand() % 10));
	min *= divider;
	max *= divider;
	return static_cast<T>((rand() % (max - min)) + min) / static_cast<T>(divider);
}

inline reindexer::Point randPoint(long long range) noexcept {
	return reindexer::Point{randBin<double>(-range, range), randBin<double>(-range, range)};
}

template <size_t Dim>
void rndFloatVector(std::array<float, Dim>& buf) {
	for (float& v : buf) {
		v = randBin<float>(-1'000'000, 1'000'000);
	}
}

#define CATCH_AND_ASSERT                           \
	catch (const std::exception& err) {            \
		ASSERT_TRUE(false) << err.what();          \
	}                                              \
	catch (...) {                                  \
		ASSERT_TRUE(false) << "Unknown exception"; \
	}

inline const gason::JsonNode& findJsonField(const gason::JsonNode& json, std::string_view fieldName) {
	using namespace std::string_view_literals;
	std::vector<std::string_view> fields;
	reindexer::split(fieldName, "."sv, false, fields);
	assertrx(!fields.empty());
	const auto* node = &json;
	for (auto it = fields.begin(); it != fields.end() - 1; ++it) {
		node = &(*node)[*it];
		if (!node->isObject()) {
			const static auto emptyNode = gason::JsonNode::EmptyNode();
			return emptyNode;
		}
	}
	return (*node)[fields.back()];
}

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
	return [f = std::move(func)] {	// NOLINT(*.NewDeleteLeaks) False positive
		try {
			f();
		}
		CATCH_AND_ASSERT
	};
}

#define ASSERT_JSON_CONTAIN_FIELD(json, fieldName) ASSERT_FALSE(findJsonField(json, fieldName).empty()) << fieldName;

#define ASSERT_JSON_NOT_CONTAIN_FIELD(json, fieldName) ASSERT_TRUE(findJsonField(json, fieldName).empty()) << fieldName;

#define ASSERT_JSON_FIELD_ABSENT_OR_IS_NULL(json, fieldName)                \
	if (const auto& node = findJsonField(json, fieldName); !node.empty()) { \
		ASSERT_EQ(node.value.getTag(), gason::JsonTag::JSON_NULL);          \
	}

#define ASSERT_JSON_FIELD_IS_NULL(json, fieldName)                                    \
	if (const auto& node = findJsonField(json, fieldName); !node.empty()) {           \
		const auto tag = node.value.getTag();                                         \
		ASSERT_TRUE(tag == gason::JsonTag::JSON_NULL || tag == gason::JsonTag::ARRAY) \
			<< "fieldName: " << fieldName << "; tag: " << JsonTagToTypeStr(tag);      \
		if (tag == gason::JsonTag::ARRAY) {                                           \
			ASSERT_EQ(begin(node.value), end(node.value));                            \
		}                                                                             \
	}

#define ASSERT_JSON_FIELD_INT_EQ(json, fieldName, expectedVal)                      \
	{                                                                               \
		const auto field = findJsonField(json, fieldName);                          \
		const auto tag = field.value.getTag();                                      \
		ASSERT_TRUE(tag == gason::JsonTag::DOUBLE || tag == gason::JsonTag::NUMBER) \
			<< "fieldName: " << fieldName << "; tag: " << JsonTagToTypeStr(tag);    \
		ASSERT_EQ(field.value.toNumber(), expectedVal) << fieldName;                \
	}

#define ASSERT_JSON_FIELD_FLOAT_EQ(json, fieldName, expectedVal)                    \
	{                                                                               \
		const auto field = findJsonField(json, fieldName);                          \
		const auto tag = field.value.getTag();                                      \
		ASSERT_TRUE(tag == gason::JsonTag::DOUBLE || tag == gason::JsonTag::NUMBER) \
			<< "fieldName: " << fieldName << "; tag: " << JsonTagToTypeStr(tag);    \
		ASSERT_EQ(field.value.toDouble(), expectedVal) << fieldName;                \
	}

#define ASSERT_JSON_FIELD_ARRAY_EQ(json, fieldName, expectedVal)                                                \
	if (expectedVal.empty()) {                                                                                  \
		ASSERT_JSON_FIELD_IS_NULL(json, fieldName);                                                             \
	} else {                                                                                                    \
		const auto& node = findJsonField(json, fieldName);                                                      \
		ASSERT_TRUE(node.isArray()) << JsonTagToTypeStr(node.value.getTag());                                   \
		auto expectedIt = expectedVal.begin();                                                                  \
		const auto expectedEnd = expectedVal.end();                                                             \
		auto it = begin(node.value);                                                                            \
		const auto end = gason::end(node.value);                                                                \
		for (; it != end && expectedIt != expectedEnd; ++it, ++expectedIt) {                                    \
			ASSERT_EQ(it->As<std::remove_cv_t<std::remove_reference_t<decltype(*expectedIt)>>>(), *expectedIt); \
		}                                                                                                       \
		ASSERT_EQ(it, end);                                                                                     \
		ASSERT_EQ(expectedIt, expectedEnd);                                                                     \
	}
