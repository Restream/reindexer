#include "gtest/gtest.h"
#include "gtests/tests/gtest_cout.h"

#include <limits>
#include <string>
#include "core/keyvalue/allset_match_state.h"
#include "core/keyvalue/condition_comparison.h"
#include "core/keyvalue/relaxed_allset.h"
#include "core/keyvalue/uuid.h"
#include "estl/tokenizer.h"
#include "fmt/format.h"
#include "tools/enum_compare.h"

namespace reindexer_tests {

#define TEST_ENUM(name)                       \
	enum class [[nodiscard]] name : uint8_t { \
		none = 0,                             \
		v1 = 1,                               \
		v2 = 1 << 1,                          \
		v3 = 1 << 2,                          \
		v4 = 1 << 3,                          \
		v5 = 1 << 4,                          \
		v6 = 1 << 5,                          \
	};

#define ALL_ENUMS(E) E::v1, E::v2, E::v3, E::v4, E::v5, E::v6

template <auto... e>
static void Set(auto& diff, auto mask) {
	(diff.template Set<e>(!(uint8_t(mask) & uint8_t(e))), ...);
}

template <typename Enum>
static void DiffFromMask(auto& diff, Enum mask) {
	Set<ALL_ENUMS(Enum)>(diff, mask);
}

enum class [[nodiscard]] Bits { Unset, Set };

template <auto... e>
static auto tupleFromMask(auto mask, Bits bits) {
	auto cond = [&](bool set) { return bits == Bits::Set ? set : !set; };
	return std::make_tuple(cond(uint8_t(mask) & uint8_t(e)) ? e : decltype(e)::none...);
}

template <typename Enum>
static auto tupleOfEqualBitsFromMask(Enum mask) {
	return tupleFromMask<ALL_ENUMS(Enum)>(mask, Bits::Unset);
}

template <typename Enum>
static auto tupleOfNonEqualBitsFromMask(Enum mask) {
	return tupleFromMask<ALL_ENUMS(Enum)>(mask, Bits::Set);
}

template <typename... Enums>
static bool CheckAllOfIsEqual(const auto& diff, Enums... masks) {
	return std::apply([&diff](auto... args) { return diff.template AllOfIsEqual<decltype(args)...>(args...); },
					  std::tuple_cat(tupleOfEqualBitsFromMask(masks)...));
}

template <typename... Enums>
static auto& SkipByMask(auto& diff, Enums... masks) {
	return std::apply([&diff](auto... args) -> auto& { return diff.template Skip<decltype(args)...>(args...); },
					  std::tuple_cat(tupleOfNonEqualBitsFromMask(masks)...));
}

TEST_ENUM(E1)
TEST_ENUM(E2)
TEST_ENUM(E3)

TEST(EnumDiffClass, BaseTest) {
	const auto maskE1 = E1(1 + std::rand() % 63);
	const auto maskE2 = E2(1 + std::rand() % 63);
	const auto maskE3 = E3(1 + std::rand() % 63);

	TestCout() << fmt::format("Test for maskE1 = {}, maskE2 = {}, maskE3 = {}\n", uint8_t(maskE1), uint8_t(maskE2), uint8_t(maskE3));

	compare_enum::Diff<E1, E2, E3> diff;
	compare_enum::Diff<E2, E3> subDiff;

	DiffFromMask(diff, maskE1);

	DiffFromMask(subDiff, maskE2);
	DiffFromMask(subDiff, maskE3);

	diff.Set(subDiff);

	EXPECT_EQ(diff.Get<E1>(), uint8_t(maskE1));
	EXPECT_EQ(diff.Get<E2>(), uint8_t(maskE2));
	EXPECT_EQ(diff.Get<E3>(), uint8_t(maskE3));

	EXPECT_EQ(subDiff.Get<E2>(), uint8_t(maskE2));
	EXPECT_EQ(subDiff.Get<E3>(), uint8_t(maskE3));

	EXPECT_TRUE((CheckAllOfIsEqual(subDiff, maskE2, maskE3)));
	EXPECT_TRUE((CheckAllOfIsEqual(diff, maskE1, maskE2)));
	EXPECT_TRUE((CheckAllOfIsEqual(diff, maskE1, maskE2, maskE3)));

	auto diffCopy = diff;
	SkipByMask(diff, maskE3);
	EXPECT_FALSE(diff.Equal());
	SkipByMask(diff, maskE2);
	EXPECT_FALSE(diff.Equal());
	SkipByMask(diff, maskE1);
	EXPECT_TRUE(diff.Equal());

	SkipByMask(diffCopy, maskE1, maskE2, maskE3);
	EXPECT_TRUE(diffCopy.Equal());
}

TEST(AllSetMatchState, RepeatedMatchesAndReset) {
	reindexer::AllSetMatchState state;
	for (const size_t count : {100U, 101U, 2U, 101U, 100U}) {
		SCOPED_TRACE(count);
		state.Reset(count);
		for (size_t id = 0; id + 1 < count; ++id) {
			EXPECT_FALSE(state.Mark(id));
			EXPECT_FALSE(state.Mark(id));
		}
		EXPECT_TRUE(state.Mark(count - 1));
		EXPECT_TRUE(state.Mark(0));
		state.Reset(count);
		EXPECT_FALSE(state.Mark(count - 1));
		EXPECT_FALSE(state.Mark(count - 1));
	}
}

TEST(RelaxedAllSet, MixedTypeMatches) {
	using namespace reindexer;
	const Uuid first{std::string_view{"a0000000-0000-4000-8000-000000000001"}};
	const Uuid second{std::string_view{"b0000000-0000-4000-8000-000000000002"}};
	const RelaxedAllSet uuids{VariantArray{Variant{first}, Variant{second}, Variant{first}}};
	const Variant upperUuid{"A0000000-0000-4000-8000-000000000001"};
	const Variant compactUuid{"B0000000000040008000000000000002"};
	EXPECT_TRUE(uuids.Check(VariantArray{upperUuid, compactUuid}));
	EXPECT_FALSE(uuids.Check(VariantArray{upperUuid}));
	const RelaxedAllSet uuidStrings{VariantArray{upperUuid, compactUuid}};
	EXPECT_TRUE(uuidStrings.Check(VariantArray{Variant{first}, Variant{second}}));

	constexpr int64_t kExactDoubleInteger = int64_t{1} << 53;
	const RelaxedAllSet rounded{VariantArray{Variant{static_cast<double>(kExactDoubleInteger)}}};
	EXPECT_TRUE(rounded.Check(VariantArray{Variant{kExactDoubleInteger + 1}, Variant{kExactDoubleInteger}}));
	EXPECT_FALSE(rounded.Check(VariantArray{Variant{kExactDoubleInteger + 1}}));
	const RelaxedAllSet limit{VariantArray{Variant{std::numeric_limits<int64_t>::max()}}};
	EXPECT_FALSE(limit.Check(VariantArray{Variant{0x1p63}}));

	const Variant nan{std::numeric_limits<double>::quiet_NaN()};
	const RelaxedAllSet numbers{VariantArray{Variant{1}, Variant{2}}};
	EXPECT_FALSE(numbers.Check(VariantArray{nan}));
	const RelaxedAllSet nonFinite{VariantArray{nan}};
	EXPECT_FALSE(nonFinite.Check(VariantArray{Variant{1}}));
	EXPECT_FALSE(nonFinite.Check(VariantArray{Variant{1.0}}));
	const RelaxedAllSet numericStrings{VariantArray{Variant{"1"}, Variant{"02"}, Variant{"2.0"}}};
	EXPECT_FALSE(numericStrings.Check(VariantArray{nan}));
}

TEST(ConditionComparison, NaNDoesNotSatisfyComparisons) {
	using namespace reindexer;
	const auto compare = [](const Variant& lhs, const Variant& rhs) {
		return CompareForCondition<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(lhs, rhs);
	};
	const Variant doubleNan{std::numeric_limits<double>::quiet_NaN()};
	const Variant floatNan{std::numeric_limits<float>::quiet_NaN()};
	constexpr bool kTrue = true;
	const VariantArray values{Variant{1},	  Variant{int64_t{1}}, Variant{1.0}, Variant{1.0f},
							  Variant{kTrue}, Variant{"1"},		   doubleNan,	 floatNan};
	for (const Variant& nan : {doubleNan, floatNan}) {
		for (const Variant& value : values) {
			EXPECT_EQ(compare(nan, value), ComparationResult::NotComparable);
			EXPECT_EQ(compare(value, nan), ComparationResult::NotComparable);
		}
		const RelaxedAllSet onlyNumber{VariantArray{Variant{1}}};
		EXPECT_TRUE(onlyNumber.Check(VariantArray{nan, Variant{1}}));
		const RelaxedAllSet requiresNan{VariantArray{nan, Variant{1}}};
		EXPECT_FALSE(requiresNan.Check(VariantArray{Variant{1}}));
	}

	const Variant infinity{std::numeric_limits<double>::infinity()};
	EXPECT_EQ(compare(infinity, infinity), ComparationResult::Eq);
	EXPECT_EQ(compare(infinity, Variant{1}), ComparationResult::Gt);
	EXPECT_EQ(compare(Variant{-std::numeric_limits<double>::infinity()}, Variant{1}), ComparationResult::Lt);
	EXPECT_EQ(compare(Variant{"nan"}, Variant{"nan"}), ComparationResult::Eq);
	EXPECT_EQ(compare(Variant{"nan"}, Variant{1}), ComparationResult::NotComparable);
	EXPECT_EQ(compare(Variant{}, Variant{}), ComparationResult::NotComparable);
	EXPECT_TRUE(ConditionComparator<NotComparable::Return>::equal(Variant{}, Variant{}));

	const Variant tuple{VariantArray{Variant{1}, doubleNan}};
	EXPECT_EQ(compare(tuple, Variant{VariantArray{Variant{1}, Variant{2}}}), ComparationResult::NotComparable);
	EXPECT_EQ(compare(tuple, Variant{VariantArray{Variant{2}, Variant{2}}}), ComparationResult::Lt);
}

TEST(RelaxedAllSet, EachRightValueMustMatch) {
	using namespace reindexer;
	constexpr bool kTrue = true;
	struct [[nodiscard]] TestCase {
		const char* name;
		VariantArray lhs;
		VariantArray rhs;
		bool expected;
	};
	const TestCase cases[] = {
		{.name = "one left value matches several right values",
		 .lhs = {Variant{2}},
		 .rhs = {Variant{"2"}, Variant{"02"}, Variant{2}, Variant{"2"}},
		 .expected = true},
		{.name = "different strings remain distinct", .lhs = {Variant{"2"}}, .rhs = {Variant{"2"}, Variant{"02"}}, .expected = false},
		{.name = "missing right value", .lhs = {Variant{1}, Variant{2}}, .rhs = {Variant{3}, Variant{1}}, .expected = false},
		{.name = "numbers match bool", .lhs = {Variant{1}}, .rhs = {Variant{kTrue}, Variant{int64_t{1}}}, .expected = true},
		{.name = "bool and int remain distinct requirements",
		 .lhs = {Variant{"true"}},
		 .rhs = {Variant{kTrue}, Variant{1}},
		 .expected = false},
		{.name = "numeric strings do not use truthiness", .lhs = {Variant{kTrue}}, .rhs = {Variant{"1"}, Variant{"2"}}, .expected = false},
		{.name = "null matches null", .lhs = {Variant{}, Variant{2}}, .rhs = {Variant{}, Variant{"02"}}, .expected = true},
		{.name = "missing null", .lhs = {Variant{2}}, .rhs = {Variant{}}, .expected = false},
	};
	for (const auto& testCase : cases) {
		SCOPED_TRACE(testCase.name);
		const RelaxedAllSet condition{testCase.rhs};
		EXPECT_EQ(condition.Check(testCase.lhs), testCase.expected);
		EXPECT_FALSE(condition.Check(VariantArray{}));
		EXPECT_EQ(condition.Check(testCase.lhs), testCase.expected);
	}
}

TEST(RelaxedAllSet, EmptyArrays) {
	using namespace reindexer;
	const RelaxedAllSet empty{VariantArray{}};
	const RelaxedAllSet nonEmpty{VariantArray{Variant{1}}};
	EXPECT_FALSE(empty.Check(VariantArray{}));
	EXPECT_FALSE(empty.Check(VariantArray{Variant{1}}));
	EXPECT_FALSE(nonEmpty.Check(VariantArray{}));
}

TEST(RelaxedAllSet, IncompatibleTypes) {
	using namespace reindexer;
	const RelaxedAllSet number{VariantArray{Variant{2}}};
	const RelaxedAllSet text{VariantArray{Variant{"abc"}}};
	EXPECT_FALSE(number.Check(VariantArray{Variant{"abc"}}));
	EXPECT_FALSE(text.Check(VariantArray{Variant{2}}));
}

TEST(RelaxedAllSet, OwnsCopiedStrings) {
	using namespace reindexer;
	std::string first{"2"};
	std::string second{"02"};
	const auto copied = [&first, &second] {
		const RelaxedAllSet original{VariantArray{Variant{first.c_str()}, Variant{second.c_str()}}};
		return RelaxedAllSet{original};
	}();
	first[0] = '9';
	second[0] = '9';
	EXPECT_TRUE(copied.Check(VariantArray{Variant{2}}));
	EXPECT_FALSE(copied.Check(VariantArray{Variant{"2"}}));
	EXPECT_FALSE(copied.Check(VariantArray{Variant{9}, Variant{92}}));
}

namespace {

using reindexer::GetVariantFromToken;
using reindexer::TokenEnd;
using reindexer::TokenName;
using reindexer::TokenNumber;
using reindexer::TokenSign;
using reindexer::TokenString;
using reindexer::TokenSymbol;
using reindexer::Tokenizer;

struct [[nodiscard]] TokenExpect {
	reindexer::TokenType type;
	const char* text;
	bool expectVariantThrow = false;
	bool quoted = false;
};

struct [[nodiscard]] TokenizerCase {
	const char* input;
	Tokenizer::Flags flags;
	std::initializer_list<TokenExpect> tokens;
};

static bool IsNumericVariant(const reindexer::Variant& v) {
	return v.Type().EvaluateOneOf([](reindexer::KeyValueType::Int) { return true; }, [](reindexer::KeyValueType::Int64) { return true; },
								  [](reindexer::KeyValueType::Double) { return true; }, [](reindexer::KeyValueType::Float) { return true; },
								  [](auto) { return false; });
}

static void ExpectTokenization(const TokenizerCase& testCase) {
	SCOPED_TRACE(testCase.input);
	Tokenizer tokenizer{testCase.input};
	for (const auto& expected : testCase.tokens) {
		const auto token = tokenizer.NextToken(testCase.flags);
		EXPECT_EQ(token.Type(), expected.type) << "text='" << token.Text() << "'";
		EXPECT_EQ(token.Text(), expected.text);
		EXPECT_EQ(token.Quoted(), expected.quoted) << "text='" << token.Text() << "'";
		if (expected.type == TokenNumber) {
			if (expected.expectVariantThrow) {
				EXPECT_THROW(std::ignore = GetVariantFromToken(token), reindexer::Error);
			} else {
				EXPECT_TRUE(IsNumericVariant(GetVariantFromToken(token)))
					<< "GetVariantFromToken returned non-numeric for '" << token.Text() << "'";
			}
		}
	}
	const auto endToken = tokenizer.NextToken(testCase.flags);
	EXPECT_EQ(endToken.Type(), TokenEnd);
	EXPECT_TRUE(tokenizer.End());
}

}  // namespace

TEST(TokenizerBasicTokenization, DeclarativeCases) {
	const TokenizerCase cases[]{
		{"9d", Tokenizer::Flags::NoFlags, {{TokenName, "9d"}}},
		{"d9", Tokenizer::Flags::NoFlags, {{TokenName, "d9"}}},
		{"12345", Tokenizer::Flags::NoFlags, {{TokenNumber, "12345"}}},
		{"\"12345\"", Tokenizer::Flags::NoFlags, {{TokenName, "12345", false, true}}},
		{"1ee5", Tokenizer::Flags::NoFlags, {{TokenName, "1ee5"}}},
		{"1e5", Tokenizer::Flags::NoFlags, {{TokenNumber, "1e5"}}},
		{"1E5", Tokenizer::Flags::NoFlags, {{TokenNumber, "1E5"}}},
		{"1E5", Tokenizer::Flags::ToLower, {{TokenNumber, "1E5"}}},
		{"1.2E-3", Tokenizer::Flags::NoFlags, {{TokenNumber, "1.2E-3"}}},
		{"1.2", Tokenizer::Flags::NoFlags, {{TokenNumber, "1.2"}}},
		{"1.2.3", Tokenizer::Flags::NoFlags, {{TokenName, "1.2.3"}}},
		{"-1.2e-3", Tokenizer::Flags::NoFlags, {{TokenNumber, "-1.2e-3"}}},
		{"e5", Tokenizer::Flags::NoFlags, {{TokenName, "e5"}}},
		{".5", Tokenizer::Flags::NoFlags, {{TokenSymbol, "."}, {TokenNumber, "5"}}},
		{"index+field", Tokenizer::Flags::NoFlags, {{TokenName, "index"}, {TokenSign, "+"}, {TokenName, "field"}}},
		{"a+b", Tokenizer::Flags::NoFlags, {{TokenName, "a"}, {TokenSign, "+"}, {TokenName, "b"}}},
		{"a+b", Tokenizer::Flags::TreatSignAsToken, {{TokenName, "a"}, {TokenSign, "+"}, {TokenName, "b"}}},
		{"+5", Tokenizer::Flags::NoFlags, {{TokenNumber, "+5"}}},
		{"+5", Tokenizer::Flags::TreatSignAsToken, {{TokenSign, "+"}, {TokenNumber, "5"}}},
		{"++5", Tokenizer::Flags::NoFlags, {{TokenSign, "+"}, {TokenNumber, "+5"}}},
		{"++5", Tokenizer::Flags::TreatSignAsToken, {{TokenSign, "+"}, {TokenSign, "+"}, {TokenNumber, "5"}}},
		{"+-5", Tokenizer::Flags::NoFlags, {{TokenSign, "+"}, {TokenNumber, "-5"}}},
		{"+-5", Tokenizer::Flags::TreatSignAsToken, {{TokenSign, "+"}, {TokenSign, "-"}, {TokenNumber, "5"}}},
		{"a+5", Tokenizer::Flags::NoFlags, {{TokenName, "a"}, {TokenNumber, "+5"}}},
		{"a+5", Tokenizer::Flags::TreatSignAsToken, {{TokenName, "a"}, {TokenSign, "+"}, {TokenNumber, "5"}}},
		{"NS.123ABC", Tokenizer::Flags::NoFlags, {{TokenName, "NS.123ABC"}}},
		{"NS.123ABC", Tokenizer::Flags::ToLower, {{TokenName, "ns.123abc"}}},
		{"123abc", Tokenizer::Flags::NoFlags, {{TokenName, "123abc"}}},
		{"+.", Tokenizer::Flags::NoFlags, {{TokenSign, "+"}, {TokenSymbol, "."}}},
		{"- 5", Tokenizer::Flags::NoFlags, {{TokenSign, "-"}, {TokenNumber, "5"}}},
		{"-.5", Tokenizer::Flags::NoFlags, {{TokenNumber, "-.5"}}},
		{"1.", Tokenizer::Flags::NoFlags, {{TokenNumber, "1."}}},
		{"123*456", Tokenizer::Flags::NoFlags, {{TokenNumber, "123"}, {TokenSymbol, "*"}, {TokenNumber, "456"}}},
		{"*field", Tokenizer::Flags::NoFlags, {{TokenSymbol, "*"}, {TokenName, "field"}}},
		{"+1.2.3", Tokenizer::Flags::NoFlags, {{TokenSign, "+"}, {TokenName, "1.2.3"}}},
		{"", Tokenizer::Flags::NoFlags, {}},
		{" ", Tokenizer::Flags::NoFlags, {}},
		{"\t", Tokenizer::Flags::NoFlags, {}},
		{"\n", Tokenizer::Flags::NoFlags, {}},
		{"\tabc", Tokenizer::Flags::NoFlags, {{TokenName, "abc"}}},
		{"\t123", Tokenizer::Flags::NoFlags, {{TokenNumber, "123"}}},
		{"\t-.e23", Tokenizer::Flags::NoFlags, {{TokenSign, "-"}, {TokenSymbol, "."}, {TokenName, "e23"}}},
		{"'abc'", Tokenizer::Flags::NoFlags, {{TokenString, "abc"}}},
		{"true", Tokenizer::Flags::NoFlags, {{TokenName, "true"}}},
		{"\"true\"", Tokenizer::Flags::NoFlags, {{TokenName, "true", false, true}}},
		{"'true'", Tokenizer::Flags::NoFlags, {{TokenString, "true"}}},
		{"false", Tokenizer::Flags::NoFlags, {{TokenName, "false"}}},
		{"\"false\"", Tokenizer::Flags::NoFlags, {{TokenName, "false", false, true}}},
		{"'false'", Tokenizer::Flags::NoFlags, {{TokenString, "false"}}},
		{"null", Tokenizer::Flags::NoFlags, {{TokenName, "null"}}},
		{"\"null\"", Tokenizer::Flags::NoFlags, {{TokenName, "null", false, true}}},
		{"'null'", Tokenizer::Flags::NoFlags, {{TokenString, "null"}}},
		// Special case: number too large for int64_t, but tokenizer treats it as a number. Not sure if it's a good idea, but that's how it
		// works for a long time.
		{"9999999999999999999", Tokenizer::Flags::NoFlags, {{TokenNumber, "9999999999999999999", true}}},
		{"-0", Tokenizer::Flags::NoFlags, {{TokenNumber, "-0"}}},
		{"..", Tokenizer::Flags::NoFlags, {{TokenSymbol, "."}, {TokenSymbol, "."}}},
		{"1..2", Tokenizer::Flags::NoFlags, {{TokenName, "1..2"}}},
		{"1e+", Tokenizer::Flags::NoFlags, {{TokenName, "1e"}, {TokenSign, "+"}}},
		{"1e-", Tokenizer::Flags::NoFlags, {{TokenName, "1e"}, {TokenSign, "-"}}},
		{".e+", Tokenizer::Flags::NoFlags, {{TokenSymbol, "."}, {TokenName, "e"}, {TokenSign, "+"}}},
		{".e", Tokenizer::Flags::NoFlags, {{TokenSymbol, "."}, {TokenName, "e"}}},
		{"1.2.3e4", Tokenizer::Flags::NoFlags, {{TokenName, "1.2.3e4"}}},
		{"(a+b)",
		 Tokenizer::Flags::NoFlags,
		 {{TokenSymbol, "("}, {TokenName, "a"}, {TokenSign, "+"}, {TokenName, "b"}, {TokenSymbol, ")"}}},
		{"a+-b", Tokenizer::Flags::NoFlags, {{TokenName, "a"}, {TokenSign, "+"}, {TokenSign, "-"}, {TokenName, "b"}}},
		{"a + b", Tokenizer::Flags::NoFlags, {{TokenName, "a"}, {TokenSign, "+"}, {TokenName, "b"}}},
		{"1+-2", Tokenizer::Flags::NoFlags, {{TokenNumber, "1"}, {TokenSign, "+"}, {TokenNumber, "-2"}}},
		{"1 + 2", Tokenizer::Flags::NoFlags, {{TokenNumber, "1"}, {TokenSign, "+"}, {TokenNumber, "2"}}},
	};

	for (const auto& testCase : cases) {
		ExpectTokenization(testCase);
	}
}
}  // namespace reindexer_tests
