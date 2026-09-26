#include "gtest/gtest.h"
#include "gtests/tests/gtest_cout.h"

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
