#include <ostream>
#include "estl/tokenizer.h"
#include "gtest/gtest.h"
#include "tools/stringstools.h"

class CustomStrCompareApi : public virtual ::testing::Test {
public:
	enum class ComparisonResult { Less, Greater, Equal };
	static std::string_view ComparisonResultToString(ComparisonResult r) {
		switch (r) {
			case ComparisonResult::Less:
				return "less";
			case ComparisonResult::Greater:
				return "greater";
			case ComparisonResult::Equal:
				return "equal";
		}
		return "<unknown>";
	}

	struct StringTestCase {
		std::string str1;
		std::string str2;
		ComparisonResult expectedResult;

		friend std::ostream& operator<<(std::ostream& os, const StringTestCase& c) {
			os << fmt::format("{{ str1: '{}', str2: '{}', expected comparison result: {} }}", c.str1, c.str2,
							  ComparisonResultToString(c.expectedResult));
			return os;
		}
	};

	ComparisonResult InvertComparisonResult(ComparisonResult v) {
		switch (v) {
			case ComparisonResult::Less:
				return ComparisonResult::Greater;
			case ComparisonResult::Greater:
				return ComparisonResult::Less;
			case ComparisonResult::Equal:
				return ComparisonResult::Equal;
		}
		std::abort();
	}
	void ValidateLessResult(bool less, const StringTestCase& c) {
		switch (c.expectedResult) {
			case ComparisonResult::Less:
				EXPECT_TRUE(less) << c;
				return;
			case ComparisonResult::Greater:
				EXPECT_FALSE(less) << c;
				return;
			case ComparisonResult::Equal:
				EXPECT_FALSE(less) << c;
				return;
		}
		std::abort();
	}
	void ValidateEqualResult(bool equal, const StringTestCase& c) {
		switch (c.expectedResult) {
			case ComparisonResult::Less:
				EXPECT_FALSE(equal) << c;
				return;
			case ComparisonResult::Greater:
				EXPECT_FALSE(equal) << c;
				return;
			case ComparisonResult::Equal:
				EXPECT_TRUE(equal) << c;
				return;
		}
		std::abort();
	}
	template <typename ComparatorT>
	void CheckTypeConversions(const StringTestCase& c) {
		ComparatorT compare;
		auto r1 = compare(c.str1, c.str2);
		auto r2 = compare(c.str1, std::string_view(c.str2));
		auto r3 = compare(std::string_view(c.str1), c.str2);
		auto r4 = compare(std::string_view(c.str1), std::string_view(c.str2));
		EXPECT_EQ(r1, r2);
		EXPECT_EQ(r1, r3);
		EXPECT_EQ(r1, r4);
	}

	const std::vector<StringTestCase> kCaseSensitiveCases = {{"abc", "aBc", ComparisonResult::Greater},
															 {"abcdef", "abcdef", ComparisonResult::Equal},
															 {"abcdef", "abcde", ComparisonResult::Greater},
															 {"abcdef", "aecde", ComparisonResult::Less},
															 {"abcdef", "aBcde", ComparisonResult::Greater},
															 {"abc", "aBcde", ComparisonResult::Greater},
															 {"", "abc", ComparisonResult::Less}};
	const std::vector<StringTestCase> kCaseInsensitiveCases = {{"abc", "aBc", ComparisonResult::Equal},
															   {"abcdef", "abcdef", ComparisonResult::Equal},
															   {"abcdef", "abcde", ComparisonResult::Greater},
															   {"abcdef", "aecde", ComparisonResult::Less},
															   {"abcdef", "aBcde", ComparisonResult::Greater},
															   {"abc", "aBcde", ComparisonResult::Less},
															   {"", "abc", ComparisonResult::Less}};
};

TEST_F(CustomStrCompareApi, Less) {
	using CompareT = reindexer::less_str;
	for (auto& c : kCaseSensitiveCases) {
		CheckTypeConversions<CompareT>(c);
		CompareT less;
		auto res = less(c.str1, c.str2);
		ValidateLessResult(res, c);
		res = less(c.str2, c.str1);
		StringTestCase cInverted{c.str2, c.str1, InvertComparisonResult(c.expectedResult)};
		ValidateLessResult(res, cInverted);
	}
}

TEST_F(CustomStrCompareApi, Equal) {
	using CompareT = reindexer::equal_str;
	for (auto& c : kCaseSensitiveCases) {
		CheckTypeConversions<CompareT>(c);
		CompareT equal;
		auto res = equal(c.str1, c.str2);
		ValidateEqualResult(res, c);
		res = equal(c.str2, c.str1);
		StringTestCase cInverted{c.str2, c.str1, InvertComparisonResult(c.expectedResult)};
		ValidateEqualResult(res, cInverted);
	}
}

TEST_F(CustomStrCompareApi, ILess) {
	using CompareT = reindexer::nocase_less_str;
	for (auto& c : kCaseInsensitiveCases) {
		CheckTypeConversions<CompareT>(c);
		CompareT less;
		auto res = less(c.str1, c.str2);
		ValidateLessResult(res, c);
		res = less(c.str2, c.str1);
		StringTestCase cInverted{c.str2, c.str1, InvertComparisonResult(c.expectedResult)};
		ValidateLessResult(res, cInverted);
	}
}

TEST_F(CustomStrCompareApi, IEqual) {
	using CompareT = reindexer::nocase_equal_str;
	for (auto& c : kCaseInsensitiveCases) {
		CheckTypeConversions<CompareT>(c);
		CompareT equal;
		auto res = equal(c.str1, c.str2);
		ValidateEqualResult(res, c);
		res = equal(c.str2, c.str1);
		StringTestCase cInverted{c.str2, c.str1, InvertComparisonResult(c.expectedResult)};
		ValidateEqualResult(res, cInverted);
	}
}

TEST(ConversionStringToNumber, DetectValueTypeTest) {
	using namespace reindexer;
	constexpr auto Int64V = KeyValueType{KeyValueType::Int64{}};
	constexpr auto DoubleV = KeyValueType{KeyValueType::Double{}};
	constexpr auto StringV = KeyValueType{KeyValueType::String{}};

	struct TestCase {
		struct Empty {};
		TestCase(std::string_view value, KeyValueType expectedType, std::variant<int64_t, double, Empty> expectedValue = Empty{})
			: value(value),
			  expectedType(expectedType),
			  expectedValue(std::visit(overloaded{[](auto v) { return Variant(v); },
												  [value](Empty) { return Variant(make_key_string(value.data(), value.length())); }},
									   expectedValue)) {}

		std::string_view value;
		KeyValueType expectedType;
		Variant expectedValue;
	};

	std::initializer_list<TestCase> values = {
		{"9223372036854775807", Int64V, 9223372036854775807},
		{"9223372036854775807.", Int64V, 9223372036854775807},
		{"9223372036854775807.0", Int64V, 9223372036854775807},
		{"9223372036854775807.000", Int64V, 9223372036854775807},
		{"9223372036854775807.00000", Int64V, 9223372036854775807},

		{"+9223372036854775807", Int64V, 9223372036854775807},
		{"+9223372036854775807.", Int64V, 9223372036854775807},
		{"+9223372036854775807.0", Int64V, 9223372036854775807},
		{"+9223372036854775807.000", Int64V, 9223372036854775807},
		{"+9223372036854775807.00000", Int64V, 9223372036854775807},

		{"-9223372036854775807", Int64V, -9223372036854775807},
		{"-9223372036854775807.", Int64V, -9223372036854775807},
		{"-9223372036854775807.0", Int64V, -9223372036854775807},
		{"-9223372036854775807.000", Int64V, -9223372036854775807},
		{"-9223372036854775807.00000", Int64V, -9223372036854775807},

		{"-922337203685477580.7", DoubleV, -922337203685477580.7},
		{"922337203685477580.7", DoubleV, 922337203685477580.7},
		{"92247758070.00456402", DoubleV, 92247758070.00456402},
		{"+92247758070.00456402", DoubleV, 92247758070.00456402},
		{"+922358070.000002", DoubleV, 922358070.000002},
		{"-922358070.000002", DoubleV, -922358070.000002},
		{"92547758070.1", DoubleV, 92547758070.1},

		{"9223372036854775807.01", DoubleV, 9223372036854775807.01},
		{"92233720368547758070.0002", DoubleV, 92233720368547758070.0002},
		{"9223372036854775807.000002", DoubleV, 9223372036854775807.000002},
		{"92233720368547758070.1", DoubleV, 92233720368547758070.1},
		{"92233720368547758070.01", DoubleV, 92233720368547758070.01},
		{"92233720368547758070.0002", DoubleV, 92233720368547758070.0002},
		{"9223372036854775834257834562345234654324524070.00023452346452345234452", DoubleV,
		 9223372036854775834257834562345234654324524070.00023452346452345234452},
		{"92233720368547758070.000012", DoubleV, 92233720368547758070.000012},

		{"", StringV},
		{"-", StringV},
		{"+", StringV},
		{".", StringV},
		{" ", StringV},
		{"str", StringV},
		{"s", StringV},

		{"92233720368547758070", StringV},
		{"92233720368547758070.", DoubleV, 92233720368547758070.0},
		{"92233720368547758070.0", DoubleV, 92233720368547758070.0},
		{"92233720368547758070.000", DoubleV, 92233720368547758070.0},
		{"92233720368547758070.00000", DoubleV, 92233720368547758070.0},

		{"+92233720368547758070", StringV},
		{"+92233720368547758070.", DoubleV, 92233720368547758070.0},
		{"+92233720368547758070.0", DoubleV, 92233720368547758070.0},
		{"+92233720368547758070.000", DoubleV, 92233720368547758070.0},
		{"+92233720368547758070.00000", DoubleV, 92233720368547758070.0},

		{"-92233720368547758070", StringV},
		{"-92233720368547758070.", DoubleV, -92233720368547758070.0},
		{"-92233720368547758070.0", DoubleV, -92233720368547758070.0},
		{"-92233720368547758070.000", DoubleV, -92233720368547758070.0},
		{"-92233720368547758070.00000", DoubleV, -92233720368547758070.0},

		{"9'223'372'036'854'775'807", StringV},
		{"9223372802345370L", StringV},
		{"92233728070.0145L", StringV},

		{"1.35e10", StringV},
		{"1.1e-2", StringV},
		{"123.456.7", StringV},
	};

	auto genToken = [](std::string_view text) {
		token tok{TokenNumber};
		tok.text_ = {text.begin(), text.end()};
		return tok;
	};

	for (const auto& testCase : values) {
		auto res = getVariantFromToken(genToken(testCase.value));

		std::stringstream expected, actual;
		testCase.expectedValue.Dump(expected);
		res.Dump(actual);

		auto errMessage = fmt::format("token value: {}; expected: {}; get: {}", testCase.value, expected.str(), actual.str());
		EXPECT_EQ(testCase.expectedValue, res) << errMessage;
		EXPECT_TRUE(res.Type().IsSame(testCase.expectedType)) << errMessage;
	}
}
