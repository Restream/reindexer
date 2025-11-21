#include <ostream>
#include "estl/tokenizer.h"
#include "gtest/gtest.h"
#include "tools/stringstools.h"

class [[nodiscard]] CustomStrCompareApi : public virtual ::testing::Test {
public:
	enum class [[nodiscard]] ComparisonResult { Less, Greater, Equal };
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

	struct [[nodiscard]] StringTestCase {
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

	struct [[nodiscard]] TestCase {
		struct [[nodiscard]] Empty {};
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

		{"1.35e10", DoubleV, 1.35e10},
		{"1.1e-2", DoubleV, 1.1e-2},
		{"2.e-2", DoubleV, 2e-2},
		{"2.e2", DoubleV, 2e2},
		{"2e-2", DoubleV, 2e-2},
		{"2e+2", DoubleV, 2e+2},
		{"2ee2", StringV},
		{"2e.2", StringV},
		{"2-2e", StringV},
		{"2e", StringV},
		{"2e-", StringV},
		{"2e+", StringV},
		{"-e1", StringV},

		{"123.456.7", StringV},
	};

	auto genToken = [](std::string_view text) { return Token{TokenNumber, text.begin(), text.end()}; };

	for (const auto& testCase : values) {
		auto res = GetVariantFromToken(genToken(testCase.value));

		std::stringstream expected, actual;
		testCase.expectedValue.Dump(expected);
		res.Dump(actual);

		auto errMessage = fmt::format("token value: {}; expected: {}; get: {}", testCase.value, expected.str(), actual.str());
		EXPECT_EQ(testCase.expectedValue, res) << errMessage;
		EXPECT_TRUE(res.Type().IsSame(testCase.expectedType)) << errMessage;
	}
}

TEST(StringToolsTest, EscapeBasicCharacters) {
	EXPECT_EQ(reindexer::escapeString("hello"), "hello");
	EXPECT_EQ(reindexer::escapeString(""), "");
}

TEST(StringToolsTest, EscapeControlCharacters) {
	EXPECT_EQ(reindexer::escapeString("\b"), "\\b");
	EXPECT_EQ(reindexer::escapeString("\f"), "\\f");
	EXPECT_EQ(reindexer::escapeString("\n"), "\\n");
	EXPECT_EQ(reindexer::escapeString("\r"), "\\r");
	EXPECT_EQ(reindexer::escapeString("\t"), "\\t");
	EXPECT_EQ(reindexer::escapeString("\\"), "\\\\");
	EXPECT_EQ(reindexer::escapeString("\""), "\\\"");
}

TEST(StringToolsTest, EscapeMixedContent) {
	EXPECT_EQ(reindexer::escapeString("Hello\nWorld\t!"), "Hello\\nWorld\\t!");
	EXPECT_EQ(reindexer::escapeString("Path: C:\\Windows\\System"), "Path: C:\\\\Windows\\\\System");
	EXPECT_EQ(reindexer::escapeString("Quote: \"Hello\""), "Quote: \\\"Hello\\\"");
}

TEST(StringToolsTest, UnescapeBasicCharacters) {
	EXPECT_EQ(reindexer::unescapeString("hello"), "hello");
	EXPECT_EQ(reindexer::unescapeString(""), "");
}

TEST(StringToolsTest, UnescapeControlCharacters) {
	EXPECT_EQ(reindexer::unescapeString("\\b"), "\b");
	EXPECT_EQ(reindexer::unescapeString("\\f"), "\f");
	EXPECT_EQ(reindexer::unescapeString("\\n"), "\n");
	EXPECT_EQ(reindexer::unescapeString("\\r"), "\r");
	EXPECT_EQ(reindexer::unescapeString("\\t"), "\t");
	EXPECT_EQ(reindexer::unescapeString("\\\\"), "\\");
	EXPECT_EQ(reindexer::unescapeString("\\\""), "\"");
	EXPECT_EQ(reindexer::unescapeString("\\/"), "/");
}

TEST(StringToolsTest, UnescapeMixedContent) {
	EXPECT_EQ(reindexer::unescapeString("Hello\\nWorld\\t!"), "Hello\nWorld\t!");
	EXPECT_EQ(reindexer::unescapeString("Path: C:\\\\Windows\\\\System"), "Path: C:\\Windows\\System");
	EXPECT_EQ(reindexer::unescapeString("Quote: \\\"Hello\\\""), "Quote: \"Hello\"");
}

TEST(StringToolsTest, UnescapeUnicodeSequences) {
	EXPECT_EQ(reindexer::unescapeString("\\u0041"), "A");	  // 'A'
	EXPECT_EQ(reindexer::unescapeString("\\u0061"), "a");	  // 'a'
	EXPECT_EQ(reindexer::unescapeString("\\u0030"), "0");	  // '0'
	EXPECT_EQ(reindexer::unescapeString("\\u0020"), " ");	  // space
	EXPECT_EQ(reindexer::unescapeString("\\u007F"), "\x7F");  // DEL

	EXPECT_EQ(reindexer::unescapeString("\\u0048\\u0065\\u006C\\u006C\\u006F"), "Hello");
}

TEST(StringToolsTest, UnescapeInvalidUnicode) {
	// Invalid hex digits
	EXPECT_EQ(reindexer::unescapeString("\\u00GG"), "\\u00GG");
	EXPECT_EQ(reindexer::unescapeString("\\u0G12"), "\\u0G12");
	EXPECT_EQ(reindexer::unescapeString("\\uG012"), "\\uG012");

	// Incomplete sequences
	EXPECT_EQ(reindexer::unescapeString("\\u"), "\\u");
	EXPECT_EQ(reindexer::unescapeString("\\u0"), "\\u0");
	EXPECT_EQ(reindexer::unescapeString("\\u00"), "\\u00");
	EXPECT_EQ(reindexer::unescapeString("\\u001"), "\\u001");
}

TEST(StringToolsTest, UnescapeUnknownEscapeSequences) {
	// Unknown escape sequences should be preserved as literal
	EXPECT_EQ(reindexer::unescapeString("\\x"), "\\x");
	EXPECT_EQ(reindexer::unescapeString("\\z"), "\\z");
	EXPECT_EQ(reindexer::unescapeString("\\'"), "\\'");
}

void TestRoundTrip(const std::string& original) {
	std::string escaped = reindexer::escapeString(original);
	std::string unescaped = reindexer::unescapeString(escaped);
	EXPECT_EQ(original, unescaped);
}

TEST(StringToolsTest, RoundTripBasic) {
	TestRoundTrip("hello world");
	TestRoundTrip("");
	TestRoundTrip("normal text");
}

TEST(StringToolsTest, RoundTripControlCharacters) {
	TestRoundTrip("\b\f\n\r\t");
	TestRoundTrip("text with\nnewlines\tand\ttabs");
	TestRoundTrip("backslash: \\");
	TestRoundTrip("quotes: \"\"");
}

TEST(StringToolsTest, RoundTripMixedContent) {
	TestRoundTrip("Line1\nLine2\tLine3\r\n");
	TestRoundTrip("Special: \\ \" \b \f");
	TestRoundTrip("File: C:\\Path\\To\\File.txt");
}

TEST(StringToolsTest, EscapeEndingWithBackslash) {
	EXPECT_EQ(reindexer::escapeString("end\\"), "end\\\\");
	EXPECT_EQ(reindexer::unescapeString("end\\\\"), "end\\");
}

TEST(StringToolsTest, UnescapeConsecutiveEscapes) {
	EXPECT_EQ(reindexer::unescapeString("\\\\n"), "\\n");
	EXPECT_EQ(reindexer::unescapeString("\\n\\t"), "\n\t");
	EXPECT_EQ(reindexer::unescapeString("\\\\u0041"), "\\u0041");
}

TEST(StringToolsTest, PerformanceTest_LongString) {
	std::string long_string(10000, 'a');
	long_string[5000] = '\n';
	long_string[6000] = '\t';
	long_string[7000] = '\\';

	std::string escaped = reindexer::escapeString(long_string);
	std::string unescaped = reindexer::unescapeString(escaped);
	EXPECT_EQ(long_string, unescaped);
}

TEST(StringToolsTest, EscapeSpecificValues) {
	EXPECT_EQ(reindexer::escapeString("value\\"), "value\\\\");
	EXPECT_EQ(reindexer::escapeString("value\""), "value\\\"");
	EXPECT_EQ(reindexer::escapeString("value\\\\"), "value\\\\\\\\");
	EXPECT_EQ(reindexer::escapeString("value\\\\\\"), "value\\\\\\\\\\\\");
	EXPECT_EQ(reindexer::escapeString("value\\\\\\\""), "value\\\\\\\\\\\\\\\"");
	EXPECT_EQ(reindexer::unescapeString("value\\\\"), "value\\");
	EXPECT_EQ(reindexer::unescapeString("value\\\""), "value\"");
	EXPECT_EQ(reindexer::unescapeString("value\\\\\\\\"), "value\\\\");
	EXPECT_EQ(reindexer::unescapeString("value\\\\\\\\\\\\"), "value\\\\\\");
	EXPECT_EQ(reindexer::unescapeString("value\\\\\\\\\\\\\\\""), "value\\\\\\\"");

	EXPECT_EQ(reindexer::escapeString("hello"), "hello");
	EXPECT_EQ(reindexer::escapeString("line1\nline2"), "line1\\nline2");
	EXPECT_EQ(reindexer::escapeString("tab\there"), "tab\\there");
	EXPECT_EQ(reindexer::unescapeString("hello"), "hello");
	EXPECT_EQ(reindexer::unescapeString("line1\\nline2"), "line1\nline2");
	EXPECT_EQ(reindexer::unescapeString("tab\\there"), "tab\there");
}
