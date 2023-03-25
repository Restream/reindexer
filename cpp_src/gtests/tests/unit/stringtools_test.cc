#include <ostream>
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
			os << fmt::sprintf("{ str1: '%s', str2: '%s', expected comparison result: %s }", c.str1, c.str2,
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
