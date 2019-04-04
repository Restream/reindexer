#include <regex>
#include "gtest/gtest.h"
#include "tools/customlocal.h"
#include "tools/string_regexp_functions.h"
#include "tools/stringstools.h"

namespace {
// using reindexer::operator""_sv;
// gcc 4.8 can't use using
constexpr reindexer::string_view operator"" _sv(const char* str, size_t len) noexcept { return reindexer::string_view(str, len); }

static const std::wstring symbols =
	L" 	,-_!abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZабвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ";

static std::string randString() {
	const size_t len = rand() % 100;
	std::string result;
	result.reserve(len + 1);
	while (result.size() < len) {
		const size_t f = rand() % symbols.size();
		result += reindexer::utf16_to_utf8({symbols[f]});
	}
	return result;
}

static std::string randLikePattern() {
	const size_t len = rand() % 100;
	size_t skipLen = 0;
	std::string result;
	result.reserve(len + 1);
	while (result.size() + skipLen < len) {
		if (rand() % 4 == 0) {
			skipLen += (rand() % (len - result.size() + 1));
			result += '%';
		} else {
			if (rand() % 4 == 0) {
				result += '_';
			} else {
				const size_t f = rand() % symbols.size();
				result += reindexer::utf16_to_utf8({symbols[f]});
			}
		}
	}
	return result;
}

static bool isLikePattern(const std::string& str, const std::string& pattern) {
	std::wstring wstr = reindexer::utf8_to_utf16(str);
	reindexer::ToLower(wstr);
	std::wstring wpattern = reindexer::utf8_to_utf16(reindexer::sqlLikePattern2ECMAScript(pattern));
	reindexer::ToLower(wpattern);
	return std::regex_match(wstr, std::wregex{wpattern});
}

}  //  namespace

TEST(StringFunctions, IsLikeSqlPattern) {
	srand(std::time(0));
	struct {
		int caseNumber;
		reindexer::string_view str;
		reindexer::string_view pattern;
		bool expected;
	} testCases[]{
		{0, ""_sv, ""_sv, true},
		{1, ""_sv, "%"_sv, true},
		{2, ""_sv, "%%"_sv, true},
		{3, ""_sv, "q"_sv, false},
		{4, "q"_sv, "q"_sv, true},
		{5, "q"_sv, "qq"_sv, false},
		{6, "qq"_sv, "q"_sv, false},
		{7, "qq"_sv, "qq"_sv, true},
		{8, "qq"_sv, "q_"_sv, true},
		{9, "qq"_sv, "q%"_sv, true},
		{10, "qq"_sv, "%q"_sv, true},
		{11, "qq"_sv, "qQ%"_sv, true},
		{12, "qq"_sv, "%Qq"_sv, true},
		{13, "qq"_sv, "Q%q"_sv, true},
		{14, "qq"_sv, "%"_sv, true},
		{15, "qq"_sv, "%%"_sv, true},
		{16, "qq"_sv, "%_%"_sv, true},
		{17, "qq"_sv, "%__%"_sv, true},
		{18, "qq"_sv, "%___%"_sv, false},
		{19, "qwerASDFфываЯЧСМ"_sv, "%_E_a%Ф_%аяЧсм"_sv, true},
		{20, "riend"_sv, "_%_e_%_%d"_sv, false},
		{21, "аБВ  Гдеж"_sv, "%%%аБв%%%гДе%%%"_sv, true},
	};
	for (const auto& testCase : testCases) {
		const bool result = matchLikePattern(testCase.str, testCase.pattern);
		EXPECT_EQ(result, testCase.expected) << "Test case number " << testCase.caseNumber;
	}

	for (int i = 0; i < 1000; ++i) {
		const std::string str = randString();
		std::string pattern = randLikePattern();
		bool match;
		try {
			match = isLikePattern(str, pattern);
		} catch (...) {
			continue;
		}
		EXPECT_EQ(reindexer::matchLikePattern(str, pattern), match) << "String: '" << str << "'\nPattern: '" << pattern << "'";

		pattern = reindexer::makeLikePattern(str);
		EXPECT_TRUE(reindexer::matchLikePattern(str, pattern)) << "String: '" << str << "'\nPattern: '" << pattern << "'";
	}
}
