#include <regex>
#include "gtest/gtest.h"
#include "tools/customlocal.h"
#include "tools/string_regexp_functions.h"
#include "tools/stringstools.h"

namespace {

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
	using namespace std::string_view_literals;
	srand(std::time(0));
	struct {
		int caseNumber;
		std::string_view str;
		std::string_view pattern;
		bool expected;
	} testCases[]{
		{0, ""sv, ""sv, true},
		{1, ""sv, "%"sv, true},
		{2, ""sv, "%%"sv, true},
		{3, ""sv, "q"sv, false},
		{4, "q"sv, "q"sv, true},
		{5, "q"sv, "qq"sv, false},
		{6, "qq"sv, "q"sv, false},
		{7, "qq"sv, "qq"sv, true},
		{8, "qq"sv, "q_"sv, true},
		{9, "qq"sv, "q%"sv, true},
		{10, "qq"sv, "%q"sv, true},
		{11, "qq"sv, "qQ%"sv, true},
		{12, "qq"sv, "%Qq"sv, true},
		{13, "qq"sv, "Q%q"sv, true},
		{14, "qq"sv, "%"sv, true},
		{15, "qq"sv, "%%"sv, true},
		{16, "qq"sv, "%_%"sv, true},
		{17, "qq"sv, "%__%"sv, true},
		{18, "qq"sv, "%___%"sv, false},
		{19, "qwerASDFфываЯЧСМ"sv, "%_E_a%Ф_%аяЧсм"sv, true},
		{20, "riend"sv, "_%_e_%_%d"sv, false},
		{21, "аБВ  Гдеж"sv, "%%%аБв%%%гДе%%%"sv, true},
	};
	for (const auto& testCase : testCases) {
		const bool result = reindexer::matchLikePattern(testCase.str, testCase.pattern);
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
