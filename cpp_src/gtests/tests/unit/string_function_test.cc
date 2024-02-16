#if defined(__GNUC__) && (__GNUC__ == 12) && defined(REINDEX_WITH_ASAN)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#include <regex>
#pragma GCC diagnostic pop
#else  // REINDEX_WITH_ASAN
#include <regex>
#endif	// REINDEX_WITH_ASAN

#include "gtest/gtest.h"
#include "reindexer_api.h"
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
	struct {  // NOLINT (*performance.Padding) Padding does not matter here
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

// test to check
// 1. equality of character length in bytes for uppercase and lowercase letters
// 2. character length equality after the substitution function 'check_for_replacement'

TEST(StringFunctions, ToLowerUTF8ByteLen) {
	for (wchar_t a = 0; a < UINT16_MAX; ++a) {
		auto utf8ByteSize = [](wchar_t a) {
			std::string symUtf8;
			std::wstring symIn;
			symIn += a;
			reindexer::utf16_to_utf8(symIn, symUtf8);
			return symUtf8.size();
		};
		ASSERT_EQ(utf8ByteSize(a), utf8ByteSize(reindexer::ToLower(a)));
		{
			wchar_t replaceChar = a;
			reindexer::check_for_replacement(replaceChar);
			if (replaceChar != a) {
				ASSERT_EQ(utf8ByteSize(a), utf8ByteSize(replaceChar));
			}
		}
		{
			uint32_t replaceChar = a;
			reindexer::check_for_replacement(replaceChar);
			if (replaceChar != uint32_t(a)) {
				ASSERT_EQ(utf8ByteSize(a), utf8ByteSize(replaceChar));
			}
		}
	}
}

// Make sure 'Like' operator does not work with FT indexes
TEST_F(ReindexerApi, LikeWithFullTextIndex) {
	// Define structure of the Namespace, where one of
	// the indexes is of type 'text' (Full text)
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"name", {"name"}, "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	// Insert 100 items to newly created Namespace
	std::vector<std::string> content;
	for (int i = 0; i < 100; ++i) {
		Item item = NewItem(default_namespace);
		content.emplace_back(RandString());
		item["id"] = i;
		item["name"] = content.back();
		Upsert(default_namespace, item);
	}

	// Make sure query with 'Like' operator to FT index leads to error
	QueryResults qr;
	err = rt.reindexer->Select(Query(default_namespace).Where("name", CondLike, "%" + content[rand() % content.size()]), qr);
	ASSERT_TRUE(!err.ok());
}
