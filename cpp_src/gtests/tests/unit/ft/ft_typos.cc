#include <gtest/gtest-param-test.h>
#include "ft_api.h"

class [[nodiscard]] FTTyposApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_typos_default_namespace"; }

	template <typename T>
	std::string DumpStrings(const T& container) {
		bool first = true;
		std::string res;
		res.append("[");
		for (auto& v : container) {
			if (first) {
				res.append(v);
				first = false;
			} else {
				res.append(",").append(v);
			}
		}
		res.append("]");
		return res;
	}
	void CheckResultsByField(const reindexer::QueryResults& res, const std::set<std::string>& expected, std::string_view fieldName,
							 std::string_view description) {
		std::set<std::string> resSet;
		for (auto& r : res) {
			auto word = r.GetItem(false)[fieldName].As<std::string>();
			EXPECT_TRUE(expected.find(word) != expected.end()) << description << ": word '" << word << "' was not expected in results";
			resSet.emplace(std::move(word));
		}
		for (auto& e : expected) {
			EXPECT_TRUE(resSet.find(e) != resSet.end()) << description << ": word '" << e << "' was expected in results, but was not found";
		}
		if (!::testing::Test::HasFailure()) {
			EXPECT_EQ(res.Count(), expected.size())
				<< description << "; expected(values): " << DumpStrings(expected) << "; got(IDs): " << res.ToLocalQr().Dump();
		}
	}
};

using namespace std::string_view_literals;

TEST_P(FTTyposApi, SelectWithTypos) {
	auto cfg = GetDefaultConfig();
	cfg.stopWords.clear();
	cfg.stemmers.clear();
	cfg.enableKbLayout = false;
	cfg.enableTranslit = false;
	const auto kDefaultMaxTypoDist = cfg.maxTypoDistance;

	cfg.maxTypos = 0;
	Init(cfg);
	Add("A");
	Add("AB");
	Add("ABC");
	Add("ABCD");
	Add("ABCDE");
	Add("ABCDEF");
	Add("ABCDEFG");
	Add("ABCDEFGH");
	// Only full match
	CheckAllPermutations("", {"A~"}, "", {{"!A!", ""}});
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}});
	CheckAllPermutations("", {"ABC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"ABCDEFGHI~"}, "", {});
	CheckAllPermutations("", {"XBCD~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {});

	cfg.maxTypos = 1;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	CheckAllPermutations("", {"ABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDEFGHI~"}, "", {{"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XBCD~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});

	cfg.maxTypos = 2;
	cfg.maxTypoDistance = -1;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one typo
	CheckAllPermutations("", {"ABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"BXCX~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!ABCD!", ""}});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});

	cfg.maxTypos = 2;
	cfg.maxTypoDistance = kDefaultMaxTypoDist;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one letter switch
	// Max typo distance is 0 (by default). Only the letter on the same position may be changed
	// Max letter permutation is 1 (by default). The same letter may be moved by 1
	CheckAllPermutations("", {"ABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {});
	CheckAllPermutations("", {"BXCX~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!ABCD!", ""}});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});

	cfg.maxTypos = 3;
	cfg.maxTypoDistance = -1;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one missing char in one word and two missing chars in another one
	// Or up to two typos
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {{"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BADC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"BACDFE~"}, "", {{"!ABCDE!", ""}});
	CheckAllPermutations("", {"XBCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"XBXD~"}, "", {});
	CheckAllPermutations("", {"AXXD~"}, "", {});
	CheckAllPermutations("", {"XBCX~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"XXCD~"}, "", {});
	CheckAllPermutations("", {"XXABX~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});
	CheckAllPermutations("", {"AXX~"}, "", {});

	cfg.maxTypos = 3;
	cfg.maxTypoDistance = kDefaultMaxTypoDist;
	SetFTConfig(cfg);
	// Full match
	// Or up to two missing chars in any word
	// Or one letter switch and one missing char in any word
	// Max typo distance is 0 (by default). Only the letter on the same position may be changed
	// Max letter permutation is 1 (by default). The same letter may be moved by 1
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"XBCDEF~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});

	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BADC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"BACDFE~"}, "", {{"!ABCDE!", ""}});
	CheckAllPermutations("", {"XBCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"XBXD~"}, "", {});
	CheckAllPermutations("", {"AXXD~"}, "", {});
	CheckAllPermutations("", {"XBCX~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"XXCD~"}, "", {});
	CheckAllPermutations("", {"XXABX~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});
	CheckAllPermutations("", {"AXX~"}, "", {});

	cfg.maxTypos = 4;
	cfg.maxTypoDistance = -1;
	SetFTConfig(cfg);
	// Full match
	// Or up to two missing chars in any of the both words
	// Or up to two typos
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BADC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"BACDFE~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BADCFE~"}, "", {});
	CheckAllPermutations("", {"XBCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XBXD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"AXXD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XBCX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XXABX~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});
	CheckAllPermutations("", {"AXX~"}, "", {});

	cfg.maxTypos = 4;
	cfg.maxTypoDistance = kDefaultMaxTypoDist;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one letter switch and one missing char in one of the words
	// Or two letters switch
	// Max typo distance is 0 (by default). Only the letter on the same position may be changed
	// Max letter permutation is 1 (by default). The same letter may be moved by 1
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BADC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"BACDFE~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BADCFE~"}, "", {});
	CheckAllPermutations("", {"XBCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XBXD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"AXXD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XBCX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XXABX~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});
	CheckAllPermutations("", {"AXX~"}, "", {});
}

TEST_P(FTTyposApi, TyposDistance) {
	// Check different max_typos_distance values with default max_typos and default max_symbol_permutation_distance (=1)
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("блачныйк"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct [[nodiscard]] Case {
		std::string description;
		int maxTypoDistance;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"wrong_letter_default_config", std::numeric_limits<int>::max(), "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_default_config", std::numeric_limits<int>::max(), "=облочный~", {"облачный"}},
		{"extra_letter_default_config", std::numeric_limits<int>::max(), "=облачкный~", {"облачный"}},
		{"missing_letter_default_config", std::numeric_limits<int>::max(), "=обланый~", {"облачный"}},

		{"wrong_letter_0_typo_distance", 0, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_0_typo_distance", 0, "=облочный~", {"облачный"}},
		{"extra_letter_0_typo_distance", 0, "=облачкный~", {"облачный"}},
		{"missing_letter_0_typo_distance", 0, "=обланый~", {"облачный"}},

		{"wrong_letter_any_typo_distance", -1, "=аблачный~", {"облачный", "табачный", "блачныйк"}},
		{"wrong_letter_in_the_middle_any_typo_distance", -1, "=облочный~", {"облачный"}},
		{"extra_letter_any_typo_distance", -1, "=облачкный~", {"облачный"}},
		{"missing_letter_any_typo_distance", -1, "=обланый~", {"облачный"}},

		{"wrong_letter_2_typo_distance", 2, "=аблачный~", {"облачный", "табачный"}},
		{"wrong_letter_in_the_middle_2_typo_distance", -1, "=облочный~", {"облачный"}},
		{"extra_letter_2_typo_distance", 2, "=облачкный~", {"облачный"}},
		{"missing_letter_2_typo_distance", 2, "=обланый~", {"облачный"}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		if (c.maxTypoDistance != std::numeric_limits<int>::max()) {
			cfg.maxTypoDistance = c.maxTypoDistance;
		}
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		auto res = rt.Select(q);
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTTyposApi, TyposDistanceWithMaxTypos) {
	// Check basic max_typos_distance funtionality with different max_typos values.
	// Letters permutations are not allowed (max_symbol_permutation_distance = 0)
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("блачныйк"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct [[nodiscard]] Case {
		std::string description;
		int maxTypos;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"full_match_0_max_typo", 0, "=облачный~", {"облачный"}},
		{"wrong_letter_0_max_typo", 0, "=аблачный~", {}},
		{"wrong_letter_in_the_middle_0_max_typo", 0, "=облочный~", {}},
		{"2_wrong_letters_0_max_typo_1", 0, "=аблочный~", {}},
		{"2_wrong_letters_0_max_typo_2", 0, "=оплачнык~", {}},
		{"extra_letter_0_max_typo", 0, "=облачкный~", {}},
		{"missing_letter_0_max_typo", 0, "=обланый~", {}},
		{"2_extra_letters_0_max_typo", 0, "=поблачкный~", {}},
		{"2_missing_letters_0_max_typo", 0, "=обланы~", {}},

		{"full_match_1_max_typo", 1, "=облачный~", {"облачный"}},
		{"wrong_letter_1_max_typo", 1, "=аблачный~", {}},
		{"wrong_letter_in_the_middle_1_max_typo", 1, "=облочный~", {}},
		{"2_wrong_letters_1_max_typo_1", 1, "=аблочный~", {}},
		{"2_wrong_letters_1_max_typo_2", 1, "=оплачнык~", {}},
		{"extra_letter_1_max_typo", 1, "=облачкный~", {"облачный"}},
		{"missing_letter_1_max_typo", 1, "=обланый~", {"облачный"}},
		{"2_extra_letters_1_max_typo", 1, "=поблачкный~", {}},
		{"2_missing_letters_1_max_typo", 1, "=обланы~", {}},

		{"full_match_2_max_typo", 2, "=облачный~", {"облачный"}},
		{"wrong_letter_2_max_typo", 2, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_2_max_typo", 2, "=облочный~", {"облачный"}},
		{"2_wrong_letters_2_max_typo_1", 2, "=аблочный~", {}},
		{"2_wrong_letters_2_max_typo_2", 2, "=оплачнык~", {}},
		{"extra_letter_2_max_typo", 2, "=облачкный~", {"облачный"}},
		{"missing_letter_2_max_typo", 2, "=обланый~", {"облачный"}},
		{"2_extra_letters_2_max_typo", 2, "=поблачкный~", {}},
		{"2_missing_letters_2_max_typo", 2, "=обланы~", {}},

		{"full_match_3_max_typo", 3, "=облачный~", {"облачный"}},
		{"wrong_letter_3_max_typo", 3, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_3_max_typo", 3, "=облочный~", {"облачный"}},
		{"2_wrong_letters_3_max_typo_1", 3, "=аблочный~", {}},
		{"2_wrong_letters_3_max_typo_2", 3, "=оплачнык~", {}},
		{"extra_letter_3_max_typo", 3, "=облачкный~", {"облачный"}},
		{"missing_letter_3_max_typo", 3, "=обланый~", {"облачный"}},
		{"2_extra_letters_3_max_typo", 3, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_3_max_typo", 3, "=обланы~", {"облачный"}},
		{"3_extra_letters_3_max_typo", 3, "=поблачкныйк~", {}},
		{"1_wrong_1_extra_letter_3_max_typo", 3, "=облочкный~", {"облачный"}},
		{"1_wrong_1_missing_letter_3_max_typo", 3, "=облоный~", {"облачный"}},
		{"1_letter_permutation_3_max_typo", 3, "=болачный~", {}},
		{"2_letters_permutation_3_max_typo", 3, "=болачынй~", {}},

		{"full_match_4_max_typo", 4, "=облачный~", {"облачный", "отличный"}},
		{"wrong_letter_4_max_typo", 4, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_4_max_typo", 4, "=облочный~", {"облачный", "отличный"}},
		{"2_wrong_letters_4_max_typo_1", 4, "=аблочный~", {"облачный"}},
		{"2_wrong_letters_4_max_typo_2", 4, "=оплачнык~", {"облачный"}},
		{"extra_letter_4_max_typo", 4, "=облачкный~", {"облачный"}},
		{"missing_letter_4_max_typo", 4, "=обланый~", {"облачный"}},
		{"2_extra_letters_4_max_typo", 4, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_4_max_typo", 4, "=обланы~", {"облачный"}},
		{"3_extra_letters_4_max_typo", 4, "=поблачкныйк~", {}},
		{"3_missing_letters_4_max_typo", 4, "=обаны~", {}},
		{"1_wrong_1_extra_letter_4_max_typo", 4, "=облочкный~", {"облачный"}},
		{"1_wrong_1_missing_letter_4_max_typo", 4, "=облоный~", {"облачный"}},
		{"1_letter_permutation_4_max_typo", 4, "=болачный~", {"облачный"}},
		{"2_letters_permutation_4_max_typo", 4, "=болачынй~", {}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		EXPECT_EQ(cfg.maxTypoDistance, 0) << "This test expects default max_typo_distance == 0";
		cfg.maxSymbolPermutationDistance = 0;
		cfg.maxTypos = c.maxTypos;
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		auto res = rt.Select(q);
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTTyposApi, LettersPermutationDistance) {
	// Check different max_symbol_permutation_distance values with default max_typos and default max_typos_distance (=0)
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct [[nodiscard]] Case {
		std::string description;
		int maxLettPermDist;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"first_letter_1_move_default_config", std::numeric_limits<int>::max(), "=болачный~", {"облачный"}},
		{"first_letter_1_move_and_switch_default_config", std::numeric_limits<int>::max(), "=волачный~", {}},
		{"first_letter_2_move_default_config", std::numeric_limits<int>::max(), "=блоачный~", {}},
		{"first_letter_3_move_default_config", std::numeric_limits<int>::max(), "=блаочный~", {}},
		{"mid_letter_1_move_default_config", std::numeric_limits<int>::max(), "=обалчный~", {"облачный"}},
		{"mid_letter_1_move_and_switch_default_config", std::numeric_limits<int>::max(), "=обакчный~", {}},
		{"mid_letter_2_move_default_config", std::numeric_limits<int>::max(), "=обачлный~", {}},
		{"mid_letter_3_move_default_config", std::numeric_limits<int>::max(), "=обачнлый~", {}},

		{"first_letter_1_move_0_lett_perm", 0, "=болачный~", {}},
		{"first_letter_2_move_0_lett_perm", 0, "=блоачный~", {}},
		{"first_letter_3_move_0_lett_perm", 0, "=блаочный~", {}},
		{"mid_letter_1_move_0_lett_perm", 0, "=обалчный~", {}},
		{"mid_letter_2_move_0_lett_perm", 0, "=обачлный~", {}},
		{"mid_letter_3_move_0_lett_perm", 0, "=обачнлый~", {}},

		{"first_letter_1_move_1_lett_perm", 1, "=болачный~", {"облачный"}},
		{"first_letter_2_move_1_lett_perm", 1, "=блоачный~", {}},
		{"first_letter_3_move_1_lett_perm", 1, "=блаочный~", {}},
		{"mid_letter_1_move_1_lett_perm", 1, "=обалчный~", {"облачный"}},
		{"mid_letter_2_move_1_lett_perm", 1, "=обачлный~", {}},
		{"mid_letter_3_move_1_lett_perm", 1, "=обачнлый~", {}},

		{"first_letter_1_move_2_lett_perm", 2, "=болачный~", {"облачный"}},
		{"first_letter_1_move_and_switch_2_lett_perm", 2, "=бклачный~", {}},
		{"first_letter_2_move_2_lett_perm", 2, "=блоачный~", {"облачный"}},
		{"first_letter_3_move_2_lett_perm", 2, "=блаочный~", {}},
		{"mid_letter_1_move_2_lett_perm", 2, "=обалчный~", {"облачный"}},
		{"mid_letter_1_move_and_switch_2_lett_perm", 2, "=обапчный~", {}},
		{"mid_letter_2_move_2_lett_perm", 2, "=обачлный~", {"облачный"}},
		{"mid_letter_3_move_2_lett_perm", 2, "=обачнлый~", {}},

		{"first_letter_1_move_3_lett_perm", 3, "=болачный~", {"облачный"}},
		{"first_letter_2_move_3_lett_perm", 3, "=блоачный~", {"облачный"}},
		{"first_letter_3_move_3_lett_perm", 3, "=блаочный~", {"облачный"}},
		{"mid_letter_switch_3_lett_perm", 3, "=обалчный~", {"облачный"}},
		{"mid_letter_2_move_3_lett_perm", 3, "=обачлный~", {"облачный"}},
		{"mid_letter_3_move_3_lett_perm", 3, "=обачнлый~", {"облачный"}},

		{"first_letter_1_move_any_lett_perm", -1, "=болачный~", {"облачный"}},
		{"first_letter_1_move_and_switch_any_lett_perm", -1, "=бклачный~", {}},
		{"first_letter_2_move_any_lett_perm", -1, "=блоачный~", {"облачный"}},
		{"first_letter_3_move_any_lett_perm", -1, "=блаочный~", {"облачный"}},
		{"mid_letter_1_move_any_lett_perm", -1, "=обалчный~", {"облачный"}},
		{"mid_letter_1_move_and_switch_any_lett_perm", -1, "=обапчный~", {}},
		{"mid_letter_2_move_any_lett_perm", -1, "=обачлный~", {"облачный"}},
		{"mid_letter_3_move_any_lett_perm", -1, "=обачнлый~", {"облачный"}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		if (c.maxLettPermDist != std::numeric_limits<int>::max()) {
			cfg.maxSymbolPermutationDistance = c.maxLettPermDist;
		}
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		auto res = rt.Select(q);
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTTyposApi, LettersPermutationDistanceWithMaxTypos) {
	// Check basic max_symbol_permutation_distance funtionality with different max_typos values.
	// max_typo_distance is 0
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("блачныйк"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct [[nodiscard]] Case {
		std::string description;
		int maxTypos;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"full_match_0_max_typo", 0, "=облачный~", {"облачный"}},
		{"wrong_letter_0_max_typo", 0, "=аблачный~", {}},
		{"extra_letter_0_max_typo", 0, "=облачкный~", {}},
		{"missing_letter_0_max_typo", 0, "=обланый~", {}},
		{"2_extra_letters_0_max_typo", 0, "=поблачкный~", {}},
		{"2_missing_letters_0_max_typo", 0, "=обланы~", {}},
		{"1_letter_permutation_0_max_typo", 0, "=болачный~", {}},
		{"1_letter_permutation_and_switch_0_max_typo", 0, "=долачный~", {}},
		{"1_far_letter_permutation_0_max_typo", 0, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_0_max_typo", 0, "=балчный~", {}},
		{"1_permutation_and_2_missing_letters_0_max_typo", 0, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_0_max_typo", 0, "=болачныйк~", {}},
		{"1_permutation_and_2_extra_letters_0_max_typo", 0, "=болачныйкк~", {}},
		{"2_letters_permutation_0_max_typo", 0, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_0_max_typo", 0, "=болачынйк~", {}},
		{"1_permutation_and_1_wrong_letter_0_max_typo_1", 0, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_0_max_typo_2", 0, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_0_max_typo", 0, "=блоочный~", {}},

		{"full_match_1_max_typo", 1, "=облачный~", {"облачный"}},
		{"wrong_letter_1_max_typo", 1, "=аблачный~", {}},
		{"extra_letter_1_max_typo", 1, "=облачкный~", {"облачный"}},
		{"missing_letter_1_max_typo", 1, "=обланый~", {"облачный"}},
		{"2_extra_letters_1_max_typo", 1, "=поблачкный~", {}},
		{"2_missing_letters_1_max_typo", 1, "=обланы~", {}},
		{"1_letter_permutation_1_max_typo", 1, "=болачный~", {}},
		{"1_letter_permutation_and_switch_1_max_typo", 1, "=долачный~", {}},
		{"1_far_letter_permutation_1_max_typo", 1, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_1_max_typo", 1, "=балчный~", {}},
		{"1_permutation_and_2_missing_letters_1_max_typo", 1, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_1_max_typo", 1, "=болачныйк~", {"блачныйк"}},
		{"1_permutation_and_2_extra_letters_1_max_typo", 1, "=болачныйкк~", {}},
		{"2_letters_permutation_1_max_typo", 1, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_1_max_typo", 1, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_1_max_typo_1", 1, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_1_max_typo_2", 1, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_1_max_typo", 1, "=блоочный~", {}},

		{"full_match_2_max_typo", 2, "=облачный~", {"облачный"}},
		{"wrong_letter_2_max_typo", 2, "=аблачный~", {"облачный"}},
		{"extra_letter_2_max_typo", 2, "=облачкный~", {"облачный"}},
		{"missing_letter_2_max_typo", 2, "=обланый~", {"облачный"}},
		{"2_extra_letters_2_max_typo", 2, "=поблачкный~", {}},
		{"2_missing_letters_2_max_typo", 2, "=обланы~", {}},
		{"1_letter_permutation_2_max_typo", 2, "=болачный~", {"облачный"}},
		{"1_letter_permutation_and_switch_2_max_typo", 2, "=долачный~", {}},
		{"1_far_letter_permutation_2_max_typo", 2, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_2_max_typo", 2, "=балчный~", {}},
		{"1_permutation_and_2_missing_letters_2_max_typo", 2, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_2_max_typo", 2, "=болачныйк~", {"блачныйк"}},
		{"1_permutation_and_2_extra_letters_2_max_typo", 2, "=болачныйкк~", {}},
		{"2_letters_permutation_2_max_typo", 2, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_2_max_typo", 2, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_2_max_typo_1", 2, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_2_max_typo_2", 2, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_2_max_typo", 2, "=блоочный~", {}},

		{"full_match_3_max_typo", 3, "=облачный~", {"облачный"}},
		{"wrong_letter_3_max_typo", 3, "=аблачный~", {"облачный"}},
		{"extra_letter_3_max_typo", 3, "=облачкный~", {"облачный"}},
		{"missing_letter_3_max_typo", 3, "=обланый~", {"облачный"}},
		{"2_extra_letters_3_max_typo", 3, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_3_max_typo", 3, "=обланы~", {"облачный"}},
		{"1_letter_permutation_3_max_typo", 3, "=болачный~", {"облачный"}},
		{"1_letter_permutation_and_switch_3_max_typo", 3, "=долачный~", {}},
		{"1_far_letter_permutation_3_max_typo", 3, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_3_max_typo", 3, "=балчный~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_missing_letters_3_max_typo", 3, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_3_max_typo", 3, "=болачныйт~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_extra_letters_3_max_typo", 3, "=болачныйтт~", {}},
		{"2_letters_permutation_3_max_typo", 3, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_3_max_typo", 3, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_3_max_typo_1", 3, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_3_max_typo_2", 3, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_3_max_typo", 3, "=блоочный~", {}},

		{"full_match_4_max_typo", 4, "=облачный~", {"облачный", "отличный"}},
		{"wrong_letter_4_max_typo", 4, "=аблачный~", {"облачный"}},
		{"extra_letter_4_max_typo", 4, "=облачкный~", {"облачный"}},
		{"missing_letter_4_max_typo", 4, "=обланый~", {"облачный"}},
		{"2_extra_letters_4_max_typo", 4, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_4_max_typo", 4, "=обланы~", {"облачный"}},
		{"1_letter_permutation_4_max_typo", 4, "=болачный~", {"облачный"}},
		{"1_letter_permutation_and_switch_4_max_typo", 4, "=долачный~", {"облачный"}},
		{"1_far_letter_permutation_4_max_typo_1", 4, "=блоачный~", {"облачный"}},  // Will be handled as double permutation
		{"1_far_letter_permutation_4_max_typo_2", 4, "=блаочный~", {}},
		{"1_permutation_and_1_missing_letter_4_max_typo", 4, "=балчный~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_missing_letters_4_max_typo", 4, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_4_max_typo", 4, "=болачныйт~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_extra_letters_4_max_typo", 4, "=болачныйтт~", {}},
		{"2_letters_permutation_4_max_typo", 4, "=болачынй~", {"облачный"}},
		{"2_permutations_and_1_extra_letter_4_max_typo", 4, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_4_max_typo_1", 4, "=болочный~", {"облачный"}},
		{"1_permutation_and_1_wrong_letter_4_max_typo_2", 4, "=облончый~", {"облачный"}},
		{"1_far_permutation_and_1_wrong_letter_4_max_typo", 4, "=блоочный~", {}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		EXPECT_EQ(cfg.maxSymbolPermutationDistance, 1) << "This test expects default max_symbol_permutation_distance == 1";
		cfg.maxTypoDistance = 0;
		cfg.maxTypos = c.maxTypos;
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		auto res = rt.Select(q);
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTTyposApi, TyposMissingAndExtraLetters) {
	// Check different max_typos, max_extra_letters and max_missing_letters combinations.
	// max_typos must always override max_extra_letters and max_missing_letters.
	// max_missing_letters and max_extra_letters must restrict corresponding letters' counts
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct [[nodiscard]] Case {
		std::string description;
		int maxTypos;
		int maxExtraLetters;
		int maxMissingLetter;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"full_match_0_max_typos_0_max_extras_0_max_missing", 0, 0, 0, "=облачный~", {"облачный"}},
		{"1_missing_0_max_typos_1_max_extras_1_max_missing", 0, 1, 1, "=облчный~", {}},
		{"1_extra_0_max_typos_1_max_extras_1_max_missing", 0, 1, 1, "=облкачный~", {}},

		{"full_match_1_max_typos_0_max_extras_0_max_missing", 0, 0, 0, "=облачный~", {"облачный"}},
		{"1_missing_1_max_typos_0_max_extras_1_max_missing", 1, 0, 1, "=облчный~", {"облачный"}},
		{"1_missing_1_max_typos_1_max_extras_0_max_missing", 1, 1, 0, "=облчный~", {}},
		{"1_missing_1_max_typos_1_max_extras_1_max_missing", 1, 1, 1, "=облчный~", {"облачный"}},
		{"2_missing_1_max_typos_0_max_extras_2_max_missing", 1, 0, 2, "=облчны~", {}},
		{"1_extra_1_max_typos_0_max_extras_1_max_missing", 1, 0, 1, "=облкачный~", {}},
		{"1_extra_1_max_typos_1_max_extras_0_max_missing", 1, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_1_max_typos_1_max_extras_1_max_missing", 1, 1, 1, "=облкачный~", {"облачный"}},
		{"2_extra_1_max_typos_2_max_extras_0_max_missing", 1, 2, 0, "=облкачныйп~", {}},

		{"full_match_2_max_typos_1_max_extras_1_max_missing", 2, 1, 1, "=облачный~", {"облачный"}},
		{"1_missing_2_max_typos_0_max_extras_1_max_missing", 2, 0, 1, "=облчный~", {"облачный"}},
		{"1_missing_2_max_typos_1_max_extras_0_max_missing", 2, 1, 0, "=облчный~", {}},
		{"1_missing_2_max_typos_1_max_extras_1_max_missing", 2, 1, 1, "=облчный~", {"облачный"}},
		{"2_missing_2_max_typos_0_max_extras_2_max_missing", 2, 0, 2, "=облчны~", {}},
		{"1_missing_2_max_typos_0_max_extras_any_max_missing", 2, 0, -1, "=облчный~", {"облачный"}},
		{"2_missing_2_max_typos_0_max_extras_any_max_missing", 2, 0, -1, "=облчны~", {}},
		{"1_extra_2_max_typos_0_max_extras_1_max_missing", 2, 0, 1, "=облкачный~", {}},
		{"1_extra_2_max_typos_1_max_extras_0_max_missing", 2, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_2_max_typos_1_max_extras_1_max_missing", 2, 1, 1, "=облкачный~", {"облачный"}},
		{"2_extra_2_max_typos_2_max_extras_0_max_missing", 2, 2, 0, "=облкачныйп~", {}},
		{"1_extra_2_max_typos_any_max_extras_0_max_missing", 2, -1, 0, "=облкачный~", {"облачный"}},
		{"2_extra_2_max_typos_any_max_extras_0_max_missing", 2, -1, 0, "=облкачныйп~", {}},

		{"full_match_3_max_typos_2_max_extras_2_max_missing", 3, 2, 2, "=облачный~", {"облачный"}},
		{"1_missing_3_max_typos_0_max_extras_1_max_missing", 3, 0, 1, "=облчный~", {"облачный", "отличный"}},
		{"1_missing_3_max_typos_1_max_extras_0_max_missing", 3, 1, 0, "=облчный~", {}},
		{"1_missing_3_max_typos_1_max_extras_1_max_missing", 3, 1, 1, "=облчный~", {"облачный", "отличный"}},
		{"2_missing_3_max_typos_0_max_extras_1_max_missing", 3, 0, 1, "=облчны~", {}},
		{"2_missing_3_max_typos_0_max_extras_2_max_missing", 3, 0, 2, "=облчны~", {"облачный"}},
		{"2_missing_3_max_typos_0_max_extras_any_max_missing", 3, 0, -1, "=облчны~", {"облачный"}},
		{"3_missing_3_max_typos_0_max_extras_any_max_missing", 3, 0, -1, "=облчн~", {}},
		{"1_extra_3_max_typos_0_max_extras_1_max_missing", 3, 0, 1, "=облкачный~", {}},
		{"1_extra_3_max_typos_1_max_extras_0_max_missing", 3, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_3_max_typos_1_max_extras_1_max_missing", 3, 1, 1, "=облкачный~", {"облачный"}},
		{"2_extra_3_max_typos_1_max_extras_0_max_missing", 3, 1, 0, "=облкачныйп~", {}},
		{"2_extra_3_max_typos_2_max_extras_0_max_missing", 3, 2, 0, "=облкачныйп~", {"облачный"}},
		{"2_extra_3_max_typos_any_max_extras_0_max_missing", 3, -1, 0, "=облкачныйп~", {"облачный"}},
		{"3_extra_3_max_typos_any_max_extras_0_max_missing", 3, -1, 0, "=оболкачныйп~", {}},

		{"full_match_4_max_typos_2_max_extras_2_max_missing", 4, 2, 2, "=облачный~", {"облачный", "отличный"}},
		{"1_missing_4_max_typos_0_max_extras_1_max_missing", 4, 0, 1, "=облчный~", {"облачный", "отличный"}},
		{"1_missing_4_max_typos_1_max_extras_0_max_missing", 4, 1, 0, "=облчный~", {}},
		{"1_missing_4_max_typos_1_max_extras_1_max_missing", 4, 1, 1, "=облчный~", {"облачный", "отличный"}},
		{"2_missing_4_max_typos_0_max_extras_1_max_missing", 4, 0, 1, "=облчны~", {}},
		{"2_missing_4_max_typos_0_max_extras_2_max_missing", 4, 0, 2, "=облчны~", {"облачный"}},
		{"2_missing_4_max_typos_0_max_extras_any_max_missing", 4, 0, -1, "=облчны~", {"облачный"}},
		{"3_missing_4_max_typos_0_max_extras_any_max_missing", 4, 0, -1, "=облчн~", {}},
		{"1_extra_4_max_typos_0_max_extras_1_max_missing", 4, 0, 1, "=облкачный~", {}},
		{"1_extra_4_max_typos_1_max_extras_0_max_missing", 4, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_4_max_typos_1_max_extras_1_max_missing", 4, 1, 0, "=облкачный~", {"облачный"}},
		{"2_extra_4_max_typos_1_max_extras_0_max_missing", 4, 1, 0, "=облкачныйп~", {}},
		{"2_extra_4_max_typos_2_max_extras_0_max_missing", 4, 2, 0, "=облкачныйп~", {"облачный"}},
		{"2_extra_4_max_typos_any_max_extras_0_max_missing", 4, -1, 0, "=облкачныйп~", {"облачный"}},
		{"3_extra_4_max_typos_any_max_extras_0_max_missing", 4, -1, 0, "=оболкачныйп~", {}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		cfg.maxTypos = c.maxTypos;
		cfg.maxExtraLetters = c.maxExtraLetters;
		cfg.maxMissingLetters = c.maxMissingLetter;
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		auto res = rt.Select(q);
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

INSTANTIATE_TEST_SUITE_P(, FTTyposApi,
						 ::testing::Values(reindexer::FtFastConfig::Optimization::Memory, reindexer::FtFastConfig::Optimization::CPU),
						 [](const auto& info) {
							 switch (info.param) {
								 case reindexer::FtFastConfig::Optimization::Memory:
									 return "OptimizationByMemory";
								 case reindexer::FtFastConfig::Optimization::CPU:
									 return "OptimizationByCPU";
								 default:
									 assert(false);
									 std::abort();
							 }
						 });
