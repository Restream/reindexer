#include <gtest/gtest-param-test.h>
#include <fstream>
#include <unordered_map>
#include "core/cjson/jsonbuilder.h"
#include "core/ft/ft_fast/frisosplitter.h"
#include "core/ft/limits.h"
#include "ft_api.h"
#include "gtests/tests/tests_data.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "yaml-cpp/yaml.h"

using namespace std::string_view_literals;
using reindexer::fast_hash_map;
using reindexer::Query;

class FTGenericApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_generic_default_namespace"; }

	void CreateAndFillSimpleNs(const std::string& ns, int from, int to, fast_hash_map<int, std::string>* outItems) {
		assertrx(from <= to);
		std::vector<std::string> items;
		items.reserve(to - from);
		auto err = rt.reindexer->OpenNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();
		rt.DefineNamespaceDataset(
			ns, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"data", "hash", "string", IndexOpts(), 0}});
		reindexer::WrSerializer ser;
		for (int i = from; i < to; ++i) {
			ser.Reset();
			reindexer::JsonBuilder jb(ser);
			jb.Put("id", i);
			jb.Put("data", rt.RandString());
			jb.End();
			auto item = rt.NewItem(ns);
			if (outItems) {
				(*outItems)[i] = ser.Slice();
			}
			err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			rt.Upsert(ns, item);
		}
	}
	template <typename T>
	std::string PrintArray(const T& data, const std::string& name) {
		std::stringstream stream;
		stream << name << " :";
		for (const auto& v : data) {
			stream << v << ", ";
		}
		stream << std::endl;
		return stream.str();
	}
};

TEST_P(FTGenericApi, CompositeSelect) {
	Init(GetDefaultConfig(), NS1 | NS2);
	Add("An entity is something|"sv, "| that in exists entity as itself"sv, NS1 | NS2);
	Add("In law, a legal entity is|"sv, "|an entity that is capable of something bearing legal rights"sv, NS1 | NS2);
	Add("In politics, entity is used as|"sv, "| term for entity territorial divisions of some countries"sv, NS1 | NS2);

	for (const auto& query : CreateAllPermutatedQueries("", {"*entity", "somethin*"}, "")) {
		auto res = SimpleCompositeSelect(query);
		std::unordered_set<std::string_view> data{"An <b>entity</b> is <b>something</b>|"sv,
												  "| that in exists <b>entity</b> as itself"sv,
												  "An <b>entity</b> is <b>something</b>|d"sv,
												  "| that in exists entity as itself"sv,
												  "In law, a legal <b>entity</b> is|"sv,
												  "|an <b>entity</b> that is capable of <b>something</b> bearing legal rights"sv,
												  "al <b>entity</b> id"sv,
												  "|an entity that is capable of something bearing legal rights"sv,
												  "In politics, <b>entity</b> is used as|"sv,
												  "| term for <b>entity</b> territorial divisions of some countries"sv,
												  "s, <b>entity</b> id"sv,
												  "| term for entity territorial divisions of some countries"sv};

		rt.PrintQueryResults("nm1", res);
		for (auto it : res) {
			auto ritem(it.GetItem(false));
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				auto field = ritem[idx].Name();
				if (field == "id") {
					continue;
				}
				auto it = data.find(ritem[field].As<std::string>());
				ASSERT_TRUE(it != data.end()) << ritem[field].As<std::string>();
				data.erase(it);
			}
		}
		EXPECT_TRUE(data.empty());
	}
}

TEST_P(FTGenericApi, CompositeSelectWithFields) {
	Init(GetDefaultConfig(), NS1 | NS2);
	AddInBothFields("An entity is something|"sv, "| that in exists entity as itself"sv, NS1 | NS2);
	AddInBothFields("In law, a legal entity is|"sv, "|an entity that is capable of something bearing legal rights"sv, NS1 | NS2);
	AddInBothFields("In politics, entity is used as|"sv, "| term for entity territorial divisions of some countries"sv, NS1 | NS2);

	for (const auto& query : CreateAllPermutatedQueries("", {"*entity", "somethin*"}, "")) {
		for (const char* field : {"ft1", "ft2"}) {
			auto res = CompositeSelectField(field, query);
			std::unordered_set<std::string_view> data{"An <b>entity</b> is <b>something</b>|"sv,
													  "An <b>entity</b> is <b>something</b>|d"sv,
													  "| that in exists <b>entity</b> as itself"sv,
													  "In law, a legal <b>entity</b> is|"sv,
													  "|an <b>entity</b> that is capable of <b>something</b> bearing legal rights"sv,
													  "an <b>entity</b> tdof <b>something</b> bd"sv,
													  "al <b>entity</b> id"sv,
													  "In politics, <b>entity</b> is used as|"sv,
													  "| term for <b>entity</b> territorial divisions of some countries"sv,
													  "ts <b>entity</b> ad"sv,
													  "s, <b>entity</b> id"sv,
													  "or <b>entity</b> td"sv};

			rt.PrintQueryResults("nm1", res);
			for (auto it : res) {
				auto ritem(it.GetItem(false));
				for (auto idx = 1; idx < ritem.NumFields(); idx++) {
					auto curField = ritem[idx].Name();
					if (curField != field) {
						continue;
					}
					auto it = data.find(ritem[curField].As<std::string>());
					ASSERT_TRUE(it != data.end());
					data.erase(it);
				}
			}
			EXPECT_TRUE(data.empty());
		}
	}
}

TEST_P(FTGenericApi, MergeWithSameNSAndSelectFunctions) {
	Init(GetDefaultConfig());
	AddInBothFields("An entity is something|"sv, "| that in exists entity as itself"sv);
	AddInBothFields("In law, a legal entity is|"sv, "|an entity that is capable of something bearing legal rights"sv);
	AddInBothFields("In politics, entity is used as|"sv, "| term for entity territorial divisions of some countries"sv);

	for (const auto& query : CreateAllPermutatedQueries("", {"*entity", "somethin*"}, "")) {
		for (const auto& field : {std::string("ft1"), std::string("ft2")}) {
			auto dsl = std::string("@").append(field).append(" ").append(query);
			auto qr{reindexer::Query("nm1").Where("ft3", CondEq, dsl)};
			reindexer::QueryResults res;
			auto mqr{reindexer::Query("nm1").Where("ft3", CondEq, std::move(dsl))};
			mqr.AddFunction(field + " = snippet(<xxx>,\"\"</xf>,3,2,,d)");

			qr.Merge(std::move(mqr));
			qr.AddFunction(field + " = highlight(<b>,</b>)");
			auto err = rt.reindexer->Select(qr, res);
			EXPECT_TRUE(err.ok()) << err.what();

			std::unordered_set<std::string_view> data{"An <b>entity</b> is <b>something</b>|"sv,
													  "An <xxx>entity</xf> is <xxx>something</xf>|d"sv,
													  "| that in exists <b>entity</b> as itself"sv,
													  "In law, a legal <b>entity</b> is|"sv,
													  "|an <b>entity</b> that is capable of <b>something</b> bearing legal rights"sv,
													  "an <xxx>entity</xf> tdof <xxx>something</xf> bd"sv,
													  "al <xxx>entity</xf> id"sv,
													  "In politics, <b>entity</b> is used as|"sv,
													  "| term for <b>entity</b> territorial divisions of some countries"sv,
													  "ts <xxx>entity</xf> ad"sv,
													  "s, <xxx>entity</xf> id"sv,
													  "or <xxx>entity</xf> td"sv};

			rt.PrintQueryResults("nm1", res);
			for (auto it : res) {
				auto ritem(it.GetItem(false));
				for (auto idx = 1; idx < ritem.NumFields(); idx++) {
					auto curField = ritem[idx].Name();
					if (curField != field) {
						continue;
					}
					auto it = data.find(ritem[curField].As<std::string>());
					ASSERT_TRUE(it != data.end());
					data.erase(it);
				}
			}
			EXPECT_TRUE(data.empty());
		}
	}
}

TEST_P(FTGenericApi, SelectWithPlus) {
	Init(GetDefaultConfig());

	Add("added three words"sv);
	Add("added something else"sv);

	CheckAllPermutations("", {"+added"}, "", {{"!added! something else", ""}, {"!added! three words", ""}});
}

TEST_P(FTGenericApi, SelectWithPlusWithSingleAlternative) {
	auto cfg = GetDefaultConfig();
	cfg.enableKbLayout = false;
	cfg.enableTranslit = false;
	Init(cfg);

	Add("мониторы"sv);

	// FT search by single mandatory word with single alternative
	CheckAllPermutations("", {"+монитор*"}, "", {{"!мониторы!", ""}});
}

TEST_P(FTGenericApi, SelectWithMinus) {
	Init(GetDefaultConfig());

	Add("including me, excluding you"sv);
	Add("including all of them"sv);

	CheckAllPermutations("", {"+including", "-excluding"}, "", {{"!including! all of them", ""}});
	CheckAllPermutations("", {"including", "-excluding"}, "", {{"!including! all of them", ""}});
}

TEST_P(FTGenericApi, SelectWithFieldsList) {
	Init(GetDefaultConfig());

	Add("nm1"sv, "Never watch their games"sv, "Because nothing can be worse than Spartak Moscow"sv);
	Add("nm1"sv, "Spartak Moscow is the worst team right now"sv, "Yes, for sure"sv);

	CheckAllPermutations("@ft1 ", {"Spartak", "Moscow"}, "", {{"!Spartak Moscow! is the worst team right now", "Yes, for sure"}});
}

TEST_P(FTGenericApi, SelectWithRelevanceBoost) {
	Init(GetDefaultConfig());

	Add("She was a very bad girl"sv);
	Add("All the naughty kids go to hell, not to heaven"sv);
	Add("I've never seen a man as cruel as him"sv);

	CheckAllPermutations("@ft1 ", {"girl^2", "kids", "cruel^3"}, "",
						 {{"I've never seen a man as !cruel! as him", ""},
						  {"She was a very bad !girl!", ""},
						  {"All the naughty !kids! go to hell, not to heaven", ""}},
						 true);
}

TEST_P(FTGenericApi, SelectWithDistance) {
	Init(GetDefaultConfig());

	Add("Her nose was very very long"sv);
	Add("Her nose was exceptionally long"sv);
	Add("Her nose was long"sv);

	CheckResults("'nose long'~3", {{"Her !nose was long!", ""}, {"Her !nose was exceptionally long!", ""}}, true);
}

TEST_P(FTGenericApi, AreasOnSuffix) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.optimization = reindexer::FtFastConfig::Optimization::CPU;
	Init(ftCfg);

	Add("the nos1 the nos2 the nosmn the nose"sv);
	Add("the ssmask the nnmask the mask the "sv);
	Add("the sslevel1 the nnlevel2 the kklevel the level"sv);
	Add("the nos1 the mmask stop nos2 table"sv);
	Add("Маша ела кашу. Каша кушалась сама. Маша кашляла."sv);

	CheckResults("каш*", {{"Маша ела !кашу. Каша! кушалась сама. Маша !кашляла!.", ""}}, false);
	CheckResults("nos*", {{"the !nos1! the !nos2! the !nosmn! the !nose!", ""}, {"the !nos1! the mmask stop !nos2! table", ""}}, false);
	CheckResults("*mask", {{"the !ssmask! the !nnmask! the !mask! the ", ""}, {"the nos1 the !mmask! stop nos2 table", ""}}, false);
	CheckResults("*level*", {{"the !sslevel1! the !nnlevel2! the !kklevel! the !level!", ""}}, false);
	CheckResults("+nos* +*mask ", {{"the !nos1! the !mmask! stop !nos2! table", ""}}, false);
}

TEST_P(FTGenericApi, DebugInfo) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	Add("Маша ела кашу. Каша кушалась сама. Машу ругали."sv);
	Add("Коля, Сеня гуляли."sv);
	Add("слово простая фраза что то еще."sv);
	Add("слово начало простая фраза конец что то еще простая фраза слово слово."sv);
	Add("жил пил гулял"sv);

	auto removeLineEnd = [](std::vector<std::string>& dataCompare) {
		for (auto& s : dataCompare) {
			s.erase(std::remove(s.begin(), s.end(), '\n'), s.end());
		}
	};

	{
		reindexer::Query q("nm1");
		q.Where("ft3", CondEq, "маша");
		q.AddFunction("ft3 = debug_rank()");
		q.Select({"ft1"});
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 1);
		auto it = res.begin();
		reindexer::WrSerializer wrSer;
		err = it.GetJSON(wrSer, false);
		ASSERT_TRUE(err.ok()) << err.what();
		// clang-format off
        std::vector<std::string> dataCompare={R"({"ft1":
"{term_rank:97, term:маша, pattern:маша, bm25_norm:0.9798439468181269, term_len_boost:1, position_rank:1, norm_dist:0, proc:100, full_match_boost:0} Маша ела кашу. Каша кушалась сама.
 {term_rank:77, term:маша, pattern:машу, bm25_norm:0.9798439468181269, term_len_boost:1, position_rank:0.994, norm_dist:0, proc:80, full_match_boost:0} Машу ругали."})"};
		// clang-format on
		removeLineEnd(dataCompare);
		ASSERT_EQ(wrSer.Slice(), dataCompare[0]);
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft3", CondEq, "коля сеня");
		q.AddFunction("ft3 = debug_rank()");
		q.Select({"ft1"});
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 1);
		auto it = res.begin();
		reindexer::WrSerializer wrSer;
		err = it.GetJSON(wrSer, false);
		ASSERT_TRUE(err.ok()) << err.what();
		// clang-format off
		std::vector<std::string> dataCompare={R"({"ft1":
"{term_rank:102, term:коля, pattern:коля, bm25_norm:1.022314131295854, term_len_boost:1, position_rank:1, norm_dist:0, proc:100, full_match_boost:0} Коля,
 {term_rank:102, term:сеня, pattern:сеня, bm25_norm:1.022314131295854, term_len_boost:1, position_rank:0.999, norm_dist:0, proc:100, full_match_boost:0} Сеня гуляли."})"};
		// clang-format on
		removeLineEnd(dataCompare);
		ASSERT_EQ(wrSer.Slice(), dataCompare[0]);
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft3", CondEq, "'начало простая фраза конец' 'простая фраза'");
		q.AddFunction("ft3 = debug_rank()");
		q.Select({"ft1"});
		q.Sort("id", false);
		q.WithRank();
		// clang-format off
        std::vector<std::string> dataCompare={
R"###({"ft1":"слово
 <!>{term_rank:93, term:простая, pattern:простая, bm25_norm:0.9399331930048559, term_len_boost:1, position_rank:0.999, norm_dist:0, proc:100, full_match_boost:0} простая
 {term_rank:85, term:фраза, pattern:фраза, bm25_norm:0.9399331930048559, term_len_boost:0.9142857193946838, position_rank:0.998, norm_dist:0, proc:100, full_match_boost:0} фраза<!!>
 что то еще.","rank()":101.0})###",
R"##({"ft1":"слово
 <!>{term_rank:92, term:начало, pattern:начало, bm25_norm:0.9624865670750559, term_len_boost:0.9571428596973419, position_rank:0.999, norm_dist:0, proc:100, full_match_boost:0} начало
 {term_rank:94, term:простая, pattern:простая, bm25_norm:0.9436916111700189, term_len_boost:1, position_rank:0.998, norm_dist:0, proc:100, full_match_boost:0}
 <!>{term_rank:94, term:простая, pattern:простая, bm25_norm:0.9436916111700189, term_len_boost:1, position_rank:0.998, norm_dist:0, proc:100, full_match_boost:0} простая
 {term_rank:86, term:фраза, pattern:фраза, bm25_norm:0.9436916111700189, term_len_boost:0.9142857193946838, position_rank:0.997, norm_dist:0, proc:100, full_match_boost:0}
 {term_rank:86, term:фраза, pattern:фраза, bm25_norm:0.9436916111700189, term_len_boost:0.9142857193946838, position_rank:0.997, norm_dist:0, proc:100, full_match_boost:0} фраза<!!>
 {term_rank:87, term:конец, pattern:конец, bm25_norm:0.9624865670750559, term_len_boost:0.9142857193946838, position_rank:0.996, norm_dist:0, proc:100, full_match_boost:0} конец<!!>
 что то еще
 <!>{term_rank:94, term:простая, pattern:простая, bm25_norm:0.9436916111700189, term_len_boost:1, position_rank:0.998, norm_dist:0, proc:100, full_match_boost:0} простая
 {term_rank:86, term:фраза, pattern:фраза, bm25_norm:0.9436916111700189, term_len_boost:0.9142857193946838, position_rank:0.997, norm_dist:0, proc:100, full_match_boost:0} фраза<!!>
 слово слово.","rank()":255.0})##"
            };
		// clang-format on
		removeLineEnd(dataCompare);
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 2);

		size_t i = 0;
		for (auto it : res) {
			reindexer::WrSerializer wrSer;
			err = it.GetJSON(wrSer, false);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(dataCompare[i], wrSer.Slice());
			i++;
		}
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft3", CondEq, "'простыми фразами'");
		q.AddFunction("ft3 = debug_rank()");
		q.Select({"ft1"});
		q.Sort("id", false);
		// clang-format off
        std::vector<std::string> dataCompare={
R"###({"ft1":"слово
 <!>{term_rank:74, term:простыми, pattern:простая, bm25_norm:0.9399331930048559, term_len_boost:1, position_rank:0.999, norm_dist:0, proc:79, full_match_boost:0} простая
 {term_rank:74, term:фразами, pattern:фраза, bm25_norm:0.9399331930048559, term_len_boost:0.9624999999999999, position_rank:0.998, norm_dist:0, proc:82, full_match_boost:0} фраза<!!>
 что то еще."})###",
R"###({"ft1":"слово начало
 <!>{term_rank:74, term:простыми, pattern:простая, bm25_norm:0.9436916111700189, term_len_boost:1, position_rank:0.998, norm_dist:0, proc:79, full_match_boost:0} простая
 {term_rank:74, term:фразами, pattern:фраза, bm25_norm:0.9436916111700189, term_len_boost:0.9624999999999999, position_rank:0.997, norm_dist:0, proc:82, full_match_boost:0} фраза<!!>
 конец что то еще
 <!>{term_rank:74, term:простыми, pattern:простая, bm25_norm:0.9436916111700189, term_len_boost:1, position_rank:0.998, norm_dist:0, proc:79, full_match_boost:0} простая
 {term_rank:74, term:фразами, pattern:фраза, bm25_norm:0.9436916111700189, term_len_boost:0.9624999999999999, position_rank:0.997, norm_dist:0, proc:82, full_match_boost:0} фраза<!!>
 слово слово."})###"
            };
		// clang-format on
		removeLineEnd(dataCompare);

		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 2);
		size_t i = 0;
		for (auto it : res) {
			reindexer::WrSerializer wrSer;
			err = it.GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(dataCompare[i], wrSer.Slice());
			i++;
		}
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft3", CondEq, "жил~ пил");
		q.Select({"ft1"});
		q.AddFunction("ft3 = debug_rank()");
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 1);
		auto it = res.begin();
		reindexer::WrSerializer wrSer;
		err = it.GetJSON(wrSer, false);
		ASSERT_TRUE(err.ok()) << err.what();
		//clang-format off
		std::vector<std::string> dataCompare = {
			R"({"ft1":"{term_rank:102, term:жил, pattern:жил, bm25_norm:1.022314131295854, term_len_boost:1, position_rank:1, norm_dist:0, proc:100, full_match_boost:0} жил
 {term_rank:71, term:жил, pattern:ил, bm25_norm:1.022314131295854, term_len_boost:1, position_rank:0.999, norm_dist:0, proc:70, full_match_boost:0}
 {term_rank:102, term:пил, pattern:пил, bm25_norm:1.022314131295854, term_len_boost:1, position_rank:0.999, norm_dist:0, proc:100, full_match_boost:0} пил гулял"})"};
		//clang-format on
		removeLineEnd(dataCompare);
		ASSERT_EQ(wrSer.Slice(), dataCompare[0]);
	}
}

TEST_P(FTGenericApi, AreasMaxRank) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.maxAreasInDoc = 3;
	Init(ftCfg);
	// the longer the word, the greater its rank
	Add("empty bb empty ccc empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty jjjjjjjjjjj empty kkkkkkkkkkkk empty lllllllllllll"sv);
	Add("empty lllllllllllll empty ccc empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty jjjjjjjjjjj empty kkkkkkkkkkkk empty bb"sv);
	// clang-format off
	CheckResults("bb ccc dddd eeeee ffffff gggggggg hhhhhhhhh iiiiiiiiii jjjjjjjjjjj kkkkkkkkkkkk lllllllllllll",
				{
					{"empty bb empty ccc empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty !jjjjjjjjjjj! empty !kkkkkkkkkkkk! empty !lllllllllllll!", ""},
					{"empty !lllllllllllll! empty ccc empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty !jjjjjjjjjjj! empty !kkkkkkkkkkkk! empty bb", ""}
				},
				 false);
	CheckResults("lllllllllllll bb ccc dddd eeeee ffffff gggggggg hhhhhhhhh iiiiiiiiii jjjjjjjjjjj kkkkkkkkkkkk",
			{
				{"empty !bb! empty !ccc! empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty jjjjjjjjjjj empty kkkkkkkkkkkk empty !lllllllllllll!", ""},
				{"empty !lllllllllllll! empty !ccc! empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty jjjjjjjjjjj empty kkkkkkkkkkkk empty !bb!", ""}
			},
			 false);
	CheckResults("bb ccc lllllllllllll dddd eeeee ffffff gggggggg  hhhhhhhhh iiiiiiiiii jjjjjjjjjjj kkkkkkkkkkkk",
		{
			{"empty !bb! empty !ccc! empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty jjjjjjjjjjj empty kkkkkkkkkkkk empty !lllllllllllll!", ""},
			{"empty !lllllllllllll! empty !ccc! empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty jjjjjjjjjjj empty kkkkkkkkkkkk empty !bb!", ""}
		},
		 false);
	CheckResults("lllllllllllll jjjjjjjjjjj kkkkkkkkkkkk bb ccc dddd eeeee ffffff gggggggg  hhhhhhhhh iiiiiiiiii",
		{
			{"empty bb empty ccc empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty !jjjjjjjjjjj! empty !kkkkkkkkkkkk! empty !lllllllllllll!", ""},
			{"empty !lllllllllllll! empty ccc empty dddd empty eeeee empty ffffff empty gggggggg empty hhhhhhhhh empty iiiiiiiiii empty !jjjjjjjjjjj! empty !kkkkkkkkkkkk! empty bb", ""}
		},
	 	false);

	// clang-format on
}

TEST_P(FTGenericApi, SelectWithDistance2) {
	auto check = [&](bool withHighlight) {
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two!", ""}, {"!one ецщ!", ""}, {"empty !one two!", ""}, {"empty !one two! word", ""}};
			CheckResults(R"s("one two")s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two!", ""}, {"!one ецщ!", ""}, {"empty !one two!", ""}, {"empty !one two! word", ""}};
			CheckResults(R"s("one two"~1)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"empty !one two!", ""},
																				  {"!one empty two!", ""},
																				  {"empty !one two! word", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""}};
			CheckResults(R"s(+"one two"~3)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"empty !one two!", ""},
																				  {"!one empty two!", ""},
																				  {"empty !one two! word", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""}};
			CheckResults(R"s("one two"~3)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two!", ""}, {"!one ецщ!", ""}, {"!empty one two!", ""}, {"!empty one two! word", ""}};
			CheckAllPermutations("", {"empty", R"s(+"one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty one two!", ""}, {"!empty one two! word", ""}};
			CheckAllPermutations("", {"+empty", R"s(+"one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {"empty", R"s("one two")s"}, "", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH),
								 false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {"empty", R"s(-"one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {R"s(-"one two")s", "+empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!one two!", ""},
																				  {"empty !one two!", ""},
																				  {"empty !one two! word", ""},
																				  {"!one! empty !two!", ""},
																				  {"word !one! empty !two!", ""},
																				  {"word !one! empty empty !two! word", ""},
																				  {"word !one! empty empty empty !two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one! empty empty empty !two two! word", ""},
																				  {"word !one one! empty empty empty !two! word", ""}};
			CheckAllPermutations("", {R"s("one")s", R"s("two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!one two!", ""},
																				  {"empty !one two!", ""},
																				  {"empty !one two! word", ""},
																				  {"!one! empty !two!", ""},
																				  {"word !one! empty !two!", ""},
																				  {"word !one! empty empty !two! word", ""},
																				  {"word !one! empty empty empty !two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one! empty empty empty !two two! word", ""},
																				  {"word !one one! empty empty empty !two! word", ""}};
			CheckAllPermutations("", {R"s("one")s", R"s("")s", R"s("two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!one two!", ""},
																				  {"empty !one two!", ""},
																				  {"empty !one two! word", ""},
																				  {"!one! empty !two!", ""},
																				  {"word !one! empty !two!", ""},
																				  {"word !one! empty empty !two! word", ""},
																				  {"word !one! empty empty empty !two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one! empty empty empty !two two! word", ""},
																				  {"word !one one! empty empty empty !two! word", ""}};
			CheckAllPermutations("", {R"s("one"~3)s", R"s(""~4)s", R"s("two"~5)s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"!one empty two!", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""},
																				  {"word !one empty empty empty two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one empty empty empty two two! word", ""},
																				  {"word !one one empty empty empty two! word", ""}};
			CheckAllPermutations("", {R"s("one")s", R"s("two")s", R"s("empty")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"!one empty two!", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""},
																				  {"word !one empty empty empty two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one empty empty empty two two! word", ""},
																				  {"word !one one empty empty empty two! word", ""}};
			CheckAllPermutations("", {R"s("one")s", R"s("two")s", "empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"!one empty two!", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""},
																				  {"word !one empty empty empty two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one empty empty empty two two! word", ""},
																				  {"word !one one empty empty empty two! word", ""}};
			CheckAllPermutations("", {R"s("one")s", R"s("")s", R"s("two")s", "empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one!", ""},
																				  {"!two!", ""},
																				  {"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"!one empty two!", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""},
																				  {"word !one empty empty empty two! word", ""},
																				  {"!one ецщ!", ""},
																				  {"word !one empty empty empty two two! word", ""},
																				  {"word !one one empty empty empty two! word", ""}};
			CheckAllPermutations("", {R"s("one"~2)s", R"s("two"~3)s", "empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {R"s("empty")s", R"s("one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
	};

	Init(GetDefaultConfig());

	Add("one"sv);
	Add("two"sv);
	Add("empty"sv);
	Add("one two"sv);
	Add("empty one two"sv);
	Add("empty one two word"sv);
	Add("one empty two"sv);
	Add("word one empty two"sv);
	Add("word one empty empty two word"sv);
	Add("word one empty empty empty two word"sv);
	Add("one ецщ"sv);
	Add("word one empty empty empty two two word"sv);
	Add("word one one empty empty empty two word"sv);

	check(true);
	check(false);
}

TEST_P(FTGenericApi, SelectWithDistance3) {
	Init(GetDefaultConfig());

	Add("one"sv);
	Add("two"sv);
	Add("three"sv);
	Add("empty"sv);
	Add("one two three"sv);
	Add("empty one two three"sv);
	Add("empty one two three word"sv);
	Add("one empty two three"sv);
	Add("word one empty two three"sv);
	Add("word one empty empty two word three"sv);
	Add("word one empty empty empty two word three"sv);
	Add("one ецщ three"sv);
	Add("one two empty two three"sv);
	Add("one two empty two empty empty three"sv);
	auto check = [&](bool withHighlight) {
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two three!", ""}, {"!one ецщ three!", ""}, {"empty !one two three!", ""}, {"empty !one two three! word", ""}};
			CheckResults(R"s("one two three")s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two three!", ""}, {"!one ецщ three!", ""}, {"empty !one two three!", ""}, {"empty !one two three! word", ""}};
			CheckResults(R"s("one two three"~1)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two three!", ""},
																				  {"!one ецщ three!", ""},
																				  {"empty !one two three!", ""},
																				  {"!one empty two three!", ""},
																				  {"empty !one two three! word", ""},
																				  {"word !one empty two three!", ""},
																				  {"word !one empty empty two word three!", ""},
																				  {"!one two empty two three!", ""},
																				  {"!one two empty two empty empty three!", ""}};
			CheckResults(R"s("one two three"~3)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two three!", ""},
																				  {"!one ецщ three!", ""},
																				  {"empty !one two three!", ""},
																				  {"!one empty two three!", ""},
																				  {"empty !one two three! word", ""},
																				  {"word !one empty two three!", ""}};
			CheckResults(R"s("one two three"~2)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two three!", ""}, {"!one ецщ three!", ""}, {"!empty one two three!", ""}, {"!empty one two three! word", ""}};
			CheckAllPermutations("", {"empty", R"s(+"one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}

		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty one two three!", ""},
																				  {"!empty one two three! word", ""}};
			CheckAllPermutations("", {"+empty", R"s(+"one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}

		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"!one two three!", ""},
																				  {"!one ецщ three!", ""},
																				  {"!empty one two three!", ""},
																				  {"one two !empty! two three", ""},
																				  {"!empty one two three! word", ""},
																				  {"one !empty! two three", ""},
																				  {"word one !empty empty! two word three", ""},
																				  {"word one !empty empty empty! two word three", ""},
																				  {"word one !empty! two three", ""},
																				  {"one two !empty! two !empty empty! three", ""}};
			CheckAllPermutations("", {"empty", R"s("one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two three", ""},
																				  {"word one !empty! two three", ""},
																				  {"word one !empty empty! two word three", ""},
																				  {"word one !empty empty empty! two word three", ""},
																				  {"one two !empty! two three", ""},
																				  {"one two !empty! two !empty empty! three", ""}};
			CheckAllPermutations("", {"empty", R"s(-"one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two three", ""},
																				  {"word one !empty! two three", ""},
																				  {"word one !empty empty! two word three", ""},
																				  {"word one !empty empty empty! two word three", ""},
																				  {"one two !empty! two three", ""},
																				  {"one two !empty! two !empty empty! three", ""}};
			CheckAllPermutations("", {R"s(-"one two three")s", "+empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
	};
	check(true);
	check(false);
}
TEST_P(FTGenericApi, SelectWithDistanceSubTerm) {
	Init(GetDefaultConfig());
	Add("one two empty щту two empty one ецщ"sv);
	CheckResults(R"s("one two")s", {{"!one two! empty !щту two! empty !one ецщ!", ""}}, false, true);
	CheckResults(R"s("one two")s", {{"one two empty щту two empty one ецщ", ""}}, false, false);
}

TEST_P(FTGenericApi, SelectWithDistance2Field) {
	Init(GetDefaultConfig());
	Add("empty two empty two one"sv, "two"sv);
	// 24 bits - the number of words in the field
	CheckResults("'one two'~" + std::to_string((1 << 24) + 100), {}, false);
}

TEST_P(FTGenericApi, SelectWithSeveralGroup) {
	Init(GetDefaultConfig());

	Add("one empty two word three four"sv);
	Add("word one empty two word three four word"sv);
	Add("one three two four"sv);
	Add("word three one two four word"sv);
	CheckAllPermutations("", {R"s(+"one two"~2)s", R"s(+"three four"~3)s"}, "",
						 {{"!one empty two! word !three four!", ""},
						  {"word !one empty two! word !three four! word", ""},
						  {"!one three two four!", ""},
						  {"word !three one two four! word", ""}},
						 false);
}

TEST_P(FTGenericApi, NumberToWordsSelect) {
	Init(GetDefaultConfig());
	Add("оценка 5 майкл джордан 23"sv, ""sv);

	CheckAllPermutations("", {"пять", "+двадцать", "+три"}, "", {{"оценка !5! майкл джордан !23!", ""}});
}

// Make sure FT seeks by a huge number set by string in DSL
TEST_P(FTGenericApi, HugeNumberToWordsSelect) {
	// Initialize namespace
	Init(GetDefaultConfig());
	// Add a record with a big number
	Add("много 7343121521906522180408440 денег"sv, ""sv);
	// Execute FT query, where search words are set as strings
	auto qr = SimpleSelect(
		"+семь +септиллионов +триста +сорок +три +секстиллиона +сто +двадцать +один +квинтиллион +пятьсот +двадцать +один +квадриллион "
		"+девятьсот +шесть +триллионов +пятьсот +двадцать +два +миллиарда +сто +восемьдесят +миллионов +четыреста +восемь +тысяч "
		"+четыреста +сорок");
	// Make sure it found this only string
	ASSERT_TRUE(qr.Count() == 1);
}

// Make sure way too huge numbers are ignored in FT
TEST_P(FTGenericApi, HugeNumberToWordsSelect2) {
	// Initialize namespace
	Init(GetDefaultConfig());
	// Add a record with a huge number
	Add("1127343121521906522180408440"sv, ""sv);
	// Execute FT query, where search words are set as strings
	reindexer::QueryResults qr;
	const std::string searchWord =
		"+один +октиллион +сто +двадцать +семь +септиллионов +триста +сорок +три +секстиллиона +сто +двадцать +один +квинтиллион "
		"+пятьсот "
		"+двадцать +один +квадриллион +девятьсот +шесть +триллионов +пятьсот +двадцать +два +миллиарда +сто +восемьдесят +миллионов "
		"+четыреста +восемь +тысяч +четыреста +сорок";
	auto q{reindexer::Query("nm1").Where("ft3", CondEq, searchWord)};
	auto err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	// Make sure it has found absolutely nothing
	ASSERT_EQ(qr.Count(), 0);
}

TEST_P(FTGenericApi, DeleteTest) {
	Init(GetDefaultConfig());

	std::unordered_map<std::string, int> data;
	for (int i = 0; i < 10000; ++i) {
		data.insert(Add(rt.RuRandString()));
	}
	auto res = SimpleSelect("entity");
	for (int i = 0; i < 10000; ++i) {
		data.insert(Add(rt.RuRandString()));
	}
	res = SimpleSelect("entity");

	data.insert(Add("An entity is something that exists as itself"sv));
	data.insert(Add("In law, a legal entity is an entity that is capable of bearing legal rights"sv));
	data.insert(Add("In politics, entity is used as term for territorial divisions of some countries"sv));
	data.insert(Add("Юридическое лицо — организация, которая имеет обособленное имущество"sv));
	data.insert(Add("Aftermath - the consequences or aftereffects of a significant unpleasant event"sv));
	data.insert(Add("Food prices soared in the aftermath of the drought"sv));
	data.insert(Add("In the aftermath of the war ..."sv));

	const auto err = Delete(data.find("In law, a legal entity is an entity that is capable of bearing legal rights")->second);
	ASSERT_TRUE(err.ok()) << err.what();
	res = SimpleSelect("entity");

	// TODO: add validation
}

TEST_P(FTGenericApi, RebuildAfterDeletion) {
	Init(GetDefaultConfig());

	auto cfg = GetDefaultConfig();
	cfg.maxStepSize = 5;
	auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
	ASSERT_TRUE(err.ok()) << err.what();

	auto selectF = [this](const std::string& word) {
		const auto q{reindexer::Query("nm1").Where("ft1", CondEq, word)};
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	};

	std::unordered_map<std::string, int> data;
	data.insert(Add("An entity is something that exists as itself"sv));
	data.insert(Add("In law, a legal entity is an entity that is capable of bearing legal rights"sv));
	data.insert(Add("In politics, entity is used as term for territorial divisions of some countries"sv));
	data.insert(Add("Юридическое лицо — организация, которая имеет обособленное имущество"sv));
	data.insert(Add("Aftermath - the consequences or aftereffects of a significant unpleasant event"sv));
	data.insert(Add("Food prices soared in the aftermath of the drought"sv));
	data.insert(Add("In the aftermath of the war ..."sv));

	auto res = selectF("entity");
	ASSERT_EQ(res.Count(), 3);

	err = Delete(data.find("In law, a legal entity is an entity that is capable of bearing legal rights")->second);
	ASSERT_TRUE(err.ok()) << err.what();
	res = selectF("entity");
	ASSERT_EQ(res.Count(), 2);
}

TEST_P(FTGenericApi, Unique) {
	Init(GetDefaultConfig());

	std::vector<std::string> data;
	std::set<size_t> check;
	std::set<std::string> checks;
	reindexer::logInstallWriter([](int, char*) { /*std::cout << buf << std::endl;*/ }, reindexer::LoggerPolicy::WithLocks, int(LogTrace));

	for (int i = 0; i < 1000; ++i) {
		bool inserted = false;
		size_t n;
		std::string s;

		while (!inserted) {
			n = rand();
			auto res = check.insert(n);
			inserted = res.second;
		}

		inserted = false;

		while (!inserted) {
			s = rt.RandString();
			auto res = checks.insert(s);
			inserted = res.second;
		}

		data.push_back(s + std::to_string(n));
	}

	for (size_t i = 0; i < data.size(); i++) {
		Add(data[i], data[i]);
		if (i % 5 == 0) {
			for (size_t j = 0; j < i; j++) {
				if (i == 40 && j == 26) {
					int a = 3;	// NOLINT(*unused-but-set-variable) This code is just to load CPU by non-rx stuff
					a++;
					(void)a;
				}
				auto res = StressSelect(data[j]);
				if (res.Count() != 1) {
					for (auto it : res) {
						auto ritem(it.GetItem(false));
					}
					abort();
				}
			}
		}
	}
}

TEST_P(FTGenericApi, SummationOfRanksInSeveralFields) {
	auto ftCfg = GetDefaultConfig(3);
	ftCfg.summationRanksByFieldsRatio = 0.0f;
	Init(ftCfg, NS3);

	Add("nm3"sv, "word"sv, "word"sv, "word"sv);
	Add("nm3"sv, "word"sv, "test"sv, "test"sv);
	Add("nm3"sv, "test"sv, "word"sv, "test"sv);
	Add("nm3"sv, "test"sv, "test"sv, "word"sv);
	uint16_t rank = 0;
	// Do not sum ranks by fields, as it is not asked in request and sum ratio in config is zero
	const auto queries = CreateAllPermutatedQueries("@", {"ft1", "ft2", "ft3"}, " word", ",");
	for (size_t i = 0; i < queries.size(); ++i) {
		const auto& q = queries[i];
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		auto& lqr = qr.ToLocalQr();
		auto it = lqr.begin();
		if (i == 0) {
			rank = it.GetItemRef().Proc();
		}
		for (const auto end = lqr.end(); it != end; ++it) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	// Do not sum ranks by fields, inspite of it is asked in request, as sum ratio in config is zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "+ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		for (const auto& it : qr.ToLocalQr()) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	// Do not sum ranks by fields, inspite of it is asked in request, as sum ratio in config is zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+*"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		for (const auto& it : qr.ToLocalQr()) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	ftCfg.summationRanksByFieldsRatio = 1.0f;
	auto err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
	ASSERT_TRUE(err.ok()) << err.what();
	Add("nm3"sv, "test"sv, "test"sv, "test"sv);
	// Do not sum ranks by fields, inspite of sum ratio in config is not zero, as it is not asked in request
	for (const auto& q : CreateAllPermutatedQueries("@", {"ft1", "ft2", "ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		for (const auto& it : qr.ToLocalQr()) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	// Do sum ranks by fields, as it is asked in request and sum ratio in config is not zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "+ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		auto it = qr.ToLocalQr().begin();
		rank = it.GetItemRef().Proc() / 3;
		++it;
		for (const auto end = qr.ToLocalQr().end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// Do sum ranks by fields, as it is asked in request and sum ratio in config is not zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+*"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		auto it = qr.ToLocalQr().begin();
		rank = it.GetItemRef().Proc() / 3;
		++it;
		for (const auto end = qr.ToLocalQr().end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// ft2 is skipped as is not marked with +
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		auto it = qr.ToLocalQr().begin();
		rank = it.GetItemRef().Proc() / 2;
		++it;
		for (const auto end = qr.ToLocalQr().end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// ft2 is not skipped as it has max rank
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "ft2^2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		auto it = qr.ToLocalQr().begin();
		rank = it.GetItemRef().Proc() / 4;
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), (rank + 1) * 2) << q;
		EXPECT_GE(it.GetItemRef().Proc(), (rank - 1) * 2) << q;
		++it;
		for (const auto end = qr.ToLocalQr().end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// Ranks summated with ratio 0.5
	ftCfg.summationRanksByFieldsRatio = 0.5f;
	err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
	ASSERT_TRUE(err.ok()) << err.what();
	Add("nm3"sv, "test"sv, "test"sv, "test"sv);
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "+ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		assert(qr.IsLocal());
		auto it = qr.ToLocalQr().begin();
		rank = it.GetItemRef().Proc() / (1.0 + 0.5 + 0.5 * 0.5);
		++it;
		for (const auto end = qr.ToLocalQr().end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// Ranks summated with ratio 0.5
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1^1.5", "+ft2^1.3", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 true);
		assert(qr.IsLocal());
		auto it = qr.ToLocalQr().begin();
		rank = it.GetItemRef().Proc() / (1.5 + 0.5 * 1.3 + 0.5 * 0.5);
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), (rank + 5) * 1.5) << q;
		EXPECT_GE(it.GetItemRef().Proc(), (rank - 5) * 1.5) << q;
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), (rank + 5) * 1.3) << q;
		EXPECT_GE(it.GetItemRef().Proc(), (rank - 5) * 1.3) << q;
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), rank + 5) << q;
		EXPECT_GE(it.GetItemRef().Proc(), rank - 5) << q;
	}
}

TEST_P(FTGenericApi, SelectTranslitWithComma) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.logLevel = 5;
	Init(ftCfg);

	Add("nm1"sv, "хлебопечка"sv, ""sv);
	Add("nm1"sv, "электрон"sv, ""sv);
	Add("nm1"sv, "матэ"sv, ""sv);

	auto qr = SimpleSelect("@ft1 [kt,jgtxrf");
	EXPECT_EQ(qr.Count(), 1);
	auto item = qr.begin().GetItem(false);
	EXPECT_EQ(item["ft1"].As<std::string>(), "!хлебопечка!");

	qr = SimpleSelect("@ft1 \\'ktrnhjy");
	EXPECT_EQ(qr.Count(), 1);
	item = qr.begin().GetItem(false);
	EXPECT_EQ(item["ft1"].As<std::string>(), "!электрон!");

	qr = SimpleSelect("@ft1 vfn\\'");
	EXPECT_EQ(qr.Count(), 1);
	item = qr.begin().GetItem(false);
	EXPECT_EQ(item["ft1"].As<std::string>(), "!матэ!");
}

TEST_P(FTGenericApi, RankWithPosition) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fieldsCfg[0].positionWeight = 1.0;
	Init(ftCfg);

	Add("nm1"sv, "one two three word"sv, ""sv);
	Add("nm1"sv, "one two three four five six word"sv, ""sv);
	Add("nm1"sv, "one two three four word"sv, ""sv);
	Add("nm1"sv, "one word"sv, ""sv);
	Add("nm1"sv, "one two three four five word"sv, ""sv);
	Add("nm1"sv, "word"sv, ""sv);
	Add("nm1"sv, "one two word"sv, ""sv);

	CheckAllPermutations("", {"word"}, "",
						 {{"!word!", ""},
						  {"one !word!", ""},
						  {"one two !word!", ""},
						  {"one two three !word!", ""},
						  {"one two three four !word!", ""},
						  {"one two three four five !word!", ""},
						  {"one two three four five six !word!", ""}},
						 true);
}

TEST_P(FTGenericApi, DifferentFieldRankPosition) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fieldsCfg[0].positionWeight = 1.0;
	ftCfg.fieldsCfg[0].positionBoost = 10.0;
	Init(ftCfg);

	Add("nm1"sv, "one two three word"sv, "one word"sv);
	Add("nm1"sv, "one two three four five six word"sv, "one two word"sv);
	Add("nm1"sv, "one two three four word"sv, "one two three four five word"sv);
	Add("nm1"sv, "one word"sv, "one two three four five six word"sv);
	Add("nm1"sv, "one two three four five word"sv, "one two three word"sv);
	Add("nm1"sv, "word"sv, "one two three four word"sv);
	Add("nm1"sv, "one two word"sv, "word"sv);

	CheckAllPermutations("", {"word"}, "",
						 {{"!word!", "one two three four !word!"},
						  {"one !word!", "one two three four five six !word!"},
						  {"one two !word!", "!word!"},
						  {"one two three !word!", "one !word!"},
						  {"one two three four !word!", "one two three four five !word!"},
						  {"one two three four five !word!", "one two three !word!"},
						  {"one two three four five six !word!", "one two !word!"}},
						 true);

	ftCfg.fieldsCfg[0].positionWeight = 0.1;
	ftCfg.fieldsCfg[0].positionBoost = 1.0;
	ftCfg.fieldsCfg[1].positionWeight = 1.0;
	ftCfg.fieldsCfg[1].positionBoost = 10.0;
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"word"}, "",
						 {{"one two !word!", "!word!"},
						  {"one two three !word!", "one !word!"},
						  {"one two three four five six !word!", "one two !word!"},
						  {"one two three four five !word!", "one two three !word!"},
						  {"!word!", "one two three four !word!"},
						  {"one two three four !word!", "one two three four five !word!"},
						  {"one !word!", "one two three four five six !word!"}},
						 true);
}

TEST_P(FTGenericApi, PartialMatchRank) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.partialMatchDecrease = 0;
	Init(ftCfg);

	Add("nm1"sv, "ТНТ4"sv, ""sv);
	Add("nm1"sv, ""sv, "ТНТ"sv);

	CheckAllPermutations("@", {"ft1^1.1", "ft2^1"}, " ТНТ*", {{"!ТНТ4!", ""}, {"", "!ТНТ!"}}, true, ", ");

	ftCfg.partialMatchDecrease = 100;
	SetFTConfig(ftCfg);

	CheckAllPermutations("@", {"ft1^1.1", "ft2^1"}, " ТНТ*", {{"", "!ТНТ!"}, {"!ТНТ4!", ""}}, true, ", ");
}

TEST_P(FTGenericApi, SelectFullMatch) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fullMatchBoost = 0.9;
	Init(ftCfg);

	Add("nm1"sv, "test"sv, "love"sv);
	Add("nm1"sv, "test"sv, "love second"sv);

	CheckAllPermutations("", {"love"}, "", {{"test", "!love! second"}, {"test", "!love!"}}, true);

	ftCfg.fullMatchBoost = 1.1;
	SetFTConfig(ftCfg);
	CheckAllPermutations("", {"love"}, "", {{"test", "!love!"}, {"test", "!love! second"}}, true);
}

TEST_P(FTGenericApi, SetFtFieldsCfgErrors) {
	auto cfg = GetDefaultConfig(2);
	Init(cfg);
	cfg.fieldsCfg[0].positionWeight = 0.1;
	cfg.fieldsCfg[1].positionWeight = 0.2;
	// Задаем уникальный конфиг для поля ft, которого нет в индексе ft3
	auto err = SetFTConfig(cfg, "nm1", "ft3", {"ft", "ft2"});
	// Получаем ошибку
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Field 'ft' is not included to full text index");

	err = rt.reindexer->OpenNamespace("nm3");
	ASSERT_TRUE(err.ok()) << err.what();
	rt.DefineNamespaceDataset(
		"nm3", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft", "text", "string", IndexOpts(), 0}});
	// Задаем уникальный конфиг для единственного поля ft в индексе ft
	err = SetFTConfig(cfg, "nm3", "ft", {"ft"});
	// Получаем ошибку
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Configuration for single field fulltext index cannot contain field specifications");

	// maxTypos < 0
	cfg.maxTypos = -1;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	// Error
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "FtFastConfig: Value of 'max_typos' - -1 is out of bounds: [0,4]");

	// maxTypos > 4
	cfg.maxTypos = 5;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	// Error
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "FtFastConfig: Value of 'max_typos' - 5 is out of bounds: [0,4]");
}

TEST_P(FTGenericApi, MergeLimitConstraints) {
	auto cfg = GetDefaultConfig();
	Init(cfg);
	cfg.mergeLimit = reindexer::kMinMergeLimitValue - 1;
	reindexer::Error err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_EQ(err.code(), errParseJson);
	cfg.mergeLimit = reindexer::kMaxMergeLimitValue + 1;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_EQ(err.code(), errParseJson);
	cfg.mergeLimit = reindexer::kMinMergeLimitValue;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	cfg.mergeLimit = reindexer::kMaxMergeLimitValue;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_P(FTGenericApi, ConfigBm25Coefficients) {
	reindexer::FtFastConfig cfgDef = GetDefaultConfig();
	cfgDef.maxAreasInDoc = 100;
	reindexer::FtFastConfig cfg = cfgDef;
	cfg.bm25Config.bm25b = 0.0;
	cfg.bm25Config.bm25Type = reindexer::FtFastConfig::Bm25Config::Bm25Type::rx;

	Init(cfg);
	Add("nm1"sv, "слово пусто слова пусто словами"sv, ""sv);
	Add("nm1"sv, "слово пусто слово"sv, ""sv);
	Add("nm1"sv, "otherword targetword"sv, ""sv);
	Add("nm1"sv, "otherword targetword otherword targetword"sv, ""sv);
	Add("nm1"sv, "otherword targetword otherword targetword targetword"sv, ""sv);
	Add("nm1"sv,
		"otherword targetword otherword otherword otherword targetword otherword targetword otherword targetword otherword otherword otherword otherword otherword otherword otherword otherword targetword"sv,
		""sv);

	CheckResults("targetword",
				 {{"otherword !targetword! otherword otherword otherword !targetword! otherword !targetword! otherword !targetword! "
				   "otherword otherword otherword otherword otherword otherword otherword otherword !targetword!",
				   ""},
				  {"otherword !targetword! otherword !targetword targetword!", ""},
				  {"otherword !targetword! otherword !targetword!", ""},
				  {"otherword !targetword!", ""}},
				 true);

	cfg = cfgDef;
	cfg.bm25Config.bm25b = 0.75;
	reindexer::Error err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();

	CheckResults("targetword",
				 {{"otherword !targetword! otherword !targetword targetword!", ""},
				  {"otherword !targetword! otherword !targetword!", ""},
				  {"otherword !targetword! otherword otherword otherword !targetword! otherword !targetword! otherword !targetword! "
				   "otherword otherword otherword otherword otherword otherword otherword otherword !targetword!",
				   ""},
				  {"otherword !targetword!", ""}},
				 true);
	cfg = cfgDef;
	cfg.bm25Config.bm25Type = reindexer::FtFastConfig::Bm25Config::Bm25Type::wordCount;
	cfg.fieldsCfg[0].positionWeight = 0.0;
	cfg.fullMatchBoost = 1.0;

	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();

	CheckResults("targetword",
				 {
					 {"otherword !targetword! otherword otherword otherword !targetword! otherword !targetword! otherword !targetword! "
					  "otherword otherword otherword otherword otherword otherword otherword otherword !targetword!",
					  ""},
					 {"otherword !targetword! otherword !targetword targetword!", ""},
					 {"otherword !targetword! otherword !targetword!", ""},
					 {"otherword !targetword!", ""},

				 },
				 true);

	CheckResults("словах", {{"!слово! пусто !слово!", ""}, {"!слово! пусто !слова! пусто !словами!", ""}}, true);
}

TEST_P(FTGenericApi, ConfigFtProc) {
	reindexer::FtFastConfig cfgDef = GetDefaultConfig();
	cfgDef.synonyms = {{{"тестов"}, {"задача"}}};
	reindexer::FtFastConfig cfg = cfgDef;

	cfg.rankingConfig.fullMatch = 100;
	cfg.rankingConfig.stemmerPenalty = 1;  // for idf/tf boost
	cfg.rankingConfig.translit = 50;
	cfg.rankingConfig.kblayout = 40;
	cfg.rankingConfig.synonyms = 30;
	Init(cfg);
	Add("nm1"sv, "маленький тест"sv, "");
	Add("nm1"sv, "один тестов очень очень тестов тестов тестов"sv, "");
	Add("nm1"sv, "два тестов очень очень тестов тестов тестов"sv, "");
	Add("nm1"sv, "testov"sv, "");
	Add("nm1"sv, "ntcnjd"sv, "");
	Add("nm1"sv, "задача"sv, "");
	Add("nm1"sv, "Местов"sv, "");
	Add("nm1"sv, "МестоД"sv, "");

	reindexer::Error err;
	CheckResults("тестов",
				 {{"маленький !тест!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"!testov!", ""},
				  {"!ntcnjd!", ""},
				  {"!задача!", ""}},
				 true);
	cfg = cfgDef;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов",
				 {{"!задача!", ""},
				  {"!testov!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);
	cfg = cfgDef;
	cfg.rankingConfig.stemmerPenalty = 500;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов",
				 {{"!задача!", ""},
				  {"!testov!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);

	cfg = cfgDef;
	cfg.rankingConfig.stemmerPenalty = -1;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_EQ(err.code(), errParseJson);
	ASSERT_EQ(err.what(), "FtFastConfig: Value of 'stemmer_proc_penalty' - -1 is out of bounds: [0,500]");

	cfg = cfgDef;
	cfg.rankingConfig.synonyms = 500;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов",
				 {{"!задача!", ""},
				  {"!testov!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);
	cfg = cfgDef;
	cfg.rankingConfig.synonyms = 501;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_EQ(err.code(), errParseJson);
	ASSERT_EQ(err.what(), "FtFastConfig: Value of 'synonyms_proc' - 501 is out of bounds: [0,500]");

	cfg = cfgDef;
	cfg.rankingConfig.translit = 200;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов",
				 {{"!testov!", ""},
				  {"!задача!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);

	cfg = cfgDef;
	cfg.rankingConfig.typo = 300;
	cfg.rankingConfig.translit = 200;
	cfg.maxTypos = 4;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов~",
				 {{"!Местов!", ""},
				  {"!МестоД!", ""},
				  {"!testov!", ""},
				  {"!задача!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);

	cfg = cfgDef;
	cfg.rankingConfig.typo = 300;
	cfg.rankingConfig.typoPenalty = 150;
	cfg.rankingConfig.translit = 200;
	cfg.maxTypos = 4;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов~",
				 {{"!Местов!", ""},
				  {"!testov!", ""},
				  {"!МестоД!", ""},
				  {"!задача!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);

	cfg = cfgDef;
	cfg.rankingConfig.typo = 300;
	cfg.rankingConfig.typoPenalty = 500;
	cfg.rankingConfig.translit = 200;
	cfg.maxTypos = 4;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	CheckResults("тестов~",
				 {{"!testov!", ""},
				  {"!Местов!", ""},
				  {"!задача!", ""},
				  {"!ntcnjd!", ""},
				  {"один !тестов! очень очень !тестов тестов тестов!", ""},
				  {"два !тестов! очень очень !тестов тестов тестов!", ""},
				  {"маленький !тест!", ""}},
				 true);
}

TEST_P(FTGenericApi, InvalidDSLErrors) {
	auto cfg = GetDefaultConfig();
	cfg.stopWords.clear();
	cfg.stopWords.emplace("teststopword");
	Init(cfg);
	constexpr std::string_view kExpectedErrorMessage = "Fulltext query can not contain only 'NOT' terms (i.e. terms with minus)";

	{
		auto q = Query("nm1").Where("ft3", CondEq, "-word");
		reindexer::QueryResults qr;
		auto err = rt.reindexer->Select(q, qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kExpectedErrorMessage);

		qr.Clear();
		q = Query("nm1").Where("ft3", CondEq, "-word1 -word2 -word3");
		err = rt.reindexer->Select(q, qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kExpectedErrorMessage);

		qr.Clear();
		q = Query("nm1").Where("ft3", CondEq, "-\"word1 word2\"");
		err = rt.reindexer->Select(q, qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kExpectedErrorMessage);

		qr.Clear();
		q = Query("nm1").Where("ft3", CondEq, "-'word1 word2'");
		err = rt.reindexer->Select(q, qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kExpectedErrorMessage);

		qr.Clear();
		q = Query("nm1").Where("ft3", CondEq, "-word0 -'word1 word2' -word7");
		err = rt.reindexer->Select(q, qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), kExpectedErrorMessage);

		// Empty DSL is allowed
		qr.Clear();
		q = Query("nm1").Where("ft3", CondEq, "");
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 0);

		// Stop-word + 'minus' have to return empty response, to avoid random errors for user
		qr.Clear();
		q = Query("nm1").Where("ft3", CondEq, "-word1 teststopword -word2");
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
}

// Check ft preselect logic with joins. Joined results have to be return even after multiple queries (issue #1437)
TEST_P(FTGenericApi, JoinsWithFtPreselect) {
	using reindexer::Query;
	using reindexer::QueryResults;

	auto cfg = GetDefaultConfig();
	cfg.enablePreselectBeforeFt = true;
	Init(cfg);
	const int firstId = counter_;
	Add("word1 word2 word3"sv);
	Add("word3 word4"sv);
	Add("word2 word5 word7"sv);

	fast_hash_map<int, std::string> joinedNsItems;
	const std::string kJoinedNs = "ns_for_joins";
	const std::string kMainNs = "nm1";
	constexpr unsigned kQueryRepetitions = 6;
	CreateAndFillSimpleNs(kJoinedNs, 0, 10, &joinedNsItems);

	const Query q =
		Query(kMainNs).Where("ft3", CondEq, "word2").InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, firstId + 1));
	const auto expectedJoinedJSON = fmt::sprintf(R"json("joined_%s":[%s])json", kJoinedNs, joinedNsItems[firstId]);
	for (unsigned i = 0; i < kQueryRepetitions; ++i) {
		QueryResults qr;
		auto err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 1);
		auto item = qr.begin().GetItem();
		ASSERT_EQ(item["id"].As<int>(), firstId);
		reindexer::WrSerializer wser;
		err = qr.begin().GetJSON(wser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_TRUE(wser.Slice().find(expectedJoinedJSON) != std::string_view::npos)
			<< "Expecting substring '" << expectedJoinedJSON << "', but json was: '" << wser.Slice() << "'. Iteration: " << i;
	}
}

// Check that explain with ft preselect contains all the expected entries (issue #1437)
TEST_P(FTGenericApi, ExplainWithFtPreselect) {
	using reindexer::Query;
	using reindexer::QueryResults;

	auto cfg = GetDefaultConfig();
	cfg.enablePreselectBeforeFt = true;
	Init(cfg);
	const int firstId = counter_;
	Add("word1 word2 word3"sv);
	Add("word3 word4"sv);
	Add("word2 word5 word7"sv);
	const int lastId = counter_;

	const std::string kJoinedNs = "ns_for_joins";
	const std::string kMainNs = "nm1";
	CreateAndFillSimpleNs(kJoinedNs, 0, 10, nullptr);

	{
		const Query q = Query(kMainNs)
							.Where("ft3", CondEq, "word2")
							.OpenBracket()
							.InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, firstId + 1))
							.Or()
							.Where("id", CondEq, lastId - 1)
							.CloseBracket()
							.Explain();
		QueryResults qr;
		auto err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 2);
		// Check explain's content
		YAML::Node root = YAML::Load(qr.GetExplainResults());
		auto selectors = root["selectors"];
		ASSERT_TRUE(selectors.IsSequence()) << qr.GetExplainResults();
		ASSERT_EQ(selectors.size(), 2) << qr.GetExplainResults();
		EXPECT_EQ(selectors[0]["field"].as<std::string>(), "(-scan and (id and inner_join ns_for_joins) or id)") << qr.GetExplainResults();
		EXPECT_EQ(selectors[1]["field"].as<std::string>(), "ft3") << qr.GetExplainResults();
	}
	{
		// Check the same query with extra brackets over ft condition. Make sure, that ft-index was still move to the end of the query
		const Query q = Query(kMainNs)
							.OpenBracket()
							.Where("ft3", CondEq, "word2")
							.CloseBracket()
							.OpenBracket()
							.InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, firstId + 1))
							.Or()
							.Where("id", CondEq, lastId - 1)
							.CloseBracket()
							.Explain();
		QueryResults qr;
		auto err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 2);
		// Check explain's content
		YAML::Node root = YAML::Load(qr.GetExplainResults());
		auto selectors = root["selectors"];
		ASSERT_TRUE(selectors.IsSequence()) << qr.GetExplainResults();
		ASSERT_EQ(selectors.size(), 2) << qr.GetExplainResults();
		EXPECT_EQ(selectors[0]["field"].as<std::string>(), "(-scan and (id and inner_join ns_for_joins) or id)") << qr.GetExplainResults();
		EXPECT_EQ(selectors[1]["field"].as<std::string>(), "ft3") << qr.GetExplainResults();
	}
}

TEST_P(FTGenericApi, TotalCountWithFtPreselect) {
	using reindexer::Query;
	using reindexer::QueryResults;
	using reindexer::Variant;

	auto cfg = GetDefaultConfig();
	auto preselectIsEnabled = true;
	cfg.enablePreselectBeforeFt = preselectIsEnabled;
	Init(cfg);
	const int firstId = counter_;
	Add("word5"sv);
	Add("word1 word2 word3"sv);
	Add("word3 word4"sv);
	Add("word2 word5 word7"sv);
	const int lastId = counter_;

	const std::string kJoinedNs = "ns_for_joins";
	const std::string kMainNs = "nm1";
	CreateAndFillSimpleNs(kJoinedNs, 0, 10, nullptr);

	for (auto preselect : {true, false}) {
		if (preselectIsEnabled != preselect) {
			auto cfg = GetDefaultConfig();
			preselectIsEnabled = preselect;
			cfg.enablePreselectBeforeFt = preselectIsEnabled;
			SetFTConfig(cfg);
		}
		std::string_view kPreselectStr = preselect ? " (with ft preselect) " : " (no ft preselect) ";

		struct Case {
			Query query;
			int limit;
			int expectedTotalCount;
		};
		std::vector<Case> cases = {{.query = Query(kMainNs).Where("ft3", CondEq, "word2 word4"), .limit = 2, .expectedTotalCount = 3},
								   {.query = Query(kMainNs).Where("ft3", CondEq, "word2").Where("id", CondEq, {Variant{lastId - 3}}),
									.limit = 0,
									.expectedTotalCount = 1},
								   {.query = Query(kMainNs)
												 .Where("ft3", CondEq, "word2")
												 .InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, firstId + 2).Limit(0)),
									.limit = 0,
									.expectedTotalCount = 1},
								   {.query = Query(kMainNs)
												 .Where("ft3", CondEq, "word2 word3")
												 .OpenBracket()
												 .InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, firstId + 2).Limit(0))
												 .Or()
												 .Where("id", CondSet, {Variant{lastId - 1}, Variant{lastId - 2}})
												 .CloseBracket(),
									.limit = 1,
									.expectedTotalCount = 3},
								   {.query = Query(kMainNs)
												 .Where("ft3", CondEq, "word2 word3")
												 .InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, lastId).Limit(0))
												 .Where("id", CondSet, {Variant{lastId - 1}, Variant{lastId - 2}}),
									.limit = 1,
									.expectedTotalCount = 2},
								   {.query = Query(kMainNs)
												 .OpenBracket()
												 .Where("ft3", CondEq, "word2")
												 .CloseBracket()
												 .OpenBracket()
												 .InnerJoin("id", "id", CondEq, Query(kJoinedNs).Where("id", CondLt, firstId + 2))
												 .Or()
												 .Where("id", CondEq, lastId - 1)
												 .CloseBracket(),
									.limit = 0,
									.expectedTotalCount = 2}};

		for (auto& c : cases) {
			c.query.ReqTotal();
			// Execute initial query
			{
				QueryResults qr;
				auto err = rt.reindexer->Select(c.query, qr);
				ASSERT_TRUE(err.ok()) << kPreselectStr << err.what() << "\n" << c.query.GetSQL();
				EXPECT_EQ(qr.Count(), c.expectedTotalCount) << kPreselectStr << c.query.GetSQL();
				EXPECT_EQ(qr.TotalCount(), c.expectedTotalCount) << kPreselectStr << c.query.GetSQL();
			}

			// Execute query with limit
			const Query q = Query(c.query).Limit(c.limit);
			{
				QueryResults qr;
				auto err = rt.reindexer->Select(q, qr);
				ASSERT_TRUE(err.ok()) << kPreselectStr << err.what() << "\n" << c.query.GetSQL();
				EXPECT_EQ(qr.Count(), c.limit) << kPreselectStr << c.query.GetSQL();
				EXPECT_EQ(qr.TotalCount(), c.expectedTotalCount) << kPreselectStr << c.query.GetSQL();
			}
		}
	}
}

TEST_P(FTGenericApi, StopWordsWithMorphemes) {
	reindexer::FtFastConfig cfg = GetDefaultConfig();

	Init(cfg);
	Add("Шахматы из слоновой кости"sv);
	Add("Мат в эфире "sv);
	Add("Известняк"sv);
	Add("Известия"sv);
	Add("Изверг"sv);

	Add("Подобрал подосиновики, положил в лубочек"sv);
	Add("Подопытный кролик"sv);
	Add("Шла Саша по шоссе"sv);

	Add("Зайка серенький под елочкой скакал"sv);
	Add("За Альянс! (с)"sv);
	Add("Заноза в пальце"sv);

	Add("На западном фронте без перемен"sv);
	Add("Наливные яблочки"sv);
	Add("Нарком СССР"sv);

	CheckResults("*из*", {{"!Известняк!", ""}, {"!Известия!", ""}, {"!Изверг!", ""}}, false);
	CheckResults("из", {}, false);

	CheckResults("*под*", {{"!Подобрал подосиновики!, положил в лубочек", ""}, {"!Подопытный! кролик", ""}}, false);
	CheckResults("под", {}, false);

	CheckResults(
		"*за*", {{"!Зайка! серенький под елочкой скакал", ""}, {"!Заноза! в пальце", ""}, {"На !западном! фронте без перемен", ""}}, false);
	CheckResults("за", {}, false);

	CheckResults("*на*",
				 {
					 {"!Наливные! яблочки", ""},
					 {"!Нарком! СССР", ""},
				 },
				 false);
	CheckResults("на", {}, false);

	cfg.stopWords.clear();

	cfg.stopWords.insert({"на"});
	cfg.stopWords.insert({"мат", reindexer::StopWord::Type::Morpheme});

	SetFTConfig(cfg);

	CheckResults("*из*", {{"Шахматы !из! слоновой кости", ""}, {"!Известняк!", ""}, {"!Известия!", ""}, {"!Изверг!", ""}}, false);
	CheckResults("из", {{"Шахматы !из! слоновой кости", ""}}, false);

	CheckResults(
		"*под*",
		{{"!Подобрал подосиновики!, положил в лубочек", ""}, {"!Подопытный! кролик", ""}, {"Зайка серенький !под! елочкой скакал", ""}},
		false);
	CheckResults("под", {{"Зайка серенький !под! елочкой скакал", ""}}, false);

	CheckResults("*по*",
				 {{"Шла Саша !по! шоссе", ""},
				  {"!Подобрал подосиновики, положил! в лубочек", ""},
				  {"!Подопытный! кролик", ""},
				  {"Зайка серенький !под! елочкой скакал", ""}},
				 false);
	CheckResults("по~", {{"Шла Саша !по! шоссе", ""}, {"Зайка серенький !под! елочкой скакал", ""}}, false);
	CheckResults("по", {{"Шла Саша !по! шоссе", ""}}, false);

	CheckResults("*мат*", {{"!Шахматы! из слоновой кости", ""}}, false);
	CheckResults("мат", {}, false);

	CheckResults("*за*",
				 {{"!Зайка! серенький под елочкой скакал", ""},
				  {"!Заноза! в пальце", ""},
				  {"!За! Альянс! (с)", ""},
				  {"На !западном! фронте без перемен", ""}},
				 false);
	CheckResults("за", {{"!За! Альянс! (с)", ""}}, false);

	CheckResults("*на*", {}, false);
	CheckResults("на~", {}, false);
	CheckResults("на", {}, false);
}

TEST_P(FTGenericApi, FrisoTest) {
	auto splitter = reindexer::make_intrusive<reindexer::FrisoTextSplitter>();
	gason::JsonParser parser;
	auto frisoCorpusPath = reindexer::fs::JoinPath(std::string(kTestsDataPath), "friso/friso_corpus.json");
	std::ifstream t(frisoCorpusPath);
	ASSERT_TRUE(t.is_open());
	std::string json((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
	auto root = parser.Parse(reindexer::giftStr(json));
	auto task = splitter->CreateTask();
	for (auto& elem : root["result"]) {
		int id = elem["id"].As<int>();
		std::string str = elem["str"].As<std::string>();
		std::vector<std::string> res;
		for (auto& word : elem["words"]) {
			res.push_back(word.As<std::string>());
		}
		task->SetText(str);
		const std::vector<std::string_view>& words = task->GetResults();
		ASSERT_EQ(words.size(), res.size()) << "id=" << id << " " << PrintArray(words, "words ") << " " << PrintArray(res, "res ");
		for (size_t j = 0; j < words.size(); j++) {
			ASSERT_EQ(words[j], res[j]) << "id=" << id << " j=" << j << " splitWords[j]=" << words[j] << " res[j]=" << res[j];
		}
	}
}

TEST_P(FTGenericApi, FrisoTestSelect) {
	reindexer::FtFastConfig cfg = GetDefaultConfig();
	cfg.stopWords = {};
	cfg.splitterType = reindexer::FtFastConfig::Splitter::MMSegCN;
	Init(cfg);

	std::unordered_map<std::string, std::set<int>> index;
	std::vector<std::string> key;
	gason::JsonParser parser;
	auto frisoCorpusPath = reindexer::fs::JoinPath(std::string(kTestsDataPath), "friso/friso_corpus.json");
	std::ifstream t(frisoCorpusPath);
	ASSERT_TRUE(t.is_open());
	std::string json((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
	auto root = parser.Parse(reindexer::giftStr(json));
	for (auto& elem : root["result"]) {
		int id = elem["id"].As<int>();
		std::string str = elem["str"].As<std::string>();
		Add(str);
		std::vector<std::string> res;
		for (auto& word : elem["words"]) {
			std::string w = word.As<std::string>();
			index[w].insert(id);
		}
	}
	key.reserve(index.size());
	for (const auto& v : index) {
		key.emplace_back(v.first);
	}
	std::vector<std::string> testData = {
		"各方", "硫胺素", "求精", "花腔",	  "下去", "插花", "推演", "怎",	  "野",		 "6折",	 "象征",	   "work",	 "说明",
		"sv",	"詳",	  "u91",  "悬壶济世", "均有", "鬱",	  "探究", "含磷", "束手",	 "櫃",	 "中央社",	   "吸力",	 "oxy",
		"接受", "富豪",	  "療",	  "820",	  "授奖", "姻",	  "潛",	  "tei",  "限值",	 "西部", "pagination", "paiwan", "咖啡因",
		"俊逸", "假的",	  "pnh",  "245mm",	  "哭著", "谷底", "汆",	  "意表", "liuchiu", "殆",	 "mhw5500fw"};

	for (unsigned int i = 0; i < testData.size(); i++) {
		std::string findWord = testData[i];
		if (findWord == "~" || findWord == "*" || findWord == "-" || findWord == "<" || findWord == ">" || findWord == "," ||
			findWord == "」") {
			continue;
		}

		reindexer::QueryResults res = SimpleSelect("=" + findWord, false);
		std::set<int> ids;
		for (auto r : res) {
			auto item = r.GetItem();
			int val = item["id"].As<int>();
			ids.insert(val);
		}
		std::set<int>& wordDocs = index[findWord];
		std::vector<int> diff;
		std::set_symmetric_difference(ids.begin(), ids.end(), wordDocs.begin(), wordDocs.end(), std::back_inserter(diff));
		ASSERT_TRUE(diff.empty()) << "i=" << i << "findWord=" << findWord << "" << PrintArray(ids, "find ") << " "
								  << PrintArray(wordDocs, "reference ");
	}
}

TEST_P(FTGenericApi, FrisoTextPostprocess) {
	reindexer::FtFastConfig cfg = GetDefaultConfig();
	cfg.splitterType = reindexer::FtFastConfig::Splitter::MMSegCN;
	cfg.stopWords = {};
	cfg.maxAreasInDoc = 10;
	Init(cfg);
	Add("以下為聯絡我們訊息，歡迎大家瀏覽查閱。");
	// words of sentence
	//{"以下", "為", "聯", "絡", "我", "們", "訊", "息", "，",	"歡", "迎", "大家", "瀏", "覽", "查", "閱", "。"};

	{
		reindexer::QueryResults res = SimpleSelect("為", true);
		ASSERT_EQ(res.Count(), 1);
		auto item = res.begin().GetItem();
		std::string json = item["ft1"].As<std::string>();
		ASSERT_EQ(json, "以下!為!聯絡我們訊息，歡迎大家瀏覽查閱。");
	}
	{
		reindexer::QueryResults res = SimpleSelect("大家", true);
		ASSERT_EQ(res.Count(), 1);
		auto item = res.begin().GetItem();
		std::string json = item["ft1"].As<std::string>();
		ASSERT_EQ(json, "以下為聯絡我們訊息，歡迎!大家!瀏覽查閱。");
	}

	{
		reindexer::QueryResults res = SimpleSelect("大家 瀏", true);
		ASSERT_EQ(res.Count(), 1);
		auto item = res.begin().GetItem();
		std::string json = item["ft1"].As<std::string>();
		ASSERT_EQ(json, "以下為聯絡我們訊息，歡迎!大家瀏!覽查閱。");
	}

	{
		auto q{reindexer::Query("nm1").Where("ft3", CondEq, "大家").WithRank()};
		reindexer::QueryResults res;
		q.AddFunction("ft3 = snippet(<,>,2,2,'#','#')");
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 1);
		auto item = res.begin().GetItem();
		std::string json = item["ft1"].As<std::string>();
		ASSERT_EQ(json, "#歡迎<大家>瀏覽#");
	}

	{
		std::vector<std::pair<std::string, std::string>> tests = {{"為", "{[2,3]<b>為</b>!"}, {"大家", "{[12,14]<b>大家</b>!"}};
		for (const auto& t : tests) {
			auto q{reindexer::Query("nm1").Where("ft3", CondEq, t.first).WithRank()};
			reindexer::QueryResults res;
			q.AddFunction("ft3 = snippet_n('<b>','</b>',0,0,pre_delim='{',post_delim='!', with_area=1, left_bound='|',right_bound='|')");
			auto err = rt.reindexer->Select(q, res);
			EXPECT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(res.Count(), 1);
			auto item = res.begin().GetItem();
			std::string json = item["ft1"].As<std::string>();
			ASSERT_EQ(json, t.second);
		}
	}
	{
		auto q{reindexer::Query("nm1").Where("ft3", CondEq, "大家 查").WithRank()};
		reindexer::QueryResults res;
		q.AddFunction("ft3 = debug_rank()");
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(res.Count(), 1);
		auto item = res.begin().GetItem();
		std::string json = item["ft1"].As<std::string>();
		ASSERT_EQ(json,
				  "以下為聯絡我們訊息，歡迎{term_rank:90, term:大家, pattern:大家, bm25_norm:0.92, term_len_boost:1, position_rank:0.9889, "
				  "norm_dist:0, proc:100, full_match_boost:0} 大家瀏覽{term_rank:77, term:查, pattern:查, bm25_norm:0.92, "
				  "term_len_boost:0.85, position_rank:0.9886, norm_dist:0, proc:100, full_match_boost:0} 查閱。");
	}
}

INSTANTIATE_TEST_SUITE_P(, FTGenericApi,
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
