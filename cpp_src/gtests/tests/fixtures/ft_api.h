#pragma once
#include <limits>
#include "core/cjson/jsonbuilder.h"
#include "core/ft/config/ftfastconfig.h"
#include "reindexer_api.h"

class FTApi : public ReindexerApi {
public:
	void Init(const reindexer::FtFastConfig& ftCfg) {
		rt.reindexer.reset(new Reindexer);
		Error err;

		err = rt.reindexer->OpenNamespace("nm1");
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace("nm2");
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace("nm3");
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(
			"nm1", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		DefineNamespaceDataset(
			"nm2", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		DefineNamespaceDataset(
			"nm3", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft", "text", "string", IndexOpts(), 0}});
		SetFTConfig(ftCfg, "nm1", "ft3");
	}

	reindexer::FtFastConfig GetDefaultConfig(size_t fieldsCount = 2) {
		reindexer::FtFastConfig cfg(fieldsCount);
		cfg.enableNumbersSearch = true;
		cfg.logLevel = 5;
		cfg.mergeLimit = 20000;
		cfg.maxStepSize = 100;
		return cfg;
	}

	void SetFTConfig(const reindexer::FtFastConfig& ftCfg, const string& ns, const string& index) {
		const auto err = SetFTConfig(ftCfg, ns, index, {"ft1", "ft2"});
		EXPECT_TRUE(err.ok()) << err.what();
	}

	Error SetFTConfig(const reindexer::FtFastConfig& ftCfg, const string& ns, const string& index, const std::vector<std::string>& fields) {
		assert(!ftCfg.fieldsCfg.empty());
		assert(ftCfg.fieldsCfg.size() >= fields.size());
		reindexer::WrSerializer wrser;
		reindexer::JsonBuilder cfgBuilder(wrser);
		cfgBuilder.Put("enable_translit", ftCfg.enableTranslit);
		cfgBuilder.Put("enable_numbers_search", ftCfg.enableNumbersSearch);
		cfgBuilder.Put("enable_kb_layout", ftCfg.enableKbLayout);
		cfgBuilder.Put("merge_limit", ftCfg.mergeLimit);
		cfgBuilder.Put("log_level", ftCfg.logLevel);
		cfgBuilder.Put("max_step_size", ftCfg.maxStepSize);
		cfgBuilder.Put("full_match_boost", ftCfg.fullMatchBoost);
		cfgBuilder.Put("extra_word_symbols", ftCfg.extraWordSymbols);
		cfgBuilder.Put("partial_match_decrease", ftCfg.partialMatchDecrease);
		bool defaultPositionBoost{true};
		bool defaultPositionWeight{true};
		for (size_t i = 1; i < ftCfg.fieldsCfg.size(); ++i) {
			if (ftCfg.fieldsCfg[0].positionBoost != ftCfg.fieldsCfg[i].positionBoost) defaultPositionBoost = false;
			if (ftCfg.fieldsCfg[0].positionWeight != ftCfg.fieldsCfg[i].positionWeight) defaultPositionWeight = false;
		}
		if (defaultPositionBoost) {
			cfgBuilder.Put("position_boost", ftCfg.fieldsCfg[0].positionBoost);
		}
		if (defaultPositionWeight) {
			cfgBuilder.Put("position_weight", ftCfg.fieldsCfg[0].positionWeight);
		}
		{
			auto synonymsNode = cfgBuilder.Array("synonyms");
			for (auto& synonym : ftCfg.synonyms) {
				auto synonymObj = synonymsNode.Object();
				{
					auto tokensNode = synonymObj.Array("tokens");
					for (auto& token : synonym.tokens) tokensNode.Put(nullptr, token);
				}
				{
					auto alternativesNode = synonymObj.Array("alternatives");
					for (auto& token : synonym.alternatives) alternativesNode.Put(nullptr, token);
				}
			}
		}
		if (!defaultPositionWeight || !defaultPositionBoost) {
			auto fieldsNode = cfgBuilder.Array("fields");
			for (size_t i = 0; i < fields.size(); ++i) {
				auto fldNode = fieldsNode.Object();
				fldNode.Put("field_name", fields[i]);
				if (!defaultPositionBoost) {
					fldNode.Put("position_boost", ftCfg.fieldsCfg[i].positionBoost);
				}
				if (!defaultPositionWeight) {
					fldNode.Put("position_weight", ftCfg.fieldsCfg[i].positionWeight);
				}
			}
		}
		cfgBuilder.End();
		vector<reindexer::NamespaceDef> nses;
		rt.reindexer->EnumNamespaces(nses, reindexer::EnumNamespacesOpts().WithFilter(ns));
		auto it = std::find_if(nses[0].indexes.begin(), nses[0].indexes.end(),
							   [&index](const reindexer::IndexDef& idef) { return idef.name_ == index; });
		it->opts_.SetConfig(wrser.c_str());

		return rt.reindexer->UpdateIndex(ns, *it);
	}

	void FillData(int64_t count) {
		for (int i = 0; i < count; ++i) {
			Item item = NewItem(default_namespace);
			item["id"] = counter_;
			auto ft1 = RandString();

			counter_++;

			item["ft1"] = ft1;

			Upsert(default_namespace, item);
			Commit(default_namespace);
		}
	}
	void Add(const std::string& ft1, const std::string& ft2) {
		Add("nm1", ft1, ft2);
		Add("nm2", ft1, ft2);
	}

	std::pair<string, int> Add(const std::string& ft1) {
		Item item = NewItem("nm1");
		item["id"] = counter_;
		counter_++;
		item["ft1"] = ft1;

		Upsert("nm1", item);
		Commit("nm1");
		return make_pair(ft1, counter_ - 1);
	}
	void Add(const std::string& ns, const std::string& ft1, const std::string& ft2) {
		Item item = NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = ft1;
		item["ft2"] = ft2;

		Upsert(ns, item);
		Commit(ns);
	}

	void AddInBothFields(const std::string& w1, const std::string& w2) {
		AddInBothFields("nm1", w1, w2);
		AddInBothFields("nm2", w1, w2);
	}

	void AddInBothFields(const std::string& ns, const std::string& w1, const std::string& w2) {
		Item item = NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = w1;
		item["ft2"] = w1;
		Upsert(ns, item);

		item = NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = w2;
		item["ft2"] = w2;
		Upsert(ns, item);

		Commit(ns);
	}

	QueryResults SimpleSelect(string word) {
		Query qr = std::move(Query("nm1").Where("ft3", CondEq, word));
		QueryResults res;
		qr.AddFunction("ft3 = highlight(!,!)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}

	void Delete(int id) {
		Item item = NewItem("nm1");
		item["id"] = id;

		this->rt.reindexer->Delete("nm1", item);
	}
	QueryResults SimpleCompositeSelect(string word) {
		Query qr = std::move(Query("nm1").Where("ft3", CondEq, word));
		QueryResults res;
		Query mqr = std::move(Query("nm2").Where("ft3", CondEq, word));
		mqr.AddFunction("ft1 = snippet(<b>,\"\"</b>,3,2,,d)");

		qr.mergeQueries_.emplace_back(std::move(mqr));
		qr.AddFunction("ft3 = highlight(<b>,</b>)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	QueryResults CompositeSelectField(const string& field, string word) {
		word = '@' + field + ' ' + word;
		Query qr = std::move(Query("nm1").Where("ft3", CondEq, word));
		QueryResults res;
		Query mqr = std::move(Query("nm2").Where("ft3", CondEq, word));
		mqr.AddFunction(field + " = snippet(<b>,\"\"</b>,3,2,,d)");

		qr.mergeQueries_.push_back(std::move(mqr));
		qr.AddFunction(field + " = highlight(<b>,</b>)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	QueryResults StressSelect(string word) {
		Query qr = std::move(Query("nm1").Where("ft3", CondEq, word));
		QueryResults res;
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	std::vector<std::string> CreateAllPermutatedQueries(const std::string& queryStart, std::vector<std::string> words,
														const std::string& queryEnd, const std::string& sep = " ") {
		std::vector<std::pair<size_t, std::string>> indexedWords;
		indexedWords.reserve(words.size());
		for (std::string& w : words) {
			indexedWords.emplace_back(indexedWords.size(), std::move(w));
		}
		std::vector<std::string> result;
		do {
			result.push_back(queryStart);
			std::string& query = result.back();
			for (auto it = indexedWords.cbegin(); it != indexedWords.cend(); ++it) {
				if (it != indexedWords.cbegin()) query += sep;
				query += it->second;
			}
			query += queryEnd;
		} while (std::next_permutation(
			indexedWords.begin(), indexedWords.end(),
			[](const std::pair<size_t, std::string>& a, const std::pair<size_t, std::string>& b) { return a.first < b.first; }));
		return result;
	}
	void CheckAllPermutations(const std::string& queryStart, std::vector<std::string> words, const std::string& queryEnd,
							  const std::vector<std::pair<std::string, std::string>>& expectedResults, bool withOrder = false,
							  const std::string& sep = " ") {
		for (const auto& query : CreateAllPermutatedQueries(queryStart, words, queryEnd, sep)) {
			CheckResults(query, expectedResults, withOrder);
		}
	}

	void CheckResults(const std::string& query, std::vector<std::pair<std::string, std::string>> expectedResults, bool withOrder) {
		const auto qr = SimpleSelect(query);
		EXPECT_EQ(qr.Count(), expectedResults.size()) << "Query: " << query;
		for (auto itRes : qr) {
			const auto item = itRes.GetItem();
			const auto it =
				std::find_if(expectedResults.begin(), expectedResults.end(), [&item](const std::pair<std::string, std::string>& p) {
					return p.first == item["ft1"].As<string>() && p.second == item["ft2"].As<string>();
				});
			if (it == expectedResults.end()) {
				ADD_FAILURE() << "Found not expected: \"" << item["ft1"].As<string>() << "\" \"" << item["ft2"].As<string>()
							  << "\"\nQuery: " << query;
			} else {
				if (withOrder) {
					EXPECT_EQ(it, expectedResults.begin()) << "Found not in order: \"" << item["ft1"].As<string>() << "\" \""
														   << item["ft2"].As<string>() << "\"\nQuery: " << query;
				}
				expectedResults.erase(it);
			}
		}
		for (const auto& expected : expectedResults) {
			ADD_FAILURE() << "Not found: \"" << expected.first << "\" \"" << expected.second << "\"\nQuery: " << query;
		}
		if (!expectedResults.empty()) {
			ADD_FAILURE() << "Query: " << query;
		}
	}
	FTApi() {}

protected:
	struct Data {
		std::string ft1;
		std::string ft2;
	};
	struct FTDSLQueryParams {
		reindexer::fast_hash_map<string, int> fields;
		reindexer::fast_hash_set<string, reindexer::hash_str, reindexer::equal_str> stopWords;
		string extraWordSymbols = "-/+";
	};
	int counter_ = 0;
};
