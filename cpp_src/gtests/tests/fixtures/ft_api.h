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

		DefineNamespaceDataset(
			"nm1", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		DefineNamespaceDataset(
			"nm2", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		SetFTConfig(ftCfg, "nm1", "ft3");
	}

	reindexer::FtFastConfig GetDefaultConfig() {
		reindexer::FtFastConfig cfg;
		cfg.enableNumbersSearch = true;
		cfg.logLevel = 5;
		cfg.mergeLimit = 20000;
		cfg.maxStepSize = 100;
		return cfg;
	}

	void SetFTConfig(const reindexer::FtFastConfig& ftCfg, const string& ns, const string& index) {
		reindexer::WrSerializer wrser;
		reindexer::JsonBuilder cfgBuilder(wrser);
		cfgBuilder.Put("enable_translit", ftCfg.enableTranslit);
		cfgBuilder.Put("enable_numbers_search", ftCfg.enableNumbersSearch);
		cfgBuilder.Put("enable_kb_layout", ftCfg.enableKbLayout);
		cfgBuilder.Put("merge_limit", ftCfg.mergeLimit);
		cfgBuilder.Put("log_level", ftCfg.logLevel);
		cfgBuilder.Put("max_step_size", ftCfg.maxStepSize);
		cfgBuilder.Put("full_match_boost", ftCfg.fullMatchBoost);
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
		cfgBuilder.End();
		vector<reindexer::NamespaceDef> nses;
		rt.reindexer->EnumNamespaces(nses, reindexer::EnumNamespacesOpts().WithFilter(ns));
		auto it = std::find_if(nses[0].indexes.begin(), nses[0].indexes.end(),
							   [&index](const reindexer::IndexDef& idef) { return idef.name_ == index; });
		it->opts_.SetConfig(wrser.c_str());

		auto err = rt.reindexer->UpdateIndex(ns, *it);
		EXPECT_TRUE(err.ok()) << err.what();
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
	QueryResults SimpleSelect(string word) {
		Query qr = Query("nm1").Where("ft3", CondEq, word);
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
		Query qr = Query("nm1").Where("ft3", CondEq, word);
		QueryResults res;
		Query mqr = Query("nm2").Where("ft3", CondEq, word);
		mqr.AddFunction("ft1 = snippet(<b>,\"\"</b>,3,2,,d)");

		qr.mergeQueries_.push_back(mqr);
		qr.AddFunction("ft3 = highlight(<b>,</b>)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	QueryResults StressSelect(string word) {
		Query qr = Query("nm1").Where("ft3", CondEq, word);
		QueryResults res;
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
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
