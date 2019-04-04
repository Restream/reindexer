#pragma once
#include <limits>
#include "reindexer_api.h"
#include "unordered_map"
using std::unordered_map;

class FTApi : public ReindexerApi {
public:
	void SetUp() {
		rt.reindexer.reset(new Reindexer);
		Error err;

		err = rt.reindexer->OpenNamespace("nm1");
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->OpenNamespace("nm2");
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(
			"nm1",
			{IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
			 IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
			 IndexDeclaration{
				 "ft1+ft2=ft3", "text", "composite",
				 IndexOpts().SetConfig(
					 R"xxx({"enable_translit": true,"enable_numbers_search": true,"enable_kb_layout": true,"merge_limit": 20000,"log_level": 5,"max_step_size": 100})xxx"),
				 0}});
		DefineNamespaceDataset(
			"nm2", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
					IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
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
		return make_pair(ft1, item.GetID());
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

private:
	struct Data {
		std::string ft1;
		std::string ft2;
	};
	int counter_ = 0;
};
