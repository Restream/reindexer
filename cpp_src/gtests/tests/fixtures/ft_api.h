#pragma once
#include <limits>
#include "reindexer_api.h"
#include "unordered_map"
using std::unordered_map;

class FTApi : public ReindexerApi {
public:
	void SetUp() {
		reindexer.reset(new Reindexer);
		CreateNamespace("nm1");
		CreateNamespace("nm2");

		//		IndexOpts opts{false, true, false};

		DefineNamespaceDataset(
			"nm1",
			{IndexDeclaration{"id", "hash", "int", IndexOpts().PK()}, IndexDeclaration{"ft1", "text", "string", IndexOpts()},
			 IndexDeclaration{"ft2", "text", "string", IndexOpts()}, IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts()}});
		DefineNamespaceDataset(
			"nm2",
			{IndexDeclaration{"id", "hash", "int", IndexOpts().PK()}, IndexDeclaration{"ft1", "text", "string", IndexOpts()},
			 IndexDeclaration{"ft2", "text", "string", IndexOpts()}, IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts()}});
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
	void Add(const std::string& ns, const std::string& ft1, const std::string& ft2) {
		Item item = NewItem(ns);
		item["id"] = counter_;
		counter_++;
		item["ft1"] = ft1;
		item["ft2"] = ft2;

		Upsert(ns, item);
		Commit(ns);
	}
	QueryResults SimpleCompositeSelect(string word) {
		Query qr = Query("nm1").Where("ft3", CondEq, word);
		QueryResults res;
		Query mqr = Query("nm2").Where("ft3", CondEq, word);
		mqr.selectFunctions_.push_back("ft1 = snippet(<b>,\"\"</b>,3,2,,d)");

		qr.mergeQueries_.push_back(mqr);
		qr.selectFunctions_.push_back("ft3 = highlight(<b>,</b>)");
		reindexer->Select(qr, res);

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
