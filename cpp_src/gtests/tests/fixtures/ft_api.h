#pragma once
#include <limits>
#include "reindexer_api.h"
#include "unordered_map"
using std::unordered_map;

class FTApi : public ReindexerApi {
public:
	void SetUp() {
		reindexer.reset(new Reindexer);
		CreateNamespace(default_namespace);
		//		IndexOpts opts{false, true, false};

		DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK()},
												   IndexDeclaration{"ft1", "fulltext", "string", IndexOpts()}});
	}

	void FillData(int64_t count) {
		for (int i = 0; i < count; ++i) {
			auto item = AddData(default_namespace, "id", counter_);
			auto ft1 = RandString();

			counter_++;

			AddData(default_namespace, "ft1", ft1, item);

			Upsert(default_namespace, item);
			Commit(default_namespace);
		}
	}
	void Add(const std::string& ft1) { Add(default_namespace, ft1); }
	void Add(const std::string& ns, const std::string& ft1) {
		auto item = AddData(ns, "id", counter_);
		counter_++;
		AddData(ns, "ft1", ft1, item);

		Upsert(ns, item);
		Commit(ns);
	}
	QueryResults SimpleCompositeSelect(string word) {
		Query qr, qr1;
		FillQuery(default_namespace, word, "ft1", OpAnd, CondEq, qr);
		QueryResults res, res1;
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
