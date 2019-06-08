#pragma once

#include "reindexer_api.h"

class CollateCustomModeAPI : public ReindexerApi {
protected:
	void PrepareNs(std::shared_ptr<Reindexer> reindexer, const string& nsName, const string& sortOrder, const vector<string>& sourceTable) {
		auto err = reindexer->OpenNamespace(nsName, StorageOpts().Enabled(false));
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->AddIndex(nsName, {kFieldID, "hash", "int", IndexOpts().PK()});
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->AddIndex(nsName, {kFieldName, "hash", "string", IndexOpts(sortOrder).SetCollateMode(CollateCustom)});
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->Commit(nsName);
		EXPECT_TRUE(err.ok()) << err.what();

		for (size_t i = 0; i < sourceTable.size(); ++i) {
			Item item(reindexer->NewItem(nsName));
			EXPECT_TRUE(!!item);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			item[kFieldID] = static_cast<int>(i);
			item[kFieldName] = sourceTable[i];

			err = rt.reindexer->Upsert(nsName, item);
			EXPECT_TRUE(err.ok()) << err.what();
		}

		err = reindexer->Commit(nsName);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void SortByName(QueryResults& qr) {
		Query query = Query(default_namespace).Sort(kFieldName, false);
		Error err = rt.reindexer->Select(query, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void CompareResults(const QueryResults& qr, const vector<string>& sortedTable) {
		ASSERT_TRUE(qr.Count() == sortedTable.size());
		for (size_t i = 0; i < qr.Count(); ++i) {
			Item item = (qr.begin() + i).GetItem();

			string gotten = item["name"].As<string>();
			size_t l1 = gotten.length();
			size_t l2 = sortedTable[i].length();

			ASSERT_TRUE(l1 == l2);
			ASSERT_TRUE(memcmp(gotten.c_str(), sortedTable[i].c_str(), l1) == 0);
		}
	}

	const char* kFieldID = "id";
	const char* kFieldName = "name";
};
