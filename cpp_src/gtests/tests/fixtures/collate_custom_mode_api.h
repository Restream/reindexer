#pragma once

#include "reindexer_api.h"

class CollateCustomModeAPI : public ReindexerApi {
protected:
	void PrepareNs(const std::shared_ptr<Reindexer>& reindexer, const std::string& nsName, const std::string& sortOrder,
				   const std::vector<std::string_view>& sourceTable) {
		auto err = reindexer->OpenNamespace(nsName, StorageOpts().Enabled(false));
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->AddIndex(nsName, {kFieldID, "hash", "int", IndexOpts().PK()});
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->AddIndex(nsName, {kFieldName, "hash", "string", IndexOpts(sortOrder).SetCollateMode(CollateCustom)});
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
	}

	void SortByName(QueryResults& qr) {
		Query query{default_namespace};
		query.Sort(kFieldName, false);
		Error err = rt.reindexer->Select(query, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	void CompareResults(const QueryResults& qr, const std::vector<std::string_view>& sortedTable) {
		ASSERT_TRUE(qr.Count() == sortedTable.size());
		for (size_t i = 0; i < qr.Count(); ++i) {
			Item item = (qr.begin() + i).GetItem(false);

			std::string got = item["name"].As<std::string>();
			ASSERT_EQ(std::string_view(got), sortedTable[i]);
		}
	}

	const char* kFieldID = "id";
	const char* kFieldName = "name";
};
