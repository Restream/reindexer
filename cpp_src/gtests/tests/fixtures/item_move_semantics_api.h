#pragma once

#include <gtest/gtest.h>
#include <deque>
#include <map>
#include "gason/gason.h"
#include "reindexer_api.h"

using reindexer::Item;
using reindexer::ItemImpl;

class ItemMoveSemanticsApi : public ReindexerApi {
protected:
	const std::string pkField = "bookid";
	const int32_t itemsCount = 100000;
	const char *jsonPattern = "{\"bookid\":%d,\"title\":\"title\",\"pages\":200,\"price\":299,\"genreid_fk\":3,\"authorid_fk\":10}";
	std::map<int, Item> items_;

	void SetUp() override {
		ReindexerApi::SetUp();
		rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
		rt.reindexer->AddIndex(default_namespace, {"bookid", "hash", "int", IndexOpts().PK()});
		rt.reindexer->AddIndex(default_namespace, {"title", "text", "string", IndexOpts()});
		rt.reindexer->AddIndex(default_namespace, {"pages", "hash", "int", IndexOpts().PK()});
		rt.reindexer->AddIndex(default_namespace, {"price", "hash", "int", IndexOpts().PK()});
		rt.reindexer->AddIndex(default_namespace, {"genreid_fk", "hash", "int", IndexOpts().PK()});
		rt.reindexer->AddIndex(default_namespace, {"authorid_fk", "hash", "int", IndexOpts().PK()});
		rt.reindexer->Commit(default_namespace);
	}

	void prepareItems() {
		const size_t bufSize = 4096;
		char buf[bufSize];
		for (int i = 1; i < itemsCount; ++i) {
			int id = i;
			snprintf(&buf[0], bufSize, jsonPattern, id);
			Item item(rt.reindexer->NewItem(default_namespace));
			Error err = item.FromJSON(buf);
			ASSERT_TRUE(err.ok()) << err.what();
			items_[id] = std::move(item);
		}
	}

	void verifyAndUpsertItems() {
		for (auto &pair : items_) {
			auto &&item = pair.second;
			Error err = rt.reindexer->Upsert(default_namespace, item);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_NO_THROW(gason::JsonParser().Parse(item.GetJSON()));
		}
		rt.reindexer->Commit(default_namespace);
	}

	Item getItemById(int id) {
		auto itItem = items_.find(id);
		if (itItem == items_.end()) {
			return Item();
		}
		auto &&item = itItem->second;
		return std::move(item);
	}

	void verifyJsonsOfUpsertedItems() {
		reindexer::QueryResults qres;
		Error err = rt.reindexer->Select("SELECT * FROM " + default_namespace, qres);

		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_TRUE(int(qres.Count()) == (itemsCount - 1)) << (itemsCount - 1) << " items upserted, but selected only " << qres.Count();

		for (auto it : qres) {
			Item item(it.GetItem(false));
			std::string_view jsonRead(item.GetJSON());

			int itemId = item[pkField].Get<int>();
			auto originalItem = getItemById(itemId);
			ASSERT_TRUE(!!originalItem) << "No item for this id: " << itemId;

			std::string_view originalJson = originalItem.GetJSON();
			ASSERT_TRUE(originalJson == jsonRead) << "Inserted and selected items' jsons are different."
												  << "\nExpected: " << jsonRead << "\nGot:" << originalJson;
		}
	}
};
