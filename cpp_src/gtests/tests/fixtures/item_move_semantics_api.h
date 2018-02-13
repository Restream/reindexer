#pragma once

#include <gtest/gtest.h>
#include <map>
#include "reindexer_api.h"

using reindexer::Item;
using reindexer::ItemImpl;
using reindexer::Slice;
using std::to_string;
using std::vector;

class ItemMoveSemanticsApi : public ReindexerApi {
protected:
	const string pkField = "bookid";
	const int32_t itemsCount = 100000;
	const char *jsonPattern = "{\"bookid\":%d,\"title\":\"title\",\"pages\":200,\"price\":299,\"genreid_fk\":3,\"authorid_fk\":10}";
	map<int, unique_ptr<Item>> items_;
	vector<string> jsons_;

	void SetUp() override {
		ReindexerApi::SetUp();
		reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
		reindexer->AddIndex(default_namespace, {"bookid", "bookid", "hash", "int", IndexOpts().PK()});
		reindexer->AddIndex(default_namespace, {"title", "title", "text", "string", IndexOpts()});
		reindexer->AddIndex(default_namespace, {"pages", "pages", "hash", "int", IndexOpts().PK()});
		reindexer->AddIndex(default_namespace, {"price", "price", "hash", "int", IndexOpts().PK()});
		reindexer->AddIndex(default_namespace, {"genreid_fk", "genreid_fk", "hash", "int", IndexOpts().PK()});
		reindexer->AddIndex(default_namespace, {"authorid_fk", "authorid_fk", "hash", "int", IndexOpts().PK()});
		reindexer->Commit(default_namespace);
	}

	void prepareItems() {
		const size_t bufSize = 4096;
		char buf[bufSize];
		for (int i = 1; i < itemsCount; ++i) {
			int id = i;
			snprintf(&buf[0], bufSize, jsonPattern, id);
			unique_ptr<reindexer::Item> item(reindexer->NewItem(default_namespace));
			jsons_.push_back(buf);
			Error err = item->FromJSON(jsons_.back());
			ASSERT_TRUE(err.ok()) << err.what();
			items_[id] = std::move(item);
		}
	}

	void verifyAndUpsertItems() {
		char *endptr = nullptr;
		JsonValue jsonValue;
		JsonAllocator jsonAllocator;
		for (auto &pair : items_) {
			auto &&item = pair.second;
			Error err = reindexer->Upsert(default_namespace, item.get());
			ASSERT_TRUE(err.ok()) << err.what();
			reindexer::Slice jsonSlice = item->GetJSON();
			int status = jsonParse(const_cast<char *>(jsonSlice.data()), &endptr, &jsonValue, jsonAllocator);
			ASSERT_TRUE(status == JSON_OK);
		}
		reindexer->Commit(default_namespace);
	}

	Item *getItemById(int id) {
		auto itItem = items_.find(id);
		if (itItem == items_.end()) {
			return nullptr;
		}
		auto &&item = itItem->second;
		return item.get();
	}

	void verifyJsonsOfUpsertedItems() {
		reindexer::QueryResults qres;
		Error err = reindexer->Select("SELECT * FROM " + default_namespace, qres);

		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_TRUE(int(qres.size()) == (itemsCount - 1))
			<< to_string(itemsCount - 1) << " items upserted, but selected only " << qres.size();

		for (size_t i = 0; i < qres.size(); ++i) {
			unique_ptr<Item> item(qres.GetItem(static_cast<int>(i)));
			string jsonRead(item->GetJSON().ToString());

			int itemId = KeyValue(item->GetField(pkField)).toInt();
			auto originalItem = getItemById(itemId);
			ASSERT_TRUE(originalItem != nullptr) << "No item for this id: " << to_string(itemId);

			string originalJson = originalItem->GetJSON().ToString();
			ASSERT_TRUE(originalJson == jsonRead) << "Inserted and selected items' jsons are different."
												  << "\nExpected: " << jsonRead << "\nGot:" << originalJson;
		}
	}
};
