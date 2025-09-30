#pragma once

#include <gtest/gtest.h>
#include <map>
#include "gason/gason.h"
#include "reindexer_api.h"

class [[nodiscard]] ItemMoveSemanticsApi : public ReindexerApi {
protected:
	const std::string pkField = "bookid";
	const int32_t itemsCount = 100000;
	const char* jsonPattern = "{\"bookid\":%d,\"title\":\"title\",\"pages\":200,\"price\":299,\"genreid_fk\":3,\"authorid_fk\":10}";
	std::map<int, Item> items_;

	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
		rt.AddIndex(default_namespace, {"bookid", "hash", "int", IndexOpts().PK()});
		rt.AddIndex(default_namespace, {"title", "text", "string", IndexOpts()});
		rt.AddIndex(default_namespace, {"pages", "hash", "int", IndexOpts()});
		rt.AddIndex(default_namespace, {"price", "hash", "int", IndexOpts()});
		rt.AddIndex(default_namespace, {"genreid_fk", "hash", "int", IndexOpts()});
		rt.AddIndex(default_namespace, {"authorid_fk", "hash", "int", IndexOpts()});
	}

	void prepareItems() {
		const size_t bufSize = 4096;
		char buf[bufSize];
		for (int i = 1; i < itemsCount; ++i) {
			int id = i;
			snprintf(&buf[0], bufSize, jsonPattern, id);
			Item item(rt.NewItem(default_namespace));
			auto err = item.FromJSON(buf);
			ASSERT_TRUE(err.ok()) << err.what();
			items_[id] = std::move(item);
		}
	}

	void verifyAndUpsertItems() {
		for (auto& pair : items_) {
			auto&& item = pair.second;
			rt.Upsert(default_namespace, item);
			gason::JsonParser parser;
			ASSERT_NO_THROW(parser.Parse(item.GetJSON()));
		}
	}

	Item getItemById(int id) {
		auto itItem = items_.find(id);
		if (itItem == items_.end()) {
			return Item();
		}
		auto&& item = itItem->second;
		return std::move(item);
	}

	void verifyJsonsOfUpsertedItems() {
		auto qres = rt.ExecSQL("SELECT * FROM " + default_namespace);
		EXPECT_EQ(int(qres.Count()), (itemsCount - 1));

		for (auto it : qres) {
			Item item(it.GetItem(false));
			std::string_view jsonRead(item.GetJSON());

			int itemId = item[pkField].Get<int>();
			auto originalItem = getItemById(itemId);
			ASSERT_TRUE(!!originalItem) << "No item for this id: " << itemId;

			std::string_view originalJson = originalItem.GetJSON();
			ASSERT_EQ(originalJson, jsonRead) << "Inserted and selected items' jsons are different";
		}
	}
};
