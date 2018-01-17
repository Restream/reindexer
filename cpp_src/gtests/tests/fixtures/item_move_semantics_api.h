#pragma once

#include <gtest/gtest.h>
#include "reindexer_api.h"

using reindexer::Item;
using reindexer::ItemImpl;
using reindexer::Slice;
using std::to_string;
using std::vector;

class ItemMoveSemanticsApi : public ReindexerApi {
protected:
	const uint8_t upsertTimes = 2;
	const uint8_t fieldsCount = 2;
	const string idFieldName = "id";

	vector<string> jsons_;
	vector<unique_ptr<Item>> items_;

	void prepareJsons() {
		for (uint8_t i = 0; i < upsertTimes; i++) {
			string str = "{\"" + idFieldName + "\": " + to_string(i + 1);

			if (i % 2) {
				for (uint8_t j = fieldsCount; j > 0; j--) {
					str += ", \"f" + to_string(j) + "\": \"" + "testdata" + to_string(i + 1) + to_string(j) + "\"";
				}
			} else {
				for (uint8_t j = 0; j < fieldsCount; j++) {
					str += ", \"f" + to_string(j + 1) + "\": \"" + "testdata" + to_string(i + 1) + to_string(j + 1) + "\"";
				}
			}

			str += "}";

			jsons_.push_back(str);
		}
	}

	void prepareItemsFromJsons() {
		for (auto json : jsons_) {
			auto item = unique_ptr<Item>(reindexer->NewItem(default_namespace));
			Error err = item->FromJSON(Slice(json));
			ASSERT_TRUE(err.ok()) << err.what();

			items_.push_back(move(item));
		}
	}

	void upsertItems() {
		for (auto &&item : items_) {
			auto ritem = reinterpret_cast<ItemImpl *>(item.get());
			Error err = reindexer->Upsert(default_namespace, ritem);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	Item *getPreparedItemById(int id) {
		for (auto &&item : items_) {
			auto ritem = reinterpret_cast<ItemImpl *>(item.get());
			int itemId = KeyValue(ritem->GetField(idFieldName)).toInt();

			if (id == itemId) {
				return ritem;
			}
		}

		return nullptr;
	}

	void checkJsonsOfWrittenItems() {
		reindexer::QueryResults res;
		Error err = reindexer->Select("SELECT * FROM " + default_namespace, res);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(res.size() == upsertTimes) << to_string(upsertTimes) << " items were upserted, but got " << res.size();

		for (size_t i = 0; i < res.size(); ++i) {
			unique_ptr<Item> item(res.GetItem(static_cast<int>(i)));
			auto ritem = reinterpret_cast<ItemImpl *>(item.get());

			string jsonRead = ritem->GetJSON().ToString();

			int itemId = KeyValue(ritem->GetField(idFieldName)).toInt();

			auto prItem = getPreparedItemById(itemId);
			ASSERT_TRUE(prItem != nullptr) << "Unexpected item was read. Expected prepared item.";

			string jsonWritten = prItem->GetJSON().ToString();

			ASSERT_TRUE(jsonWritten == jsonRead) << "Json is read and json is written are not the same."
												 << "\nExpected: " << jsonRead << "\nGot:      " << jsonWritten;
		}
	}
};
