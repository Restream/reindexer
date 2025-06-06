#pragma once

#include "core/system_ns_names.h"
#include "reindexer_api.h"
#include "vendor/gason/gason.h"

class TransactionApi : public ReindexerApi {
public:
	struct DataRange {
		int from;
		int till;
		std::string data;
	};

	void SetUp() override {
		ReindexerApi::SetUp();
		OpenNamespace(*rt.reindexer);
	}

	void OpenNamespace(Reindexer& reindexer) {
		Error err = reindexer.OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(reindexer, default_namespace,
							   {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
								IndexDeclaration{kFieldData, "text", "string",
												 IndexOpts().SetConfig(IndexFastFT, R"xxx({"enable_warmup_on_ns_copy":false})xxx"), 0},
								IndexDeclaration{kFieldData1, "text", "string",
												 IndexOpts().SetConfig(IndexFastFT, R"xxx({"enable_warmup_on_ns_copy":true})xxx"), 0},
								IndexDeclaration{kFieldData2, "text", "string",
												 IndexOpts().SetConfig(IndexFastFT, R"xxx({"enable_warmup_on_ns_copy":true})xxx"), 0}});
	}

	Item MakeItem(Reindexer& reindexer, int id, const std::string& baseData) {
		Item item = reindexer.NewItem(default_namespace);
		if (item.Status().ok()) {
			item["id"] = id;
			item["data"] = baseData + "_" + std::to_string(id);
		}
		return item;
	}

	void AddDataToNsTx(Reindexer& reindexer, int from, int count, const std::string& data) {
		auto tx = reindexer.NewTransaction(default_namespace);
		for (int i = from; i < from + count; ++i) {
			auto err = tx.Insert(MakeItem(reindexer, i, data));
			ASSERT_TRUE(err.ok()) << err.what();
		}
		QueryResults result;
		Error err = reindexer.CommitTransaction(tx, result);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	int GetItemsCount(Reindexer& reindexer) {
		QueryResults qr;
		Error err = reindexer.Select(Query(default_namespace).CachedTotal().Limit(0), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		return qr.TotalCount();
	}

	void SelectData(Reindexer& reindexer, int fromMax, int tillMax) {
		int from = fromMax ? (random() % fromMax + 1) : 0;
		int till = random() % (tillMax - from) + (from + 1);
		QueryResults qr;
		Error err =
			reindexer.Select(Query(default_namespace).Where(kFieldId, CondGe, Variant(from)).Where(kFieldId, CondLe, Variant(till)), qr);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	static size_t GetPortion(size_t from, size_t expected, size_t upperBound) {
		return from + expected <= upperBound ? expected : upperBound - from;
	}

	reindexer::TxPerfStat GetTxPerfStats(Reindexer& rx, std::string_view ns) {
		QueryResults qr;
		auto err = rx.Select(reindexer::Query(reindexer::kPerfStatsNamespace).Where("name", CondEq, ns), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
		reindexer::WrSerializer ser;
		err = qr.begin().GetJSON(ser, false);
		EXPECT_TRUE(err.ok()) << err.what();

		gason::JsonParser parser;
		auto root = parser.Parse(ser.Slice());
		auto txNode = root["transactions"];
		EXPECT_TRUE(txNode.isObject());
		reindexer::TxPerfStat stats;
		stats.FromJSON(txNode);
		return stats;
	}

protected:
	const char* kFieldId = "id";
	const char* kFieldData = "data";
	const char* kFieldData1 = "data1";
	const char* kFieldData2 = "data2";
};
