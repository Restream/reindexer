#pragma once

#include "core/system_ns_names.h"
#include "reindexer_api.h"
#include "vendor/gason/gason.h"

class [[nodiscard]] TransactionApi : public ReindexerApi {
public:
	struct [[nodiscard]] DataRange {
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
							   {IndexDeclaration{kFieldId, "tree", "int", IndexOpts().PK(), 0},
								IndexDeclaration{kFieldData, "text", "string",
												 IndexOpts().SetConfig(IndexFastFT, R"xxx({"enable_warmup_on_ns_copy":false})xxx"), 0},
								IndexDeclaration{kFieldData1, "text", "string",
												 IndexOpts().SetConfig(IndexFastFT, R"xxx({"enable_warmup_on_ns_copy":true})xxx"), 0},
								IndexDeclaration{kFieldData2, "text", "string",
												 IndexOpts().SetConfig(IndexFastFT, R"xxx({"enable_warmup_on_ns_copy":true})xxx"), 0}});
	}

	void SetTxCopyConfigs([[maybe_unused]] Reindexer& reindexer) {
#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_TSAN)
		QueryResults qr;
		auto err = reindexer.Update(Query(reindexer::kConfigNamespace)
										.Set("namespaces[*].tx_size_to_always_copy", 8000)
										.Set("namespaces[*].start_copy_policy_tx_size", 2000)
										.Where("type", CondEq, "namespaces"),
									qr);
		ASSERT_TRUE(err.ok()) << err.what();
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_TSAN)
	}

	Item MakeItem(Reindexer& reindexer, int id, const std::string& baseData) {
		Item item = reindexer.NewItem(default_namespace);
		if (item.Status().ok()) {
			item[kFieldId] = id;
			item[kFieldData] = fmt::format("{}_{}_{}", kFieldData, baseData, id);
			item[kFieldData1] = fmt::format("{}_{}_{}", kFieldData1, baseData, id);
			item[kFieldData2] = fmt::format("{}_{}_{}", kFieldData2, baseData, id);
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

	void SelectData(Reindexer& reindexer, int tillMax, int limit) {
		int from = tillMax ? (random() % tillMax) : 0;
		int till = (tillMax > from) ? (random() % (tillMax - from) + (from + 1)) : (from + 1);
		QueryResults qr;
		const auto q = Query(default_namespace).Where(kFieldId, CondGe, Variant(from)).Where(kFieldId, CondLe, Variant(till)).Limit(limit);
		Error err = reindexer.Select(q, qr);
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
