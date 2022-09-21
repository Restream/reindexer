#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <thread>
#include "reindexer_api.h"

class TransactionApi : public ReindexerApi {
public:
	struct DataRange {
		int from;
		int till;
		std::string data;
	};

	void SetUp() override {
		Error err = rt.reindexer->InitSystemNamespaces();
		ASSERT_TRUE(err.ok()) << err.what();
		OpenNamespace(*rt.reindexer);
	}

	void OpenNamespace(Reindexer& reindexer) {
		Error err = reindexer.OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			reindexer, default_namespace,
			{IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
			 IndexDeclaration{kFieldData, "text", "string", IndexOpts().SetConfig(R"xxx({"enable_warmup_on_ns_copy":false})xxx"), 0},
			 IndexDeclaration{kFieldData1, "text", "string", IndexOpts().SetConfig(R"xxx({"enable_warmup_on_ns_copy":true})xxx"), 0},
			 IndexDeclaration{kFieldData2, "text", "string", IndexOpts().SetConfig(R"xxx({"enable_warmup_on_ns_copy":true})xxx"), 0}});
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
			tx.Insert(MakeItem(reindexer, i, data));
		}
		QueryResults result;
		Error err = reindexer.CommitTransaction(tx, result);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	int GetItemsCount(Reindexer& reindexer) {
		QueryResults qr;
		Error err = reindexer.Select(Query(default_namespace), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		return qr.Count();
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

protected:
	const char* kFieldId = "id";
	const char* kFieldData = "data";
	const char* kFieldData1 = "data1";
	const char* kFieldData2 = "data2";
};

#endif	// TRANSACTION_H
