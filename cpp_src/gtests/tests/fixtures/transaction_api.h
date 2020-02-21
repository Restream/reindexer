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
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldData, "text", "string", IndexOpts(), 0}});
	}

	Item MakeItem(int id, const std::string& baseData) {
		Item item = rt.reindexer->NewItem(default_namespace);
		if (item.Status().ok()) {
			item["id"] = id;
			item["data"] = baseData + "_" + std::to_string(id);
		}
		return item;
	}

	void AddDataToNsTx(int from, int count, const std::string& data) {
		auto tx = rt.reindexer->NewTransaction(default_namespace);
		for (int i = from; i < from + count; ++i) {
			tx.Insert(MakeItem(i, data));
		}
		QueryResults result;
		Error err = rt.reindexer->CommitTransaction(tx, result);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	int GetItemsCount() {
		QueryResults qr;
		Error err = rt.reindexer->Select(Query(default_namespace), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		return qr.Count();
	}

	void SelectData(int fromMax, int tillMax) {
		int from = fromMax ? (random() % fromMax + 1) : 0;
		int till = random() % (tillMax - from) + (from + 1);
		QueryResults qr;
		Error err = rt.reindexer->Select(
			Query(default_namespace).Where(kFieldId, CondGe, Variant(from)).Where(kFieldId, CondLe, Variant(till)), qr);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	size_t GetPortion(size_t from, size_t expected, size_t upperBound) {
		return from + expected <= upperBound ? expected : upperBound - from;
	}

protected:
	const char* kFieldId = "id";
	const char* kFieldData = "data";
};

#endif	// TRANSACTION_H
