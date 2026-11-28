#include "reindexer_storage_api.h"

#include <algorithm>
#include <tuple>
#include "core/system_ns_names.h"
#include "core/type_consts.h"

namespace reindexer_tests {

using reindexer::IndexOpts;

TEST_F(ReindexerStorageApi, MetaIndexTest) {
	std::string readMeta;
	const std::string emptyValue;
	const std::string unsettedKey = "unexpected#meta#key##name";
	const std::vector<std::pair<std::string, std::string>> meta = {{"key1", "data1"}, {"key2", "data2"}};

	for (const auto& key : EnumMeta()) {
		DeleteMeta(key);
	}

	auto err = rt.reindexer->GetMeta(default_namespace, emptyValue, readMeta);
	ASSERT_FALSE(err.ok()) << err.what();
	err = rt.reindexer->PutMeta(default_namespace, emptyValue, emptyValue);
	ASSERT_FALSE(err.ok()) << err.what();
	err = rt.reindexer->DeleteMeta(default_namespace, emptyValue);
	ASSERT_FALSE(err.ok()) << err.what();

	ASSERT_EQ(GetMeta(meta.front().first), emptyValue);
	ASSERT_EQ(EnumMeta(), std::vector<std::string>{});
	DeleteMeta(unsettedKey);

	for (const auto& item : meta) {
		PutMeta(item.first, item.second);
	}

	ReopenNamespace();

	auto readKeys = EnumMeta();
	ASSERT_EQ(readKeys.size(), meta.size());

	auto data = GetShardedMeta("key1");
	ASSERT_EQ(data.size(), 1);
	ASSERT_EQ(data[0].data, "data1");
	ASSERT_EQ(data[0].shardId, ShardingKeyType::NotSetShard);

	for (const auto& key : readKeys) {
		auto it = std::find_if(meta.begin(), meta.end(), [&key](const auto& elem) { return elem.first == key; });
		ASSERT_TRUE(it != meta.end());
		ASSERT_EQ(GetMeta(key), it != meta.end() ? it->second : unsettedKey);
	}

	DeleteMeta(unsettedKey);
	for (const auto& item : meta) {
		DeleteMeta(item.first);
		ASSERT_EQ(GetMeta(item.first), emptyValue);
		PutMeta(item.first, item.second);
		DeleteMeta(item.first);
	}
	ASSERT_EQ(EnumMeta(), std::vector<std::string>{});
}

TEST_F(ReindexerStorageApi, MetaIndexTxPersistence) {
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	auto tx = rt.NewTransaction(default_namespace);
	auto err = tx.PutMeta("txkey", "txval");
	ASSERT_TRUE(err.ok()) << err.what();
	std::ignore = rt.CommitTransaction(tx);

	ASSERT_EQ(GetMeta("txkey"), "txval");

	ReopenNamespace();
	ASSERT_EQ(GetMeta("txkey"), "txval");

	auto keys = EnumMeta();
	ASSERT_NE(std::find(keys.begin(), keys.end(), "txkey"), keys.end());

	DeleteMeta("txkey");
	ReopenNamespace();
	ASSERT_EQ(GetMeta("txkey"), "");
	keys = EnumMeta();
	ASSERT_EQ(std::find(keys.begin(), keys.end(), "txkey"), keys.end());
}

TEST_F(ReindexerStorageApi, MetaCopiedTransactionIsolationAndPersistence) {
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.UpsertJSON(default_namespace, R"({"id":1})");
	PutMeta("txkey", "original");
	PutMeta("untouched", "preserved");
	ASSERT_GT(
		rt.Update(Query(reindexer::kConfigNamespace).Where("type", CondEq, "namespaces").Set("namespaces[*].tx_size_to_always_copy", 1)),
		0);

	auto failedTx = rt.NewTransaction(default_namespace);
	auto err = failedTx.PutMeta("txkey", "failed");
	ASSERT_TRUE(err.ok()) << err.what();
	err = failedTx.PutMeta("newkey", "failed");
	ASSERT_TRUE(err.ok()) << err.what();
	// Fail after applying the metadata steps to the unpublished namespace copy.
	err = failedTx.Modify(Query(default_namespace).Where("id", CondEq, 1).Set("id", "not_an_integer"));
	ASSERT_TRUE(err.ok()) << err.what();
	QueryResults result;
	err = rt.reindexer->CommitTransaction(failedTx, result);
	ASSERT_FALSE(err.ok()) << err.what();
	EXPECT_EQ(GetMeta("txkey"), "original");
	EXPECT_EQ(GetMeta("newkey"), "");
	EXPECT_EQ(GetMeta("untouched"), "preserved");

	ReopenNamespace();
	EXPECT_EQ(GetMeta("txkey"), "original");
	EXPECT_EQ(GetMeta("newkey"), "");
	EXPECT_EQ(GetMeta("untouched"), "preserved");

	auto tx = rt.NewTransaction(default_namespace);
	err = tx.PutMeta("txkey", "committed");
	ASSERT_TRUE(err.ok()) << err.what();
	err = tx.PutMeta("newkey", "created");
	ASSERT_TRUE(err.ok()) << err.what();
	std::ignore = rt.CommitTransaction(tx);
	EXPECT_EQ(GetMeta("txkey"), "committed");
	EXPECT_EQ(GetMeta("newkey"), "created");
	EXPECT_EQ(GetMeta("untouched"), "preserved");

	ReopenNamespace();
	EXPECT_EQ(GetMeta("txkey"), "committed");
	EXPECT_EQ(GetMeta("newkey"), "created");
	EXPECT_EQ(GetMeta("untouched"), "preserved");
}

}  // namespace reindexer_tests
