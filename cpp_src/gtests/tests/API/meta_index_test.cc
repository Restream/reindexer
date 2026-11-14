#include "reindexer_storage_api.h"

#include <algorithm>
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

}  // namespace reindexer_tests
