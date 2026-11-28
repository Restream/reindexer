#include <utility>
#include "core/system_ns_names.h"
#include "reindexer_storage_api.h"

namespace reindexer_tests {

TEST_F(ReindexerStorageApi, SystemRecordsSurviveCopiedTransactionAndReopen) {
	rt.AddIndex(default_namespace, {"id", "hash", "int", reindexer::IndexOpts().PK()});
	rt.SetSchema(default_namespace, R"json({"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}}})json");
	const auto schema = rt.GetSchema(default_namespace, JsonSchemaType);
	rt.UpsertJSON(default_namespace, R"json({"id":1,"name":"original","before":{"value":10}})json");
	ASSERT_GT(
		rt.Update(Query(reindexer::kConfigNamespace).Where("type", CondEq, "namespaces").Set("namespaces[*].tx_size_to_always_copy", 1)),
		0);

	auto tx = rt.NewTransaction(default_namespace);
	auto item = tx.NewItem();
	auto err = item.FromJSON(R"json({"id":2,"name":"copied","new_field":{"nested":[1,2,3]}})json");
	ASSERT_TRUE(err.ok()) << err.what();
	err = tx.Upsert(std::move(item));
	ASSERT_TRUE(err.ok()) << err.what();
	const auto committed = rt.CommitTransaction(tx);
	ASSERT_EQ(committed.Count(), 1);

	auto before = rt.Select(Query(default_namespace).Sort("id", SortOrder::Asc));
	const auto expected = rt.GetSerializedQrItems(before);
	ReopenNamespace();
	EXPECT_EQ(rt.GetSchema(default_namespace, JsonSchemaType), schema);
	auto after = rt.Select(Query(default_namespace).Sort("id", SortOrder::Asc));
	EXPECT_EQ(rt.GetSerializedQrItems(after), expected);

	// An upsert after reopening must still use the restored PK rather than add another item.
	rt.UpsertJSON(default_namespace, R"json({"id":2,"name":"updated","new_field":{"nested":[4]}})json");
	EXPECT_EQ(rt.Select(Query(default_namespace)).Count(), 2);
}

}  // namespace reindexer_tests
