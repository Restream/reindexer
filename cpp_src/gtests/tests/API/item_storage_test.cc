#include "reindexer_storage_api.h"

#include <string>
#include <vector>

namespace reindexer_tests {

TEST_F(ReindexerStorageApi, ItemWritesPreserveVectorsAcrossPkUpdateAndReopen) {
	rt.AddIndex(default_namespace, {"id", "hash", "int", reindexer::IndexOpts().PK()});
	rt.AddIndex(default_namespace,
				{"vec", "vec_bf", "float_vector",
				 reindexer::IndexOpts{}.SetFloatVector(
					 IndexVectorBruteforce, reindexer::FloatVectorIndexOpts{}.SetDimension(2).SetMetric(reindexer::VectorMetric::L2))});
	rt.UpsertJSON(default_namespace, R"({"id":1,"name":"first","vec":[1.25,-2.5]})");
	rt.UpsertJSON(default_namespace, R"({"id":2,"name":"deleted","vec":[3.5,4.25]})");
	rt.UpsertJSON(default_namespace, R"({"id":3,"name":"original","vec":[5,6]})");

	// A partial UPDATE must retain the vector and remove the previous PK-derived storage key.
	ASSERT_EQ(rt.Update(Query(default_namespace).Where("id", CondEq, 1).Set("id", 11).Set("name", "updated")), 1);
	rt.UpsertJSON(default_namespace, R"({"id":3,"name":"replaced","vec":[7.5,8.25]})");
	auto deleted = rt.NewItem(default_namespace);
	deleted["id"] = 2;
	rt.Delete(default_namespace, deleted);

	const auto allItems = Query(default_namespace).Sort("id", SortOrder::Asc).SelectAllFields();
	auto before = rt.Select(allItems);
	ASSERT_EQ(before.Count(), 2);
	const std::vector<std::string> expected = {
		R"({"id":3,"name":"replaced","vec":[7.5,8.25]})",
		R"({"id":11,"name":"updated","vec":[1.25,-2.5]})",
	};
	ASSERT_EQ(rt.GetSerializedQrItems(before), expected);
	ReopenNamespace();
	auto after = rt.Select(allItems);
	EXPECT_EQ(rt.GetSerializedQrItems(after), expected);
	EXPECT_EQ(rt.Select(Query(default_namespace).Where("id", CondSet, {1, 2})).Count(), 0);

	// PK type migration requires removing the vector index; its values remain in the documents.
	rt.DropIndex(default_namespace, "vec");
	rt.UpdateIndex(default_namespace, {"id", "hash", "int64", reindexer::IndexOpts().PK()});
	ReopenNamespace();
	auto migrated = rt.Select(allItems);
	EXPECT_EQ(rt.GetSerializedQrItems(migrated), expected);

	ASSERT_EQ(rt.Delete(Query(default_namespace).Where("id", CondEq, 11)), 1);
	ReopenNamespace();
	auto remaining = rt.Select(allItems);
	ASSERT_EQ(remaining.Count(), 1);
	EXPECT_EQ(rt.GetSerializedQrItems(remaining).front(), expected.front());

	rt.TruncateNamespace(default_namespace);
	ReopenNamespace();
	EXPECT_EQ(rt.Select(allItems).Count(), 0);
}

}  // namespace reindexer_tests
