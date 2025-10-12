#include "get_pk_api.h"

#define SIMPLE_ITEM_NAMESPACE "test_extrack_pk_simple"
#define NESTED_ITEM_NAMESPACE "test_extract_pk_nested"
#define NESTED_ITEM_WITH_OBJ_NAMESPACE "test_extract_pk_nested_obj"

#define CHANGE_TYPE_JSON_PATTERN \
	"{"                          \
	"\"id\": %d,"                \
	"\"name\": %d,"              \
	"\"color\": %d,"             \
	"\"weight\": \"%s\","        \
	"\"height\": \"%s\","        \
	"\"fk_id\": %d "             \
	"}"

#define SIMPLE_JSON_PATTERN \
	"{"                     \
	"\"id\": %d,"           \
	"\"name\": \"%s\","     \
	"\"color\": \"%s\","    \
	"\"weight\": %d,"       \
	"\"height\": %d,"       \
	"\"fk_id\": %d "        \
	"}"

#define NESTED_JSON_PATTERN \
	"{"                     \
	"\"id\": %d,"           \
	"\"name\": \"%s\","     \
	"\"desc\": {"           \
	"  \"color\": \"%s\","  \
	"  \"weight\": %d,"     \
	"  \"height\": %d,"     \
	"  \"fk_id\": %d "      \
	"  }"                   \
	"}"

#define NESTED_JSON_WITH_OBJ_PATTERN \
	"{"                              \
	"\"id\": %d,"                    \
	"\"name\": \"%s\","              \
	"\"desc\": {"                    \
	"  \"color\": \"%s\""            \
	"  },"                           \
	"\"other\": {"                   \
	"  \"weight\": %d,"              \
	"  \"height\": %d,"              \
	"  \"fk_id\": %d "               \
	"  }"                            \
	"}"

#define CHECK_SUCCESS(expr)              \
	{                                    \
		const Error& e = expr;           \
		ASSERT_TRUE(e.ok()) << e.what(); \
	}

using std::ignore;
using std::string;

TEST_F(ExtractPK, DeleteByPKOnlyJSON) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(SIMPLE_ITEM_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", "text", "string", IndexOpts())
									  .AddIndex("weight", "tree", "int", IndexOpts())
									  .AddIndex("height", "tree", "int", IndexOpts())
									  .AddIndex("fk_id", "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(SIMPLE_ITEM_NAMESPACE, SIMPLE_JSON_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());
	ASSERT_EQ(item["id"].As<int>(), data.id);
	ASSERT_EQ(item["name"].As<std::string>(), data.name);
	ASSERT_EQ(item["color"].As<std::string>(), data.color);
	ASSERT_EQ(item["weight"].As<int>(), data.weight);
	ASSERT_EQ(item["height"].As<int>(), data.height);
	ASSERT_EQ(item["fk_id"].As<int>(), data.fk_id);

	CHECK_SUCCESS(Upsert(SIMPLE_ITEM_NAMESPACE, item));

	for (int id = 1; id < 4; id++) {
		for (int fk_id = 1; fk_id < 4; fk_id++) {
			item["id"] = data.id + id;
			item["fk_id"] = data.fk_id + fk_id;
			item["name"] = names_.at(rand() % names_.size());
			item["color"] = colors_.at(rand() % names_.size());
			item["weight"] = rand() % 1000;
			item["height"] = rand() % 1000;

			CHECK_SUCCESS(Upsert(SIMPLE_ITEM_NAMESPACE, item));
		}
	}

	// generate JSON
	std::string json = fmt::sprintf(SIMPLE_JSON_PATTERN, data.id, data.name, data.color, data.weight, data.height, data.fk_id);

	// we need create new 'Item' for getting updated TagsMatcher
	Item itemForDelete = db_->NewItem(SIMPLE_ITEM_NAMESPACE);
	CHECK_SUCCESS(itemForDelete.FromJSON(json, nullptr, true));
	CHECK_SUCCESS(db_->Delete(SIMPLE_ITEM_NAMESPACE, itemForDelete))

	QueryResults deleteRes;
	CHECK_SUCCESS(db_->Delete(Query(SIMPLE_ITEM_NAMESPACE).Where("id", CondEq, data.id), deleteRes));
	ASSERT_TRUE(!deleteRes.Count()) << "Result of deletion must be empty";

	const auto [err2, qres] = Select(Query(SIMPLE_ITEM_NAMESPACE).Where("id", CondEq, data.id), false);
	CHECK_SUCCESS(err2);
	ASSERT_TRUE(!qres.Count()) << "Result of selection must be empty";
}

TEST_F(ExtractPK, ChangedTypeJSON) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(SIMPLE_ITEM_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", "text", "string", IndexOpts())
									  .AddIndex("weight", "tree", "int", IndexOpts())
									  .AddIndex("height", "tree", "int", IndexOpts())
									  .AddIndex("fk_id", "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(SIMPLE_ITEM_NAMESPACE, SIMPLE_JSON_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());

	for (int id = 0; id < 3; id++) {
		for (int fk_id = 0; fk_id < 3; fk_id++) {
			item["id"] = data.id + id;
			item["fk_id"] = data.fk_id + fk_id;
			item["name"] = names_.at(rand() % names_.size());
			item["color"] = colors_.at(rand() % names_.size());
			item["weight"] = rand() % 1000;
			item["height"] = rand() % 1000;

			CHECK_SUCCESS(Upsert(SIMPLE_ITEM_NAMESPACE, item));
		}
	}

	std::string json = fmt::sprintf(CHANGE_TYPE_JSON_PATTERN, data.id, data.weight, data.height, data.name, data.color, data.fk_id);
	Item cItem = db_->NewItem(SIMPLE_ITEM_NAMESPACE);
	CHECK_SUCCESS(cItem.FromJSON(json, nullptr, true));
	CHECK_SUCCESS(db_->Delete(SIMPLE_ITEM_NAMESPACE, cItem));

	QueryResults deleteRes;
	CHECK_SUCCESS(db_->Delete(Query(SIMPLE_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), deleteRes));
	ASSERT_TRUE(!deleteRes.Count()) << "Result of deletion must be empty";

	QueryResults selectRes;
	CHECK_SUCCESS(db_->Select(Query(SIMPLE_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), selectRes));
	ASSERT_TRUE(!selectRes.Count()) << "Result of selection must be empty";
}

TEST_F(ExtractPK, NestedJSON) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(NESTED_ITEM_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", {"desc.color"}, "text", "string", IndexOpts())
									  .AddIndex("weight", {"desc.weight"}, "tree", "int", IndexOpts())
									  .AddIndex("height", {"desc.height"}, "tree", "int", IndexOpts())
									  .AddIndex("fk_id", {"desc.fk_id"}, "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(NESTED_ITEM_NAMESPACE, NESTED_JSON_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());
	CHECK_SUCCESS(Upsert(NESTED_ITEM_NAMESPACE, item));

	for (int id = 0; id < 3; id++) {
		for (int fk_id = 0; fk_id < 3; fk_id++) {
			item["id"] = data.id + id;
			item["fk_id"] = data.fk_id + fk_id;
			item["name"] = names_.at(rand() % names_.size());
			item["color"] = colors_.at(rand() % names_.size());
			item["weight"] = rand() % 1000;
			item["height"] = rand() % 1000;

			CHECK_SUCCESS(Upsert(NESTED_ITEM_NAMESPACE, item));
		}
	}

	std::string json = fmt::sprintf(NESTED_JSON_PATTERN, ++data.id, data.name, data.color, data.weight, data.height, ++data.fk_id);
	Item checkItem = db_->NewItem(NESTED_ITEM_NAMESPACE);
	CHECK_SUCCESS(checkItem.FromJSON(json, nullptr, true));
	CHECK_SUCCESS(db_->Delete(NESTED_ITEM_NAMESPACE, checkItem));

	QueryResults dRes;
	CHECK_SUCCESS(db_->Delete(Query(NESTED_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), dRes));
	ASSERT_TRUE(!dRes.Count()) << "Result of deletion must be empty";

	QueryResults qRes;
	CHECK_SUCCESS(db_->Select(Query(NESTED_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), qRes));
	ASSERT_TRUE(!qRes.Count()) << "Result of selection must be empty";
}

TEST_F(ExtractPK, CJson2CJson_PrintJSON) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(SIMPLE_ITEM_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", "text", "string", IndexOpts())
									  .AddIndex("weight", "tree", "int", IndexOpts())
									  .AddIndex("height", "tree", "int", IndexOpts())
									  .AddIndex("fk_id", "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(SIMPLE_ITEM_NAMESPACE, SIMPLE_JSON_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());
	CHECK_SUCCESS(Upsert(SIMPLE_ITEM_NAMESPACE, item));

	std::string originalJson(item.GetJSON());

	Item test = db_->NewItem(SIMPLE_ITEM_NAMESPACE);
	CHECK_SUCCESS(test.FromCJSON(item.GetCJSON()));

	auto testJson = test.GetJSON();
	EXPECT_EQ(testJson, originalJson);
}

TEST_F(ExtractPK, SimpleCJSON) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(SIMPLE_ITEM_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", "text", "string", IndexOpts())
									  .AddIndex("weight", "tree", "int", IndexOpts())
									  .AddIndex("height", "tree", "int", IndexOpts())
									  .AddIndex("fk_id", "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(SIMPLE_ITEM_NAMESPACE, SIMPLE_JSON_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());
	CHECK_SUCCESS(Upsert(SIMPLE_ITEM_NAMESPACE, item));

	for (int id = 1; id < 4; id++) {
		for (int fk_id = 1; fk_id < 4; fk_id++) {
			item["id"] = data.id + id;
			item["fk_id"] = data.fk_id + fk_id;
			item["name"] = names_.at(rand() % names_.size());
			item["color"] = colors_.at(rand() % names_.size());
			item["weight"] = rand() % 1000;
			item["height"] = rand() % 1000;
			CHECK_SUCCESS(Upsert(SIMPLE_ITEM_NAMESPACE, item));
		}
	}

	auto [err2, item4cjson, _] = NewItem(SIMPLE_ITEM_NAMESPACE, SIMPLE_JSON_PATTERN, &data);
	CHECK_SUCCESS(err2);

	Item fromCJSON = db_->NewItem(SIMPLE_ITEM_NAMESPACE);
	CHECK_SUCCESS(fromCJSON.FromCJSON(item4cjson.GetCJSON(), true));
	CHECK_SUCCESS(db_->Delete(SIMPLE_ITEM_NAMESPACE, fromCJSON));

	QueryResults dRes;
	CHECK_SUCCESS(db_->Delete(Query(SIMPLE_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), dRes));
	ASSERT_TRUE(!dRes.Count()) << "Result of deletion must be empty";

	QueryResults qRes;
	CHECK_SUCCESS(db_->Select(Query(SIMPLE_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), qRes));
	ASSERT_TRUE(!qRes.Count()) << "Result of selection must be empty";
}

TEST_F(ExtractPK, NestedCJSON) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(NESTED_ITEM_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", {"desc.color"}, "text", "string", IndexOpts())
									  .AddIndex("weight", {"desc.weight"}, "tree", "int", IndexOpts())
									  .AddIndex("height", {"desc.height"}, "tree", "int", IndexOpts())
									  .AddIndex("fk_id", {"desc.fk_id"}, "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(NESTED_ITEM_NAMESPACE, NESTED_JSON_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());
	CHECK_SUCCESS(Upsert(NESTED_ITEM_NAMESPACE, item));

	for (int id = 1; id < 4; id++) {
		for (int fk_id = 1; fk_id < 4; fk_id++) {
			item["id"] = data.id + id;
			item["fk_id"] = data.fk_id + fk_id;
			item["name"] = names_.at(rand() % names_.size());
			item["color"] = colors_.at(rand() % names_.size());
			item["weight"] = rand() % 1000;
			item["height"] = rand() % 1000;
			CHECK_SUCCESS(Upsert(NESTED_ITEM_NAMESPACE, item));
		}
	}

	auto [err2, item4cjson, _] = NewItem(NESTED_ITEM_NAMESPACE, NESTED_JSON_PATTERN, &data);
	CHECK_SUCCESS(err2);

	Item fromCJSON = db_->NewItem(NESTED_ITEM_NAMESPACE);
	CHECK_SUCCESS(fromCJSON.FromCJSON(item4cjson.GetCJSON(), true));
	CHECK_SUCCESS(db_->Delete(NESTED_ITEM_NAMESPACE, fromCJSON));

	QueryResults dRes;
	CHECK_SUCCESS(db_->Delete(Query(NESTED_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), dRes));
	ASSERT_TRUE(!dRes.Count()) << "Result of deletion must be empty";

	QueryResults qRes;
	CHECK_SUCCESS(db_->Select(Query(NESTED_ITEM_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), qRes));
	ASSERT_TRUE(!qRes.Count()) << "Result of selection must be empty";
}

TEST_F(ExtractPK, NestedCJSONWithObject) {
	CHECK_SUCCESS(CreateNamespace(NamespaceDef(NESTED_ITEM_WITH_OBJ_NAMESPACE)
									  .AddIndex("id", "hash", "int", IndexOpts())
									  .AddIndex("name", "text", "string", IndexOpts())
									  .AddIndex("color", {"desc.color"}, "text", "string", IndexOpts())
									  .AddIndex("weight", {"other.weight"}, "tree", "int", IndexOpts())
									  .AddIndex("height", {"other.height"}, "tree", "int", IndexOpts())
									  .AddIndex("fk_id", {"other.fk_id"}, "tree", "int", IndexOpts())
									  .AddIndex("id+fk_id", {"id", "fk_id"}, "tree", "composite", IndexOpts().PK())));

	auto [err, item, data] = NewItem(NESTED_ITEM_WITH_OBJ_NAMESPACE, NESTED_JSON_WITH_OBJ_PATTERN);
	CHECK_SUCCESS(err);
	CHECK_SUCCESS(item.Status());
	CHECK_SUCCESS(Upsert(NESTED_ITEM_WITH_OBJ_NAMESPACE, item));

	for (int id = 1; id < 4; id++) {
		for (int fk_id = 1; fk_id < 4; fk_id++) {
			item["id"] = data.id + id;
			item["fk_id"] = data.fk_id + fk_id;
			item["name"] = names_.at(rand() % names_.size());
			item["color"] = colors_.at(rand() % names_.size());
			item["weight"] = rand() % 1000;
			item["height"] = rand() % 1000;
			CHECK_SUCCESS(Upsert(NESTED_ITEM_WITH_OBJ_NAMESPACE, item));
		}
	}

	auto [err2, item4cjson, _] = NewItem(NESTED_ITEM_WITH_OBJ_NAMESPACE, NESTED_JSON_WITH_OBJ_PATTERN, &data);
	CHECK_SUCCESS(err2);

	Item fromCJSON = db_->NewItem(NESTED_ITEM_WITH_OBJ_NAMESPACE);
	CHECK_SUCCESS(fromCJSON.FromCJSON(item4cjson.GetCJSON(), true));
	CHECK_SUCCESS(db_->Delete(NESTED_ITEM_WITH_OBJ_NAMESPACE, fromCJSON));

	QueryResults dRes;
	CHECK_SUCCESS(db_->Delete(Query(NESTED_ITEM_WITH_OBJ_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), dRes));
	ASSERT_TRUE(!dRes.Count()) << "Result of deletion must be empty";

	QueryResults qRes;
	CHECK_SUCCESS(db_->Select(Query(NESTED_ITEM_WITH_OBJ_NAMESPACE).Where("id", CondEq, data.id).Where("fk_id", CondEq, data.fk_id), qRes));
	ASSERT_TRUE(!qRes.Count()) << "Result of selection must be empty";
}
