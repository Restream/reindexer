#include "core/cjson/jschemachecker.h"
#include "estl/span.h"
#include "gtest/gtest.h"

#include "core/query/dsl/query.json.h"

using namespace reindexer;
TEST(SchemaTest, BaseTest) {
	static reindexer::JsonSchemaChecker check(kQueryJson, "query");
	gason::JsonParser parser;

	std::string dsl = R"#({
		"namespace":"12223"
		"limit":-1,
		"offset":0,
		"req_total":"disabled",
		"explain":false,
		"select_with_rank":false,
		"select_filter":[],
		"select_functions":[],
		"sort":[{"field":"id"}],
		"filters": [{
						"op": "OR",
						"field": "id",
						"cond": "SET",
						"value": [81204872, 101326571, 101326882]
					},
					{
						"equal_positions":[{"positions": ["f4","f5"] }]
					}],
		"merge_queries":[],
		"aggregations":[]
	})#";
	{
		auto root = parser.Parse(giftStr(dsl));
		Error err = check.Check(root);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::string dslExtraTag = R"#({
		"namespace":"test_namespace",
		"extra_tag":"tag",
	})#";
	{
		auto root = parser.Parse(giftStr(dslExtraTag));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [extra_tag] not allowed in [query] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dslExtraTag2 = R"#({
		"namespace":"test_namespace",
		"sort":{
				"field":"f1",
				"extra_tag":"tag"
			}
	})#";
	{
		auto root = parser.Parse(giftStr(dslExtraTag2));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [extra_tag] not allowed in [query.sort] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dsl2Tag = R"#({
			"namespace":"test_namespace",
			"limit":100,
			"aggregations":{
				"fields": "f1",
				"type":"SUM",
				"type":"MIN",
			}

		})#";
	{
		auto root = parser.Parse(giftStr(dsl2Tag));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [type] can occur only once in [query.aggregations] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dsl0Tag = R"#({
			"limit":100,
		})#";
	{
		auto root = parser.Parse(giftStr(dsl0Tag));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [namespace] must occur in [query] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dslSort1 = R"#({
			"namespace":"test_namespace",
			"sort":[{"desc":true,"field":"abc","values":[1,2,3]}],
		})#";
	{
		auto root = parser.Parse(giftStr(dslSort1));
		Error err = check.Check(root);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::string dslSort2 = R"#({
			"namespace":"test_namespace",
			"sort":[{"desc1":true,"field":"abc","values":[1,2,3]}],
		})#";
	{
		auto root = parser.Parse(giftStr(dslSort2));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [desc1] not allowed in [query.sort] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dslSort3 = R"#({
			"namespace":"test_namespace",
			"sort":[{"values":[1,2,3]}],
		})#";
	{
		auto root = parser.Parse(giftStr(dslSort3));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [field] must occur in [query.sort] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dslSort4 = R"#({
			"namespace":"test_namespace",
			"sort":{"values":[1,2,3]},
		})#";
	{
		auto root = parser.Parse(giftStr(dslSort4));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [field] must occur in [query.sort] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}
}
TEST(SchemaTest, AdditionalProperties) {
	const std::string scemaStr = R"#({
		"required":["v1"],
		"additionalProperties": true,
		"type": "object",
		"properties":{
			"v1":{"type": "integer"},
			"v2":{"type": "integer"},
		}
	}
	)#";

	static reindexer::JsonSchemaChecker check;
	Error e = check.Init(scemaStr, "query");
	ASSERT_TRUE(e.ok()) << e.what();
	gason::JsonParser parser;

	std::string str1 = R"#({
			"v1":10,
			"v2":20,
			"v3":30
		})#";
	{
		auto root = parser.Parse(giftStr(str1));
		Error err = check.Check(root);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::string str2 = R"#({
			"v2":20,
			"v3":30
		})#";
	{
		auto root = parser.Parse(giftStr(str2));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [v1] must occur in [query] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string str3 = R"#({
			"v1":10,
			"v2":{"subv":100},
			"v3":30
		})#";
	{
		auto root = parser.Parse(giftStr(str3));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [subv] not allowed in [query.v2] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string str4 = R"#({
			"v1":10,
		})#";
	{
		auto root = parser.Parse(giftStr(str4));
		Error err = check.Check(root);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

TEST(SchemaTest, LevelAny3) {
	static reindexer::JsonSchemaChecker check(kQueryJson, "query");
	gason::JsonParser parser;
	std::string dsl2level = R"#({
		"namespace":"test_namespace",
		"filters":[
			{
				"field":"a1",
				"filters":[
					{
						"field":"a2",
						"filters":[{"error":1}]
					}
				]
			}
		]

	})#";
	{
		auto root = parser.Parse(giftStr(dsl2level));
		Error err = check.Check(root);
		EXPECT_STREQ(err.what().c_str(), "Key [error] not allowed in [query.filters.filters.filters] object.");
		ASSERT_FALSE(err.ok()) << err.what();
	}

	std::string dsl3level = R"#({
		"namespace":"test_namespace",
		"filters":[
			{
				"field":"l1",
				"filters":[
					{
						"field":"l2",
						"filters":[
							{
								"field":"l3",
								"filters":[{"error":1}]
							}
						]
					}
				]
			}
		]

	})#";
	{
		auto root = parser.Parse(giftStr(dsl3level));
		Error err = check.Check(root);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}
