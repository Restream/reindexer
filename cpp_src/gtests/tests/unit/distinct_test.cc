#include "distinct_api.h"

TEST_F(DistinctApi, Serialize) {
	const std::string sql = "SELECT distinct(v1, v2, v3), * FROM " + default_namespace;
	const std::string json =
		R"#({"namespace":"test_namespace","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[],"merge_queries":[],"aggregations":[{"type":"distinct","sort":[],"fields":["v1","v2","v3"]}]})#";
	{
		Query q{Query::FromSQL(sql)};
		ASSERT_EQ(sql, q.GetSQL());
	}
	{
		Query q{Query(default_namespace).Distinct("v1", "v2", "v3")};
		ASSERT_EQ(sql, q.GetSQL());
		ASSERT_EQ(json, q.GetJSON());
	}
	{
		Query q{Query::FromJSON(json)};
		ASSERT_EQ(sql, q.GetSQL());
		ASSERT_EQ(json, q.GetJSON());
	}
}

TEST_F(DistinctApi, Verify) {
	auto insertItem = [&](std::string_view json) {
		Item item = NewItem(default_namespace);
		Error err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		Upsert(default_namespace, item);
		saveItem(std::move(item), default_namespace);
	};
	auto verify = [this]() {
		auto q = Query::FromSQL("SELECT distinct(v0,v1,v2),* from " + default_namespace);
		auto res = rt.Select(q);
		Verify(res, std::move(q), *rt.reindexer);
	};

	insertItem(R"#({"id":0,"v0":1,"v1":2.12,"v2":"string"})#");
	verify();
	insertItem(R"#({"id":1,"v0":3,"v1":3.12,"v2":"string"})#");
	verify();
	insertItem(R"#({"id":2,"v0":4,"v1":3.12,"v2":"string"})#");
	verify();
	insertItem(R"#({"id":3,"v0":4,"v1":3.12})#");
	verify();
	insertItem(R"#({"id":4,"v1":3.12})#");
	verify();
	insertItem(R"#({"id":5})#");
	verify();
	insertItem(R"#({"id":6,"v0":"4","v1":3.12})#");
	verify();
	insertItem(R"#({"id":7,"v0":["45"],"v1":[4.12]})#");
	verify();
	insertItem(R"#({"id":8,"v0":["47","48"],"v1":[5.12,5.14],"v2":["100","101"]})#");
	verify();
	insertItem(R"#({"id":9,"v0":["47","48"],"v1":[5.12,5.14],"v2":["102","101"]})#");
	verify();
	insertItem(R"#({"id":10,"v0":["47","48"],"v1":[5.12,5.14],"v2":["102","101"]})#");
	verify();
	insertItem(R"#({"id":11,"v0":["50","51","52"],"v1":[6.12],"v2":["202","201"]})#");
	verify();
	insertItem(R"#({"id":12,"v0":["55","56","57"],"v1":6.15,"v2":"202"})#");
	verify();
	insertItem(R"#({"id":13,"v0":["61","62","63"],"v1":6.15})#");
	verify();

	insertItem(R"#({"id":100,"vi1":10,"vi2":"str1"})#");
	auto verifyIndex = [this]() {
		auto q = Query::FromSQL("SELECT distinct(vi1,vi2),* from " + default_namespace);
		auto res = rt.Select(q);
		Verify(res, std::move(q), *rt.reindexer);
	};
	verifyIndex();
}
