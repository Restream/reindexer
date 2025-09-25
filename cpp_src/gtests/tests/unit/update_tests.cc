#include <gtest/gtest.h>
#include "reindexer_api.h"

class [[nodiscard]] CompositeUpdate : public ReindexerApi {
public:
	void SetUp() override {
		using namespace std::string_literals;
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(
			default_namespace,
			{IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldV1, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldV2, "tree", "int", IndexOpts(), 0}, IndexDeclaration{kFieldV3, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldV4, "hash", "string", IndexOpts(), 0},
			 IndexDeclaration{std::string(kFieldV1) + "+" + kFieldV2 + "=" + kFieldV1_V2, "hash", "composite", IndexOpts(), 0},
			 IndexDeclaration{std::string(kFieldV3) + "+" + kFieldV4 + "=" + kFieldV3_V4, "hash", "composite", IndexOpts(), 0},
			 IndexDeclaration{kFieldArray, "hash", "int", IndexOpts().Array(), 0}});
		fillDefaultNs();
	}

	static constexpr char kFieldId[] = "id";
	static constexpr char kFieldV1[] = "v1";
	static constexpr char kFieldV2[] = "v2";
	static constexpr char kFieldV3[] = "v3";
	static constexpr char kFieldV4[] = "v4";
	static constexpr char kFieldV1_V2[] = "v1_v2";
	static constexpr char kFieldV3_V4[] = "v3_4";
	static constexpr char kFieldArray[] = "array";

	void ExecuteAndCheckResult(const Query& q, const std::string& item) {
		SCOPED_TRACE(q.GetSQL());
		reindexer::QueryResults res;
		Error err;
		switch (q.type_) {
			case QuerySelect:
				res = rt.Select(q);
				break;
			case QueryUpdate:
				rt.Update(q, res);
				break;
			case QueryDelete:
				rt.Delete(q, res);
				break;
			case QueryTruncate:
				assertrx(false);
		}
		if (!item.empty()) {
			ASSERT_EQ(res.Count(), 1);
			reindexer::WrSerializer ser;
			auto err = res.begin().GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(ser.Slice(), item);
		} else {
			ASSERT_EQ(res.Count(), 0);
		}
	}

private:
	void fillDefaultNs() { rt.UpsertJSON(default_namespace, R"({"id":1, "array":[1,2,3], "v1": 1, "v2":200, "v3":1000, "v4":"v4"})"); }
};

TEST_F(CompositeUpdate, CompositeAndArray) {
	{
		auto q = Query(default_namespace).Set(kFieldV1, 10).Set(kFieldArray, {10, 11}).Where(kFieldId, CondEq, 1);
		q.type_ = QueryUpdate;
		ExecuteAndCheckResult(q, R"({"id":1,"array":[10,11],"v1":10,"v2":200,"v3":1000,"v4":"v4"})");
	}
	{
		auto q =
			Query(default_namespace).Set(kFieldV1, 20).Set(kFieldV4, "str").Set(kFieldArray, {10, 11, 20, 30}).Where(kFieldId, CondEq, 1);
		q.type_ = QueryUpdate;
		ExecuteAndCheckResult(q, R"({"id":1,"array":[10,11,20,30],"v1":20,"v2":200,"v3":1000,"v4":"str"})");
	}
	{
		auto q = Query(default_namespace).WhereComposite(kFieldV1_V2, CondEq, {{Variant(20), Variant(200)}});
		ExecuteAndCheckResult(q, R"({"id":1,"array":[10,11,20,30],"v1":20,"v2":200,"v3":1000,"v4":"str"})");
	}
	{
		auto q = Query(default_namespace).WhereComposite(kFieldV1_V2, CondEq, {{Variant(10), Variant(200)}});
		ExecuteAndCheckResult(q, "");
	}
	{
		auto q = Query(default_namespace).Set(kFieldArray, {11, 11}).Set(kFieldV1, 11).Where(kFieldId, CondEq, 1);
		q.type_ = QueryUpdate;
		ExecuteAndCheckResult(q, R"({"id":1,"array":[11,11],"v1":11,"v2":200,"v3":1000,"v4":"str"})");
	}
	{
		auto q = Query(default_namespace).Set(kFieldV1, 12).Set(kFieldArray, {12, 12}).Set(kFieldV4, "a").Where(kFieldId, CondEq, 1);
		q.type_ = QueryUpdate;
		ExecuteAndCheckResult(q, R"({"id":1,"array":[12,12],"v1":12,"v2":200,"v3":1000,"v4":"a"})");
	}
	{
		auto q = Query(default_namespace)
					 .Set(kFieldV1, 22)
					 .Set(kFieldArray, {22, 22})
					 .Set(kFieldV4, "b")
					 .Set(kFieldV2, 22)
					 .Set(kFieldArray, {23, 23})
					 .Set(kFieldV1, 23)
					 .Where(kFieldId, CondEq, 1);
		q.type_ = QueryUpdate;
		ExecuteAndCheckResult(q, R"({"id":1,"array":[23,23],"v1":23,"v2":22,"v3":1000,"v4":"b"})");
	}
}
