#include <gtest/gtest.h>
#include "core/dbconfig.h"
#include "core/index/index.h"
#include "core/namespace/stringsholder.h"
#include "core/query/query_impl.h"
#include "reindexer_api.h"

namespace reindexer_tests {

using reindexer::IndexOpts;
using reindexer::Variant;

#if REINDEX_WITH_STRHOLDER_CHECKS
TEST(StringArrayIndex, RemovingRowReleasesAllOriginalStrings) {
	struct [[nodiscard]] StringCollector final : reindexer::Index::OwnedStringPtrCollector {
		size_t searchKeys{0};
		size_t originalStrings{0};
		void Add(const reindexer::key_string_impl*) override { ++originalStrings; }
		void Add(const reindexer::key_string_impl*, const void*, ContainsIdFn) override { ++searchKeys; }
	};
	for (const auto type : {"hash", "tree"}) {
		SCOPED_TRACE(type);

		const auto checkRemoval = [type](const char* scenario, CollateMode mode, const reindexer::VariantArray& tags,
										 size_t expectedOriginals) {
			SCOPED_TRACE(scenario);
			const reindexer::IndexDef indexDef{"tags", {"tags"}, type, "string", IndexOpts{}.Array().SetCollateMode(mode)};
			auto index{reindexer::Index::New(indexDef, reindexer::PayloadType{}, reindexer::FieldsSet{},
											 reindexer::NamespaceCacheConfigData{}, 0)};

			bool clearCache = false;
			reindexer::VariantArray values;
			reindexer::StringsHolder holder;
			const auto rowId = reindexer::IdType::FromNumber(1);
			index->Upsert(values, {Variant{"unchanged"}}, reindexer::IdType::FromNumber(0), clearCache);
			index->Upsert(values, tags, rowId, clearCache);
			ASSERT_EQ(values.size(), 2);

			StringCollector before;
			index->CollectOwnedStringPtrs(before);
			ASSERT_EQ(before.searchKeys, 2);
			ASSERT_EQ(before.originalStrings, expectedOriginals);

			index->Delete(values, rowId, reindexer::MustExist_True, holder, clearCache);
			StringCollector after;
			index->CollectOwnedStringPtrs(after);
			EXPECT_EQ(after.searchKeys, 1);
			EXPECT_EQ(after.originalStrings, 1);

			index->Delete(Variant{"unchanged"}, reindexer::IdType::FromNumber(0), reindexer::MustExist_True, holder, clearCache);
			StringCollector empty;
			index->CollectOwnedStringPtrs(empty);
			EXPECT_EQ(empty.searchKeys, 0);
			EXPECT_EQ(empty.originalStrings, 0);
		};

		checkRemoval("DuplicateStrings", CollateNumeric, {Variant{"tag"}, Variant{"tag"}}, 2);
		checkRemoval("CollateEquivalentStrings", CollateASCII, {Variant{"Tag"}, Variant{"tag"}}, 3);
	}
}
#endif

TEST_F(ReindexerApi, ClearNumericCollatedStringArray) {
	constexpr auto kFieldCollateNumericStr{"collate_numeric_str"};

	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {kFieldCollateNumericStr, "hash", "string", IndexOpts().Array().SetCollateMode(CollateNumeric)});
	rt.UpsertJSON(default_namespace, R"({"id":1,"collate_numeric_str":["10","20","30"]})");
	rt.UpsertJSON(default_namespace, R"({"id":2,"collate_numeric_str":["20","10"]})");
	rt.UpsertJSON(default_namespace, R"({"id":3,"collate_numeric_str":["10","20"]})");
	rt.UpsertJSON(default_namespace, R"({"id":4,"collate_numeric_str":["10"]})");

	const Query by10{Query(default_namespace).Where(kFieldCollateNumericStr, CondEq, "10")};
	const Query by20{Query(default_namespace).Where(kFieldCollateNumericStr, CondEq, "20")};
	const Query by30{Query(default_namespace).Where(kFieldCollateNumericStr, CondEq, "30")};
	ASSERT_EQ(rt.Select(by10).Count(), 4);
	ASSERT_EQ(rt.Select(by20).Count(), 3);
	ASSERT_EQ(rt.Select(by30).Count(), 1);

	ASSERT_EQ(rt.Update(Query::FromSQL("UPDATE test_namespace SET collate_numeric_str = null WHERE id = 1")), 1);
	auto remaining{rt.Select(by10)};
	ASSERT_EQ(remaining.Count(), 3);
	for (auto& row : remaining) {
		EXPECT_NE(row.GetItem()["id"].As<int>(), 1);
	}
	EXPECT_EQ(rt.Select(by20).Count(), 2);
	EXPECT_EQ(rt.Select(by30).Count(), 0);

	ASSERT_EQ(rt.Update(Query::FromSQL("UPDATE test_namespace SET collate_numeric_str = null")), 4);
	EXPECT_EQ(rt.Select(Query(default_namespace).Where(kFieldCollateNumericStr, CondEmpty, Variant{})).Count(), 4);
	EXPECT_EQ(rt.Select(by10).Count(), 0);
	EXPECT_EQ(rt.Select(by20).Count(), 0);
	EXPECT_EQ(rt.Select(by30).Count(), 0);
}

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

	void ExecuteAndCheckResult(const Query& q, const std::string& item) { ExecuteAndCheckResult(Impl(q), item); }
	void ExecuteAndCheckResult(reindexer::ConstQueryImpl q, const std::string& item) {
		SCOPED_TRACE(q.GetSQL());
		reindexer::QueryResults res;
		switch (q.Type()) {
			case QuerySelect:
				res = rt.Select(*q);
				break;
			case QueryUpdate:
				rt.Update(*q, res);
				break;
			case QueryDelete:
				rt.Delete(*q, res);
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
		ExecuteAndCheckResult(q, R"({"id":1,"array":[10,11],"v1":10,"v2":200,"v3":1000,"v4":"v4"})");
	}
	{
		auto q =
			Query(default_namespace).Set(kFieldV1, 20).Set(kFieldV4, "str").Set(kFieldArray, {10, 11, 20, 30}).Where(kFieldId, CondEq, 1);
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
		ExecuteAndCheckResult(q, R"({"id":1,"array":[11,11],"v1":11,"v2":200,"v3":1000,"v4":"str"})");
	}
	{
		auto q = Query(default_namespace).Set(kFieldV1, 12).Set(kFieldArray, {12, 12}).Set(kFieldV4, "a").Where(kFieldId, CondEq, 1);
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
		ExecuteAndCheckResult(q, R"({"id":1,"array":[23,23],"v1":23,"v2":22,"v3":1000,"v4":"b"})");
	}
}

}  // namespace reindexer_tests
