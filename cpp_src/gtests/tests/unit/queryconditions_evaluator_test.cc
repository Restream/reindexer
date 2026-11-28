#include "core/query/queryconditions_evaluator.h"
#include <gtest/gtest.h>
#include <limits>
#include <set>
#include "core/index/indexing_condition.h"
#include "core/nsselecter/comparator/comparator_indexed.h"
#include "core/query/expression/arithmetic_expression.h"
#include "core/query/query_impl.h"
#include "queries_verifier.h"
#include "reindexer_api.h"

namespace reindexer_tests {

class [[nodiscard]] QueryConditionsEvaluatorApi : public ReindexerApi {
protected:
	static std::set<int> ids(const reindexer::QueryResults& result) {
		std::set<int> values;
		for (auto it : result) {
			values.insert(it.GetItem()["id"].As<int>());
		}
		return values;
	}

	void check(const reindexer::Query& query, const std::set<int>& expected) {
		using namespace reindexer;
		SCOPED_TRACE(query.GetSQL());
		EXPECT_EQ(ids(rt.Select(query)), expected);
		const auto all = rt.Select(Query(Impl(query).NsName()));
		const auto pt = all.GetPayloadType(0);
		const auto tm = all.GetTagsMatcher(0);
		QueryEntries entries{Impl(query).Entries()};
		const auto bind = [&pt, &tm](QueryField& field) {
			int index = 0;
			if (pt.FieldByName(field.FieldName(), index)) {
				const auto type = pt.Field(index).Type();
				field.SetIndexData(index, field.FieldName(), FieldsSet{index}, type, type, {});
			} else {
				field.SetField(FieldsSet{tm.path2tag(field.FieldName())});
			}
		};
		for (size_t i = 0; i < entries.Size(); ++i) {
			if (entries.Is<QueryEntry>(i)) {
				auto& entry = entries.Get<QueryEntry>(i);
				bind(entry.FieldData());
				if (entry.IsFieldIndexed()) {
					IndexingCondition::VerifyQueryEntry(entry, nullptr, pt);
				}
			} else if (entries.Is<BetweenFieldsQueryEntry>(i)) {
				auto& entry = entries.Get<BetweenFieldsQueryEntry>(i);
				bind(entry.LeftFieldData());
				bind(entry.RightFieldData());
			}
		}
		const auto evaluator = QueryConditionsEvaluator::PrepareForEvaluation(entries, pt);
		std::set<int> actual;
		for (auto it : all) {
			const auto item = it.GetItemRef();
			if (evaluator.Evaluate(entries, ConstPayload{pt, item.Value()})) {
				actual.insert(it.GetItem()["id"].As<int>());
			}
		}
		EXPECT_EQ(actual, expected);
	}
};

class [[nodiscard]] QueryConditionsVerifierApi : public QueryConditionsEvaluatorApi, public QueriesVerifier {};

TEST_F(QueryConditionsVerifierApi, NanComparisonsMatchWhere) {
	using namespace reindexer;
	const std::string ns = "nan_conditions_verifier";
	rt.OpenNamespace(ns);
	rt.AddIndex(ns, IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	rt.AddIndex(ns, IndexDef{"first", {"first"}, "-", "int", IndexOpts{}});
	rt.AddIndex(ns, IndexDef{"value", {"value"}, "-", "double", IndexOpts{}});
	rt.AddIndex(ns, IndexDef{"pair", {"first", "value"}, "hash", "composite", IndexOpts{}});
	setPkFields(ns, std::vector<std::string>{"id"});
	addIndexFields(ns, "id", {{.name = "id", .type = KeyValueType::Int{}}});
	addIndexFields(ns, "first", {{.name = "first", .type = KeyValueType::Int{}}});
	addIndexFields(ns, "value", {{.name = "value", .type = KeyValueType::Double{}}});
	addIndexFields(ns, "pair", {{.name = "first", .type = KeyValueType::Int{}}, {.name = "value", .type = KeyValueType::Double{}}});
	const Variant nan{std::numeric_limits<double>::quiet_NaN()};
	const struct [[nodiscard]] Row {
		int first;
		Variant value;
		Variant peer;
	} rows[] = {
		{.first = 1, .value = nan, .peer = Variant{1}},
		{.first = 1, .value = Variant{1.0}, .peer = nan},
		{.first = 2, .value = nan, .peer = nan},
		{.first = 0, .value = Variant{2.0}, .peer = Variant{2}},
	};
	int id = 0;
	for (const auto& row : rows) {
		auto item = rt.NewItem(ns);
		ASSERT_TRUE(item.FromJSON(R"({"id":0,"first":0,"value":0,"raw":0,"peer":0})").ok());
		item["id"] = ++id;
		item["first"] = row.first;
		item["value"] = row.value;
		item["raw"] = row.value;
		item["peer"] = row.peer;
		rt.Upsert(ns, item);
		saveItem(std::move(item), ns);
	}
	const auto verify = [this](Query query, const std::set<int>& expected) {
		SCOPED_TRACE(query.GetSQL());
		const auto result = rt.Select(query);
		EXPECT_EQ(ids(result), expected);
		Verify(result, std::move(query), *rt.reindexer);
	};
	for (const auto* field : {"raw", "value"}) {
		verify(Query(ns).Where(field, CondEq, {1}), {2});
		verify(Query(ns).Where(field, CondGt, {1}), {4});
		verify(Query(ns).Where(field, CondEq, VariantArray{nan}), {});
	}
	verify(Query(ns).WhereBetweenFields("raw", CondEq, "peer"), {4});
	verify(Query(ns).WhereBetweenFields("raw", CondGt, "peer"), {});
	verify(Query(ns).Where("pair", CondEq, VariantArray{Variant{VariantArray{Variant{1}, nan}}}), {});
	verify(Query(ns).Where("pair", CondLt, VariantArray{Variant{VariantArray{Variant{1}, Variant{2}}}}), {2, 4});
	verify(Query(ns).Where("pair", CondGt, VariantArray{Variant{VariantArray{Variant{1}, Variant{2}}}}), {3});
	verify(Query(ns).Where("pair", CondLt, VariantArray{Variant{VariantArray{Variant{2}, Variant{2}}}}), {1, 2, 4});
}

TEST_F(QueryConditionsEvaluatorApi, NonIndexedNanDoesNotMatch) {
	using namespace reindexer;
	const std::string ns = "nonindexed_nan_conditions";
	rt.OpenNamespace(ns);
	rt.AddIndex(ns, IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	const Variant infinity{std::numeric_limits<double>::infinity()};
	const Variant minusInfinity{-std::numeric_limits<double>::infinity()};
	for (const auto& nan : {Variant{std::numeric_limits<double>::quiet_NaN()}, Variant{std::numeric_limits<float>::quiet_NaN()}}) {
		SCOPED_TRACE(nan.Type().Name());
		rt.TruncateNamespace(ns);
		const struct [[nodiscard]] Row {
			VariantArray x;
			VariantArray y;
		} rows[] = {
			{.x = {nan}, .y = {Variant{1}}},
			{.x = {Variant{1}}, .y = {nan}},
			{.x = {nan, Variant{1}}, .y = {Variant{1}, Variant{1}}},
			{.x = {Variant{"1"}}, .y = {Variant{1}}},
			{.x = {Variant{2}}, .y = {Variant{2}}},
			{.x = {infinity}, .y = {infinity}},
			{.x = {minusInfinity}, .y = {minusInfinity}},
		};
		int id = 0;
		for (const auto& row : rows) {
			auto item = rt.NewItem(ns);
			ASSERT_TRUE(item.FromJSON(R"({"id":0,"x":[],"y":[],"bounds":[]})").ok());
			item["id"] = ++id;
			item["x"] = row.x;
			item["y"] = row.y;
			item["bounds"] = VariantArray{nan, Variant{2}};
			rt.Upsert(ns, item);
		}
		const struct [[nodiscard]] ComparisonCase {
			CondType condition;
			std::set<int> expected;
		} comparisons[] = {
			{.condition = CondEq, .expected = {2, 3, 4}},		{.condition = CondLt, .expected = {7}},
			{.condition = CondLe, .expected = {2, 3, 4, 7}},	{.condition = CondGt, .expected = {5, 6}},
			{.condition = CondGe, .expected = {2, 3, 4, 5, 6}},
		};
		for (const auto& test : comparisons) {
			for (const auto& one : {Variant{1}, Variant{int64_t{1}}, Variant{1.0F}, Variant{1.0}, Variant{"1"}}) {
				check(Query(ns).Where("x", test.condition, VariantArray{one}), test.expected);
			}
			check(Query(ns).Where("x", test.condition, VariantArray{nan}), {});
		}
		check(Query(ns).Where("x", CondRange, {1, 2}), {2, 3, 4, 5});
		check(Query(ns).Where("x", CondRange, VariantArray{nan, Variant{2}}), {});
		check(Query(ns).Where("x", CondRange, VariantArray{Variant{1}, nan}), {});
		check(Query(ns).WhereBetweenFields("x", CondRange, "bounds"), {});
		check(Query(ns).Where("x", CondSet, VariantArray{nan, Variant{1}, Variant{2}}), {2, 3, 4, 5});
		check(Query(ns).Where(
				  "x", CondSet,
				  VariantArray{nan, Variant{1}, Variant{2}, Variant{3}, Variant{4}, Variant{5}, Variant{6}, Variant{7}, Variant{8}}),
			  {2, 3, 4, 5});
		check(Query(ns).Where("x", CondAllSet, {1}), {2, 3, 4});
		check(Query(ns).Where("x", CondAllSet, {1, 1}), {2, 3, 4});
		check(Query(ns).Where("x", CondAllSet, {1, 2}), {});
		check(Query(ns).Where("x", CondAllSet, VariantArray{nan, Variant{1}}), {});
		check(Query(ns).Where("x", CondAllSet, VariantArray{Variant{1}, nan}), {});
		for (const auto condition : {CondEq, CondLe, CondGe, CondSet, CondAllSet}) {
			check(Query(ns).WhereBetweenFields("x", condition, "y"), {3, 5, 6, 7});
		}
		for (const auto condition : {CondLt, CondGt}) {
			check(Query(ns).WhereBetweenFields("x", condition, "y"), {});
		}
		check(Query(ns).Not().Where("x", CondEq, VariantArray{nan}), {1, 2, 3, 4, 5, 6, 7});
	}
}

TEST_F(QueryConditionsEvaluatorApi, IndexedAndArithmeticNanDoesNotMatch) {
	using namespace reindexer;
	using expressions::ArithmeticExpression;
	const std::string ns = "indexed_nan_conditions";
	rt.OpenNamespace(ns);
	rt.AddIndex(ns, IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	// Store indexes exercise comparators without using NaN as an ordered or hashed index key.
	rt.AddIndex(ns, IndexDef{"value", {"value"}, "-", "double", IndexOpts{}});
	rt.AddIndex(ns, IndexDef{"values", {"values"}, "-", "double", IndexOpts{}.Array()});
	const Variant nan{std::numeric_limits<double>::quiet_NaN()};
	const Variant infinity{std::numeric_limits<double>::infinity()};
	int id = 0;
	for (const auto& value : {nan, Variant{1.0}, Variant{2.0}, infinity}) {
		auto item = rt.NewItem(ns);
		item["id"] = ++id;
		item["value"] = value;
		item["values"] = id == 2 ? VariantArray{Variant{1}, Variant{2}} : VariantArray{nan, value};
		rt.Upsert(ns, item);
	}
	for (const auto condition : {CondEq, CondLt, CondLe, CondGt, CondGe}) {
		check(Query(ns).Where("value", condition, VariantArray{nan}), {});
		check(Query(ns).Where("values", condition, VariantArray{nan}), {});
		EXPECT_TRUE(ids(rt.Select(Query(ns).Where(ArithmeticExpression("value + value"), condition, VariantArray{nan}))).empty());
	}
	check(Query(ns).Where("value", CondEq, 1), {2});
	check(Query(ns).Where("value", CondLt, 2), {2});
	check(Query(ns).Where("value", CondGe, 1), {2, 3, 4});
	check(Query(ns).Where("values", CondSet, VariantArray{nan, Variant{1}}), {2});
	check(Query(ns).Where("values", CondAllSet, {1, 1}), {2});
	check(Query(ns).Where("values", CondAllSet, {1, 2}), {2});
	check(Query(ns).Where("values", CondAllSet, VariantArray{nan, Variant{1}, Variant{2}}), {});
	check(Query(ns).Where("values", CondAllSet, VariantArray{nan, Variant{1}}), {});
	check(Query(ns).Where("values", CondAllSet, VariantArray{Variant{1}, nan}), {});
	check(Query(ns).Where("values", CondRange, VariantArray{nan, Variant{2}}), {});
	check(Query(ns).WhereBetweenFields("value", CondEq, "values"), {2, 3, 4});
	const struct [[nodiscard]] ArithmeticCase {
		CondType condition;
		std::set<int> expected;
	} cases[] = {
		{.condition = CondEq, .expected = {2}},	   {.condition = CondLt, .expected = {}},		 {.condition = CondLe, .expected = {2}},
		{.condition = CondGt, .expected = {3, 4}}, {.condition = CondGe, .expected = {2, 3, 4}},
	};
	for (const auto& test : cases) {
		EXPECT_EQ(ids(rt.Select(Query(ns).Where(ArithmeticExpression("value + value"), test.condition, {2}))), test.expected);
	}
	EXPECT_EQ(ids(rt.Select(Query(ns).Where("value", CondLt, ArithmeticExpression("value + value")))), (std::set<int>{2, 3}));
}

TEST_F(QueryConditionsEvaluatorApi, CompositeNanMatchesSelectComparator) {
	using namespace reindexer;
	const std::string ns = "composite_nan_conditions";
	rt.OpenNamespace(ns);
	rt.AddIndex(ns, IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	rt.AddIndex(ns, IndexDef{"first", {"first"}, "-", "int", IndexOpts{}});
	rt.AddIndex(ns, IndexDef{"second", {"second"}, "-", "double", IndexOpts{}});
	const Variant nan{std::numeric_limits<double>::quiet_NaN()};
	const struct [[nodiscard]] Row {
		int first;
		Variant second;
	} rows[] = {
		{.first = 1, .second = nan},		  {.first = 1, .second = Variant{1.0}}, {.first = 2, .second = nan},
		{.first = 0, .second = Variant{5.0}}, {.first = 1, .second = Variant{2.0}},
	};
	int id = 0;
	for (const auto& row : rows) {
		auto item = rt.NewItem(ns);
		item["id"] = ++id;
		item["first"] = row.first;
		item["second"] = row.second;
		rt.Upsert(ns, item);
	}
	const auto all = rt.Select(Query(ns));
	const auto pt = all.GetPayloadType(0);
	const FieldsSet fields{pt.FieldByName("first"), pt.FieldByName("second")};
	const Variant one{VariantArray{Variant{1}, Variant{1}}};
	const Variant nonFinite{VariantArray{Variant{1}, nan}};
	const struct [[nodiscard]] TestCase {
		CondType condition;
		VariantArray values;
		std::set<int> expected;
	} cases[] = {
		{.condition = CondEq, .values = {one}, .expected = {2}},
		{.condition = CondLt, .values = {one}, .expected = {4}},
		{.condition = CondLe, .values = {one}, .expected = {2, 4}},
		{.condition = CondGt, .values = {one}, .expected = {3, 5}},
		{.condition = CondGe, .values = {one}, .expected = {2, 3, 5}},
		{.condition = CondEq, .values = {nonFinite}, .expected = {}},
		{.condition = CondLt, .values = {nonFinite}, .expected = {4}},
		{.condition = CondGt, .values = {nonFinite}, .expected = {3}},
		{.condition = CondRange, .values = {one, Variant{VariantArray{Variant{2}, Variant{3}}}}, .expected = {2, 5}},
		{.condition = CondSet, .values = {nonFinite, one, Variant{VariantArray{Variant{0}, Variant{5}}}}, .expected = {2, 4}},
		{.condition = CondAllSet, .values = {one, one}, .expected = {2}},
		{.condition = CondAllSet, .values = {nonFinite, one}, .expected = {}},
	};
	const CollateOpts collate;
	for (const auto& test : cases) {
		SCOPED_TRACE(CondTypeToStr(test.condition));
		const auto query = Query(ns).Where("pair", test.condition, test.values);
		QueryEntries entries{Impl(query).Entries()};
		auto& entry = entries.Get<QueryEntry>(0);
		entry.SetIndexData(0, "pair", FieldsSet{fields}, KeyValueType::Composite{}, KeyValueType::Composite{},
						   {KeyValueType::Int{}, KeyValueType::Double{}});
		IndexingCondition::VerifyQueryEntry(entry, nullptr, pt);
		const auto evaluator = QueryConditionsEvaluator::PrepareForEvaluation(entries, pt);
		comparators::ComparatorIndexedCompositeBase comparator{entry.Values(), collate, fields, pt, test.condition};
		std::set<int> evaluated;
		std::set<int> selected;
		for (auto it : all) {
			const auto item = it.GetItemRef();
			const auto itemId = it.GetItem()["id"].As<int>();
			if (evaluator.Evaluate(entries, ConstPayload{pt, item.Value()})) {
				evaluated.insert(itemId);
			}
			if (comparator.CompareItems(item.Value())) {
				selected.insert(itemId);
			}
		}
		EXPECT_EQ(evaluated, test.expected);
		EXPECT_EQ(selected, test.expected);
	}
}

TEST_F(QueryConditionsEvaluatorApi, CompositeHashNanDoesNotMatch) {
	using namespace reindexer;
	const std::string ns = "composite_hash_nan_conditions";
	rt.OpenNamespace(ns);
	rt.AddIndex(ns, IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	rt.AddIndex(ns, IndexDef{"first", {"first"}, "-", "int", IndexOpts{}});
	rt.AddIndex(ns, IndexDef{"second", {"second"}, "-", "double", IndexOpts{}});
	rt.AddIndex(ns, IndexDef{"pair", {"first", "second"}, "hash", "composite", IndexOpts{}});
	const Variant nan{std::numeric_limits<double>::quiet_NaN()};
	int id = 0;
	for (const auto& value : {nan, Variant{1.0}, Variant{2.0}}) {
		auto item = rt.NewItem(ns);
		item["id"] = ++id;
		item["first"] = id == 3 ? 2 : 1;
		item["second"] = value;
		rt.Upsert(ns, item);
	}
	const auto all = rt.Select(Query(ns));
	const auto pt = all.GetPayloadType(0);
	const FieldsSet fields{pt.FieldByName("first"), pt.FieldByName("second")};
	const Variant normal{VariantArray{Variant{1}, Variant{1}}};
	const Variant nonFinite{VariantArray{Variant{1}, nan}};
	const struct [[nodiscard]] TestCase {
		CondType condition;
		VariantArray values;
		std::set<int> expected;
	} cases[] = {
		{.condition = CondEq, .values = {nonFinite}, .expected = {}},
		{.condition = CondSet, .values = {nonFinite}, .expected = {}},
		{.condition = CondAllSet, .values = {nonFinite}, .expected = {}},
		{.condition = CondSet, .values = {nonFinite, normal}, .expected = {2}},
		{.condition = CondAllSet, .values = {nonFinite, normal}, .expected = {}},
		{.condition = CondEq, .values = {normal}, .expected = {2}},
		{.condition = CondSet, .values = {normal}, .expected = {2}},
		{.condition = CondAllSet, .values = {normal, normal}, .expected = {2}},
	};
	for (const auto& test : cases) {
		const auto query = Query(ns).Where("pair", test.condition, test.values);
		SCOPED_TRACE(query.GetSQL());
		EXPECT_EQ(ids(rt.Select(query)), test.expected);
		QueryEntries entries{Impl(query).Entries()};
		auto& entry = entries.Get<QueryEntry>(0);
		entry.SetIndexData(0, "pair", FieldsSet{fields}, KeyValueType::Composite{}, KeyValueType::Composite{},
						   {KeyValueType::Int{}, KeyValueType::Double{}});
		IndexingCondition::VerifyQueryEntry(entry, nullptr, pt);
		const auto evaluator = QueryConditionsEvaluator::PrepareForEvaluation(entries, pt);
		std::set<int> evaluated;
		for (auto it : all) {
			const auto item = it.GetItemRef();
			if (evaluator.Evaluate(entries, ConstPayload{pt, item.Value()})) {
				evaluated.insert(it.GetItem()["id"].As<int>());
			}
		}
		EXPECT_EQ(evaluated, test.expected);
	}
}

}  // namespace reindexer_tests
