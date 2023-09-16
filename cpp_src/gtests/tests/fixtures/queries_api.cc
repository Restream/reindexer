#include "queries_api.h"

void QueriesApi::CheckMergeQueriesWithLimit() {
	Query q = Query{default_namespace}.Merge(Query{joinNs}.Limit(1));
	QueryResults qr;
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Limit and offset in inner merge query is not allowed");

	q = Query{default_namespace}.Merge(Query{joinNs}.Offset(1));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Limit and offset in inner merge query is not allowed");

	q = Query{default_namespace}.Merge(Query{joinNs}.Sort(kFieldNameId, false));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in inner merge query is not allowed");  // TODO #1449

	q = Query{default_namespace}.Merge(Query{joinNs}).Sort(kFieldNameId, false);
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in merge query is not implemented yet");	 // TODO #1449

	q = Query{default_namespace}.Where(kFieldNameDescription, CondEq, RandString()).Merge(Query{joinNs});
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "In merge query without sorting all subqueries should be fulltext or not fulltext at the same time");

	q = Query{default_namespace}.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString()));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "In merge query without sorting all subqueries should be fulltext or not fulltext at the same time");

	q = Query{default_namespace}.Where(kFieldNameDescription, CondEq, RandString()).Merge(Query{joinNs}).Sort(kFieldNameId, false);
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in merge query is not implemented yet");	 // TODO #1449

	q = Query{default_namespace}.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString())).Sort(kFieldNameId, false);
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in merge query is not implemented yet");	 // TODO #1449

	qr.Clear();
	q = Query{default_namespace}.Merge(Query{joinNs}).Limit(10);
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 10);
	EXPECT_EQ(qr.getMergedNSCount(), 2);

	qr.Clear();
	q = Query{default_namespace}.Merge(Query{joinNs}).Offset(10);
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.getMergedNSCount(), 2);

	q = Query{default_namespace}
			.Where(kFieldNameDescription, CondEq, RandString())
			.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString()));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.getMergedNSCount(), 2);
}

void QueriesApi::CheckMergeQueriesWithAggregation() {
	auto AggSelect = [this](const Query& q, AggType tp, double& val) -> void {
		QueryResults qr;
		Error err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.aggregationResults.size(), 1);
		ASSERT_TRUE(qr.aggregationResults[0].GetValue().has_value());
		ASSERT_EQ(qr.aggregationResults[0].type, tp);
		val = qr.aggregationResults[0].GetValue().value();
		if (tp == AggCount || tp == AggCountCached) {
			ASSERT_EQ(val, qr.totalCount);
		}
	};
	// check the correctness of the aggregation functions with the merge query
	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggCount, {}), AggCount, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggCount, {}), AggCount, c2);
		double c3;
		AggSelect(Query{default_namespace}.Aggregate(AggCount, {}).Merge(Query{joinNs}), AggCount, c3);
		double c4;
		AggSelect(Query{testSimpleNs}.Aggregate(AggCount, {}), AggCount, c4);
		double c5;
		AggSelect(Query{default_namespace}.Aggregate(AggCount, {}).Merge(Query{joinNs}).Merge(Query{testSimpleNs}), AggCount, c5);
		{
			QueryResults qr;
			Error err = rt.reindexer->Select(Query{default_namespace}.ReqTotal().Merge(Query{joinNs}).Merge(Query{testSimpleNs}), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.totalCount, c5);
		}
		ASSERT_EQ(c1 + c2, c3);
		ASSERT_EQ(c1 + c2 + c4, c5);
	}

	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggCount, {}), AggCount, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggCount, {}), AggCount, c2);
		double c4;
		AggSelect(Query{testSimpleNs}.Aggregate(AggCount, {}), AggCount, c4);
		double c3;
		Query q3;
		q3.FromSQL(fmt::sprintf("SELECT count(*) FROM %s MERGE (SELECT * FROM %s)", default_namespace, joinNs));
		AggSelect(q3, AggCount, c3);
		double c5;
		Query q5;
		q5.FromSQL(fmt::sprintf("SELECT count(*) FROM %s MERGE (SELECT * FROM %s) MERGE (SELECT * FROM %s)", default_namespace, joinNs,
								testSimpleNs));
		AggSelect(q5, AggCount, c5);
		{
			QueryResults qr;
			Error err = rt.reindexer->Select(Query{default_namespace}.CachedTotal().Merge(Query{joinNs}).Merge(Query{testSimpleNs}), qr);
			ASSERT_EQ(qr.totalCount, c5);
		}
		ASSERT_EQ(c1 + c2, c3);
		ASSERT_EQ(c1 + c2 + c4, c5);
	}

	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggCountCached, {}), AggCountCached, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggCountCached, {}), AggCountCached, c2);
		double c3;
		AggSelect(Query{default_namespace}.Aggregate(AggCountCached, {}).Merge(Query{joinNs}), AggCountCached, c3);
		double c4;
		AggSelect(Query{testSimpleNs}.Aggregate(AggCountCached, {}), AggCountCached, c4);
		double c5;
		AggSelect(Query{default_namespace}.Aggregate(AggCountCached, {}).Merge(Query{joinNs}).Merge(Query{testSimpleNs}), AggCountCached, c5);
		ASSERT_EQ(c1 + c2, c3);
		ASSERT_EQ(c1 + c2 + c4, c5);
	}

	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggMin, {"id"}), AggMin, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggMin, {"id"}), AggMin, c2);
		double c3;
		AggSelect(Query{default_namespace}.Aggregate(AggMin, {"id"}).Merge(Query{joinNs}), AggMin, c3);
		double c4;
		AggSelect(Query{testSimpleNs}.Aggregate(AggMin, {"id"}), AggMin, c4);
		double c5;
		AggSelect(Query{default_namespace}.Aggregate(AggMin, {"id"}).Merge(Query{joinNs}).Merge(Query{testSimpleNs}), AggMin, c5);
		ASSERT_EQ(std::min(c1, c2), c3);
		ASSERT_EQ(std::min({c1, c2, c4}), c5);
	}
	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggMax, {"id"}), AggMax, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggMax, {"id"}), AggMax, c2);
		double c3;
		AggSelect(Query{default_namespace}.Aggregate(AggMax, {"id"}).Merge(Query{joinNs}), AggMax, c3);
		double c4;
		AggSelect(Query{testSimpleNs}.Aggregate(AggMax, {"id"}), AggMax, c4);
		double c5;
		AggSelect(Query{default_namespace}.Aggregate(AggMax, {"id"}).Merge(Query{joinNs}).Merge(Query{testSimpleNs}), AggMax, c5);
		ASSERT_EQ(std::max(c1, c2), c3);
		ASSERT_EQ(std::max({c1, c2, c4}), c5);
	}

	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggSum, {"id"}), AggSum, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggSum, {"id"}), AggSum, c2);
		double c3;
		AggSelect(Query{default_namespace}.Aggregate(AggSum, {"id"}).Merge(Query{joinNs}), AggSum, c3);
		double c4;
		AggSelect(Query{testSimpleNs}.Aggregate(AggSum, {"id"}), AggSum, c4);
		double c5;
		AggSelect(Query{default_namespace}.Aggregate(AggSum, {"id"}).Merge(Query{joinNs}).Merge(Query{testSimpleNs}), AggSum, c5);
		ASSERT_EQ(c1 + c2, c3);
		ASSERT_EQ(c1 + c2 + c4, c5);
	}
	// check the correctness of the error for unsupported cases
	{
		QueryResults qr;
		Error err = rt.reindexer->Select(Query{default_namespace}.Aggregate(AggSum, {"id"}).Limit(10).Offset(10).Merge(Query{joinNs}), qr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Limit and offset are not supported for aggregations 'sum'");
	}
	{
		QueryResults qr;
		Error err = rt.reindexer->Select(Query{default_namespace}.Aggregate(AggMin, {"id"}).Limit(10).Merge(Query{joinNs}), qr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Limit and offset are not supported for aggregations 'min'");
	}
	{
		QueryResults qr;
		Error err = rt.reindexer->Select(Query{default_namespace}.Aggregate(AggMax, {"id"}).Offset(10).Merge(Query{joinNs}), qr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Limit and offset are not supported for aggregations 'max'");
	}
	{
		QueryResults qr;
		Error err = rt.reindexer->Select(Query{default_namespace}.Merge(Query{joinNs}.ReqTotal()), qr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Aggregations in inner merge query is not allowed");
	}
	{
		QueryResults qr;
		Error err = rt.reindexer->Select(Query{default_namespace}.Merge(Query{joinNs}.CachedTotal()), qr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Aggregations in inner merge query is not allowed");
	}
	// checking the work of several aggregate functions with the merge query
	{
		Query q = Query{default_namespace}
					  .Aggregate(AggSum, {"id"})
					  .Aggregate(AggCount, {})
					  .Aggregate(AggMin, {"id"})
					  .Merge(Query{joinNs})
					  .Merge(Query{testSimpleNs});
		QueryResults qr;
		Error err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.aggregationResults.size(), 3);
		for (auto a : {AggSum, AggCount, AggMin}) {
			int exist = 0;
			for (const auto& ar : qr.aggregationResults) {
				if (ar.type == a) {
					exist++;
				}
			}
			ASSERT_EQ(exist, 1);
		}
	}
	{
		double c1;
		AggSelect(Query{default_namespace}.Aggregate(AggCount, {}), AggCount, c1);
		double c2;
		AggSelect(Query{joinNs}.Aggregate(AggCount, {}), AggCount, c2);
		double c3;
		AggSelect(Query{testSimpleNs}.Aggregate(AggCount, {}), AggCount, c3);

		Query q =
			Query{default_namespace}.Aggregate(AggCount, {}).Aggregate(AggCountCached, {}).Merge(Query{joinNs}).Merge(Query{testSimpleNs});
		QueryResults qr;
		Error err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.aggregationResults.size(), 1);
		ASSERT_EQ(qr.aggregationResults[0].type, AggCount);
		ASSERT_EQ(qr.aggregationResults[0].GetValueOrZero(), c1 + c2 + c3);
	}
}

static struct {
	reindexer::KeyValueType fieldType;
	std::vector<std::string> indexTypes;
} const fieldIndexTypes[]{// TODO add type Point #1352
						  {reindexer::KeyValueType::Bool{}, {"-"}},
						  {reindexer::KeyValueType::Int{}, {"-", "hash", "tree"}},
						  {reindexer::KeyValueType::Int64{}, {"-", "hash", "tree"}},
						  {reindexer::KeyValueType::Double{}, {"-", "tree"}},
						  {reindexer::KeyValueType::String{}, {"-", "hash", "tree"}},
						  {reindexer::KeyValueType::Uuid{}, {"hash"}}};

static std::string createIndexName(const std::string& fieldType, const std::string& indexType, bool isArray, bool isSparse) {
	return fieldType + '_' + indexType + (isArray ? "_array" : "") + (isSparse ? "_sparse" : "");
}

void QueriesApi::initConditionsNs() {
	auto err = rt.reindexer->OpenNamespace(conditionsNs);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(conditionsNs, {kFieldNameId, "hash", "int", IndexOpts{}.PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	for (const auto& fit : fieldIndexTypes) {
		for (const auto& it : fit.indexTypes) {
			for (const bool isArray : {true, false}) {
				for (const bool isSparse : {true, false}) {
					if (isSparse && fit.fieldType.Is<reindexer::KeyValueType::Uuid>()) {  // TODO remove this #1470
						continue;
					}
					const std::string fieldType{fit.fieldType.Name()};
					const std::string indexName{createIndexName(fieldType, it, isArray, isSparse)};
					err = rt.reindexer->AddIndex(conditionsNs, {indexName, it, fieldType, IndexOpts{}.Array(isArray).Sparse(isSparse)});
					addIndexFields(conditionsNs, indexName, {{indexName, fit.fieldType}});
					ASSERT_TRUE(err.ok()) << err.what();
				}
			}
		}
	}
	setPkFields(conditionsNs, {kFieldNameId});
}

static reindexer::Variant createRandValue(int id, reindexer::KeyValueType fieldType) {
	using namespace reindexer;
	return fieldType.EvaluateOneOf(overloaded{
		[](KeyValueType::Bool) { return Variant{rand() % 2}; }, [id](KeyValueType::Int) { return Variant{id - 50 + rand() % 100}; },
		[id](KeyValueType::Int64) { return Variant{int64_t{id - 50 + rand() % 100}}; },
		[id](KeyValueType::Double) { return Variant{(id - 50 + rand() % 100) / 3000.0}; },
		[id](KeyValueType::String) { return Variant{std::to_string(id - 50 + rand() % 100)}; },
		[](KeyValueType::Uuid) { return (rand() % 2) ? Variant{randUuid()} : Variant{randStrUuid()}; },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Composite>) -> Variant { assert(0); }});
}

static constexpr size_t kMaxArraySize = 20;

void QueriesApi::FillConditionsNs() {
	static constexpr size_t kConditionsNsSize = 300;

	reindexer::WrSerializer ser;
	for (size_t id = conditionsNsSize; id < kConditionsNsSize + conditionsNsSize; ++id) {
		ser.Reset();
		{
			reindexer::JsonBuilder json{ser};
			json.Put(kFieldNameId, id);
			for (const auto& fit : fieldIndexTypes) {
				const std::string fieldType{fit.fieldType.Name()};
				for (const bool isSparse : {true, false}) {
					if (isSparse &&
						((rand() % 2) || fit.fieldType.Is<reindexer::KeyValueType::Uuid>())) {	// TODO remove fieldType check #1470
						continue;
					}
					for (const auto& it : fit.indexTypes) {
						for (const bool isArray : {true, false}) {
							const std::string indexName{createIndexName(fieldType, it, isArray, isSparse)};
							if (isArray) {
								auto arr = json.Array(indexName);
								for (size_t i = 0, s = rand() % kMaxArraySize; i < s; ++i) {
									arr.Put({}, createRandValue(id, fit.fieldType));
								}
							} else {
								json.Put(indexName, createRandValue(id, fit.fieldType));
							}
						}
					}
				}
				if (rand() % 2) {
					json.Put(fieldType, createRandValue(id, fit.fieldType));
				}
				if (rand() % 2) {
					auto arr = json.Array(fieldType + "_array");
					for (size_t i = 0, s = rand() % kMaxArraySize; i < s; ++i) {
						arr.Put({}, createRandValue(id, fit.fieldType));
					}
				}
			}
		}
		Item item = rt.reindexer->NewItem(conditionsNs);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		const auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		Upsert(conditionsNs, item);
		saveItem(std::move(item), conditionsNs);
	}
	conditionsNsSize += kConditionsNsSize;
}

static reindexer::VariantArray createRandArrValues(size_t min, size_t max, int id, reindexer::KeyValueType fieldType) {
	reindexer::VariantArray ret;
	const size_t count = (min == max) ? min : min + rand() % (max - min);
	ret.reserve(count);
	for (size_t i = 0; i < count; ++i) {
		ret.emplace_back(createRandValue(id, fieldType));
	}
	return ret;
}

void QueriesApi::checkAllConditions(const std::string& fieldName, reindexer::KeyValueType fieldType, NullAllowed nullAllowed) {
	for (const auto cond : {CondEq, CondSet, CondAllSet, CondLt, CondLe, CondGt, CondGe, CondRange, CondAny, CondEmpty, CondLike}) {
		size_t min = 0, max = rand() % kMaxArraySize;
		switch (cond) {
			case CondEq:
			case CondSet:
			case CondAllSet:
				break;
			case CondLike:
				if (!fieldType.Is<reindexer::KeyValueType::String>()) {
					continue;
				}
				[[fallthrough]];
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
				min = max = 1;
				break;
			case CondRange:
				min = max = 2;
				break;
			case CondAny:
			case CondEmpty:
				if (nullAllowed == NullAllowed::No) {
					continue;
				}
				min = max = 0;
				break;
			case CondDWithin:  // TODO #1352
				assert(0);
		}
		for (size_t i = 0; i < 3; ++i) {
			ExecuteAndVerify(
				reindexer::Query{conditionsNs}.Where(fieldName, cond, createRandArrValues(min, max, rand() % conditionsNsSize, fieldType)));
			if (min <= 1 && max >= 1) {
				ExecuteAndVerify(
					reindexer::Query{conditionsNs}.Where(fieldName, cond, createRandValue(rand() % conditionsNsSize, fieldType)));
			}
		}
	}
}

void QueriesApi::CheckConditions() {
	for (const auto& fit : fieldIndexTypes) {
		const std::string fieldType{fit.fieldType.Name()};
		for (const auto& it : fit.indexTypes) {
			for (const bool isArray : {true, false}) {
				for (const bool isSparse : {true, false}) {
					if (isSparse && fit.fieldType.Is<reindexer::KeyValueType::Uuid>()) {  // TODO remove this #1470
						continue;
					}
					const std::string indexName{createIndexName(fieldType, it, isArray, isSparse)};
					checkAllConditions(indexName, fit.fieldType, isArray || isSparse ? NullAllowed::Yes : NullAllowed::No);
				}
			}
		}
		if (fit.fieldType.Is<reindexer::KeyValueType::Uuid>()) {  // TODO remove this #1470
			continue;
		}
		checkAllConditions(fieldType, fit.fieldType, NullAllowed::Yes);
		checkAllConditions(fieldType + "_array", fit.fieldType, NullAllowed::Yes);
	}
}
