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
	EXPECT_EQ(qr.GetMergedNSCount(), 2);

	qr.Clear();
	q = Query{default_namespace}.Merge(Query{joinNs}).Offset(10);
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.GetMergedNSCount(), 2);

	q = Query{default_namespace}
			.Where(kFieldNameDescription, CondEq, RandString())
			.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString()));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.GetMergedNSCount(), 2);
}

void QueriesApi::CheckMergeQueriesWithAggregation() {
	auto AggSelect = [this](const Query& q, AggType tp, double& val) -> void {
		QueryResults qr;
		Error err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		auto aggs = qr.GetAggregationResults();
		ASSERT_EQ(aggs.size(), 1);
		ASSERT_TRUE(aggs[0].GetValue().has_value());
		ASSERT_EQ(aggs[0].type, tp);
		val = aggs[0].GetValue().value();
		if (tp == AggCount || tp == AggCountCached) {
			ASSERT_EQ(val, qr.TotalCount());
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
			ASSERT_EQ(qr.TotalCount(), c5);
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
		Query q3 = Query::FromSQL(fmt::sprintf("SELECT count(*) FROM %s MERGE (SELECT * FROM %s)", default_namespace, joinNs));
		AggSelect(q3, AggCount, c3);
		double c5;
		Query q5 = Query::FromSQL(fmt::sprintf("SELECT count(*) FROM %s MERGE (SELECT * FROM %s) MERGE (SELECT * FROM %s)",
											   default_namespace, joinNs, testSimpleNs));
		AggSelect(q5, AggCount, c5);
		{
			QueryResults qr;
			Error err = rt.reindexer->Select(Query{default_namespace}.CachedTotal().Merge(Query{joinNs}).Merge(Query{testSimpleNs}), qr);
			ASSERT_EQ(qr.TotalCount(), c5);
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
		AggSelect(Query{default_namespace}.Aggregate(AggCountCached, {}).Merge(Query{joinNs}).Merge(Query{testSimpleNs}), AggCountCached,
				  c5);
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
		EXPECT_EQ(err.what(), "Aggregations in inner merge query are not allowed");
	}
	{
		QueryResults qr;
		Error err = rt.reindexer->Select(Query{default_namespace}.Merge(Query{joinNs}.CachedTotal()), qr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Aggregations in inner merge query are not allowed");
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
		ASSERT_EQ(qr.GetAggregationResults().size(), 3);
		for (auto a : {AggSum, AggCount, AggMin}) {
			int exist = 0;
			for (const auto& ar : qr.GetAggregationResults()) {
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
		auto& aggs = qr.GetAggregationResults();
		ASSERT_EQ(aggs.size(), 1);
		ASSERT_EQ(aggs[0].type, AggCount);
		ASSERT_EQ(aggs[0].GetValueOrZero(), c1 + c2 + c3);
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
	addIndexFields(conditionsNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
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
					ASSERT_TRUE(err.ok()) << err.what();
					addIndexFields(conditionsNs, indexName, {{indexName, fit.fieldType}});
				}
			}
		}
	}
	setPkFields(conditionsNs, {kFieldNameId});
}

void QueriesApi::initUUIDNs() {
	const auto err = rt.reindexer->OpenNamespace(uuidNs);
	ASSERT_TRUE(err.ok()) << err.what();
	DefineNamespaceDataset(
		uuidNs,
		{
			IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
			IndexDeclaration{kFieldNameUuid, "hash", "uuid", IndexOpts{}, 0},
			/*IndexDeclaration{kFieldNameUuidSparse, "hash", "uuid", IndexOpts{}.Sparse(), 0}, // TODO uncomment this #1470
			IndexDeclaration{kFieldNameUuidNotIndex2, "hash", "uuid", IndexOpts{}, 0},
			IndexDeclaration{kFieldNameUuidNotIndex3, "hash", "uuid", IndexOpts{}.Sparse(), 0},*/
			IndexDeclaration{kFieldNameUuidArr, "hash", "uuid", IndexOpts{}.Array(), 0},
			// IndexDeclaration{kFieldNameUuidArrSparse, "hash", "uuid", IndexOpts{}.Array().Sparse(), 0} // TODO uncomment this #1470
		});
	for (const auto& idx :
		 {kFieldNameUuid, kFieldNameUuidArr /*, kFieldNameUuidSparse, kFieldNameUuidArrSparse*/}) {	 // TODO uncomment this #1470
		addIndexFields(uuidNs, idx, {{idx, reindexer::KeyValueType::Uuid{}}});
	}
	setPkFields(uuidNs, {kFieldNameId});
}

static reindexer::Variant createRandValue(int id, reindexer::KeyValueType fieldType) {
	using namespace reindexer;
	return fieldType.EvaluateOneOf(overloaded{
		[](KeyValueType::Bool) { return Variant{rand() % 2}; }, [id](KeyValueType::Int) { return Variant{id - 50 + rand() % 100}; },
		[id](KeyValueType::Int64) { return Variant{int64_t{id - 50 + rand() % 100}}; },
		[id](KeyValueType::Double) { return Variant{(id - 50 + rand() % 100) / 3000.0}; },
		[id](KeyValueType::String) { return Variant{std::to_string(id - 50 + rand() % 100)}; },
		[](KeyValueType::Uuid) { return (rand() % 2) ? Variant{randUuid()} : Variant{randStrUuid()}; },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Composite>) -> Variant {
			assert(0);
			std::abort();
		}});
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
	for (const auto cond : {CondEq, CondSet, CondAllSet, CondLt, CondLe, CondGt, CondGe, CondRange, CondAny, CondEmpty,
							CondLike}) {  // TODO CondDWithin #1352
		if (cond == CondLike && !fieldType.Is<reindexer::KeyValueType::String>()) {
			continue;
		}
		if (nullAllowed == NullAllowed::No && (cond == CondAny || cond == CondEmpty)) {
			continue;
		}
		const auto argsCount = minMaxArgs(cond, 20);
		for (size_t i = 0; i < 3; ++i) {
			ExecuteAndVerify(reindexer::Query{conditionsNs}.Where(
				fieldName, cond, createRandArrValues(argsCount.min, argsCount.max, rand() % conditionsNsSize, fieldType)));
			if (argsCount.min <= 1 && argsCount.max >= 1) {
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

void QueriesApi::FillUUIDNs() {
	static size_t lastId = 0;
	reindexer::WrSerializer ser;
	for (size_t i = lastId; i < uuidNsSize + lastId; ++i) {
		Item item = rt.reindexer->NewItem(uuidNs);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		if (rand() % 2) {
			ser.Reset();
			{
				reindexer::JsonBuilder json{ser};
				json.Put(kFieldNameId, i);
				json.Put(kFieldNameUuid, randStrUuid());
				/*if (rand() % 2) {
					json.Put(kFieldNameUuidSparse, randStrUuid()); // TODO uncomment this #1470
				}*/
				{
					auto arr = json.Array(kFieldNameUuidArr);
					for (size_t j = 0, s = rand() % 10; j < s; ++j) {
						arr.Put({}, randStrUuid());
					}
				}
				/*if (rand() % 2) {
					auto arr = json.Array(kFieldNameUuidArrSparse); // TODO uncomment this #1470
					for (size_t j = 0, s = rand() % 10; j < s; ++j) {
						arr.Put({}, randStrUuid());
					}
				}*/
				if (rand() % 2) {
					json.Put(kFieldNameUuidNotIndex, randStrUuid());
				}
				/*json.Put(kFieldNameUuidNotIndex2, randStrUuid()); // TODO uncomment this #1470
				if (rand() % 2) {
					json.Put(kFieldNameUuidNotIndex3, randStrUuid());
				}*/
				if (rand() % 2) {
					json.Put(kFieldNameRndString, RandString());
				}
			}
			const auto err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			item[kFieldNameId] = int(i);
			if (rand() % 2) {
				item[kFieldNameUuid] = randUuid();
			} else {
				item[kFieldNameUuid] = randStrUuid();
			}
			/*if (rand() % 2) {
				item[kFieldNameUuidSparse] = randUuid(); // TODO uncomment this #1470
			}*/
			item[kFieldNameUuidArr] = randHeterogeneousUuidArray(0, 20);
			/*if (rand() % 2) {
				item[kFieldNameUuidArrSparse] = randHeterogeneousUuidArray(0, 20); // TODO uncomment this #1470
			}
			if (rand() % 2) {
				item[kFieldNameUuidNotIndex2] = randUuid();
			} else {
				item[kFieldNameUuidNotIndex2] = randStrUuid();
			}
			if (rand() % 2) {
				if (rand() % 2) {
					item[kFieldNameUuidNotIndex3] = randUuid();
				} else {
					item[kFieldNameUuidNotIndex3] = randStrUuid();
				}
			}*/
		}
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		Upsert(uuidNs, item);
		saveItem(std::move(item), uuidNs);
	}
	lastId += uuidNsSize;
}

void QueriesApi::CheckUUIDQueries() {
	for (size_t i = 0; i < 10; ++i) {
		for (const auto& field : {
				 kFieldNameUuid, kFieldNameUuidArr, kFieldNameUuidNotIndex, kFieldNameRndString /*,
				  kFieldNameUuidSparse, kFieldNameUuidArrSparse, kFieldNameUuidNotIndex2, kFieldNameUuidNotIndex3*/
			 }) {																				// TODO uncomment this #1470
			for (auto cond : {CondEq, CondLe, CondLt, CondSet, CondGe, CondGt, CondAllSet, CondRange}) {
				const auto argsCount = minMaxArgs(cond, 20);
				if (argsCount.min <= 1 && argsCount.max >= 1) {
					ExecuteAndVerify(Query(uuidNs).Where(field, cond, randUuid()));
					ExecuteAndVerify(Query(uuidNs).Where(field, cond, randStrUuid()));
				}
				ExecuteAndVerify(Query(uuidNs).Where(field, cond, randUuidArray(argsCount.min, argsCount.max)));
				ExecuteAndVerify(Query(uuidNs).Where(field, cond, randStrUuidArray(argsCount.min, argsCount.max)));
				ExecuteAndVerify(Query(uuidNs).Where(field, cond, randHeterogeneousUuidArray(argsCount.min, argsCount.max)));
			}
		}
	}
}

void QueriesApi::checkSqlQuery(std::string_view sqlQuery, Query&& checkQuery) {
	QueryResults sqlQr;
	Error err = rt.reindexer->Select(sqlQuery, sqlQr);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults checkQr;
	err = rt.reindexer->Select(checkQuery, checkQr);
	ASSERT_TRUE(err.ok()) << err.what();

	CompareQueryResults(sqlQuery, sqlQr, checkQr);
	Verify(checkQr, std::move(checkQuery), *rt.reindexer);
}

void QueriesApi::CheckSqlQueries() {
	using namespace std::string_literals;
	using namespace std::string_view_literals;
	using reindexer::randPoint;
	using reindexer::randBinDouble;

	checkSqlQuery("SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' ORDER BY year DESC LIMIT 10000000"sv,
				  Query(default_namespace, 0, 10000000).Where(kFieldNameYear, CondGt, 2016).Sort(kFieldNameYear, true));

	checkSqlQuery("SELECT ID, Year, Genre FROM test_namespace WHERE genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000"sv,
				  Query(default_namespace, 0, 10000000).Where(kFieldNameGenre, CondSet, {1, 2, 3}).Sort(kFieldNameYear, true));

	const std::string likePattern = RandLikePattern();
	checkSqlQuery("SELECT ID, Year, Genre FROM test_namespace WHERE name LIKE '"s + likePattern + "' ORDER BY year DESC LIMIT 10000000"s,
				  Query(default_namespace, 0, 10000000).Where(kFieldNameName, CondLike, likePattern).Sort(kFieldNameYear, true));

	checkSqlQuery("SELECT FACET(ID, Year ORDER BY ID DESC ORDER BY Year ASC LIMIT 20 OFFSET 1) FROM test_namespace LIMIT 10000000"sv,
				  Query(default_namespace, 0, 10000000)
					  .Aggregate(AggFacet, {kFieldNameId, kFieldNameYear}, {{kFieldNameId, true}, {kFieldNameYear, false}}, 20, 1));

	checkSqlQuery("SELECT ID FROM test_namespace WHERE name LIKE '"s + likePattern +
					  "' AND (genre IN ('1', '2', '3') AND year > '2016' ) OR age IN ('1', '2', '3', '4') LIMIT 10000000"s,
				  Query(default_namespace, 0, 10000000)
					  .Where(kFieldNameName, CondLike, likePattern)
					  .OpenBracket()
					  .Where(kFieldNameGenre, CondSet, {1, 2, 3})
					  .Where(kFieldNameYear, CondGt, 2016)
					  .CloseBracket()
					  .Or()
					  .Where(kFieldNameAge, CondSet, {1, 2, 3, 4}));

	checkSqlQuery(fmt::sprintf("SELECT ID FROM test_namespace ORDER BY '%s + %s * 5' DESC LIMIT 10000000", kFieldNameYear, kFieldNameId),
				  Query(default_namespace, 0, 10000000).Sort(kFieldNameYear + std::string(" + ") + kFieldNameId + " * 5", true));

	checkSqlQuery(fmt::sprintf("SELECT ID FROM test_namespace ORDER BY '%s + %s * 5' DESC ORDER BY '2 * %s / (1 + %s)' ASC LIMIT 10000000",
							   kFieldNameYear, kFieldNameId, kFieldNameGenre, kFieldNameIsDeleted),
				  Query(default_namespace, 0, 10000000)
					  .Sort(kFieldNameYear + std::string(" + ") + kFieldNameId + " * 5", true)
					  .Sort(std::string("2 * ") + kFieldNameGenre + " / (1 + " + kFieldNameIsDeleted + ')', false));

	// Checks that SQL queries with DWithin and sort by Distance work and compares the result with the result of corresponding C++ query
	reindexer::Point point = randPoint(10);
	double distance = randBinDouble(0, 1);
	checkSqlQuery(fmt::sprintf("SELECT * FROM %s WHERE ST_DWithin(%s, %s, %s);", geomNs, kFieldNamePointNonIndex, pointToSQL(point),
							   toString(distance)),
				  Query(geomNs).DWithin(kFieldNamePointNonIndex, point, distance));

	point = randPoint(10);
	distance = randBinDouble(0, 1);
	checkSqlQuery(fmt::sprintf("SELECT * FROM %s WHERE ST_DWithin(%s, %s, %s) ORDER BY 'ST_Distance(%s, %s)';", geomNs, pointToSQL(point),
							   kFieldNamePointNonIndex, toString(distance), kFieldNamePointLinearRTree, pointToSQL(point, true)),
				  Query(geomNs)
					  .DWithin(kFieldNamePointNonIndex, point, distance)
					  .Sort(std::string("ST_Distance(") + kFieldNamePointLinearRTree + ", " + pointToSQL(point) + ')', false));

	checkSqlQuery(fmt::sprintf("SELECT * FROM %s WHERE %s >= %s;", default_namespace, kFieldNameGenre, kFieldNameRate),
				  Query(default_namespace).WhereBetweenFields(kFieldNameGenre, CondGe, kFieldNameRate));
}

void QueriesApi::checkDslQuery(std::string_view dslQuery, Query&& checkQuery) {
	Query parsedQuery;
	Error err = parsedQuery.FromJSON(dslQuery);
	ASSERT_TRUE(err.ok()) << "Query: " << dslQuery << "; err: " << err.what();

	QueryResults dslQr;
	err = rt.reindexer->Select(parsedQuery, dslQr);
	ASSERT_TRUE(err.ok()) << "Query: " << dslQuery << "; err: " << err.what();

	QueryResults checkQr;
	err = rt.reindexer->Select(checkQuery, checkQr);
	ASSERT_TRUE(err.ok()) << "Query: " << dslQuery << "; err: " << err.what();

	CompareQueryResults(dslQuery, dslQr, checkQr);
	Verify(checkQr, std::move(checkQuery), *rt.reindexer);
}

// Checks that DSL queries works and compares the result with the result of corresponding C++ query
void QueriesApi::CheckDslQueries() {
	using namespace std::string_literals;
	using reindexer::randPoint;
	using reindexer::randBinDouble;
	using reindexer::double_to_str;

	auto point{randPoint(10)};
	auto distance = randBinDouble(0, 1);
	checkDslQuery(
		fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"dwithin","field":"%s","value":[[%s, %s], %s]}],"merge_queries":[],"aggregations":[]})",
			geomNs, kFieldNamePointLinearRTree, double_to_str(point.X()), double_to_str(point.Y()), double_to_str(distance)),
		Query(geomNs).DWithin(kFieldNamePointLinearRTree, point, distance));

	point = randPoint(10);
	distance = randBinDouble(0, 1);
	checkDslQuery(
		fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"dwithin","field":"%s","value":[%s,[%s,%s]]}],"merge_queries":[],"aggregations":[]})",
			geomNs, kFieldNamePointLinearRTree, double_to_str(distance), double_to_str(point.X()), double_to_str(point.Y())),
		Query(geomNs).DWithin(kFieldNamePointLinearRTree, point, distance));

	checkDslQuery(
		fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"gt","first_field":"%s","second_field":"%s"}],"merge_queries":[],"aggregations":[]})",
			default_namespace, kFieldNameStartTime, kFieldNamePackages),
		Query{default_namespace}.WhereBetweenFields(kFieldNameStartTime, CondGt, kFieldNamePackages));

	checkDslQuery(
		fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"SET","field":"%s","Value":["1", " 10 ", "100 ", " 1000"]}],"merge_queries":[],"aggregations":[]})",
			default_namespace, kFieldNameId),
		Query{default_namespace}.Where(kFieldNameId, CondSet, {1, 10, 100, 1000}));
}

void QueriesApi::CheckStandartQueries() {
	try {
		using namespace std::string_literals;
		static const std::vector<std::string> sortIdxs = {
			"",
			kFieldNameName,
			kFieldNameYear,
			kFieldNameRate,
			kFieldNameBtreeIdsets,
			std::string{"-2.5 * "} + kFieldNameRate + " / (" + kFieldNameYear + " + " + kFieldNameId + ')'};
		static const std::vector<std::string> distincts = {"", kFieldNameYear, kFieldNameRate};
		static const std::vector<bool> sortOrders = {true, false};

		for ([[maybe_unused]] const bool sortOrder : sortOrders) {
			for ([[maybe_unused]] const auto& sortIdx : sortIdxs) {
				for ([[maybe_unused]] const std::string& distinct : distincts) {
					[[maybe_unused]] const int randomAge = rand() % 50;
					[[maybe_unused]] const int randomGenre = rand() % 50;
					[[maybe_unused]] const int randomGenreUpper = rand() % 100;
					[[maybe_unused]] const int randomGenreLower = rand() % 100;

					ExecuteAndVerify(Query(default_namespace).Distinct(distinct).Sort(sortIdx, sortOrder).Limit(1));

					ExecuteAndVerify(
						Query(default_namespace).Where(kFieldNameGenre, CondEq, randomGenre).Distinct(distinct).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondEq, std::to_string(randomGenre))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace).Where(kFieldNameName, CondEq, RandString()).Distinct(distinct).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameRate, CondEq, (rand() % 100) / 10.0)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameRate, CondEq, std::to_string((rand() % 100) / 10.0))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondGt, randomGenre)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace));

					ExecuteAndVerify(
						Query(default_namespace).Where(kFieldNameName, CondGt, RandString()).Distinct(distinct).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameRate, CondGt, (rand() % 100) / 10.0)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace).Where(kFieldNameGenre, CondLt, randomGenre).Distinct(distinct).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondLt, std::to_string(randomGenre))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace).Where(kFieldNameName, CondLt, RandString()).Distinct(distinct).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameRate, CondLt, (rand() % 100) / 10.0)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameRate, CondLt, std::to_string((rand() % 100) / 10.0))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameBtreeIdsets, CondLt, static_cast<int>(rand() % 10000))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameBtreeIdsets, CondGt, static_cast<int>(rand() % 10000))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameBtreeIdsets, CondEq, static_cast<int>(rand() % 10000))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondRange, {randomGenreLower, randomGenreUpper})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameName, CondRange, {RandString(), RandString()})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameName, CondLike, RandLikePattern())
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(10, 10000, 50))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNamePackages, CondAllSet, RandIntVector(2, 10000, 50))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNamePackages, CondAllSet, 10000 + rand() % 50)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					// check substituteCompositIndexes
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameAge, CondEq, randomAge)
										 .Where(kFieldNameGenre, CondEq, randomGenre)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Where(kFieldNameGenre, CondEq, randomGenre)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameAge, CondAllSet, RandIntVector(1, 0, 50))
										 .Where(kFieldNameGenre, CondEq, randomGenre)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Where(kFieldNameGenre, CondSet, RandIntVector(10, 0, 50))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 20))
										 .Where(kFieldNameGenre, CondSet, RandIntVector(10, 0, 50))
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 30, 50))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 20))
										 .Where(kFieldNameGenre, CondEq, randomGenre)
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 30, 50))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));
					// end of check substituteCompositIndexes

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNamePackages, CondEmpty, VariantArray{})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameName, CondRange, {RandString(), RandString()})
										 .Distinct(distinct)
										 .Sort(kFieldNameYear, true)
										 .Sort(kFieldNameName, false)
										 .Sort(kFieldNameLocation, true));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameName, CondRange, {RandString(), RandString()})
										 .Distinct(distinct)
										 .Sort(kFieldNameGenre, true)
										 .Sort(kFieldNameActor, false)
										 .Sort(kFieldNameRate, true)
										 .Sort(kFieldNameLocation, false));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameName, CondLike, RandLikePattern())
										 .Distinct(distinct)
										 .Sort(kFieldNameGenre, true)
										 .Sort(kFieldNameActor, false)
										 .Sort(kFieldNameRate, true)
										 .Sort(kFieldNameLocation, false));

					ExecuteAndVerify(Query(default_namespace).Sort(kFieldNameGenre, true, {10, 20, 30}));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNamePackages, CondAny, VariantArray{})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace).Where(kFieldNameIsDeleted, CondEq, 1).Distinct(distinct).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Where(kFieldNameAge, CondEq, 3)
										 .Where(kFieldNameYear, CondGe, 2010)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Debug(LogTrace));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Where(kFieldNameAge, CondEq, 3)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Where(kFieldNameAge, CondEq, 3)
										 .OpenBracket()
										 .Where(kFieldNameYear, CondGe, 2010)
										 .CloseBracket()
										 .Or()
										 .Where(kFieldNameYear, CondGe, 2010));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameYear, CondGt, 2002)
										 .Where(kFieldNameGenre, CondEq, 4)
										 .Where(kFieldNameAge, CondEq, 3)
										 .Where(kFieldNameIsDeleted, CondEq, 3)
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Debug(LogTrace));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameAge, CondSet, {1, 2, 3, 4})
										 .Where(kFieldNameId, CondEq, rand() % 5000)
										 .Where(kFieldNameTemp, CondEq, "")
										 .Where(kFieldNameIsDeleted, CondEq, 1)
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .Debug(LogTrace));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondSet, {5, 1, 7})
										 .Where(kFieldNameYear, CondLt, 2010)
										 .Where(kFieldNameGenre, CondEq, 3)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Or()
										 .Where(kFieldNamePackages, CondEmpty, VariantArray{})
										 .Debug(LogTrace));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondSet, {5, 1, 7})
										 .Where(kFieldNameYear, CondLt, 2010)
										 .Or()
										 .Where(kFieldNamePackages, CondAny, VariantArray{})
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Debug(LogTrace));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .Where(kFieldNameGenre, CondEq, 6)
										 .Where(kFieldNameYear, CondRange, {2001, 2020})
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .Where(kFieldNameGenre, CondEq, 6)
										 .Not()
										 .Where(kFieldNameName, CondLike, RandLikePattern())
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameActor, CondEq, RandString()));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Not()
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Where(kFieldNameYear, CondRange, {2001, 2020})
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Not()
										 .Where(kFieldNameYear, CondRange, {2001, 2020})
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Not()
										 .Where(kFieldNameYear, CondEq, 10));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(kFieldNameNumeric, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameNumeric, CondGt, std::to_string(5)));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(kFieldNameNumeric, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameNumeric, CondLt, std::to_string(600)));

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameGenre, CondLt, 6)
										 .Where(kFieldNameYear, CondRange, {2001, 2020})
										 .CloseBracket()
										 .Not()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .OpenBracket()
										 .Where(kFieldNameNumeric, CondLt, std::to_string(600))
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Where(kFieldNameName, CondLike, RandLikePattern())
										 .CloseBracket()
										 .Or()
										 .Where(kFieldNameYear, CondEq, 10)
										 .CloseBracket());

					ExecuteAndVerify(Query(default_namespace)
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Not()
										 .OpenBracket()
										 .Where(kFieldNameYear, CondRange, {2001, 2020})
										 .Or()
										 .Where(kFieldNameName, CondLike, RandLikePattern())
										 .CloseBracket()
										 .Or()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .OpenBracket()
										 .Where(kFieldNameNumeric, CondLt, std::to_string(600))
										 .Not()
										 .OpenBracket()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Where(kFieldNameGenre, CondLt, 6)
										 .CloseBracket()
										 .Or()
										 .Where(kFieldNameYear, CondEq, 10)
										 .CloseBracket());

					ExecuteAndVerify(
						Query(default_namespace)
							.Distinct(distinct)
							.Sort(sortIdx, sortOrder)
							.Debug(LogTrace)
							.Where(kFieldNameNumeric, CondRange, {std::to_string(rand() % 100), std::to_string(rand() % 100 + 500)}));

					ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS"));
					ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002));
					ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, 2002));
					ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 2002));
					ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 1989));
					ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, "MMM"));

					ExecuteAndVerify(Query(default_namespace)
										 .ReqTotal()
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .WhereComposite(kCompositeFieldAgeGenre, CondLe, {{Variant(27), Variant(10000)}}));

					ExecuteAndVerify(Query(default_namespace)
										 .ReqTotal()
										 .Distinct(distinct)
										 .Sort(kFieldNameAge + " + "s + kFieldNameId, sortOrder)
										 .Sort(kFieldNameRate + " * "s + kFieldNameGenre, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .ReqTotal()
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder)
										 .WhereComposite(kCompositeFieldAgeGenre, CondEq, {{Variant(rand() % 10), Variant(rand() % 50)}}));

					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameYear, kFieldNameYear, CondEq, Query(joinNs))
										 .Distinct(distinct)
										 .Sort(joinNs + '.' + kFieldNameId, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameYear, kFieldNameYear, CondEq, Query(joinNs))
										 .Distinct(distinct)
										 .Sort(joinNs + '.' + kFieldNameName, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameYear, kFieldNameYear, CondEq, Query(joinNs))
										 .Distinct(distinct)
										 .Sort(joinNs + '.' + kFieldNameId + " * " + joinNs + '.' + kFieldNameGenre +
												   (sortIdx.empty() || (sortIdx == kFieldNameName) ? "" : (" + " + sortIdx)),
											   sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameYear, kFieldNameYear, CondEq,
													Query(joinNs)
														.Where(kFieldNameId, CondSet, RandIntVector(20, 0, 100))
														.Sort(kFieldNameId + " + "s + kFieldNameYear, sortOrder))
										 .Distinct(distinct));

					ExecuteAndVerify(
						Query(default_namespace)
							.InnerJoin(
								kFieldNameYear, kFieldNameYear, CondEq,
								Query(joinNs).Where(kFieldNameYear, CondGe, 1925).Sort(kFieldNameId + " + "s + kFieldNameYear, sortOrder))
							.Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameYear, kFieldNameYear, randCond(),
													Query(joinNs).Where(kFieldNameYear, CondGe, 2000 + rand() % 210))
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameYear, kFieldNameYear, randCond(),
													Query(joinNs).Where(kFieldNameYear, CondLe, 2000 + rand() % 210).Limit(rand() % 10))
										 .Distinct(distinct));
					ExecuteAndVerify(
						Query(default_namespace)
							.Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondGt, 2000 + rand() % 210) /*.Offset(rand() % 10)*/)
							.On(kFieldNameYear, randCond(), kFieldNameYear)
							.Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameYearSparse, CondEq, std::to_string(2000 + rand() % 60))
										 .Distinct(distinct));
					ExecuteAndVerify(
						Query(default_namespace)
							.InnerJoin(kFieldNameYear, kFieldNameYear, randCond(),
									   Query(joinNs).Where(kFieldNameYear, CondLt, 2000 + rand() % 210).Sort(kFieldNameName, sortOrder))
							.Distinct(distinct));

					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameId, kFieldNameId, CondEq,
													Query(joinNs)
														.Where(kFieldNameRegion, CondSet,
															   {Variant{rand() % 10}, Variant{rand() % 10}, Variant{rand() % 10},
																Variant{rand() % 10}, Variant{rand() % 10}})
														.Where(kFieldNameYear, CondLt, 2000 + rand() % 210)
														.Where(kFieldNameAge, CondGe, rand() % 30)
														.Sort(kFieldNameAge, sortOrder)
														.Limit(3))
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .InnerJoin(kFieldNameId, kFieldNameId, CondEq,
													Query(joinNs)
														.Where(kFieldNameRegion, CondSet,
															   {Variant{rand() % 10}, Variant{rand() % 10}, Variant{rand() % 10},
																Variant{rand() % 10}, Variant{rand() % 10}})
														.Where(kFieldNameYear, CondLt, 2000 + rand() % 210)
														.Where(kFieldNameAge, CondGe, rand() % 30)
														.Sort(kFieldNameYear, sortOrder)
														.Limit(3))
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondGt, 2000 + rand() % 210))
										 .OpenBracket()
										 .On(kFieldNameYear, randCond(), kFieldNameYear)
										 .Or()
										 .On(kFieldNameName, CondEq, kFieldNameName)
										 .CloseBracket()
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondGt, 2000 + rand() % 210))
										 .OpenBracket()
										 .On(kFieldNameYear, randCond(), kFieldNameYear)
										 .Or()
										 .On(kFieldNameName, CondEq, kFieldNameName)
										 .On(kFieldNameAge, randCond(), kFieldNameAge)
										 .Not()
										 .On(kFieldNameYear, randCond(), kFieldNameYear)
										 .CloseBracket()
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondGt, 2000 + rand() % 210))
										 .OpenBracket()
										 .On(kFieldNameYearSparse, CondLe, kFieldNameYearSparse)
										 .Or()
										 .On(kFieldNameName, CondEq, kFieldNameName)
										 .Or()
										 .On(kFieldNameAge, randCond(), kFieldNameAge)
										 .Not()
										 .On(kFieldNameYear, randCond(), kFieldNameYear)
										 .CloseBracket()
										 .Distinct(distinct));
					ExecuteAndVerify(
						Query(default_namespace)
							.Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondGt, 2000 + rand() % 210).Limit(rand() % 100))
							.OpenBracket()
							.On(kFieldNameYear, randCond(), kFieldNameYear)
							.Or()
							.On(kFieldNameName, CondEq, kFieldNameName)
							.Or()
							.On(kFieldNameAge, randCond(), kFieldNameAge)
							.CloseBracket()
							.Or()
							.Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondLt, 2000 + rand() % 210).Limit(rand() % 100))
							.OpenBracket()
							.On(kFieldNameYear, randCond(), kFieldNameYear)
							.Or()
							.On(kFieldNameName, CondEq, kFieldNameName)
							.Or()
							.On(kFieldNameAge, randCond(), kFieldNameAge)
							.CloseBracket()
							.Join(OrInnerJoin, Query(joinNs).Where(kFieldNameYear, CondEq, 2000 + rand() % 210).Limit(rand() % 100))
							.OpenBracket()
							.On(kFieldNameYear, randCond(), kFieldNameYear)
							.Or()
							.On(kFieldNameName, CondEq, kFieldNameName)
							.Or()
							.On(kFieldNameAge, randCond(), kFieldNameAge)
							.CloseBracket()
							.Or()
							.Where(kFieldNameId, CondSet, RandIntVector(20, 0, 100))
							.Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .WhereBetweenFields(kFieldNameGenre, CondEq, kFieldNameAge)
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .WhereBetweenFields(kFieldNameName, CondLike, kFieldNameActor)
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .WhereBetweenFields(kFieldNamePackages, CondGt, kFieldNameStartTime)
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(
						Query(compositeIndexesNs).WhereBetweenFields(kCompositeFieldPriceTitle, CondEq, kCompositeFieldPagesTitle));
					ExecuteAndVerify(Query(default_namespace)
										 .Not()
										 .Where(kFieldNameIsDeleted, CondEq, true)
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameGenre, CondEq, 4)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .CloseBracket()
										 .Not()
										 .Where(kFieldNameYear, CondRange, {2001, 2010})
										 .OpenBracket()
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Or()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Not()
										 .OpenBracket()
										 .OpenBracket()
										 .OpenBracket()
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Or()
										 .Where(kFieldNameId, CondEq, rand() % 5000)
										 .Where(kFieldNameTemp, CondEq, "")
										 .CloseBracket()
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameTemp, CondEq, "")
										 .Not()
										 .Where(kFieldNameIsDeleted, CondEq, true)
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameGenre, CondEq, 4)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .CloseBracket()
										 .Not()
										 .Where(kFieldNameYear, CondRange, {2001, 2010})
										 .OpenBracket()
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Or()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Not()
										 .OpenBracket()
										 .OpenBracket()
										 .OpenBracket()
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Or()
										 .Where(kFieldNameId, CondEq, rand() % 5000)
										 .Where(kFieldNameTemp, CondEq, "")
										 .CloseBracket()
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameTemp, CondEq, "")
										 .Not()
										 .OpenBracket()
										 .Where(kFieldNameIsDeleted, CondEq, true)
										 .CloseBracket()
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameGenre, CondEq, 4)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .CloseBracket()
										 .Not()
										 .Where(kFieldNameYear, CondRange, {2001, 2010})
										 .OpenBracket()
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Or()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Not()
										 .OpenBracket()
										 .OpenBracket()
										 .OpenBracket()
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Or()
										 .Where(kFieldNameId, CondEq, rand() % 5000)
										 .Where(kFieldNameTemp, CondEq, "")
										 .CloseBracket()
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameTemp, CondEq, "")
										 .OpenBracket()
										 .Not()
										 .Where(kFieldNameIsDeleted, CondEq, true)
										 .CloseBracket()
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameGenre, CondEq, 4)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .CloseBracket()
										 .Not()
										 .Where(kFieldNameYear, CondRange, {2001, 2010})
										 .OpenBracket()
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Or()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Not()
										 .OpenBracket()
										 .OpenBracket()
										 .OpenBracket()
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Or()
										 .Where(kFieldNameId, CondEq, rand() % 5000)
										 .Where(kFieldNameTemp, CondEq, "")
										 .CloseBracket()
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameTemp, CondEq, "")
										 .OpenBracket()
										 .Not()
										 .Where(kFieldNameIsDeleted, CondEq, true)
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondEq, 5)
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameGenre, CondEq, 4)
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .CloseBracket()
										 .Not()
										 .Where(kFieldNameYear, CondRange, {2001, 2010})
										 .OpenBracket()
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Or()
										 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
										 .Not()
										 .OpenBracket()
										 .OpenBracket()
										 .OpenBracket()
										 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
										 .Or()
										 .Where(kFieldNameId, CondEq, rand() % 5000)
										 .Where(kFieldNameTemp, CondEq, "")
										 .CloseBracket()
										 .Or()
										 .OpenBracket()
										 .Where(kFieldNameTemp, CondEq, "")
										 .Not()
										 .OpenBracket()
										 .Where(kFieldNameIsDeleted, CondEq, true)
										 .Or()
										 .Where(kFieldNameYear, CondGt, 2001)
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .CloseBracket()
										 .Sort(sortIdx, sortOrder)
										 .Distinct(distinct));
					ExecuteAndVerify(
						Query(default_namespace)
							.Join(InnerJoin, Query(joinNs).Where(kFieldNameYear, CondGt, 2000 + rand() % 210).Limit(rand() % 100))
							.OpenBracket()
							.Not()
							.On(kFieldNameYear, randCond(), kFieldNameYear)
							.Or()
							.On(kFieldNameName, CondEq, kFieldNameName)
							.Or()
							.On(kFieldNameAge, randCond(), kFieldNameAge)
							.CloseBracket()
							.Distinct(distinct));

					for (CondType cond : {CondEq, CondSet, CondLt, CondLe, CondGt, CondGe, CondRange}) {
						const auto argsCount = minMaxArgs(cond, 20);
						if (argsCount.min <= 1 && argsCount.max >= 1) {
							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuid, cond, randUuid())
												 .Distinct(distinct)
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuid, cond, randStrUuid())
												 .Distinct(distinct)
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuidArr, cond, randUuid())
												 .Distinct(distinct)
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuidArr, cond, randStrUuid())
												 .Distinct(distinct)
												 .Sort(sortIdx, sortOrder));
						}

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameUuid, cond, randHeterogeneousUuidArray(argsCount.min, argsCount.max))
											 .Distinct(distinct)
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameUuidArr, cond, randHeterogeneousUuidArray(argsCount.min, argsCount.max))
											 .Distinct(distinct)
											 .Sort(sortIdx, sortOrder));

						std::vector<VariantArray> compositeKeyValues;
						VariantArray hetUuidArray = randHeterogeneousUuidArray(argsCount.min, argsCount.max);
						compositeKeyValues.reserve(hetUuidArray.size());
						std::transform(std::make_move_iterator(hetUuidArray.begin()), std::make_move_iterator(hetUuidArray.end()),
									   std::back_inserter(compositeKeyValues),
									   [this](Variant&& uuid) { return VariantArray::Create(std::move(uuid), RandString()); });
						ExecuteAndVerify(Query(default_namespace)
											 .WhereComposite(kCompositeFieldUuidName, cond, compositeKeyValues)
											 .Distinct(distinct)
											 .Sort(sortIdx, sortOrder));
					}

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameUuid, CondRange, randHeterogeneousUuidArray(2, 2))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameUuidArr, CondRange, randHeterogeneousUuidArray(2, 2))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace)
							.WhereComposite(kCompositeFieldUuidName, CondRange,
											{VariantArray::Create(nilUuid(), RandString()), VariantArray::Create(randUuid(), RandString())})
							.Distinct(distinct)
							.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(Query(default_namespace).Where(kFieldNameId, CondEq, 10), CondAny, {})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Not()
										 .Where(Query(default_namespace), CondEmpty, {})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameId, CondLt, Query(default_namespace).Aggregate(AggAvg, {kFieldNameId}))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kFieldNameGenre, CondSet,
												Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondSet, {10, 20, 30, 40}))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace)
							.Where(Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondGt, 10), CondSet, {10, 20, 30, 40})
							.Distinct(distinct)
							.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondGt, 10).Offset(1), CondSet,
												{10, 20, 30, 40})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace)
							.Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).Aggregate(AggMax, {kFieldNameGenre}), CondRange, {48, 50})
							.Distinct(distinct)
							.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).ReqTotal(), CondGt, {50})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace)
							.Distinct(distinct)
							.Sort(sortIdx, sortOrder)
							.Debug(LogTrace)
							.Where(kFieldNameGenre, CondEq, 5)
							.Not()
							.Where(Query(default_namespace).Where(kFieldNameGenre, CondEq, 5), CondAny, {})
							.Or()
							.Where(kFieldNameGenre, CondSet,
								   Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondSet, {10, 20, 30, 40}))
							.Not()
							.OpenBracket()
							.Where(kFieldNameYear, CondRange, {2001, 2020})
							.Or()
							.Where(kFieldNameName, CondLike, RandLikePattern())
							.Or()
							.Where(Query(joinNs).Where(kFieldNameYear, CondEq, 2000 + rand() % 210), CondEmpty, {})
							.CloseBracket()
							.Or()
							.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
							.OpenBracket()
							.Where(kFieldNameNumeric, CondLt, std::to_string(600))
							.Not()
							.OpenBracket()
							.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
							.Where(kFieldNameGenre, CondLt, 6)
							.Or()
							.Where(kFieldNameId, CondLt, Query(default_namespace).Aggregate(AggAvg, {kFieldNameId}))
							.CloseBracket()
							.Not()
							.Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).Aggregate(AggMax, {kFieldNameGenre}), CondRange, {48, 50})
							.Or()
							.Where(kFieldNameYear, CondEq, 10)
							.CloseBracket());

					ExecuteAndVerify(Query(default_namespace)
										 .Where(kCompositeFieldIdTemp, CondEq,
												Query(default_namespace).Select({kCompositeFieldIdTemp}).Where(kFieldNameId, CondGt, 10))
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						Query(default_namespace)
							.Where(Query(default_namespace).Select({kCompositeFieldUuidName}).Where(kFieldNameId, CondGt, 10), CondRange,
								   {VariantArray::Create(nilUuid(), RandString()), VariantArray::Create(randUuid(), RandString())})
							.Distinct(distinct)
							.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(Query(default_namespace)
													.Select({kCompositeFieldAgeGenre})
													.Where(kFieldNameId, CondGt, 10)
													.Sort(kCompositeFieldAgeGenre, false)
													.Limit(10),
												CondLe, {Variant(VariantArray::Create(rand() % 50, rand() % 50))})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(Query(default_namespace).Where(kFieldNameId, CondGt, 10).ReqTotal(), CondGe, {10})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(Query(default_namespace)
										 .Where(Query(default_namespace).Where(kFieldNameId, CondGt, 10).CachedTotal(), CondGe, {10})
										 .Distinct(distinct)
										 .Sort(sortIdx, sortOrder));
				}
			}
		}
	} catch (const reindexer::Error& err) {
		ASSERT_TRUE(false) << err.what();
	} catch (const std::exception& err) {
		ASSERT_TRUE(false) << err.what();
	} catch (...) {
		ASSERT_TRUE(false);
	}
}
