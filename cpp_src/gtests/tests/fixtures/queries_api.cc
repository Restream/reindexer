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
