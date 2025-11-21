#pragma once

#include <limits>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/geometry.h"
#include "gtests/tools.h"
#include "queries_verifier.h"
#include "reindexer_api.h"
#include "tools/serializer.h"

class [[nodiscard]] TestQuery : private reindexer::Query {
public:
	using Query::Query;
	template <reindexer::concepts::ConvertibleToString Str>
	reindexer::Query& Distinct(Str&& name) & {
		if (!reindexer::strEmpty(name)) {
			reindexer::Query::Distinct(std::forward<Str>(name));
		}
		return *this;
	}
	template <reindexer::concepts::ConvertibleToString Str>
	[[nodiscard]] reindexer::Query&& Distinct(Str&& name) && {
		return std::move(Distinct(std::forward<Str>(name)));
	}
	using Query::Distinct;
};

class [[nodiscard]] QueriesApi : public ReindexerApi, public QueriesVerifier {
public:
	void SetUp() override {
		ReindexerApi::SetUp();

		setPkFields(default_namespace, {kFieldNameId, kFieldNameTemp});
		setPkFields(testSimpleNs, {kFieldNameId});
		setPkFields(joinNs, {kFieldNameId});
		setPkFields(compositeIndexesNs, {kFieldNameBookid, kFieldNameBookid2});
		setPkFields(comparatorsNs, {kFieldNameColumnInt64});
		setPkFields(forcedSortNs, {kFieldNameId});
		setPkFields(geomNs, {kFieldNameId});
		setPkFields(btreeIdxOptNs, {kFieldNameId});

		indexesCollates = {{kFieldNameActor, CollateOpts{CollateUTF8}},
						   {kFieldNameLocation, CollateOpts{CollateNone}},
						   {kFieldNameTemp, CollateOpts{CollateASCII}},
						   {kFieldNameNumeric, CollateOpts{CollateNumeric}}};

		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(
			default_namespace,
			{
				IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameGenre, "tree", "int", IndexOpts{}.NoIndexColumn(), 0},
				IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts{}, 0},
				IndexDeclaration{kFieldNamePackages, "hash", "int", IndexOpts{}.Array(), 0},
				IndexDeclaration{kFieldNameName, "tree", "string", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameCountries, "tree", "string", IndexOpts{}.Array(), 0},
				IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts{}.NoIndexColumn(), 0},
				IndexDeclaration{kFieldNameDescription, "fuzzytext", "string", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameRate, "tree", "double", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameIsDeleted, "-", "bool", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameActor, "tree", "string", IndexOpts{}.SetCollateMode(CollateUTF8), 0},
				IndexDeclaration{kFieldNamePriceId, "hash", "int", IndexOpts{}.Array(), 0},
				IndexDeclaration{kFieldNameLocation, "tree", "string", IndexOpts{}.SetCollateMode(CollateNone).NoIndexColumn(), 0},
				IndexDeclaration{kFieldNameEndTime, "hash", "int", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameStartTime, "tree", "int", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameBtreeIdsets, "hash", "int", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameTemp, "tree", "string", IndexOpts{}.SetCollateMode(CollateASCII), 0},
				IndexDeclaration{kFieldNameNumeric, "tree", "string", IndexOpts{}.SetCollateMode(CollateNumeric).Sparse(), 0},
				IndexDeclaration{kFieldNameUuid, "hash", "uuid", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameUuidArr, "hash", "uuid", IndexOpts{}.Array(), 0},
				IndexDeclaration{kCompositeFieldIdTemp, "tree", "composite", IndexOpts{}.PK(), 0},
				IndexDeclaration{kCompositeFieldAgeGenre, "hash", "composite", IndexOpts{}, 0},
				IndexDeclaration{kCompositeFieldUuidName, "hash", "composite", IndexOpts{}, 0},
				IndexDeclaration{kFieldNameYearSparse, "hash", "string", IndexOpts{}.Sparse(), 0},
			});
		addIndexFields(default_namespace, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameGenre, {{kFieldNameGenre, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameYear, {{kFieldNameYear, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNamePackages, {{kFieldNamePackages, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameName, {{kFieldNameName, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameCountries, {{kFieldNameCountries, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameAge, {{kFieldNameAge, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameDescription, {{kFieldNameDescription, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameRate, {{kFieldNameRate, reindexer::KeyValueType::Double{}}});
		addIndexFields(default_namespace, kFieldNameIsDeleted, {{kFieldNameIsDeleted, reindexer::KeyValueType::Bool{}}});
		addIndexFields(default_namespace, kFieldNameActor, {{kFieldNameActor, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNamePriceId, {{kFieldNamePriceId, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameLocation, {{kFieldNameLocation, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameEndTime, {{kFieldNameEndTime, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameStartTime, {{kFieldNameStartTime, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameBtreeIdsets, {{kFieldNameBtreeIdsets, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kFieldNameTemp, {{kFieldNameTemp, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameNumeric, {{kFieldNameNumeric, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameUuid, {{kFieldNameUuid, reindexer::KeyValueType::Uuid{}}});
		addIndexFields(default_namespace, kFieldNameUuidArr, {{kFieldNameUuidArr, reindexer::KeyValueType::Uuid{}}});
		addIndexFields(default_namespace, kCompositeFieldIdTemp,
					   {{kFieldNameId, reindexer::KeyValueType::Int{}}, {kFieldNameTemp, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kCompositeFieldAgeGenre,
					   {{kFieldNameAge, reindexer::KeyValueType::Int{}}, {kFieldNameGenre, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kCompositeFieldUuidName,
					   {{kFieldNameUuid, reindexer::KeyValueType::Uuid{}}, {kFieldNameName, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kFieldNameYearSparse, {{kFieldNameYearSparse, reindexer::KeyValueType::String{}}});

		rt.OpenNamespace(joinNs);
		DefineNamespaceDataset(joinNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
										IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0},
										IndexDeclaration{kFieldNameAge, "tree", "int", IndexOpts(), 0},
										IndexDeclaration{kFieldNameName, "tree", "string", IndexOpts(), 0},
										IndexDeclaration{kFieldNameDescription, "text", "string", IndexOpts{}, 0},
										IndexDeclaration{kFieldNameYearSparse, "hash", "string", IndexOpts().Sparse(), 0},
										IndexDeclaration{kFieldNameRegion, "hash", "int", IndexOpts{}, 0}});
		addIndexFields(joinNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(joinNs, kFieldNameYear, {{kFieldNameYear, reindexer::KeyValueType::Int{}}});
		addIndexFields(joinNs, kFieldNameAge, {{kFieldNameAge, reindexer::KeyValueType::Int{}}});
		addIndexFields(joinNs, kFieldNameName, {{kFieldNameName, reindexer::KeyValueType::String{}}});
		addIndexFields(joinNs, kFieldNameDescription, {{kFieldNameDescription, reindexer::KeyValueType::String{}}});
		addIndexFields(joinNs, kFieldNameYearSparse, {{kFieldNameYearSparse, reindexer::KeyValueType::String{}}});
		addIndexFields(joinNs, kFieldNameRegion, {{kFieldNameRegion, reindexer::KeyValueType::Int{}}});

		rt.OpenNamespace(testSimpleNs);
		DefineNamespaceDataset(testSimpleNs, {
												 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
												 IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0},
												 IndexDeclaration{kFieldNameName, "hash", "string", IndexOpts(), 0},
												 IndexDeclaration{kFieldNamePhone, "hash", "string", IndexOpts(), 0},
											 });
		addIndexFields(testSimpleNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(testSimpleNs, kFieldNameYear, {{kFieldNameYear, reindexer::KeyValueType::Int{}}});
		addIndexFields(testSimpleNs, kFieldNameName, {{kFieldNameName, reindexer::KeyValueType::String{}}});
		addIndexFields(testSimpleNs, kFieldNamePhone, {{kFieldNamePhone, reindexer::KeyValueType::String{}}});

		rt.OpenNamespace(compositeIndexesNs);
		DefineNamespaceDataset(compositeIndexesNs,
							   {IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts(), 0},
								IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts(), 0},
								IndexDeclaration{kFieldNameTitle, "text", "string", IndexOpts(), 0},
								IndexDeclaration{kFieldNamePages, "hash", "int", IndexOpts(), 0},
								IndexDeclaration{kFieldNamePrice, "hash", "int", IndexOpts(), 0},
								IndexDeclaration{kFieldNameName, "text", "string", IndexOpts(), 0},
								IndexDeclaration{kCompositeFieldPricePages, "hash", "composite", IndexOpts(), 0},
								IndexDeclaration{kCompositeFieldTitleName, "tree", "composite", IndexOpts(), 0},
								IndexDeclaration{kCompositeFieldPriceTitle, "hash", "composite", IndexOpts(), 0},
								IndexDeclaration{kCompositeFieldPagesTitle, "hash", "composite", IndexOpts(), 0},
								IndexDeclaration{kCompositeFieldBookidBookid2, "hash", "composite", IndexOpts().PK(), 0}});
		addIndexFields(compositeIndexesNs, kFieldNameBookid, {{kFieldNameBookid, reindexer::KeyValueType::Int{}}});
		addIndexFields(compositeIndexesNs, kFieldNameBookid2, {{kFieldNameBookid2, reindexer::KeyValueType::Int{}}});
		addIndexFields(compositeIndexesNs, kFieldNameTitle, {{kFieldNameTitle, reindexer::KeyValueType::String{}}});
		addIndexFields(compositeIndexesNs, kFieldNamePages, {{kFieldNamePages, reindexer::KeyValueType::Int{}}});
		addIndexFields(compositeIndexesNs, kFieldNamePrice, {{kFieldNamePrice, reindexer::KeyValueType::Int{}}});
		addIndexFields(compositeIndexesNs, kFieldNameName, {{kFieldNameName, reindexer::KeyValueType::String{}}});
		addIndexFields(compositeIndexesNs, kCompositeFieldPricePages,
					   {{kFieldNamePrice, reindexer::KeyValueType::Int{}}, {kFieldNamePages, reindexer::KeyValueType::Int{}}});
		addIndexFields(compositeIndexesNs, kCompositeFieldTitleName,
					   {{kFieldNameTitle, reindexer::KeyValueType::String{}}, {kFieldNameName, reindexer::KeyValueType::String{}}});
		addIndexFields(compositeIndexesNs, kCompositeFieldPriceTitle,
					   {{kFieldNamePrice, reindexer::KeyValueType::Int{}}, {kFieldNameTitle, reindexer::KeyValueType::String{}}});
		addIndexFields(compositeIndexesNs, kCompositeFieldPagesTitle,
					   {{kFieldNamePages, reindexer::KeyValueType::Int{}}, {kFieldNameTitle, reindexer::KeyValueType::String{}}});
		addIndexFields(compositeIndexesNs, kCompositeFieldBookidBookid2,
					   {{kFieldNameBookid, reindexer::KeyValueType::Int{}}, {kFieldNameBookid2, reindexer::KeyValueType::Int{}}});

		rt.OpenNamespace(comparatorsNs);
		DefineNamespaceDataset(
			comparatorsNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts(), 0},
							IndexDeclaration{kFieldNameColumnInt, "hash", "int", IndexOpts(), 0},
							IndexDeclaration{kFieldNameColumnInt64, "hash", "int64", IndexOpts().PK(), 0},
							IndexDeclaration{kFieldNameColumnDouble, "tree", "double", IndexOpts(), 0},
							IndexDeclaration{kFieldNameColumnString, "-", "string", IndexOpts(), 0},
							IndexDeclaration{kFieldNameColumnFullText, "text", "string",
											 IndexOpts().SetConfig(IndexFastFT, R"xxx({"stemmers":[]})xxx"), 0},
							IndexDeclaration{kFieldNameColumnStringNumeric, "-", "string", IndexOpts().SetCollateMode(CollateNumeric), 0}});
		addIndexFields(comparatorsNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(comparatorsNs, kFieldNameColumnInt, {{kFieldNameColumnInt, reindexer::KeyValueType::Int{}}});
		addIndexFields(comparatorsNs, kFieldNameColumnInt64, {{kFieldNameColumnInt64, reindexer::KeyValueType::Int64{}}});
		addIndexFields(comparatorsNs, kFieldNameColumnDouble, {{kFieldNameColumnDouble, reindexer::KeyValueType::Double{}}});
		addIndexFields(comparatorsNs, kFieldNameColumnString, {{kFieldNameColumnString, reindexer::KeyValueType::String{}}});
		addIndexFields(comparatorsNs, kFieldNameColumnFullText, {{kFieldNameColumnFullText, reindexer::KeyValueType::String{}}});
		addIndexFields(comparatorsNs, kFieldNameColumnStringNumeric, {{kFieldNameColumnStringNumeric, reindexer::KeyValueType::String{}}});

		rt.OpenNamespace(forcedSortNs);
		DefineNamespaceDataset(forcedSortNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
											  IndexDeclaration{kFieldNameColumnHash, "hash", "int", IndexOpts(), 0},
											  IndexDeclaration{kFieldNameColumnTree, "tree", "int", IndexOpts(), 0}});
		addIndexFields(forcedSortNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(forcedSortNs, kFieldNameColumnHash, {{kFieldNameColumnHash, reindexer::KeyValueType::Int{}}});
		addIndexFields(forcedSortNs, kFieldNameColumnTree, {{kFieldNameColumnTree, reindexer::KeyValueType::Int{}}});

		rt.OpenNamespace(geomNs);
		DefineNamespaceDataset(
			geomNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
					 IndexDeclaration{kFieldNamePointQuadraticRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::Quadratic), 0},
					 IndexDeclaration{kFieldNamePointLinearRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::Linear), 0},
					 IndexDeclaration{kFieldNamePointGreeneRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::Greene), 0},
					 IndexDeclaration{kFieldNamePointRStarRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::RStar), 0}});
		addIndexFields(geomNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(geomNs, kFieldNamePointQuadraticRTree, {{kFieldNamePointQuadraticRTree, reindexer::KeyValueType::Double{}}});
		addIndexFields(geomNs, kFieldNamePointLinearRTree, {{kFieldNamePointLinearRTree, reindexer::KeyValueType::Double{}}});
		addIndexFields(geomNs, kFieldNamePointGreeneRTree, {{kFieldNamePointGreeneRTree, reindexer::KeyValueType::Double{}}});
		addIndexFields(geomNs, kFieldNamePointRStarRTree, {{kFieldNamePointRStarRTree, reindexer::KeyValueType::Double{}}});

		rt.OpenNamespace(btreeIdxOptNs);
		DefineNamespaceDataset(btreeIdxOptNs, {IndexDeclaration{kFieldNameId, "tree", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{kFieldNameStartTime, "tree", "int", IndexOpts(), 0}});
		addIndexFields(btreeIdxOptNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
		addIndexFields(btreeIdxOptNs, kFieldNameStartTime, {{kFieldNameStartTime, reindexer::KeyValueType::Int{}}});
		initConditionsNs();
		initUUIDNs();
	}

	void initConditionsNs();
	void FillConditionsNs();
	void CheckConditions();
	enum class [[nodiscard]] NullAllowed : bool { Yes = true, No = false };
	void checkAllConditions(const std::string& fieldName, reindexer::KeyValueType fieldType, NullAllowed);
	void initUUIDNs();
	void FillUUIDNs();
	void CheckUUIDQueries();
	void CheckStandardQueries();
	void CheckDslQueries();
	void checkDslQuery(std::string_view dsl, Query&& checkQuery);
	void CheckSqlQueries();
	void checkSqlQuery(std::string_view sql, Query&& checkQuery);

	template <typename Q, typename... Args>
	void ExecuteAndVerify(Q&& query, Args&&... args) {
		query.Explain();
		auto qr = rt.Select(query);
		if constexpr (std::is_rvalue_reference_v<decltype(query)>) {
			Verify(qr, std::move(query), *rt.reindexer);
		} else {
			Verify(qr, reindexer::Query(query), *rt.reindexer);
		}
		Verify(qr, std::forward<Args>(args)...);
	}

	template <typename Q>
	void ExecuteAndVerifyWithSql(Q&& query) {
		ExecuteAndVerify(query);
		Query queryFromSql = Query::FromSQL(query.GetSQL()).Strict(query.GetStrictMode()).Debug(query.GetDebugLevel());
		ASSERT_EQ(query, queryFromSql) << "query: " << query.GetSQL() << "\nqueryFromSql: " << queryFromSql.GetSQL();
		ExecuteAndVerify(std::move(queryFromSql));
	}

	template <typename Q, typename... Args>
	void ExecuteAndVerify(Q&& query, QueryResults& qr, Args&&... args) {
		query.Explain();
		rt.Select(query, qr);
		if constexpr (std::is_rvalue_reference_v<decltype(query)>) {
			Verify(qr, std::move(query), *rt.reindexer);
		} else {
			Verify(qr, reindexer::Query(query), *rt.reindexer);
		}
		Verify(qr, std::forward<Args>(args)...);
	}

	template <typename Q>
	void ExecuteAndVerifyWithSql(Q&& query, QueryResults& qr) {
		ExecuteAndVerify(query, qr);
		Query queryFromSql = Query::FromSQL(query.GetSQL()).Strict(query.GetStrictMode()).Debug(query.GetDebugLevel());
		ASSERT_EQ(query, queryFromSql);
		qr.Clear();
		ExecuteAndVerify(std::move(queryFromSql), qr);
	}

	void Verify(const reindexer::QueryResults&) const noexcept {}
	void Verify(const LocalQueryResults&) const noexcept {}

	template <typename... Args>
	void Verify(const QueryResults& qr, const char* fieldName, const std::vector<Variant>& expectedValues, Args&&... args) {
		Verify(qr.ToLocalQr(), fieldName, expectedValues, std::forward<Args>(args)...);
	}
	template <typename... Args>
	void Verify(const reindexer::LocalQueryResults& qr, const char* fieldName, const std::vector<reindexer::Variant>& expectedValues,
				Args&&... args) {
		reindexer::WrSerializer ser;
		if (qr.Count() != expectedValues.size()) {
			ser << "Sizes different: expected size " << expectedValues.size() << ", obtained size " << qr.Count() << '\n';
		} else {
			for (size_t i = 0; i < expectedValues.size(); ++i) {
				reindexer::Item item(qr[i].GetItem(false));
				const reindexer::Variant fieldValue = item[fieldName];
				if (fieldValue != expectedValues[i]) {
					ser << "Field " << fieldName << " of item " << i << " different: expected ";
					expectedValues[i].Dump(ser);
					ser << " obtained ";
					fieldValue.Dump(ser);
					ser << '\n';
				}
			}
		}
		if (ser.Len()) {
			ser << "\nExpected values:\n";
			for (size_t i = 0; i < expectedValues.size(); ++i) {
				if (i != 0) {
					ser << ", ";
				}
				expectedValues[i].Dump(ser);
			}
			ser << "\nObtained values:\n";
			for (size_t i = 0; i < qr.Count(); ++i) {
				if (i != 0) {
					ser << ", ";
				}
				reindexer::Item item(qr[i].GetItem(false));
				const reindexer::Variant fieldValue = item[fieldName];
				fieldValue.Dump(ser);
			}
			FAIL() << ser.Slice() << std::endl;
		}
		Verify(qr, std::forward<Args>(args)...);
	}
	using QueriesVerifier::Verify;

protected:
	void CheckStandardQueries(bool sortOrder, const std::string& sortIdx, const std::string& distinct);
	void FillCompositeIndexesNamespace(size_t since, size_t till) {
		for (size_t i = since; i < till; ++i) {
			int idValue(static_cast<int>(i));
			Item item = NewItem(compositeIndexesNs);
			item[this->kFieldNameBookid] = idValue;
			item[this->kFieldNameBookid2] = idValue + 77777;
			item[this->kFieldNameTitle] = kFieldNameTitle + RandString();
			item[this->kFieldNamePages] = rand() % 1000 + 10;
			item[this->kFieldNamePrice] = rand() % 1000 + 150;
			item[this->kFieldNameName] = kFieldNameName + RandString();

			Upsert(compositeIndexesNs, item);

			saveItem(std::move(item), compositeIndexesNs);
		}

		Item lastItem = NewItem(compositeIndexesNs);
		lastItem[this->kFieldNameBookid] = 300;
		lastItem[this->kFieldNameBookid2] = 3000;
		lastItem[this->kFieldNameTitle] = "test book1 title";
		lastItem[this->kFieldNamePages] = 88888;
		lastItem[this->kFieldNamePrice] = 77777;
		lastItem[this->kFieldNameName] = "test book1 name";
		Upsert(compositeIndexesNs, lastItem);

		saveItem(std::move(lastItem), compositeIndexesNs);
	}

	void FillForcedSortNamespace() {
		forcedSortOffsetValues.clear();
		forcedSortOffsetValues.reserve(forcedSortOffsetNsSize);
		for (size_t i = 0; i < forcedSortOffsetNsSize; ++i) {
			reindexer::WrSerializer ser;
			{
				reindexer::JsonBuilder json(ser);
				json.Put(kFieldNameId, int(i));
				forcedSortOffsetValues.emplace_back(rand() % forcedSortOffsetMaxValue, rand() % forcedSortOffsetMaxValue);
				json.Put(kFieldNameColumnHash, forcedSortOffsetValues.back().first);
				json.Put(kFieldNameColumnTree, forcedSortOffsetValues.back().second);
				json.Put(kFieldNameColumnString, "columnString");
			}
			Item item = NewItem(forcedSortNs);
			const auto err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			Upsert(forcedSortNs, item);
			saveItem(std::move(item), forcedSortNs);
		}
	}

	void FillTestJoinNamespace(int start, int count) {
		for (int i = start; i < start + count; ++i) {
			Item item = NewItem(joinNs);
			item[kFieldNameId] = i;
			item[kFieldNameYear] = 1900 + i;
			item[kFieldNameAge] = rand() % 50;
			item[kFieldNameName] = RandString().c_str();
			item[kFieldNameGenre] = rand() % 50;
			item[kFieldNameDescription] = RandString();
			if (rand() % 4 != 0) {
				item[kFieldNameYearSparse] = std::to_string(rand() % 50 + 2000);
			}
			item[kFieldNameRegion] = rand() % 10;
			Upsert(joinNs, item);
			saveItem(std::move(item), joinNs);
		}
	}

	void FillTestSimpleNamespace() {
		Item item1 = NewItem(testSimpleNs);
		item1[kFieldNameId] = 1;
		item1[kFieldNameYear] = 2002;
		item1[kFieldNameName] = "SSS";
		Upsert(testSimpleNs, item1);

		saveItem(std::move(item1), testSimpleNs);

		Item item2 = NewItem(testSimpleNs);
		item2[kFieldNameId] = 2;
		item2[kFieldNameYear] = 1989;
		item2[kFieldNameName] = "MMM";
		Upsert(testSimpleNs, item2);

		saveItem(std::move(item2), testSimpleNs);
	}

	void FillGeomNamespace() {
		static size_t lastId = 0;
		reindexer::WrSerializer ser;
		for (size_t i = 0; i < geomNsSize; ++i) {
			ser.Reset();
			{
				reindexer::JsonBuilder bld(ser);

				const size_t id = i + lastId;
				bld.Put(kFieldNameId, id);

				reindexer::Point point{randPoint(10)};
				bld.Array(kFieldNamePointQuadraticRTree, {point.X(), point.Y()});

				point = randPoint(10);
				bld.Array(kFieldNamePointLinearRTree, {point.X(), point.Y()});

				point = randPoint(10);
				bld.Array(kFieldNamePointGreeneRTree, {point.X(), point.Y()});

				point = randPoint(10);
				bld.Array(kFieldNamePointRStarRTree, {point.X(), point.Y()});

				point = randPoint(10);
				bld.Array(kFieldNamePointNonIndex, {point.X(), point.Y()});
			}
			auto item = NewItem(geomNs);
			const auto err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			Upsert(geomNs, item);

			saveItem(std::move(item), geomNs);
		}
		lastId += geomNsSize;
	}

	void UpsertBtreeIdxOptNsItem(std::pair<int, int> values) {
		Item item = NewItem(btreeIdxOptNs);
		ASSERT_TRUE(item.Status().ok());
		item[kFieldNameId] = values.first;
		item[kFieldNameStartTime] = values.second;
		Upsert(btreeIdxOptNs, item);

		saveItem(std::move(item), btreeIdxOptNs);
	}

	enum Column { First, Second };

	std::vector<Variant> ForcedSortOffsetTestExpectedResults(size_t offset, size_t limit, bool desc,
															 const std::vector<int>& forcedSortOrder, Column column) const {
		if (limit == 0 || offset >= forcedSortOffsetValues.size()) {
			return {};
		}
		std::vector<int> res;
		res.resize(forcedSortOffsetValues.size());
		std::transform(
			forcedSortOffsetValues.cbegin(), forcedSortOffsetValues.cend(), res.begin(),
			column == First ? [](const std::pair<int, int>& v) { return v.first; } : [](const std::pair<int, int>& v) { return v.second; });
		std::sort(res.begin(), res.end(), desc ? [](int lhs, int rhs) { return lhs > rhs; } : [](int lhs, int rhs) { return lhs < rhs; });
		const auto boundary = std::stable_partition(res.begin(), res.end(), [&forcedSortOrder, desc](int v) {
			return desc == (std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), v) == forcedSortOrder.cend());
		});
		if (desc) {
			std::sort(boundary, res.end(), [&forcedSortOrder](int lhs, int rhs) {
				return std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs) >
					   std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs);
			});
		} else {
			std::sort(res.begin(), boundary, [&forcedSortOrder](int lhs, int rhs) {
				return std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs) <
					   std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs);
			});
		}
		return {res.cbegin() + offset, (offset + limit >= res.size()) ? res.cend() : (res.begin() + offset + limit)};
	}

	std::pair<std::vector<Variant>, std::vector<Variant>> ForcedSortOffsetTestExpectedResults(size_t offset, size_t limit, bool desc1Column,
																							  bool desc2Column,
																							  const std::vector<int>& forcedSortOrder,
																							  Column firstSortColumn) {
		if (limit == 0 || offset >= forcedSortOffsetValues.size()) {
			return {};
		}
		if (firstSortColumn == First) {
			std::sort(forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(),
					  [desc1Column, desc2Column](std::pair<int, int> lhs, std::pair<int, int> rhs) {
						  return lhs.first == rhs.first ? (desc2Column ? (lhs.second > rhs.second) : (lhs.second < rhs.second))
														: (desc1Column ? (lhs.first > rhs.first) : (lhs.first < rhs.first));
					  });
			const auto boundary = std::stable_partition(
				forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(), [&forcedSortOrder, desc1Column](std::pair<int, int> v) {
					return desc1Column == (std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), v.first) == forcedSortOrder.cend());
				});
			std::sort(desc1Column ? boundary : forcedSortOffsetValues.begin(), desc1Column ? forcedSortOffsetValues.end() : boundary,
					  [&forcedSortOrder, desc1Column, desc2Column](std::pair<int, int> lhs, std::pair<int, int> rhs) {
						  const auto lhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs.first);
						  const auto rhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs.first);
						  if (lhsPos == rhsPos) {
							  return desc2Column ? lhs.second > rhs.second : lhs.second < rhs.second;
						  } else {
							  return desc1Column ? lhsPos > rhsPos : lhsPos < rhsPos;
						  }
					  });
		} else {
			std::sort(forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(),
					  [desc1Column, desc2Column](std::pair<int, int> lhs, std::pair<int, int> rhs) {
						  return lhs.second == rhs.second ? (desc1Column ? (lhs.first > rhs.first) : (lhs.first < rhs.first))
														  : (desc2Column ? (lhs.second > rhs.second) : (lhs.second < rhs.second));
					  });
			const auto boundary = std::stable_partition(
				forcedSortOffsetValues.begin(), forcedSortOffsetValues.end(), [&forcedSortOrder, desc2Column](std::pair<int, int> v) {
					return desc2Column == (std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), v.second) == forcedSortOrder.cend());
				});
			std::sort(desc2Column ? boundary : forcedSortOffsetValues.begin(), desc2Column ? forcedSortOffsetValues.end() : boundary,
					  [&forcedSortOrder, desc1Column, desc2Column](std::pair<int, int> lhs, std::pair<int, int> rhs) {
						  const auto lhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), lhs.second);
						  const auto rhsPos = std::find(forcedSortOrder.cbegin(), forcedSortOrder.cend(), rhs.second);
						  if (lhsPos == rhsPos) {
							  return desc1Column ? lhs.first > rhs.first : lhs.first < rhs.first;
						  } else {
							  return desc2Column ? lhsPos > rhsPos : lhsPos < rhsPos;
						  }
					  });
		}
		std::vector<Variant> resFirstColumn, resSecondColumn;
		resFirstColumn.resize(std::min(limit, forcedSortOffsetValues.size() - offset));
		resSecondColumn.resize(std::min(limit, forcedSortOffsetValues.size() - offset));
		const bool byLimit = limit + offset < forcedSortOffsetValues.size();
		std::transform(forcedSortOffsetValues.cbegin() + offset,
					   byLimit ? (forcedSortOffsetValues.cbegin() + offset + limit) : forcedSortOffsetValues.cend(), resFirstColumn.begin(),
					   [](const std::pair<int, int>& v) { return Variant(v.first); });
		std::transform(forcedSortOffsetValues.cbegin() + offset,
					   byLimit ? (forcedSortOffsetValues.cbegin() + offset + limit) : forcedSortOffsetValues.cend(),
					   resSecondColumn.begin(), [](const std::pair<int, int>& v) { return Variant(v.second); });
		return std::make_pair(std::move(resFirstColumn), std::move(resSecondColumn));
	}

	void FillComparatorsNamespace() {
		for (size_t i = 0; i < 1000; ++i) {
			Item item(rt.NewItem(comparatorsNs));
			item[kFieldNameId] = static_cast<int>(i);
			item[kFieldNameColumnInt] = rand();
			item[kFieldNameColumnInt64] = static_cast<int64_t>(rand());
			item[kFieldNameColumnDouble] = static_cast<double>(rand()) / RAND_MAX;
			item[kFieldNameColumnString] = RandString();
			item[kFieldNameColumnStringNumeric] = std::to_string(i);
			item[kFieldNameColumnFullText] = RandString();

			Upsert(comparatorsNs, item);

			saveItem(std::move(item), comparatorsNs);
		}
	}

	void FillDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);

			saveItem(std::move(item), default_namespace);
		}
	}

	void AddToDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = start; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);
		}
	}

	void FillDefaultNamespaceTransaction(int start, int count, int packagesCount) {
		auto tr = rt.NewTransaction(default_namespace);

		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			auto err = tr.Insert(std::move(item));
			ASSERT_TRUE(err.ok()) << err.what();
		}
		std::ignore = rt.CommitTransaction(tr);
	}

	int GetcurrBtreeIdsetsValue(int id) {
		if (id % 200) {
			auto newValue = rand() % 10000;
			currBtreeIdsetsValue.store(newValue, std::memory_order_relaxed);
			return newValue;
		}
		return currBtreeIdsetsValue.load(std::memory_order_relaxed);
	}

	std::vector<std::string> RandStrVector(size_t count) {
		std::vector<std::string> res;
		res.reserve(count);
		for (size_t i = 0; i < count; ++i) {
			res.emplace_back(RandString());
		}
		return res;
	}

	Item GenerateDefaultNsItem(int idValue, size_t packagesCount) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = idValue;
		item[kFieldNameYear] = rand() % 50 + 2000;
		if (rand() % 4 != 0) {
			item[kFieldNameYearSparse] = std::to_string(rand() % 50 + 2000);
		}
		item[kFieldNameGenre] = rand() % 50;
		item[kFieldNameName] = RandString();
		item[kFieldNameCountries] = RandStrVector(1 + rand() % 5);
		item[kFieldNameAge] = rand() % 50;
		item[kFieldNameDescription] = RandString();

		auto packagesVec(RandIntVector(packagesCount, 10000, 50));
		item[kFieldNamePackages] = packagesVec;

		item[kFieldNameRate] = static_cast<double>(rand() % 100) / 10;

		auto pricesIds(RandIntVector(10, 7000, 50));
		item[kFieldNamePriceId] = pricesIds;

		int stTime = rand() % 50000;
		item[kFieldNameLocation] = RandString().c_str();
		item[kFieldNameStartTime] = stTime;
		item[kFieldNameEndTime] = stTime + (rand() % 5) * 1000;
		item[kFieldNameActor] = RandString();
		item[kFieldNameNumeric] = std::to_string(rand() % 1000);
		item[kFieldNameBtreeIdsets] = GetcurrBtreeIdsetsValue(idValue);
		const size_t s = rand() % 20;
		if (rand() % 2 == 0) {
			item[kFieldNameUuid] = randUuid();
			std::vector<reindexer::Uuid> arr;
			arr.reserve(s);
			for (size_t i = 0; i < s; ++i) {
				arr.emplace_back(randUuid());
			}
			item[kFieldNameUuidArr] = std::move(arr);
		} else {
			item[kFieldNameUuid] = randStrUuid();
			std::vector<std::string> arr;
			arr.reserve(s);
			for (size_t i = 0; i < s; ++i) {
				arr.emplace_back(randStrUuid());
			}
			item[kFieldNameUuidArr] = std::move(arr);
		}

		return item;
	}

	VariantArray RandVariantArray(size_t size, size_t min, size_t range) {
		VariantArray result;
		RandVariantArray(size, min, min + range, result);
		return result;
	}

	void RandVariantArray(size_t size, size_t min, size_t max, VariantArray& arr) {
		assert(min < max);
		arr.clear<false>();
		arr.reserve(size);
		for (size_t i = 0; i < size; ++i) {
			arr.emplace_back(int(rand() % (max - min) + min));
		}
	}

	static std::string pointToSQL(reindexer::Point point, bool escape = false) {
		return escape ? fmt::format("ST_GeomFromText(\\'point({:.12f} {:.12f})\\')", point.X(), point.Y())
					  : fmt::format("ST_GeomFromText('point({:.12f} {:.12f})')", point.X(), point.Y());
	}

	void CheckMergeQueriesWithLimit();
	void CheckMergeQueriesWithAggregation();

	void CheckGeomQueries() {
		for (size_t i = 0; i < 10; ++i) {
			// Checks that DWithin and sort by Distance work and verifies the result
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointQuadraticRTree, randPoint(10), randBin<double>(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointLinearRTree, randPoint(10), randBin<double>(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointGreeneRTree, randPoint(10), randBin<double>(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointRStarRTree, randPoint(10), randBin<double>(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointNonIndex, randPoint(10), randBin<double>(0, 1)));
			ExecuteAndVerify(Query(geomNs)
								 .DWithin(kFieldNamePointLinearRTree, randPoint(10), randBin<double>(0, 1))
								 .SortStDistance(kFieldNamePointNonIndex, kFieldNamePointLinearRTree, false));
			ExecuteAndVerify(
				Query(geomNs)
					.DWithin(kFieldNamePointLinearRTree, randPoint(10), randBin<double>(0, 1))
					.SortStDistance(kFieldNamePointNonIndex, randPoint(10), false)
					.Sort(std::string("ST_Distance(") + pointToSQL(randPoint(10)) + ", " + kFieldNamePointGreeneRTree + ')', false));
			ExecuteAndVerify(Query(geomNs)
								 .DWithin(kFieldNamePointQuadraticRTree, randPoint(10), randBin<double>(0, 1))
								 .Or()
								 .DWithin(kFieldNamePointRStarRTree, randPoint(10), randBin<double>(0, 1))
								 .Sort(std::string("ST_Distance(") + pointToSQL(randPoint(10)) + ", " + kFieldNamePointQuadraticRTree +
										   ") + 3 * ST_Distance(" + kFieldNamePointLinearRTree + ", " + kFieldNamePointNonIndex +
										   ") + ST_Distance(" + kFieldNamePointRStarRTree + ", " + kFieldNamePointGreeneRTree + ')',
									   false));
		}
	}

	void CheckDistinctQueries() {
		static const std::vector<std::string> distincts = {"", kFieldNameYear, kFieldNameRate};

		for (const std::string& distinct : distincts) {
			const int randomAge = rand() % 50;
			const int randomGenre = rand() % 50;

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameGenre, CondEq, randomGenre)
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameName, CondEq, RandString())
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameRate, CondEq, static_cast<double>(rand() % 100) / 10)
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameGenre, CondGt, randomGenre)
										.Sort(kFieldNameYear, true)
										.Debug(LogTrace));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameName, CondGt, RandString())
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameRate, CondGt, static_cast<double>(rand() % 100) / 10)
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameGenre, CondLt, randomGenre)
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Where(kFieldNameAge, CondEq, randomAge)
										.Where(kFieldNameGenre, CondEq, randomGenre)
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(TestQuery(default_namespace)
										.Distinct(distinct.c_str())
										.Select({distinct.c_str()})
										.Where(kFieldNameGenre, CondEq, randomGenre)
										.Sort(kFieldNameYear, true));
		}
	}

	void CheckConditionsMergingQueries() {
		const auto randCond = [conds = {CondEq, CondLt, CondLe, CondGt, CondGe}]() noexcept {
			return *(conds.begin() + rand() % std::size(conds));
		};
		// Check merging of conditions by the same index with large sets of values
		int64_t tmp;
		for (size_t i = 0; i < 3; ++i) {
			struct {
				CondType cond;
				VariantArray values;
			} testData[]{
				{CondSet, RandVariantArray(500, 0, 1000)},
				{CondSet, RandVariantArray(100, 0, 1000)},
				{CondSet, {}},
				{CondSet, RandVariantArray(rand() % 4, rand() % 1000, rand() % 100 + 1)},
				{CondRange, (tmp = rand() % 1000, VariantArray::Create(tmp, rand() % 1000 + tmp))},
				{CondRange, (tmp = rand() % 1000, VariantArray::Create(tmp, (rand() % 2) * 100 + tmp))},
			};
			testData[1].values.insert(testData[1].values.end(), testData[0].values.begin(), testData[0].values.begin() + 100);

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameNumeric, testData[0].cond, testData[0].values)
										.Where(kFieldNameNumeric, testData[1].cond, testData[1].values));

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameNumeric, testData[0].cond, testData[0].values)
										.Where(kFieldNameNumeric, testData[2].cond, testData[2].values)
										.Where(kFieldNameNumeric, testData[1].cond, testData[1].values));
			for (size_t j = 0; j < 10; ++j) {
				Query q{default_namespace};
				for (size_t l = 0, n = rand() % 10 + 2; l < n; ++l) {
					const size_t testCase = rand() % (std::size(testData) * 2 + 1);
					if (testCase < std::size(testData)) {
						q.Where(kFieldNameNumeric, testData[testCase].cond, testData[testCase].values);
					} else if (testCase == std::size(testData)) {
						q.Where(kFieldNameNumeric, rand() % 2 ? CondAny : CondEmpty, VariantArray{});
					} else {
						q.Where(kFieldNameNumeric, randCond(), VariantArray::Create(rand() % 1000));
					}
				}
				ExecuteAndVerifyWithSql(q);
			}
		}
	}

	static CondType randCond() noexcept {
		constexpr static CondType conds[]{CondEq, CondSet, CondLt, CondLe, CondGe, CondGt};
		return conds[rand() % (sizeof(conds) / sizeof(*conds))];
	}

	template <typename FacetMap>
	static void frameFacet(FacetMap& facet, size_t offset, size_t limit) {
		if (offset >= facet.size()) {
			facet.clear();
		} else {
			auto end = facet.begin();
			std::advance(end, offset);
			facet.erase(facet.begin(), end);
		}
		if (limit < facet.size()) {
			auto begin = facet.begin();
			std::advance(begin, limit);
			facet.erase(begin, facet.end());
		}
	}

	static void checkFacetValues(const reindexer::h_vector<std::string, 1>& result, const std::string& expected, const std::string& name) {
		ASSERT_EQ(result.size(), 1) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(result[0], expected) << (name + " aggregation Facet result is incorrect!");
	}

	static void checkFacetValues(const reindexer::h_vector<std::string, 1>& result, int expected, const std::string& name) {
		ASSERT_EQ(result.size(), 1) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(std::stoi(result[0]), expected) << (name + " aggregation Facet result is incorrect!");
	}

	template <typename T>
	static void checkFacetValues(const reindexer::h_vector<std::string, 1>& result, const T& expected, const std::string& name) {
		ASSERT_EQ(result.size(), 2) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(result[0], expected.name) << (name + " aggregation Facet result is incorrect!");
		EXPECT_EQ(std::stoi(result[1]), expected.year) << (name + " aggregation Facet result is incorrect!");
	}

	template <typename ExpectedFacet>
	static void checkFacet(const std::vector<reindexer::FacetResult>& result, const ExpectedFacet& expected, const std::string& name) {
		ASSERT_EQ(result.size(), expected.size()) << (name + " aggregation Facet result is incorrect!");
		auto resultIt = result.begin();
		auto expectedIt = expected.cbegin();
		for (; resultIt != result.end() && expectedIt != expected.cend(); ++resultIt, ++expectedIt) {
			checkFacetValues(resultIt->values, expectedIt->first, name);
			EXPECT_EQ(resultIt->count, expectedIt->second) << (name + " aggregation Facet result is incorrect!");
		}
	}

	static void getFacetValue(const reindexer::h_vector<std::string, 1>& f, std::string& v, const std::string& name) {
		ASSERT_EQ(f.size(), 1) << (name + " aggregation Facet result is incorrect!");
		v = f[0];
	}

	static void getFacetValue(const reindexer::h_vector<std::string, 1>& f, int& v, const std::string& name) {
		ASSERT_EQ(f.size(), 1) << (name + " aggregation Facet result is incorrect!");
		v = std::stoi(f[0]);
	}

	template <typename T>
	static void checkFacetUnordered(const std::vector<reindexer::FacetResult>& result, std::unordered_map<T, int>& expected,
									const std::string& name) {
		ASSERT_EQ(result.size(), expected.size()) << (name + " aggregation Facet result is incorrect!");
		T facetValue;
		for (auto it = result.begin(), endIt = result.end(); it != endIt; ++it) {
			getFacetValue(it->values, facetValue, name);
			EXPECT_EQ(expected[facetValue], it->count) << (name + " aggregation Facet result is incorrect!");
		}
	}

	void InitNSObj() {
		rt.OpenNamespace(nsWithObject);
		DefineNamespaceDataset(nsWithObject, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});
		reindexer::WrSerializer ser;
		for (int i = 0; i < 10; ++i) {
			ser.Reset();
			reindexer::JsonBuilder bld(ser);
			bld.Put("id", i);
			auto objNode = bld.Object(kFieldNameObjectField);
			objNode.Put("data", rand() % 3);
			objNode.End();
			bld.End();
			auto item = NewItem(nsWithObject);
			const auto err = item.Unsafe(true).FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			Upsert(nsWithObject, item);
		}
	}

	void CheckAggregationQueries() {
		constexpr size_t facetLimit = 10;
		constexpr size_t facetOffset = 10;

		Query q(default_namespace);
		EXPECT_THROW(q.Aggregate(AggAvg, {}), reindexer::Error);

		EXPECT_THROW(q.Aggregate(AggAvg, {kFieldNameYear, kFieldNameName}), reindexer::Error);

		EXPECT_THROW(q.Aggregate(AggAvg, {kFieldNameYear}, {{kFieldNameYear, true}}), reindexer::Error);

		EXPECT_THROW(q.Aggregate(AggAvg, {kFieldNameYear}, {}, 10), reindexer::Error);

		const Query wrongQuery1{Query(default_namespace).Aggregate(AggFacet, {kFieldNameYear}, {{kFieldNameName, true}})};
		reindexer::QueryResults wrongQr1;
		auto err = rt.reindexer->Select(wrongQuery1, wrongQr1);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "The aggregation facet cannot provide sort by 'name'");

		const Query wrongQuery2{Query(default_namespace).Aggregate(AggFacet, {kFieldNameCountries, kFieldNameYear})};
		reindexer::QueryResults wrongQr2;
		err = rt.reindexer->Select(wrongQuery2, wrongQr2);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Multifield facet cannot contain an array field");

		InitNSObj();
		const Query wrongQuery3{Query(nsWithObject).Distinct(kFieldNameObjectField)};
		reindexer::QueryResults wrongQr3;
		err = rt.reindexer->Select(wrongQuery3, wrongQr3);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Cannot aggregate object field");

		Query testQuery{Query(default_namespace)
							.Aggregate(AggAvg, {kFieldNameYear})
							.Aggregate(AggSum, {kFieldNameYear})
							.Aggregate(AggMin, {kFieldNamePackages})
							.Aggregate(AggFacet, {kFieldNameName})
							.Aggregate(AggFacet, {kFieldNameName}, {{kFieldNameName, false}}, facetLimit, facetOffset)
							.Aggregate(AggFacet, {kFieldNameName}, {{"count", true}}, facetLimit, facetOffset)
							.Aggregate(AggFacet, {kFieldNamePackages})
							.Aggregate(AggFacet, {kFieldNamePackages}, {{"count", false}}, facetLimit, facetOffset)
							.Aggregate(AggFacet, {kFieldNamePackages}, {{kFieldNamePackages, true}}, facetLimit, facetOffset)
							.Aggregate(AggFacet, {kFieldNameName, kFieldNameYear}, {{kFieldNameYear, true}, {kFieldNameName, false}},
									   facetLimit, facetOffset)};
		Query checkQuery = Query(default_namespace);

		auto testQr = rt.Select(testQuery);
		auto checkQr = rt.Select(checkQuery);

		double yearSum = 0.0;
		int packagesMin = std::numeric_limits<int>::max();
		struct [[nodiscard]] MultifieldFacetItem {
			std::string name;
			int year;
			bool operator<(const MultifieldFacetItem& other) const {
				if (year == other.year) {
					return name < other.name;
				}
				return year > other.year;
			}
		};
		std::map<MultifieldFacetItem, int> multifieldFacet;
		std::map<std::string, int> singlefieldFacetByName;
		std::unordered_map<std::string, int> singlefieldFacetUnordered;
		std::map<int, int, std::greater<int>> arrayFacetByName;
		std::unordered_map<int, int> arrayFacetUnordered;
		for (auto it : checkQr) {
			Item item(it.GetItem(false));
			yearSum += item[kFieldNameYear].Get<int>();
			++multifieldFacet[MultifieldFacetItem{std::string(item[kFieldNameName].Get<std::string_view>()),
												  item[kFieldNameYear].Get<int>()}];
			++singlefieldFacetByName[std::string(item[kFieldNameName].Get<std::string_view>())];
			++singlefieldFacetUnordered[std::string(item[kFieldNameName].Get<std::string_view>())];
			for (const Variant& pack : static_cast<reindexer::VariantArray>(item[kFieldNamePackages])) {
				const int value = pack.As<int>();
				packagesMin = std::min(value, packagesMin);
				++arrayFacetByName[value];
				++arrayFacetUnordered[value];
			}
		}
		std::vector<std::pair<std::string, int>> singlefieldFacetByCount(singlefieldFacetByName.begin(), singlefieldFacetByName.end());
		std::sort(singlefieldFacetByCount.begin(), singlefieldFacetByCount.end(),
				  [](const std::pair<std::string, int>& lhs, const std::pair<std::string, int>& rhs) {
					  return lhs.second == rhs.second ? lhs.first < rhs.first : lhs.second > rhs.second;
				  });
		std::vector<std::pair<int, int>> arrayFacetByCount(arrayFacetByName.begin(), arrayFacetByName.end());
		std::sort(arrayFacetByCount.begin(), arrayFacetByCount.end(), [](const std::pair<int, int>& lhs, const std::pair<int, int>& rhs) {
			return lhs.second == rhs.second ? lhs.first < rhs.first : lhs.second < rhs.second;
		});
		frameFacet(multifieldFacet, facetOffset, facetLimit);
		frameFacet(singlefieldFacetByName, facetOffset, facetLimit);
		frameFacet(singlefieldFacetByCount, facetOffset, facetLimit);
		frameFacet(arrayFacetByName, facetOffset, facetLimit);
		frameFacet(arrayFacetByCount, facetOffset, facetLimit);

		ASSERT_EQ(testQr.GetAggregationResults().size(), 10);
		EXPECT_DOUBLE_EQ(testQr.GetAggregationResults()[0].GetValueOrZero(), yearSum / checkQr.Count())
			<< "Aggregation Avg result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.GetAggregationResults()[1].GetValueOrZero(), yearSum) << "Aggregation Sum result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.GetAggregationResults()[2].GetValueOrZero(), packagesMin) << "Aggregation Min result is incorrect!";
		checkFacetUnordered(testQr.GetAggregationResults()[3].GetFacets(), singlefieldFacetUnordered, "SinglefieldUnordered");
		checkFacet(testQr.GetAggregationResults()[4].GetFacets(), singlefieldFacetByName, "SinglefieldName");
		checkFacet(testQr.GetAggregationResults()[5].GetFacets(), singlefieldFacetByCount, "SinglefieldCount");
		checkFacetUnordered(testQr.GetAggregationResults()[6].GetFacets(), arrayFacetUnordered, "ArrayUnordered");
		checkFacet(testQr.GetAggregationResults()[7].GetFacets(), arrayFacetByCount, "ArrayByCount");
		checkFacet(testQr.GetAggregationResults()[8].GetFacets(), arrayFacetByName, "ArrayByName");
		checkFacet(testQr.GetAggregationResults()[9].GetFacets(), multifieldFacet, "Multifield");
	}

	void CompareQueryResults(std::string_view serializedQuery, const QueryResults& lhs, const QueryResults& rhs) {
		CompareQueryResults(serializedQuery, lhs.ToLocalQr(), rhs.ToLocalQr());
	}

	void CompareQueryResults(std::string_view serializedQuery, const LocalQueryResults& lhs, const LocalQueryResults& rhs) {
		EXPECT_EQ(lhs.Count(), rhs.Count());
		if (lhs.Count() == rhs.Count()) {
			for (size_t i = 0; i < lhs.Count(); ++i) {
				Item ritem1(rhs[i].GetItem(false));
				Item ritem2(lhs[i].GetItem(false));
				EXPECT_EQ(ritem1.NumFields(), ritem2.NumFields());
				if (ritem1.NumFields() == ritem2.NumFields()) {
					for (int idx = 1; idx < ritem1.NumFields(); ++idx) {
						const VariantArray& v1 = ritem1[idx];
						const VariantArray& v2 = ritem2[idx];

						EXPECT_EQ(v1.size(), v2.size());
						if (v1.size() == v2.size()) {
							for (size_t j = 0; j < v1.size(); ++j) {
								const auto cmpRes =
									v1[j].Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(v2[j]);
								EXPECT_EQ(cmpRes, reindexer::ComparationResult::Eq);
							}
						}
					}
				}
			}

			EXPECT_EQ(lhs.aggregationResults.size(), rhs.aggregationResults.size());
			if (lhs.aggregationResults.size() == rhs.aggregationResults.size()) {
				for (size_t i = 0; i < rhs.aggregationResults.size(); ++i) {
					const auto& aggRes1 = rhs.aggregationResults[i];
					const auto& aggRes2 = lhs.aggregationResults[i];
					EXPECT_EQ(aggRes1.GetType(), aggRes2.GetType());
					EXPECT_DOUBLE_EQ(aggRes1.GetValueOrZero(), aggRes2.GetValueOrZero());
					EXPECT_EQ(aggRes1.GetFields().size(), aggRes2.GetFields().size());
					const auto& fieldsRes1 = aggRes1.GetFields();
					const auto& fieldsRes2 = aggRes2.GetFields();
					if (fieldsRes1.size() == fieldsRes2.size()) {
						for (size_t j = 0; j < fieldsRes1.size(); ++j) {
							EXPECT_EQ(fieldsRes1[j], fieldsRes1[j]);
						}
					}
					const auto fasetsRes1 = aggRes1.GetFacets();
					const auto fasetsRes2 = aggRes2.GetFacets();
					EXPECT_EQ(fasetsRes1.size(), fasetsRes2.size());
					if (fasetsRes1.size() == fasetsRes2.size()) {
						for (size_t j = 0; j < fasetsRes1.size(); ++j) {
							EXPECT_EQ(fasetsRes1[j].count, fasetsRes2[j].count);
							EXPECT_EQ(fasetsRes1[j].values.size(), fasetsRes2[j].values.size());
							if (fasetsRes1[j].values.size() == fasetsRes2[j].values.size()) {
								for (size_t k = 0; k < fasetsRes1[j].values.size(); ++k) {
									if (fasetsRes1[j].values[k] != fasetsRes1[j].values[k]) {
										assertrx(0);
									}
									EXPECT_EQ(aggRes1.GetFacets()[j].values[k], aggRes2.GetFacets()[j].values[k])
										<< aggRes1.GetFacets()[j].values[0];
								}
							}
						}
					}
				}
			}
		}
		if (::testing::Test::HasFailure()) {
			FAIL() << "Failed query: " << serializedQuery;
			assertrx(false);
		}
	}

	static std::string toString(double v) {
		std::ostringstream res;
		res.precision(std::numeric_limits<double>::digits10 + 1);
		res << v;
		return res.str();
	}

	void CheckCompositeIndexesQueries() {
		int priceValue = 77777;
		int pagesValue = 88888;
		const char* titleValue = "test book1 title";
		const char* nameValue = "test book1 name";

		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondEq, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondLt, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondLe, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondGt, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondGe, {{Variant(priceValue), Variant(pagesValue)}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondRange,
											 {{Variant(1), Variant(1)}, {Variant(priceValue), Variant(pagesValue)}}));

		std::vector<VariantArray> intKeys;
		intKeys.reserve(10);
		for (int i = 0; i < 10; ++i) {
			intKeys.emplace_back(VariantArray{Variant(i), Variant(i * 5)});
		}
		ExecuteAndVerify(Query(compositeIndexesNs).WhereComposite(kCompositeFieldPricePages.c_str(), CondSet, intKeys));

		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
											 {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldTitleName.c_str(), CondGe,
											 {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));

		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldTitleName.c_str(), CondLt,
											 {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldTitleName.c_str(), CondLe,
											 {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));
		constexpr size_t kStringKeysCnt = 1010;
		std::vector<VariantArray> stringKeys;
		stringKeys.reserve(kStringKeysCnt);
		for (size_t i = 0; i < kStringKeysCnt; ++i) {
			stringKeys.emplace_back(VariantArray{Variant(RandString()), Variant(RandString())});
		}
		ExecuteAndVerify(Query(compositeIndexesNs).WhereComposite(kCompositeFieldTitleName.c_str(), CondSet, stringKeys));

		ExecuteAndVerify(Query(compositeIndexesNs)
							 .Where(kFieldNameName, CondEq, nameValue)
							 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
											 {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));

		// Fulltext query is inside brackets
		ExecuteAndVerify(Query(compositeIndexesNs)
							 .OpenBracket()
							 .Where(kFieldNameName, CondEq, nameValue)
							 .CloseBracket()
							 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
											 {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));

		ExecuteAndVerify(Query(compositeIndexesNs));
	}

	void CheckComparatorsQueries() {
		ExecuteAndVerify(Query(comparatorsNs).Where("columnInt64", CondLe, {Variant(static_cast<int64_t>(10000))}));

		std::vector<double> doubleSet;
		doubleSet.reserve(1010);
		for (size_t i = 0; i < 1010; i++) {
			doubleSet.emplace_back(static_cast<double>(rand()) / RAND_MAX);
		}
		ExecuteAndVerify(Query(comparatorsNs).Where("columnDouble", CondSet, doubleSet));
		doubleSet.clear();
		for (size_t i = 0; i < 2; i++) {
			doubleSet.emplace_back(static_cast<double>(rand()) / RAND_MAX);
		}
		ExecuteAndVerify(Query(comparatorsNs).Where("columnDouble", CondAllSet, doubleSet));
		ExecuteAndVerify(Query(comparatorsNs).Where("columnString", CondGe, std::string("test_string1")));
		ExecuteAndVerify(Query(comparatorsNs).Where("columnString", CondLe, std::string("test_string2")));
		ExecuteAndVerify(Query(comparatorsNs).Where("columnString", CondEq, std::string("test_string3")));

		std::vector<std::string> stringSet;
		stringSet.reserve(1010);
		for (size_t i = 0; i < 1010; i++) {
			stringSet.emplace_back(RandString());
		}
		ExecuteAndVerify(Query(comparatorsNs).Where("columnString", CondSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 2; i++) {
			stringSet.emplace_back(RandString());
		}
		ExecuteAndVerify(Query(comparatorsNs).Where("columnString", CondAllSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 100; i++) {
			stringSet.emplace_back(std::to_string(i + 20000));
		}
		ExecuteAndVerify(Query(comparatorsNs).Where("columnStringNumeric", CondSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 100; i++) {
			stringSet.emplace_back(std::to_string(i + 1));
		}
		ExecuteAndVerify(Query(comparatorsNs).Where("columnStringNumeric", CondSet, stringSet));
		ExecuteAndVerify(Query(comparatorsNs).Where("columnStringNumeric", CondEq, std::string("777")));
		ExecuteAndVerify(Query(comparatorsNs).Where("columnFullText", CondEq, RandString()));
	}
	void sortByNsDifferentTypesImpl(std::string_view fillingNs, const reindexer::Query& q, const std::string& sortPrefix);

	const std::string kFieldNameId = "id";
	const char* kFieldNameGenre = "genre";
	const char* kFieldNameYear = "year";
	const char* kFieldNameYearSparse = "year_sparse";
	const char* kFieldNamePackages = "packages";
	const char* kFieldNameName = "name";
	const char* kFieldNameCountries = "countries";
	const char* kFieldNameAge = "age";
	const char* kFieldNameDescription = "description";
	const char* kFieldNameRegion = "region";
	const char* kFieldNameRate = "rate";
	const char* kFieldNameIsDeleted = "is_deleted";
	const char* kFieldNameActor = "actor";
	const char* kFieldNamePriceId = "price_id";
	const char* kFieldNameLocation = "location";
	const char* kFieldNameEndTime = "end_time";
	const char* kFieldNameStartTime = "start_time";
	const char* kFieldNamePhone = "phone";
	const std::string kFieldNameTemp = "tmp";
	const char* kFieldNameNumeric = "numeric";
	const std::string kFieldNameBookid = "bookid";
	const std::string kFieldNameBookid2 = "bookid2";
	const char* kFieldNameTitle = "title";
	const char* kFieldNamePages = "pages";
	const char* kFieldNamePrice = "price";
	const char* kFieldNameUuid = "uuid";
	const char* kFieldNameUuidSparse = "uuid_sparse";
	const char* kFieldNameUuidArr = "uuid_arr";
	const char* kFieldNameUuidArrSparse = "uuid_arr_sparse";
	const char* kFieldNameUuidNotIndex = "uuid_not_index";
	const char* kFieldNameUuidNotIndex2 = "uuid_not_index_2";
	const char* kFieldNameUuidNotIndex3 = "uuid_not_index_3";
	const char* kFieldNameRndString = "rndString";
	const char* kFieldNameBtreeIdsets = "btree_idsets";
	const char* kFieldNamePointQuadraticRTree = "point_quadratic_rtree";
	const char* kFieldNamePointLinearRTree = "point_linear_rtree";
	const char* kFieldNamePointGreeneRTree = "point_greene_rtree";
	const char* kFieldNamePointRStarRTree = "point_rstar_rtree";
	const char* kFieldNamePointNonIndex = "point_field_non_index";

	const char* kFieldNameColumnInt = "columnInt";
	const std::string kFieldNameColumnInt64 = "columnInt64";
	const char* kFieldNameColumnDouble = "columnDouble";
	const char* kFieldNameColumnString = "columnString";
	const char* kFieldNameColumnFullText = "columnFullText";
	const char* kFieldNameColumnStringNumeric = "columnStringNumeric";

	const char* kFieldNameColumnHash = "columnHash";
	const char* kFieldNameColumnTree = "columnTree";
	const char* kFieldNameObjectField = "object";

	const std::string testSimpleNs = "test_simple_namespace";
	const std::string joinNs = "join_namespace";
	const std::string compositeIndexesNs = "composite_indexes_namespace";
	const std::string comparatorsNs = "comparators_namespace";
	const std::string forcedSortNs = "forced_sort_offset_namespace";
	const std::string nsWithObject = "namespace_with_object";
	const std::string geomNs = "geom_namespace";
	const std::string uuidNs = "uuid_namespace";
	const std::string btreeIdxOptNs = "btree_idx_opt_namespace";
	const std::string conditionsNs = "conditions_namespace";

	const std::string compositePlus = "+";
	const std::string kCompositeFieldIdTemp = kFieldNameId + compositePlus + kFieldNameTemp;
	const std::string kCompositeFieldAgeGenre = kFieldNameAge + compositePlus + kFieldNameGenre;
	const std::string kCompositeFieldUuidName = kFieldNameUuid + compositePlus + kFieldNameName;
	const std::string kCompositeFieldPricePages = kFieldNamePrice + compositePlus + kFieldNamePages;
	const std::string kCompositeFieldTitleName = kFieldNameTitle + compositePlus + kFieldNameName;
	const std::string kCompositeFieldPriceTitle = kFieldNamePrice + compositePlus + kFieldNameTitle;
	const std::string kCompositeFieldPagesTitle = kFieldNamePages + compositePlus + kFieldNameTitle;
	const std::string kCompositeFieldBookidBookid2 = kFieldNameBookid + compositePlus + kFieldNameBookid2;

	std::atomic<int> currBtreeIdsetsValue = rand() % 10000;
	static constexpr size_t forcedSortOffsetNsSize = 1000;
	static constexpr int forcedSortOffsetMaxValue = 1000;
	static constexpr size_t geomNsSize = 10000;
	static constexpr size_t uuidNsSize = 10000;
	static constexpr int btreeIdxOptNsSize = 10000;
	size_t conditionsNsSize = 0;
	std::vector<std::pair<int, int>> forcedSortOffsetValues;
};
