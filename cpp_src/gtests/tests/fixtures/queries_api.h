#pragma once

#include <cmath>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/geometry.h"
#include "core/nsselecter/sortexpression.h"
#include "core/queryresults/joinresults.h"
#include "core/type_consts_helpers.h"
#include "gtests/tools.h"
#include "queries_verifier.h"
#include "reindexer_api.h"
#include "tools/random.h"
#include "tools/stringstools.h"

class QueriesApi : public ReindexerApi, public QueriesVerifier {
public:
	void SetUp() override {
		Error err = rt.reindexer->InitSystemNamespaces();
		ASSERT_TRUE(err.ok()) << err.what();
		setPkFields(default_namespace, {kFieldNameId, kFieldNameTemp});
		setPkFields(testSimpleNs, {kFieldNameId});
		setPkFields(joinNs, {kFieldNameId});
		setPkFields(compositeIndexesNs, {kFieldNameBookid, kFieldNameBookid2});
		setPkFields(comparatorsNs, {kFieldNameColumnInt64});
		setPkFields(forcedSortOffsetNs, {kFieldNameId});
		setPkFields(geomNs, {kFieldNameId});
		setPkFields(btreeIdxOptNs, {kFieldNameId});

		indexesCollates = {{kFieldNameActor, CollateOpts{CollateUTF8}},
						   {kFieldNameLocation, CollateOpts{CollateNone}},
						   {kFieldNameTemp, CollateOpts{CollateASCII}},
						   {kFieldNameNumeric, CollateOpts{CollateUTF8}}};

		err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(default_namespace,
							   {
								   IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameGenre, "tree", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNamePackages, "hash", "int", IndexOpts{}.Array(), 0},
								   IndexDeclaration{kFieldNameName, "tree", "string", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameCountries, "tree", "string", IndexOpts{}.Array(), 0},
								   IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameDescription, "fuzzytext", "string", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameRate, "tree", "double", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameIsDeleted, "-", "bool", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameActor, "tree", "string", IndexOpts{}.SetCollateMode(CollateUTF8), 0},
								   IndexDeclaration{kFieldNamePriceId, "hash", "int", IndexOpts{}.Array(), 0},
								   IndexDeclaration{kFieldNameLocation, "tree", "string", IndexOpts{}.SetCollateMode(CollateNone), 0},
								   IndexDeclaration{kFieldNameEndTime, "hash", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameStartTime, "tree", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameBtreeIdsets, "hash", "int", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameTemp, "tree", "string", IndexOpts{}.SetCollateMode(CollateASCII), 0},
								   IndexDeclaration{kFieldNameNumeric, "tree", "string", IndexOpts{}.SetCollateMode(CollateUTF8), 0},
								   IndexDeclaration{kFieldNameUuid, "hash", "uuid", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameUuidArr, "hash", "uuid", IndexOpts{}.Array(), 0},
								   IndexDeclaration{kCompositeFieldIdTemp, "tree", "composite", IndexOpts{}.PK(), 0},
								   IndexDeclaration{kCompositeFieldAgeGenre, "hash", "composite", IndexOpts{}, 0},
								   IndexDeclaration{kCompositeFieldUuidName, "hash", "composite", IndexOpts{}, 0},
								   IndexDeclaration{kFieldNameYearSparse, "hash", "string", IndexOpts{}.Sparse(), 0},
							   });
		addIndexFields(default_namespace, kCompositeFieldIdTemp,
					   {{kFieldNameId, reindexer::KeyValueType::Int{}}, {kFieldNameTemp, reindexer::KeyValueType::String{}}});
		addIndexFields(default_namespace, kCompositeFieldAgeGenre,
					   {{kFieldNameAge, reindexer::KeyValueType::Int{}}, {kFieldNameGenre, reindexer::KeyValueType::Int{}}});
		addIndexFields(default_namespace, kCompositeFieldUuidName,
					   {{kFieldNameUuid, reindexer::KeyValueType::Uuid{}}, {kFieldNameName, reindexer::KeyValueType::String{}}});

		err = rt.reindexer->OpenNamespace(joinNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(joinNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
										IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0},
										IndexDeclaration{kFieldNameAge, "tree", "int", IndexOpts(), 0},
										IndexDeclaration{kFieldNameName, "tree", "string", IndexOpts(), 0},
										IndexDeclaration{kFieldNameDescription, "text", "string", IndexOpts{}, 0},
										IndexDeclaration{kFieldNameYearSparse, "hash", "string", IndexOpts().Sparse(), 0}});

		err = rt.reindexer->OpenNamespace(testSimpleNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(testSimpleNs, {
												 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
												 IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0},
												 IndexDeclaration{kFieldNameName, "hash", "string", IndexOpts(), 0},
												 IndexDeclaration{kFieldNamePhone, "hash", "string", IndexOpts(), 0},
											 });

		err = rt.reindexer->OpenNamespace(compositeIndexesNs);
		ASSERT_TRUE(err.ok()) << err.what();
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

		err = rt.reindexer->OpenNamespace(comparatorsNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			comparatorsNs,
			{IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnInt, "hash", "int", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnInt64, "hash", "int64", IndexOpts().PK(), 0},
			 IndexDeclaration{kFieldNameColumnDouble, "tree", "double", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnString, "-", "string", IndexOpts(), 0},
			 IndexDeclaration{kFieldNameColumnFullText, "text", "string", IndexOpts().SetConfig(R"xxx({"stemmers":[]})xxx"), 0},
			 IndexDeclaration{kFieldNameColumnStringNumeric, "-", "string", IndexOpts().SetCollateMode(CollateNumeric), 0}});

		err = rt.reindexer->OpenNamespace(forcedSortOffsetNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(forcedSortOffsetNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
													IndexDeclaration{kFieldNameColumnHash, "hash", "int", IndexOpts(), 0},
													IndexDeclaration{kFieldNameColumnTree, "tree", "int", IndexOpts(), 0}});

		err = rt.reindexer->OpenNamespace(geomNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			geomNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
					 IndexDeclaration{kFieldNamePointQuadraticRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::Quadratic), 0},
					 IndexDeclaration{kFieldNamePointLinearRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::Linear), 0},
					 IndexDeclaration{kFieldNamePointGreeneRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::Greene), 0},
					 IndexDeclaration{kFieldNamePointRStarRTree, "rtree", "point", IndexOpts{}.RTreeType(IndexOpts::RStar), 0}});

		err = rt.reindexer->OpenNamespace(btreeIdxOptNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(btreeIdxOptNs, {IndexDeclaration{kFieldNameId, "tree", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{kFieldNameStartTime, "tree", "int", IndexOpts(), 0}});
		initConditionsNs();
	}

	void initConditionsNs();
	void FillConditionsNs();
	void CheckConditions();
	enum class NullAllowed : bool { Yes = true, No = false };
	void checkAllConditions(const std::string& fieldName, reindexer::KeyValueType fieldType, NullAllowed);

	template <typename... T>
	void ExecuteAndVerify(const Query& query, T... args) {
		reindexer::QueryResults qr;
		const_cast<Query&>(query).Explain();
		Error err = rt.reindexer->Select(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		Verify(qr, query, *rt.reindexer);
		Verify(qr, args...);
	}

	void ExecuteAndVerifyWithSql(const Query& query) {
		ExecuteAndVerify(query);
		Query queryFromSql;
		queryFromSql.FromSQL(query.GetSQL());
		ExecuteAndVerify(queryFromSql);
	}

	template <typename... T>
	void ExecuteAndVerify(const Query& query, QueryResults& qr, T... args) {
		const_cast<Query&>(query).Explain();
		Error err = rt.reindexer->Select(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		Verify(qr, query, *rt.reindexer);
		Verify(qr, args...);
	}

	void ExecuteAndVerifyWithSql(const Query& query, QueryResults& qr) {
		ExecuteAndVerify(query, qr);
		Query queryFromSql;
		queryFromSql.FromSQL(query.GetSQL());
		qr.Clear();
		ExecuteAndVerify(queryFromSql, qr);
	}

	void Verify(const reindexer::QueryResults&) const noexcept {}

	template <typename... T>
	void Verify(const reindexer::QueryResults& qr, const char* fieldName, const std::vector<reindexer::Variant>& expectedValues,
				T... args) {
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
				if (i != 0) ser << ", ";
				expectedValues[i].Dump(ser);
			}
			ser << "\nObtained values:\n";
			for (size_t i = 0; i < qr.Count(); ++i) {
				if (i != 0) ser << ", ";
				reindexer::Item item(qr[i].GetItem(false));
				const reindexer::Variant fieldValue = item[fieldName];
				fieldValue.Dump(ser);
			}
			FAIL() << ser.Slice() << std::endl;
		}
		Verify(qr, args...);
	}
	using QueriesVerifier::Verify;

protected:
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
			const auto err = Commit(compositeIndexesNs);
			ASSERT_TRUE(err.ok()) << err.what();

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
		const auto err = Commit(compositeIndexesNs);
		ASSERT_TRUE(err.ok()) << err.what();

		saveItem(std::move(lastItem), compositeIndexesNs);
	}

	void FillForcedSortNamespace() {
		forcedSortOffsetValues.clear();
		forcedSortOffsetValues.reserve(forcedSortOffsetNsSize);
		for (size_t i = 0; i < forcedSortOffsetNsSize; ++i) {
			Item item = NewItem(forcedSortOffsetNs);
			item[kFieldNameId] = static_cast<int>(i);
			forcedSortOffsetValues.emplace_back(rand() % forcedSortOffsetMaxValue, rand() % forcedSortOffsetMaxValue);
			item[kFieldNameColumnHash] = forcedSortOffsetValues.back().first;
			item[kFieldNameColumnTree] = forcedSortOffsetValues.back().second;
			Upsert(forcedSortOffsetNs, item);
			saveItem(std::move(item), forcedSortOffsetNs);
		}
		const auto err = Commit(forcedSortOffsetNs);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void FillTestJoinNamespace() {
		for (int i = 0; i < 300; ++i) {
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
			Upsert(joinNs, item);
			saveItem(std::move(item), joinNs);
		}
		const auto err = Commit(testSimpleNs);
		ASSERT_TRUE(err.ok()) << err.what();
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

		const auto err = Commit(testSimpleNs);
		ASSERT_TRUE(err.ok()) << err.what();
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
				double arr[]{point.X(), point.Y()};
				bld.Array(kFieldNamePointQuadraticRTree, reindexer::span<double>{arr, 2});

				point = randPoint(10);
				arr[0] = point.X();
				arr[1] = point.Y();
				bld.Array(kFieldNamePointLinearRTree, reindexer::span<double>{arr, 2});

				point = randPoint(10);
				arr[0] = point.X();
				arr[1] = point.Y();
				bld.Array(kFieldNamePointGreeneRTree, reindexer::span<double>{arr, 2});

				point = randPoint(10);
				arr[0] = point.X();
				arr[1] = point.Y();
				bld.Array(kFieldNamePointRStarRTree, reindexer::span<double>{arr, 2});

				point = randPoint(10);
				arr[0] = point.X();
				arr[1] = point.Y();
				bld.Array(kFieldNamePointNonIndex, reindexer::span<double>{arr, 2});
			}
			auto item = NewItem(geomNs);
			const auto err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			Upsert(geomNs, item);

			saveItem(std::move(item), geomNs);
		}
		const auto err = Commit(geomNs);
		ASSERT_TRUE(err.ok()) << err.what();
		lastId += geomNsSize;
	}

	void UpsertBtreeIdxOptNsItem(std::pair<int, int> values) {
		Item item = NewItem(btreeIdxOptNs);
		ASSERT_TRUE(item.Status().ok());
		item[kFieldNameId] = values.first;
		item[kFieldNameStartTime] = values.second;
		Upsert(btreeIdxOptNs, item);

		saveItem(std::move(item), btreeIdxOptNs);

		Error err = Commit(btreeIdxOptNs);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	enum Column { First, Second };

	std::vector<Variant> ForcedSortOffsetTestExpectedResults(size_t offset, size_t limit, bool desc,
															 const std::vector<int>& forcedSortOrder, Column column) const {
		if (limit == 0 || offset >= forcedSortOffsetValues.size()) return {};
		std::vector<int> res;
		res.resize(forcedSortOffsetValues.size());
		std::transform(
			forcedSortOffsetValues.cbegin(), forcedSortOffsetValues.cend(), res.begin(),
			column == First ? [](const std::pair<int, int>& v) { return v.first; } : [](const std::pair<int, int>& v) { return v.second; });
		std::sort(
			res.begin(), res.end(), desc ? [](int lhs, int rhs) { return lhs > rhs; } : [](int lhs, int rhs) { return lhs < rhs; });
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
		if (limit == 0 || offset >= forcedSortOffsetValues.size()) return {};
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
			Item item(rt.reindexer->NewItem(comparatorsNs));
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

		const auto err = Commit(comparatorsNs);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void FillDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);

			saveItem(std::move(item), default_namespace);
		}
		const auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void AddToDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = start; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);
		}
		const auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void FillDefaultNamespaceTransaction(int start, int count, int packagesCount) {
		auto tr = rt.reindexer->NewTransaction(default_namespace);

		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			tr.Insert(std::move(item));
		}
		QueryResults res;
		rt.reindexer->CommitTransaction(tr, res);
		const auto err = Commit(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
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
		for (size_t i = 0; i < count; ++i) res.emplace_back(RandString());
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
			for (size_t i = 0; i < s; ++i) arr.emplace_back(randUuid());
			item[kFieldNameUuidArr] = std::move(arr);
		} else {
			item[kFieldNameUuid] = randStrUuid();
			std::vector<std::string> arr;
			arr.reserve(s);
			for (size_t i = 0; i < s; ++i) arr.emplace_back(randStrUuid());
			item[kFieldNameUuidArr] = std::move(arr);
		}

		return item;
	}

	void RandVariantArray(size_t size, size_t max, VariantArray& arr) {
		arr.clear<false>();
		arr.reserve(size);
		for (size_t i = 0; i < size; ++i) {
			arr.emplace_back(int(rand() % max));
		}
	}

	static std::string pointToSQL(reindexer::Point point, bool escape = false) {
		return escape ? fmt::sprintf("ST_GeomFromText(\\'point(%.12f %.12f)\\')", point.X(), point.Y())
					  : fmt::sprintf("ST_GeomFromText('point(%.12f %.12f)')", point.X(), point.Y());
	}

	void CheckMergeQueriesWithLimit();

	void CheckGeomQueries() {
		for (size_t i = 0; i < 10; ++i) {
			// Checks that DWithin and sort by Distance work and verifies the result
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointQuadraticRTree, randPoint(10), randBinDouble(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointLinearRTree, randPoint(10), randBinDouble(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointGreeneRTree, randPoint(10), randBinDouble(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointRStarRTree, randPoint(10), randBinDouble(0, 1)));
			ExecuteAndVerify(Query(geomNs).DWithin(kFieldNamePointNonIndex, randPoint(10), randBinDouble(0, 1)));
			ExecuteAndVerify(Query(geomNs)
								 .DWithin(kFieldNamePointLinearRTree, randPoint(10), randBinDouble(0, 1))
								 .SortStDistance(kFieldNamePointNonIndex, kFieldNamePointLinearRTree, false));
			ExecuteAndVerify(
				Query(geomNs)
					.DWithin(kFieldNamePointLinearRTree, randPoint(10), randBinDouble(0, 1))
					.SortStDistance(kFieldNamePointNonIndex, randPoint(10), false)
					.Sort(std::string("ST_Distance(") + pointToSQL(randPoint(10)) + ", " + kFieldNamePointGreeneRTree + ')', false));
			ExecuteAndVerify(Query(geomNs)
								 .DWithin(kFieldNamePointQuadraticRTree, randPoint(10), randBinDouble(0, 1))
								 .Or()
								 .DWithin(kFieldNamePointRStarRTree, randPoint(10), randBinDouble(0, 1))
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

			ExecuteAndVerifyWithSql(
				Query(default_namespace).Where(kFieldNameGenre, CondEq, randomGenre).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(
				Query(default_namespace).Where(kFieldNameName, CondEq, RandString()).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameRate, CondEq, static_cast<double>(rand() % 100) / 10)
										.Distinct(distinct.c_str())
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameGenre, CondGt, randomGenre)
										.Distinct(distinct.c_str())
										.Sort(kFieldNameYear, true)
										.Debug(LogTrace));

			ExecuteAndVerifyWithSql(
				Query(default_namespace).Where(kFieldNameName, CondGt, RandString()).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameRate, CondGt, static_cast<double>(rand() % 100) / 10)
										.Distinct(distinct.c_str())
										.Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(
				Query(default_namespace).Where(kFieldNameGenre, CondLt, randomGenre).Distinct(distinct.c_str()).Sort(kFieldNameYear, true));

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameAge, CondEq, randomAge)
										.Where(kFieldNameGenre, CondEq, randomGenre)
										.Distinct(distinct.c_str())
										.Sort(kFieldNameYear, true));
		}
	}

	void CheckConditionsMergingQueries() {
		// Check merging of conditions by the same index with large sets of values
		VariantArray arr1, arr2, emptyArr;
		for (size_t i = 0; i < 3; ++i) {
			RandVariantArray(500, 1000, arr1);
			RandVariantArray(100, 1000, arr2);
			arr2.insert(arr2.end(), arr1.begin(), arr1.begin() + 100);

			ExecuteAndVerifyWithSql(
				Query(default_namespace).Where(kFieldNameNumeric, CondSet, arr1).Where(kFieldNameNumeric, CondSet, arr2));

			ExecuteAndVerifyWithSql(Query(default_namespace)
										.Where(kFieldNameNumeric, CondSet, arr1)
										.Where(kFieldNameNumeric, CondSet, emptyArr)
										.Where(kFieldNameNumeric, CondSet, arr2));
		}
	}

	void CheckStandartQueries() {
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

			static const std::string compositeIndexName(kFieldNameAge + compositePlus + kFieldNameGenre);

			for (const bool sortOrder : sortOrders) {
				for (const auto& sortIdx : sortIdxs) {
					for (const std::string& distinct : distincts) {
						const int randomAge = rand() % 50;
						const int randomGenre = rand() % 50;
						const int randomGenreUpper = rand() % 100;
						const int randomGenreLower = rand() % 100;

						ExecuteAndVerify(Query(default_namespace).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder).Limit(1));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameGenre, CondEq, randomGenre)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameGenre, CondEq, std::to_string(randomGenre))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondEq, RandString())
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameRate, CondEq, (rand() % 100) / 10.0)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameRate, CondEq, std::to_string((rand() % 100) / 10.0))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameGenre, CondGt, randomGenre)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondGt, RandString())
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameRate, CondGt, (rand() % 100) / 10.0)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameGenre, CondLt, randomGenre)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameGenre, CondLt, std::to_string(randomGenre))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondLt, RandString())
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameRate, CondLt, (rand() % 100) / 10.0)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameRate, CondLt, std::to_string((rand() % 100) / 10.0))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameBtreeIdsets, CondLt, static_cast<int>(rand() % 10000))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameBtreeIdsets, CondGt, static_cast<int>(rand() % 10000))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameBtreeIdsets, CondEq, static_cast<int>(rand() % 10000))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameGenre, CondRange, {randomGenreLower, randomGenreUpper})
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondRange, {RandString(), RandString()})
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondLike, RandLikePattern())
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameRate, CondRange,
													{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNamePackages, CondSet, RandIntVector(10, 10000, 50))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNamePackages, CondAllSet, RandIntVector(2, 10000, 50))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNamePackages, CondAllSet, 10000 + rand() % 50)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						// check substituteCompositIndexes
						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameAge, CondEq, randomAge)
											 .Where(kFieldNameGenre, CondEq, randomGenre)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
											 .Where(kFieldNameGenre, CondEq, randomGenre)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameAge, CondAllSet, RandIntVector(1, 0, 50))
											 .Where(kFieldNameGenre, CondEq, randomGenre)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 50))
											 .Where(kFieldNameGenre, CondSet, RandIntVector(10, 0, 50))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 20))
											 .Where(kFieldNameGenre, CondSet, RandIntVector(10, 0, 50))
											 .Where(kFieldNameAge, CondSet, RandIntVector(10, 30, 50))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameAge, CondSet, RandIntVector(10, 0, 20))
											 .Where(kFieldNameGenre, CondEq, randomGenre)
											 .Where(kFieldNameAge, CondSet, RandIntVector(10, 30, 50))
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));
						// end of check substituteCompositIndexes

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNamePackages, CondEmpty, 0)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondRange, {RandString(), RandString()})
											 .Distinct(distinct.c_str())
											 .Sort(kFieldNameYear, true)
											 .Sort(kFieldNameName, false)
											 .Sort(kFieldNameLocation, true));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondRange, {RandString(), RandString()})
											 .Distinct(distinct.c_str())
											 .Sort(kFieldNameGenre, true)
											 .Sort(kFieldNameActor, false)
											 .Sort(kFieldNameRate, true)
											 .Sort(kFieldNameLocation, false));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameName, CondLike, RandLikePattern())
											 .Distinct(distinct.c_str())
											 .Sort(kFieldNameGenre, true)
											 .Sort(kFieldNameActor, false)
											 .Sort(kFieldNameRate, true)
											 .Sort(kFieldNameLocation, false));

						ExecuteAndVerify(Query(default_namespace).Sort(kFieldNameGenre, true, {10, 20, 30}));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNamePackages, CondAny, 0)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameIsDeleted, CondEq, 1)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Where(kFieldNameGenre, CondEq, 5)
											 .Where(kFieldNameAge, CondEq, 3)
											 .Where(kFieldNameYear, CondGe, 2010)
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
											 .Debug(LogTrace));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
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
											 .Distinct(distinct.c_str())
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
											 .Distinct(distinct.c_str())
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
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameGenre, CondSet, {5, 1, 7})
											 .Where(kFieldNameYear, CondLt, 2010)
											 .Where(kFieldNameGenre, CondEq, 3)
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
											 .Or()
											 .Where(kFieldNamePackages, CondEmpty, 0)
											 .Debug(LogTrace));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameGenre, CondSet, {5, 1, 7})
											 .Where(kFieldNameYear, CondLt, 2010)
											 .Or()
											 .Where(kFieldNamePackages, CondAny, 0)
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
											 .Debug(LogTrace));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameGenre, CondEq, 5)
											 .Or()
											 .Where(kFieldNameGenre, CondEq, 6)
											 .Where(kFieldNameYear, CondRange, {2001, 2020})
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameGenre, CondEq, 5)
											 .Or()
											 .Where(kFieldNameGenre, CondEq, 6)
											 .Not()
											 .Where(kFieldNameName, CondLike, RandLikePattern())
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameActor, CondEq, RandString()));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Not()
											 .Where(kFieldNameGenre, CondEq, 5)
											 .Where(kFieldNameYear, CondRange, {2001, 2020})
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameGenre, CondEq, 5)
											 .Not()
											 .Where(kFieldNameYear, CondRange, {2001, 2020})
											 .Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder)
											 .Debug(LogTrace)
											 .Not()
											 .Where(kFieldNameYear, CondEq, 10));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(kFieldNameNumeric, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameNumeric, CondGt, std::to_string(5)));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
											 .Sort(kFieldNameNumeric, sortOrder)
											 .Debug(LogTrace)
											 .Where(kFieldNameNumeric, CondLt, std::to_string(600)));

						ExecuteAndVerify(Query(default_namespace)
											 .Distinct(distinct.c_str())
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
											 .Distinct(distinct.c_str())
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
								.Distinct(distinct.c_str())
								.Sort(sortIdx, sortOrder)
								.Debug(LogTrace)
								.Where(kFieldNameNumeric, CondRange, {std::to_string(rand() % 100), std::to_string(rand() % 100 + 500)}));

						ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS"));
						ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002));
						ExecuteAndVerify(Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, 2002));
						ExecuteAndVerify(
							Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 2002));
						ExecuteAndVerify(
							Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 1989));
						ExecuteAndVerify(
							Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, "MMM"));

						ExecuteAndVerify(Query(default_namespace)
											 .ReqTotal()
											 .Distinct(distinct)
											 .Sort(sortIdx, sortOrder)
											 .WhereComposite(compositeIndexName.c_str(), CondLe, {{Variant(27), Variant(10000)}}));

						ExecuteAndVerify(Query(default_namespace)
											 .ReqTotal()
											 .Distinct(distinct)
											 .Sort(kFieldNameAge + " + "s + kFieldNameId, sortOrder)
											 .Sort(kFieldNameRate + " * "s + kFieldNameGenre, sortOrder));

						ExecuteAndVerify(
							Query(default_namespace)
								.ReqTotal()
								.Distinct(distinct)
								.Sort(sortIdx, sortOrder)
								.WhereComposite(compositeIndexName.c_str(), CondEq, {{Variant(rand() % 10), Variant(rand() % 50)}}));

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

						ExecuteAndVerify(Query(default_namespace)
											 .InnerJoin(kFieldNameYear, kFieldNameYear, CondEq,
														Query(joinNs)
															.Where(kFieldNameYear, CondGe, 1925)
															.Sort(kFieldNameId + " + "s + kFieldNameYear, sortOrder))
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

						for (CondType cond : {CondEq, CondSet, CondLt, CondLe, CondGt, CondGe}) {
							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuid, cond, randUuid())
												 .Distinct(distinct.c_str())
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuid, cond, randStrUuid())
												 .Distinct(distinct.c_str())
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(
								Query(default_namespace)
									.Where(kFieldNameUuid, cond,
										   VariantArray::Create(randUuid(), randStrUuid(), randUuid(), randStrUuid(), randUuid()))
									.Distinct(distinct.c_str())
									.Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuidArr, cond, randUuid())
												 .Distinct(distinct.c_str())
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuidArr, cond, randStrUuid())
												 .Distinct(distinct.c_str())
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(Query(default_namespace)
												 .Where(kFieldNameUuidArr, cond,
														VariantArray::Create(randUuid(), randStrUuid(), randUuid(), randStrUuid(),
																			 randUuid(), randStrUuid()))
												 .Distinct(distinct.c_str())
												 .Sort(sortIdx, sortOrder));

							ExecuteAndVerify(
								Query(default_namespace)
									.WhereComposite(
										kFieldNameUuid + compositePlus + kFieldNameName, cond,
										{VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString()), VariantArray::Create(randStrUuid(), RandString()),
										 VariantArray::Create(randUuid(), RandString())})
									.Distinct(distinct.c_str())
									.Sort(sortIdx, sortOrder));
						}

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameUuid, CondRange, {nilUuid(), randUuid()})
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .Where(kFieldNameUuidArr, CondRange, {nilUuid(), randUuid()})
											 .Distinct(distinct.c_str())
											 .Sort(sortIdx, sortOrder));

						ExecuteAndVerify(Query(default_namespace)
											 .WhereComposite(kFieldNameUuid + compositePlus + kFieldNameName, CondRange,
															 {VariantArray::Create(nilUuid(), RandString()),
															  VariantArray::Create(randUuid(), RandString())})
											 .Distinct(distinct.c_str())
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
		Error err = rt.reindexer->OpenNamespace(nsWithObject);
		ASSERT_TRUE(err.ok()) << err.what();
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
		err = Commit(nsWithObject);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void CheckAggregationQueries() {
		constexpr size_t facetLimit = 10;
		constexpr size_t facetOffset = 10;

		EXPECT_THROW(Query(default_namespace).Aggregate(AggAvg, {}), reindexer::Error);

		EXPECT_THROW(Query(default_namespace).Aggregate(AggAvg, {kFieldNameYear, kFieldNameName}), reindexer::Error);

		EXPECT_THROW(Query(default_namespace).Aggregate(AggAvg, {kFieldNameYear}, {{kFieldNameYear, true}}), reindexer::Error);

		EXPECT_THROW(Query(default_namespace).Aggregate(AggAvg, {kFieldNameYear}, {}, 10), reindexer::Error);

		const Query wrongQuery1{Query(default_namespace).Aggregate(AggFacet, {kFieldNameYear}, {{kFieldNameName, true}})};
		reindexer::QueryResults wrongQr1;
		auto err = rt.reindexer->Select(wrongQuery1, wrongQr1);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "The aggregation facet cannot provide sort by 'name'");

		const Query wrongQuery2{Query(default_namespace).Aggregate(AggFacet, {kFieldNameCountries, kFieldNameYear})};
		reindexer::QueryResults wrongQr2;
		err = rt.reindexer->Select(wrongQuery2, wrongQr2);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Multifield facet cannot contain an array field");

		InitNSObj();
		const Query wrongQuery3{Query(nsWithObject).Distinct(kFieldNameObjectField)};
		reindexer::QueryResults wrongQr3;
		err = rt.reindexer->Select(wrongQuery3, wrongQr3);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Cannot aggregate object field");

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

		reindexer::QueryResults testQr;
		err = rt.reindexer->Select(testQuery, testQr);
		EXPECT_TRUE(err.ok()) << err.what();

		reindexer::QueryResults checkQr;
		err = rt.reindexer->Select(checkQuery, checkQr);
		EXPECT_TRUE(err.ok()) << err.what();

		double yearSum = 0.0;
		int packagesMin = std::numeric_limits<int>::max();
		struct MultifieldFacetItem {
			std::string name;
			int year;
			bool operator<(const MultifieldFacetItem& other) const {
				if (year == other.year) return name < other.name;
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

		ASSERT_EQ(testQr.aggregationResults.size(), 10);
		EXPECT_DOUBLE_EQ(testQr.aggregationResults[0].GetValueOrZero(), yearSum / checkQr.Count())
			<< "Aggregation Avg result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.aggregationResults[1].GetValueOrZero(), yearSum) << "Aggregation Sum result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.aggregationResults[2].GetValueOrZero(), packagesMin) << "Aggregation Min result is incorrect!";
		checkFacetUnordered(testQr.aggregationResults[3].facets, singlefieldFacetUnordered, "SinglefieldUnordered");
		checkFacet(testQr.aggregationResults[4].facets, singlefieldFacetByName, "SinglefieldName");
		checkFacet(testQr.aggregationResults[5].facets, singlefieldFacetByCount, "SinglefieldCount");
		checkFacetUnordered(testQr.aggregationResults[6].facets, arrayFacetUnordered, "ArrayUnordered");
		checkFacet(testQr.aggregationResults[7].facets, arrayFacetByCount, "ArrayByCount");
		checkFacet(testQr.aggregationResults[8].facets, arrayFacetByName, "ArrayByName");
		checkFacet(testQr.aggregationResults[9].facets, multifieldFacet, "Multifield");
	}

	void CompareQueryResults(const std::string& serializedQuery, const QueryResults& lhs, const QueryResults& rhs) {
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
								EXPECT_EQ(v1[j].Compare(v2[j]), 0);
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
					EXPECT_EQ(aggRes1.type, aggRes2.type);
					EXPECT_DOUBLE_EQ(aggRes1.GetValueOrZero(), aggRes2.GetValueOrZero());
					EXPECT_EQ(aggRes1.fields.size(), aggRes2.fields.size());
					if (aggRes1.fields.size() == aggRes2.fields.size()) {
						for (size_t j = 0; j < aggRes1.fields.size(); ++j) {
							EXPECT_EQ(aggRes1.fields[j], aggRes2.fields[j]);
						}
					}
					EXPECT_EQ(aggRes1.facets.size(), aggRes2.facets.size());
					if (aggRes1.facets.size() == aggRes2.facets.size()) {
						for (size_t j = 0; j < aggRes1.facets.size(); ++j) {
							EXPECT_EQ(aggRes1.facets[j].count, aggRes2.facets[j].count);
							EXPECT_EQ(aggRes1.facets[j].values.size(), aggRes2.facets[j].values.size());
							if (aggRes1.facets[j].values.size() == aggRes2.facets[j].values.size()) {
								for (size_t k = 0; k < aggRes1.facets[j].values.size(); ++k) {
									if (aggRes1.facets[j].values[k] != aggRes2.facets[j].values[k]) {
										assertrx(0);
									}
									EXPECT_EQ(aggRes1.facets[j].values[k], aggRes2.facets[j].values[k]) << aggRes1.facets[j].values[0];
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

	void checkDslQuery(const std::string& dslQuery, const Query& checkQuery) {
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
		Verify(checkQr, checkQuery, *rt.reindexer);
	}

	static std::string toString(double v) {
		std::ostringstream res;
		res.precision(std::numeric_limits<double>::digits10 + 1);
		res << v;
		return res.str();
	}
	// Checks that DSL queries works and compares the result with the result of corresponding C++ query
	void CheckDslQueries() {
		using namespace std::string_literals;
		// ----------
		reindexer::Point point{randPoint(10)};
		double distance = randBinDouble(0, 1);
		std::string dslQuery = fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"dwithin","field":"%s","value":[[%f, %f], %f]}],"merge_queries":[],"aggregations":[]})",
			geomNs, kFieldNamePointLinearRTree, point.X(), point.Y(), distance);
		const Query checkQuery1{Query(geomNs).DWithin(kFieldNamePointLinearRTree, point, distance)};
		checkDslQuery(dslQuery, checkQuery1);

		// ----------
		point = randPoint(10);
		distance = randBinDouble(0, 1);
		dslQuery = fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"dwithin","field":"%s","value":[%f,[%f,%f]]}],"merge_queries":[],"aggregations":[]})",
			geomNs, kFieldNamePointLinearRTree, distance, point.X(), point.Y());
		const Query checkQuery2{Query(geomNs).DWithin(kFieldNamePointLinearRTree, point, distance)};
		checkDslQuery(dslQuery, checkQuery2);

		// ----------
		dslQuery = fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"gt","first_field":"%s","second_field":"%s"}],"merge_queries":[],"aggregations":[]})",
			default_namespace, kFieldNameStartTime, kFieldNamePackages);
		const Query checkQuery3{Query{default_namespace}.WhereBetweenFields(kFieldNameStartTime, CondGt, kFieldNamePackages)};
		checkDslQuery(dslQuery, checkQuery3);

		// -------
		dslQuery = fmt::sprintf(
			R"({"namespace":"%s","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"SET","field":"%s","Value":["1", " 10 ", "100 ", " 1000"]}],"merge_queries":[],"aggregations":[]})",
			default_namespace, kFieldNameId);
		const Query checkQuery4{Query{default_namespace}.Where(kFieldNameId, CondSet, {1, 10, 100, 1000})};
		checkDslQuery(dslQuery, checkQuery4);
	}

	void CheckSqlQueries() {
		using namespace std::string_literals;
		std::string sqlQuery = "SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery1{Query(default_namespace, 0, 10000000).Where(kFieldNameYear, CondGt, 2016).Sort(kFieldNameYear, true)};

		QueryResults sqlQr1;
		Error err = rt.reindexer->Select(sqlQuery, sqlQr1);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr1;
		err = rt.reindexer->Select(checkQuery1, checkQr1);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr1, checkQr1);
		Verify(checkQr1, checkQuery1, *rt.reindexer);

		sqlQuery = "SELECT ID, Year, Genre FROM test_namespace WHERE genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery2{
			Query(default_namespace, 0, 10000000).Where(kFieldNameGenre, CondSet, {1, 2, 3}).Sort(kFieldNameYear, true)};

		QueryResults sqlQr2;
		err = rt.reindexer->Select(sqlQuery, sqlQr2);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr2;
		err = rt.reindexer->Select(checkQuery2, checkQr2);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr2, checkQr2);
		Verify(checkQr2, checkQuery2, *rt.reindexer);

		const std::string likePattern = RandLikePattern();
		sqlQuery = "SELECT ID, Year, Genre FROM test_namespace WHERE name LIKE '" + likePattern + "' ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery3{
			Query(default_namespace, 0, 10000000).Where(kFieldNameName, CondLike, likePattern).Sort(kFieldNameYear, true)};

		QueryResults sqlQr3;
		err = rt.reindexer->Select(sqlQuery, sqlQr3);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr3;
		err = rt.reindexer->Select(checkQuery3, checkQr3);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr3, checkQr3);
		Verify(checkQr3, checkQuery3, *rt.reindexer);

		sqlQuery = "SELECT FACET(ID, Year ORDER BY ID DESC ORDER BY Year ASC LIMIT 20 OFFSET 1) FROM test_namespace LIMIT 10000000";
		const Query checkQuery4{
			Query(default_namespace, 0, 10000000)
				.Aggregate(AggFacet, {kFieldNameId, kFieldNameYear}, {{kFieldNameId, true}, {kFieldNameYear, false}}, 20, 1)};

		QueryResults sqlQr4;
		err = rt.reindexer->Select(sqlQuery, sqlQr4);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr4;
		err = rt.reindexer->Select(checkQuery4, checkQr4);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr4, checkQr4);
		Verify(checkQr4, checkQuery4, *rt.reindexer);

		sqlQuery = "SELECT ID FROM test_namespace WHERE name LIKE '" + likePattern +
				   "' AND (genre IN ('1', '2', '3') AND year > '2016' ) OR age IN ('1', '2', '3', '4') LIMIT 10000000";
		const Query checkQuery5{Query(default_namespace, 0, 10000000)
									.Where(kFieldNameName, CondLike, likePattern)
									.OpenBracket()
									.Where(kFieldNameGenre, CondSet, {1, 2, 3})
									.Where(kFieldNameYear, CondGt, 2016)
									.CloseBracket()
									.Or()
									.Where(kFieldNameAge, CondSet, {1, 2, 3, 4})};

		QueryResults sqlQr5;
		err = rt.reindexer->Select(sqlQuery, sqlQr5);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr5;
		err = rt.reindexer->Select(checkQuery5, checkQr5);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr5, checkQr5);
		Verify(checkQr5, checkQuery5, *rt.reindexer);

		sqlQuery = fmt::sprintf("SELECT ID FROM test_namespace ORDER BY '%s + %s * 5' DESC LIMIT 10000000", kFieldNameYear, kFieldNameId);
		const Query checkQuery6{
			Query(default_namespace, 0, 10000000).Sort(kFieldNameYear + std::string(" + ") + kFieldNameId + " * 5", true)};

		QueryResults sqlQr6;
		err = rt.reindexer->Select(sqlQuery, sqlQr6);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr6;
		err = rt.reindexer->Select(checkQuery6, checkQr6);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr6, checkQr6);
		Verify(checkQr6, checkQuery6, *rt.reindexer);

		sqlQuery = fmt::sprintf("SELECT ID FROM test_namespace ORDER BY '%s + %s * 5' DESC ORDER BY '2 * %s / (1 + %s)' ASC LIMIT 10000000",
								kFieldNameYear, kFieldNameId, kFieldNameGenre, kFieldNameIsDeleted);
		const Query checkQuery7{Query(default_namespace, 0, 10000000)
									.Sort(kFieldNameYear + std::string(" + ") + kFieldNameId + " * 5", true)
									.Sort(std::string("2 * ") + kFieldNameGenre + " / (1 + " + kFieldNameIsDeleted + ')', false)};

		QueryResults sqlQr7;
		err = rt.reindexer->Select(sqlQuery, sqlQr7);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr7;
		err = rt.reindexer->Select(checkQuery7, checkQr7);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr7, checkQr7);
		Verify(checkQr7, checkQuery7, *rt.reindexer);

		// Checks that SQL queries with DWithin and sort by Distance work and compares the result with the result of corresponding C++ query
		reindexer::Point point = randPoint(10);
		double distance = randBinDouble(0, 1);
		sqlQuery = fmt::sprintf("SELECT * FROM %s WHERE ST_DWithin(%s, %s, %s);", geomNs, kFieldNamePointNonIndex, pointToSQL(point),
								toString(distance));
		const Query checkQuery8{Query(geomNs).DWithin(kFieldNamePointNonIndex, point, distance)};

		QueryResults sqlQr8;
		err = rt.reindexer->Select(sqlQuery, sqlQr8);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr8;
		err = rt.reindexer->Select(checkQuery8, checkQr8);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr8, checkQr8);
		Verify(checkQr8, checkQuery8, *rt.reindexer);

		point = randPoint(10);
		distance = randBinDouble(0, 1);
		sqlQuery = fmt::sprintf("SELECT * FROM %s WHERE ST_DWithin(%s, %s, %s) ORDER BY 'ST_Distance(%s, %s)';", geomNs, pointToSQL(point),
								kFieldNamePointNonIndex, toString(distance), kFieldNamePointLinearRTree, pointToSQL(point, true));
		const Query checkQuery9{
			Query(geomNs)
				.DWithin(kFieldNamePointNonIndex, point, distance)
				.Sort(std::string("ST_Distance(") + kFieldNamePointLinearRTree + ", " + pointToSQL(point) + ')', false)};

		QueryResults sqlQr9;
		err = rt.reindexer->Select(sqlQuery, sqlQr9);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr9;
		err = rt.reindexer->Select(checkQuery9, checkQr9);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr9, checkQr9);
		Verify(checkQr9, checkQuery9, *rt.reindexer);

		sqlQuery = fmt::sprintf("SELECT * FROM %s WHERE %s >= %s;", default_namespace, kFieldNameGenre, kFieldNameRate);
		const Query checkQuery10{Query(default_namespace).WhereBetweenFields(kFieldNameGenre, CondGe, kFieldNameRate)};

		QueryResults sqlQr10;
		err = rt.reindexer->Select(sqlQuery, sqlQr10);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults checkQr10;
		err = rt.reindexer->Select(checkQuery10, checkQr10);
		ASSERT_TRUE(err.ok()) << err.what();

		CompareQueryResults(sqlQuery, sqlQr10, checkQr10);
		Verify(checkQr10, checkQuery10, *rt.reindexer);
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
		std::vector<VariantArray> stringKeys;
		for (size_t i = 0; i < 1010; ++i) {
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
	const char* kFieldNameUuidArr = "uuid_arr";
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
	const std::string forcedSortOffsetNs = "forced_sort_offset_namespace";
	const std::string nsWithObject = "namespace_with_object";
	const std::string geomNs = "geom_namespace";
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
	static constexpr int btreeIdxOptNsSize = 10000;
	size_t conditionsNsSize = 0;
	std::vector<std::pair<int, int>> forcedSortOffsetValues;
};
