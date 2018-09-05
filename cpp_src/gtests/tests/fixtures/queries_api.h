#pragma once

#include <cmath>
#include <limits>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include "reindexer_api.h"

using std::unordered_map;
using std::unordered_set;
using std::map;
using std::numeric_limits;
using reindexer::KeyArray;

class QueriesApi : public ReindexerApi {
public:
	void SetUp() override {
		indexesOptions = {
			{kFieldNameId, IndexOpts().PK()},
			{kFieldNameGenre, IndexOpts()},
			{kFieldNameYear, IndexOpts()},
			{kFieldNamePackages, IndexOpts().Array()},
			{kFieldNameName, IndexOpts()},
			{kFieldNameCountries, IndexOpts().Array()},
			{kFieldNameAge, IndexOpts()},
			{kFieldNameDescription, IndexOpts()},
			{kFieldNameRate, IndexOpts()},
			{kFieldNameIsDeleted, IndexOpts()},
			{kFieldNameActor, IndexOpts().SetCollateMode(CollateUTF8)},
			{kFieldNamePriceId, IndexOpts().Array()},
			{kFieldNameLocation, IndexOpts().SetCollateMode(CollateNone)},
			{kFieldNameEndTime, IndexOpts()},
			{kFieldNameStartTime, IndexOpts()},
			{kFieldNamePhone, IndexOpts()},
			{kFieldNameTemp, IndexOpts().PK().SetCollateMode(CollateASCII)},
			{kFieldNameNumeric, IndexOpts().SetCollateMode(CollateUTF8)},
			{string(kFieldNameId + compositePlus + kFieldNameTemp), IndexOpts()},
			{string(kFieldNameAge + compositePlus + kFieldNameGenre), IndexOpts()},
		};

		Error err = reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(default_namespace,
							   {
								   IndexDeclaration{kFieldNameId, "hash", "int", indexesOptions[kFieldNameId]},
								   IndexDeclaration{kFieldNameGenre, "tree", "int", indexesOptions[kFieldNameGenre]},
								   IndexDeclaration{kFieldNameYear, "tree", "int", indexesOptions[kFieldNameYear]},
								   IndexDeclaration{kFieldNamePackages, "hash", "int", indexesOptions[kFieldNamePackages]},
								   IndexDeclaration{kFieldNameName, "tree", "string", indexesOptions[kFieldNameName]},
								   IndexDeclaration{kFieldNameCountries, "tree", "string", indexesOptions[kFieldNameCountries]},
								   IndexDeclaration{kFieldNameAge, "hash", "int", indexesOptions[kFieldNameAge]},
								   IndexDeclaration{kFieldNameDescription, "fuzzytext", "string", indexesOptions[kFieldNameDescription]},
								   IndexDeclaration{kFieldNameRate, "tree", "double", indexesOptions[kFieldNameRate]},
								   IndexDeclaration{kFieldNameIsDeleted, "-", "bool", indexesOptions[kFieldNameIsDeleted]},
								   IndexDeclaration{kFieldNameActor, "tree", "string", indexesOptions[kFieldNameActor]},
								   IndexDeclaration{kFieldNamePriceId, "hash", "int", indexesOptions[kFieldNamePriceId]},
								   IndexDeclaration{kFieldNameLocation, "tree", "string", indexesOptions[kFieldNameLocation]},
								   IndexDeclaration{kFieldNameEndTime, "hash", "int", indexesOptions[kFieldNameEndTime]},
								   IndexDeclaration{kFieldNameStartTime, "tree", "int", indexesOptions[kFieldNameStartTime]},
								   IndexDeclaration{kFieldNameTemp, "tree", "string", indexesOptions[kFieldNameTemp]},
								   IndexDeclaration{kFieldNameNumeric, "tree", "string", indexesOptions[kFieldNameNumeric]},
								   IndexDeclaration{string(kFieldNameId + compositePlus + kFieldNameTemp).c_str(), "tree", "composite",
													indexesOptions[kFieldNameId + compositePlus + kFieldNameTemp]},
								   IndexDeclaration{string(kFieldNameAge + compositePlus + kFieldNameGenre).c_str(), "hash", "composite",
													indexesOptions[kFieldNameAge + compositePlus + kFieldNameGenre]},
							   });
		defaultNsPks.push_back(kFieldNameId);
		defaultNsPks.push_back(kFieldNameTemp);

		err = reindexer->OpenNamespace(testSimpleNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(testSimpleNs, {
												 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK()},
												 IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts()},
												 IndexDeclaration{kFieldNameName, "text", "string", IndexOpts()},
												 IndexDeclaration{kFieldNamePhone, "text", "string", IndexOpts()},
											 });
		simpleTestNsPks.push_back(kFieldNameId);

		err = reindexer->OpenNamespace(compositeIndexesNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(compositeIndexesNs, {IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts().PK()},
													IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts().PK()},
													IndexDeclaration{kFieldNameTitle, "text", "string", IndexOpts()},
													IndexDeclaration{kFieldNamePages, "hash", "int", IndexOpts()},
													IndexDeclaration{kFieldNamePrice, "hash", "int", IndexOpts()},
													IndexDeclaration{kFieldNameName, "text", "string", IndexOpts()},
													IndexDeclaration{kCompositeFieldPricePages.c_str(), "hash", "composite", IndexOpts()},
													IndexDeclaration{kCompositeFieldTitleName.c_str(), "tree", "composite", IndexOpts()}});

		compositeIndexesNsPks.push_back(kFieldNameBookid);
		compositeIndexesNsPks.push_back(kFieldNameBookid2);

		err = reindexer->OpenNamespace(comparatorsNs);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			comparatorsNs,
			{IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts()}, IndexDeclaration{kFieldNameColumnInt, "hash", "int", IndexOpts()},
			 IndexDeclaration{kFieldNameColumnInt64, "hash", "int64", IndexOpts().PK()},
			 IndexDeclaration{kFieldNameColumnDouble, "tree", "double", IndexOpts()},
			 IndexDeclaration{kFieldNameColumnString, "-", "string", IndexOpts()},
			 IndexDeclaration{kFieldNameColumnFullText, "text", "string", IndexOpts()},
			 IndexDeclaration{kFieldNameColumnStringNumeric, "-", "string", IndexOpts().SetCollateMode(CollateNumeric)}});
		comparatorsNsPks.push_back(kFieldNameColumnInt64);
	}

	void ExecuteAndVerify(const string& ns, const Query& query) {
		reindexer::QueryResults qr;
		Error err = reindexer->Select(query, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		if (err.ok()) {
			Verify(ns, qr, query);
		}
	}

	void Verify(const string& ns, const QueryResults& qr, const Query& query) {
		unordered_set<string> pks;
		unordered_map<string, unordered_set<string>> distincts;

		KeyRefs lastSortedColumnValues;
		lastSortedColumnValues.resize(query.sortingEntries_.size());

		size_t itemsCount = 0;
		for (size_t i = 0; i < qr.Count(); ++i) {
			Item itemr(qr[i].GetItem());

			auto pk = getPkString(itemr, ns);
			EXPECT_TRUE(pks.insert(pk).second) << "Duplicated primary key: " + pk;

			InsertedItemsByPk& insertedItemsByPk = insertedItems[ns];
			auto itInsertedItem = insertedItemsByPk.find(pk);
			EXPECT_TRUE(itInsertedItem != insertedItemsByPk.end()) << "Item with such PK has not been inserted yet: " + pk;
			if (itInsertedItem != insertedItemsByPk.end()) {
				Item& insertedItem = itInsertedItem->second;
				bool eq = (insertedItem.GetJSON().ToString() == itemr.GetJSON().ToString());
				EXPECT_TRUE(eq) << "Items' jsons are different!";
			}

			reindexer::QueryEntries failedEntries;
			bool conditionsSatisfied = checkConditions(itemr, query, failedEntries);
			if (conditionsSatisfied) ++itemsCount;
			EXPECT_TRUE(conditionsSatisfied) << "Item doesn't match conditions: " + itemr.GetJSON().ToString();
			if (!conditionsSatisfied) {
				printf("Query: %s\n", query.Dump().c_str());
				PrintFailedQueryEntries(failedEntries);
			}
			EXPECT_TRUE(checkDistincts(itemr, query, distincts)) << "Distinction check failed";

			std::vector<int> cmpRes(query.sortingEntries_.size());
			std::fill(cmpRes.begin(), cmpRes.end(), -1);

			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				if (!query.forcedSortOrder.empty()) break;
				const reindexer::SortingEntry& sortingEntry(query.sortingEntries_[j]);
				KeyRef sortedValue = itemr[sortingEntry.column];
				if (lastSortedColumnValues[j].Type() != KeyValueEmpty) {
					bool needToVerify = true;
					if (j != 0) {
						for (int k = j - 1; k >= 0; --k)
							if (cmpRes[k] != 0) {
								needToVerify = false;
								break;
							}
					}
					needToVerify = (j == 0) || needToVerify;
					if (needToVerify) {
						cmpRes[j] = lastSortedColumnValues[j].Compare(sortedValue);
						bool sortOrderSatisfied =
							(sortingEntry.desc && cmpRes[j] >= 0) || (!sortingEntry.desc && cmpRes[j] <= 0) || (cmpRes[j] == 0);
						EXPECT_TRUE(sortOrderSatisfied) << "\nSort order is incorrect for column: " << sortingEntry.column;
						if (!sortOrderSatisfied) {
							printf("Query: %s\n", query.Dump().c_str());
							PrintFailedSortOrder(query, qr, i);
						}
					}
				}
				lastSortedColumnValues[j] = sortedValue;
			}
		}

		if (!query.forcedSortOrder.empty()) {
			EXPECT_TRUE(query.forcedSortOrder.size() <= qr.Count()) << "Size of QueryResults is incorrect!";
			if (query.forcedSortOrder.size() <= qr.Count()) {
				for (size_t i = 0; i < qr.Count(); ++i) {
					Item item(qr[i].GetItem());
					KeyRef sortedValue = item[query.sortingEntries_[0].column];
					EXPECT_EQ(query.forcedSortOrder[i].Compare(sortedValue), 0) << "Forced sort order is incorrect!";
				}
			}
		}
	}

protected:
	const std::vector<string>& getNsPks(const string& ns) {
		if (ns == default_namespace) return defaultNsPks;
		if (ns == testSimpleNs) return simpleTestNsPks;
		if (ns == compositeIndexesNs) return compositeIndexesNsPks;
		if (ns == comparatorsNs) return comparatorsNsPks;
		std::abort();
	}

	string getPkString(reindexer::Item& item, const string& ns) {
		string ret;
		const vector<string>& pkFields(getNsPks(ns));
		for (const string& field : pkFields) {
			ret += item[field].As<string>() + "#";
		}
		return ret;
	}

	bool checkConditions(reindexer::Item& item, const Query& qr, reindexer::QueryEntries& failedEntries) {
		bool result = true;
		for (const QueryEntry& qentry : qr.entries) {
			if (qentry.distinct) continue;
			bool iterationResult = checkCondition(item, qentry);
			switch (qentry.op) {
				case OpNot:
					if (iterationResult) {
						failedEntries.push_back(qentry);
						return false;
					}
					break;
				case OpAnd:
					if (!result) {
						failedEntries.push_back(qentry);
						return false;
					}
					result = iterationResult;
					break;
				case OpOr:
					result = iterationResult || result;
					break;
			}
		}
		if (!result) {
			failedEntries.push_back(qr.entries.back());
		}
		return result;
	}

	bool isIndexComposite(Item& item, const QueryEntry& qentry) {
		if (qentry.idxNo >= item.NumFields()) return true;
		return (qentry.values[0].Type() == KeyValueComposite);
	}

	bool compareValues(CondType condition, const KeyRef& key, const KeyValues& values, const CollateOpts& opts) {
		bool result = false;
		switch (condition) {
			case CondEq:
				result = (key.Compare(values[0], opts) == 0);
				break;
			case CondGe:
				result = (key.Compare(values[0], opts) >= 0);
				break;
			case CondGt:
				result = (key.Compare(values[0], opts) > 0);
				break;
			case CondLt:
				result = (key.Compare(values[0], opts) < 0);
				break;
			case CondLe:
				result = (key.Compare(values[0], opts) <= 0);
				break;
			case CondRange:
				result = (key.Compare(values[0], opts) >= 0) && (key.Compare(values[1], opts) <= 0);
				break;
			case CondSet:
				for (const KeyValue& kv : values) {
					result = (key.Compare(kv, opts) == 0);
					if (result) break;
				}
				break;
			default:
				std::abort();
		}
		return result;
	}

	KeyValues getValues(Item& item, const std::vector<string>& indexes) {
		KeyValues kvalues;
		for (const string& idxName : indexes) {
			kvalues.push_back(KeyValue(static_cast<KeyRef>(item[idxName])));
		}
		return kvalues;
	}

	int compareCompositeValues(const KeyValues& indexesValues, const KeyValue& keyValue, const CollateOpts& opts) {
		const std::vector<KeyValue>& compositeValues = keyValue.getCompositeValues();
		EXPECT_TRUE(indexesValues.size() == compositeValues.size());

		int cmpRes = 0;
		for (size_t i = 0; i < indexesValues.size() && (cmpRes == 0); ++i) {
			cmpRes = indexesValues[i].Compare(compositeValues[i], opts);
		}

		return cmpRes;
	}

	bool checkCompositeValues(Item& item, const QueryEntry& qentry, const CollateOpts& opts) {
		vector<string> subIndexes;
		reindexer::split(qentry.index, "+", true, subIndexes);

		KeyValues indexesValues = getValues(item, subIndexes);
		const KeyValues& keyValues = qentry.values;

		switch (qentry.condition) {
			case CondEmpty:
				return indexesValues.size() == 0;
			case CondAny:
				return indexesValues.size() > 0;
			default:
				break;
		}

		bool result = false;
		switch (qentry.condition) {
			case CondEq:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) == 0);
				break;
			case CondGe:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) >= 0);
				break;
			case CondGt:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) > 0);
				break;
			case CondLt:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) < 0);
				break;
			case CondLe:
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) <= 0);
				break;
			case CondRange:
				EXPECT_TRUE(keyValues.size() == 2);
				result = (compareCompositeValues(indexesValues, keyValues[0], opts) >= 0) &&
						 (compareCompositeValues(indexesValues, keyValues[1], opts) <= 0);
				break;
			case CondSet:
				for (const KeyValue& kv : keyValues) {
					result = (compareCompositeValues(indexesValues, kv, opts) == 0);
					if (result) break;
				}
				break;
			default:
				std::abort();
		}

		return result;
	}

	bool checkCondition(Item& item, const QueryEntry& qentry) {
		EXPECT_TRUE(item.NumFields() > 0);
		EXPECT_TRUE(qentry.values.size() > 0);

		bool result = false;
		IndexOpts& opts = indexesOptions[qentry.index];

		if (isIndexComposite(item, qentry)) {
			return checkCompositeValues(item, qentry, opts.collateOpts_);
		} else {
			KeyRefs fieldValues = item[qentry.index];
			switch (qentry.condition) {
				case CondEmpty:
					return fieldValues.size() == 0;
				case CondAny:
					return fieldValues.size() > 0;
				default:
					break;
			}
			for (const KeyRef& fieldValue : fieldValues) {
				result = compareValues(qentry.condition, fieldValue, qentry.values, opts.collateOpts_);
				if (result) break;
			}
		}

		return result;
	}

	bool checkDistincts(reindexer::Item& item, const Query& qr, unordered_map<string, unordered_set<string>>& distincts) {
		bool result = true;
		for (const QueryEntry& qentry : qr.entries) {
			if (!qentry.distinct) continue;

			reindexer::KeyRefs fieldValue = item[qentry.index];

			EXPECT_TRUE(fieldValue.size() == 1) << "Distinct field's size cannot be > 1";

			unordered_set<string>& values = distincts[qentry.index];
			KeyValue keyValue(fieldValue[0]);
			bool inserted = values.insert(keyValue.As<string>()).second;
			EXPECT_TRUE(inserted) << "Duplicate distinct item for index: " << keyValue.As<string>() << ", " << std::to_string(qentry.idxNo);
			result &= inserted;
		}
		return result;
	}

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
			Commit(compositeIndexesNs);

			string pkString = getPkString(item, compositeIndexesNs);
			insertedItems[compositeIndexesNs].emplace(pkString, std::move(item));
		}

		Item lastItem = NewItem(compositeIndexesNs);
		lastItem[this->kFieldNameBookid] = 300;
		lastItem[this->kFieldNameBookid2] = 3000;
		lastItem[this->kFieldNameTitle] = "test book1 title";
		lastItem[this->kFieldNamePages] = 88888;
		lastItem[this->kFieldNamePrice] = 77777;
		lastItem[this->kFieldNameName] = "test book1 name";
		Upsert(compositeIndexesNs, lastItem);
		Commit(compositeIndexesNs);

		string pkString = getPkString(lastItem, compositeIndexesNs);
		insertedItems[compositeIndexesNs].emplace(pkString, std::move(lastItem));
	}

	void FillTestSimpleNamespace() {
		Item item1 = NewItem(testSimpleNs);
		item1[kFieldNameId] = 1;
		item1[kFieldNameYear] = 2002;
		item1[kFieldNameName] = "SSS";
		Upsert(testSimpleNs, item1);

		string pkString = getPkString(item1, testSimpleNs);
		insertedItems[testSimpleNs].emplace(pkString, std::move(item1));

		Item item2 = NewItem(testSimpleNs);
		item2[kFieldNameId] = 2;
		item2[kFieldNameYear] = 1989;
		item2[kFieldNameName] = "MMM";
		Upsert(testSimpleNs, item2);

		pkString = getPkString(item2, testSimpleNs);
		insertedItems[testSimpleNs].emplace(pkString, std::move(item2));

		Commit(testSimpleNs);
	}

	void FillComparatorsNamespace() {
		for (size_t i = 0; i < 1000; ++i) {
			Item item(reindexer->NewItem(comparatorsNs));
			item[kFieldNameId] = static_cast<int>(i);
			item[kFieldNameColumnInt] = rand();
			item[kFieldNameColumnInt64] = static_cast<int64_t>(rand());
			item[kFieldNameColumnDouble] = static_cast<double>(rand()) / RAND_MAX;
			item[kFieldNameColumnString] = RandString();
			item[kFieldNameColumnStringNumeric] = std::to_string(i);
			item[kFieldNameColumnFullText] = RandString();

			Upsert(comparatorsNs, item);

			string pkString = getPkString(item, comparatorsNs);
			insertedItems[comparatorsNs].emplace(pkString, std::move(item));
		}

		Commit(comparatorsNs);
	}

	void FillDefaultNamespace(int start, int count, int packagesCount) {
		for (int i = 0; i < count; ++i) {
			Item item(GenerateDefaultNsItem(start + i, static_cast<size_t>(packagesCount)));
			Upsert(default_namespace, item);

			string pkString = getPkString(item, default_namespace);
			insertedItems[default_namespace].emplace(pkString, std::move(item));
		}
		Commit(default_namespace);
	}

	Item GenerateDefaultNsItem(int idValue, size_t packagesCount) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = idValue;
		item[kFieldNameYear] = rand() % 50 + 2000;
		item[kFieldNameGenre] = rand() % 50;
		item[kFieldNameName] = RandString().c_str();
		item[kFieldNameAge] = rand() % 50;
		item[kFieldNameDescription] = RandString().c_str();

		auto packagesVec(RandIntVector(packagesCount, 10000, 50));
		item[kFieldNamePackages] = packagesVec;

		item[kFieldNameRate] = static_cast<double>(rand() % 100) / 10;
		item[kFieldNameAge] = static_cast<int>(rand() % 2);

		auto pricesIds(RandIntVector(10, 7000, 50));
		item[kFieldNamePriceId] = pricesIds;

		int stTime = rand() % 50000;
		item[kFieldNameLocation] = RandString().c_str();
		item[kFieldNameStartTime] = stTime;
		item[kFieldNameEndTime] = stTime + (rand() % 5) * 1000;
		item[kFieldNameActor] = RandString().c_str();
		item[kFieldNameNumeric] = to_string(rand() % 1000);

		return item;
	}

	void CheckStandartQueries() {
		const char* sortIdxs[] = {kFieldNameName, kFieldNameYear, kFieldNameRate};
		const vector<string> distincts = {kFieldNameYear, kFieldNameRate};
		const vector<bool> sortOrders = {true, false};

		const string compositeIndexName(kFieldNameAge + compositePlus + kFieldNameGenre);

		for (const bool sortOrder : sortOrders) {
			for (const char* sortIdx : sortIdxs) {
				for (const string& distinct : distincts) {
					int randomGenre = rand() % 50;
					int randomGenreUpper = rand() % 100;
					int randomGenreLower = rand() % 100;

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder).Limit(1));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondEq, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondEq, RandString())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameRate, CondEq, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondGt, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondGt, RandString())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameRate, CondGt, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondLt, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondLt, RandString())
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameRate, CondLt, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameGenre, CondRange, {randomGenreLower, randomGenreUpper})
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace)
										 .Where(kFieldNameRate, CondRange,
												{static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
										 .Distinct(distinct.c_str())
										 .Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNamePackages, CondSet, RandIntVector(10, 10000, 50))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNamePackages, CondEmpty, 0)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(kFieldNameYear, true)
															.Sort(kFieldNameName, false)
															.Sort(kFieldNameLocation, true));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(kFieldNameName, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(kFieldNameGenre, true)
															.Sort(kFieldNameActor, false)
															.Sort(kFieldNameRate, true)
															.Sort(kFieldNameLocation, false));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(kFieldNamePackages, CondAny, 0).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(kFieldNameIsDeleted, CondEq, 1).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Where(kFieldNameAge, CondEq, 3)
															.Where(kFieldNameYear, CondGe, 2010)
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
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

					ExecuteAndVerify(default_namespace, Query(default_namespace)
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

					ExecuteAndVerify(default_namespace, Query(default_namespace)
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

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondSet, {5, 1, 7})
															.Where(kFieldNameYear, CondLt, 2010)
															.Or()
															.Where(kFieldNamePackages, CondAny, 0)
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Or()
															.Where(kFieldNameGenre, CondEq, 6)
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameActor, CondEq, RandString()));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Not()
															.Where(kFieldNameGenre, CondEq, 5)
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameGenre, CondEq, 5)
															.Not()
															.Where(kFieldNameYear, CondRange, {2001, 2020})
															.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Not()
															.Where(kFieldNameYear, CondEq, 10));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(kFieldNameNumeric, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameNumeric, CondGt, to_string(5)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(kFieldNameNumeric, sortOrder)
															.Debug(LogTrace)
															.Where(kFieldNameNumeric, CondLt, to_string(600)));

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace)
										 .Distinct(distinct.c_str())
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(kFieldNameNumeric, CondRange, {to_string(rand() % 100), to_string(rand() % 100 + 500)}));

					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS"));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameName, CondEq, "SSS").Not().Where(kFieldNameYear, CondEq, 1989));
					ExecuteAndVerify(testSimpleNs,
									 Query(testSimpleNs).Where(kFieldNameYear, CondEq, 2002).Not().Where(kFieldNameName, CondEq, "MMM"));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondLe, {{KeyValue(27), KeyValue(10000)}}));
				}
			}
		}
	}

	void CheckAggregationQueries() {
		const int limit = 100;
		Query testQuery = Query(default_namespace)
							  .Where(kFieldNameGenre, CondEq, 10)
							  .Limit(limit)
							  .Aggregate(kFieldNameYear, AggAvg)
							  .Aggregate(kFieldNameYear, AggSum);
		Query checkQuery = Query(default_namespace).Where(kFieldNameGenre, CondEq, 10).Limit(limit);

		reindexer::QueryResults testQr;
		Error err = reindexer->Select(testQuery, testQr);
		EXPECT_TRUE(err.ok()) << err.what();

		reindexer::QueryResults checkQr;
		err = reindexer->Select(checkQuery, checkQr);
		EXPECT_TRUE(err.ok()) << err.what();

		double yearSum = 0.0;
		for (auto it : checkQr) {
			Item item(it.GetItem());
			yearSum += item[kFieldNameYear].Get<int>();
		}

		EXPECT_DOUBLE_EQ(testQr.aggregationResults[1], yearSum) << "Aggregation Sum result is incorrect!";
		EXPECT_DOUBLE_EQ(testQr.aggregationResults[0], yearSum / checkQr.Count()) << "Aggregation Sum result is incorrect!";
	}

	void CheckSqlQueries() {
		const string sqlQuery =
			"SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' AND genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery = Query(default_namespace, 0, 10000000)
									 .Where(kFieldNameYear, CondGt, 2016)
									 .Where(kFieldNameGenre, CondSet, {1, 2, 3})
									 .Sort(kFieldNameYear, true);

		QueryResults sqlQr;
		Error err = reindexer->Select(sqlQuery, sqlQr);
		EXPECT_TRUE(err.ok()) << err.what();

		QueryResults checkQr;
		err = reindexer->Select(checkQuery, checkQr);
		EXPECT_TRUE(err.ok()) << err.what();

		EXPECT_EQ(sqlQr.Count(), checkQr.Count());
		if (sqlQr.Count() == checkQr.Count()) {
			for (size_t i = 0; i < checkQr.Count(); ++i) {
				Item ritem1(checkQr[i].GetItem());
				Item ritem2(sqlQr[i].GetItem());
				EXPECT_EQ(ritem1.NumFields(), ritem2.NumFields());
				if (ritem1.NumFields() == ritem2.NumFields()) {
					for (int idx = 1; idx < ritem1.NumFields(); ++idx) {
						KeyRefs lhs = ritem1[idx];
						KeyRefs rhs = ritem2[idx];

						EXPECT_EQ(lhs.size(), rhs.size());
						if (lhs.size() == rhs.size()) {
							for (size_t j = 0; j < lhs.size(); ++j) {
								EXPECT_EQ(lhs[j].Compare(rhs[j]), 0);
							}
						}
					}
				}
			}
		}

		Verify(default_namespace, checkQr, checkQuery);
	}

	void CheckCompositeIndexesQueries() {
		int priceValue = 77777;
		int pagesValue = 88888;
		const char* titleValue = "test book1 title";
		const char* nameValue = "test book1 name";

		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondEq, {{KeyValue(priceValue), KeyValue(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondLt, {{KeyValue(priceValue), KeyValue(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondLe, {{KeyValue(priceValue), KeyValue(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondGt, {{KeyValue(priceValue), KeyValue(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondGe, {{KeyValue(priceValue), KeyValue(pagesValue)}}));
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs)
							 .WhereComposite(kCompositeFieldPricePages.c_str(), CondRange,
											 {{KeyValue(1), KeyValue(1)}, {KeyValue(priceValue), KeyValue(pagesValue)}}));

		vector<KeyValues> intKeys;
		for (int i = 0; i < 10; ++i) {
			intKeys.emplace_back(KeyValues{KeyValue(i), KeyValue(i * 5)});
		}
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs).WhereComposite(kCompositeFieldPricePages.c_str(), CondSet, intKeys));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
																 {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}));
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondGe,
																 {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondLt,
																 {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}));
		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondLe,
																 {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}));
		vector<KeyValues> stringKeys;
		for (size_t i = 0; i < 1010; ++i) {
			stringKeys.emplace_back(KeyValues{KeyValue(RandString()), KeyValue(RandString())});
		}
		ExecuteAndVerify(compositeIndexesNs,
						 Query(compositeIndexesNs).WhereComposite(kCompositeFieldTitleName.c_str(), CondSet, stringKeys));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs)
												 .Where(kFieldNameName, CondEq, nameValue)
												 .WhereComposite(kCompositeFieldTitleName.c_str(), CondEq,
																 {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}));

		ExecuteAndVerify(compositeIndexesNs, Query(compositeIndexesNs));
	}

	void CheckComparatorsQueries() {
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnInt64", CondLe, {KeyRef(static_cast<int64_t>(10000))}));

		vector<double> doubleSet;
		for (size_t i = 0; i < 1010; i++) {
			doubleSet.emplace_back(static_cast<double>(rand()) / RAND_MAX);
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnDouble", CondSet, doubleSet));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondGe, string("test_string1")));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondLe, string("test_string2")));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondEq, string("test_string3")));

		vector<string> stringSet;
		for (size_t i = 0; i < 1010; i++) {
			stringSet.emplace_back(RandString());
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnString", CondSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 100; i++) {
			stringSet.emplace_back(to_string(i + 20000));
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnStringNumeric", CondSet, stringSet));

		stringSet.clear();
		for (size_t i = 0; i < 100; i++) {
			stringSet.emplace_back(to_string(i + 1));
		}
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnStringNumeric", CondSet, stringSet));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnStringNumeric", CondEq, string("777")));
		ExecuteAndVerify(comparatorsNs, Query(comparatorsNs).Where("columnFullText", CondEq, RandString()));
	}

	void PrintFailedQueryEntries(const reindexer::QueryEntries& failedEntries) {
		printf("Failed entries: ");
		for (size_t i = 0; i < failedEntries.size(); ++i) {
			printf("%s", failedEntries[i].Dump().c_str());
			if (i != failedEntries.size() - 1) {
				printf(": ");
			}
		}
		printf("\n\n");
		fflush(stdout);
	}

	static void boldOn() { printf("\e[1m"); }
	static void boldOff() { printf("\e[0m"); }

	void PrintFailedSortOrder(const Query& query, const QueryResults& qr, int itemIndex, int itemsToShow = 10) {
		if (qr.Count() == 0) return;

		printf("Sort order or last items: \n");
		Item rdummy(qr[0].GetItem());
		boldOn();
		for (size_t idx = 0; idx < query.sortingEntries_.size(); idx++) {
			printf("\t%.60s", rdummy[query.sortingEntries_[idx].column].Name().c_str());
		}
		boldOff();
		printf("\n\n");

		int firstItem = itemIndex - itemsToShow;
		if (firstItem < 0) firstItem = 0;
		for (int i = firstItem; i <= itemIndex; ++i) {
			Item item(qr[i].GetItem());
			if (i == itemIndex) boldOn();
			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				printf("\t%.60s", item[query.sortingEntries_[j].column].As<string>().c_str());
			}
			if (i == itemIndex) boldOff();
			printf("\n");
		}

		firstItem = itemIndex + 1;
		int lastItem = firstItem + itemsToShow;
		const int count = static_cast<int>(qr.Count());
		if (firstItem >= count) firstItem = count - 1;
		if (lastItem > count) lastItem = count;
		for (int i = firstItem; i < lastItem; ++i) {
			Item item(qr[i].GetItem());
			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				printf("\t%.60s", item[query.sortingEntries_[j].column].As<string>().c_str());
			}
			printf("\n");
		}

		printf("\n\n");
		fflush(stdout);
	}

	using NamespaceName = string;
	using InsertedItemsByPk = std::map<string, reindexer::Item>;
	std::unordered_map<NamespaceName, InsertedItemsByPk> insertedItems;
	std::unordered_map<string, IndexOpts> indexesOptions;

	const char* kFieldNameId = "id";
	const char* kFieldNameGenre = "genre";
	const char* kFieldNameYear = "year";
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
	const char* kFieldNameTemp = "tmp";
	const char* kFieldNameNumeric = "numeric";
	const char* kFieldNameBookid = "bookid";
	const char* kFieldNameBookid2 = "bookid2";
	const char* kFieldNameTitle = "title";
	const char* kFieldNamePages = "pages";
	const char* kFieldNamePrice = "price";

	const char* kFieldNameColumnInt = "columnInt";
	const char* kFieldNameColumnInt64 = "columnInt64";
	const char* kFieldNameColumnDouble = "columnDouble";
	const char* kFieldNameColumnString = "columnString";
	const char* kFieldNameColumnFullText = "columnFullText";
	const char* kFieldNameColumnStringNumeric = "columnStringNumeric";

	const string compositePlus = "+";
	const string testSimpleNs = "test_simple_namespace";
	const string compositeIndexesNs = "composite_indexes_namespace";
	const string comparatorsNs = "comparators_namespace";

	const string kCompositeFieldPricePages = kFieldNamePrice + compositePlus + kFieldNamePages;
	const string kCompositeFieldTitleName = kFieldNameTitle + compositePlus + kFieldNameName;

	vector<string> defaultNsPks;
	vector<string> simpleTestNsPks;
	vector<string> compositeIndexesNsPks;
	vector<string> comparatorsNsPks;
};
