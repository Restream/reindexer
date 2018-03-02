#pragma once

#include <limits>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include "reindexer_api.h"
using std::unordered_map;
using std::unordered_set;
using std::map;
using std::numeric_limits;


class QueriesApi : public ReindexerApi {
public:
	void SetUp() override {
		indexesOptions = {
			{id, IndexOpts().PK()},
			{genre, IndexOpts()},
			{year, IndexOpts()},
			{packages, IndexOpts().Array()},
			{name, IndexOpts()},
			{countries, IndexOpts().Array()},
			{age, IndexOpts()},
			{description, IndexOpts()},
			{rate, IndexOpts()},
			{isDeleted, IndexOpts()},
			{actor, IndexOpts().SetCollateMode(CollateUTF8)},
			{priceId, IndexOpts().Array()},
			{location, IndexOpts().SetCollateMode(CollateNone)},
			{endTime, IndexOpts()},
			{startTime, IndexOpts()},
			{phone, IndexOpts()},
			{temp, IndexOpts().PK().SetCollateMode(CollateASCII)},
			{numeric, IndexOpts().SetCollateMode(CollateUTF8)},
			{string(id + compositePlus + temp), IndexOpts()},
			{string(age + compositePlus + genre), IndexOpts()},
		};

		CreateNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {
													  IndexDeclaration{id, "hash", "int", indexesOptions[id]},
													  IndexDeclaration{genre, "tree", "int", indexesOptions[genre]},
													  IndexDeclaration{year, "tree", "int", indexesOptions[year]},
													  IndexDeclaration{packages, "hash", "int", indexesOptions[packages]},
													  IndexDeclaration{name, "tree", "string", indexesOptions[name]},
													  IndexDeclaration{countries, "tree", "string", indexesOptions[countries]},
													  IndexDeclaration{age, "hash", "int", indexesOptions[age]},
													  IndexDeclaration{description, "fulltext", "string", indexesOptions[description]},
													  IndexDeclaration{rate, "tree", "double", indexesOptions[rate]},
													  IndexDeclaration{isDeleted, "-", "bool", indexesOptions[isDeleted]},
													  IndexDeclaration{actor, "tree", "string", indexesOptions[actor]},
													  IndexDeclaration{priceId, "hash", "int", indexesOptions[priceId]},
													  IndexDeclaration{location, "tree", "string", indexesOptions[location]},
													  IndexDeclaration{endTime, "hash", "int", indexesOptions[endTime]},
													  IndexDeclaration{startTime, "tree", "int", indexesOptions[startTime]},
													  IndexDeclaration{temp, "tree", "string", indexesOptions[temp]},
													  IndexDeclaration{numeric, "tree", "string", indexesOptions[numeric]},
													  IndexDeclaration{string(id + compositePlus + temp).c_str(), "tree", "composite",
																	   indexesOptions[id + compositePlus + temp]},
													  IndexDeclaration{string(age + compositePlus + genre).c_str(), "hash", "composite",
																	   indexesOptions[age + compositePlus + genre]},
												  });
		defaultNsPks.push_back(id);
		defaultNsPks.push_back(temp);

		CreateNamespace(testSimpleNs);
		DefineNamespaceDataset(testSimpleNs, {
												 IndexDeclaration{id, "hash", "int", IndexOpts().PK()},
												 IndexDeclaration{year, "tree", "int", IndexOpts()},
												 IndexDeclaration{name, "text", "string", IndexOpts()},
												 IndexDeclaration{phone, "text", "string", IndexOpts()},
											 });
		simpleTestNsPks.push_back(id);
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
		KeyRef lastSortemColumnValue;

		size_t itemsCount = 0;
		for (size_t i = 0; i < qr.size(); ++i) {
			Item itemr(qr.GetItem(static_cast<int>(i)));

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

			if (!query.sortBy.empty() && query.forcedSortOrder.empty()) {
				KeyRef sortedValue = itemr[query.sortBy];
				if (lastSortemColumnValue.Type() != KeyValueEmpty) {
					int cmpRes = lastSortemColumnValue.Compare(sortedValue);
					bool sortOrderSatisfied = (query.sortDirDesc && cmpRes > 0) || (!query.sortDirDesc && cmpRes < 0) || (cmpRes == 0);
					EXPECT_TRUE(sortOrderSatisfied) << "Sort order is incorrect!";
					if (!sortOrderSatisfied) {
						printf("Query: %s\n", query.Dump().c_str());
						PrintFailedSortOrdered(query, qr, i);
					}
				}
				lastSortemColumnValue = sortedValue;
			}
		}

		if (!query.forcedSortOrder.empty()) {
			EXPECT_TRUE(query.forcedSortOrder.size() <= qr.size()) << "Size of QueryResults is incorrect!";
			if (query.forcedSortOrder.size() <= qr.size()) {
				for (size_t i = 0; i < qr.size(); ++i) {
					Item item(qr.GetItem(static_cast<int>(i)));
					KeyRef sortedValue = item[query.sortBy];
					EXPECT_EQ(query.forcedSortOrder[i].Compare(sortedValue), 0) << "Forced sort order is incorrect!";
				}
			}
		}
	}

protected:
	string getPkString(reindexer::Item& item, const string& ns) {
		vector<string>& pkFields((ns == default_namespace) ? defaultNsPks : simpleTestNsPks);
		string ret;
		for (auto& field : pkFields) {
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

	bool checkCondition(Item& item, const QueryEntry& qentry) {
		KeyRefs fieldValues = item[qentry.index];

		IndexOpts& opts = indexesOptions[qentry.index];

		switch (qentry.condition) {
			case CondEmpty:
				return fieldValues.size() == 0;
			case CondAny:
				return fieldValues.size() > 0;
			default:
				break;
		}

		bool result = false;
		for (const KeyRef& fieldValue : fieldValues) {
			switch (qentry.condition) {
				case CondEq:
					result = (fieldValue.Compare(qentry.values[0], opts.GetCollateMode()) == 0);
					break;
				case CondGe:
					result = (fieldValue.Compare(qentry.values[0], opts.GetCollateMode()) >= 0);
					break;
				case CondGt:
					result = (fieldValue.Compare(qentry.values[0], opts.GetCollateMode()) > 0);
					break;
				case CondLt:
					result = (fieldValue.Compare(qentry.values[0], opts.GetCollateMode()) < 0);
					break;
				case CondLe:
					result = (fieldValue.Compare(qentry.values[0], opts.GetCollateMode()) <= 0);
					break;
				case CondRange:
					result = (fieldValue.Compare(qentry.values[0], opts.GetCollateMode()) >= 0) &&
							 (fieldValue.Compare(qentry.values[1], opts.GetCollateMode()) <= 0);
					break;
				case CondSet:
					for (const KeyValue& kv : qentry.values) {
						result = (fieldValue.Compare(kv, opts.GetCollateMode()) == 0);
						if (result) break;
					}
					break;
				default:
					break;
			}
			if (result) break;
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

	void FillTestSimpleNamespace() {
		Item item1 = NewItem(testSimpleNs);
		item1[id] = 1;
		item1[year] = 2002;
		item1[name] = "SSS";
		Upsert(testSimpleNs, item1);

		string pkString = getPkString(item1, testSimpleNs);
		insertedItems[testSimpleNs].emplace(pkString, std::move(item1));

		Item item2 = NewItem(testSimpleNs);
		item2[id] = 2;
		item2[year] = 1989;
		item2[name] = "MMM";
		Upsert(testSimpleNs, item2);

		pkString = getPkString(item2, testSimpleNs);
		insertedItems[testSimpleNs].emplace(pkString, std::move(item2));

		Commit(testSimpleNs);
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
		item[id] = idValue;
		item[year] = rand() % 50 + 2000;
		item[genre] = rand() % 50;
		item[name] = RandString().c_str();
		item[age] = rand() % 5;
		item[description] = RandString().c_str();

		auto packagesVec(RandIntVector(packagesCount, 10000, 50));
		item[packages] = packagesVec;

		item[rate] = static_cast<double>(rand() % 100) / 10;
		item[age] = static_cast<int>(rand() % 2);

		auto pricesIds(RandIntVector(10, 7000, 50));
		item[priceId] = pricesIds;

		int stTime = rand() % 50000;
		item[location] = RandString().c_str();
		item[startTime] = stTime;
		item[endTime] = stTime + (rand() % 5) * 1000;
		item[actor] = RandString().c_str();
		item[numeric] = to_string(rand() % 1000);

		return item;
	}

	void CheckStandartQueries() {
		const char* sortIdxs[] = {name, year, rate};
		const vector<string> distincts = {year, rate};
		const vector<bool> sortOrders = {true, false};

		for (const bool sortOrder : sortOrders) {
			for (const char* sortIdx : sortIdxs) {
				for (const string& distinct : distincts) {
					int randomGenre = rand() % 50;
					int randomGenreUpper = rand() % 100;
					int randomGenreLower = rand() % 100;

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder).Limit(1));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(genre, CondEq, randomGenre).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(name, CondEq, RandString()).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(rate, CondEq, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(genre, CondGt, randomGenre)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(name, CondGt, RandString()).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(rate, CondGt, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(genre, CondLt, randomGenre).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(name, CondLt, RandString()).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(rate, CondLt, static_cast<double>(rand() % 100) / 10)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(genre, CondRange, {randomGenreLower, randomGenreUpper})
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(name, CondRange, {RandString(), RandString()})
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace)
							.Where(rate, CondRange, {static_cast<double>(rand() % 100) / 10, static_cast<double>(rand() % 100) / 10})
							.Distinct(distinct.c_str())
							.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Where(packages, CondSet, RandIntVector(10, 10000, 50))
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(packages, CondEmpty, 0).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(packages, CondAny, 0).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(
						default_namespace,
						Query(default_namespace).Where(isDeleted, CondEq, 1).Distinct(distinct.c_str()).Sort(sortIdx, sortOrder));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(genre, CondEq, 5)
															.Where(age, CondEq, 3)
															.Where(year, CondGe, 2010)
															.Where(packages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(year, CondGt, 2002)
															.Where(genre, CondEq, 4)
															.Where(age, CondEq, 3)
															.Where(isDeleted, CondEq, 3)
															.Or()
															.Where(year, CondGt, 2001)
															.Where(packages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(age, CondSet, {1, 2, 3, 4})
															.Where(id, CondEq, rand() % 5000)
															.Where(temp, CondEq, "")
															.Where(isDeleted, CondEq, 1)
															.Or()
															.Where(year, CondGt, 2001)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(genre, CondSet, {5, 1, 7})
															.Where(year, CondLt, 2010)
															.Where(genre, CondEq, 3)
															.Where(packages, CondSet, RandIntVector(5, 10000, 50))
															.Or()
															.Where(packages, CondEmpty, 0)
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(genre, CondSet, {5, 1, 7})
															.Where(year, CondLt, 2010)
															.Or()
															.Where(packages, CondAny, 0)
															.Where(packages, CondSet, RandIntVector(5, 10000, 50))
															.Debug(LogTrace));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(genre, CondEq, 5)
															.Or()
															.Where(genre, CondEq, 6)
															.Where(year, CondRange, {2001, 2020})
															.Where(packages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(actor, CondEq, RandString()));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Not()
															.Where(genre, CondEq, 5)
															.Where(year, CondRange, {2001, 2020})
															.Where(packages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Where(genre, CondEq, 5)
															.Not()
															.Where(year, CondRange, {2001, 2020})
															.Where(packages, CondSet, RandIntVector(5, 10000, 50)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(sortIdx, sortOrder)
															.Debug(LogTrace)
															.Not()
															.Where(year, CondEq, 10));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(numeric, sortOrder)
															.Debug(LogTrace)
															.Where(numeric, CondGt, to_string(5)));

					ExecuteAndVerify(default_namespace, Query(default_namespace)
															.Distinct(distinct.c_str())
															.Sort(numeric, sortOrder)
															.Debug(LogTrace)
															.Where(numeric, CondLt, to_string(600)));

					ExecuteAndVerify(default_namespace,
									 Query(default_namespace)
										 .Distinct(distinct.c_str())
										 .Sort(sortIdx, sortOrder)
										 .Debug(LogTrace)
										 .Where(numeric, CondRange, {to_string(rand() % 100), to_string(rand() % 100 + 500)}));

					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(name, CondEq, "SSS"));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(year, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(year, CondEq, 2002).Not().Where(name, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(name, CondEq, "SSS").Not().Where(year, CondEq, 2002));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(name, CondEq, "SSS").Not().Where(year, CondEq, 1989));
					ExecuteAndVerify(testSimpleNs, Query(testSimpleNs).Where(year, CondEq, 2002).Not().Where(name, CondEq, "MMM"));
				}
			}
		}
	}

	void CheckAggregationQueries() {
		const int limit = 100;
		Query testQuery = Query(default_namespace).Where(genre, CondEq, 10).Limit(limit).Aggregate(year, AggAvg).Aggregate(year, AggSum);
		Query checkQuery = Query(default_namespace).Where(genre, CondEq, 10).Limit(limit);

		reindexer::QueryResults testQr;
		Error err = reindexer->Select(testQuery, testQr);
		EXPECT_TRUE(err.ok()) << err.what();

		reindexer::QueryResults checkQr;
		err = reindexer->Select(checkQuery, checkQr);
		EXPECT_TRUE(err.ok()) << err.what();

		double yearSum = 0.0;
		for (size_t i = 0; i < checkQr.size(); ++i) {
			Item item(checkQr.GetItem(static_cast<int>(i)));
			yearSum += item[year].Get<int>();
		}

		EXPECT_TRUE(AreDoublesEqual(testQr.aggregationResults[1], yearSum)) << "Aggregation Sum result is incorrect!";
		EXPECT_TRUE(AreDoublesEqual(testQr.aggregationResults[0], yearSum / checkQr.size())) << "Aggregation Sum result is incorrect!";
	}

	void CheckSqlQueries() {
		const string sqlQuery =
			"SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' AND genre IN ('1',2,'3') ORDER BY year DESC LIMIT 10000000";
		const Query checkQuery =
			Query(default_namespace, 0, 10000000).Where(year, CondGt, 2016).Where(genre, CondSet, {1, 2, 3}).Sort(year, true);

		QueryResults sqlQr;
		Error err = reindexer->Select(sqlQuery, sqlQr);
		EXPECT_TRUE(err.ok()) << err.what();

		QueryResults checkQr;
		err = reindexer->Select(checkQuery, checkQr);
		EXPECT_TRUE(err.ok()) << err.what();

		EXPECT_EQ(sqlQr.size(), checkQr.size());
		if (sqlQr.size() == checkQr.size()) {
			for (size_t i = 0; i < checkQr.size(); ++i) {
				Item ritem1(checkQr.GetItem(static_cast<int>(i)));
				Item ritem2(sqlQr.GetItem(static_cast<int>(i)));
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

	static bool AreDoublesEqual(double lhs, double rhs) { return std::abs(lhs - rhs) < numeric_limits<double>::epsilon(); }

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

	void PrintFailedSortOrdered(const Query& query, const QueryResults& qr, int itemIndex) {
		const int range = 5;
		printf("Sort order or last items: ");
		if (itemIndex > 0) {
			for (int i = itemIndex; i >= (itemIndex - range >= 0 ? itemIndex - range : 0); ++i) {
				Item item(qr.GetItem(static_cast<int>(i)));
				printf("%s, ", item[query.sortBy].As<string>().c_str());
			}
		}
		int numResults = qr.size();
		for (int i = itemIndex + 1; i < (itemIndex + range < numResults ? itemIndex + range : numResults - 1); ++i) {
			Item item(qr.GetItem(static_cast<int>(i)));
			printf("%s, ", item[query.sortBy].As<string>().c_str());
		}
		printf("\n\n");
		fflush(stdout);
	}

	using NamespaceName = string;
	using InsertedItemsByPk = std::map<string, reindexer::Item>;
	std::unordered_map<NamespaceName, InsertedItemsByPk> insertedItems;
	std::unordered_map<string, IndexOpts> indexesOptions;

	const char* id = "id";
	const char* genre = "genre";
	const char* year = "year";
	const char* packages = "packages";
	const char* name = "name";
	const char* countries = "countries";
	const char* age = "age";
	const char* description = "description";
	const char* rate = "rate";
	const char* isDeleted = "is_deleted";
	const char* actor = "actor";
	const char* priceId = "price_id";
	const char* location = "location";
	const char* endTime = "end_time";
	const char* startTime = "start_time";
	const char* phone = "phone";
	const char* temp = "tmp";
	const char* numeric = "numeric";
	const string compositePlus = "+";
	const string testSimpleNs = "test_simple_namespace";

	vector<string> defaultNsPks;
	vector<string> simpleTestNsPks;
};
