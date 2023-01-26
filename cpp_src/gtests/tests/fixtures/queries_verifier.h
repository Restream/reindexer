#pragma once

#include <gtest/gtest.h>
#include <regex>
#include <unordered_map>
#include "core/nsselecter/joinedselectormock.h"
#include "core/query/query.h"
#include "core/queryresults/joinresults.h"
#include "core/queryresults/queryresults.h"
#include "core/reindexer.h"
#include "core/sorting/sortexpression.h"
#include "test_helpers.h"
#include "tools/string_regexp_functions.h"

class QueriesVerifier : public virtual ::testing::Test {
	struct PkHash {
		size_t operator()(const std::vector<reindexer::VariantArray> pk) const noexcept {
			size_t ret = pk.size();
			for (const auto& k : pk) ret = ((ret * 127) ^ (ret >> 3)) ^ k.Hash();
			return ret;
		}
	};

protected:
	void Verify(const reindexer::QueryResults& qr, const reindexer::Query& query, reindexer::Reindexer& rx) {
		Verify(qr.ToLocalQr(), query, rx);
	}
	void Verify(const reindexer::LocalQueryResults& qr, const reindexer::Query& query, reindexer::Reindexer& rx) {
		std::unordered_set<std::vector<reindexer::VariantArray>, PkHash> pks;
		std::unordered_map<std::string, std::unordered_set<std::string>> distincts;
		QueryWatcher watcher{query};

		reindexer::VariantArray lastSortedColumnValues;
		lastSortedColumnValues.resize(query.sortingEntries_.size());

		auto joinedSelectors = getJoinedSelectors(query);
		for (auto& js : joinedSelectors) {
			const reindexer::Error err = rx.Select(js.JoinQuery(), js.QueryResults());
			ASSERT_TRUE(err.ok()) << err.what();
			Verify(js.QueryResults().ToLocalQr(), js.JoinQuery(), rx);
		}
		const auto& indexesFields = indexesFields_[query._namespace];
		for (size_t i = 0; i < qr.Count(); ++i) {
			reindexer::Item itemr(qr[i].GetItem(false));

			auto pk = getPk(itemr, query._namespace);
			EXPECT_TRUE(pks.insert(pk).second) << "Duplicated primary key: " + getPkString(pk);

			InsertedItemsByPk& insertedItemsByPk = insertedItems_[query._namespace];
			auto itInsertedItem = insertedItemsByPk.find(pk);
			EXPECT_NE(itInsertedItem, insertedItemsByPk.end()) << "Item with such PK has not been inserted yet: " + getPkString(pk);
			if (itInsertedItem != insertedItemsByPk.end()) {
				reindexer::Item& insertedItem = itInsertedItem->second;
				bool eq = (insertedItem.GetJSON() == itemr.GetJSON());
				EXPECT_TRUE(eq) << "Items' jsons are different! pk: " << getPkString(pk) << std::endl
								<< "expect json: " << insertedItem.GetJSON() << std::endl
								<< "got json: " << itemr.GetJSON() << std::endl
								<< "expect fields: " << PrintItem(insertedItem) << std::endl
								<< "got fields: " << PrintItem(itemr) << std::endl
								<< "explain: " << qr.GetExplainResults();
			}

			bool conditionsSatisfied = checkConditions(itemr, query.entries.cbegin(), query.entries.cend(), joinedSelectors, indexesFields);
			if (!conditionsSatisfied) {
				std::stringstream ss;
				ss << "Item doesn't match conditions: " << itemr.GetJSON() << std::endl;
				const auto& jit = qr[i].GetJoined();
				if (jit.getJoinedItemsCount() > 0) {
					ss << "Joined:" << std::endl;
					for (int fIdx = 0, fCount = jit.getJoinedFieldsCount(); fIdx < fCount; ++fIdx) {
						for (int j = 0, iCount = jit.at(fIdx).ItemsCount(); j < iCount; ++j) {
							const auto nsCtxIdx = qr.GetJoinedNsCtxIndex(jit.at(fIdx)[j].Nsid());
							ss << jit.at(fIdx).GetItem(j, qr.getPayloadType(nsCtxIdx), qr.getTagsMatcher(nsCtxIdx)).GetJSON() << std::endl;
						}
					}
				}
				ss << "explain: " << qr.GetExplainResults();
				EXPECT_TRUE(conditionsSatisfied) << ss.str();
				TEST_COUT << query.GetSQL() << std::endl;
				printFailedQueryEntries(query.entries, joinedSelectors);
			}
			EXPECT_TRUE(checkDistincts(itemr, query, distincts, indexesFields)) << "Distinction check failed";

			std::vector<int> cmpRes(query.sortingEntries_.size());
			std::fill(cmpRes.begin(), cmpRes.end(), -1);

			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				const reindexer::SortingEntry& sortingEntry(query.sortingEntries_[j]);
				const auto sortExpr = reindexer::SortExpression::Parse(sortingEntry.expression, joinedSelectors);

				reindexer::Variant sortedValue;
				if (sortExpr.ByField()) {
					sortedValue = itemr[sortingEntry.expression];
				} else if (sortExpr.ByJoinedField()) {
					auto jItemIt = (qr.begin() + i).GetJoined();
					EXPECT_EQ(jItemIt.getJoinedFieldsCount(), 1);
					EXPECT_EQ(jItemIt.getJoinedItemsCount(), 1);
					reindexer::ItemImpl joinItem(jItemIt.begin().GetItem(0, qr.getPayloadType(1), qr.getTagsMatcher(1)));
					auto fieldName = sortingEntry.expression.substr(sortingEntry.expression.find_first_of('.'));
					sortedValue = joinItem.GetValueByJSONPath(fieldName)[0];
				} else {
					sortedValue = reindexer::Variant{calculateSortExpression(sortExpr.cbegin(), sortExpr.cend(), itemr, qr)};
				}
				if (!lastSortedColumnValues[j].Type().Is<reindexer::KeyValueType::Null>()) {
					bool needToVerify = true;
					for (int k = j - 1; k >= 0; --k) {
						if (cmpRes[k] != 0) {
							needToVerify = false;
							break;
						}
					}
					if (needToVerify) {
						if (j == 0 && !query.forcedSortOrder_.empty()) {
							const auto currValIt = std::find(query.forcedSortOrder_.cbegin(), query.forcedSortOrder_.cend(), sortedValue);
							const auto lastValIt =
								std::find(query.forcedSortOrder_.cbegin(), query.forcedSortOrder_.cend(), lastSortedColumnValues[0]);
							if (lastValIt < currValIt) {
								cmpRes[0] = -1;
							} else if (lastValIt > currValIt) {
								cmpRes[0] = 1;
							} else if (lastValIt == query.forcedSortOrder_.cend()) {
								cmpRes[0] = lastSortedColumnValues[0].Compare(sortedValue);
							} else {
								cmpRes[0] = 0;
							}
						} else {
							cmpRes[j] = lastSortedColumnValues[j].Compare(sortedValue);
						}
						bool sortOrderSatisfied =
							(sortingEntry.desc && cmpRes[j] >= 0) || (!sortingEntry.desc && cmpRes[j] <= 0) || (cmpRes[j] == 0);
						if (!sortOrderSatisfied) {
							EXPECT_TRUE(sortOrderSatisfied) << "\nSort order is incorrect for column: " << sortingEntry.expression;
							TEST_COUT << query.GetSQL() << std::endl;
							printFailedSortOrder(query, qr, i);
						}
					}
				}
				lastSortedColumnValues[j] = sortedValue;
			}
		}

		// Check non found items, to not match conditions

		// If query has limit and offset, skip verification
		if (query.start != 0 || query.count != UINT_MAX) return;

		// If query has distinct, skip verification
		for (const auto& agg : query.aggregations_) {
			if (agg.type_ == AggDistinct) return;
		}

		for (auto& insertedItem : insertedItems_[query._namespace]) {
			if (pks.find(insertedItem.first) != pks.end()) continue;
			bool conditionsSatisfied =
				checkConditions(insertedItem.second, query.entries.cbegin(), query.entries.cend(), joinedSelectors, indexesFields);

			EXPECT_FALSE(conditionsSatisfied) << "Item match conditions (found " << qr.Count()
											  << " items), but not found: " << insertedItem.second.GetJSON() << std::endl
											  << "query:" << query.GetSQL() << std::endl
											  << "explain: " << qr.GetExplainResults() << std::endl;
		}

		auto aggResults = qr.GetAggregationResults();
		if (query.calcTotal != ModeNoTotal) {
			// calcTotal from version 3.0.2  also return total count in aggregations, so we have remove it from here for
			// clean compare aggresults with aggregations
			aggResults.pop_back();
		}

		EXPECT_EQ(aggResults.size(), query.aggregations_.size());

		if (aggResults.size() == query.aggregations_.size()) {
			for (size_t i = 0; i < aggResults.size(); ++i) {
				EXPECT_EQ(aggResults[i].type, query.aggregations_[i].type_) << "i = " << i;
				EXPECT_EQ(aggResults[i].fields.size(), query.aggregations_[i].fields_.size()) << "i = " << i;
				if (aggResults[i].fields.size() == query.aggregations_[i].fields_.size()) {
					for (size_t j = 0; j < aggResults[i].fields.size(); ++j) {
						EXPECT_EQ(aggResults[i].fields[j], query.aggregations_[i].fields_[j]) << "i = " << i << ", j = " << j;
					}
				}
				EXPECT_LE(aggResults[i].facets.size(), query.aggregations_[i].limit_) << "i = " << i;
			}
		}
	}

private:
	using InsertedItemsByPk = std::unordered_map<std::vector<reindexer::VariantArray>, reindexer::Item, PkHash>;

protected:
	void saveItem(reindexer::Item&& item, const std::string& ns) {
		auto pk = getPk(item, ns);
		auto& ii = insertedItems_[ns];
		const auto it = ii.find(pk);
		if (it == ii.end()) {
			for (auto& p : pk) p.EnsureHold();
			[[maybe_unused]] const auto res = ii.emplace(std::move(pk), std::move(item));
			assert(res.second);
		} else {
			it->second = std::move(item);
		}
	}
	void deleteItem(reindexer::Item& item, const std::string& ns) {
		const auto pk = getPk(item, ns);
		auto& ii = insertedItems_[ns];
		const auto it = ii.find(pk);
		if (it != ii.end()) {
			ii.erase(it);
		}
	}

	void setPkFields(std::string ns, std::vector<std::string> fields) {
		[[maybe_unused]] const auto res = ns2pk_.emplace(std::move(ns), std::move(fields));
		assert(res.second);
	}

	void addIndexFields(const std::string& ns, std::string indexName, std::vector<std::string> fields) {
		const bool success = indexesFields_[ns].emplace(std::move(indexName), std::move(fields)).second;
		ASSERT_TRUE(success) << ns << ' ' << indexName;
	}

	std::unordered_map<std::string, CollateOpts> indexesCollates;
	std::unordered_map<std::string, InsertedItemsByPk> insertedItems_;

private:
	bool checkConditions(const reindexer::Item& item, reindexer::QueryEntries::const_iterator it,
						 reindexer::QueryEntries::const_iterator to, const std::vector<JoinedSelectorMock>& joinedSelectors,
						 const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		bool result = true;
		for (; it != to; ++it) {
			OpType op = it->operation;
			if (op != OpOr && !result) return false;
			bool skip = false;
			bool const iterationResult = it->InvokeAppropriate<bool>(
				[&](const reindexer::QueryEntriesBracket&) {
					if (op == OpOr && result && !containsJoins(it.cbegin(), it.cend())) {
						skip = true;
						return false;
					}
					return checkConditions(item, it.cbegin(), it.cend(), joinedSelectors, indexesFields);
				},
				[&](const reindexer::QueryEntry& qe) {
					if ((op == OpOr && result) || qe.distinct) {
						skip = true;
						return false;
					}
					return checkCondition(item, qe, indexesFields);
				},
				[&](const reindexer::JoinQueryEntry& jqe) {
					assert(jqe.joinIndex < joinedSelectors.size());
					if (joinedSelectors[jqe.joinIndex].Type() == OrInnerJoin) {
						assert(op != OpNot);
						op = OpOr;
					}
					const auto& js = joinedSelectors[jqe.joinIndex];
					const auto& rightIndexesFields = indexesFields_[js.RightNsName()];
					return checkCondition(item, js, indexesFields, rightIndexesFields);
				},
				[&](const reindexer::BetweenFieldsQueryEntry& qe) {
					if (op == OpOr && result) {
						skip = true;
						return false;
					}
					return checkCondition(item, qe, indexesFields);
				},
				[](const reindexer::AlwaysFalse&) { return false; });
			if (skip) continue;
			switch (op) {
				case OpNot:
					result = !iterationResult;
					break;
				case OpAnd:
					result = iterationResult;
					break;
				case OpOr:
					result = iterationResult || result;
					break;
			}
		}
		return result;
	}

	static std::string getFieldName(const std::string& indexName,
									const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		if (const auto it = indexesFields.find(indexName); it == indexesFields.end()) {
			return indexName;
		} else {
			EXPECT_EQ(it->second.size(), 1);
			assert(!it->second.empty());
			return it->second[0];
		}
	}

	static bool checkDistincts(reindexer::Item& item, const reindexer::Query& qr,
							   std::unordered_map<std::string, std::unordered_set<std::string>>& distincts,
							   const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		bool result = true;
		// check only on root level
		for (auto it = qr.entries.cbegin(); it != qr.entries.cend(); ++it) {
			if (!it->HoldsOrReferTo<reindexer::QueryEntry>()) continue;
			const reindexer::QueryEntry& qentry = it->Value<reindexer::QueryEntry>();
			if (!qentry.distinct) continue;

			const std::string fieldName = getFieldName(qentry.index, indexesFields);
			reindexer::VariantArray fieldValue = item[fieldName];
			EXPECT_EQ(fieldValue.size(), 1) << "Distinct field's size cannot be > 1";
			if (fieldValue.empty()) return false;

			std::unordered_set<std::string>& values = distincts[fieldName];
			reindexer::Variant keyValue(fieldValue[0]);
			bool inserted = values.insert(keyValue.As<std::string>()).second;
			EXPECT_TRUE(inserted) << "Duplicate distinct item for index: " << keyValue.As<std::string>() << ", " << qentry.idxNo;
			result &= inserted;
		}
		return result;
	}

	bool checkCondition(const reindexer::Item& item, const JoinedSelectorMock& joinedSelector,
						const std::unordered_map<std::string, std::vector<std::string>>& leftIndexesFields,
						const std::unordered_map<std::string, std::vector<std::string>>& rightIndexesFields) {
		for (auto it : joinedSelector.QueryResults()) {
			const reindexer::Item& rightItem = it.GetItem(false);
			bool result = true;
			const auto& joinEntries{joinedSelector.JoinQuery().joinEntries_};
			assert(!joinEntries.empty());
			assert(joinEntries[0].op_ != OpOr);
			for (const auto& je : joinEntries) {
				if (je.op_ == OpOr) {
					if (result) continue;
				} else if (!result) {
					break;
				}
				const bool curResult =
					checkOnCondition(item, rightItem, je.index_, je.joinIndex_, je.condition_, leftIndexesFields, rightIndexesFields);
				switch (je.op_) {
					case OpAnd:
						result = curResult;
						break;
					case OpNot:
						result = !curResult;
						break;
					case OpOr:
						result = result || curResult;
						break;
					default:
						assert(0);
				}
			}
			if (result) return true;
		}
		return false;
	}

	bool checkOnCondition(const reindexer::Item& leftItem, const reindexer::Item& rightItem, const std::string& leftIndexName,
						  const std::string& rightIndexName, CondType cond,
						  const std::unordered_map<std::string, std::vector<std::string>>& leftIndexesFields,
						  const std::unordered_map<std::string, std::vector<std::string>>& rightIndexesFields) {
		const CollateOpts& collate = indexesCollates[leftIndexName];
		const std::string leftFieldName = getFieldName(leftIndexName, leftIndexesFields);
		const std::string rightFieldName = getFieldName(rightIndexName, rightIndexesFields);
		const reindexer::VariantArray fieldValues = leftItem[leftFieldName];
		for (const reindexer::Variant& fieldValue : fieldValues) {
			if (compareValues(cond, fieldValue, rightItem[rightFieldName], collate)) return true;
		}
		return false;
	}

	bool checkCondition(const reindexer::Item& item, const reindexer::QueryEntry& qentry,
						const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		EXPECT_GT(item.NumFields(), 0);
		if (isGeomConditions(qentry.condition)) {
			return checkGeomConditions(item, qentry, indexesFields);
		}
		const CollateOpts& collate = indexesCollates[qentry.index];

		if (isIndexComposite(item, qentry)) {
			return checkCompositeValues(item, qentry, collate, indexesFields);
		} else {
			std::string fieldName;
			if (const auto it = indexesFields.find(qentry.index); it == indexesFields.end()) {
				fieldName = qentry.index;
			} else {
				EXPECT_EQ(it->second.size(), 1);
				assert(!it->second.empty());
				fieldName = it->second[0];
			}
			reindexer::VariantArray fieldValues = item[fieldName];
			switch (qentry.condition) {
				case CondEmpty:
					return fieldValues.size() == 0;
				case CondAny:
					return fieldValues.size() > 0;
				case CondAllSet:
					for (const auto& qvalue : qentry.values) {
						if (!compareValues(CondSet, qvalue, fieldValues, collate)) return false;
					}
					return true;
				default:
					break;
			}
			for (const reindexer::Variant& fieldValue : fieldValues) {
				if (compareValues(qentry.condition, fieldValue, qentry.values, collate)) return true;
			}
		}

		return false;
	}

	static bool isGeomConditions(CondType cond) noexcept { return cond == CondType::CondDWithin; }

	static bool checkGeomConditions(const reindexer::Item& item, const reindexer::QueryEntry& qentry,
									const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		assert(qentry.values.size() == 2);
		const reindexer::VariantArray coordinates = item[getFieldName(qentry.index, indexesFields)];
		if (coordinates.empty()) return false;
		assert(coordinates.size() == 2);
		const double x = coordinates[0].As<double>();
		const double y = coordinates[1].As<double>();
		switch (qentry.condition) {
			case CondDWithin:
				return DWithin({x, y}, qentry.values[0].As<reindexer::Point>(), qentry.values[1].As<double>());
			default:
				assert(0);
				abort();
		}
	}

	static reindexer::VariantArray getValues(const reindexer::Item& item, const std::vector<std::string>& fields) {
		reindexer::VariantArray kvalues;
		for (const std::string& field : fields) {
			kvalues.push_back(item[field].operator reindexer::Variant());
		}
		return kvalues;
	}

	static std::vector<std::string> getCompositeFieldsNames(
		const std::string& indexName, const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		if (const auto it = indexesFields.find(indexName); it == indexesFields.end()) {
			std::vector<std::string> fields;
			reindexer::split(indexName, "+", true, fields);
			return fields;
		} else {
			return it->second;
		}
	}

	static bool checkCompositeValues(const reindexer::Item& item, const reindexer::QueryEntry& qentry, const CollateOpts& opts,
									 const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		const std::vector<std::string> fields = getCompositeFieldsNames(qentry.index, indexesFields);

		const reindexer::VariantArray indexesValues = getValues(item, fields);
		const reindexer::VariantArray& keyValues = qentry.values;

		switch (qentry.condition) {
			case CondEmpty:
				return indexesValues.size() == 0;
			case CondAny:
				return indexesValues.size() > 0;
			case CondGe:
				return compareCompositeValues(indexesValues, keyValues[0], opts) >= 0;
			case CondGt:
				return compareCompositeValues(indexesValues, keyValues[0], opts) > 0;
			case CondLt:
				return compareCompositeValues(indexesValues, keyValues[0], opts) < 0;
			case CondLe:
				return compareCompositeValues(indexesValues, keyValues[0], opts) <= 0;
			case CondRange:
				EXPECT_TRUE(keyValues.size() == 2);
				return (compareCompositeValues(indexesValues, keyValues[0], opts) >= 0) &&
					   (compareCompositeValues(indexesValues, keyValues[1], opts) <= 0);
			case CondEq:
			case CondSet:
				for (const reindexer::Variant& kv : keyValues) {
					if (compareCompositeValues(indexesValues, kv, opts) == 0) return true;
				}
				return false;
			case CondAllSet:
				for (const reindexer::Variant& kv : indexesValues) {
					if (compareCompositeValues(indexesValues, kv, opts) != 0) return false;
				}
				return true;
			default:
				std::abort();
		}
	}

	static int compareCompositeValues(const reindexer::VariantArray& indexesValues, const reindexer::Variant& keyValue,
									  const CollateOpts& opts) {
		reindexer::VariantArray compositeValues = keyValue.getCompositeValues();
		EXPECT_TRUE(indexesValues.size() == compositeValues.size());
		int cmpRes = 0;
		for (size_t i = 0; i < indexesValues.size() && (cmpRes == 0); ++i) {
			compositeValues[i].convert(indexesValues[i].Type());
			cmpRes = indexesValues[i].Compare(compositeValues[i], opts);
		}
		return cmpRes;
	}

	static bool compareValues(CondType condition, reindexer::Variant key, const reindexer::VariantArray& values, const CollateOpts& opts) {
		if (values.empty()) return false;
		try {
			key.convert(values[0].Type());
		} catch (const reindexer::Error& err) {
			return false;
		}
		switch (condition) {
			case CondGe:
				return key.Compare(values[0], opts) >= 0;
			case CondGt:
				return key.Compare(values[0], opts) > 0;
			case CondLt:
				return key.Compare(values[0], opts) < 0;
			case CondLe:
				return key.Compare(values[0], opts) <= 0;
			case CondRange:
				assert(values.size() > 1);
				return (key.Compare(values[0], opts) >= 0) && (key.Compare(values[1], opts) <= 0);
			case CondEq:
			case CondSet:
				for (const reindexer::Variant& kv : values) {
					if (key.Compare(kv, opts) == 0) return true;
				}
				return false;
			case CondLike:
				if (!key.Type().Is<reindexer::KeyValueType::String>()) {
					return false;
				}
				return isLikeSqlPattern(*static_cast<reindexer::key_string>(key.convert(reindexer::KeyValueType::String{})),
										*static_cast<reindexer::key_string>(values[0].convert(reindexer::KeyValueType::String{})));
			default:
				std::abort();
		}
	}

	static bool isLikeSqlPattern(std::string_view str, std::string_view pattern) {
		return std::regex_match(std::string(str), std::regex{reindexer::sqlLikePattern2ECMAScript(std::string(pattern))});
	}

	bool checkCompositeCondition(const reindexer::Item& item, const reindexer::BetweenFieldsQueryEntry& qentry,
								 const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		const std::vector<std::string> firstFields = getCompositeFieldsNames(qentry.firstIndex, indexesFields);
		const std::vector<std::string> secondFields = getCompositeFieldsNames(qentry.secondIndex, indexesFields);
		assert(firstFields.size() == secondFields.size());

		reindexer::BetweenFieldsQueryEntry qe{qentry};
		for (size_t i = 0; i < firstFields.size(); ++i) {
			qe.firstIndex = firstFields[i];
			qe.secondIndex = secondFields[i];
			if (!checkCondition(item, qe, indexesFields)) return false;
		}
		return !firstFields.empty();
	}

	static bool isIndexComposite(const reindexer::BetweenFieldsQueryEntry& qe,
								 const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		if (qe.firstIndex.find('+') != std::string::npos || qe.secondIndex.find('+') != std::string::npos) return true;
		if (const auto it = indexesFields.find(qe.firstIndex); it != indexesFields.end() && it->second.size() > 1) return true;
		if (const auto it = indexesFields.find(qe.secondIndex); it != indexesFields.end() && it->second.size() > 1) return true;
		return false;
	}

	static bool isIndexComposite(const reindexer::Item& item, const reindexer::QueryEntry& qentry) {
		if (qentry.idxNo >= item.NumFields()) return true;
		return qentry.values[0].Type().Is<reindexer::KeyValueType::Composite>() ||
			   qentry.values[0].Type().Is<reindexer::KeyValueType::Tuple>();
	}

	bool checkCondition(const reindexer::Item& item, const reindexer::BetweenFieldsQueryEntry& qentry,
						const std::unordered_map<std::string, std::vector<std::string>>& indexesFields) {
		EXPECT_GT(item.NumFields(), 0);
		assert(!isGeomConditions(qentry.Condition()));

		const CollateOpts& collate = indexesCollates[qentry.firstIndex];

		if (isIndexComposite(qentry, indexesFields)) {
			return checkCompositeCondition(item, qentry, indexesFields);
		}

		const std::string firstField = getFieldName(qentry.firstIndex, indexesFields);
		const std::string secondField = getFieldName(qentry.secondIndex, indexesFields);
		reindexer::VariantArray lValues = item[firstField];
		reindexer::VariantArray rValues = item[secondField];
		switch (qentry.Condition()) {
			case CondAllSet:
				for (const auto& rv : rValues) {
					if (!compareValues(CondSet, rv, lValues, collate)) return false;
				}
				return true;
			case CondRange:
				assert(rValues.size() == 2);
				for (const auto& lv : lValues) {
					if (lv.RelaxCompare(rValues[0], collate) >= 0 && lv.Compare(rValues[1], collate) <= 0) return true;
				}
				return false;
			case CondLike:
				for (const reindexer::Variant& lv : lValues) {
					assert(lv.Type().Is<reindexer::KeyValueType::String>());
					const auto lstr = *static_cast<reindexer::key_string>(lv.convert(reindexer::KeyValueType::String{}));
					for (const reindexer::Variant& rv : rValues) {
						assert(rv.Type().Is<reindexer::KeyValueType::String>());
						if (isLikeSqlPattern(lstr, *static_cast<reindexer::key_string>(rv.convert(reindexer::KeyValueType::String{}))))
							return true;
					}
				}
				return false;
			default:
				for (const reindexer::Variant& lv : lValues) {
					for (const reindexer::Variant& rv : rValues) {
						const int res = lv.RelaxCompare(rv, collate);
						switch (qentry.Condition()) {
							case CondGe:
								if (res >= 0) return true;
								break;
							case CondGt:
								if (res > 0) return true;
								break;
							case CondLt:
								if (res < 0) return true;
								break;
							case CondLe:
								if (res <= 0) return true;
								break;
							case CondEq:
							case CondSet:
								if (res == 0) return true;
								break;
							default:
								std::abort();
						}
					}
				}
				return false;
		}
	}

	static inline double distance(reindexer::Point p1, reindexer::Point p2) noexcept {
		return std::sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
	}

	static reindexer::VariantArray getJoinedField(int id, const reindexer::LocalQueryResults& qr, size_t nsIdx, int index,
												  std::string_view column) noexcept {
		const reindexer::joins::ItemIterator itemIt{&qr.joined_[0], id};
		const auto joinedIt = itemIt.at(nsIdx);
		assert(joinedIt.ItemsCount() == 1);
		auto joinedItem = joinedIt.GetItem(0, qr.getPayloadType(nsIdx + 1), qr.getTagsMatcher(nsIdx + 1));
		reindexer::VariantArray values;
		if (index == IndexValueType::SetByJsonPath || index == IndexValueType::NotSet) {
			values = joinedItem.GetValueByJSONPath(column);
		} else {
			joinedItem.GetField(index, values);
		}
		return values;
	}

	static double calculateSortExpression(reindexer::SortExpression::const_iterator begin, reindexer::SortExpression::const_iterator end,
										  reindexer::Item& item, const reindexer::LocalQueryResults& qr) {
		double result = 0.0;
		assert(begin != end);
		assert(begin->operation.op == OpPlus);
		for (auto it = begin; it != end; ++it) {
			double value = it->InvokeAppropriate<double>(
				[&it, &item, &qr](const reindexer::SortExpressionBracket&) {
					return calculateSortExpression(it.cbegin(), it.cend(), item, qr);
				},
				[](const reindexer::SortExprFuncs::Value& v) { return v.value; },
				[&item](const reindexer::SortExprFuncs::Index& i) { return item[i.column].As<double>(); },
				[&item, &qr](const reindexer::SortExprFuncs::JoinedIndex& i) {
					const auto values = getJoinedField(item.GetID(), qr, i.nsIdx, i.index, i.column);
					assert(values.size() == 1);
					return values[0].As<double>();
				},
				[](const reindexer::SortExprFuncs::Rank&) -> double { abort(); },
				[&item](const reindexer::SortExprFuncs::DistanceFromPoint& i) {
					return distance(static_cast<reindexer::Point>(static_cast<reindexer::VariantArray>(item[i.column])), i.point);
				},
				[&item, &qr](const reindexer::SortExprFuncs::DistanceJoinedIndexFromPoint& i) {
					const auto values = getJoinedField(item.GetID(), qr, i.nsIdx, i.index, i.column);
					return distance(static_cast<reindexer::Point>(values), i.point);
				},
				[&item](const reindexer::SortExprFuncs::DistanceBetweenIndexes& i) {
					return distance(static_cast<reindexer::Point>(static_cast<reindexer::VariantArray>(item[i.column1])),
									static_cast<reindexer::Point>(static_cast<reindexer::VariantArray>(item[i.column2])));
				},
				[&item, &qr](const reindexer::SortExprFuncs::DistanceBetweenIndexAndJoinedIndex& i) {
					const auto jValues = getJoinedField(item.GetID(), qr, i.jNsIdx, i.jIndex, i.jColumn);
					return distance(static_cast<reindexer::Point>(static_cast<reindexer::VariantArray>(item[i.column])),
									static_cast<reindexer::Point>(jValues));
				},
				[&item, &qr](const reindexer::SortExprFuncs::DistanceBetweenJoinedIndexes& i) {
					const auto values1 = getJoinedField(item.GetID(), qr, i.nsIdx1, i.index1, i.column1);
					const auto values2 = getJoinedField(item.GetID(), qr, i.nsIdx2, i.index2, i.column2);
					return distance(static_cast<reindexer::Point>(values1), static_cast<reindexer::Point>(values2));
				},
				[&item, &qr](const reindexer::SortExprFuncs::DistanceBetweenJoinedIndexesSameNs& i) {
					const auto values1 = getJoinedField(item.GetID(), qr, i.nsIdx, i.index1, i.column1);
					const auto values2 = getJoinedField(item.GetID(), qr, i.nsIdx, i.index2, i.column2);
					return distance(static_cast<reindexer::Point>(values1), static_cast<reindexer::Point>(values2));
				});
			if (it->operation.negative) value = -value;
			switch (it->operation.op) {
				case OpPlus:
					result += value;
					break;
				case OpMinus:
					result -= value;
					break;
				case OpMult:
					result *= value;
					break;
				case OpDiv:
					assert(value != 0.0);
					result /= value;
					break;
			}
		}
		return result;
	}

	static bool containsJoins(reindexer::QueryEntries::const_iterator it, reindexer::QueryEntries::const_iterator end) noexcept {
		for (; it != end; ++it) {
			if (it->InvokeAppropriate<bool>(
					[&it](const reindexer::QueryEntriesBracket&) { return containsJoins(it.cbegin(), it.cend()); },
					[](const reindexer::JoinQueryEntry&) { return true; }, [](const reindexer::QueryEntry&) { return false; },
					[](const reindexer::BetweenFieldsQueryEntry&) { return false; }, [](const reindexer::AlwaysFalse&) { return false; })) {
				return true;
			}
		}
		return false;
	}

	static std::vector<JoinedSelectorMock> getJoinedSelectors(const reindexer::Query& query) {
		std::vector<JoinedSelectorMock> result;
		result.reserve(query.joinQueries_.size());
		for (auto jq : query.joinQueries_) {
			jq.count = UINT_MAX;
			jq.start = 0;
			jq.sortingEntries_.clear();
			jq.forcedSortOrder_.clear();
			result.emplace_back(InnerJoin, std::move(jq));
		}
		return result;
	}

	std::vector<reindexer::VariantArray> getPk(reindexer::Item& item, const std::string& ns) const {
		std::vector<reindexer::VariantArray> ret;
		const std::vector<std::string>& pkFields(getNsPks(ns));
		for (const std::string& field : pkFields) {
			ret.push_back(item[field].operator reindexer::VariantArray());
		}
		return ret;
	}

	static std::string getPkString(const std::vector<reindexer::VariantArray>& data) {
		std::string ret;
		for (const auto& values : data) {
			if (values.size() == 1) {
				ret += values[0].template As<std::string>();
			} else {
				ret += '[';
				for (size_t i = 0, s = values.size(); i < s; ++i) {
					if (i != 0) ret += ',';
					ret += values[i].template As<std::string>();
				}
				ret += ']';
			}
			ret += '#';
		}
		return ret;
	}

	const std::vector<std::string>& getNsPks(const std::string& ns) const {
		const auto it = ns2pk_.find(ns);
		assert(it != ns2pk_.end());
		return it->second;
	}

	static void printFailedQueryEntries(const reindexer::QueryEntries& failedEntries, const std::vector<JoinedSelectorMock>& js) {
		TestCout() << "Failed entries: ";
		printQueryEntries(failedEntries.cbegin(), failedEntries.cend(), js);
		TestCout() << std::endl << std::endl;
	}

	static void printQueryEntries(reindexer::QueryEntries::const_iterator it, reindexer::QueryEntries::const_iterator to,
								  const std::vector<JoinedSelectorMock>& js) {
		TestCout() << "(";
		for (; it != to; ++it) {
			TestCout() << (it->operation == OpAnd ? "AND" : (it->operation == OpOr ? "OR" : "NOT"));
			it->InvokeAppropriate<void>(
				[&it, &js](const reindexer::QueryEntriesBracket&) { printQueryEntries(it.cbegin(), it.cend(), js); },
				[](const reindexer::QueryEntry& qe) { TestCout() << qe.Dump(); },
				[&js](const reindexer::JoinQueryEntry& jqe) { TestCout() << jqe.Dump(js); },
				[](const reindexer::BetweenFieldsQueryEntry& qe) { TestCout() << qe.Dump(); },
				[](const reindexer::AlwaysFalse&) { TestCout() << "Always False"; });
		}
		TestCout() << ")";
	}

	static void printFailedSortOrder(const reindexer::Query& query, const reindexer::LocalQueryResults& qr, int itemIndex,
									 int itemsToShow = 10) {
		if (qr.Count() == 0) return;

		TestCout() << "Sort order or last items:" << std::endl;
		reindexer::Item rdummy(qr[0].GetItem(false));
		TestCout().BoldOn();
		for (size_t idx = 0; idx < query.sortingEntries_.size(); idx++) {
			TestCout() << rdummy[query.sortingEntries_[idx].expression].Name() << " ";
		}
		TestCout().Endl().Endl();
		TestCout().BoldOff();

		int firstItem = itemIndex - itemsToShow;
		if (firstItem < 0) firstItem = 0;
		for (int i = firstItem; i <= itemIndex; ++i) {
			reindexer::Item item(qr[i].GetItem(false));
			if (i == itemIndex) TestCout().BoldOn();
			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				TestCout() << item[query.sortingEntries_[j].expression].As<std::string>() << " ";
			}
			if (i == itemIndex) TestCout().BoldOff();
			TestCout().Endl();
		}

		firstItem = itemIndex + 1;
		int lastItem = firstItem + itemsToShow;
		const int count = static_cast<int>(qr.Count());
		if (firstItem >= count) firstItem = count - 1;
		if (lastItem > count) lastItem = count;
		for (int i = firstItem; i < lastItem; ++i) {
			reindexer::Item item(qr[i].GetItem(false));
			for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
				TestCout() << item[query.sortingEntries_[j].expression].As<std::string>() << " ";
			}
			TestCout().Endl();
		}
		TestCout().Endl().Endl();
	}

	std::unordered_map<std::string, std::vector<std::string>> ns2pk_;
	std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string>>> indexesFields_;
};
