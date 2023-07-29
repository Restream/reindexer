#pragma once

#include <gtest/gtest.h>
#include <regex>
#include <unordered_map>
#include "core/nsselecter/joinedselectormock.h"
#include "core/nsselecter/sortexpression.h"
#include "core/query/query.h"
#include "core/queryresults/joinresults.h"
#include "core/queryresults/queryresults.h"
#include "core/reindexer.h"
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
	struct FieldData {
		std::string name;
		reindexer::KeyValueType type;
	};
	void Verify(const reindexer::QueryResults& qr, const reindexer::Query& query, reindexer::Reindexer& rx) {
		std::unordered_set<std::vector<reindexer::VariantArray>, PkHash> pks;
		std::unordered_map<std::string, std::unordered_set<std::string>> distincts;
		QueryWatcher watcher{query};

		reindexer::VariantArray lastSortedColumnValues;
		lastSortedColumnValues.resize(query.sortingEntries_.size());

		auto joinedSelectors = getJoinedSelectors(query);
		for (auto& js : joinedSelectors) {
			const reindexer::Error err = rx.Select(js.JoinQuery(), js.QueryResults());
			ASSERT_TRUE(err.ok()) << err.what();
			Verify(js.QueryResults(), js.JoinQuery(), rx);
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
				EXPECT_EQ(insertedItem.GetJSON(), itemr.GetJSON()) << "Items' jsons are different! pk: " << getPkString(pk) << std::endl
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
				if (sortExpr.ByIndexField()) {
					sortedValue = itemr[sortingEntry.expression];
				} else if (sortExpr.ByJoinedIndexField()) {
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
								cmpRes[0] = lastSortedColumnValues[0].RelaxCompare<reindexer::WithString::Yes>(sortedValue);
							} else {
								cmpRes[0] = 0;
							}
						} else {
							cmpRes[j] = lastSortedColumnValues[j].RelaxCompare<reindexer::WithString::Yes>(sortedValue);
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
		if (query.start != 0 || query.count != reindexer::QueryEntry::kDefaultLimit) return;

		// If query has distinct, skip verification
		for (const auto& agg : query.aggregations_) {
			if (agg.Type() == AggDistinct) return;
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
				EXPECT_EQ(aggResults[i].type, query.aggregations_[i].Type()) << "i = " << i;
				EXPECT_EQ(aggResults[i].fields.size(), query.aggregations_[i].Fields().size()) << "i = " << i;
				if (aggResults[i].fields.size() == query.aggregations_[i].Fields().size()) {
					for (size_t j = 0; j < aggResults[i].fields.size(); ++j) {
						EXPECT_EQ(aggResults[i].fields[j], query.aggregations_[i].Fields()[j]) << "i = " << i << ", j = " << j;
					}
				}
				EXPECT_LE(aggResults[i].facets.size(), query.aggregations_[i].Limit()) << "i = " << i;
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
			assertrx(res.second);
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
		assertrx(res.second);
	}

	void setPkFields(std::string ns, const std::vector<FieldData>& fields) {
		[[maybe_unused]] const auto res = ns2pk_.emplace(std::move(ns), std::vector<std::string>{});
		assertrx(res.second);
		for (const auto& f : fields) {
			res.first->second.push_back(f.name);
		}
	}

	void addIndexFields(const std::string& ns, std::string indexName, std::vector<FieldData> fields) {
		const bool success = indexesFields_[ns].emplace(std::move(indexName), std::move(fields)).second;
		ASSERT_TRUE(success) << ns << ' ' << indexName;
	}

	std::unordered_map<std::string, CollateOpts> indexesCollates;
	std::unordered_map<std::string, InsertedItemsByPk> insertedItems_;

private:
	bool checkConditions(const reindexer::Item& item, reindexer::QueryEntries::const_iterator it,
						 reindexer::QueryEntries::const_iterator to, const std::vector<JoinedSelectorMock>& joinedSelectors,
						 const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
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
					assertrx(jqe.joinIndex < joinedSelectors.size());
					if (joinedSelectors[jqe.joinIndex].Type() == OrInnerJoin) {
						assertrx(op != OpNot);
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
									const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		if (const auto it = indexesFields.find(indexName); it == indexesFields.end()) {
			return indexName;
		} else {
			EXPECT_EQ(it->second.size(), 1);
			assertrx(!it->second.empty());
			return it->second[0].name;
		}
	}

	static bool checkDistincts(reindexer::Item& item, const reindexer::Query& qr,
							   std::unordered_map<std::string, std::unordered_set<std::string>>& distincts,
							   const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
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
						const std::unordered_map<std::string, std::vector<FieldData>>& leftIndexesFields,
						const std::unordered_map<std::string, std::vector<FieldData>>& rightIndexesFields) {
		for (auto it : joinedSelector.QueryResults()) {
			const reindexer::Item& rightItem = it.GetItem(false);
			bool result = true;
			const auto& joinEntries{joinedSelector.JoinQuery().joinEntries_};
			assertrx(!joinEntries.empty());
			assertrx(joinEntries[0].op_ != OpOr);
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
						assertrx(0);
				}
			}
			if (result) return true;
		}
		return false;
	}

	bool checkOnCondition(const reindexer::Item& leftItem, const reindexer::Item& rightItem, const std::string& leftIndexName,
						  const std::string& rightIndexName, CondType cond,
						  const std::unordered_map<std::string, std::vector<FieldData>>& leftIndexesFields,
						  const std::unordered_map<std::string, std::vector<FieldData>>& rightIndexesFields) {
		const CollateOpts& collate = indexesCollates[leftIndexName];
		const std::string leftFieldName = getFieldName(leftIndexName, leftIndexesFields);
		const std::string rightFieldName = getFieldName(rightIndexName, rightIndexesFields);
		const reindexer::VariantArray lFieldValues = leftItem[leftFieldName];
		const reindexer::VariantArray rFieldValues = rightItem[rightFieldName];
		for (const reindexer::Variant& lFieldValue : lFieldValues) {
			if (compareValue<BetweenFields::Yes>(lFieldValue, cond, rFieldValues, collate, reindexer::KeyValueType::Undefined{})) {
				return true;
			}
		}
		return false;
	}

	bool checkCondition(const reindexer::Item& item, const reindexer::QueryEntry& qentry,
						const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		EXPECT_GT(item.NumFields(), 0);
		if (isGeomConditions(qentry.condition)) {
			return checkGeomConditions(item, qentry, indexesFields);
		}
		const CollateOpts& collate = indexesCollates[qentry.index];

		if (isIndexComposite(item, qentry)) {
			return checkCompositeValues(item, qentry, collate, indexesFields);
		} else {
			std::string fieldName;
			reindexer::KeyValueType fieldType = reindexer::KeyValueType::Undefined{};
			if (const auto it = indexesFields.find(qentry.index); it == indexesFields.end()) {
				fieldName = qentry.index;
			} else {
				EXPECT_EQ(it->second.size(), 1);
				assertrx(!it->second.empty());
				fieldName = it->second[0].name;
				fieldType = it->second[0].type;
			}
			reindexer::VariantArray fieldValues = item[fieldName];
			switch (qentry.condition) {
				case CondEmpty:
					return fieldValues.size() == 0;
				case CondAny:
					return fieldValues.size() > 0;
				case CondAllSet:
					return checkAllSet<BetweenFields::No>(fieldValues, qentry.values, collate, fieldType);
				case CondEq:
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondRange:
				case CondSet:
				case CondLike:
				case CondDWithin:
					for (const reindexer::Variant& fieldValue : fieldValues) {
						if (compareValue<BetweenFields::No>(fieldValue, qentry.condition, qentry.values, collate, fieldType)) return true;
					}
			}
		}

		return false;
	}

	static bool isGeomConditions(CondType cond) noexcept { return cond == CondType::CondDWithin; }

	static bool checkGeomConditions(const reindexer::Item& item, const reindexer::QueryEntry& qentry,
									const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		assertrx(qentry.values.size() == 2);
		const reindexer::VariantArray coordinates = item[getFieldName(qentry.index, indexesFields)];
		if (coordinates.empty()) return false;
		assertrx(coordinates.size() == 2);
		const double x = coordinates[0].As<double>();
		const double y = coordinates[1].As<double>();
		if (qentry.condition == CondDWithin) {
			return DWithin(reindexer::Point{x, y}, qentry.values[0].As<reindexer::Point>(), qentry.values[1].As<double>());
		} else {
			assertrx(0);
			abort();
		}
	}

	static reindexer::VariantArray getValues(const reindexer::Item& item, const std::vector<FieldData>& fields) {
		reindexer::VariantArray kvalues;
		for (const auto& field : fields) {
			kvalues.push_back(item[field.name].operator reindexer::Variant());
		}
		return kvalues;
	}

	static std::vector<FieldData> getCompositeFields(const std::string& indexName,
													 const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		const auto it = indexesFields.find(indexName);
		assert(it != indexesFields.end());
		return it->second;
	}

	static bool checkCompositeValues(const reindexer::Item& item, const reindexer::QueryEntry& qentry, const CollateOpts& opts,
									 const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		const auto fields = getCompositeFields(qentry.index, indexesFields);

		const reindexer::VariantArray& indexesValues = getValues(item, fields);
		const reindexer::VariantArray& keyValues = qentry.values;

		switch (qentry.condition) {
			case CondEmpty:
				return indexesValues.size() == 0;
			case CondAny:
				return indexesValues.size() > 0;
			case CondGe:
				assert(!keyValues.empty());
				return compareCompositeValues(indexesValues, keyValues[0], opts) >= 0;
			case CondGt:
				assert(!keyValues.empty());
				return compareCompositeValues(indexesValues, keyValues[0], opts) > 0;
			case CondLt:
				assert(!keyValues.empty());
				return compareCompositeValues(indexesValues, keyValues[0], opts) < 0;
			case CondLe:
				assert(!keyValues.empty());
				return compareCompositeValues(indexesValues, keyValues[0], opts) <= 0;
			case CondRange:
				assert(keyValues.size() > 1);
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
				return !indexesValues.empty();
			case CondLike:
			case CondDWithin:
			default:
				std::abort();
		}
	}

	static int compareCompositeValues(const reindexer::VariantArray& indexesValues, const reindexer::Variant& keyValue,
									  const CollateOpts& opts) {
		reindexer::VariantArray compositeValues = keyValue.getCompositeValues();
		EXPECT_EQ(indexesValues.size(), compositeValues.size());
		int cmpRes = 0;
		for (size_t i = 0; i < indexesValues.size() && (cmpRes == 0); ++i) {
			compositeValues[i].convert(indexesValues[i].Type());
			cmpRes = indexesValues[i].RelaxCompare<reindexer::WithString::Yes>(compositeValues[i], opts);
		}
		return cmpRes;
	}

	enum class BetweenFields : bool { Yes = true, No = false };

	template <BetweenFields betweenFields>
	static int compare(const reindexer::Variant& v, const reindexer::Variant& k, const CollateOpts& opts,
					   reindexer::KeyValueType fieldType) {
		if constexpr (betweenFields == BetweenFields::Yes) {
			return v.RelaxCompare<reindexer::WithString::Yes>(k, opts);
		} else {
			return v.RelaxCompare<reindexer::WithString::Yes>(
				k.convert(fieldType.Is<reindexer::KeyValueType::Undefined>() ? v.Type() : fieldType), opts);
		}
	}

	template <BetweenFields betweenFields>
	static bool checkAllSet(const reindexer::VariantArray& values, const reindexer::VariantArray& keys, const CollateOpts& opts,
							reindexer::KeyValueType fieldType) {
		try {
			for (const auto& k : keys) {
				bool found = false;
				for (const auto& v : values) {
					if (compare<betweenFields>(v, k, opts, fieldType) == 0) {
						found = true;
						break;
					}
				}
				if (!found) {
					return false;
				}
			}
			return !keys.empty();
		} catch (const reindexer::Error& err) {
			if (err.code() == errParams && reindexer::checkIfStartsWith("Can't convert ", err.what())) {
				return false;
			} else {
				throw;
			}
		}
	}

	template <BetweenFields betweenFields>
	static bool compareValue(const reindexer::Variant& value, CondType condition, const reindexer::VariantArray& keys,
							 const CollateOpts& opts, reindexer::KeyValueType fieldType) {
		try {
			switch (condition) {
				case CondGe:
				case CondGt:
				case CondLt:
				case CondLe: {
					const size_t size = betweenFields == BetweenFields::Yes ? keys.size() : 1;
					assert(keys.size() >= size);
					for (size_t i = 0; i < size; ++i) {
						const auto cmp = compare<betweenFields>(value, keys[i], opts, fieldType);
						switch (condition) {
							case CondGe:
								if (cmp >= 0) {
									return true;
								} else {
									break;
								}
							case CondGt:
								if (cmp > 0) {
									return true;
								} else {
									break;
								}
							case CondLt:
								if (cmp < 0) {
									return true;
								} else {
									break;
								}
							case CondLe:
								if (cmp <= 0) {
									return true;
								} else {
									break;
								}
							case CondAny:
							case CondEmpty:
							case CondDWithin:
							case CondEq:
							case CondSet:
							case CondLike:
							case CondRange:
							case CondAllSet:
								assert(0);
						}
					}
					return false;
				}
				case CondRange:
					assert(keys.size() > 1);
					return compare<betweenFields>(value, keys[0], opts, fieldType) >= 0 &&
						   compare<betweenFields>(value, keys[1], opts, fieldType) <= 0;
				case CondEq:
				case CondSet:
					for (const reindexer::Variant& kv : keys) {
						if (compare<betweenFields>(value, kv, opts, fieldType) == 0) return true;
					}
					return false;
				case CondLike:
					if (!value.Type().Is<reindexer::KeyValueType::String>()) {
						return false;
					}
					assert(!keys.empty());
					return isLikeSqlPattern(value.As<std::string>(), keys[0].convert(reindexer::KeyValueType::String{}).As<std::string>());
				case CondAny:
				case CondAllSet:
				case CondEmpty:
				case CondDWithin:
				default:
					std::abort();
			}
		} catch (const reindexer::Error& err) {
			if (err.code() == errParams && reindexer::checkIfStartsWith("Can't convert ", err.what())) {
				return false;
			} else {
				throw;
			}
		}
	}

	static bool isLikeSqlPattern(std::string str, std::string pattern) {
		return std::regex_match(str, std::regex{reindexer::sqlLikePattern2ECMAScript(std::move(pattern))});
	}

	bool checkCompositeCondition(const reindexer::Item& item, const reindexer::BetweenFieldsQueryEntry& qentry,
								 const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		const auto firstFields = getCompositeFields(qentry.firstIndex, indexesFields);
		const auto secondFields = getCompositeFields(qentry.secondIndex, indexesFields);
		assertrx(firstFields.size() == secondFields.size());

		reindexer::BetweenFieldsQueryEntry qe{qentry};
		for (size_t i = 0; i < firstFields.size(); ++i) {
			qe.firstIndex = firstFields[i].name;
			qe.secondIndex = secondFields[i].name;
			if (!checkCondition(item, qe, indexesFields)) return false;
		}
		return !firstFields.empty();
	}

	static bool isIndexComposite(const reindexer::BetweenFieldsQueryEntry& qe,
								 const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		if (qe.firstIndex.find('+') != std::string::npos || qe.secondIndex.find('+') != std::string::npos) return true;
		if (const auto it = indexesFields.find(qe.firstIndex); it != indexesFields.end() && it->second.size() > 1) return true;
		if (const auto it = indexesFields.find(qe.secondIndex); it != indexesFields.end() && it->second.size() > 1) return true;
		return false;
	}

	static bool isIndexComposite(const reindexer::Item& item, const reindexer::QueryEntry& qentry) {
		if (qentry.values.empty()) return false;
		if (qentry.idxNo < 0) {
			return qentry.values.size() && (qentry.values[0].Type().Is<reindexer::KeyValueType::Composite>() ||
											qentry.values[0].Type().Is<reindexer::KeyValueType::Tuple>());
		}
		const auto indexType = item.GetIndexType(qentry.idxNo);
		return indexType.Is<reindexer::KeyValueType::Composite>() || indexType.Is<reindexer::KeyValueType::Tuple>();
	}

	bool checkCondition(const reindexer::Item& item, const reindexer::BetweenFieldsQueryEntry& qentry,
						const std::unordered_map<std::string, std::vector<FieldData>>& indexesFields) {
		EXPECT_GT(item.NumFields(), 0);
		assertrx(!isGeomConditions(qentry.Condition()));

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
				return checkAllSet<BetweenFields::Yes>(lValues, rValues, collate, reindexer::KeyValueType::Undefined{});
			case CondRange:
			case CondLike:
			case CondAny:
			case CondEq:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondSet:
			case CondEmpty:
			case CondDWithin:
				for (const reindexer::Variant& lv : lValues) {
					if (compareValue<BetweenFields::Yes>(lv, qentry.Condition(), rValues, collate, reindexer::KeyValueType::Undefined{})) {
						return true;
					}
				}
				return false;
			default:
				abort();
		}
	}

	static inline double distance(reindexer::Point p1, reindexer::Point p2) noexcept {
		return std::sqrt((p1.X() - p2.X()) * (p1.X() - p2.X()) + (p1.Y() - p2.Y()) * (p1.Y() - p2.Y()));
	}

	static reindexer::VariantArray getJoinedField(int id, const reindexer::QueryResults& qr, size_t nsIdx, int index,
												  std::string_view column) noexcept {
		const reindexer::joins::ItemIterator itemIt{&qr.joined_[0], id};
		const auto joinedIt = itemIt.at(nsIdx);
		assertrx(joinedIt.ItemsCount() == 1);
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
										  reindexer::Item& item, const reindexer::QueryResults& qr) {
		double result = 0.0;
		assertrx(begin != end);
		assertrx(begin->operation.op == OpPlus);
		for (auto it = begin; it != end; ++it) {
			double value = it->InvokeAppropriate<double>(
				[&it, &item, &qr](const reindexer::SortExpressionBracket&) {
					return calculateSortExpression(it.cbegin(), it.cend(), item, qr);
				},
				[](const reindexer::SortExprFuncs::Value& v) { return v.value; },
				[&item](const reindexer::SortExprFuncs::Index& i) { return item[i.column].As<double>(); },
				[&item, &qr](const reindexer::SortExprFuncs::JoinedIndex& i) {
					const auto values = getJoinedField(item.GetID(), qr, i.nsIdx, i.index, i.column);
					assertrx(values.size() == 1);
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
					assertrx(value != 0.0);
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
			jq.count = reindexer::QueryEntry::kDefaultLimit;
			jq.start = reindexer::QueryEntry::kDefaultOffset;
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
		assertrx(it != ns2pk_.end());
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

	static void printFailedSortOrder(const reindexer::Query& query, const reindexer::QueryResults& qr, int itemIndex,
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
	std::unordered_map<std::string, std::unordered_map<std::string, std::vector<FieldData>>> indexesFields_;
};
