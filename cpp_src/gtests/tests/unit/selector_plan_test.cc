#include "selector_plan_test.h"
#include "core/enums.h"
#include "core/index/index.h"
#include "json_helpers.h"

namespace reindexer_tests {

using namespace json_helpers;
using reindexer::IndexOpts;
using reindexer::VariantArray;

TEST_F(SelectorPlanTest, SortByBtreeIndex) {
	FillNs(btreeNs);
	AwaitIndexOptimization(btreeNs);
	for (const char* searchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
		const bool searchByBtreeField = (searchField == kFieldTree1 || searchField == kFieldTree2);
		for (CondType cond : {CondLt, CondLe, CondGt, CondGe}) {
			{
				const Query query{Query(btreeNs).Explain().Where(searchField, cond, RandInt())};
				auto qr = rt.Select(query);
				const std::string& explain = qr.GetExplainResults();
				// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

				ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
				if (searchByBtreeField) {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
					const auto matched = GetJsonFieldValues<int>(explain, "matched");
					ASSERT_EQ(1, matched.size());
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {matched[0] == 0 ? "Forward" : "SingleRange"}));
				} else {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {scanMethods, scanMethods}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "Comparator"}));
				}
			}

			for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				for (const Query& query :
					 {Query(btreeNs).Explain().Where(additionalSearchField, CondEq, RandInt()).Where(searchField, cond, RandInt()),
					  Query(btreeNs).Explain().Where(searchField, cond, RandInt()).Where(additionalSearchField, CondEq, RandInt())}) {
					auto qr = rt.Select(query);
					const std::string& explain = qr.GetExplainResults();
					// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
					const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
					if (additionalSearchField != searchField) {
						ASSERT_EQ(2, cost.size());
						ASSERT_LE(cost[0], cost[1]);
						if (searchByBtreeField) {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods, indexMethods}));
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods, scanMethods}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "Comparator"}));
						}
					}
				}
			}

			for (const char* sortField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				const bool sortByBtreeField = (sortField == kFieldTree1 || sortField == kFieldTree2);
				for (const auto sortOrder : {SortOrder::Asc, SortOrder::Desc}) {
					{
						const Query query{Query(btreeNs).Explain().Where(searchField, cond, RandInt()).Sort(sortField, sortOrder)};
						auto qr = rt.Select(query);
						const std::string& explain = qr.GetExplainResults();
						// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
						if (sortByBtreeField) {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {scanMethods, scanMethods}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(
									explain, "type", {sortOrder == SortOrder::Desc ? "RevSingleRange" : "SingleRange", "Comparator"}));
							}
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {scanMethods, scanMethods}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "Comparator"}));
							}
						}
					}

					const std::unordered_set<std::string> btreeMethods{"index", "index(cached)"};
					for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
						for (const Query& query : {Query(btreeNs)
													   .Explain()
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Where(searchField, cond, RandInt())
													   .Sort(sortField, sortOrder),
												   Query(btreeNs)
													   .Explain()
													   .Where(searchField, cond, RandInt())
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Sort(sortField, sortOrder)}) {
							auto qr = rt.Select(query);
							const std::string& explain = qr.GetExplainResults();
							// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortByBtreeField ? sortField : "-"}));
							const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
							if (additionalSearchField != searchField) {
								ASSERT_EQ(2, cost.size());
								ASSERT_LE(cost[0], cost[1]);
								if (searchByBtreeField) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods, indexMethods}));
								} else {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualToOneOf(explain, "method", {indexMethods, scanMethods}));
									if (sortByBtreeField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(
											explain, "type",
											{sortOrder == SortOrder::Desc ? "RevSingleIdset" : "SingleIdset", "Comparator"}));
									} else {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "Comparator"}));
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

TEST_F(SelectorPlanTest, SortByUnbuiltBtreeIndex) {
	FillNs(unbuiltBtreeNs);

	for (const char* searchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
		const bool searchByBtreeField = (searchField == kFieldTree1 || searchField == kFieldTree2);
		for (CondType cond : {CondLt, CondLe, CondGt, CondGe}) {
			{
				const Query query{Query(unbuiltBtreeNs).Explain().Where(searchField, cond, RandInt())};
				SCOPED_TRACE(query.GetSQL());
				auto qr = rt.Select(query);
				const std::string& explain = qr.GetExplainResults();
				// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

				ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {searchByBtreeField}));
				if (searchByBtreeField) {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
					const auto matched = GetJsonFieldValues<int>(explain, "matched");
					ASSERT_EQ(1, matched.size());
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
					ASSERT_NO_FATAL_FAILURE(
						AssertJsonFieldEqualTo(explain, "type", {matched[0] == 0 ? "Forward" : "UnbuiltSortOrdersIndex"}));
				} else {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "Comparator"}));
				}
			}

			for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				if (additionalSearchField == searchField) {
					continue;
				}
				for (const Query& query : {Query(unbuiltBtreeNs)
											   .Explain()
											   .Where(additionalSearchField, CondEq, RandInt())
											   .Where(searchField, cond, 0 /*RandInt()*/),
										   Query(unbuiltBtreeNs)
											   .Explain()
											   .Where(searchField, cond, RandInt())
											   .Where(additionalSearchField, CondEq, RandInt())}) {
					SCOPED_TRACE(query.GetSQL());
					auto qr = rt.Select(query);
					const std::string& explain = qr.GetExplainResults();
					// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

					const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
					ASSERT_EQ(2, cost.size());
					if (searchByBtreeField) {
						const auto sortByUnbuiltIndex = GetJsonFieldValues<bool>(explain, "sort_by_uncommitted_index");
						ASSERT_EQ(1, sortByUnbuiltIndex.size());
						if (sortByUnbuiltIndex[0]) {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField, additionalSearchField}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
						}
					} else {
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {additionalSearchField, searchField}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "Comparator"}));
					}
				}
			}

			for (const char* sortField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				const bool sortByBtreeField = (sortField == kFieldTree1 || sortField == kFieldTree2);
				for (const auto sortOrder : {SortOrder::Desc, SortOrder::Asc}) {
					{
						const Query query{Query(unbuiltBtreeNs).Explain().Where(searchField, cond, RandInt()).Sort(sortField, sortOrder)};
						SCOPED_TRACE(query.GetSQL());
						auto qr = rt.Select(query);
						const std::string& explain = qr.GetExplainResults();
						// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

						const auto matched = GetJsonFieldValues<int>(explain, "matched");
						if (sortByBtreeField) {
							if (searchByBtreeField && (sortField == searchField || (matched.size() == 1))) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								if (matched[0] == 0) {
									if (sortField == searchField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(
											explain, "type", {sortOrder == SortOrder::Desc ? "Reverse" : "Forward"}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
									} else {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"Forward"}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
									}
								} else {
									if (sortField == searchField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"UnbuiltSortOrdersIndex"}));
									} else {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
									}
								}
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"UnbuiltSortOrdersIndex", "Comparator"}));
							}
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "Comparator"}));
							}
						}
					}

					for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
						if (additionalSearchField == searchField) {
							continue;
						}
						for (const Query& query : {Query(unbuiltBtreeNs)
													   .Explain()
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Where(searchField, cond, RandInt())
													   .Sort(sortField, sortOrder),
												   Query(unbuiltBtreeNs)
													   .Explain()
													   .Where(searchField, cond, RandInt())
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Sort(sortField, sortOrder)}) {
							SCOPED_TRACE(query.GetSQL());
							auto qr = rt.Select(query);
							const std::string& explain = qr.GetExplainResults();

							const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
							ASSERT_EQ(2, cost.size());
							if (sortByBtreeField) {
								const auto sortByUnbuiltIndex = GetJsonFieldValues<bool>(explain, "sort_by_uncommitted_index");
								ASSERT_EQ(1, sortByUnbuiltIndex.size());
								if (sortByUnbuiltIndex[0]) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
									if (sortField == additionalSearchField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {sortField, searchField}));
									} else {
										ASSERT_NO_FATAL_FAILURE(
											AssertJsonFieldEqualTo(explain, "field", {searchField, additionalSearchField}));
									}
								} else {
									ASSERT_LE(cost[0], cost[1]);
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
									if (searchByBtreeField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
									} else {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
									}
								}
							} else {
								ASSERT_LE(cost[0], cost[1]);
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
								if (searchByBtreeField) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "comparators"));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
								} else {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "Comparator"}));
								}
							}
						}
					}
				}
			}
		}
	}
}

TEST_F(SelectorPlanTest, ConditionsMergeIntoEmptyCondition) {
	// Check cases, when condition merge algorithm gets empty result sets multiple times in a row
	const std::string nsName{"conditions_merge_always_false"};
	rt.OpenNamespace(nsName);
	rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	for (int id = 0; id < 20; ++id) {
		Item item(rt.NewItem(nsName));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		item["id"] = id;
		item["value"] = 123;
		Upsert(nsName, item);
	}

	for (const Query& q : {Query(nsName)  // Query without intersection in CondEq/CondSet values
							   .Where("id", CondEq, 31)
							   .Where("id", CondSet, {32, 33, 34})
							   .Where("id", CondEq, 310)
							   .Where("id", CondSet, {35, 36, 37})
							   .Where("value", CondAny, VariantArray{})
							   .Explain(),
						   Query(nsName)  // Query with empty set
							   .Where("id", CondEq, 39)
							   .Where("id", CondSet, {32, 39, 34})
							   .Where("id", CondSet, VariantArray{})
							   .Where("value", CondAny, VariantArray{})
							   .Explain(),
						   Query(nsName)  // Query with multiple empty sets
							   .Where("id", CondEq, 45)
							   .Where("id", CondSet, VariantArray{})
							   .Where("id", CondSet, VariantArray{})
							   .Where("value", CondAny, VariantArray{})
							   .Explain()}) {
		auto qr = rt.Select(q);
		ASSERT_EQ(qr.Count(), 0);
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "field", {"always_false"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "keys", {1}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "matched", {0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "method", {"index"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "type", {"SingleRange"}));
	}
}

TEST_F(SelectorPlanTest, DistinctWithFilterOnUnbuiltBtreeIndex) {
	// Filter + distinct on the same unbuilt btree field should pack into a single UnbuiltSortOrdersIndex iterator.
	// Seed enough CondLt matches so the ordered Distinct≡Sort gate can prefer unbuilt without Limit.
	constexpr int kExtraMatchingRows = 200;
	std::vector<std::pair<int, int>> rows{{0, 1}, {1, -3}, {2, 10}, {3, 1}, {4, 0}, {5, 0}};
	rows.reserve(rows.size() + kExtraMatchingRows);
	const int distinctVals[] = {-3, 0, 1};
	for (int i = 0; i < kExtraMatchingRows; ++i) {
		rows.emplace_back(6 + i, distinctVals[i % 3]);
	}
	for (const auto& [id, data] : rows) {
		UpsertUnbuilt(id, IndexValues{.tree1 = data});
	}

	struct [[nodiscard]] Case {
		Query query;
		std::vector<std::string> expectedTypes;
		std::vector<int> expectedMatched;
	};

	// date < 10 excludes id=2; distinct(date) leaves {-3, 0, 1}, ordered by date
	const std::vector<int> expectedDates{-3, 0, 1};
	auto validate = [&](QueryResults& qr, const Case& tc) {
		ASSERT_EQ(qr.Count(), expectedDates.size());
		size_t i = 0;
		for (auto& it : qr) {
			auto item = it.GetItem();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			ASSERT_LT(i, expectedDates.size());
			EXPECT_EQ(item[kFieldTree1].As<int>(), expectedDates[i]) << "item: " << item.GetJSON();
			++i;
		}
		ASSERT_EQ(i, expectedDates.size());

		ASSERT_EQ(qr.GetAggregationResults().size(), 1);
		const auto& agg = qr.GetAggregationResults()[0];
		ASSERT_EQ(agg.GetType(), AggDistinct);
		ASSERT_EQ(agg.GetDistinctRowCount(), expectedDates.size());

		const std::string& explain = qr.GetExplainResults();
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {kFieldTree1}));
		EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type"), tc.expectedTypes) << explain;
		EXPECT_EQ(GetJsonFieldValues<int>(explain, "matched"), tc.expectedMatched) << explain;
	};

	const std::vector<Case> cases{
		{.query = Query(unbuiltBtreeNs).Explain().Distinct(kFieldTree1).Where(kFieldTree1, CondLt, 10).Sort(kFieldTree1, SortOrder::Asc),
		 .expectedTypes = {"UnbuiltSortOrdersIndex"},
		 .expectedMatched = {3}},
		{.query = Query(unbuiltBtreeNs).Explain().Distinct(kFieldTree1).Where(kFieldTree1, CondLt, 10),
		 .expectedTypes = {"UnbuiltSortOrdersIndex"},
		 .expectedMatched = {3}},
		// id=999 is absent → NOT (id = 999) is always true (extra QueryEntry on another index for heuristics).
		{.query = Query(unbuiltBtreeNs)
					  .Explain()
					  .Distinct(kFieldTree1)
					  .Where(kFieldTree1, CondLt, 10)
					  .Not()
					  .Where(kFieldId, CondEq, 999)
					  .Sort(kFieldTree1, SortOrder::Asc),
		 .expectedTypes = {"UnbuiltSortOrdersIndex", "Comparator"},
		 .expectedMatched = {3, 3}},
	};

	for (const Case& tc : cases) {
		SCOPED_TRACE(tc.query.GetSQL());
		auto qr = rt.Select(tc.query);
		EXPECT_NO_FATAL_FAILURE(validate(qr, tc));
	}

	// With multiple conditions ReqTotal forces the normal full-pass plan.
	{
		const Query totalQuery = Query(unbuiltBtreeNs)
									 .Explain()
									 .Distinct(kFieldTree1)
									 .Where(kFieldTree1, CondLt, 10)
									 .Where(kFieldTree2, CondGe, 0)
									 .ReqTotal()
									 .Limit(20);
		SCOPED_TRACE(totalQuery.GetSQL());
		auto qr = rt.Select(totalQuery);
		ASSERT_EQ(qr.Count(), expectedDates.size());
		ASSERT_EQ(qr.TotalCount(), expectedDates.size());
		const std::string& explain = qr.GetExplainResults();
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false})) << explain;
		EXPECT_NE(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
	}

	// A single unbuilt distinct iterator remains the best plan for ReqTotal, but its total must be calculated in the select loop:
	// the iterator's exact max-iterations probe counts row IDs, not unique keys.
	for (bool explicitSort : {false, true}) {
		Query totalQuery = Query(unbuiltBtreeNs).Explain().Distinct(kFieldTree1).Where(kFieldTree1, CondLt, 10);
		if (explicitSort) {
			totalQuery.Sort(kFieldTree1, SortOrder::Asc);
		}
		totalQuery.ReqTotal().Limit(2);
		SCOPED_TRACE(totalQuery.GetSQL());
		auto qr = rt.Select(totalQuery);
		ASSERT_EQ(qr.Count(), 2);
		ASSERT_EQ(qr.TotalCount(), expectedDates.size());
		const std::string& explain = qr.GetExplainResults();
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true})) << explain;
		EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
	}
}

TEST_F(SelectorPlanTest, MultiDistinctSortHeuristics) {
	struct [[nodiscard]] Case {
		const char* name;
		int hashModulo;
		int hashEq;
		std::pair<int, int> tree1Range;
		bool expectUnbuilt;
	};

	const std::vector<Case> cases{
		// Weak unordered Eq: without multi-distinct ignore AdviceSortingIndex aborts (2nd Distinct cannot be
		// Compatible); with ignore → implicit unbuilt on tree1.
		{.name = "weak_eq_allows_implicit_unbuilt", .hashModulo = 3, .hashEq = 1, .tree1Range = {10, 25}, .expectUnbuilt = true},
		// Stronger Eq: ignore must not force unbuilt when IsSortOptimizationEffective rejects it.
		{.name = "strong_eq_keeps_no_implicit_sort", .hashModulo = 10, .hashEq = 5, .tree1Range = {10, 16}, .expectUnbuilt = false},
	};

	for (const Case& tc : cases) {
		SCOPED_TRACE(tc.name);
		RefillUnbuilt(kNsSize, [&](int i) { return IndexValues{.tree1 = 10 + (i % 20), .tree2 = i % 7, .hash = i % tc.hashModulo}; });

		const Query query = Query(unbuiltBtreeNs)
								.Explain()
								.Distinct(kFieldTree1)
								.Distinct(kFieldTree2)
								.Where(kFieldHash, CondEq, tc.hashEq)
								.Where(kFieldTree1, CondRange, {tc.tree1Range.first, tc.tree1Range.second});

		auto qr = rt.Select(query);
		ASSERT_EQ(qr.Count(), 0);
		ASSERT_EQ(qr.GetAggregationResults().size(), 2);
		for (const auto& agg : qr.GetAggregationResults()) {
			ASSERT_EQ(agg.GetType(), AggDistinct);
		}

		const std::string& explain = qr.GetExplainResults();
		if (tc.expectUnbuilt) {
			ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {kFieldTree1})) << explain;
			ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true})) << explain;
			EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
		} else {
			ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"})) << explain;
			ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false})) << explain;
			EXPECT_NE(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
		}
	}
}

TEST_F(SelectorPlanTest, SingleDistinctCompatibleNotBlockedByUnorderedEq) {
	// Distinct on an ordered condition is Compatible advice; a selective hash Eq must not abort
	// AdviceSortingIndex (unlike Distinct that cannot lead unbuilt, e.g. CondAny-only / non-ordered).
	constexpr int kHashModulo = 3;
	RefillUnbuilt(kNsSize, [](int i) { return IndexValues{.tree1 = 10 + (i % 20), .tree2 = i % 7, .hash = i % kHashModulo}; });

	const Query query = Query(unbuiltBtreeNs)
							.Explain()
							.Distinct(kFieldTree1)
							.Where(kFieldHash, CondEq, 1)
							.Where(kFieldTree1, CondRange, {10, 25})
							.Limit(20);
	SCOPED_TRACE(query.GetSQL());
	auto qr = rt.Select(query);
	const std::string& explain = qr.GetExplainResults();
	ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {kFieldTree1})) << explain;
	ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true})) << explain;
	EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
}

TEST_F(SelectorPlanTest, SingleDistinctCompatibleBlockedByHighlySelectiveAlternative) {
	constexpr int kRows = 400;
	RefillUnbuilt(kRows, [](int i) { return IndexValues{.tree1 = i, .tree2 = i % 7, .hash = 0}; });

	auto expectNoUnbuilt = [&](const Query& query, size_t expectedCount) {
		SCOPED_TRACE(query.GetSQL());
		auto qr = rt.Select(query);
		ASSERT_EQ(qr.Count(), expectedCount);

		const std::string& explain = qr.GetExplainResults();
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false})) << explain;
		const auto types = GetJsonFieldValues<std::string>(explain, "type");
		EXPECT_EQ(std::find(types.begin(), types.end(), "UnbuiltSortOrdersIndex"), types.end()) << explain;
	};

	for (const Query& query : {Query(unbuiltBtreeNs)
								   .Explain()
								   .Distinct(kFieldTree1)
								   .Where(kFieldId, CondEq, kRows - 1)
								   .Where(kFieldTree1, CondRange, {0, kRows - 1})
								   .Limit(20),
							   Query(unbuiltBtreeNs)
								   .Explain()
								   .Distinct(kFieldTree1)
								   .Where(kFieldTree1, CondRange, {0, kRows - 1})
								   .Where(kFieldId, CondEq, kRows - 1)
								   .Limit(20)}) {
		expectNoUnbuilt(query, 1);
	}

	VariantArray ids;
	for (int i = 0; i < 15; ++i) {
		ids.emplace_back(i);
	}
	const Query largeOffsetQuery = Query(unbuiltBtreeNs)
									   .Explain()
									   .Distinct(kFieldTree1)
									   .Where(kFieldId, CondSet, std::move(ids))
									   .Where(kFieldTree1, CondRange, {0, kRows - 1})
									   .Offset(std::numeric_limits<int>::max() - 10)
									   .Limit(20);
	expectNoUnbuilt(largeOffsetQuery, 0);
}

TEST_F(SelectorPlanTest, AdviceSortingIndexSelectivityRanking) {
	// Wide Distinct CondLt must not beat a narrow Compatible Range on another tree.
	constexpr int kRows = 8000;
	constexpr int kTree1Uniques = 80;
	static_assert(kTree1Uniques > reindexer::kAdviceOrderedConditionProbeKeyCap);
	constexpr int kTree2Uniques = 40;
	RefillUnbuilt(kRows, [](int i) { return IndexValues{.tree1 = i % kTree1Uniques, .tree2 = i % kTree2Uniques, .hash = 0}; });

	// tree1 CondLt spans 80 keys (>cap) -> incomplete estimate; tree2 Range {5,5} -> 1 complete key.
	const Query query = Query(unbuiltBtreeNs)
							.Explain()
							.Distinct(kFieldTree1)
							.Where(kFieldTree1, CondLt, kTree1Uniques)
							.Where(kFieldTree2, CondRange, {5, 5})
							.Limit(20);
	SCOPED_TRACE(query.GetSQL());
	auto qr = rt.Select(query);
	const std::string& explain = qr.GetExplainResults();
	ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {kFieldTree2})) << explain;
	ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true})) << explain;
	EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
}

TEST_F(SelectorPlanTest, AdviceSortingIndexRanksIdsOrKeysForDistinct) {
	constexpr int kRows = 3000;
	RefillUnbuilt(kRows, [](int i) { return IndexValues{.tree1 = i < 1000 ? 0 : i - 999, .tree2 = i / 20, .hash = 0}; });

	// tree1: 1 key / 2000 IDs; tree2: 30 keys / 600 IDs.
	for (const auto& [query, expectedSortIndex] :
		 {std::pair{Query(unbuiltBtreeNs).Explain().Where(kFieldTree1, CondRange, {0, 0}).Where(kFieldTree2, CondRange, {0, 29}).Limit(20),
					kFieldTree2},
		  std::pair{Query(unbuiltBtreeNs)
						.Explain()
						.Distinct(kFieldTree1)
						.Where(kFieldTree1, CondRange, {0, 0})
						.Where(kFieldTree2, CondRange, {0, 29})
						.Limit(20),
					kFieldTree1}}) {
		SCOPED_TRACE(query.GetSQL());
		auto qr = rt.Select(query);
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "sort_index", {expectedSortIndex}));
	}
}

TEST_F(SelectorPlanTest, AdviceSortingIndexFallsBackToLargerIndex) {
	constexpr int kRows = 1000;
	RefillUnbuilt(kRows, [](int i) {
		return IndexValues{.tree1 = i < 60 ? i : 60 + (i % 40), .tree2 = i < 600 ? i % 60 : 60 + (i % 140), .hash = 0};
	});

	// Both ranges exceed the 50-key probing cap -> incomplete estimates. tree2 has the larger Index::Size
	// (200 unique keys vs 100), so the fallback must preserve the old larger-index preference.
	const Query query =
		Query(unbuiltBtreeNs).Explain().Where(kFieldTree1, CondRange, {0, 59}).Where(kFieldTree2, CondRange, {0, 59}).Limit(20);
	SCOPED_TRACE(query.GetSQL());
	auto qr = rt.Select(query);
	ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "sort_index", {kFieldTree2}));
}

TEST_F(SelectorPlanTest, UnbuiltSortIndexTotalCountFastPath) {
	// A lazy unbuilt iterator has only an O(1) planning upper bound. ReqTotal must use an explicit exact count-only probe rather than
	// treating that bound as cardinality or forcing a payload-level full select loop.
	constexpr int kRows = 500;
	constexpr int kRangeLo = 10;
	constexpr int kRangeHi = 100;
	RefillUnbuilt(kRows, [](int i) { return IndexValues{.tree1 = i % 120, .tree2 = i % 7, .hash = i % 5}; });

	const Query fullSelect = Query(unbuiltBtreeNs).Where(kFieldTree1, CondRange, {kRangeLo, kRangeHi}).Sort(kFieldTree1, SortOrder::Asc);
	auto fullQr = rt.Select(fullSelect);
	const int expectedTotal = fullQr.Count();
	ASSERT_GT(expectedTotal, 0);

	constexpr int kLimit = 20;
	const Query totalQuery = Query(unbuiltBtreeNs)
								 .Explain()
								 .Where(kFieldTree1, CondRange, {kRangeLo, kRangeHi})
								 .Sort(kFieldTree1, SortOrder::Asc)
								 .ReqTotal()
								 .Limit(kLimit);
	SCOPED_TRACE(totalQuery.GetSQL());
	auto totalQr = rt.Select(totalQuery);
	EXPECT_EQ(totalQr.TotalCount(), expectedTotal);
	EXPECT_EQ(totalQr.Count(), std::min(expectedTotal, kLimit));

	const std::string& explain = totalQr.GetExplainResults();
	ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true})) << explain;
	EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
}

TEST_F(SelectorPlanTest, UnbuiltSortIndexWithInnerJoinIteratorPlan) {
	constexpr int kRows = 3000;
	RefillUnbuilt(kRows, [](int i) { return IndexValues{.tree1 = i % 120, .tree2 = i % 7, .hash = i % 5}; });

	const Query query = Query(unbuiltBtreeNs)
							.Explain()
							.Distinct(kFieldTree1)
							.Where(kFieldTree1, CondRange, {10, 100})
							.Sort(kFieldTree1, SortOrder::Asc)
							.Limit(20)
							.InnerJoin(Query(unbuiltBtreeNs).Where(kFieldHash, CondEq, 0), kFieldId, CondEq, kFieldId);
	SCOPED_TRACE(query.GetSQL());
	auto qr = rt.Select(query);
	EXPECT_LE(qr.Count(), 20);

	const std::string& explain = qr.GetExplainResults();
	const auto unbuiltFlags = GetJsonFieldValues<bool>(explain, "sort_by_uncommitted_index");
	ASSERT_FALSE(unbuiltFlags.empty()) << explain;
	EXPECT_TRUE(unbuiltFlags.front()) << explain;
	EXPECT_EQ(GetJsonFieldValues<std::string>(explain, "type").front(), "UnbuiltSortOrdersIndex") << explain;
}

}  // namespace reindexer_tests
