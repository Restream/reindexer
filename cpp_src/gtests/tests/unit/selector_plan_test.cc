#include "selector_plan_test.h"
#include "json_helpers.h"

using namespace json_helpers;

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
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {matched[0] == 0 ? "Forward" : "SingleRange"}));
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
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "Comparator"}));
						}
					}
				}
			}

			for (const char* sortField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				const bool sortByBtreeField = (sortField == kFieldTree1 || sortField == kFieldTree2);
				for (bool desc : {true, false}) {
					{
						const Query query{Query(btreeNs).Explain().Where(searchField, cond, RandInt()).Sort(sortField, desc)};
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
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
								ASSERT_NO_FATAL_FAILURE(
									AssertJsonFieldEqualTo(explain, "type", {desc ? "RevSingleRange" : "SingleRange", "Comparator"}));
							}
						} else {
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
						for (const Query& query : {Query(btreeNs)
													   .Explain()
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Where(searchField, cond, RandInt())
													   .Sort(sortField, desc),
												   Query(btreeNs)
													   .Explain()
													   .Where(searchField, cond, RandInt())
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Sort(sortField, desc)}) {
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
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
								} else {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
									if (sortByBtreeField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(
											explain, "type", {desc ? "RevSingleIdset" : "SingleIdset", "Comparator"}));
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
				for (const Query& query :
					 {Query(unbuiltBtreeNs).Explain().Where(additionalSearchField, CondEq, RandInt()).Where(searchField, cond, RandInt()),
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
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {additionalSearchField, searchField}));
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
				for (bool desc : {true, false}) {
					{
						const Query query{Query(unbuiltBtreeNs).Explain().Where(searchField, cond, RandInt()).Sort(sortField, desc)};
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
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {desc ? "Reverse" : "Forward"}));
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
													   .Sort(sortField, desc),
												   Query(unbuiltBtreeNs)
													   .Explain()
													   .Where(searchField, cond, RandInt())
													   .Where(additionalSearchField, CondEq, RandInt())
													   .Sort(sortField, desc)}) {
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
