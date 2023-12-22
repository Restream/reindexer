#include "selector_plan_test.h"
#include <thread>

template <>
std::string SelectorPlanTest::readFieldValue<std::string>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	assertrx(str[pos] == '"');
	++pos;
	assertrx(pos < str.size());
	const std::string::size_type end = str.find('"', pos);
	assertrx(end != std::string::npos);
	return str.substr(pos, end - pos);
}

template <>
bool SelectorPlanTest::readFieldValue<bool>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	if (reindexer::checkIfStartsWith("true", str.substr(pos))) {
		return true;
	} else if (reindexer::checkIfStartsWith("false", str.substr(pos))) {
		return false;
	} else {
		assertrx(0);
	}
	return false;
}

template <>
int SelectorPlanTest::readFieldValue<int>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	const std::string::size_type end = str.find_first_not_of("+-0123456789", pos);
	assertrx(end != std::string::npos);
	assertrx(end != pos);
	return std::stoi(str.substr(pos, end - pos));
}

template <>
int64_t SelectorPlanTest::readFieldValue<int64_t>(const std::string& str, std::string::size_type pos) {
	pos = findFieldValueStart(str, pos);
	const std::string::size_type end = str.find_first_not_of("+-0123456789", pos);
	assertrx(end != std::string::npos);
	assertrx(end != pos);
	return std::stoll(str.substr(pos, end - pos));
}

TEST_F(SelectorPlanTest, SortByBtreeIndex) {
	FillNs(btreeNs);
	AwaitIndexOptimization(btreeNs);
	for (const char* searchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
		const bool searchByBtreeField = (searchField == kFieldTree1 || searchField == kFieldTree2);
		for (CondType cond : {CondLt, CondLe, CondGt, CondGe}) {
			{
				reindexer::QueryResults qr;
				const Query query{Query(btreeNs).Explain().Where(searchField, cond, RandInt())};
				Error err = rt.reindexer->Select(query, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				const std::string& explain = qr.GetExplainResults();
				// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

				ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
				if (searchByBtreeField) {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
					const auto matched = GetJsonFieldValues<int>(explain, "matched");
					ASSERT_EQ(1, matched.size());
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {matched[0] == 0 ? "OnlyComparator" : "SingleRange"}));
				} else {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "OnlyComparator"}));
				}
			}

			for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				for (const Query& query :
					 {Query(btreeNs).Explain().Where(additionalSearchField, CondEq, RandInt()).Where(searchField, cond, RandInt()),
					  Query(btreeNs).Explain().Where(searchField, cond, RandInt()).Where(additionalSearchField, CondEq, RandInt())}) {
					reindexer::QueryResults qr;
					Error err = rt.reindexer->Select(query, qr);
					ASSERT_TRUE(err.ok()) << err.what();
					const std::string& explain = qr.GetExplainResults();
					// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
					const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
					ASSERT_EQ(2, cost.size());
					ASSERT_LE(cost[0], cost[1]);
					if (searchByBtreeField) {
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 0}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
					} else {
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "OnlyComparator"}));
					}
				}
			}

			for (const char* sortField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				const bool sortByBtreeField = (sortField == kFieldTree1 || sortField == kFieldTree2);
				for (bool desc : {true, false}) {
					{
						reindexer::QueryResults qr;
						const Query query{Query(btreeNs).Explain().Where(searchField, cond, RandInt()).Sort(sortField, desc)};
						Error err = rt.reindexer->Select(query, qr);
						ASSERT_TRUE(err.ok()) << err.what();
						const std::string& explain = qr.GetExplainResults();
						// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
						if (sortByBtreeField) {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
								ASSERT_NO_FATAL_FAILURE(
									AssertJsonFieldEqualTo(explain, "type", {desc ? "RevSingleRange" : "SingleRange", "OnlyComparator"}));
							}
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "OnlyComparator"}));
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
							reindexer::QueryResults qr;
							Error err = rt.reindexer->Select(query, qr);
							ASSERT_TRUE(err.ok()) << err.what();
							const std::string& explain = qr.GetExplainResults();
							// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortByBtreeField ? sortField : "-"}));
							const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
							ASSERT_EQ(2, cost.size());
							ASSERT_LE(cost[0], cost[1]);
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 0}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
								if (sortByBtreeField) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(
										explain, "type", {desc ? "RevSingleIdset" : "SingleIdset", "OnlyComparator"}));
								} else {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "OnlyComparator"}));
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
				reindexer::QueryResults qr;
				const Query query{Query(unbuiltBtreeNs).Explain().Where(searchField, cond, RandInt())};
				Error err = rt.reindexer->Select(query, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				const std::string& explain = qr.GetExplainResults();
				// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

				ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {searchByBtreeField}));
				if (searchByBtreeField) {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
					const auto matched = GetJsonFieldValues<int>(explain, "matched");
					ASSERT_EQ(1, matched.size());
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
					ASSERT_NO_FATAL_FAILURE(
						AssertJsonFieldEqualTo(explain, "type", {matched[0] == 0 ? "OnlyComparator" : "UnbuiltSortOrdersIndex"}));
				} else {
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "OnlyComparator"}));
				}
			}

			for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				for (const Query& query :
					 {Query(unbuiltBtreeNs).Explain().Where(additionalSearchField, CondEq, RandInt()).Where(searchField, cond, RandInt()),
					  Query(unbuiltBtreeNs)
						  .Explain()
						  .Where(searchField, cond, RandInt())
						  .Where(additionalSearchField, CondEq, RandInt())}) {
					reindexer::QueryResults qr;
					Error err = rt.reindexer->Select(query, qr);
					ASSERT_TRUE(err.ok()) << err.what();
					const std::string& explain = qr.GetExplainResults();
					// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

					const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
					ASSERT_EQ(2, cost.size());
					ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
					if (searchByBtreeField) {
						const auto sortByUnbuiltIndex = GetJsonFieldValues<bool>(explain, "sort_by_uncommitted_index");
						ASSERT_EQ(1, sortByUnbuiltIndex.size());
						if (sortByUnbuiltIndex[0]) {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField, additionalSearchField}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {searchField}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {additionalSearchField, searchField}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 0}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
						}
					} else {
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {additionalSearchField, searchField}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
						ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "OnlyComparator"}));
					}
				}
			}

			for (const char* sortField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
				const bool sortByBtreeField = (sortField == kFieldTree1 || sortField == kFieldTree2);
				for (bool desc : {true, false}) {
					{
						reindexer::QueryResults qr;
						const Query query{Query(unbuiltBtreeNs).Explain().Where(searchField, cond, RandInt()).Sort(sortField, desc)};
						Error err = rt.reindexer->Select(query, qr);
						ASSERT_TRUE(err.ok()) << err.what();
						const std::string& explain = qr.GetExplainResults();
						// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

						const auto matched = GetJsonFieldValues<int>(explain, "matched");
						if (sortByBtreeField) {
							if (searchByBtreeField && (sortField == searchField || (matched.size() == 1))) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								if (matched[0] == 0) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"OnlyComparator"}));
									if (sortField == searchField) {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {true}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
									} else {
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
								ASSERT_NO_FATAL_FAILURE(
									AssertJsonFieldEqualTo(explain, "type", {"UnbuiltSortOrdersIndex", "OnlyComparator"}));
							}
						} else {
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
							if (searchByBtreeField) {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index"}));
							} else {
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "field", {"-scan", searchField}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "items", {kNsSize}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {1}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"scan", "scan"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleRange", "OnlyComparator"}));
							}
						}
					}

					for (const char* additionalSearchField : {kFieldId, kFieldTree1, kFieldTree2, kFieldHash}) {
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
							reindexer::QueryResults qr;
							Error err = rt.reindexer->Select(query, qr);
							ASSERT_TRUE(err.ok()) << err.what();
							const std::string& explain = qr.GetExplainResults();
							// TestCout() << query.GetSQL() << '\n' << explain << std::endl;

							const auto cost = GetJsonFieldValues<int64_t>(explain, "cost");
							ASSERT_EQ(2, cost.size());
							ASSERT_NO_FATAL_FAILURE(AssertJsonFieldAbsent(explain, "items"));
							if (sortByBtreeField) {
								const auto sortByUnbuiltIndex = GetJsonFieldValues<bool>(explain, "sort_by_uncommitted_index");
								ASSERT_EQ(1, sortByUnbuiltIndex.size());
								if (sortByUnbuiltIndex[0]) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {sortField}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
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
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 0}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
									} else {
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
										ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
									}
								}
							} else {
								ASSERT_LE(cost[0], cost[1]);
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_index", {"-"}));
								ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "sort_by_uncommitted_index", {false}));
								if (searchByBtreeField) {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 0}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "index"}));
								} else {
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "comparators", {0, 1}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "method", {"index", "scan"}));
									ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(explain, "type", {"SingleIdset", "OnlyComparator"}));
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
	Error err = rt.reindexer->OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	for (int id = 0; id < 20; ++id) {
		Item item = rt.reindexer->NewItem(nsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		item["id"] = id;
		item["value"] = 123;
		Upsert(nsName, item);
	}

	{
		// Query without intersection in CondEq/CondSet values
		const auto q = Query(nsName)
						   .Where("id", CondEq, 31)
						   .Where("id", CondSet, {32, 33, 34})
						   .Where("id", CondEq, 310)
						   .Where("id", CondSet, {35, 36, 37})
						   .Where("value", CondAny, VariantArray{})
						   .Explain();
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 0);
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "field", {"id", "value"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "keys", {0, 0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "matched", {0, 0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "method", {"index", "scan"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "type", {"OnlyComparator", "OnlyComparator"}));
	}
	{
		// Query with empty set
		const auto q = Query(nsName)
						   .Where("id", CondEq, 39)
						   .Where("id", CondSet, {32, 39, 34})
						   .Where("id", CondSet, VariantArray{})
						   .Where("value", CondAny, VariantArray{})
						   .Explain();
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 0);
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "field", {"id", "value"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "keys", {0, 0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "matched", {0, 0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "method", {"index", "scan"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "type", {"OnlyComparator", "OnlyComparator"}));
	}
	{
		// Query with multiple empty sets
		const auto q = Query(nsName)
						   .Where("id", CondEq, 45)
						   .Where("id", CondSet, VariantArray{})
						   .Where("id", CondSet, VariantArray{})
						   .Where("value", CondAny, VariantArray{})
						   .Explain();
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 0);
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "field", {"id", "value"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "keys", {0, 0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "matched", {0, 0}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "method", {"index", "scan"}));
		ASSERT_NO_FATAL_FAILURE(AssertJsonFieldEqualTo(qr.GetExplainResults(), "type", {"OnlyComparator", "OnlyComparator"}));
	}
}
