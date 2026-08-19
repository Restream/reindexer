#include <limits>
#include "btree_idsets_api.h"
#include "core/id_type.h"
#include "core/index/index.h"
#include "core/index/string_map.h"
#include "core/nsselecter/btreeindexiterator.h"
#include "core/nsselecter/joins/item_context.h"
#include "core/nsselecter/joins/iterators.h"
#include "core/selectkeyresult.h"

namespace reindexer_tests {

TEST_F(BtreeIdsetsApi, SelectByStringField) {
	std::string strValueToCheck = lastStrValue;
	auto qr = rt.Select(Query(default_namespace).Not().Where(kFieldOne, CondEq, strValueToCheck));
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant kr = item[kFieldOne];
		EXPECT_TRUE(kr.Type().Is<reindexer::KeyValueType::String>());
		EXPECT_TRUE(kr.As<std::string>() != strValueToCheck);
	}
}

TEST_F(BtreeIdsetsApi, SelectByIntField) {
	const int boundaryValue = 5000;

	auto qr = rt.Select(Query(default_namespace).Where(kFieldTwo, CondGe, Variant(static_cast<int>(boundaryValue))));
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant kr = item[kFieldTwo];
		EXPECT_TRUE(kr.Type().Is<reindexer::KeyValueType::Int>());
		EXPECT_TRUE(static_cast<int>(kr) >= boundaryValue);
	}
}

TEST_F(BtreeIdsetsApi, SelectByBothFields) {
	const int boundaryValue = 50000;
	const std::string strValueToCheck = lastStrValue;
	const std::string strValueToCheck2 = "reindexer is fast";
	auto qr = rt.Select(Query(default_namespace)
							.Where(kFieldOne, CondLe, strValueToCheck2)
							.Not()
							.Where(kFieldOne, CondEq, strValueToCheck)
							.Where(kFieldTwo, CondGe, Variant(static_cast<int>(boundaryValue))));
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant krOne = item[kFieldOne];
		EXPECT_TRUE(krOne.Type().Is<reindexer::KeyValueType::String>());
		EXPECT_TRUE(strValueToCheck2.compare(krOne.As<std::string>()) > 0);
		EXPECT_TRUE(krOne.As<std::string>() != strValueToCheck);
		Variant krTwo = item[kFieldTwo];
		EXPECT_TRUE(krTwo.Type().Is<reindexer::KeyValueType::Int>());
		EXPECT_TRUE(static_cast<int>(krTwo) >= boundaryValue);
	}
}

TEST_F(BtreeIdsetsApi, SortByStringField) {
	auto qr = rt.Select(Query(default_namespace).Sort(kFieldOne, true));
	Variant prev;
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant curr = item[kFieldOne];
		if (it != qr.begin()) {
			EXPECT_TRUE(prev >= curr);
		}
		prev = curr;
	}
}

TEST_F(BtreeIdsetsApi, SortByIntField) {
	auto qr = rt.Select(Query(default_namespace).Sort(kFieldTwo, false));
	Variant prev;
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant curr = item[kFieldTwo];
		if (it != qr.begin()) {
			EXPECT_TRUE(prev.As<int>() <= curr.As<int>());
		}
		prev = curr;
	}
}

TEST_F(BtreeIdsetsApi, SortBySparseIndex) {
	for (const bool sortOrder : {true, false}) {
		auto qr = rt.Select(Query(default_namespace).Sort(kFieldFour, sortOrder));
		Variant prev;
		for (auto& it : qr) {
			Item item = it.GetItem(false);
			Variant curr = item[kFieldFour];
			if (it != qr.begin()) {
				if (!curr.IsNullValue() && !prev.IsNullValue()) {
					if (sortOrder) {
						EXPECT_TRUE(prev.As<int>() >= curr.As<int>());
					} else {
						EXPECT_TRUE(prev.As<int>() <= curr.As<int>());
					}
				}
			}
			prev = curr;
		}
	}
}

TEST_F(BtreeIdsetsApi, JoinSimpleNs) {
	Query joinedNs{Query(joinedNsName).Where(kFieldThree, CondGt, Variant(static_cast<int>(9000))).Sort(kFieldThree, false)};
	auto qr =
		rt.Select(Query(default_namespace, 0, 3000).InnerJoin(kFieldId, kFieldIdFk, CondEq, std::move(joinedNs)).Sort(kFieldTwo, false));
	Variant prevFieldTwo;
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		Variant currFieldTwo = item[kFieldTwo];
		if (it != qr.begin()) {
			EXPECT_TRUE(currFieldTwo.As<int>() >= prevFieldTwo.As<int>());
		}
		prevFieldTwo = currFieldTwo;

		Variant prevJoinedFk;
		auto joinedItemCtx = it.GetJoinedContext();
		auto& joinedItemIt = joinedItemCtx.iterator;
		reindexer::joins::FieldIterator joinedFieldIt = joinedItemIt.Begin();
		EXPECT_TRUE(joinedFieldIt.ItemsCount() > 0);
		auto qr = joinedFieldIt.ToQueryResults(joinedItemCtx);
		for (auto it : qr) {
			auto joinedItem = it.GetItem();
			Variant joinedFkCurr = joinedItem[kFieldIdFk];
			EXPECT_TRUE(joinedFkCurr == item[kFieldId]);
			if (it != qr.begin()) {
				EXPECT_TRUE(joinedFkCurr >= prevJoinedFk);
			}
			prevJoinedFk = joinedFkCurr;
		}
	}
}

TEST_F(ReindexerApi, BtreeUnbuiltIndexIteratorsTest) {
	reindexer::number_map<int64_t, reindexer::Index::KeyEntry> m1;
	reindexer::number_map<int64_t, reindexer::Index::KeyEntryPlain> m2;

	std::vector<reindexer::IdType> ids1, ids2;
	for (size_t i = 0; i < 10000; ++i) {
		auto it1 = m1.insert({i, reindexer::KeyEntry<reindexer::IdSet>()});
		for (int i = 0; i < rand() % 100 + 50; ++i) {
			const auto rowId = reindexer::IdType::FromNumber(i);
			it1.first->second.Unsorted().AddUnordered(rowId);
			ids1.push_back(rowId);
		}
		auto it2 = m2.insert({i, reindexer::KeyEntry<reindexer::IdSetPlain>()});
		for (int i = 0; i < rand() % 100 + 50; ++i) {
			const auto rowId = reindexer::IdType::FromNumber(i);
			it2.first->second.Unsorted().AddUnordered(rowId);
			ids2.push_back(rowId);
		}
	}

	reindexer::Index::KeyEntry emptyIdsKeyEntry;
	for (int i = 0; i < rand() % 100 + 50; ++i) {
		emptyIdsKeyEntry.Unsorted().AddUnordered(reindexer::IdType::FromNumber(i));
	}

	size_t pos = 0;
	reindexer::IdSet emptyIds{emptyIdsKeyEntry.Unsorted()};
	reindexer::IdSet::idset_iterator_range empty_ids_range{emptyIds.idset_range()};
	reindexer::IdSet::idset_iterator emptyIdsIt{empty_ids_range.begin()};
	reindexer::BtreeIndexIterator<typeof(m1)> bIt1(m1, emptyIds);
	bIt1.Start(false);
	while (pos < emptyIds.Size()) {
		const auto [ok, value] = bIt1.Next();
		ASSERT_TRUE(ok);
		EXPECT_EQ(value, *emptyIdsIt);
		++emptyIdsIt;
		++pos;
	}
	EXPECT_TRUE(pos == emptyIds.Size());

	pos = 0;
	for (auto [ok, value] = bIt1.Next(); ok; std::tie(ok, value) = bIt1.Next()) {
		EXPECT_EQ(value, ids1[pos]);
		++pos;
	}
	EXPECT_TRUE(pos == ids1.size());

	reindexer::BtreeIndexIterator<typeof(m2)> bIt2(m2, emptyIdsKeyEntry.Unsorted());
	bIt2.Start(true);
	pos = ids2.size() - 1;
	for (auto [ok, value] = bIt2.Next(); ok && pos; std::tie(ok, value) = bIt2.Next()) {
		EXPECT_EQ(value, ids2[pos]);
		if (pos) {
			--pos;
		}
	}
	EXPECT_TRUE(pos == 0);

	reindexer::IdSet::idset_reverse_iterator_range empty_ids_reverse_range{emptyIds.idset_reverse_range()};
	reindexer::IdSet::idset_reverse_iterator emptyIdsRit{empty_ids_reverse_range.begin()};

	pos = emptyIds.Size() - 1;
	for (auto [ok, value] = bIt2.Next(); ok; std::tie(ok, value) = bIt2.Next()) {
		EXPECT_EQ(value, *emptyIdsRit);
		if (pos) {
			--pos;
			++emptyIdsRit;
		}
	}
	EXPECT_TRUE(pos == 0);
}

TEST_F(ReindexerApi, BtreeUnbuiltIndexIteratorEstimates) {
	reindexer::number_map<int64_t, reindexer::Index::KeyEntryPlain> index;
	for (int64_t key = 0; key < 3; ++key) {
		auto [it, inserted] = index.insert({key, reindexer::Index::KeyEntryPlain{}});
		ASSERT_TRUE(inserted);
		it->second.Unsorted().AddUnordered(reindexer::IdType::FromNumber(2 * key));
		it->second.Unsorted().AddUnordered(reindexer::IdType::FromNumber(2 * key + 1));
	}

	constexpr size_t kNamespaceItems = 100;
	reindexer::BtreeIndexIterator<typeof(index)> iterator(index.begin(), index.end(), kNamespaceItems);

	auto estimate = iterator.GetPlanningEstimate();
	EXPECT_EQ(estimate.kind, reindexer::MaxIterationsEstimateKind::UpperBound);
	EXPECT_EQ(estimate.value, kNamespaceItems);

	estimate = iterator.ProbeMaxIterations(3);
	EXPECT_EQ(estimate.kind, reindexer::MaxIterationsEstimateKind::AtLeast);
	EXPECT_GE(estimate.value, 3);

	// A censored probe must not masquerade as exact planning cardinality.
	estimate = iterator.GetPlanningEstimate();
	EXPECT_EQ(estimate.kind, reindexer::MaxIterationsEstimateKind::UpperBound);
	EXPECT_EQ(estimate.value, kNamespaceItems);

	estimate = iterator.ProbeMaxIterations(kNamespaceItems);
	EXPECT_EQ(estimate.kind, reindexer::MaxIterationsEstimateKind::Exact);
	EXPECT_EQ(estimate.value, 6);

	estimate = iterator.GetPlanningEstimate();
	EXPECT_EQ(estimate.kind, reindexer::MaxIterationsEstimateKind::Exact);
	EXPECT_EQ(estimate.value, 6);

	auto complement = estimate.Complement(kNamespaceItems);
	EXPECT_EQ(complement.kind, reindexer::MaxIterationsEstimateKind::Exact);
	EXPECT_EQ(complement.value, kNamespaceItems - 6);

	complement = reindexer::MaxIterationsEstimate::UpperBound(6).Complement(kNamespaceItems);
	EXPECT_EQ(complement.kind, reindexer::MaxIterationsEstimateKind::UpperBound);
	EXPECT_EQ(complement.value, kNamespaceItems);

	complement = reindexer::MaxIterationsEstimate::Heuristic(6).Complement(kNamespaceItems);
	EXPECT_EQ(complement.kind, reindexer::MaxIterationsEstimateKind::UpperBound);
	EXPECT_EQ(complement.value, kNamespaceItems);
}

TEST_F(ReindexerApi, BtreeUnbuiltIndexIteratorExactProbeBeyondCostBarrier) {
	constexpr size_t kItems = 200'001;
	reindexer::number_map<int64_t, reindexer::Index::KeyEntryPlain> index;
	for (size_t key = 0; key < kItems; ++key) {
		auto [it, inserted] = index.insert({key, reindexer::Index::KeyEntryPlain{}});
		ASSERT_TRUE(inserted);
		it->second.Unsorted().AddUnordered(reindexer::IdType::FromNumber(key));
	}

	reindexer::IndexIterator::Ptr iterator(
		reindexer::make_intrusive<reindexer::BtreeIndexIterator<typeof(index)>>(index.begin(), index.end(), kItems));
	reindexer::SelectKeyResult result;
	result.emplace_back(std::move(iterator));

	EXPECT_EQ(result.EstimateMaxIterations(), std::numeric_limits<size_t>::max());
	EXPECT_EQ(result.GetPlanningEstimate().kind, reindexer::MaxIterationsEstimateKind::UpperBound);

	const auto exact = result.ProbeMaxIterations(std::numeric_limits<size_t>::max());
	EXPECT_EQ(exact.kind, reindexer::MaxIterationsEstimateKind::Exact);
	EXPECT_EQ(exact.value, kItems);
}

}  // namespace reindexer_tests
