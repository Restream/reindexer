#include "btree_idsets_api.h"
#include "core/index/index.h"
#include "core/index/string_map.h"
#include "core/nsselecter/btreeindexiterator.h"
#include "core/queryresults/joinresults.h"

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
		auto itemIt = it.GetJoined();
		reindexer::joins::JoinedFieldIterator joinedFieldIt = itemIt.begin();
		EXPECT_TRUE(joinedFieldIt.ItemsCount() > 0);
		for (int j = 0; j < joinedFieldIt.ItemsCount(); ++j) {
			reindexer::ItemImpl joinedItem = joinedFieldIt.GetItem(j, qr.GetPayloadType(1), qr.GetTagsMatcher(1));
			Variant joinedFkCurr = joinedItem.GetField(qr.GetPayloadType(1).FieldByName(kFieldIdFk));
			EXPECT_TRUE(joinedFkCurr == item[kFieldId]);
			if (j != 0) {
				EXPECT_TRUE(joinedFkCurr >= prevJoinedFk);
			}
			prevJoinedFk = joinedFkCurr;
		}
	}
}

TEST_F(ReindexerApi, BtreeUnbuiltIndexIteratorsTest) {
	reindexer::number_map<int64_t, reindexer::Index::KeyEntry> m1;
	reindexer::number_map<int64_t, reindexer::Index::KeyEntryPlain> m2;

	std::vector<IdType> ids1, ids2;
	for (size_t i = 0; i < 10000; ++i) {
		auto it1 = m1.insert({i, reindexer::KeyEntry<reindexer::IdSet>()});
		for (int i = 0; i < rand() % 100 + 50; ++i) {
			std::ignore = it1.first->second.Unsorted().Add(IdType(i), reindexer::IdSet::Unordered, 1);
			ids1.push_back(i);
		}
		auto it2 = m2.insert({i, reindexer::KeyEntry<reindexer::IdSetPlain>()});
		for (int i = 0; i < rand() % 100 + 50; ++i) {
			std::ignore = it2.first->second.Unsorted().Add(IdType(i), reindexer::IdSet::Unordered, 1);
			ids2.push_back(i);
		}
	}

	reindexer::Index::KeyEntry emptyIdsKeyEntry;
	for (int i = 0; i < rand() % 100 + 50; ++i) {
		std::ignore = emptyIdsKeyEntry.Unsorted().Add(IdType(i), reindexer::IdSet::Unordered, 1);
	}

	size_t pos = 0;
	reindexer::base_idset_ptr emptyIds{emptyIdsKeyEntry.Unsorted().BuildBaseIdSet()};
	const reindexer::base_idset emptyIdsBaseIdSet{*emptyIds};

	reindexer::BtreeIndexIterator<typeof(m1)> bIt1(m1, std::move(emptyIds));
	bIt1.Start(false);
	while (pos < emptyIdsBaseIdSet.size() && bIt1.Next()) {
		EXPECT_TRUE(bIt1.Value() == emptyIdsBaseIdSet[pos])
			<< "iterator value = " << bIt1.Value() << "; expected value = " << emptyIdsBaseIdSet[pos];
		++pos;
	}
	EXPECT_TRUE(pos == emptyIdsBaseIdSet.size());

	pos = 0;
	while (bIt1.Next()) {
		EXPECT_TRUE(bIt1.Value() == ids1[pos]) << "iterator value = " << bIt1.Value() << "; expected value = " << ids1[pos];
		++pos;
	}
	EXPECT_TRUE(pos == ids1.size());

	reindexer::BtreeIndexIterator<typeof(m2)> bIt2(m2, emptyIdsKeyEntry.Unsorted().BuildBaseIdSet());
	bIt2.Start(true);
	pos = ids2.size() - 1;
	while (bIt2.Next() && pos) {
		EXPECT_TRUE(bIt2.Value() == ids2[pos]) << "iterator value = " << bIt2.Value() << "; expected value = " << ids2[pos];
		if (pos) {
			--pos;
		}
	}
	EXPECT_TRUE(pos == 0);

	pos = emptyIdsBaseIdSet.size() - 1;
	while (bIt2.Next()) {
		EXPECT_TRUE(bIt2.Value() == emptyIdsBaseIdSet[pos])
			<< "iterator value = " << bIt2.Value() << "; expected value = " << emptyIdsBaseIdSet[pos];
		if (pos) {
			--pos;
		}
	}
	EXPECT_TRUE(pos == 0);
}
