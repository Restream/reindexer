#include "btree_idsets_api.h"

TEST_F(BtreeIdsetsApi, SelectByStringField) {
	QueryResults qr;
	string strValueToCheck = lastStrValue;
	Error err = reindexer->Select(Query(default_namespace).Not().Where(kFieldOne, CondEq, strValueToCheck), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();
		KeyRef kr = item[kFieldOne];
		EXPECT_TRUE(kr.Type() == KeyValueString);
		EXPECT_TRUE(kr.As<string>() != strValueToCheck);
	}
}

TEST_F(BtreeIdsetsApi, SelectByIntField) {
	const int boundaryValue = 5000;

	QueryResults qr;
	Error err = reindexer->Select(Query(default_namespace).Where(kFieldTwo, CondGe, KeyValue(static_cast<int>(boundaryValue))), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	for (size_t i = 0; i < qr.Count(); i++) {
		Item item = qr[i].GetItem();
		KeyRef kr = item[kFieldTwo];
		EXPECT_TRUE(kr.Type() == KeyValueInt);
		EXPECT_TRUE(static_cast<int>(kr) >= boundaryValue);
	}
}

TEST_F(BtreeIdsetsApi, SelectByBothFields) {
	QueryResults qr;
	const int boundaryValue = 50000;
	const string strValueToCheck = lastStrValue;
	const string strValueToCheck2 = "reindexer is fast";
	Error err = reindexer->Select(Query(default_namespace)
									  .Where(kFieldOne, CondLe, strValueToCheck2)
									  .Not()
									  .Where(kFieldOne, CondEq, strValueToCheck)
									  .Where(kFieldTwo, CondGe, KeyValue(static_cast<int>(boundaryValue))),
								  qr);
	EXPECT_TRUE(err.ok()) << err.what();
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();
		KeyRef krOne = item[kFieldOne];
		EXPECT_TRUE(krOne.Type() == KeyValueString);
		EXPECT_TRUE(strValueToCheck2.compare(krOne.As<string>()) > 0);
		EXPECT_TRUE(krOne.As<string>() != strValueToCheck);
		KeyRef krTwo = item[kFieldTwo];
		EXPECT_TRUE(krTwo.Type() == KeyValueInt);
		EXPECT_TRUE(static_cast<int>(krTwo) >= boundaryValue);
	}
}

TEST_F(BtreeIdsetsApi, SortByStringField) {
	QueryResults qr;
	Error err = reindexer->Select(Query(default_namespace).Sort(kFieldOne, true), qr);
	EXPECT_TRUE(err.ok()) << err.what();

	KeyRef prev;
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();
		KeyRef curr = item[kFieldOne];
		if (i != 0) {
			EXPECT_TRUE(prev >= curr);
		}
		prev = curr;
	}
}

TEST_F(BtreeIdsetsApi, SortByIntField) {
	QueryResults qr;
	Error err = reindexer->Select(Query(default_namespace).Sort(kFieldTwo, false), qr);
	EXPECT_TRUE(err.ok()) << err.what();

	KeyRef prev;
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();
		KeyRef curr = item[kFieldTwo];
		if (i != 0) {
			EXPECT_TRUE(prev.As<int>() <= curr.As<int>());
		}
		prev = curr;
	}
}

TEST_F(BtreeIdsetsApi, JoinSimpleNs) {
	QueryResults qr;
	Query joinedNs = Query(joinedNsName).Where(kFieldThree, CondGt, KeyValue(static_cast<int>(9000))).Sort(kFieldThree, false);
	Error err =
		reindexer->Select(Query(default_namespace, 0, 3000).InnerJoin(kFieldId, kFieldIdFk, CondEq, joinedNs).Sort(kFieldTwo, false), qr);
	EXPECT_TRUE(err.ok()) << err.what();

	KeyRef prevFieldTwo;
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();
		KeyRef currFieldTwo = item[kFieldTwo];
		if (i != 0) {
			EXPECT_TRUE(currFieldTwo.As<int>() >= prevFieldTwo.As<int>());
		}
		prevFieldTwo = currFieldTwo;

		KeyRef prevJoinedFk;
		const auto& joined = qr[i].GetJoined();
		const QueryResults& joinResult = joined[0];
		EXPECT_TRUE(joinResult.Count() > 0);
		for (size_t j = 0; j < joinResult.Count(); ++j) {
			Item joinedItem = joinResult[j].GetItem();
			KeyRef joinedFkCurr = joinedItem[kFieldIdFk];
			EXPECT_TRUE(joinedFkCurr == item[kFieldId]);
			if (j != 0) {
				EXPECT_TRUE(joinedFkCurr >= prevJoinedFk);
			}
			prevJoinedFk = joinedFkCurr;
		}
	}
}
