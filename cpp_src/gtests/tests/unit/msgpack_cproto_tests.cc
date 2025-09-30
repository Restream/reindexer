#include "msgpack_cproto_api.h"

#include <unordered_set>
#include "gtests/tests/tests_data.h"
#include "query_aggregate_strict_mode_test.h"

using reindexer::client::RPCDataFormat;

TEST_F(MsgPackCprotoApi, MsgPackDecodeTest) {
	using namespace reindexer;
	auto testDataPath = reindexer::fs::JoinPath(std::string(kTestsDataPath), "MsgPack");
	auto msgPackPath = fs::JoinPath(testDataPath, "msg.uu");
	auto msgJsonPath = fs::JoinPath(testDataPath, "msg.json");

	std::string content;
	int res = reindexer::fs::ReadFile(msgPackPath, content);
	ASSERT_GT(res, 0) << "Test data file not found: '" << msgPackPath << "'";
	reindexer::client::Item msgPackItem = client_->NewItem(default_namespace);
	if (res > 0) {
		size_t offset = 0;
		auto err = msgPackItem.FromMsgPack(content, offset);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	content.clear();
	res = reindexer::fs::ReadFile(msgJsonPath, content);
	ASSERT_GT(res, 0) << "Test data file not found: '" << msgJsonPath << "'";
	ASSERT_GT(content.size(), 1);

	reindexer::client::Item msgJsonItem = client_->NewItem(default_namespace);
	if (res > 0) {
		auto err = msgJsonItem.FromJSON(content);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	EXPECT_EQ(msgJsonItem.GetJSON(), msgPackItem.GetJSON());
}

TEST_F(MsgPackCprotoApi, SelectTest) {
	QueryResults qr(kResultsMsgPack | kResultsWithItemID);
	Error err = client_->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1000);
	ASSERT_EQ(qr.GetFlags() & kResultsFormatMask, kResultsMsgPack) << qr.GetFlags();
	for (auto it : qr) {
		checkItem(it);
	}
}

TEST_F(MsgPackCprotoApi, AggregationSelectTest) {
	QueryResults qr(kResultsMsgPack | kResultsWithItemID);
	Error err = client_->Select(
		Query(default_namespace).Distinct("id").Aggregate(AggFacet, {"a1", "a2"}).Aggregate(AggSum, {"id"}).Limit(100000), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.GetAggregationResults().size(), 3);
	ASSERT_EQ(qr.GetFlags() & kResultsFormatMask, kResultsMsgPack) << qr.GetFlags();

	const reindexer::AggregationResult& distinct = qr.GetAggregationResults()[0];
	EXPECT_EQ(distinct.GetType(), AggDistinct);
	ASSERT_EQ(distinct.GetFields().size(), 1);
	EXPECT_EQ(distinct.GetFields()[0], kFieldId);
	unsigned int rows = distinct.GetDistinctRowCount();
	EXPECT_EQ(rows, 1000);
	std::unordered_set<int> found;
	for (size_t i = 0; i < rows; ++i) {
		auto row = distinct.GetDistinctRow(i);
		assertrx(row.size() == 1);
		found.insert(reindexer::stoi(row[0].As<std::string>(distinct.GetPayloadType(), distinct.GetDistinctFields())));
	}
	ASSERT_EQ(distinct.GetDistinctRowCount(), found.size());

	for (size_t i = 0; i < distinct.GetDistinctRowCount(); ++i) {
		EXPECT_NE(found.find(i), found.end());
	}
	{
		const reindexer::AggregationResult& facet = qr.GetAggregationResults()[1];
		EXPECT_EQ(facet.GetType(), AggFacet);
		EXPECT_EQ(facet.GetFacets().size(), 1000);
		const auto& fields = facet.GetFields();
		ASSERT_EQ(fields.size(), 2);
		EXPECT_EQ(fields[0], kFieldA1);
		EXPECT_EQ(fields[1], kFieldA2);

		for (const reindexer::FacetResult& res : facet.GetFacets()) {
			EXPECT_EQ(res.count, 1);
			const auto v1 = reindexer::stoll(res.values[0]);
			const auto v2 = reindexer::stoll(res.values[1]);
			EXPECT_EQ(v1 * 3, v2 * 2);
		}
	}
	{
		const reindexer::AggregationResult& sum = qr.GetAggregationResults()[2];
		EXPECT_EQ(sum.GetType(), AggSum);
		const auto& fields = sum.GetFields();
		ASSERT_EQ(fields.size(), 1);
		EXPECT_EQ(fields[0], kFieldId);
		double val = (999.0 / 2.0) * 1000.0;
		EXPECT_DOUBLE_EQ(sum.GetValueOrZero(), val) << sum.GetValueOrZero() << "; " << val;
	}
}

TEST_F(MsgPackCprotoApi, AggregationsWithStrictModeTest) { QueryAggStrictModeTest(client_); }

TEST_F(MsgPackCprotoApi, ModifyItemsTest) {
	auto item = client_->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser, reindexer::ObjType::TypeObject);
	jsonBuilder.Put(kFieldId, 7);
	jsonBuilder.Put(kFieldA1, 77);
	jsonBuilder.Put(kFieldA2, 777);
	jsonBuilder.Put(kFieldA3, 7777);
	jsonBuilder.End();

	std::string itemSrcJson(wrser.Slice());

	char* endp = nullptr;
	Error err = item.FromJSON(wrser.Slice(), &endp);
	ASSERT_TRUE(err.ok()) << err.what();

	err = client_->Upsert(default_namespace, item, RPCDataFormat::MsgPack);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr(kResultsMsgPack | kResultsWithItemID);
	err = client_->Select(Query(default_namespace).Where(kFieldId, CondEq, Variant(int(7))), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1);
	ASSERT_EQ(qr.GetFlags() & kResultsFormatMask, kResultsMsgPack) << qr.GetFlags();

	for (auto it : qr) {
		checkItem(it);
		auto item = it.GetItem();
		ASSERT_TRUE(itemSrcJson == std::string(item.GetJSON()));
	}
}

TEST_F(MsgPackCprotoApi, UpdateTest) {
	Query q = Query(default_namespace).Set("a1", {7}).Where("id", CondGe, {10}).Where("id", CondLe, {100});
	QueryResults qr(kResultsMsgPack | kResultsWithItemID);
	Error err = client_->Update(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 91);
	ASSERT_EQ(qr.GetFlags() & kResultsFormatMask, kResultsMsgPack) << qr.GetFlags();

	int id = 10;
	for (auto it : qr) {
		checkItem(it);

		reindexer::WrSerializer json;
		reindexer::JsonBuilder jsonBuilder(json, reindexer::ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, id);
		jsonBuilder.Put(kFieldA1, 7);
		jsonBuilder.Put(kFieldA2, id * 3);
		jsonBuilder.Put(kFieldA3, id * 4);
		jsonBuilder.End();

		reindexer::client::Item item = it.GetItem();
		ASSERT_TRUE(item.GetJSON() == json.Slice());

		++id;
	}
}

TEST_F(MsgPackCprotoApi, DeleteTest) {
	Query q = Query(default_namespace).Where("id", CondGe, {100}).Where("id", CondLe, {110});
	QueryResults qr(kResultsMsgPack | kResultsWithItemID);
	Error err = client_->Delete(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 11);
	ASSERT_EQ(qr.GetFlags() & kResultsFormatMask, kResultsMsgPack) << qr.GetFlags();

	int id = 100;
	for (auto it : qr) {
		checkItem(it);

		reindexer::WrSerializer json;
		reindexer::JsonBuilder jsonBuilder(json, reindexer::ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, id);
		jsonBuilder.Put(kFieldA1, id * 2);
		jsonBuilder.Put(kFieldA2, id * 3);
		jsonBuilder.Put(kFieldA3, id * 4);
		jsonBuilder.End();

		auto item = it.GetItem();
		ASSERT_TRUE(item.GetJSON() == json.Slice());

		++id;
	}
}
