#include "msgpack_cproto_api.h"

TEST_F(MsgPackCprotoApi, SelectTest) {
	reindexer::client::QueryResults qr;
	Error err = client_->Select(Query(default_namespace), qr, ctx_, nullptr, FormatMsgPack);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1000);
	for (auto it : qr) {
		checkItem(it);
	}
}

TEST_F(MsgPackCprotoApi, AggregationSelectTest) {
	reindexer::client::QueryResults qr;
	Error err =
		client_->Select("select distinct(id), facet(a1, a2), sum(id) from test_namespace limit 100000", qr, ctx_, nullptr, FormatMsgPack);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.GetAggregationResults().size() == 3);

	const reindexer::AggregationResult& distinct = qr.GetAggregationResults()[0];
	ASSERT_TRUE(distinct.type == AggDistinct);
	ASSERT_TRUE(distinct.distincts.size() == 1000);
	ASSERT_TRUE(distinct.fields.size() == 1);
	ASSERT_TRUE(distinct.fields[0] == kFieldId);
	for (size_t i = 0; i < distinct.distincts.size(); ++i) {
		ASSERT_TRUE(reindexer::stoi(distinct.distincts[i]) == int(i));
	}

	const reindexer::AggregationResult& facet = qr.GetAggregationResults()[1];
	ASSERT_TRUE(facet.type == AggFacet);
	ASSERT_TRUE(facet.facets.size() == 1000);
	ASSERT_TRUE(facet.fields.size() == 2);
	ASSERT_TRUE(facet.fields[0] == kFieldA1);
	ASSERT_TRUE(facet.fields[1] == kFieldA2);

	int i = 0;
	for (const reindexer::FacetResult& res : facet.facets) {
		for (int j = 0; j < int(res.values.size()); ++j) {
			int64_t val = reindexer::stoll(res.values[j]);
			ASSERT_TRUE(val == i * (2 + j));
		}
		++i;
	}

	const reindexer::AggregationResult& sum = qr.GetAggregationResults()[2];
	ASSERT_TRUE(sum.type == AggSum);
	ASSERT_TRUE(sum.fields.size() == 1);
	ASSERT_TRUE(sum.fields[0] == kFieldId);
	double val = (double(0 + 999.0f) / 2) * 1000;
	ASSERT_TRUE(sum.value == int64_t(val)) << sum.value << "; " << val;
}

TEST_F(MsgPackCprotoApi, ModifyItemsTest) {
	reindexer::client::Item item = client_->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
	jsonBuilder.Put(kFieldId, 7);
	jsonBuilder.Put(kFieldA1, 77);
	jsonBuilder.Put(kFieldA2, 777);
	jsonBuilder.Put(kFieldA3, 7777);
	jsonBuilder.End();

	string itemSrcJson(wrser.Slice());

	char* endp = nullptr;
	Error err = item.FromJSON(wrser.Slice(), &endp);
	ASSERT_TRUE(err.ok()) << err.what();

	err = client_->Upsert(default_namespace, item, ctx_, FormatMsgPack);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::client::QueryResults qr;
	err = client_->Select(Query(default_namespace).Where(kFieldId, CondEq, Variant(int(7))), qr, ctx_, nullptr, FormatMsgPack);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1);

	for (auto it : qr) {
		checkItem(it);
		reindexer::client::Item item = it.GetItem();
		ASSERT_TRUE(itemSrcJson == string(item.GetJSON()));
	}
}

TEST_F(MsgPackCprotoApi, UpdateTest) {
	const reindexer::string_view sql = "update test_namespace set a1 = 7 where id >= 10 and id <= 100";
	Query q;
	q.FromSQL(sql);

	reindexer::client::QueryResults qr;
	Error err = client_->Update(q, qr, ctx_, FormatMsgPack);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 91);

	int id = 10;
	for (auto it : qr) {
		checkItem(it);

		reindexer::WrSerializer json;
		reindexer::JsonBuilder jsonBuilder(json, ObjType::TypeObject);
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
	const reindexer::string_view sql = "delete from test_namespace where id >= 100 and id <= 110";
	Query q;
	q.FromSQL(sql);

	reindexer::client::QueryResults qr;
	Error err = client_->Delete(q, qr, ctx_, FormatMsgPack);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 11);

	int id = 100;
	for (auto it : qr) {
		checkItem(it);

		reindexer::WrSerializer json;
		reindexer::JsonBuilder jsonBuilder(json, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, id);
		jsonBuilder.Put(kFieldA1, id * 2);
		jsonBuilder.Put(kFieldA2, id * 3);
		jsonBuilder.Put(kFieldA3, id * 4);
		jsonBuilder.End();

		reindexer::client::Item item = it.GetItem();
		ASSERT_TRUE(item.GetJSON() == json.Slice());

		++id;
	}
}
