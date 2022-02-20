#include <future>
#include "cluster/stats/replicationstats.h"
#include "core/itemimpl.h"
#include "sharding_api.h"

static void CheckServerIDs(std::vector<std::vector<ServerControl>> &svc) {
	WrSerializer ser;
	size_t idx = 0;
	for (auto &cluster : svc) {
		for (auto &sc : cluster) {
			auto rx = sc.Get()->api.reindexer;
			client::SyncCoroQueryResults qr;
			auto err = rx->Select(Query("#config").Where("type", CondEq, "replication"), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 1);
			err = qr.begin().GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(reindexer::giftStr(ser.Slice()));
			const auto serverId = root["replication"]["server_id"].As<int>(-1);
			ASSERT_EQ(serverId, idx);
			ser.Reset();
			++idx;
		}
	}
}

TEST_F(ShardingApi, Select) {
	Init();
	CheckServerIDs(svc_);

	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			Query q = Query(default_namespace)
						  .Where(kFieldLocation, CondEq, key)
						  .InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace).Where(kFieldLocation, CondEq, key));

			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key << std::endl;
			ASSERT_EQ(qr.Count(), 40);
			for (auto it : qr) {
				auto item = it.GetItem();
				std::string_view json = item.GetJSON();
				ASSERT_TRUE(json.find("\"location\":\"" + key + "\"") != std::string_view::npos) << json;

				const auto &joinedData = it.GetJoined();
				EXPECT_EQ(joinedData.size(), 1);
				for (size_t joinedField = 0; joinedField < joinedData.size(); ++joinedField) {
					QueryResults qrJoined;
					const auto &joinedItems = joinedData[joinedField];
					for (const auto &itemData : joinedItems) {
						ItemImpl itemimpl = ItemImpl(qr.GetPayloadType(1), qr.GetTagsMatcher(1));
						itemimpl.Unsafe(true);
						err = itemimpl.FromCJSON(itemData.data);
						ASSERT_TRUE(err.ok()) << err.what();

						std::string_view joinedJson = itemimpl.GetJSON();

						EXPECT_EQ(joinedJson, json);
					}
				}
			}
		}
	}
}

#ifndef REINDEX_WITH_TSAN

TEST_F(ShardingApi, LargeProxiedSelects) {
	// Check if qr streaming works (size of 21k item per shard does not allow to recieve all the data right after first request)
	const size_t kShardDataCount = 21000;
	fast_hash_map<int, std::string> insertedItemsById;

	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	cfg.rowsInTableOnShard = kShardDataCount;
	cfg.insertedItemsById = &insertedItemsById;
	Init(std::move(cfg));
	std::cout << "Init done" << std::endl;

	// Distributed qr
	{
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace);

		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), kShardDataCount * kShards);
		std::cout << "Distributed select done" << std::endl;
		WrSerializer wser;
		for (auto &it : qr) {
			wser.Reset();
			err = it.GetJSON(wser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_FALSE(it.IsRaw());
			gason::JsonParser parser;
			auto json = parser.Parse(wser.Slice());
			auto id = json[kFieldId].As<int>(-1);
			const auto found = insertedItemsById.find(id);
			ASSERT_TRUE(found != insertedItemsById.end()) << "Unexpected item: " << wser.Slice();
			ASSERT_EQ(wser.Slice(), found->second);
		}
	}

	std::cout << "Validation for distributed select done" << std::endl;

	// Proxied qr
	{
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key2");

		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), kShardDataCount);
		std::cout << "Proxied select done" << std::endl;
		WrSerializer wser;
		for (auto &it : qr) {
			wser.Reset();
			err = it.GetJSON(wser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_FALSE(it.IsRaw());
			gason::JsonParser parser;
			auto json = parser.Parse(wser.Slice());
			auto id = json[kFieldId].As<int>(-1);
			const auto found = insertedItemsById.find(id);
			ASSERT_TRUE(found != insertedItemsById.end()) << "Unexpected item: " << wser.Slice();
			ASSERT_EQ(wser.Slice(), found->second);
		}
	}
	std::cout << "Validation for proxied select done" << std::endl;
}
#endif	// REINDEX_WITH_TSAN

TEST_F(ShardingApi, JoinBetweenShardedAndNonSharded) {
	const size_t kShardDataCount = 3;
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = kShardDataCount;
	Init(std::move(cfg));

	const std::string kLocalNamespace = "local_namespace";
	const std::size_t kShardWithLocalNs = 1;
	const std::vector<std::string> kLocalNsData = {R"json({"id":0,"data":"data1"})json", R"json({"id":3,"data":"data2"})json"};
	const std::unordered_map<int, std::string> kExpectedJoinResults1 = {{0, "\"joined_local_namespace\":[" + kLocalNsData[0] + "]"},
																		{3, "\"joined_local_namespace\":[" + kLocalNsData[1] + "]"}};

	// Create and fill local namespace on shard1
	auto shard1 = svc_[kShardWithLocalNs][0].Get()->api.reindexer;
	NamespaceDef nsDef(kLocalNamespace);
	nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
	nsDef.AddIndex(kFieldData, "hash", "string", IndexOpts());
	Error err = shard1->AddNamespace(nsDef);
	ASSERT_TRUE(err.ok()) << err.what();
	for (auto &json : kLocalNsData) {
		auto item = shard1->NewItem(kLocalNamespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		err = shard1->Upsert(kLocalNamespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	std::unordered_map<int, std::string> kExpectedJoinResults2;
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key" + std::to_string(kShardWithLocalNs)).Sort(kFieldId, false);
		err = shard1->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), kShardDataCount);
		int id = 0;
		for (auto it : qr) {
			if (id <= 3) {
				auto item = it.GetItem();
				auto json = item.GetJSON();
				kExpectedJoinResults2[id] = "\"joined_test_namespace\":[" + std::string(json) + "]";
				id += 3;
			}
		}
	}

	waitSync(kShardWithLocalNs, kLocalNamespace);

	// Use local ns as right_ns on correct shard
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		const std::string key = "key" + std::to_string(kShardWithLocalNs);
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).InnerJoin(kFieldId, kFieldId, CondEq, Query(kLocalNamespace));
		err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
		ASSERT_EQ(qr.Count(), kExpectedJoinResults1.size()) << "; i = " << i << "; location = " << key;
		for (auto it : qr) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
			gason::JsonParser parser;
			auto json = parser.Parse(ser.Slice());
			auto id = json["id"].As<int>(-1);
			auto found = ser.Slice().find(kExpectedJoinResults1.at(id));
			EXPECT_NE(found, std::string_view::npos)
				<< ser.Slice() << "; expected substring: " << kExpectedJoinResults1.at(id) << "; i = " << i << "; location = " << key;
		}
	}
	// Use local ns as left_ns (sharded ns has proper shardin key)
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		const std::string key = "key" + std::to_string(kShardWithLocalNs);
		client::SyncCoroQueryResults qr;
		Query q = Query(kLocalNamespace).InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace).Where(kFieldLocation, CondEq, key));
		err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
		ASSERT_EQ(qr.Count(), kExpectedJoinResults2.size()) << "; i = " << i << "; location = " << key;
		for (auto it : qr) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
			gason::JsonParser parser;
			auto json = parser.Parse(ser.Slice());
			auto id = json["id"].As<int>(-1);
			auto found = ser.Slice().find(kExpectedJoinResults2.at(id));
			EXPECT_NE(found, std::string_view::npos)
				<< ser.Slice() << "; expected substring: " << kExpectedJoinResults2.at(id) << "; i = " << i << "; location = " << key;
		}
	}
	// Use local ns as left_ns (sharded ns does not have sharding key)
	// Expecting error
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		client::SyncCoroQueryResults qr;
		Query q = Query(kLocalNamespace).InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace));
		err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic) << err.what() << "; i = " << i;
		ASSERT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE") << "; i = " << i;
	}
	// Use local ns as right_ns or left ns on wrong shard (this shard does not have this local namespace)
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		const std::string key = "key" + std::to_string((kShardWithLocalNs + 1) % kShards);
		{
			client::SyncCoroQueryResults qr;
			Query q =
				Query(default_namespace).Where(kFieldLocation, CondEq, key).InnerJoin(kFieldId, kFieldId, CondEq, Query(kLocalNamespace));
			err = rx->Select(q, qr);
			ASSERT_EQ(err.code(), errNotFound) << err.what() << "; i = " << i << "; location = " << key;
		}

		if (i / kNodesInCluster == kShardWithLocalNs) continue;	 // Skipping shard with local ns here

		{
			client::SyncCoroQueryResults qr;
			Query q =
				Query(kLocalNamespace).InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace).Where(kFieldLocation, CondEq, key));
			err = rx->Select(q, qr);
			ASSERT_EQ(err.code(), errNotFound) << err.what() << "; i = " << i << "; location = " << key;
		}
	}
	// Use sharded ns as left_ns without sharding key
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).InnerJoin(kFieldId, kFieldId, CondEq, Query(kLocalNamespace));
		err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic) << err.what() << "; i = " << i;
		ASSERT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE") << "; i = " << i;
	}
}

TEST_F(ShardingApi, SelectFTSeveralShards) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;

	client::SyncCoroQueryResults qr1;
	Query q = Query(default_namespace).Where(kFieldFTData, CondEq, RandString());
	Error err = rx->Select(q, qr1);
	ASSERT_FALSE(err.ok());
	ASSERT_EQ(err.what(), "Full text query by several sharding hosts");

	client::SyncCoroQueryResults qr2;
	q.Where(kFieldLocation, CondEq, "key1");
	err = rx->Select(q, qr2);
	ASSERT_TRUE(err.ok());
}

TEST_F(ShardingApi, CheckQueryWithSharding) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(kFieldId, CondEq, 0);
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(kFieldLocation, CondEq, "key2");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Duplication of shard key condition in the query");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Or().Where(kFieldId, CondEq, 0);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be connected with other conditions by operator OR");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldId, CondEq, 0).Or().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be connected with other conditions by operator OR");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Not().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be negative");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).WhereBetweenFields(kFieldLocation, CondEq, kFieldData);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key cannot be compared with another field");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket();
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).OpenBracket().OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket().CloseBracket();
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	// JOIN
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q =
			Query(default_namespace).Where(kFieldLocation, CondEq, "key1").InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Join query must contain shard key.");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q =
			Query(default_namespace).InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key from other node");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Not().Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be negative");
	}
	// MERGE
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Merge(Query{default_namespace});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Merge query must contain shard key.");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key from other node");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket());
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Distinct(kFieldId);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Query to all shard can't contain aggregations other than COUNT or COUNT CACHED");
	}
	for (const auto agg : {AggSum, AggAvg, AggMin, AggMax, AggFacet, AggDistinct}) {
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Aggregate(agg, {kFieldId});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Query to all shard can't contain aggregations other than COUNT or COUNT CACHED");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).ReqTotal();
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).CachedTotal();
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	for (const auto agg : {AggSum, AggAvg, AggMin, AggMax, AggFacet, AggDistinct}) {
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Aggregate(agg, {kFieldId});
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Distinct(kFieldId).Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).ReqTotal().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).CachedTotal().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Sort(kFieldId + " * 10", false);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		ASSERT_EQ(err.what(), "Query to all shard can't contain ordering by expression");
	}
	{
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Sort(kFieldId + " * 10", false);
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok());
	}
}

TEST_F(ShardingApi, TagsMatcherConfusion) {
	const std::string kNewField = "new_field";
	auto buildItem = [&](WrSerializer &wrser, int id, string &&location, const string &data, string &&newFieldValue) {
		reindexer::JsonBuilder jsonBuilder(wrser);
		jsonBuilder.Put(kFieldId, int(id));
		jsonBuilder.Put(kFieldLocation, location);
		jsonBuilder.Put(kFieldData, data);
		jsonBuilder.Put(kNewField, newFieldValue);
		jsonBuilder.End();
	};
	Init();
	for (size_t i = 0; i < NodesCount(); i += 2) {
		size_t shard = 1;
		const std::string updated = "updated_" + RandString();
		reindexer::client::Item item = getNode(i)->api.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok());

		WrSerializer wrser;
		buildItem(wrser, i, std::string("key" + std::to_string(shard)), updated, RandString());

		Error err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();

		err = getNode(i)->api.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; shard = " << shard;

		if (i != (NodesCount() - 1)) {
			wrser.Reset();
			buildItem(wrser, i, std::string("key" + std::to_string(shard + 1)), updated, RandString());

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();

			err = getNode(i + 1)->api.reindexer->Upsert(default_namespace, item);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; shard = " << shard;
		}
	}
}

TEST_F(ShardingApi, Update) {
	Init();
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			// key1, key2, key3(proxy shardId=0)
			{
				const std::string key = "key" + std::to_string(shard + 1);
				const std::string updated = "updated_" + RandString();
				client::SyncCoroQueryResults qr;
				Query q = Query(default_namespace).Set(kFieldData, updated);
				q.Where(kFieldLocation, CondEq, key);
				q.type_ = QueryUpdate;
				Error err = rx->Update(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; location = " << key << std::endl;
				std::string toFind;
				toFind = "\"location\":\"" + key + "\",\"data\":\"" + updated + "\"";
				ASSERT_TRUE(qr.Count() == 40) << qr.Count();
				for (auto it : qr) {
					auto item = it.GetItem();
					std::string_view json = item.GetJSON();
					ASSERT_TRUE(json.find(toFind) != std::string_view::npos) << json << "; expected {" << toFind << "}";
				}
			}

			{
				const std::string updatedErr = "updated_" + RandString();
				Query qNoShardKey = Query(default_namespace).Set(kFieldData, updatedErr);
				client::SyncCoroQueryResults qrErr;
				Error err = rx->Update(qNoShardKey, qrErr);
				ASSERT_FALSE(err.ok());
				Query qNoShardKeySelect = Query(default_namespace);
				client::SyncCoroQueryResults qrSelect;
				err = rx->Select(qNoShardKeySelect, qrSelect);
				ASSERT_TRUE(err.ok()) << err.what();
				for (auto it : qrSelect) {
					auto item = it.GetItem();
					std::string_view json = item.GetJSON();
					ASSERT_TRUE(json.find(updatedErr) == std::string_view::npos);
				}
			}
		}
	}
}

TEST_F(ShardingApi, Delete) {
	const int count = 120;
	int pos = count;
	Init();
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			std::string sql = "delete from " + default_namespace + " where " + kFieldLocation + " = '" + key + "'";
			Query q;
			q.FromSQL(sql);
			Error err = rx->Delete(q, qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; location = " << key << std::endl;
			ASSERT_TRUE(qr.Count() == 40) << key << ": " << qr.Count();
			std::string toFind = "\"location\":\"" + key + "\"";
			for (auto it : qr) {
				auto item = it.GetItem();
				std::string_view json = item.GetJSON();
				ASSERT_TRUE(json.find(toFind) != std::string_view::npos) << json;
			}
		}
		Fill(i / kNodesInCluster, pos, count);
		pos += count;
	}
}

TEST_F(ShardingApi, Meta) {
	// User's meta must be duplicated on each shard
	Init();
	const std::string keyPrefix = "key";
	const std::string keyDataPrefix = "key_data";
	constexpr size_t kMetaCount = 10;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			if ((i == 0) && (shard == 0)) {
				for (size_t j = 0; j < kMetaCount; ++j) {
					Error err = rx->PutMeta(default_namespace, keyPrefix + std::to_string(j), keyDataPrefix + std::to_string(j));
					ASSERT_TRUE(err.ok()) << err.what();
				}
				waitSync(default_namespace);
			}
			for (size_t j = 0; j < kMetaCount; ++j) {
				std::string actualValue;
				const std::string properValue = keyDataPrefix + std::to_string(j);
				Error err = rx->GetMeta(default_namespace, keyPrefix + std::to_string(j), actualValue);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(properValue, actualValue) << "i = " << i << "; j = " << j << "; shard = " << shard;

				// Check meta vector for each shard
				std::vector<ShardedMeta> sharded;
				err = rx->GetMeta(default_namespace, keyPrefix + std::to_string(j), sharded);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(sharded.size(), kShards);
				std::unordered_set<int> shardsSet;
				for (int k = 0; k < int(kShards); ++k) {
					shardsSet.emplace(k);
				}
				for (auto &m : sharded) {
					ASSERT_EQ(m.data, properValue) << m.shardId;
					ASSERT_EQ(shardsSet.count(m.shardId), 1)
						<< "m.shardId = " << m.shardId << "; i = " << i << "; j = " << j << "; shard = " << shard;
					shardsSet.erase(m.shardId);
				}
				ASSERT_EQ(shardsSet.size(), 0) << "i = " << i << "; j = " << j << "; shard = " << shard;
			}
		}
	}
}

TEST_F(ShardingApi, Serial) {
	// _SERIAL_* meta must be independent for each shard
	Init();
	Error err;
	const std::vector<size_t> kItemsCountByShard = {10, 9, 8};
	const std::string kSerialFieldName = "linearValues";
	const std::string kSerialMetaKey = "_SERIAL_" + kSerialFieldName;
	std::unordered_map<std::string, int> shardsUniqueItems;
	std::vector<size_t> serialByShard(kItemsCountByShard.size(), 0);
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		if (i == 0) {
			err = rx->AddIndex(default_namespace, IndexDef{kSerialFieldName, "hash", "int", IndexOpts()});
			ASSERT_TRUE(err.ok()) << err.what();
			waitSync(default_namespace);
		}
		for (size_t shard = 0; shard < kShards; ++shard) {
			size_t startId = 0;
			const std::string key = "key" + std::to_string(shard);
			auto itKey = shardsUniqueItems.find(key);
			if (itKey == shardsUniqueItems.end()) {
				shardsUniqueItems[key] = 0;
			} else {
				startId = shardsUniqueItems[key];
			}
			for (size_t j = 0; j < kItemsCountByShard[shard]; ++j) {
				reindexer::client::Item item = rx->NewItem(default_namespace);
				ASSERT_TRUE(item.Status().ok());

				WrSerializer wrser;
				reindexer::JsonBuilder jsonBuilder(wrser);
				jsonBuilder.Put(kFieldId, int(j));
				jsonBuilder.Put(kFieldLocation, key);
				jsonBuilder.Put(kSerialFieldName, int(0));
				jsonBuilder.End();

				err = item.FromJSON(wrser.Slice());
				ASSERT_TRUE(err.ok()) << err.what();
				item.SetPrecepts({kSerialFieldName + "=SERIAL()"});
				serialByShard[shard]++;

				err = rx->Upsert(default_namespace, item);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();
				gason::JsonParser parser;
				auto itemJson = parser.Parse(item.GetJSON());
				unsigned int serialField = itemJson[kSerialFieldName].As<int>(-1);
				ASSERT_EQ(serialField, serialByShard[shard]);

				Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(j));
				client::SyncCoroQueryResults qr;
				err = rx->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(qr.Count() == 1) << qr.Count() << "; i = " << i << "; shard = " << shard << "; location = " << key << std::endl;
				size_t correctSerial = startId + j + 1;
				for (auto it : qr) {
					reindexer::client::Item itm = it.GetItem();
					std::string_view json = itm.GetJSON();
					std::string_view::size_type pos = json.find(kSerialFieldName + "\":" + std::to_string(correctSerial));
					ASSERT_TRUE(pos != std::string_view::npos) << correctSerial;
				}
			}
			shardsUniqueItems[key] += kItemsCountByShard[shard];
		}
	}
	waitSync(default_namespace);
	// Validate _SERIAL_* values for each shard
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		std::vector<ShardedMeta> sharded;
		err = rx->GetMeta(default_namespace, kSerialMetaKey, sharded);
		ASSERT_TRUE(err.ok()) << err.what() << "i = " << i;
		ASSERT_EQ(sharded.size(), kShards) << "i = " << i;
		std::unordered_map<int, std::string> expectedSerials;
		for (int j = 0; j < int(kShards); ++j) {
			expectedSerials.emplace(j, std::to_string(NodesCount() * kItemsCountByShard[j]));
		}
		for (auto &m : sharded) {
			ASSERT_EQ(expectedSerials.count(m.shardId), 1) << "m.shardId = " << m.shardId << "; i = " << i;
			ASSERT_EQ(expectedSerials[m.shardId], m.data) << m.shardId << "; i = " << i;
			expectedSerials.erase(m.shardId);
		}
		ASSERT_EQ(expectedSerials.size(), 0) << "i = " << i;
	}
}

TEST_F(ShardingApi, EnumLocalNamespaces) {
	// Check if each shard has it's local namespaces and sharded namespace
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	const std::vector<std::vector<std::string>> kExpectedNss = {
		{default_namespace, "ns1"}, {default_namespace, "ns2", "ns3"}, {default_namespace}};
	for (size_t shard = 0; shard < kShards - 1; ++shard) {
		auto rx = svc_[shard][0].Get()->api.reindexer;
		for (size_t nsId = 1; nsId < kExpectedNss[shard].size(); ++nsId) {
			auto err = rx->OpenNamespace(kExpectedNss[shard][nsId]);
			ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard << "; ns: " << kExpectedNss[shard][nsId];
		}
	}
	for (size_t shard = 0; shard < kShards; ++shard) {
		auto rx = svc_[shard][0].Get()->api.reindexer;
		std::vector<NamespaceDef> nss;
		auto err = rx->EnumNamespaces(nss, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard;
		ASSERT_EQ(nss.size(), kExpectedNss[shard].size());
		for (auto &expNs : kExpectedNss[shard]) {
			const auto found = std::find_if(nss.begin(), nss.end(), [&expNs](const NamespaceDef &def) { return def.name == expNs; });
			ASSERT_TRUE(found != nss.end()) << expNs << "; shard = " << shard;
		}
	}
	// Check if we can drop local namespace and can't drop other shard local namespace
	{
		size_t shard = 0;
		auto rx = svc_[shard][0].Get()->api.reindexer;
		const auto kDroppendNs = kExpectedNss[shard][1];
		auto err = rx->DropNamespace(kExpectedNss[shard][1]);
		ASSERT_TRUE(err.ok()) << err.what();
		std::vector<NamespaceDef> nss;
		err = rx->EnumNamespaces(nss, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(nss.size(), 1);
		ASSERT_EQ(nss[0].name, default_namespace);

		err = rx->DropNamespace(kExpectedNss[shard + 1][1]);
		ASSERT_FALSE(err.ok()) << err.what();
		nss.clear();

		shard = shard + 1;
		rx = svc_[shard][0].Get()->api.reindexer;
		err = rx->EnumNamespaces(nss, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard;
		ASSERT_EQ(nss.size(), kExpectedNss[shard].size());
		for (auto &expNs : kExpectedNss[shard]) {
			const auto found = std::find_if(nss.begin(), nss.end(), [&expNs](const NamespaceDef &def) { return def.name == expNs; });
			ASSERT_TRUE(found != nss.end()) << expNs << "; shard = " << shard;
		}
	}
}

TEST_F(ShardingApi, UpdateItems) {
	Init();
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string updated = "updated_" + RandString();
			const std::string key = "key" + std::to_string(shard + 1);

			reindexer::client::Item item = rx->NewItem(default_namespace);
			ASSERT_TRUE(item.Status().ok());

			WrSerializer wrser;
			reindexer::JsonBuilder jsonBuilder(wrser);
			jsonBuilder.Put(kFieldId, int(shard));
			jsonBuilder.Put(kFieldLocation, key);
			jsonBuilder.Put(kFieldData, updated);
			jsonBuilder.End();

			Error err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();

			if (i % 2 == 0) {
				err = rx->Update(default_namespace, item);
			} else {
				err = rx->Upsert(default_namespace, item);
			}
			ASSERT_TRUE(err.ok()) << err.what();

			Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::SyncCoroQueryResults qr;
			err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(qr.Count() == 1) << qr.Count() << "; i = " << i << "; shard = " << shard << "; location = " << key << std::endl;
			for (auto it : qr) {
				reindexer::client::Item itm = it.GetItem();
				std::string_view json = itm.GetJSON();
				ASSERT_TRUE(json == wrser.Slice());
			}
		}
	}
}

TEST_F(ShardingApi, DeleteItems) {
	Init();
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		std::string itemJson;
		{
			Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::SyncCoroQueryResults qr;
			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 1);
			client::Item it = qr.begin().GetItem();
			itemJson = std::string(it.GetJSON());
		}

		reindexer::client::Item item = rx->NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok());

		Error err = item.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx->Delete(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();

		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
		client::SyncCoroQueryResults qr;
		err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 0) << qr.Count() << "; from proxy; shard = " << shard << "; location = " << key << std::endl;
	}
}

TEST_F(ShardingApi, UpdateIndex) {
	Init();
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			IndexDef indexDef{"new", {"new"}, "hash", "int", IndexOpts()};
			Error err = rx->AddIndex(default_namespace, indexDef);
			ASSERT_TRUE(err.ok()) << err.what();
			std::vector<NamespaceDef> nsdefs;
			err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(default_namespace));
			ASSERT_TRUE(err.ok()) << err.what() << std::endl;
			ASSERT_TRUE((nsdefs.size() == 1) && (nsdefs.front().name == default_namespace));
			ASSERT_TRUE(std::find_if(nsdefs.front().indexes.begin(), nsdefs.front().indexes.end(),
									 [](const IndexDef &index) { return index.name_ == "new"; }) != nsdefs.front().indexes.end());
			err = rx->DropIndex(default_namespace, indexDef);
			ASSERT_TRUE(err.ok()) << err.what();
			nsdefs.clear();
			err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(default_namespace));
			ASSERT_TRUE(err.ok()) << err.what() << std::endl;
			ASSERT_TRUE((nsdefs.size() == 1) && (nsdefs.front().name == default_namespace));
			ASSERT_TRUE(std::find_if(nsdefs.front().indexes.begin(), nsdefs.front().indexes.end(),
									 [](const IndexDef &index) { return index.name_ == "new"; }) == nsdefs.front().indexes.end());
		}
	}
}

TEST_F(ShardingApi, DropNamespace) {
	Init();
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	Error err = rx->TruncateNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		client::SyncCoroQueryResults qr;
		err = rx->Select(Query(default_namespace).Where(kFieldLocation, CondEq, key), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 0) << qr.Count();
	}

	client::SyncCoroQueryResults qr;
	err = rx->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 0) << qr.Count();

	err = rx->DropNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<NamespaceDef> nsdefs;
	err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(default_namespace));
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(nsdefs.empty());
}

static void CheckTransactionErrors(client::SyncCoroReindexer &rx, std::string_view nsName) {
	// Check errros handling in transactions
	for (unsigned i = 0; i < 2; ++i) {
		auto tr = rx.NewTransaction(nsName);
		ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();

		auto item = tr.NewItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(R"json({"id":0,"location":"key0"})json");
		ASSERT_TRUE(err.ok()) << err.what();
		err = tr.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();

		item = tr.NewItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		err = item.FromJSON(R"json({"id":0,"location":"key1"})json");
		ASSERT_TRUE(err.ok()) << err.what();
		err = tr.Upsert(std::move(item));
		ASSERT_FALSE(err.ok());
		ASSERT_FALSE(tr.Status().ok());
		item = tr.NewItem();
		ASSERT_FALSE(item.Status().ok());

		if (i == 0) {
			// Check if we'll get an error on commit, if there were an errors before
			client::SyncCoroQueryResults qr;
			err = rx.CommitTransaction(tr, qr);
			EXPECT_FALSE(err.ok());
			std::string_view kExpectedErr = "Unable to commit tx with error status:";
			EXPECT_EQ(err.what().substr(0, kExpectedErr.size()), kExpectedErr);
		} else {
			// Check if rollback will still succeed
			err = rx.RollBackTransaction(tr);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
}

TEST_F(ShardingApi, Transactions) {
	Init();
	const int rowsInTr = 10;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		Error err = rx->TruncateNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = std::string("key" + std::to_string(shard + 1));
			const int modes[] = {ModeUpsert, ModeDelete};
			for (int mode : modes) {
				reindexer::client::SyncCoroTransaction tr = rx->NewTransaction(default_namespace);
				ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
				for (int id = 0; id < rowsInTr; ++id) {
					if ((mode == ModeDelete) && (shard % 2 == 0)) {
						Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key);
						q.type_ = QueryDelete;
						err = tr.Modify(std::move(q));
						ASSERT_TRUE(err.ok()) << err.what();
						break;
					}

					client::Item item = tr.NewItem();
					ASSERT_TRUE(item.Status().ok()) << item.Status().what();

					WrSerializer wrser;
					reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
					jsonBuilder.Put(kFieldId, int(id));
					jsonBuilder.Put(kFieldLocation, key);
					jsonBuilder.Put(kFieldData, RandString());
					jsonBuilder.End();

					err = item.FromJSON(wrser.Slice());
					ASSERT_TRUE(err.ok()) << err.what();

					if (mode == ModeUpsert) {
						err = tr.Upsert(std::move(item));
					} else if (mode == ModeDelete) {
						err = tr.Delete(std::move(item));
					}
					ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; shard = " << shard << "; key = " << key
										  << "; mode = " << mode << "; id = " << id;
				}
				client::SyncCoroQueryResults qrTx;
				err = rx->CommitTransaction(tr, qrTx);
				ASSERT_TRUE(err.ok()) << err.what() << "; connection = " << i << "; shard = " << shard << "; mode = " << mode << std::endl;

				client::SyncCoroQueryResults qr;

				err = rx->Select(Query(default_namespace).Where(kFieldLocation, CondEq, key), qr);
				ASSERT_TRUE(err.ok()) << err.what();
				if (mode == ModeUpsert) {
					ASSERT_EQ(qr.Count(), rowsInTr) << "; connection = " << i << "; shard = " << shard << "; location = " << key;
				} else if (mode == ModeDelete) {
					ASSERT_EQ(qr.Count(), 0) << "; connection = " << i << "; shard = " << shard << "; location = " << key;
				}

				if (mode == ModeUpsert) {
					const string updated = "updated_" + RandString();
					tr = rx->NewTransaction(default_namespace);
					ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
					Query q = Query(default_namespace).Set(kFieldData, updated).Where(kFieldLocation, CondEq, key);
					q.type_ = QueryUpdate;
					err = tr.Modify(std::move(q));
					ASSERT_TRUE(err.ok()) << err.what();
					client::SyncCoroQueryResults qrTx;
					err = rx->CommitTransaction(tr, qrTx);
					ASSERT_TRUE(err.ok()) << err.what();

					qr = client::SyncCoroQueryResults();
					err = rx->Select(Query(default_namespace).Where(kFieldLocation, CondEq, key), qr);
					ASSERT_TRUE(err.ok()) << err.what();
					std::string toFind = "\"location\":\"" + key + "\"";
					for (auto it : qr) {
						auto item = it.GetItem();
						std::string_view json = item.GetJSON();
						ASSERT_TRUE(json.find(toFind) != std::string_view::npos) << json;
					}
				}
			}

			CheckTransactionErrors(*rx, default_namespace);
		}
	}
}

TEST_F(ShardingApi, Reconnect) {
	Init();
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	for (size_t shard = 1; shard < kShards; ++shard) {
		for (size_t clusterNodeId = 0; clusterNodeId < kNodesInCluster; ++clusterNodeId) {
			const auto server = shard * kNodesInCluster + clusterNodeId;
			ASSERT_TRUE(StopByIndex(server));
			if (clusterNodeId) {
				AwaitOnlineReplicationStatus(server - 1);
			}
			Error err;
			const std::string location = "key" + std::to_string(shard);
			bool succeed = false;
			for (size_t i = 0; i < 10; ++i) {  // FIXME: Max retries count should be 1
				client::SyncCoroQueryResults qr;
				err = rx->Select(Query(default_namespace).Where(kFieldLocation, CondEq, location), qr);
				if (err.ok()) {	 // First request may get an error, because disconnect event may not be handled yet
					ASSERT_EQ(qr.Count(), 40) << "; shard = " << shard << "; node = " << clusterNodeId;
					succeed = true;
					break;
				}
			}
			ASSERT_TRUE(succeed) << err.what() << "; shard = " << shard << "; node = " << clusterNodeId;

			client::SyncCoroQueryResults qr;
			const std::string newValue = "most probably updated";
			err = rx->Update(Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard << "; node = " << clusterNodeId;
			ASSERT_EQ(qr.Count(), 40) << "; shard = " << shard << "; node = " << clusterNodeId;
			StartByIndex(server);
		}
	}
}

TEST_F(ShardingApi, ReconnectTimeout) {
	constexpr auto kTimeout = std::chrono::seconds(2);
	InitShardingConfig cfg;
	cfg.disableNetworkTimeout = true;
	Init(std::move(cfg));
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	const std::string newValue = "most probably updated";
	// Stop half of the clusters' nodes
	for (size_t shard = 1; shard < kShards; ++shard) {
		for (size_t clusterNodeId = 0; clusterNodeId < kNodesInCluster / 2; ++clusterNodeId) {
			ASSERT_TRUE(StopByIndex(shard, clusterNodeId));
		}
	}
	for (size_t shard = 1; shard < kShards; ++shard) {
		// Stop one more of the clusters' nodes. Now there is no consensus
		const auto server = shard * kNodesInCluster + kNodesInCluster / 2;
		ASSERT_TRUE(StopByIndex(server));
		const std::string location = "key" + std::to_string(shard);
		client::SyncCoroQueryResults qr;
		auto err = rx->WithTimeout(kTimeout).Update(
			Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
		if (err.code() != errTimeout && err.code() != errNetwork && err.code() != errUpdateReplication) {
			ASSERT_TRUE(false) << err.what() << "(" << err.code() << ")"
							   << "; shard = " << shard;
		}
	}

	for (size_t shard = 1; shard < kShards; ++shard) {
		const std::string location = "key" + std::to_string(shard);
		client::SyncCoroQueryResults qr;
		auto err = rx->WithTimeout(kTimeout).Update(
			Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
		ASSERT_EQ(err.code(), errTimeout) << err.what() << "; shard = " << shard;
	}
}

TEST_F(ShardingApi, MultithreadedReconnect) {
	const size_t kValidThreadsCount = 5;
	Init();
	const std::string kDSN = getNode(0)->kRPCDsn;
	auto upsertItemF = [&](int index, client::SyncCoroReindexer &rx) {
		client::Item item = rx.NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok());

		const string key = string("key" + std::to_string(kShards));

		WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, index);
		jsonBuilder.Put(kFieldLocation, key);
		jsonBuilder.Put(kFieldData, RandString());
		jsonBuilder.End();

		Error err = item.FromJSON(wrser.Slice());
		EXPECT_TRUE(err.ok()) << err.what();
		return rx.Upsert(default_namespace, item);
	};

	struct RxWithStatus {
		std::shared_ptr<client::SyncCoroReindexer> client = std::make_shared<client::SyncCoroReindexer>();
		std::atomic<int> errors = 0;
	};

	std::vector<RxWithStatus> rxClients(kValidThreadsCount);
	for (auto &rx : rxClients) {
		rx.client->Connect(kDSN);
		auto status = rx.client->Status();
		ASSERT_TRUE(status.ok()) << status.what();
	}

	for (size_t shard = 1; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard);
		// The last node won't be stopped because (in this case)
		// there will be no other nodes to reconnect to.
		std::cout << "Shard: " << shard << std::endl;
		std::vector<std::thread> anyResultThreads;
		std::atomic<bool> stop = {false};
		for (size_t i = 0; i < 3; ++i) {
			anyResultThreads.emplace_back(std::thread([&, index = i]() {
				client::SyncCoroReindexer rx;
				rx.Connect(kDSN);
				while (!stop) {
					upsertItemF(index, rx);
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			}));
			anyResultThreads.emplace_back(std::thread([&]() {
				client::SyncCoroReindexer rx;
				rx.Connect(kDSN);
				while (!stop) {
					client::SyncCoroQueryResults qr;
					Error err = rx.Select(Query(default_namespace).Where(kFieldLocation, CondEq, key), qr);
					if (err.ok()) {
						ASSERT_TRUE(qr.Count() == 40) << qr.Count();
					}
					qr = client::SyncCoroQueryResults();
					err = rx.Select(Query(default_namespace), qr);
					if (err.ok()) {
						ASSERT_GE(qr.Count(), 40 * kShards);
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
			}));
		}

		for (size_t clusterNodeId = 0; clusterNodeId < kNodesInCluster; ++clusterNodeId) {
			const auto server = shard * kNodesInCluster + clusterNodeId;
			std::cout << "Stopping: " << server << std::endl;
			ASSERT_TRUE(StopByIndex(server));
			if (clusterNodeId) {
				AwaitOnlineReplicationStatus(server - 1);
			}
			std::vector<std::thread> alwaysValidThreads;
			// std::atomic<int> errorsCount = {0};
			constexpr size_t kMaxErrors = 5;  // FIXME: Max errors should be 1
			for (size_t i = 0; i < kValidThreadsCount; ++i) {
				alwaysValidThreads.emplace_back(std::thread([&, currShard = shard, index = i, idx = i]() {
					auto &rx = rxClients[idx];
					bool succeed = false;
					Error err;
					for (size_t j = 0; j < kMaxErrors; ++j) {
						err = upsertItemF(index, *rx.client);
						if (err.ok()) {
							succeed = true;
							break;
						}
						// ASSERT_LE(++errorsCount, kMaxErrors) << err.what() << "; shard = " << currShard << "; location = " << key;
					}
					ASSERT_TRUE(succeed) << err.what() << "; shard = " << currShard << "; location = " << key;
				}));
				alwaysValidThreads.emplace_back(std::thread([&, currShard = shard, idx = i]() {
					auto &rx = rxClients[idx];
					bool succeed = false;
					Error err;
					for (size_t j = 0; j < kMaxErrors; ++j) {
						client::SyncCoroQueryResults qr;
						const auto query = Query(default_namespace).Where(kFieldLocation, CondEq, key);
						err = rx.client->Select(query, qr);
						if (err.ok()) {
							ASSERT_EQ(qr.Count(), 40) << "; shard = " << currShard;
							succeed = true;
							break;
						}
						// ASSERT_LE(++errorsCount, kMaxErrors) << err.what() << "; shard = " << currShard << "; location = " << key;
					}
					ASSERT_TRUE(succeed) << err.what() << "; shard = " << currShard;
				}));
			}
			for (auto &th : alwaysValidThreads) {
				th.join();
			}
			// ASSERT_LE(errorsCount, std::max(kMaxErrors, kValidThreadsCount)) << "Too many network errors: " << errorsCount.load();
			StartByIndex(server);
		}
		stop = true;
		for (auto &th : anyResultThreads) {
			th.join();
		}
	}
}

TEST_F(ShardingApi, ConfigYaml) {
	using namespace std::string_literals;
	using Cfg = reindexer::cluster::ShardingConfig;
	Cfg config;
	std::ifstream sampleFile{RX_SRC "/cluster/sharding/sharding.conf"};
	std::stringstream sample;
	std::string line;
	while (std::getline(sampleFile, line)) {
		const auto start = line.find_first_not_of(" \t");
		if (start != std::string::npos && line[start] != '#') {
			sample << line << '\n';
		}
	}

	struct {
		std::string yaml;
		std::variant<Cfg, Error> expected;
	} testCases[]{
		{"this_shard_id: 0", Error{errParams, "Version of sharding config file is not specified"}},
		{"version: 10", Error{errParams, "Unsupported version of sharding config file: 10"}},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    index: "location"
    keys:
      - shard_id: 1
        values:
          - "south"
          - "west"
shards:
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Default shard id is not specified for namespace 'best_namespace'"}
		},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: 1
    index: "location"
    keys:
      - shard_id: 1
        values:
          - "south"
          - "west"
shards:
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Dsns for shard id 1 are specified twice"}
		},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: 1
    index: "location"
    keys:
      - shard_id: 1
        values:
          - "south"
          - "west"
shards:
  - shard_id: 1
    dsns:
      - "127.0.0.1:19001/shard1"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Scheme of sharding dsn must be cproto: 127.0.0.1:19001/shard1"}
		},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: 0
    index: "location"
    keys:
      - shard_id: 1
        values:
          - "south"
          - "west"
shards:
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Default shard id should be defined in shards list. Undefined default shard id: 0, for namespace: best_namespace"}
		},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: 1
    index: "location"
    keys:
      - shard_id: 3
        values:
          - "south"
          - "west"
shards:
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Shard id 3 is not specified in the config but it is used in namespace keys"}
		},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: -1
    index: "location"
    keys:
      - shard_id: 2
        values:
          - "south"
          - "west"
shards:
  - shard_id: -1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Shard id should not be less than zero"}
		},
		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: 0
    index: "location"
    keys:
      - shard_id: 1
        values:
          - "south"
          - "west"
  - namespace: "best_namespacE"
    default_shard: 2
    index: "location2"
    keys:
      - shard_id: 1
        values:
          - "south2"
shards:
  - shard_id: 0
    dsns:
      - "cproto://127.0.0.1:19000/shard0"
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Namespace 'best_namespacE' already specified in the config."}

		},
		{R"(version: 1
namespaces:
  namespace: 
    - "a"
)"s,
		 Error{errParams, "'namespace' node must be scalar."}},
		{R"(version: 1
namespaces:
  - namespace: ""
)"s,
		 Error{errParams, "Namespace name incorrect ''."}},

		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    index: 
      - "location"
)"s,
		 Error{errParams, "'index' node must be scalar."}},

		{R"(version: 1
namespaces:
  - namespace: "best_namespace"
    default_shard: 0
    index: "location"
    keys:
      - shard_id: 1
        values:
          - "south"
          - "west"
      - shard_id: 2
        values:
          - "north"
shards:
  - shard_id: 0
    dsns:
      - "cproto://127.0.0.1:19000/shard0"
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19001/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19002/shard2"
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
proxy_conn_concurrency: 10
)"s,
		 Cfg{{{"best_namespace", "location", {Cfg::Key{1, ByValue, {"south", "west"}}, {2, ByValue, {"north"}}}, 0}},
			 {{0, {"cproto://127.0.0.1:19000/shard0"}}, {1, {"cproto://127.0.0.1:19001/shard1"}}, {2, {"cproto://127.0.0.2:19002/shard2"}}},
			 0,
			 std::chrono::milliseconds(5000),
			 std::chrono::seconds(25),
			 15,
			 10}},
		{R"(version: 1
namespaces:
  - namespace: "namespace1"
    default_shard: 0
    index: "count"
    keys:
      - shard_id: 1
        values:
          - 0
          - 10
          - 20
  - namespace: "namespace2"
    default_shard: 1
    index: "city"
    keys:
      - shard_id: 1
        values:
          - "Moscow"
      - shard_id: 2
        values:
          - "London"
      - shard_id: 3
        values:
          - "Paris"
shards:
  - shard_id: 0
    dsns:
      - "cproto://127.0.0.1:19000/shard0"
      - "cproto://127.0.0.1:19001/shard0"
      - "cproto://127.0.0.1:19002/shard0"
  - shard_id: 1
    dsns:
      - "cproto://127.0.0.1:19010/shard1"
      - "cproto://127.0.0.1:19011/shard1"
  - shard_id: 2
    dsns:
      - "cproto://127.0.0.2:19020/shard2"
  - shard_id: 3
    dsns:
      - "cproto://127.0.0.2:19030/shard3"
      - "cproto://127.0.0.2:19031/shard3"
      - "cproto://127.0.0.2:19032/shard3"
      - "cproto://127.0.0.2:19033/shard3"
this_shard_id: 0
reconnect_timeout_msec: 3000
shards_awaiting_timeout_sec: 30
proxy_conn_count: 6
proxy_conn_concurrency: 16
)"s,
		 Cfg{{{"namespace1", "count", {Cfg::Key{1, ByValue, {0, 10, 20}}}, 0},
			  {"namespace2", "city", {Cfg::Key{1, ByValue, {"Moscow"}}, {2, ByValue, {"London"}}, {3, ByValue, {"Paris"}}}, 1}},
			 {{0, {"cproto://127.0.0.1:19000/shard0", "cproto://127.0.0.1:19001/shard0", "cproto://127.0.0.1:19002/shard0"}},
			  {1, {"cproto://127.0.0.1:19010/shard1", "cproto://127.0.0.1:19011/shard1"}},
			  {2, {"cproto://127.0.0.2:19020/shard2"}},
			  {3,
			   {"cproto://127.0.0.2:19030/shard3", "cproto://127.0.0.2:19031/shard3", "cproto://127.0.0.2:19032/shard3",
				"cproto://127.0.0.2:19033/shard3"}}},
			 0}},
		{sample.str(), Cfg{{{"namespace1", "count", {Cfg::Key{1, ByValue, {0, 10, 20}}, {2, ByValue, {1, 2, 3, 4}}, {3, ByValue, {11}}}, 0},
							{"namespace2", "city", {Cfg::Key{1, ByValue, {"Moscow"}}, {2, ByValue, {"London"}}, {3, ByValue, {"Paris"}}}, 3}},
						   {{0, {"cproto://127.0.0.1:19000/shard0", "cproto://127.0.0.1:19001/shard0", "cproto://127.0.0.1:19002/shard0"}},
							{1, {"cproto://127.0.0.1:19010/shard1", "cproto://127.0.0.1:19011/shard1"}},
							{2, {"cproto://127.0.0.2:19020/shard2"}},
							{3,
							 {"cproto://127.0.0.2:19030/shard3", "cproto://127.0.0.2:19031/shard3", "cproto://127.0.0.2:19032/shard3",
							  "cproto://127.0.0.2:19033/shard3"}}},
						   0}}};

	for (const auto &[yaml, expected] : testCases) {
		const Error err = config.FromYML(yaml);
		if (std::holds_alternative<Cfg>(expected)) {
			const auto &cfg{std::get<Cfg>(expected)};
			EXPECT_TRUE(err.ok()) << err.what() << "\nYAML:\n" << yaml;
			if (err.ok()) {
				EXPECT_EQ(config, cfg) << yaml << "\nexpected:\n" << cfg.GetYml();
			}
			const auto generatedYml = cfg.GetYml();
			EXPECT_EQ(generatedYml, yaml);
		} else {
			EXPECT_FALSE(err.ok());
			EXPECT_EQ(err.what(), std::get<Error>(expected).what());
		}
	}
}

TEST_F(ShardingApi, ConfigJson) {
	using namespace std::string_literals;
	using Cfg = reindexer::cluster::ShardingConfig;
	Cfg config;

	struct {
		std::string json;
		Cfg expected;
	} testCases[]{
		{R"({"version":1,"namespaces":[{"namespace":"best_namespace","default_shard":0,"index":"location","keys":[{"shard_id":1,"values":["south","west"]},{"shard_id":2,"values":["north"]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19001/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.2:19002/shard2"]}],"this_shard_id":0,"reconnect_timeout_msec":5000,"shards_awaiting_timeout_sec":20,"proxy_conn_count":4,"proxy_conn_concurrency":16})"s,
		 Cfg{{{"best_namespace", "location", {Cfg::Key{1, ByValue, {"south", "west"}}, {2, ByValue, {"north"}}}, 0}},
			 {{0, {"cproto://127.0.0.1:19000/shard0"}}, {1, {"cproto://127.0.0.1:19001/shard1"}}, {2, {"cproto://127.0.0.2:19002/shard2"}}},
			 0,
			 std::chrono::milliseconds(5000),
			 std::chrono::seconds(20),
			 4}},
		{R"({"version":1,"namespaces":[{"namespace":"namespace1","default_shard":0,"index":"count","keys":[{"shard_id":1,"values":[0,10,20]},{"shard_id":2,"values":[1,5,7,9]},{"shard_id":3,"values":[100]}]},{"namespace":"namespace2","default_shard":3,"index":"city","keys":[{"shard_id":1,"values":["Moscow"]},{"shard_id":2,"values":["London"]},{"shard_id":3,"values":["Paris"]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0","cproto://127.0.0.1:19001/shard0","cproto://127.0.0.1:19002/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19010/shard1","cproto://127.0.0.1:19011/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.2:19020/shard2"]},{"shard_id":3,"dsns":["cproto://127.0.0.2:19030/shard3","cproto://127.0.0.2:19031/shard3","cproto://127.0.0.2:19032/shard3","cproto://127.0.0.2:19033/shard3"]}],"this_shard_id":0,"reconnect_timeout_msec":4000,"shards_awaiting_timeout_sec":30,"proxy_conn_count":3,"proxy_conn_concurrency":5})"s,
		 Cfg{{{"namespace1", "count", {Cfg::Key{1, ByValue, {0, 10, 20}}, {2, ByValue, {1, 5, 7, 9}}, {3, ByValue, {100}}}, 0},
			  {"namespace2", "city", {Cfg::Key{1, ByValue, {"Moscow"}}, {2, ByValue, {"London"}}, {3, ByValue, {"Paris"}}}, 3}},
			 {{0, {"cproto://127.0.0.1:19000/shard0", "cproto://127.0.0.1:19001/shard0", "cproto://127.0.0.1:19002/shard0"}},
			  {1, {"cproto://127.0.0.1:19010/shard1", "cproto://127.0.0.1:19011/shard1"}},
			  {2, {"cproto://127.0.0.2:19020/shard2"}},
			  {3,
			   {"cproto://127.0.0.2:19030/shard3", "cproto://127.0.0.2:19031/shard3", "cproto://127.0.0.2:19032/shard3",
				"cproto://127.0.0.2:19033/shard3"}}},
			 0,
			 std::chrono::milliseconds(4000),
			 std::chrono::seconds(30),
			 3,
			 5}}};

	for (const auto &[json, cfg] : testCases) {
		const auto generatedJson = cfg.GetJson();
		EXPECT_EQ(generatedJson, json);

		const Error err = config.FromJson(json);
		EXPECT_TRUE(err.ok()) << err.what();
		if (err.ok()) {
			EXPECT_EQ(config, cfg) << json << "\nexpected:\n" << cfg.GetJson();
		}
	}
}

TEST_F(ShardingApi, ConfigKeyValues) {
	struct shardInfo {
		bool result;
		using ShardKeys = std::vector<std::string>;
		std::vector<ShardKeys> shards;
	};

	auto generateConfigYaml = [](const shardInfo &info) -> std::string {
		// clang-format off
		std::string conf =
			"version: 1\n"
			"namespaces:\n"
			"  - namespace: \"ns\"\n"
			"    default_shard: 0\n"
			"    index: \"location\"\n"
			"    keys:\n";
		// clang-format on
		for (size_t i = 0; i < info.shards.size(); i++) {
			conf += "      - shard_id:" + std::to_string(i + 1) + "\n";
			conf += "        values:\n";
			for (const auto &k : info.shards[i]) {
				conf += "          - " + k + "\n";
			}
		}
		conf += "shards:\n";
		for (size_t i = 0; i <= info.shards.size(); i++) {
			std::string indxStr = std::to_string(i);
			conf += "  - shard_id:" + indxStr + "\n";
			conf += "    dsns:\n";
			conf += "      - \"cproto://127.0.0.1:1900" + indxStr + "/shard" + indxStr + "\"\n";
		}
		conf += "this_shard_id: 0";
		return conf;
	};

	auto generateConfigJson = [](const shardInfo &info) -> std::string {
		// clang-format off
		std::string conf =
					"{\n"
					"\"version\": 1,\n"
					"\"namespaces\": [\n"
					"    {\n"
					"        \"namespace\": \"ns\",\n"
					"        \"default_shard\": 0\n"
					"        \"index\": \"location\",\n"
					"        \"keys\": [\n";

		// clang-format on
		for (size_t i = 0; i < info.shards.size(); i++) {
			conf += "            {\n";
			conf += "                \"shard_id\": " + std::to_string(i + 1) + ",\n";
			conf += "                \"values\": [\n";
			for (size_t j = 0; j < info.shards[i].size(); j++) {
				conf += "                    " + info.shards[i][j] + (j == info.shards[i].size() - 1 ? "\n" : ",\n");
			}
			conf += "                 ]\n";
			conf += (i == info.shards.size() - 1) ? "            }\n" : "            },\n";
		}

		conf += "        ]\n";
		conf += "   }\n";
		conf += "],\n";
		conf += "\"shards\": [\n";
		for (size_t i = 0; i <= info.shards.size(); i++) {
			std::string indxStr = std::to_string(i);
			conf += "    {\n";
			conf += "        \"shard_id\":" + indxStr + ",\n";
			conf += "        \"dsns\":[\n";
			conf += "            \"cproto://127.0.0.1:1900" + indxStr + "/shard" + indxStr + "\"\n";
			conf += "        ]\n";
			conf += (i == info.shards.size()) ? "    }\n" : "    },\n";
		}
		conf += "],\n";
		conf += "\"this_shard_id\": 0\n}";
		return conf;
	};

	// clang-format off
	std::vector<shardInfo> tests = {{true, {{"1", "2"}, {"3"}}},
									{true, {{"1.11", "2.22"}, {"3.33"}}},
									{true, {{"true"}, {"false"}}},
									{true, {{"\"key1\"", "\"key2\""}, {"\"key3\""}}},

									{false, {{"1", "2"}, {"true"}}},
									{false, {{"1", "false"}, {"3"}}},
									{false, {{"1", "2"}, {"1.3"}}},
									{false, {{"1", "1.2"}, {"3"}}},
									{false, {{"1", "2"}, {"3","\"string\""}}},
									{false, {{"1", "\"string\""}, {"3"}}},

									{false, {{"1.1", "1.2"}, {"true"}}},
									{false, {{"1.1", "false"}, {"1.3"}}},
									{false, {{"1.1", "1.2"}, {"3"}}},
									{false, {{"1.1", "2"}, {"1.3"}}},
									{false, {{"1.1", "1.2"}, {"1.3","\"string\""}}},
									{false, {{"1.1", "\"string\""}, {"1.3"}}},

									{false, {{"false", "1.2"}, {"true"}}},
									{false, {{"false" }, {"1"}}},
									{false, {{"false", "1.2"}, {"true"}}},
									{false, {{"false", "\"string\""}, {"true"}}},
									{false, {{"false", }, {"true","\"string\""}}},


									{false, {{"1.11", "2.22"}, {"3.33","1.11"}}},
									{false, {{"true"}, {"false","true"}}},
									{false, {{"\"key1\"", "\"key2\""}, {"\"key1\"","\"key3\""}}},
									{false, {{"\"key1\"", "\"key2\"","\"key1\""}, {"\"key3\""}}},
								};
	// clang-format on

	for (const auto &test : tests) {
		std::string conf = generateConfigJson(test);
		reindexer::cluster::ShardingConfig config;
		Error err = config.FromJson(conf);
		EXPECT_TRUE(err.ok() == test.result) << err.what() << "\nconfig:\n" << conf;
	}

	tests.push_back({true, {{"key1", "key2"}, {"key3"}}});
	tests.push_back({true, {{"key1", "\"key2\""}, {"\"key3\""}}});
	tests.push_back({false, {{"1", "2"}, {"3", "string"}}});
	tests.push_back({false, {{"key1", "key2"}, {"key1", "key3"}}});

	for (const auto &test : tests) {
		std::string conf = generateConfigYaml(test);
		reindexer::cluster::ShardingConfig config;
		Error err = config.FromYML(conf);
		EXPECT_TRUE(err.ok() == test.result) << err.what() << "\nconfig:\n" << conf;
	}
}

TEST_F(ShardingApi, RestrictionOnRequest) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));

	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;
	{
		client::SyncCoroQueryResults qr;
		Query q;
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "<'key3'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::SyncCoroQueryResults qr;
		Query q;
		// key3 - proxy node
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "='key3'" + " and " + kFieldLocation + "='key2'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::SyncCoroQueryResults qr;
		Query q;
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "='key1'" + " and " + kFieldLocation + "='key2'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}

	{
		client::SyncCoroQueryResults qr;
		Query q;
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "='key1'" + " or " + kFieldLocation + "='key2'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::SyncCoroQueryResults qr;
		Query q(default_namespace);
		q.Where(kFieldLocation, CondEq, {"key1", "key2"});
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
}

TEST_F(ShardingApi, DiffTmInResultFromShards) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));

	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;

	const std::map<int, std::map<std::string, std::string>> sampleData = {
		{1, {{kFieldLocation, "key2"}, {kFieldData, RandString()}, {"f1", RandString()}}},
		{2, {{kFieldLocation, "key1"}, {kFieldData, RandString()}, {"f2", RandString()}}}};

	auto insertItem = [this, rx](int id, const std::map<std::string, std::string> &data) {
		client::Item item = rx->NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok());
		WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, id);
		for (const auto &[key, value] : data) {
			jsonBuilder.Put(key, value);
		}
		jsonBuilder.End();
		Error err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	};

	for (const auto &s : sampleData) {
		insertItem(s.first, s.second);
	}

	client::SyncCoroQueryResults qr;
	Query q = Query(default_namespace);

	auto err = rx->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qr) {
		auto item = it.GetItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		auto root = parser.Parse(item.GetJSON());
		const auto id = root[kFieldId].As<int>(-1);
		ASSERT_NE(id, -1);
		auto itData = sampleData.find(id);
		ASSERT_NE(itData, sampleData.end());
		for (const auto &[key, value] : itData->second) {
			ASSERT_EQ(root[key].As<std::string>(), value);
		}
	}
}

TEST_F(ShardingApi, Shutdown) {
	Init();

	std::vector<std::thread> threads;
	std::atomic<int> counter{0};
	std::atomic<bool> done = {false};
	constexpr auto kSleepTime = std::chrono::milliseconds(10);
	const auto kNsName = default_namespace;

	auto addItemFn = [this, &counter, &done, kSleepTime](size_t shardId, size_t node) noexcept {
		while (!done) {
			AddRow(shardId, node, counter++);
			std::this_thread::sleep_for(kSleepTime);
		}
	};
	for (size_t j = 0; j < kNodesInCluster; ++j) {
		for (size_t i = 0; i < kShards; ++i) {
			threads.emplace_back(addItemFn, i, j);
		}
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	for (size_t j = 0; j < kNodesInCluster; ++j) {
		for (size_t i = 0; i < kShards; ++i) {
			std::cout << "Stopping shard " << i << ", node " << j << std::endl;
			ASSERT_TRUE(StopSC(i, j));
		}
	}
	done = true;
	for (auto &th : threads) {
		th.join();
	}
}

TEST_F(ShardingApi, SelectOffsetLimit) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	Init(std::move(cfg));

	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;

	const unsigned long kMaxCountOnShard = 40;
	Fill(rx, "key3", 0, kMaxCountOnShard);
	Fill(rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
	Fill(rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);

	waitSync(default_namespace);

	struct LimitOffsetCase {
		unsigned offset;
		unsigned limit;
		unsigned count;
	};

	const std::vector<LimitOffsetCase> variants = {{0, 10, 10},	 {0, 40, 40},  {0, 120, 120}, {0, 130, 120}, {10, 10, 10},
												   {10, 50, 50}, {10, 70, 70}, {50, 70, 70},  {50, 100, 70}, {119, 1, 1},
												   {119, 10, 1}, {0, 0, 0},	   {120, 10, 0},  {150, 10, 0}};

	auto execVariant = [rx, this](const LimitOffsetCase &v, bool checkCount) {
		Query q;
		q.FromSQL("select * from " + default_namespace + " limit " + std::to_string(v.limit) + " offset " + std::to_string(v.offset));
		if (checkCount) {
			q.ReqTotal();
		}
		client::SyncCoroQueryResults qr;
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), v.count) << q.GetSQL();
		if (checkCount) {
			ASSERT_EQ(qr.TotalCount(), kMaxCountOnShard * kShards) << q.GetSQL();
		}
		int n = 0;
		for (auto i = qr.begin(); i != qr.end(); ++i, ++n) {
			auto item = i.GetItem();
			std::string_view json = item.GetJSON();
			gason::JsonParser parser;
			auto root = parser.Parse(json);
			ASSERT_EQ(root["id"].As<int>(), v.offset + n) << q.GetSQL();
		}
	};

	for (const auto &v : variants) {
		execVariant(v, true);
		execVariant(v, false);
	}
}

TEST_F(ShardingApi, QrContainCorrectShardingId) {
	// Check shard IDs, item IDs and LSN in responces
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(cfg);
	const unsigned int kShardCount = cfg.shards;
	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;
	Error err = rx->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	const unsigned long kMaxCountOnShard = 40;
	Fill(rx, "key0", 0, kMaxCountOnShard);
	Fill(rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
	Fill(rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);

	waitSync(default_namespace);

	std::vector<fast_hash_set<int64_t>> lsnsByShard;

	// Check selects
	for (unsigned int k = 0; k < kShardCount; k++) {
		std::shared_ptr<client::SyncCoroReindexer> rxSel = svc_[k][0].Get()->api.reindexer;
		{
			lsnsByShard.clear();
			lsnsByShard.resize(kShards);
			Query q;
			q.FromSQL("select * from " + default_namespace);
			client::SyncCoroQueryResults qr(kResultsWithItemID | kResultsCJson | kResultsWithPayloadTypes);

			err = rxSel->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			for (auto i = qr.begin(); i != qr.end(); ++i) {
				auto item = i.GetItem();
				std::string_view json = item.GetJSON();
				gason::JsonParser parser;
				auto root = parser.Parse(json);
				unsigned int id = root["id"].As<int>();
				int shardId = item.GetShardID();
				if (id < kMaxCountOnShard) {
					ASSERT_EQ(shardId, 0);
				} else if (id < 2 * kMaxCountOnShard) {
					ASSERT_EQ(shardId, 1);
				} else if (id < 3 * kMaxCountOnShard) {
					ASSERT_EQ(shardId, 2);
				}
				lsn_t lsn = item.GetLSN();
				ASSERT_FALSE(lsn.isEmpty());
				auto res = lsnsByShard[shardId].emplace(int64_t(lsn));	// Check if lsn is unique and insert it into map
				ASSERT_TRUE(res.second) << lsn;
			}
		}
		lsnsByShard.clear();
		lsnsByShard.resize(kShards);
		for (unsigned int l = 0; l < kShardCount; l++) {
			Query q;
			q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + " = 'key" + std::to_string(l) + "'");
			client::SyncCoroQueryResults qr(kResultsWithItemID | kResultsCJson | kResultsWithPayloadTypes);
			err = rxSel->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; " << l;
			for (auto i = qr.begin(); i != qr.end(); ++i) {
				auto item = i.GetItem();
				int shardId = item.GetShardID();
				ASSERT_EQ(shardId, l);
				lsn_t lsn = item.GetLSN();
				ASSERT_FALSE(lsn.isEmpty());
				auto res = lsnsByShard[shardId].emplace(int64_t(lsn));
				ASSERT_TRUE(res.second) << lsn;
			}
		}
	}

	// Check updates
	for (unsigned int k = 0; k < kShardCount; k++) {
		std::shared_ptr<client::SyncCoroReindexer> rxUpdate = svc_[k][0].Get()->api.reindexer;
		for (int l = 0; l < 3; l++) {
			Query q;
			q.FromSQL("update " + default_namespace + " set " + kFieldData + "='datanew' where " + kFieldLocation + " = 'key" +
					  std::to_string(l) + "'");
			client::SyncCoroQueryResults qr(kResultsWithItemID | kResultsCJson | kResultsWithPayloadTypes);
			err = rxUpdate->Update(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			for (auto i = qr.begin(); i != qr.end(); ++i) {
				auto item = i.GetItem();
				int shardId = item.GetShardID();
				ASSERT_EQ(shardId, l);
				lsn_t lsn = item.GetLSN();
				ASSERT_FALSE(lsn.isEmpty());
				auto res = lsnsByShard[shardId].emplace(int64_t(lsn));
				ASSERT_TRUE(res.second) << lsn;
			}
		}
	}

	// Check deletes
	lsnsByShard.clear();
	lsnsByShard.resize(kShards);
	for (unsigned int k = 0; k < kShardCount; k++) {
		std::shared_ptr<client::SyncCoroReindexer> rxDelete = svc_[k][0].Get()->api.reindexer;
		for (unsigned int l = 0; l < kShardCount; l++) {
			Query q;
			q.FromSQL("Delete from " + default_namespace + " where " + kFieldLocation + " = 'key" + std::to_string(l) + "'");
			client::SyncCoroQueryResults qr(kResultsWithItemID | kResultsCJson | kResultsWithPayloadTypes);
			err = rxDelete->Delete(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kMaxCountOnShard);
			for (auto i = qr.begin(); i != qr.end(); ++i) {
				auto item = i.GetItem();
				int shardId = item.GetShardID();
				ASSERT_EQ(shardId, l);
				lsn_t lsn = item.GetLSN();
				ASSERT_FALSE(lsn.isEmpty());
				auto res = lsnsByShard[shardId].emplace(int64_t(lsn));
				ASSERT_TRUE(res.second) << lsn;
			}
		}
		Fill(rx, "key0", 0, kMaxCountOnShard);
		Fill(rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
		Fill(rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);
		waitSync(default_namespace);
	}

	// Check transactions
	for (unsigned int k = 0; k < kShardCount; k++) {
		std::shared_ptr<client::SyncCoroReindexer> rxTx = svc_[k][0].Get()->api.reindexer;
		err = rxTx->TruncateNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		int startId = 0;
		for (unsigned int l = 0; l < kShardCount; l++) {
			auto tx = rxTx->NewTransaction(default_namespace);
			auto FillTx = [&](std::string_view key, const size_t from, const size_t count) {
				for (size_t index = from; index < from + count; ++index) {
					client::Item item = tx.NewItem();
					ASSERT_TRUE(item.Status().ok()) << item.Status().what();
					WrSerializer wrser;
					reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
					jsonBuilder.Put(kFieldId, int(index));
					jsonBuilder.Put(kFieldLocation, key);
					jsonBuilder.Put(kFieldData, RandString());
					jsonBuilder.Put(kFieldFTData, RandString());
					jsonBuilder.End();

					Error err = item.FromJSON(wrser.Slice());
					ASSERT_TRUE(err.ok()) << err.what();
					err = tx.Insert(std::move(item));
					ASSERT_TRUE(err.ok()) << err.what();
				}
			};

			FillTx("key" + std::to_string(l), startId, kMaxCountOnShard);
			startId += kMaxCountOnShard;
			client::SyncCoroQueryResults qr;
			err = rxTx->CommitTransaction(tx, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kMaxCountOnShard);
			for (auto i = qr.begin(); i != qr.end(); ++i) {
				auto item = i.GetItem();
				int shardId = item.GetShardID();
				ASSERT_EQ(shardId, l);
				lsn_t lsn = item.GetLSN();
				ASSERT_TRUE(lsn.isEmpty());	 // Transactions does not send lsn back
			}
		}
	}

	// Check single insertions
	for (unsigned int k = 0; k < kShardCount; k++) {
		std::shared_ptr<client::SyncCoroReindexer> rxTx = svc_[k][0].Get()->api.reindexer;
		err = rxTx->TruncateNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		for (unsigned int l = 0; l < kShardCount; l++) {
			WrSerializer wrser;
			client::Item item = CreateItem(rx, "key" + std::to_string(l), 1000 + k, wrser);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			err = rx->Upsert(default_namespace, item);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(item.GetShardID(), l) << k;
			ASSERT_TRUE(item.GetLSN().isEmpty()) << k;	// Item without precepts does not have lsn

			item = CreateItem(rx, "key" + std::to_string(l), 1000 + k, wrser);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			item.SetPrecepts({"id=SERIAL()"});
			err = rx->Upsert(default_namespace, item);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(item.GetShardID(), l) << k;
			lsn_t lsn = item.GetLSN();
			ASSERT_FALSE(lsn.isEmpty()) << k;  // Item with precepts has lsn
			auto res = lsnsByShard[l].emplace(int64_t(lsn));
			ASSERT_TRUE(res.second) << lsn;
		}
	}
}

static void ValidateNamespaces(size_t shard, const std::vector<std::string> &expected, const std::vector<NamespaceDef> &actual) {
	if (actual.size() == expected.size()) {
		bool hasUnexpectedNamespaces = false;
		for (auto &ns : actual) {
			auto found = std::find(expected.begin(), expected.end(), ns.name);
			if (found == expected.end()) {
				hasUnexpectedNamespaces = true;
				break;
			}
		}
		if (!hasUnexpectedNamespaces) {
			return;
		}
	}
	std::cout << "Expected:\n";
	for (auto &ns : expected) {
		std::cout << ns << std::endl;
	}
	std::cout << "Actual:\n";
	for (auto &ns : actual) {
		std::cout << ns.name << std::endl;
	}
	ASSERT_TRUE(false) << "shard: " << shard;
}

TEST_F(ShardingApi, AwaitShards) {
	const std::string kNewNs = "new_namespace";
	constexpr size_t kThreads = 5;
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.additionalNss = {kNewNs};
	Init(std::move(cfg));

	std::mutex mtx;
	bool ready = false;
	std::condition_variable cv;
	const std::vector<std::string> kNamespaces = {default_namespace, kNewNs};

	for (size_t shard = 1; shard < kShards; ++shard) {
		Stop(shard);
	}

	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	std::vector<std::thread> tds;
	for (size_t i = 0; i < kThreads; ++i) {
		tds.emplace_back([&] {
			std::unique_lock lck(mtx);
			cv.wait(lck, [&ready] { return ready; });
			lck.unlock();
			for (auto &ns : kNamespaces) {
				auto err = rx->OpenNamespace(ns);
				ASSERT_TRUE(err.ok()) << "Namespace: " << ns << "; " << err.what();
			}
		});
	}

	std::unique_lock lck(mtx);
	ready = true;
	lck.unlock();
	cv.notify_all();
	std::this_thread::yield();
	for (size_t shard = 1; shard < kShards; ++shard) {
		Start(shard);
	}
	for (auto &th : tds) {
		th.join();
	}

	std::vector<NamespaceDef> nsDefs;
	for (size_t shard = 0; shard < kShards; ++shard) {
		rx = getNode(shard)->api.reindexer;
		nsDefs.clear();
		auto err = rx->WithShardId(ShardingKeyType::ProxyOff, false).EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << "shard: " << shard << "; " << err.what();
		ValidateNamespaces(shard, kNamespaces, nsDefs);
	}
}

TEST_F(ShardingApi, AwaitShardsTimeout) {
	const std::string kNewNs = "new_namespace";
	constexpr size_t kThreads = 2;	// TODO: Add more threads, when multiple connect will be used in sharding proxy

	InitShardingConfig cfg;
	cfg.additionalNss = {kNewNs};
	cfg.awaitTimeout = std::chrono::seconds(3);
	Init(std::move(cfg));

	for (size_t shard = 1; shard < kShards; ++shard) {
		Stop(shard);
	}

	std::atomic<bool> done = false;
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	std::vector<std::thread> tds;
	for (size_t i = 0; i < kThreads; ++i) {
		tds.emplace_back([&] {
			auto err = rx->OpenNamespace(kNewNs);
			ASSERT_FALSE(err.ok());
			ASSERT_EQ(err.code(), errTimeout);
		});
	}
	std::thread cancelingThread = std::thread([&] {
		auto time = std::chrono::seconds(30);
		constexpr auto kStep = std::chrono::seconds(1);
		while (!done && time.count() > 0) {
			time -= kStep;
			std::this_thread::sleep_for(kStep);
		}
		if (!done) {
			Stop(0);
			ASSERT_TRUE(false) << "Timeout expired";
		}
	});

	for (auto &th : tds) {
		th.join();
	}
	done = true;
	cancelingThread.join();

	std::vector<NamespaceDef> nsDefs;
	for (size_t shard = 0; shard < kShards; ++shard) {
		rx = getNode(shard)->api.reindexer;
		nsDefs.clear();
		auto err = rx->WithShardId(ShardingKeyType::ProxyOff, false).EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << "shard: " << shard << "; " << err.what();
		ValidateNamespaces(shard, {default_namespace}, nsDefs);
	}
}

TEST_F(ShardingApi, StrictMode) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	Init(std::move(cfg));

	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;
	const std::string kFieldForSingleShard = "my_new_field";
	const std::string kUnknownField = "unknown_field";
	const std::string kValue = "value";

	client::Item item = rx->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok());
	WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
	jsonBuilder.Put(kFieldId, 0);
	jsonBuilder.Put(kFieldLocation, "key1");
	jsonBuilder.Put(kFieldForSingleShard, kValue);
	jsonBuilder.End();
	Error err = item.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx->Upsert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	waitSync(default_namespace);

	// Valid requests
	for (size_t node = 0; node < NodesCount(); ++node) {
		{  // Select from shard with kFieldForSingleShard
			client::SyncCoroQueryResults qr;
			const Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(kFieldForSingleShard, CondEq, kValue);
			err = getNode(node)->api.reindexer->Select(q, qr);
			EXPECT_TRUE(err.ok()) << err.what() << "; node = " << node;
			ASSERT_EQ(qr.Count(), 1) << "node = " << node;
			ASSERT_EQ(qr.begin().GetItem().GetJSON(), item.GetJSON()) << "node = " << node;
		}
		{  // Distributed select
			client::SyncCoroQueryResults qr;
			const Query q = Query(default_namespace).Where(kFieldForSingleShard, CondEq, "value");
			err = getNode(node)->api.reindexer->Select(q, qr);
			EXPECT_TRUE(err.ok()) << err.what() << "; node = " << node;
			ASSERT_EQ(qr.Count(), 1) << "node = " << node;
			ASSERT_EQ(qr.begin().GetItem().GetJSON(), item.GetJSON()) << "node = " << node;
		}
	}

	for (size_t node = 0; node < NodesCount(); ++node) {
		{  // Select from shard without kFieldForSingleShard
			client::SyncCoroQueryResults qr;
			const Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key2").Where(kFieldForSingleShard, CondEq, kValue);
			err = getNode(node)->api.reindexer->Select(q, qr);
			EXPECT_EQ(err.code(), errStrictMode) << err.what() << "; node = " << node;
		}
		{  // Select from shard without kFieldForSingleShard
			client::SyncCoroQueryResults qr;
			const Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key3").Where(kFieldForSingleShard, CondEq, kValue);
			err = getNode(node)->api.reindexer->Select(q, qr);
			EXPECT_EQ(err.code(), errStrictMode) << err.what() << "; node = " << node;
		}
		{  // Distributed select with unknown field
			client::SyncCoroQueryResults qr;
			const Query q = Query(default_namespace).Where(kUnknownField, CondEq, 1);
			err = getNode(node)->api.reindexer->Select(q, qr);
			EXPECT_EQ(err.code(), errStrictMode) << err.what() << "; node = " << node;
		}
	}
}

TEST_F(ShardingApi, NoShardingIndex) {
	InitShardingConfig cfg;
	cfg.createAdditionalIndexes = false;
	Init(std::move(cfg));

	auto rx = getNode(0)->api.reindexer;
	std::vector<NamespaceDef> nss;
	auto err = rx->EnumNamespaces(nss, EnumNamespacesOpts().HideSystem().HideTemporary());
	ASSERT_TRUE(err.ok());
	ASSERT_EQ(nss.size(), 1);
	ASSERT_EQ(nss[0].name, default_namespace);
	ASSERT_EQ(nss[0].indexes.size(), 1);
	ASSERT_EQ(nss[0].indexes[0].name_, kFieldId);

	// Check data with proxied queries
	for (size_t i = 0; i < NodesCount(); ++i) {
		rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			err = rx->Select(Query(default_namespace).Where(kFieldLocation, CondEq, key), qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key << std::endl;
			ASSERT_EQ(qr.Count(), 40);
			for (auto it : qr) {
				auto item = it.GetItem();
				std::string_view json = item.GetJSON();
				ASSERT_TRUE(json.find("\"location\":\"" + key + "\"") != std::string_view::npos) << json << "; key: " << key;
			}
		}
	}

	// Check data with local queries
	for (size_t i = 0; i < NodesCount(); ++i) {
		rx = getNode(i)->api.reindexer;
		const unsigned kShardId = i / kNodesInCluster;
		const std::string key = "key" + std::to_string(kShardId ? kShardId : 3);
		client::SyncCoroQueryResults qr;
		err = rx->WithShardId(ShardingKeyType::ProxyOff, false).Select(Query(default_namespace), qr);
		ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key << std::endl;
		ASSERT_EQ(qr.Count(), 40);
		for (auto it : qr) {
			auto item = it.GetItem();
			std::string_view json = item.GetJSON();
			ASSERT_TRUE(json.find("\"location\":\"" + key + "\"") != std::string_view::npos) << json << "; key: " << key;
		}
	}
}
