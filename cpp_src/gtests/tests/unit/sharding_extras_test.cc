#include "sharding_extras_api.h"

#ifndef REINDEX_WITH_TSAN

TEST_F(ShardingExtrasApi, LargeProxiedSelects) {
	// Check if qr streaming works (size of 21k item per shard does not allow to recieve all the data right after first request)
	const size_t kShardDataCount = 21000;
	fast_hash_map<int, std::string> insertedItemsById;

	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	cfg.rowsInTableOnShard = kShardDataCount;
	cfg.insertedItemsById = &insertedItemsById;
	Init(std::move(cfg));
	TestCout() << "Init done" << std::endl;

	// Distributed qr
	{
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace);

		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), kShardDataCount * kShards);
		TestCout() << "Distributed select done" << std::endl;
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

	TestCout() << "Validation for distributed select done" << std::endl;

	// Proxied qr
	{
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
		client::SyncCoroQueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key2");

		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), kShardDataCount);
		TestCout() << "Proxied select done" << std::endl;
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
	TestCout() << "Validation for proxied select done" << std::endl;
}
#endif	// REINDEX_WITH_TSAN

TEST_F(ShardingExtrasApi, SelectFTSeveralShards) {
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

TEST_F(ShardingExtrasApi, LocalQuery) {
	using namespace std::string_literals;
	InitShardingConfig cfg;
	Init(std::move(cfg));

	for (const char *localPreffix : {"local", "local explain", "explain local"}) {
		for (size_t i = 0; i < NodesCount(); ++i) {
			client::SyncCoroQueryResults localQr;
			Query localQuery;
			localQuery.FromSQL(localPreffix + " select * from "s + default_namespace);
			auto err = getNode(i)->api.reindexer->Select(localQuery, localQr);
			EXPECT_TRUE(err.ok());

			client::SyncCoroQueryResults shardQr;
			Query shardQuery = Query{default_namespace}.Where(kFieldLocation, CondEq, "key" + std::to_string((i % kNodesInCluster) + 1));
			err = getNode(rand() % NodesCount())->api.reindexer->Select(shardQuery, shardQr);
			EXPECT_TRUE(err.ok());

			EXPECT_EQ(localQr.Count(), shardQr.Count()) << " i = " << i;
		}
	}
	{
		Query localQuery;
		bool failed = false;
		try {
			localQuery.FromSQL("local update " + default_namespace);
		} catch (const Error &err) {
			failed = true;
			EXPECT_EQ(err.what(), "Syntax error at or near 'update', line: 1 column: 6 27; only SELECT query could be LOCAL");
		}
		EXPECT_TRUE(failed);
		localQuery = Query{default_namespace};
		localQuery.local_ = true;
		client::SyncCoroQueryResults localQr;
		const auto err = getNode(0)->api.reindexer->Update(localQuery, localQr);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Only SELECT query could be LOCAL");
	}
}

TEST_F(ShardingExtrasApi, JoinBetweenShardedAndNonSharded) {
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
	for (const bool local : {true, false}) {
		for (size_t i = 0; i < NodesCount(); ++i) {
			std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
			const std::string key = "key" + std::to_string(kShardWithLocalNs);
			client::SyncCoroQueryResults qr;
			Query q = Query(default_namespace)
						  .Local(local)
						  .Where(kFieldLocation, CondEq, key)
						  .InnerJoin(kFieldId, kFieldId, CondEq, Query(kLocalNamespace));
			err = rx->Select(q, qr);
			if (!local || getSCIdxs(i).first == kShardWithLocalNs) {
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
					EXPECT_NE(found, std::string_view::npos) << ser.Slice() << "; expected substring: " << kExpectedJoinResults1.at(id)
															 << "; i = " << i << "; location = " << key;
				}
			} else {
				ASSERT_FALSE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
				ASSERT_EQ(err.what(), "Namespace 'local_namespace' does not exist") << "i = " << i << "; location = " << key;
			}
		}
	}
	// Use local ns as left_ns (sharded ns has proper shardin key)
	for (const bool local : {true, false}) {
		for (size_t i = 0; i < NodesCount(); ++i) {
			std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
			const std::string key = "key" + std::to_string(kShardWithLocalNs);
			client::SyncCoroQueryResults qr;
			Query q = Query(kLocalNamespace)
						  .Local(local)
						  .InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace).Where(kFieldLocation, CondEq, key));
			err = rx->Select(q, qr);
			if (!local || getSCIdxs(i).first == kShardWithLocalNs) {
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
					EXPECT_NE(found, std::string_view::npos) << ser.Slice() << "; expected substring: " << kExpectedJoinResults2.at(id)
															 << "; i = " << i << "; location = " << key;
				}
			} else {
				ASSERT_FALSE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
				ASSERT_EQ(err.what(), "Namespace 'local_namespace' does not exist") << "i = " << i << "; location = " << key;
			}
		}
	}
	// Use local ns as left_ns (sharded ns does not have sharding key)
	for (const bool local : {true, false}) {
		for (size_t i = 0; i < NodesCount(); ++i) {
			std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
			client::SyncCoroQueryResults qr;
			Query q = Query(kLocalNamespace).Local(local).InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace));
			err = rx->Select(q, qr);
			if (!local) {
				ASSERT_EQ(err.code(), errLogic) << err.what() << "; i = " << i;
				ASSERT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE") << "; i = " << i;
			} else if (getSCIdxs(i).first == kShardWithLocalNs) {
				ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
				ASSERT_EQ(qr.Count(), kExpectedJoinResults2.size()) << "; i = " << i;
				for (auto it : qr) {
					WrSerializer ser;
					err = it.GetJSON(ser, false);
					ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
					gason::JsonParser parser;
					auto json = parser.Parse(ser.Slice());
					auto id = json["id"].As<int>(-1);
					auto found = ser.Slice().find(kExpectedJoinResults2.at(id));
					EXPECT_NE(found, std::string_view::npos)
						<< ser.Slice() << "; expected substring: " << kExpectedJoinResults2.at(id) << "; i = " << i;
				}
			} else {
				ASSERT_FALSE(err.ok()) << err.what() << "; i = " << i;
				ASSERT_EQ(err.what(), "Namespace 'local_namespace' does not exist") << "i = " << i;
			}
		}
	}
	// Use local ns as right_ns or left ns on wrong shard (this shard does not have this local namespace)
	for (const bool local : {true, false}) {
		for (size_t i = 0; i < NodesCount(); ++i) {
			std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
			const std::string key = "key" + std::to_string((kShardWithLocalNs + 1) % kShards);
			{
				client::SyncCoroQueryResults qr;
				Query q = Query(default_namespace)
							  .Local(local)
							  .Where(kFieldLocation, CondEq, key)
							  .InnerJoin(kFieldId, kFieldId, CondEq, Query(kLocalNamespace));
				err = rx->Select(q, qr);
				if (!local) {
					ASSERT_EQ(err.code(), errNotFound) << err.what() << "; i = " << i << "; location = " << key;
				} else if (getSCIdxs(i).first == kShardWithLocalNs) {
					ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
					ASSERT_EQ(qr.Count(), 0) << "; i = " << i << "; location = " << key;
				} else {
					ASSERT_FALSE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
					ASSERT_EQ(err.what(), "Namespace 'local_namespace' does not exist") << "i = " << i << "; location = " << key;
				}
			}
			{
				client::SyncCoroQueryResults qr;
				Query q = Query(kLocalNamespace)
							  .Local(local)
							  .InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace).Where(kFieldLocation, CondEq, key));
				err = rx->Select(q, qr);
				if (!local) {
					ASSERT_EQ(err.code(), errNotFound) << err.what() << "; i = " << i << "; location = " << key;
				} else if (getSCIdxs(i).first == kShardWithLocalNs) {
					ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
					ASSERT_EQ(qr.Count(), 0) << "; i = " << i << "; location = " << key;
				} else {
					ASSERT_FALSE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
					ASSERT_EQ(err.what(), "Namespace 'local_namespace' does not exist") << "i = " << i << "; location = " << key;
				}
			}
		}
	}
	// Use sharded ns as left_ns without sharding key
	for (const bool local : {true, false}) {
		for (size_t i = 0; i < NodesCount(); ++i) {
			std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
			client::SyncCoroQueryResults qr;
			Query q = Query(default_namespace).Local(local).InnerJoin(kFieldId, kFieldId, CondEq, Query(kLocalNamespace));
			err = rx->Select(q, qr);
			if (!local) {
				ASSERT_EQ(err.code(), errLogic) << err.what() << "; i = " << i;
				ASSERT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE") << "; i = " << i;
			} else if (getSCIdxs(i).first == kShardWithLocalNs) {
				ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
				ASSERT_EQ(qr.Count(), kExpectedJoinResults2.size()) << "; i = " << i;
				for (auto it : qr) {
					WrSerializer ser;
					err = it.GetJSON(ser, false);
					ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
					gason::JsonParser parser;
					auto json = parser.Parse(ser.Slice());
					auto id = json["id"].As<int>(-1);
					auto found = ser.Slice().find(kExpectedJoinResults1.at(id));
					EXPECT_NE(found, std::string_view::npos)
						<< ser.Slice() << "; expected substring: " << kExpectedJoinResults1.at(id) << "; i = " << i;
				}
			} else {
				ASSERT_FALSE(err.ok()) << err.what() << "; i = " << i;
				ASSERT_EQ(err.what(), "Namespace 'local_namespace' does not exist") << "i = " << i;
			}
		}
	}
}

TEST_F(ShardingExtrasApi, TagsMatcherConfusion) {
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

TEST_F(ShardingExtrasApi, DiffTmInResultFromShards) {
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

TEST_F(ShardingExtrasApi, QrContainCorrectShardingId) {
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
	Fill(default_namespace, rx, "key0", 0, kMaxCountOnShard);
	Fill(default_namespace, rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
	Fill(default_namespace, rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);

	waitSync(default_namespace);

	const std::map<std::string, int> flagSets = {{"No item IDs", kResultsCJson | kResultsWithPayloadTypes},
												 {"With item IDs", kResultsWithItemID | kResultsCJson | kResultsWithPayloadTypes}};
	auto hasIDFn = [](int flags) { return flags & kResultsWithItemID; };

	for (auto &[setName, flags] : flagSets) {
		TestCout() << "Flags set: " << setName << std::endl;
		std::vector<fast_hash_set<int64_t>> lsnsByShard;
		const bool isExpectingID = hasIDFn(flags);

		TestCout() << "Checking select queries" << std::endl;
		for (unsigned int k = 0; k < kShardCount; k++) {
			std::shared_ptr<client::SyncCoroReindexer> rxSel = svc_[k][0].Get()->api.reindexer;
			{
				lsnsByShard.clear();
				lsnsByShard.resize(kShards);
				Query q;
				q.FromSQL("select * from " + default_namespace);
				client::SyncCoroQueryResults qr(flags);

				err = rxSel->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				for (auto i = qr.begin(); i != qr.end(); ++i) {
					auto item = i.GetItem();
					std::string_view json = item.GetJSON();
					gason::JsonParser parser;
					auto root = parser.Parse(json);
					unsigned int id = root["id"].As<int>();
					int shardId = item.GetShardID();
					lsn_t lsn = item.GetLSN();
					if (isExpectingID) {
						if (id < kMaxCountOnShard) {
							ASSERT_EQ(shardId, 0);
						} else if (id < 2 * kMaxCountOnShard) {
							ASSERT_EQ(shardId, 1);
						} else if (id < 3 * kMaxCountOnShard) {
							ASSERT_EQ(shardId, 2);
						}
						ASSERT_FALSE(lsn.isEmpty());
						auto res = lsnsByShard[shardId].emplace(int64_t(lsn));	// Check if lsn is unique and insert it into map
						ASSERT_TRUE(res.second) << lsn;
					} else {
						ASSERT_EQ(shardId, ShardingKeyType::ProxyOff);
						ASSERT_TRUE(lsn.isEmpty()) << lsn;
						ASSERT_EQ(item.GetID(), -1);
					}
				}
			}
			lsnsByShard.clear();
			lsnsByShard.resize(kShards);
			for (unsigned int l = 0; l < kShardCount; l++) {
				Query q;
				q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + " = 'key" + std::to_string(l) + "'");
				client::SyncCoroQueryResults qr(flags);
				err = rxSel->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; " << l;
				for (auto i = qr.begin(); i != qr.end(); ++i) {
					auto item = i.GetItem();
					int shardId = item.GetShardID();
					lsn_t lsn = item.GetLSN();
					if (isExpectingID) {
						ASSERT_EQ(shardId, l) << "; " << k;
						ASSERT_FALSE(lsn.isEmpty());
						auto res = lsnsByShard[shardId].emplace(int64_t(lsn));
						ASSERT_TRUE(res.second) << lsn;
					} else {
						ASSERT_EQ(shardId, ShardingKeyType::ProxyOff);
						ASSERT_TRUE(lsn.isEmpty()) << lsn;
						ASSERT_EQ(item.GetID(), -1);
					}
				}
			}
		}

		TestCout() << "Checking update queries" << std::endl;
		for (unsigned int k = 0; k < kShardCount; k++) {
			std::shared_ptr<client::SyncCoroReindexer> rxUpdate = svc_[k][0].Get()->api.reindexer;
			for (int l = 0; l < 3; l++) {
				Query q;
				q.FromSQL("update " + default_namespace + " set " + kFieldData + "='datanew' where " + kFieldLocation + " = 'key" +
						  std::to_string(l) + "'");
				client::SyncCoroQueryResults qr(flags);
				err = rxUpdate->Update(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				for (auto i = qr.begin(); i != qr.end(); ++i) {
					auto item = i.GetItem();
					int shardId = item.GetShardID();
					lsn_t lsn = item.GetLSN();
					if (isExpectingID) {
						ASSERT_EQ(shardId, l) << "; " << k;
						ASSERT_FALSE(lsn.isEmpty());
						auto res = lsnsByShard[shardId].emplace(int64_t(lsn));
						ASSERT_TRUE(res.second) << lsn;
					} else {
						ASSERT_EQ(shardId, ShardingKeyType::ProxyOff);
						ASSERT_TRUE(lsn.isEmpty()) << lsn;
						ASSERT_EQ(item.GetID(), -1);
					}
				}
			}
		}

		TestCout() << "Checking delete queries" << std::endl;
		lsnsByShard.clear();
		lsnsByShard.resize(kShards);
		for (unsigned int k = 0; k < kShardCount; k++) {
			std::shared_ptr<client::SyncCoroReindexer> rxDelete = svc_[k][0].Get()->api.reindexer;
			for (unsigned int l = 0; l < kShardCount; l++) {
				Query q;
				q.FromSQL("Delete from " + default_namespace + " where " + kFieldLocation + " = 'key" + std::to_string(l) + "'");
				client::SyncCoroQueryResults qr(flags);
				err = rxDelete->Delete(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qr.Count(), kMaxCountOnShard);
				for (auto i = qr.begin(); i != qr.end(); ++i) {
					auto item = i.GetItem();
					int shardId = item.GetShardID();
					lsn_t lsn = item.GetLSN();
					if (isExpectingID) {
						ASSERT_EQ(shardId, l);
						ASSERT_FALSE(lsn.isEmpty()) << lsn;
						auto res = lsnsByShard[shardId].emplace(int64_t(lsn));
						ASSERT_TRUE(res.second) << lsn;
					} else {
						ASSERT_EQ(shardId, ShardingKeyType::ProxyOff);
						ASSERT_TRUE(lsn.isEmpty());
						ASSERT_EQ(item.GetID(), -1);
					}
				}
			}
			Fill(default_namespace, rx, "key0", 0, kMaxCountOnShard);
			Fill(default_namespace, rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
			Fill(default_namespace, rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);
			waitSync(default_namespace);
		}

		TestCout() << "Checking transactions" << std::endl;
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
				client::SyncCoroQueryResults qr(flags);
				err = rxTx->CommitTransaction(tx, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qr.Count(), kMaxCountOnShard);
				for (auto i = qr.begin(); i != qr.end(); ++i) {
					auto item = i.GetItem();
					int shardId = item.GetShardID();
					lsn_t lsn = item.GetLSN();
					if (isExpectingID) {
						ASSERT_EQ(shardId, l);
						ASSERT_TRUE(lsn.isEmpty());	 // Transactions does not send lsn back
					} else {
						ASSERT_EQ(shardId, ShardingKeyType::ProxyOff);
						ASSERT_TRUE(lsn.isEmpty()) << lsn;
						ASSERT_EQ(item.GetID(), -1);
					}
				}
			}
		}

		if (isExpectingID) {  // Single insertions do not have QR options. Only format is available
			TestCout() << "Checking insertions" << std::endl;
			for (unsigned int k = 0; k < kShardCount; k++) {
				std::shared_ptr<client::SyncCoroReindexer> rxTx = svc_[k][0].Get()->api.reindexer;
				err = rxTx->TruncateNamespace(default_namespace);
				ASSERT_TRUE(err.ok()) << err.what();
				for (unsigned int l = 0; l < kShardCount; l++) {
					WrSerializer wrser;
					client::Item item = CreateItem(default_namespace, rx, "key" + std::to_string(l), 1000 + k, wrser);
					ASSERT_TRUE(item.Status().ok()) << item.Status().what();
					err = rx->Upsert(default_namespace, item);
					ASSERT_TRUE(err.ok()) << err.what();
					ASSERT_EQ(item.GetShardID(), l) << k;
					ASSERT_TRUE(item.GetLSN().isEmpty()) << k;	// Item without precepts does not have lsn

					item = CreateItem(default_namespace, rx, "key" + std::to_string(l), 1000 + k, wrser);
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
	}
}

TEST_F(ShardingExtrasApi, StrictMode) {
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
	std::vector<size_t> limits = {UINT_MAX, 10};  // delete when executeQueryOnShard has one branch for distributed select
	for (auto l : limits) {
		// Valid requests
		for (size_t node = 0; node < NodesCount(); ++node) {
			{  // Select from shard with kFieldForSingleShard
				client::SyncCoroQueryResults qr;
				const Query q =
					Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(kFieldForSingleShard, CondEq, kValue).Limit(l);
				err = getNode(node)->api.reindexer->Select(q, qr);
				EXPECT_TRUE(err.ok()) << err.what() << "; node = " << node;
				ASSERT_EQ(qr.Count(), 1) << "node = " << node;
				ASSERT_EQ(qr.begin().GetItem().GetJSON(), item.GetJSON()) << "node = " << node;
			}
			{  // Distributed select
				client::SyncCoroQueryResults qr;
				const Query q = Query(default_namespace).Where(kFieldForSingleShard, CondEq, "value").Limit(l);
				err = getNode(node)->api.reindexer->Select(q, qr);
				EXPECT_TRUE(err.ok()) << err.what() << "; node = " << node;
				ASSERT_EQ(qr.Count(), 1) << "node = " << node;
				ASSERT_EQ(qr.begin().GetItem().GetJSON(), item.GetJSON()) << "node = " << node;
			}
		}

		for (size_t node = 0; node < NodesCount(); ++node) {
			{  // Select from shard without kFieldForSingleShard
				client::SyncCoroQueryResults qr;
				const Query q =
					Query(default_namespace).Where(kFieldLocation, CondEq, "key2").Where(kFieldForSingleShard, CondEq, kValue).Limit(l);
				err = getNode(node)->api.reindexer->Select(q, qr);
				EXPECT_EQ(err.code(), errStrictMode) << err.what() << "; node = " << node;
			}
			{  // Select from shard without kFieldForSingleShard
				client::SyncCoroQueryResults qr;
				const Query q =
					Query(default_namespace).Where(kFieldLocation, CondEq, "key3").Where(kFieldForSingleShard, CondEq, kValue).Limit(l);
				err = getNode(node)->api.reindexer->Select(q, qr);
				EXPECT_EQ(err.code(), errStrictMode) << err.what() << "; node = " << node;
			}
			{  // Distributed select with unknown field
				client::SyncCoroQueryResults qr;
				const Query q = Query(default_namespace).Where(kUnknownField, CondEq, 1).Limit(l);
				err = getNode(node)->api.reindexer->Select(q, qr);
				EXPECT_EQ(err.code(), errStrictMode) << err.what() << "; node = " << node;
			}
		}
	}
}

TEST_F(ShardingExtrasApi, NoShardingIndex) {
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

#ifdef RX_LOGACTIVITY

TEST_F(ShardingExtrasApi, DISABLED_ProxiedActivityState) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	Init(cfg);
	const unsigned int kShardCount = cfg.shards;
	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;
	Error err = rx->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	const unsigned long kMaxCountOnShard = 10;
	Fill(default_namespace, rx, "key0", 0, kMaxCountOnShard);
	Fill(default_namespace, rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
	Fill(default_namespace, rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);

	waitSync(default_namespace);

	int leaderId = -1;
	int followerId = -1;
	int curNode = 0;
	{
		client::SyncCoroQueryResults qr;
		err = svc_[0][curNode].Get()->api.reindexer->Select("select replication.clusterization_status.leader_id from #memstats", qr);
		ASSERT_EQ(qr.Count(), 1);
		ASSERT_TRUE(err.ok()) << err.what();
		auto item = qr.begin().GetItem();
		auto json = item.GetJSON();
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		leaderId = root["replication"]["clusterization_status"]["leader_id"].As<int>();
		if (leaderId == -1) {
			leaderId = curNode;
		}
		followerId = (leaderId + 1) % cfg.nodesInCluster;
	}

	auto setActivity = [](std::shared_ptr<reindexer::client::SyncCoroReindexer> rx, bool on) {
		client::SyncCoroQueryResults qr;
		Error err = rx->Select(fmt::format("update #config set profiling.activitystats={} where type='profiling'", on), qr);
		ASSERT_TRUE(err.ok()) << err.what();
	};
	auto dumpActivity = [](std::shared_ptr<reindexer::client::SyncCoroReindexer> rx) {
		client::SyncCoroQueryResults qr;
		Error err = rx->Select("select * from #activitystats", qr);
		ASSERT_TRUE(err.ok()) << err.what();
	};

	auto checkActivity = [](int followerId, const std::string &descr, const std::string &stateNameCheck) {
		std::string fileName = "activity_" + std::to_string(followerId) + ".json";
		std::ifstream f(fileName);
		std::stringstream buffer;
		buffer << f.rdbuf();
		gason::JsonParser parser;
		std::string jsonLog = buffer.str();
		auto root = parser.Parse(std::string_view(jsonLog));
		auto blocks = root["blocks"];
		bool isFind = false;
		for (auto &b : blocks) {
			if (b["description"].As<std::string>() == descr) {
				ASSERT_TRUE(!b["sub_block"].empty());
				auto arr1 = begin(b["sub_block"]);
				ASSERT_TRUE(arr1 != end(b["sub_block"]));
				auto v = *arr1;
				ASSERT_TRUE(!v["sub_block"].empty());
				auto arr2 = begin(v["sub_block"]);
				ASSERT_TRUE(arr2 != end(v["sub_block"]));
				std::string stateName = arr2->operator[]("State").As<std::string>();
				ASSERT_EQ(stateName, stateNameCheck);
				isFind = true;
				break;
			}
		}
		ASSERT_TRUE(isFind);
	};

	{
		setActivity(svc_[0][followerId].Get()->api.reindexer, true);
		Fill(default_namespace, svc_[0][followerId].Get()->api.reindexer, "key1", kMaxCountOnShard * kShardCount * 2, 1);
		dumpActivity(svc_[0][followerId].Get()->api.reindexer);
		checkActivity(followerId, "UPSERT INTO test_namespace WHERE id = 60 AND location = 'key1'", "proxied_via_sharding_proxy");
	}
	{
		setActivity(svc_[0][followerId].Get()->api.reindexer, false);
		setActivity(svc_[0][followerId].Get()->api.reindexer, true);

		Fill(default_namespace, svc_[0][followerId].Get()->api.reindexer, "key0", kMaxCountOnShard * kShardCount * 3, 1);
		dumpActivity(svc_[0][followerId].Get()->api.reindexer);

		checkActivity(followerId, "UPSERT INTO test_namespace WHERE id = 90 AND location = 'key0'", "proxied_via_cluster_proxy");
	}
	{
		setActivity(svc_[0][followerId].Get()->api.reindexer, false);
		setActivity(svc_[0][followerId].Get()->api.reindexer, true);

		client::SyncCoroQueryResults qr;
		err = svc_[0][followerId].Get()->api.reindexer->Select("select * from " + default_namespace, qr);
		ASSERT_TRUE(err.ok()) << err.what();

		dumpActivity(svc_[0][followerId].Get()->api.reindexer);
		checkActivity(followerId, "SELECT * FROM test_namespace", "proxied_via_sharding_proxy");
	}
}
#endif
