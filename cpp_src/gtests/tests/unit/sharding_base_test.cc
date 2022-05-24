#include <future>
#include "cluster/stats/replicationstats.h"
#include "core/itemimpl.h"
#include "sharding_api.h"

#include "server/pprof/gperf_profiler.h"
#include "tools/alloc_ext/tc_malloc_extension.h"

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

void ShardingApi::runSelectTest(std::string_view nsName) {
	TestCout() << "Running SelectTest" << std::endl;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			Query q = Query(std::string(nsName))
						  .Where(kFieldLocation, CondEq, key)
						  .InnerJoin(kFieldId, kFieldId, CondEq, Query(std::string(nsName)).Where(kFieldLocation, CondEq, key));

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

void ShardingApi::runUpdateTest(std::string_view nsName) {
	TestCout() << "Running UpdateTest" << std::endl;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			// key1, key2, key3(proxy shardId=0)
			{
				const std::string key = "key" + std::to_string(shard + 1);
				const std::string updated = "updated_" + RandString();
				client::SyncCoroQueryResults qr;
				Query q = Query(std::string(nsName)).Set(kFieldData, updated);
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
				Query qNoShardKey = Query(std::string(nsName)).Set(kFieldData, updatedErr);
				client::SyncCoroQueryResults qrErr;
				Error err = rx->Update(qNoShardKey, qrErr);
				ASSERT_FALSE(err.ok()) << err.what();
				Query qNoShardKeySelect = Query(std::string(nsName));
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

void ShardingApi::runDeleteTest(std::string_view nsName) {
	TestCout() << "Running DeleteTest" << std::endl;
	const int count = 120;
	int pos = count;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			std::string sql = "delete from " + std::string(nsName) + " where " + kFieldLocation + " = '" + key + "'";
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
		Fill(nsName, i / kNodesInCluster, pos, count);
		pos += count;
	}
}

void ShardingApi::runMetaTest(std::string_view nsName) {
	// User's meta must be duplicated on each shard
	TestCout() << "Running MetaTest" << std::endl;
	const std::string keyPrefix = "key";
	const std::string keyDataPrefix = "key_data";
	constexpr size_t kMetaCount = 10;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			if ((i == 0) && (shard == 0)) {
				for (size_t j = 0; j < kMetaCount; ++j) {
					Error err = rx->PutMeta(nsName, keyPrefix + std::to_string(j), keyDataPrefix + std::to_string(j));
					ASSERT_TRUE(err.ok()) << err.what();
				}
				waitSync(nsName);
			}
			for (size_t j = 0; j < kMetaCount; ++j) {
				std::string actualValue;
				const std::string properValue = keyDataPrefix + std::to_string(j);
				Error err = rx->GetMeta(nsName, keyPrefix + std::to_string(j), actualValue);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(properValue, actualValue) << "i = " << i << "; j = " << j << "; shard = " << shard;

				// Check meta vector for each shard
				std::vector<ShardedMeta> sharded;
				err = rx->GetMeta(nsName, keyPrefix + std::to_string(j), sharded);
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

void ShardingApi::runSerialTest(std::string_view nsName) {
	// _SERIAL_* meta must be independent for each shard
	TestCout() << "Running SerialTest" << std::endl;
	Error err;
	const std::vector<size_t> kItemsCountByShard = {10, 9, 8};
	const std::string kSerialFieldName = "linearValues";
	const std::string kSerialMetaKey = "_SERIAL_" + kSerialFieldName;
	std::unordered_map<std::string, int> shardsUniqueItems;
	std::vector<size_t> serialByShard(kItemsCountByShard.size(), 0);
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		if (i == 0) {
			err = rx->AddIndex(nsName, IndexDef{kSerialFieldName, "hash", "int", IndexOpts()});
			ASSERT_TRUE(err.ok()) << err.what();
			waitSync(nsName);
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
				reindexer::client::Item item = rx->NewItem(nsName);
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

				err = rx->Upsert(nsName, item);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();
				gason::JsonParser parser;
				auto itemJson = parser.Parse(item.GetJSON());
				unsigned int serialField = itemJson[kSerialFieldName].As<int>(-1);
				ASSERT_EQ(serialField, serialByShard[shard]);

				Query q = Query(std::string(nsName)).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(j));
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
	waitSync(nsName);
	// Validate _SERIAL_* values for each shard
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		std::vector<ShardedMeta> sharded;
		err = rx->GetMeta(nsName, kSerialMetaKey, sharded);
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

void ShardingApi::runUpdateItemsTest(std::string_view nsName) {
	TestCout() << "Running UpdateItemsTest" << std::endl;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string updated = "updated_" + RandString();
			const std::string key = "key" + std::to_string(shard + 1);

			reindexer::client::Item item = rx->NewItem(nsName);
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
				err = rx->Update(nsName, item);
			} else {
				err = rx->Upsert(nsName, item);
			}
			ASSERT_TRUE(err.ok()) << err.what();

			Query q = Query(std::string(nsName)).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
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

void ShardingApi::runDeleteItemsTest(std::string_view nsName) {
	TestCout() << "Running DeleteItemsTest" << std::endl;
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		std::string itemJson;
		{
			Query q = Query(std::string(nsName)).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::SyncCoroQueryResults qr;
			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 1);
			client::Item it = qr.begin().GetItem();
			itemJson = std::string(it.GetJSON());
		}

		reindexer::client::Item item = rx->NewItem(nsName);
		ASSERT_TRUE(item.Status().ok());

		Error err = item.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx->Delete(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();

		Query q = Query(std::string(nsName)).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
		client::SyncCoroQueryResults qr;
		err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 0) << qr.Count() << "; from proxy; shard = " << shard << "; location = " << key << std::endl;
	}
}

void ShardingApi::runUpdateIndexTest(std::string_view nsName) {
	TestCout() << "Running UpdateIndexTest" << std::endl;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			IndexDef indexDef{"new", {"new"}, "hash", "int", IndexOpts()};
			Error err = rx->AddIndex(nsName, indexDef);
			ASSERT_TRUE(err.ok()) << err.what();
			std::vector<NamespaceDef> nsdefs;
			err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(nsName));
			ASSERT_TRUE(err.ok()) << err.what() << std::endl;
			ASSERT_TRUE((nsdefs.size() == 1) && (nsdefs.front().name == nsName));
			ASSERT_TRUE(std::find_if(nsdefs.front().indexes.begin(), nsdefs.front().indexes.end(),
									 [](const IndexDef &index) { return index.name_ == "new"; }) != nsdefs.front().indexes.end());
			err = rx->DropIndex(nsName, indexDef);
			ASSERT_TRUE(err.ok()) << err.what();
			nsdefs.clear();
			err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(nsName));
			ASSERT_TRUE(err.ok()) << err.what() << std::endl;
			ASSERT_TRUE((nsdefs.size() == 1) && (nsdefs.front().name == nsName));
			ASSERT_TRUE(std::find_if(nsdefs.front().indexes.begin(), nsdefs.front().indexes.end(),
									 [](const IndexDef &index) { return index.name_ == "new"; }) == nsdefs.front().indexes.end());
		}
	}
}

void ShardingApi::runDropNamespaceTest(std::string_view nsName) {
	TestCout() << "Running DropNamespaceTest" << std::endl;
	std::shared_ptr<client::SyncCoroReindexer> rx = getNode(0)->api.reindexer;
	Error err = rx->TruncateNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		client::SyncCoroQueryResults qr;
		err = rx->Select(Query(std::string(nsName)).Where(kFieldLocation, CondEq, key), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 0) << qr.Count();
	}

	client::SyncCoroQueryResults qr;
	err = rx->Select(Query(std::string(nsName)), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 0) << qr.Count();

	err = rx->DropNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<NamespaceDef> nsdefs;
	err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(nsName));
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

void ShardingApi::runTransactionsTest(std::string_view nsName) {
	TestCout() << "Running TransactionsTest" << std::endl;
	const int rowsInTr = 10;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = getNode(i)->api.reindexer;
		Error err = rx->TruncateNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = std::string("key" + std::to_string(shard + 1));
			const int modes[] = {ModeUpsert, ModeDelete};
			for (int mode : modes) {
				reindexer::client::SyncCoroTransaction tr = rx->NewTransaction(nsName);
				ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
				for (int id = 0; id < rowsInTr; ++id) {
					if ((mode == ModeDelete) && (shard % 2 == 0)) {
						Query q = Query(std::string(nsName)).Where(kFieldLocation, CondEq, key);
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

				err = rx->Select(Query(std::string(nsName)).Where(kFieldLocation, CondEq, key), qr);
				ASSERT_TRUE(err.ok()) << err.what();
				if (mode == ModeUpsert) {
					ASSERT_EQ(qr.Count(), rowsInTr) << "; connection = " << i << "; shard = " << shard << "; location = " << key;
				} else if (mode == ModeDelete) {
					ASSERT_EQ(qr.Count(), 0) << "; connection = " << i << "; shard = " << shard << "; location = " << key;
				}

				if (mode == ModeUpsert) {
					const string updated = "updated_" + RandString();
					tr = rx->NewTransaction(nsName);
					ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
					Query q = Query(std::string(nsName)).Set(kFieldData, updated).Where(kFieldLocation, CondEq, key);
					q.type_ = QueryUpdate;
					err = tr.Modify(std::move(q));
					ASSERT_TRUE(err.ok()) << err.what();
					client::SyncCoroQueryResults qrTx;
					err = rx->CommitTransaction(tr, qrTx);
					ASSERT_TRUE(err.ok()) << err.what();

					qr = client::SyncCoroQueryResults();
					err = rx->Select(Query(std::string(nsName)).Where(kFieldLocation, CondEq, key), qr);
					ASSERT_TRUE(err.ok()) << err.what();
					std::string toFind = "\"location\":\"" + key + "\"";
					for (auto it : qr) {
						auto item = it.GetItem();
						std::string_view json = item.GetJSON();
						ASSERT_TRUE(json.find(toFind) != std::string_view::npos) << json;
					}
				}
			}

			CheckTransactionErrors(*rx, nsName);
		}
	}
}

TEST_F(ShardingApi, BaseApiTestset) {
	InitShardingConfig cfg;
	const std::vector<InitShardingConfig::Namespace> nss = {
		{"select_test", true},		   {"update_test", true},		{"delete_test", true},		 {"meta_test", true},
		{"serial_test", true},		   {"update_items_test", true}, {"delete_items_test", true}, {"update_index_test", true},
		{"drop_namespace_test", true}, {"transactions_test", true}};
	cfg.additionalNss = nss;
	Init(std::move(cfg));
	CheckServerIDs(svc_);

	auto nsIt = nss.begin();
	runSelectTest(nsIt++->name);
	runUpdateTest(nsIt++->name);
	runDeleteTest(nsIt++->name);
	runMetaTest(nsIt++->name);
	runSerialTest(nsIt++->name);
	runUpdateItemsTest(nsIt++->name);
	runDeleteItemsTest(nsIt++->name);
	runUpdateIndexTest(nsIt++->name);
	runDropNamespaceTest(nsIt++->name);
	runTransactionsTest(nsIt++->name);
}

TEST_F(ShardingApi, DISABLED_SelectProxyBench) {
	const size_t kShardDataCount = 10000;
	constexpr unsigned kTotalRequestsCount = 500000;

	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	cfg.rowsInTableOnShard = kShardDataCount;
	cfg.nodeIdInThread = 0;
	cfg.strlen = 7;
	Init(std::move(cfg));
	std::cout << "Init done" << std::endl;

	std::vector<unsigned> threadsCountVec = {24};
	const Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key2").Limit(20);
	for (auto thCnt : threadsCountVec) {
		std::atomic<unsigned> requests = {0};
		std::vector<std::thread> threads;
		std::condition_variable cv;
		std::mutex mtx;
		bool ready = false;
		for (unsigned i = 0; i < thCnt; ++i) {
			std::shared_ptr<client::SyncCoroReindexer> rx = std::make_shared<client::SyncCoroReindexer>();
			Error err = rx->Connect(getNode(0)->kRPCDsn);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx->Status(true);
			ASSERT_TRUE(err.ok()) << err.what();

			threads.emplace_back([&q, rx, &requests, &cv, &mtx, &ready] {
				std::unique_lock lck(mtx);
				cv.wait(lck, [&ready] { return ready; });
				lck.unlock();
				while (++requests < kTotalRequestsCount) {
					client::SyncCoroQueryResults qr;
					Error err = rx->Select(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				}
			});
		}

		const string filePath = fs::JoinPath(fs::GetTempDir(), "profile.cpu_" + std::to_string(thCnt));

#if REINDEX_WITH_GPERFTOOLS
		assert(alloc_ext::TCMallocIsAvailable());
		pprof::ProfilerStart(filePath.c_str());
#endif	// REINDEX_WITH_GPERFTOOLS
		std::unique_lock lck(mtx);
		ready = true;
		const auto beg = std::chrono::high_resolution_clock::now();
		cv.notify_all();
		lck.unlock();
		std::cout << fmt::sprintf("Start with %d threads", thCnt) << std::endl;
		for (auto &th : threads) {
			th.join();
		}
#if REINDEX_WITH_GPERFTOOLS
		pprof::ProfilerStop();
#endif	// REINDEX_WITH_GPERFTOOLS
		const auto diff = std::chrono::high_resolution_clock::now() - beg;
		std::cout << fmt::sprintf("Done with %d threads in %d usec", thCnt, diff.count() / 1000) << std::endl;
	}
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
		 Error{errParams, "Default shard id is not specified for namespace 'best_namespace'"}},
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
		 Error{errParams, "Dsns for shard id 1 are specified twice"}},
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
		 Error{errParams, "Scheme of sharding dsn must be cproto: 127.0.0.1:19001/shard1"}},
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
		 Error{errParams,
			   "Default shard id should be defined in shards list. Undefined default shard id: 0, for namespace: best_namespace"}},
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
		 Error{errParams, "Shard id 3 is not specified in the config but it is used in namespace keys"}},
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
		 Error{errParams, "Shard id should not be less than zero"}},
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
proxy_conn_threads: 5
)"s,
		 Cfg{{{"best_namespace", "location", {Cfg::Key{1, ByValue, {"south", "west"}}, {2, ByValue, {"north"}}}, 0}},
			 {{0, {"cproto://127.0.0.1:19000/shard0"}}, {1, {"cproto://127.0.0.1:19001/shard1"}}, {2, {"cproto://127.0.0.2:19002/shard2"}}},
			 0,
			 std::chrono::milliseconds(5000),
			 std::chrono::seconds(25),
			 15,
			 10,
			 5}},
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
proxy_conn_count: 8
proxy_conn_concurrency: 8
proxy_conn_threads: 4
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
		{sample.str(),
		 Cfg{{{"namespace1", "count", {Cfg::Key{1, ByValue, {0, 10, 20}}, {2, ByValue, {1, 2, 3, 4}}, {3, ByValue, {11}}}, 0},
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
		{R"({"version":1,"namespaces":[{"namespace":"best_namespace","default_shard":0,"index":"location","keys":[{"shard_id":1,"values":["south","west"]},{"shard_id":2,"values":["north"]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19001/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.2:19002/shard2"]}],"this_shard_id":0,"reconnect_timeout_msec":5000,"shards_awaiting_timeout_sec":20,"proxy_conn_count":4,"proxy_conn_concurrency":8,"proxy_conn_threads":4})"s,
		 Cfg{{{"best_namespace", "location", {Cfg::Key{1, ByValue, {"south", "west"}}, {2, ByValue, {"north"}}}, 0}},
			 {{0, {"cproto://127.0.0.1:19000/shard0"}}, {1, {"cproto://127.0.0.1:19001/shard1"}}, {2, {"cproto://127.0.0.2:19002/shard2"}}},
			 0,
			 std::chrono::milliseconds(5000),
			 std::chrono::seconds(20),
			 4}},
		{R"({"version":1,"namespaces":[{"namespace":"namespace1","default_shard":0,"index":"count","keys":[{"shard_id":1,"values":[0,10,20]},{"shard_id":2,"values":[1,5,7,9]},{"shard_id":3,"values":[100]}]},{"namespace":"namespace2","default_shard":3,"index":"city","keys":[{"shard_id":1,"values":["Moscow"]},{"shard_id":2,"values":["London"]},{"shard_id":3,"values":["Paris"]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0","cproto://127.0.0.1:19001/shard0","cproto://127.0.0.1:19002/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19010/shard1","cproto://127.0.0.1:19011/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.2:19020/shard2"]},{"shard_id":3,"dsns":["cproto://127.0.0.2:19030/shard3","cproto://127.0.0.2:19031/shard3","cproto://127.0.0.2:19032/shard3","cproto://127.0.0.2:19033/shard3"]}],"this_shard_id":0,"reconnect_timeout_msec":4000,"shards_awaiting_timeout_sec":30,"proxy_conn_count":3,"proxy_conn_concurrency":5,"proxy_conn_threads":2})"s,
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
			 5,
			 2}}};

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

static void CheckTotalCount(bool cached, std::shared_ptr<client::SyncCoroReindexer> &rx, const std::string &ns, size_t expected) {
	client::SyncCoroQueryResults qr;
	Error err;
	if (cached) {
		err = rx->Select(Query(ns).CachedTotal().Limit(0), qr);
	} else {
		err = rx->Select(Query(ns).ReqTotal().Limit(0), qr);
	}
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.TotalCount(), expected);
}

static void CheckCachedCountAggregations(std::shared_ptr<client::SyncCoroReindexer> &rx, const std::string &ns, size_t dataPerShard,
										 size_t shardsCount, const std::string fieldLocation) {
	{
		// Check distributed query without offset
		client::SyncCoroQueryResults qr;
		Error err = rx->Select(Query(ns).CachedTotal().Limit(0), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), shardsCount * dataPerShard);
		auto &agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].type, AggCountCached);
		ASSERT_EQ(agg[0].value, qr.TotalCount());
	}
	{
		// Check distributed query with offset
		client::SyncCoroQueryResults qr;
		Error err = rx->Select(Query(ns).CachedTotal().Limit(0).Offset(1), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), shardsCount * dataPerShard);
		auto &agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].type, AggCount);  // Here agg type was change to 'AggCount' by internal proxy
		ASSERT_EQ(agg[0].value, qr.TotalCount());
	}
	{
		// Check single shard query without offset
		client::SyncCoroQueryResults qr;
		Error err = rx->Select(Query(ns).Where(fieldLocation, CondEq, "key2").CachedTotal().Limit(0), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), dataPerShard);
		auto &agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].type, AggCountCached);
		ASSERT_EQ(agg[0].value, qr.TotalCount());
	}
	{
		// Check single shard query with offset
		client::SyncCoroQueryResults qr;
		Error err = rx->Select(Query(ns).Where(fieldLocation, CondEq, "key2").CachedTotal().Limit(0).Offset(1), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), dataPerShard);
		auto &agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].type, AggCountCached);
		ASSERT_EQ(agg[0].value, qr.TotalCount());
	}
}

TEST_F(ShardingApi, SelectOffsetLimit) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	const std::string_view kNsName = default_namespace;

	std::shared_ptr<client::SyncCoroReindexer> rx = svc_[0][0].Get()->api.reindexer;

	const unsigned long kMaxCountOnShard = 40;
	Fill(kNsName, rx, "key3", 0, kMaxCountOnShard);
	Fill(kNsName, rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
	Fill(kNsName, rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);

	struct LimitOffsetCase {
		unsigned offset;
		unsigned limit;
		unsigned count;
	};

	CheckCachedCountAggregations(rx, default_namespace, kMaxCountOnShard, kShards, kFieldLocation);
	CheckTotalCount(true, rx, default_namespace, kMaxCountOnShard * kShards);

	// Check offsets and limits
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

	// Check count cached with actual caching
	// 1. Make sure, the query gets cached
	for (unsigned i = 0; i < 5; ++i) {
		CheckTotalCount(true, rx, default_namespace, kMaxCountOnShard * kShards);
	}
	// 2. Change data
	Fill(kNsName, rx, "key2", kMaxCountOnShard * 3, 1);

	// 3. Check total count and count cached
	CheckTotalCount(true, rx, default_namespace, kMaxCountOnShard * kShards + 1);
	CheckTotalCount(false, rx, default_namespace, kMaxCountOnShard * kShards + 1);
}
