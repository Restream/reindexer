#include <future>
#include <sstream>
#include "cluster/stats/replicationstats.h"
#include "core/itemimpl.h"
#include "estl/tuple_utils.h"
#include "server/pprof/gperf_profiler.h"
#include "sharding_api.h"
#include "tools/alloc_ext/tc_malloc_extension.h"

static void CheckServerIDs(std::vector<std::vector<ServerControl>> &svc) {
	WrSerializer ser;
	size_t idx = 0;
	for (auto &cluster : svc) {
		for (auto &sc : cluster) {
			auto rx = sc.Get()->api.reindexer;
			client::QueryResults qr;
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::QueryResults qr;
			Query q = Query(std::string(nsName))
						  .Where(kFieldLocation, CondEq, key)
						  .InnerJoin(kFieldId, kFieldId, CondEq, Query(std::string(nsName)).Where(kFieldLocation, CondEq, key));

			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key << std::endl;
			ASSERT_EQ(qr.Count(), 40);
			for (auto it : qr) {
				auto item = it.GetItem();
				ASSERT_TRUE(item.Status().ok())
					<< "; i = " << i << "; location = " << key << "; item status: " << item.Status().what() << std::endl;
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
	using namespace std::string_literals;
	TestCout() << "Running UpdateTest" << std::endl;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			// key1, key2, key3(proxy shardId=0)
			{
				const std::string key = "key" + std::to_string(shard + 1);
				const std::string updated = "updated_" + RandString();
				client::QueryResults qr;
				Query q = Query(std::string(nsName)).Set(kFieldData, updated);
				q.Where(kFieldLocation, CondEq, key);
				q.type_ = QueryUpdate;
				Error err = rx->Update(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; location = " << key << std::endl;
				const std::string expectedLocation = "\"location\":\""s + key + '"';
				const std::string expectedData = "\"data\":\""s + updated + '"';
				EXPECT_EQ(qr.Count(), 40);
				for (auto it : qr) {
					auto item = it.GetItem();
					std::string_view json = item.GetJSON();
					EXPECT_NE(json.find(expectedLocation), std::string_view::npos) << json << "; expected: {" << expectedLocation << "}";
					EXPECT_NE(json.find(expectedData), std::string_view::npos) << json << "; expected: {" << expectedData << "}";
				}
			}

			{
				const std::string updatedErr = "updated_" + RandString();
				Query qNoShardKey = Query(std::string(nsName)).Set(kFieldData, updatedErr);
				client::QueryResults qrErr;
				Error err = rx->Update(qNoShardKey, qrErr);
				ASSERT_FALSE(err.ok()) << err.what();
				Query qNoShardKeySelect = Query(std::string(nsName));
				client::QueryResults qrSelect;
				err = rx->Select(qNoShardKeySelect, qrSelect);
				ASSERT_TRUE(err.ok()) << err.what();
				for (auto it : qrSelect) {
					auto item = it.GetItem();
					std::string_view json = item.GetJSON();
					EXPECT_EQ(json.find(updatedErr), std::string_view::npos);
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::QueryResults qr;
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
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
				client::QueryResults qr;
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
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
			client::QueryResults qr;
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
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		std::string itemJson;
		{
			Query q = Query(std::string(nsName)).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::QueryResults qr;
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
		client::QueryResults qr;
		err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 0) << qr.Count() << "; from proxy; shard = " << shard << "; location = " << key << std::endl;
	}
}

void ShardingApi::runUpdateIndexTest(std::string_view nsName) {
	TestCout() << "Running UpdateIndexTest" << std::endl;
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
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
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	Error err = rx->TruncateNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		client::QueryResults qr;
		err = rx->Select(Query(std::string(nsName)).Where(kFieldLocation, CondEq, key), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 0) << qr.Count();
	}

	client::QueryResults qr;
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

void ShardingApi::CheckTransactionErrors(client::Reindexer &rx, std::string_view nsName) {
	// Check errros handling in transactions
	for (unsigned i = 0; i < 2; ++i) {
		auto tr = rx.NewTransaction(nsName);
		ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();

		auto item = tr.NewItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON("{\"" + kFieldId + "\":0,\"" + kFieldLocation + "\":\"key0\"}");
		ASSERT_TRUE(err.ok()) << err.what();
		err = tr.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();

		item = tr.NewItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		err = item.FromJSON("{\"" + kFieldId + "\":0,\"" + kFieldLocation + "\":\"key1\"}");
		ASSERT_TRUE(err.ok()) << err.what();
		err = tr.Upsert(std::move(item));
		ASSERT_FALSE(err.ok());
		ASSERT_FALSE(tr.Status().ok());
		item = tr.NewItem();
		ASSERT_FALSE(item.Status().ok());

		if (i == 0) {
			// Check if we'll get an error on commit, if there were an errors before
			client::QueryResults qr;
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
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
		Error err = rx->TruncateNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = std::string("key" + std::to_string(shard + 1));
			const int modes[] = {ModeUpsert, ModeDelete};
			for (int mode : modes) {
				reindexer::client::Transaction tr = rx->NewTransaction(nsName);
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
				client::QueryResults qrTx;
				err = rx->CommitTransaction(tr, qrTx);
				ASSERT_TRUE(err.ok()) << err.what() << "; connection = " << i << "; shard = " << shard << "; mode = " << mode << std::endl;

				client::QueryResults qr;

				err = rx->Select(Query(std::string(nsName)).Where(kFieldLocation, CondEq, key), qr);
				ASSERT_TRUE(err.ok()) << err.what();
				if (mode == ModeUpsert) {
					ASSERT_EQ(qr.Count(), rowsInTr) << "; connection = " << i << "; shard = " << shard << "; location = " << key;
				} else if (mode == ModeDelete) {
					ASSERT_EQ(qr.Count(), 0) << "; connection = " << i << "; shard = " << shard << "; location = " << key;
				}

				if (mode == ModeUpsert) {
					const std::string updated = "updated_" + RandString();
					tr = rx->NewTransaction(nsName);
					ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
					Query q = Query(std::string(nsName)).Set(kFieldData, updated).Where(kFieldLocation, CondEq, key);
					q.type_ = QueryUpdate;
					err = tr.Modify(std::move(q));
					ASSERT_TRUE(err.ok()) << err.what();
					client::QueryResults qrTx;
					err = rx->CommitTransaction(tr, qrTx);
					ASSERT_TRUE(err.ok()) << err.what();

					qr = client::QueryResults();
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
			std::shared_ptr<client::Reindexer> rx = std::make_shared<client::Reindexer>();
			Error err = rx->Connect(getNode(0)->kRPCDsn);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx->Status(true);
			ASSERT_TRUE(err.ok()) << err.what();

			threads.emplace_back([&q, rx, &requests, &cv, &mtx, &ready] {
				std::unique_lock lck(mtx);
				cv.wait(lck, [&ready] { return ready; });
				lck.unlock();
				while (++requests < kTotalRequestsCount) {
					client::QueryResults qr;
					Error err = rx->Select(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				}
			});
		}

		const std::string filePath = fs::JoinPath(fs::GetTempDir(), "profile.cpu_" + std::to_string(thCnt));

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

TEST_F(ShardingApi, Aggregations) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	client::QueryResults qr;
	Query q = Query(default_namespace).Aggregate(AggSum, {kFieldId}).Aggregate(AggMin, {kFieldId}).Aggregate(AggMax, {kFieldId});
	Error err = rx->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.GetAggregationResults().size(), 3);
	const auto itemsCount = cfg.rowsInTableOnShard * kShards;
	EXPECT_EQ(qr.GetAggregationResults()[0].type, AggSum);
	EXPECT_EQ(qr.GetAggregationResults()[0].value, (itemsCount - 1) * itemsCount / 2);
	EXPECT_EQ(qr.GetAggregationResults()[1].type, AggMin);
	EXPECT_EQ(qr.GetAggregationResults()[1].value, 0);
	EXPECT_EQ(qr.GetAggregationResults()[2].type, AggMax);
	EXPECT_EQ(qr.GetAggregationResults()[2].value, itemsCount - 1);
}

TEST_F(ShardingApi, CheckQueryWithSharding) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(kFieldId, CondEq, 0);
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(kFieldLocation, CondEq, "key2");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Duplication of shard key condition in the query");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Or().Where(kFieldId, CondEq, 0);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be connected with other conditions by operator OR");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldId, CondEq, 0).Or().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be connected with other conditions by operator OR");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Not().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be negative");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).WhereBetweenFields(kFieldLocation, CondEq, kFieldData);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key cannot be compared with another field");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket();
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).OpenBracket().OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket().CloseBracket();
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	// JOIN
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q =
			Query(default_namespace).Where(kFieldLocation, CondEq, "key1").InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Join query must contain shard key.");
	}
	{
		client::QueryResults qr;
		Query q =
			Query(default_namespace).InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key from other node");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Not().Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be negative");
	}
	// MERGE
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Merge(Query{default_namespace});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Merge query must contain shard key.");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Query to all shard can't contain JOIN or MERGE");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key from other node");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket());
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Distinct(kFieldId);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Query to all shard can't contain aggregations AVG, Facet or Distinct");
	}
	for (const auto agg : {AggAvg, AggFacet, AggDistinct}) {
		client::QueryResults qr;
		Query q = Query(default_namespace).Aggregate(agg, {kFieldId});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Query to all shard can't contain aggregations AVG, Facet or Distinct");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).ReqTotal();
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).CachedTotal();
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	for (const auto agg : {AggSum, AggAvg, AggMin, AggMax, AggFacet, AggDistinct}) {
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Aggregate(agg, {kFieldId});
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Distinct(kFieldId).Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).ReqTotal().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).CachedTotal().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Sort(kFieldId + " * 10", false);
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
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
  - namespace: best_namespace
    index: location
    keys:
      - shard_id: 1
        values:
          - south
          - west
shards:
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19002/shard2
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Default shard id is not specified for namespace 'best_namespace'"}},
		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: 1
    index: location
    keys:
      - shard_id: 1
        values:
          - south
          - west
shards:
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 1
    dsns:
      - cproto://127.0.0.2:19002/shard2
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Dsns for shard id 1 are specified twice"}},
		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: 1
    index: location
    keys:
      - shard_id: 1
        values:
          - south
          - west
shards:
  - shard_id: 1
    dsns:
      - 127.0.0.1:19001/shard1
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Scheme of sharding dsn must be cproto: 127.0.0.1:19001/shard1"}},
		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: 0
    index: location
    keys:
      - shard_id: 1
        values:
          - south
          - west
shards:
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19002/shard2
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams,
			   "Default shard id should be defined in shards list. Undefined default shard id: 0, for namespace: best_namespace"}},
		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: 1
    index: location
    keys:
      - shard_id: 3
        values:
          - south
          - west
shards:
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19002/shard2
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Shard id 3 is not specified in the config but it is used in namespace keys"}},
		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: -1
    index: location
    keys:
      - shard_id: 2
        values:
          - south
          - west
shards:
  - shard_id: -1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19002/shard2
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Shard id should not be less than zero"}},
		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: 0
    index: location
    keys:
      - shard_id: 1
        values:
          - south
          - west
  - namespace: best_namespacE
    default_shard: 2
    index: location2
    keys:
      - shard_id: 1
        values:
          - south2
shards:
  - shard_id: 0
    dsns:
      - cproto://127.0.0.1:19000/shard0
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19002/shard2
this_shard_id: 0
reconnect_timeout_msec: 5000
shards_awaiting_timeout_sec: 25
proxy_conn_count: 15
)"s,
		 Error{errParams, "Namespace 'best_namespacE' already specified in the config."}

		},
		{R"(version: 1
namespaces:
  - namespace:
    - a
)"s,
		 Error{errParams, "'namespace' node must be scalar."}},
		{R"(version: 1
namespaces:
  - namespace: ""
)"s,
		 Error{errParams, "Namespace name incorrect ''."}},

		{R"(version: 1
namespaces:
  - namespace: best_namespace
    index:
      - location
)"s,
		 Error{errParams, "'index' node must be scalar."}},

		{R"(version: 1
namespaces:
  - namespace: best_namespace
    default_shard: 0
    index: location
    keys:
      - shard_id: 1
        values:
          - south
          - west
      - shard_id: 2
        values:
          - north
shards:
  - shard_id: 0
    dsns:
      - cproto://127.0.0.1:19000/shard0
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19001/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19002/shard2
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
  - namespace: namespace1
    default_shard: 0
    index: count
    keys:
      - shard_id: 1
        values:
          - 0
          - 10
          - 20
  - namespace: namespace2
    default_shard: 1
    index: city
    keys:
      - shard_id: 1
        values:
          - Moscow
      - shard_id: 2
        values:
          - London
      - shard_id: 3
        values:
          - Paris
shards:
  - shard_id: 0
    dsns:
      - cproto://127.0.0.1:19000/shard0
      - cproto://127.0.0.1:19001/shard0
      - cproto://127.0.0.1:19002/shard0
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19010/shard1
      - cproto://127.0.0.1:19011/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.2:19020/shard2
  - shard_id: 3
    dsns:
      - cproto://127.0.0.2:19030/shard3
      - cproto://127.0.0.2:19031/shard3
      - cproto://127.0.0.2:19032/shard3
      - cproto://127.0.0.2:19033/shard3
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
		const Error err = config.FromYAML(yaml);
		if (std::holds_alternative<Cfg>(expected)) {
			const auto &cfg{std::get<Cfg>(expected)};
			EXPECT_TRUE(err.ok()) << err.what() << "\nYAML:\n" << yaml;
			if (err.ok()) {
				EXPECT_EQ(config, cfg) << yaml << "\nexpected:\n" << cfg.GetYAML();
			}
			const auto generatedYml = cfg.GetYAML();
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
		const auto generatedJson = cfg.GetJSON();
		EXPECT_EQ(generatedJson, json);

		const Error err = config.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();
		if (err.ok()) {
			EXPECT_EQ(config, cfg) << json << "\nexpected:\n" << cfg.GetJSON();
		}
	}
}

TEST_F(ShardingApi, ConfigKeyValues) {
	struct shardInfo {
		bool result;
		using ShardKeys = std::vector<std::string>;
		std::vector<ShardKeys> shards;
	};

	// NOLINTBEGIN (performance-inefficient-string-concatenation)
	auto generateConfigYaml = [](const shardInfo &info) -> std::string {
		YAML::Node y;
		y["version"] = 1;
		y["namespaces"] = YAML::Node(YAML::NodeType::Sequence);
		y["shards"] = YAML::Node(YAML::NodeType::Sequence);
		y["this_shard_id"] = 0;
		{
			YAML::Node nsY;
			nsY["namespace"] = "ns";
			nsY["default_shard"] = 0;
			nsY["index"] = "location";
			nsY["keys"] = YAML::Node(YAML::NodeType::Sequence);
			for (size_t i = 0; i < info.shards.size(); i++) {
				YAML::Node kY;
				kY["shard_id"] = i + 1;
				kY["values"] = info.shards[i];
				nsY["keys"].push_back(std::move(kY));
			}
			y["namespaces"].push_back(std::move(nsY));
		}

		for (size_t i = 0; i <= info.shards.size(); i++) {
			YAML::Node sY;
			sY["shard_id"] = i;
			sY["dsns"].push_back(fmt::sprintf("cproto://127.0.0.1:1900%d/shard%d", i, i));
			y["shards"].push_back(std::move(sY));
		}
		return YAML::Dump(y);
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
			conf += "        \"shard_id\": " + indxStr + ",\n";
			conf += "        \"dsns\":[\n";
			conf += "            \"cproto://127.0.0.1:1900" + indxStr + "/shard" + indxStr + "\"\n";
			conf += "        ]\n";
			conf += (i == info.shards.size()) ? "    }\n" : "    },\n";
		}
		conf += "],\n";
		conf += "\"this_shard_id\": 0\n}";
		return conf;
	};
	// NOLINTEND (performance-inefficient-string-concatenation)

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
		Error err = config.FromJSON(conf);
		EXPECT_TRUE(err.ok() == test.result) << err.what() << "\nconfig:\n" << conf;
	}

	tests.push_back({true, {{"key1", "key2"}, {"key3"}}});
	tests.push_back({true, {{"key1", "\"key2\""}, {"\"key3\""}}});
	tests.push_back({false, {{"1", "2"}, {"3", "string"}}});
	tests.push_back({false, {{"key1", "key2"}, {"key1", "key3"}}});

	for (const auto &test : tests) {
		std::string conf = generateConfigYaml(test);
		reindexer::cluster::ShardingConfig config;
		Error err = config.FromYAML(conf);
		EXPECT_TRUE(err.ok() == test.result) << err.what() << "\nconfig:\n" << conf;
	}
}

TEST_F(ShardingApi, RestrictionOnRequest) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));

	std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;
	{
		client::QueryResults qr;
		Query q;
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "<'key3'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::QueryResults qr;
		Query q;
		// key3 - proxy node
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "='key3'" + " and " + kFieldLocation + "='key2'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::QueryResults qr;
		Query q;
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "='key1'" + " and " + kFieldLocation + "='key2'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}

	{
		client::QueryResults qr;
		Query q;
		q.FromSQL("select * from " + default_namespace + " where " + kFieldLocation + "='key1'" + " or " + kFieldLocation + "='key2'");
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::QueryResults qr;
		Query q(default_namespace);
		q.Where(kFieldLocation, CondEq, {"key1", "key2"});
		auto err = rx->Select(q, qr);
		ASSERT_EQ(err.code(), errLogic);
	}
}
static void CheckTotalCount(bool cached, std::shared_ptr<client::Reindexer> &rx, const std::string &ns, size_t expected) {
	client::QueryResults qr;
	Error err;
	if (cached) {
		err = rx->Select(Query(ns).CachedTotal().Limit(0), qr);
	} else {
		err = rx->Select(Query(ns).ReqTotal().Limit(0), qr);
	}
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.TotalCount(), expected);
}

static void CheckCachedCountAggregations(std::shared_ptr<client::Reindexer> &rx, const std::string &ns, size_t dataPerShard,
										 size_t shardsCount, const std::string &fieldLocation) {
	{
		// Check distributed query without offset
		client::QueryResults qr;
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
		client::QueryResults qr;
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
		client::QueryResults qr;
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
		client::QueryResults qr;
		Error err = rx->Select(Query(ns).Where(fieldLocation, CondEq, "key2").CachedTotal().Limit(0).Offset(1), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), dataPerShard);
		auto &agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].type, AggCountCached);
		ASSERT_EQ(agg[0].value, qr.TotalCount());
	}
}

template <typename T>
T getField(std::string_view field, reindexer::client::QueryResults::Iterator &it) {
	auto item = it.GetItem();
	std::string_view json = item.GetJSON();
	gason::JsonParser parser;
	auto root = parser.Parse(json);
	auto node = root;
	auto pos = field.find('.');
	while (pos != std::string_view::npos) {
		node = node[field.substr(0, pos)];
		field = field.substr(pos + 1);
		pos = field.find('.');
	}
	return node[field].As<T>();
}

TEST_F(ShardingApi, SelectOffsetLimit) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	const std::string_view kNsName = default_namespace;

	std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;

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
		client::QueryResults qr;
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), v.count) << q.GetSQL();
		if (checkCount) {
			ASSERT_EQ(qr.TotalCount(), kMaxCountOnShard * kShards) << q.GetSQL();
		}
		int n = 0;
		for (auto i = qr.begin(); i != qr.end(); ++i, ++n) {
			ASSERT_EQ(getField<int>(kFieldId, i), v.offset + n) << q.GetSQL();
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

template <typename...>
class CompareFields;

template <>
class CompareFields<>;

template <typename T>
class CompareFields<T> {
public:
	CompareFields(std::string_view f, std::vector<T> v = {}) : field_{f}, forcedValues_{std::move(v)} {}
	int operator()(reindexer::client::QueryResults::Iterator &it) {
		T v = getField<T>(field_, it);
		int result = 0;
		if (prevValue_) {
			const auto end = forcedValues_.cend();
			const auto prevIt = std::find(forcedValues_.cbegin(), end, *prevValue_);
			const auto currIt = std::find(forcedValues_.cbegin(), end, v);
			if (prevIt != end || currIt != end) {
				if (currIt != prevIt) {
					result = prevIt < currIt ? 1 : -1;
				} else {
					result = 0;
				}
			} else if (prevValue_ != v) {
				result = prevValue_ < v ? 1 : -1;
			}
		}
		prevValue_ = std::move(v);
		return result;
	}
	std::tuple<T> GetValues(reindexer::client::QueryResults::Iterator &it) const { return std::make_tuple(getField<T>(field_, it)); }
	std::tuple<T> GetPrevValues() const {
		assert(prevValue_.has_value());
		return std::make_tuple(*prevValue_);
	}
	bool HavePrevValue() const noexcept { return prevValue_.has_value(); }
	void SetPrevValues(std::tuple<T> pv) { prevValue_ = std::move(std::get<0>(pv)); }

private:
	std::string_view field_;
	std::optional<T> prevValue_;
	std::vector<T> forcedValues_;
};

template <typename T, typename... Ts>
class CompareFields<T, Ts...> : private CompareFields<Ts...> {
	using Base = CompareFields<Ts...>;
	template <typename>
	using string_view = std::string_view;

public:
	CompareFields(std::string_view f, string_view<Ts>... args, std::vector<std::tuple<T, Ts...>> v = {})
		: Base{args...}, impl_{f}, forcedValues_{std::move(v)} {}
	int operator()(reindexer::client::QueryResults::Iterator &it) {
		if (!forcedValues_.empty() && impl_.HavePrevValue()) {
			const auto prevValues = GetPrevValues();
			const auto values = GetValues(it);
			const auto end = forcedValues_.cend();
			const auto prevIt = std::find(forcedValues_.cbegin(), end, prevValues);
			const auto currIt = std::find(forcedValues_.cbegin(), end, values);
			if (prevIt != end || currIt != end) {
				SetPrevValues(values);
				if (currIt == prevIt) {
					return 0;
				}
				return prevIt < currIt ? 1 : -1;
			}
		}
		auto res = impl_(it);
		if (res == 0) {
			res = Base::operator()(it);
		} else {
			Base::operator()(it);
		}
		return res;
	}
	std::tuple<T, Ts...> GetValues(reindexer::client::QueryResults::Iterator &it) const {
		return std::tuple_cat(impl_.GetValues(it), Base::GetValues(it));
	}
	std::tuple<T, Ts...> GetPrevValues() const { return std::tuple_cat(impl_.GetPrevValues(), Base::GetPrevValues()); }
	void SetPrevValues(std::tuple<T, Ts...> pv) {
		impl_.SetPrevValues(std::make_tuple(std::move(std::get<0>(pv))));
		Base::SetPrevValues(tail(std::move(pv)));
	}

private:
	CompareFields<T> impl_;
	std::vector<std::tuple<T, Ts...>> forcedValues_;
};

template <typename... Ts>
class CompareExpr {
public:
	CompareExpr(std::vector<std::string> &&fields, std::function<double(Ts...)> &&f) : fields_{std::move(fields)}, func_{std::move(f)} {}
	int operator()(reindexer::client::QueryResults::Iterator &it) { return impl(it, std::index_sequence_for<Ts...>{}); }

private:
	template <size_t... I>
	int impl(reindexer::client::QueryResults::Iterator &it, std::index_sequence<I...>) {
		const double r = func_(getField<Ts>(fields_[I], it)...);
		int result = 0;
		if (prevResult_ && *prevResult_ != r) {
			result = *prevResult_ < r ? 1 : -1;
		}
		prevResult_ = r;
		return result;
	}
	std::optional<double> prevResult_;
	std::vector<std::string> fields_;
	std::function<double(Ts...)> func_;
};

class ShardingApi::CompareShardId {
public:
	CompareShardId(const ShardingApi &api) noexcept : api_{api} {}
	int operator()(reindexer::client::QueryResults::Iterator &it) {
		std::string keyValue = getField<std::string>(api_.kFieldLocation, it);
		keyValue = keyValue.substr(3);
		size_t curShardId = std::stoi(keyValue);
		if (curShardId >= api_.kShards) {
			curShardId = 0;
		}
		int result = 0;
		if (prevShardId_ && *prevShardId_ != curShardId) {
			result = *prevShardId_ < curShardId ? 1 : -1;
		}
		prevShardId_ = curShardId;
		return result;
	}

private:
	std::optional<size_t> prevShardId_;
	const ShardingApi &api_;
};

TEST_F(ShardingApi, OrderBy) {
	std::random_device rd;
	std::mt19937 g(rd());
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 0;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	const size_t tableSize = rand() % 200 + 1;
	Fill(default_namespace, 0, 0, tableSize);
	struct SortCase {
		std::string expression;
		std::variant<std::vector<int>, std::vector<std::string>, std::vector<std::tuple<int, std::string>>,
					 std::vector<std::tuple<std::string, int>>>
			forcedValues;
		std::function<int(reindexer::client::QueryResults::Iterator &)> test;
		bool testId{false};
	};
	std::set<std::string> compositeIndexes{kFieldIdLocation, kFieldLocationId, kIndexDataIntLocation, kIndexLocationDataInt};
	std::map<std::string, std::string> synonymIndexes{{kFieldDataInt, kIndexDataInt},
													  {kIndexDataInt, kFieldDataInt},
													  {kFieldDataString, kIndexDataString},
													  {kIndexDataString, kFieldDataString},
													  {kSparseFieldDataInt, kSparseIndexDataInt},
													  {kSparseIndexDataInt, kSparseFieldDataInt},
													  {kSparseFieldDataString, kSparseIndexDataString},
													  {kSparseIndexDataString, kSparseFieldDataString}};

	std::vector<std::tuple<int, std::string>> compositeForceValues1;
	std::vector<std::tuple<std::string, int>> compositeForceValues2;
	std::vector<int> ids(static_cast<size_t>(tableSize * 1.1));
	std::iota(ids.begin(), ids.end(), 0);
	std::shuffle(ids.begin(), ids.end(), g);
	ids.resize(rand() % ids.size());
	for (int i : ids) {
		Query q{default_namespace};
		q.Where(kFieldId, CondEq, i);
		client::QueryResults qr;
		std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		if (qr.Count() == 1) {
			auto it = qr.begin();
			std::string loc = getField<std::string>(kFieldLocation, it);
			compositeForceValues1.emplace_back(i, loc);
			compositeForceValues2.emplace_back(loc, i);
		} else {
			compositeForceValues1.emplace_back(i, RandString());
			compositeForceValues2.emplace_back(RandString(), i);
		}
	}

	std::vector<SortCase> sortCases{
		{kFieldNestedRand, {}, CompareFields<int>{kFieldNestedRand}},
		{kFieldId, {}, CompareFields<int>{kFieldId}, true},
		{kFieldData, {}, CompareFields<std::string>{kFieldData}},
		{kFieldId + " + 4", {}, CompareFields<int>{kFieldId}, true},
		{kIndexDataInt + " + 4", {}, CompareFields<int>{kFieldDataInt}},
		{"3 * " + kFieldNestedRand, {}, CompareFields<int>{kFieldNestedRand}},
		{kFieldId + " * 11 + " + kFieldNestedRand,
		 {},
		 CompareExpr<int, int>{{kFieldId, kFieldNestedRand}, [](int id, int rand) { return id * 11 + rand; }},
		 true},
		{kIndexDataInt + " / 11 + " + kFieldNestedRand,
		 {},
		 CompareExpr<int, int>{{kFieldDataInt, kFieldNestedRand}, [](int data, int rand) { return data / 11.0 + rand; }}},
		{kFieldIdLocation, {}, CompareFields<int, std::string>{kFieldId, kFieldLocation}, true},
		{kIndexDataIntLocation, {}, CompareFields<int, std::string>{kFieldDataInt, kFieldLocation}},
		{kFieldLocationId, {}, CompareFields<std::string, int>{kFieldLocation, kFieldId}},
		{kIndexLocationDataInt, {}, CompareFields<std::string, int>{kFieldLocation, kFieldDataInt}},
		{kFieldId, std::vector{10, 20, 30, 40}, CompareFields<int>{kFieldId, {10, 20, 30, 40}}},
		{kIndexDataInt, std::vector{10, 20, 30, 40}, CompareFields<int>{kFieldDataInt, {10, 20, 30, 40}}},
		{kFieldIdLocation, compositeForceValues1, CompareFields<int, std::string>{kFieldId, kFieldLocation, compositeForceValues1}},
		{kFieldLocationId, compositeForceValues2, CompareFields<std::string, int>{kFieldLocation, kFieldId, compositeForceValues2}},
		{kFieldDataInt, {}, CompareFields<int>{kFieldDataInt}},
		{kIndexDataInt, {}, CompareFields<int>{kFieldDataInt}},
		{kSparseFieldDataInt, {}, CompareFields<int>{kSparseFieldDataInt}},
		{kSparseIndexDataInt, {}, CompareFields<int>{kSparseFieldDataInt}},
		{kIndexDataString, {}, CompareFields<std::string>{kFieldDataString}},
		{kFieldDataString, {}, CompareFields<std::string>{kFieldDataString}},
		{kSparseIndexDataString, {}, CompareFields<std::string>{kSparseFieldDataString}},
		{kSparseFieldDataString, {}, CompareFields<std::string>{kSparseFieldDataString}},
		{kIndexDataInt + " * 3 + " + kFieldId + " * (" + kFieldDataInt + " - " + kSparseFieldDataInt + ") + (" + kIndexDataInt + " + abs(" +
			 kSparseIndexDataInt + " - " + kFieldNestedRand + ") / 5)",
		 {},
		 CompareExpr<int, int, int, int>{{kFieldId, kFieldDataInt, kSparseFieldDataInt, kFieldNestedRand},
										 [](int id, int data, int sparseData, int nestedRand) {
											 return data * 3 + id * (data - sparseData) + (data + std::abs(sparseData - nestedRand) / 5.0);
										 }}},
		{"5 * (" + kSparseFieldDataInt + ") + (" + kIndexDataInt + " / 4)",
		 {},
		 CompareExpr<int, int>{{kFieldDataInt, kSparseFieldDataInt}, [](int data, int sparse) { return 5.0 * sparse + data / 4.0; }}},
	};
	struct TestCase {
		TestCase(const SortCase &sc, bool d = ((rand() % 2) == 0)) : sort{sc}, desc{d} {}
		SortCase sort;
		bool desc;
	};
	for (size_t j = 0; j < 100; ++j) {
		std::shuffle(sortCases.begin(), sortCases.end(), g);
		const size_t sortsCount = std::min<size_t>(rand() % 3 + 1, sortCases.size());
		std::vector<TestCase> testCases;
		testCases.reserve(sortsCount);
		std::set<std::string> usedFields;
		for (size_t i = 0; testCases.size() < sortsCount && i < sortCases.size(); ++i) {
			if (i != 0 && !std::visit([](const auto &c) { return c.empty(); }, sortCases[i].forcedValues)) continue;
			if (compositeIndexes.find(sortCases[i].expression) == compositeIndexes.end()) {
				if (usedFields.find(sortCases[i].expression) != usedFields.end()) continue;
				usedFields.insert(sortCases[i].expression);
				if (const auto it = synonymIndexes.find(sortCases[i].expression); it != synonymIndexes.end()) {
					if (usedFields.find(it->second) != usedFields.end()) continue;
					usedFields.insert(it->second);
				}
			} else {
				bool found = false;
				std::string::size_type end = -1;
				do {
					std::string::size_type start = end + 1;
					end = sortCases[i].expression.find('+', start);
					const auto field = sortCases[i].expression.substr(start, end == std::string::npos ? end : end - start);
					if (usedFields.find(field) != usedFields.end()) {
						found = true;
						break;
					}
					if (const auto it = synonymIndexes.find(field); it != synonymIndexes.end()) {
						if (usedFields.find(it->second) != usedFields.end()) {
							found = true;
							break;
						}
					}
				} while (end != std::string::npos);
				if (found) continue;
				end = -1;
				do {
					std::string::size_type start = end + 1;
					end = sortCases[i].expression.find('+', start);
					const auto field = sortCases[i].expression.substr(start, end == std::string::npos ? end : end - start);
					usedFields.insert(field);
					if (const auto it = synonymIndexes.find(field); it != synonymIndexes.end()) {
						usedFields.insert(it->second);
					}
				} while (end != std::string::npos);
			}
			testCases.emplace_back(sortCases[i]);
		}

		std::optional<size_t> offset;
		if (rand() % 3 > 0) {
			offset = rand() % (tableSize + 5);
		}
		std::optional<size_t> limit;
		if (rand() % 3 > 0) {
			if (offset) {
				if (*offset < tableSize) {
					limit = rand() % (tableSize - *offset + 5);
				} else {
					limit = rand() % 5;
				}
			} else {
				limit = rand() % (tableSize + 5);
			}
		}
		if (limit && limit == 0) {	// test this limit 0 not working. "isWithSharding" return false on condition (q.count == 0 &&
									// q.calcTotal == ModeNoTotal && !q.joinQueries_.size() && !q.mergeQueries_.size())
			limit = 1;
		}
		Query q = Query{default_namespace};
		for (const auto &tc : testCases) {
			std::visit(
				[&](const auto &c) {
					if (c.empty()) {
						q.Sort(tc.sort.expression, tc.desc);
					} else {
						q.Sort(tc.sort.expression, tc.desc, c);
					}
				},
				tc.sort.forcedValues);
		}
		testCases.emplace_back(SortCase{"ShardId", {}, CompareShardId{*this}}, false);
		size_t expectedResultSize = tableSize;
		if (offset) {
			q.Offset(*offset);
			expectedResultSize = (*offset < tableSize) ? tableSize - *offset : 0;
		}
		if (limit) {
			q.Limit(*limit);
			expectedResultSize = std::min(expectedResultSize, *limit);
		}
		std::function<int()> expectedId;
		if (testCases.front().sort.testId) {
			if (const int r = rand() % 3; r != 0) {
				const size_t s = rand() % kShards;
				expectedResultSize = 0;
				bool first = true;
				size_t start = 0;
				if (testCases.front().desc) {
					for (size_t i = tableSize, o = offset.value_or(0); i > 0 && expectedResultSize < limit.value_or(UINT_MAX); --i) {
						if ((r == 1) != ((i - 1) % kShards == s)) {
							if (o > 0) {
								--o;
							} else {
								if (first) {
									first = false;
									start = i - 1;
								}
								++expectedResultSize;
							}
						}
					}
					if (r == 1) {
						expectedId = [id = start, s, this]() mutable {
							const auto res = id--;
							if (id % kShards == s) --id;
							return res;
						};
					} else {
						expectedId = [id = start, this]() mutable { return (id -= kShards) + kShards; };
					}
				} else {
					for (size_t i = 0, o = offset.value_or(0); i < tableSize && expectedResultSize < limit.value_or(UINT_MAX); ++i) {
						if ((r == 1) != (i % kShards == s)) {
							if (o > 0) {
								--o;
							} else {
								if (first) {
									first = false;
									start = i;
								}
								++expectedResultSize;
							}
						}
					}
					if (r == 1) {
						expectedId = [id = start, s, this]() mutable {
							const auto res = id++;
							if (id % kShards == s) ++id;
							return res;
						};
					} else {
						expectedId = [id = start, this]() mutable { return (id += kShards) - kShards; };
					}
				}
				if (r == 1) {
					q.Not();
				}
				q.Where(kFieldShard, CondEq, "key" + std::to_string(s + 1));
			} else {
				if (testCases.front().desc) {
					expectedId = [id = tableSize - 1 - offset.value_or(0)]() mutable { return id--; };
				} else {
					expectedId = [id = offset.value_or(0)]() mutable { return id++; };
				}
			}
		}
		const std::string sql = q.GetSQL();
		client::QueryResults qr;
		std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what() << " NS SIZE: " << tableSize << "; " << sql;
		EXPECT_EQ(qr.Count(), expectedResultSize) << "NS SIZE: " << tableSize << "; " << sql;
		size_t count = 0;
		for (auto it = qr.begin(), prevIt = it, end = qr.end(); it != end; prevIt = it, ++it) {
			int prevResult = 0;
			for (auto i = testCases.begin(), e = testCases.end(); i != e; ++i) {
				const int result = i->sort.test(it);
				if (prevResult == 0) {
					EXPECT_TRUE(i->desc ? result <= 0 : result >= 0)
						<< "NS SIZE: " << tableSize << "; " << sql << "\nPrevious Item: " << prevIt.GetItem().GetJSON()
						<< "\nCurrent  Item: " << it.GetItem().GetJSON();
					prevResult = result;
				}
			}
			if (testCases.front().sort.testId) {
				const auto id = getField<int>(kFieldId, it);
				EXPECT_EQ(id, expectedId()) << "NS SIZE: " << tableSize << "; " << sql;
			}
			++count;
		}
		EXPECT_EQ(count, expectedResultSize) << "NS SIZE: " << tableSize << "; " << sql;
	}
}
