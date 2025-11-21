#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <fstream>
#include <future>
#include "cluster/sharding/ranges.h"
#include "core/cjson/csvbuilder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/itemimpl.h"
#include "core/queryresults/queryresults.h"
#include "core/system_ns_names.h"
#include "estl/condition_variable.h"
#include "estl/gift_str.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/tuple_utils.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tests/unit/csv2jsonconverter.h"
#include "sharding_api.h"
#include "tools/float_comparison.h"
#include "vendor/gason/gason.h"
#include "yaml-cpp/yaml.h"

using namespace reindexer;

static void CheckServerIDs(std::vector<std::vector<ServerControl>>& svc) {
	WrSerializer ser;
	size_t idx = 0;
	for (auto& cluster : svc) {
		for (auto& sc : cluster) {
			auto rx = sc.Get()->api.reindexer;
			client::QueryResults qr;
			auto err = rx->Select(Query(kConfigNamespace).Where("type", CondEq, "replication"), qr);
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
			constexpr size_t kExpectedDataCount = 40;
			std::unordered_set<int> ids;
			{
				// Inner join
				client::QueryResults qr;
				Query q = Query(nsName)
							  .Where(kFieldLocation, CondEq, key)
							  .InnerJoin(kFieldId, kFieldId, CondEq, Query(nsName).Where(kFieldLocation, CondEq, key));

				Error err = rx->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
				ASSERT_EQ(qr.Count(), kExpectedDataCount);
				ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet);
				for (auto it : qr) {
					auto item = it.GetItem();
					ASSERT_TRUE(item.Status().ok()) << "; i = " << i << "; location = " << key << "; item status: " << item.Status().what();
					auto json = item.GetJSON();
					{
						gason::JsonParser p;
						auto root = p.Parse(json);
						ASSERT_EQ(root["location"].As<std::string>(), key) << json;
						auto [empIt, emplaced] = ids.emplace(root["id"].As<int>());
						(void)empIt;
						ASSERT_TRUE(emplaced) << "Non-unique ID. JSON: " << json;
					}

					const auto& joinedData = it.GetJoined();
					EXPECT_EQ(joinedData.size(), 1);
					for (size_t joinedField = 0; joinedField < joinedData.size(); ++joinedField) {
						QueryResults qrJoined;
						const auto& joinedItems = joinedData[joinedField];
						for (const auto& itemData : joinedItems) {
							ItemImpl itemimpl = ItemImpl(qr.GetPayloadType(1), qr.GetTagsMatcher(1));
							itemimpl.Unsafe(true);
							itemimpl.FromCJSON(itemData.data);

							std::string_view joinedJson = itemimpl.GetJSON();

							EXPECT_EQ(joinedJson, json);
						}
					}
				}
			}
			{
				// Subquery
				VariantArray requestedIDs;
				const auto kExpectedSubqueryDataCount = kExpectedDataCount / 2;
				requestedIDs.reserve(kExpectedSubqueryDataCount);
				auto idsIt = ids.begin();
				for (size_t j = 0; j < kExpectedSubqueryDataCount; ++j, ++idsIt) {
					requestedIDs.emplace_back(*idsIt);
				}
				client::QueryResults qr;
				Query q =
					Query(nsName)
						.Where(kFieldLocation, CondEq, key)
						.Where(kFieldId, CondEq,
							   Query(nsName).Select({kFieldId}).Where(kFieldId, CondSet, requestedIDs).Where(kFieldLocation, CondEq, key));

				Error err = rx->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key;
				ASSERT_EQ(qr.Count(), kExpectedSubqueryDataCount);
				ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet);
				for (auto it : qr) {
					auto item = it.GetItem();
					ASSERT_TRUE(item.Status().ok()) << "; i = " << i << "; location = " << key << "; item status: " << item.Status().what();
					{
						auto json = item.GetJSON();
						gason::JsonParser p;
						auto root = p.Parse(json);
						ASSERT_EQ(root["location"].As<std::string>(), key) << json;
						auto id = root["id"].As<int>(-1);
						ASSERT_NE(id, -1) << json;
						ASSERT_NE(
							std::find_if(requestedIDs.begin(), requestedIDs.end(), [id](const Variant& v) { return v.As<int>() == id; }),
							requestedIDs.end())
							<< json;
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
				Query q = Query(nsName).Set(kFieldData, updated);
				q.Where(kFieldLocation, CondEq, key);
				q.type_ = QueryUpdate;
				Error err = rx->Update(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; location = " << key;
				const std::string expectedLocation = "\"location\":\""s + key + '"';
				const std::string expectedData = "\"data\":\""s + updated + '"';
				EXPECT_EQ(qr.Count(), 40);
				ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet);
				for (auto it : qr) {
					auto item = it.GetItem();
					std::string_view json = item.GetJSON();
					EXPECT_NE(json.find(expectedLocation), std::string_view::npos) << json << "; expected: {" << expectedLocation << "}";
					EXPECT_NE(json.find(expectedData), std::string_view::npos) << json << "; expected: {" << expectedData << "}";
				}
			}

			{
				const std::string updatedErr = "updated_" + RandString();
				Query qNoShardKey = Query(nsName).Set(kFieldData, updatedErr);
				client::QueryResults qrErr;
				Error err = rx->Update(qNoShardKey, qrErr);
				ASSERT_FALSE(err.ok()) << err.what();
				Query qNoShardKeySelect = Query(nsName);
				client::QueryResults qrSelect;
				err = rx->Select(qNoShardKeySelect, qrSelect);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_NE(qrSelect.GetShardingConfigVersion(), ShardingSourceId::NotSet);
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
			Error err = rx->Delete(Query::FromSQL(fmt::format("delete from {} where {} = '{}'", nsName, kFieldLocation, key)), qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; location = " << key;
			ASSERT_EQ(qr.Count(), 40) << "location = " << key;
			ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet) << "location = " << key;
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
				for (auto& m : sharded) {
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

				Query q = Query(nsName).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(j));
				client::QueryResults qr;
				err = rx->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(qr.Count(), 1) << "i = " << i << "; shard = " << shard << "; location = " << key;
				ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet)
					<< "i = " << i << "; shard = " << shard << "; location = " << key;
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
		for (auto& m : sharded) {
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

			Query q = Query(nsName).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::QueryResults qr;
			err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 1) << "i = " << i << "; shard = " << shard << "; location = " << key;
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
			Query q = Query(nsName).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::QueryResults qr;
			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 1);
			ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet) << "shard = " << shard << "; location = " << key;
			client::Item it = qr.begin().GetItem();
			itemJson = std::string(it.GetJSON());
		}

		reindexer::client::Item item = rx->NewItem(nsName);
		ASSERT_TRUE(item.Status().ok());

		Error err = item.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx->Delete(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();

		Query q = Query(nsName).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
		client::QueryResults qr;
		err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 0) << qr.Count() << "; from proxy; shard = " << shard << "; location = " << key;
		ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet) << "shard = " << shard << "; location = " << key;
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
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(nsdefs.size(), 1);
			ASSERT_EQ(nsdefs.front().name, nsName);
			ASSERT_TRUE(std::find_if(nsdefs.front().indexes.begin(), nsdefs.front().indexes.end(),
									 [](const IndexDef& index) { return index.Name() == "new"; }) != nsdefs.front().indexes.end());
			err = rx->DropIndex(nsName, indexDef);
			ASSERT_TRUE(err.ok()) << err.what();
			nsdefs.clear();
			err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(nsName));
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(nsdefs.size(), 1);
			ASSERT_EQ(nsdefs.front().name, nsName);
			ASSERT_TRUE(std::find_if(nsdefs.front().indexes.begin(), nsdefs.front().indexes.end(),
									 [](const IndexDef& index) { return index.Name() == "new"; }) == nsdefs.front().indexes.end());
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
		err = rx->Select(Query(nsName).Where(kFieldLocation, CondEq, key), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 0) << qr.Count();
		ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet) << "shard = " << shard << "; location = " << key;
	}

	client::QueryResults qr;
	err = rx->Select(Query(nsName), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 0) << qr.Count();
	ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet);

	err = rx->DropNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<NamespaceDef> nsdefs;
	err = rx->EnumNamespaces(nsdefs, EnumNamespacesOpts().HideSystem().HideTemporary().WithFilter(nsName));
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(nsdefs.empty());
}

void ShardingApi::checkTransactionErrors(client::Reindexer& rx, std::string_view nsName) {
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
			EXPECT_EQ(err.whatStr().substr(0, kExpectedErr.size()), kExpectedErr);
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
						Query q = Query(nsName).Where(kFieldLocation, CondEq, key);
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
				ASSERT_TRUE(err.ok()) << err.what() << "; connection = " << i << "; shard = " << shard << "; mode = " << mode;

				client::QueryResults qr;

				err = rx->Select(Query(nsName).Where(kFieldLocation, CondEq, key), qr);
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
					Query q = Query(nsName).Set(kFieldData, updated).Where(kFieldLocation, CondEq, key);
					q.type_ = QueryUpdate;
					err = tr.Modify(std::move(q));
					ASSERT_TRUE(err.ok()) << err.what();
					client::QueryResults qrTx;
					err = rx->CommitTransaction(tr, qrTx);
					ASSERT_TRUE(err.ok()) << err.what();

					qr = client::QueryResults();
					err = rx->Select(Query(nsName).Where(kFieldLocation, CondEq, key), qr);
					ASSERT_TRUE(err.ok()) << err.what();
					std::string toFind = "\"location\":\"" + key + "\"";
					for (auto it : qr) {
						auto item = it.GetItem();
						std::string_view json = item.GetJSON();
						ASSERT_TRUE(json.find(toFind) != std::string_view::npos) << json;
					}
				}
			}

			checkTransactionErrors(*rx, nsName);
		}
	}
}

TEST_F(ShardingApi, CheckMaskingTest) {
	constexpr auto dsnTmplt = "cproto://{}:{}@127.0.0.1:6534/some_db";
	auto login = "userlogin";
	auto passwd = "userpassword";
	auto orig = fmt::format(dsnTmplt, login, passwd);
	auto masked = fmt::format(dsnTmplt, maskLogin(login), maskPassword(passwd));
	auto dsn = DSN(orig);

	std::stringstream ss;
	ss << dsn;
	ASSERT_EQ(masked, ss.str());

	Error err(errParams, "{}", dsn);
	ASSERT_EQ(err.whatStr(), masked);

	ASSERT_EQ(fmt::format("{}", dsn), masked);

	ASSERT_TRUE(RelaxCompare(dsn, DSN(masked)));
	auto maskedDsn = DSN(masked);

	ASSERT_EQ(fmt::format("{}", dsn), fmt::format("{}", maskedDsn));
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

void ShardingApi::runTransactionsTest(std::string_view nsName, const std::map<int, std::set<int>>& shardDataDistrib) {
	TestCout() << "Running TransactionsTest" << std::endl;

	std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;
	Error err = rx->TruncateNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	client::QueryResults qr;
	err = rx->Select(Query(nsName), qr);
	ASSERT_TRUE(err.ok()) << err.what() << "; ns: " << nsName;
	ASSERT_EQ(qr.Count(), 0);
	ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet);

	for (size_t shard = 0; shard < kShards; ++shard) {
		const auto& keys = shardDataDistrib.at(shard);

		reindexer::client::Transaction tr = rx->NewTransaction(nsName);
		ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();
		for (const auto& key : keys) {
			client::Item item = tr.NewItem();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			WrSerializer wrser;
			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, key);
			jsonBuilder.End();

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			err = tr.Upsert(std::move(item));
			ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard << "; key = " << key;
		}

		client::QueryResults qrTx;
		err = rx->CommitTransaction(tr, qrTx);
		ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard;
		ASSERT_NE(qrTx.GetShardingConfigVersion(), ShardingSourceId::NotSet) << "shard = " << shard;
	}

	qr = client::QueryResults();
	err = rx->Select(Query(nsName), qr);
	ASSERT_TRUE(err.ok()) << err.what() << "; ns: " << nsName;
	ASSERT_NE(qr.GetShardingConfigVersion(), ShardingSourceId::NotSet) << "ns = " << nsName;

	int sizeQR = std::accumulate(shardDataDistrib.begin(), shardDataDistrib.end(), 0,
								 [](int sum, const auto& el) { return sum + el.second.size(); });
	ASSERT_EQ(qr.Count(), sizeQR);
}

template <typename T>
void ShardingApi::runSelectTest(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib) {
	TestCout() << "Running SELECT test" << std::endl;

	std::set<T> checkData;
	for (auto& [_, s] : shardDataDistrib) {
		(void)_;
		checkData.insert(s.begin(), s.end());
	}

	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
		client::QueryResults qr;
		Error err = rx->Select(Query(nsName), qr);

		ASSERT_TRUE(err.ok()) << err.what() << "; node index = " << i;
		ASSERT_EQ(qr.Count(), checkData.size()) << "node index = " << i;

		for (auto& res : qr) {
			WrSerializer wrser;
			err = res.GetJSON(wrser);
			ASSERT_TRUE(err.ok()) << err.what();

			gason::JsonParser parser;
			auto jsonNodeValue =
				parser.Parse(wrser.Slice())["id"].As<T>(T(), std::numeric_limits<T>::lowest());	 // lowest for correct work with neg values

			ASSERT_GT(checkData.count(jsonNodeValue), 0) << "Id = " << jsonNodeValue << "; Slice -" << wrser.Slice();
		}
	}
}

template <typename T>
void ShardingApi::runLocalSelectTest(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib) {
	TestCout() << "Running LOCAL SELECT test" << std::endl;

	auto checkDataDistrib = [nsName, this, &shardDataDistrib](int shard) {
		std::shared_ptr<client::Reindexer> rx = svc_[shard][0].Get()->api.reindexer;
		Query q{std::string(nsName)};
		q.Local();

		client::QueryResults qr;
		Error err = rx->Select(q, qr);

		ASSERT_TRUE(err.ok()) << err.what() << "; ns: " << nsName;
		ASSERT_EQ(qr.Count(), shardDataDistrib.at(shard).size()) << "Shard - " << shard;

		for (auto& res : qr) {
			WrSerializer wrser;
			err = res.GetJSON(wrser);
			ASSERT_TRUE(err.ok()) << err.what();

			gason::JsonParser parser;
			auto jsonNodeValue =
				parser.Parse(wrser.Slice())["id"].As<T>(T(), std::numeric_limits<T>::lowest());	 // lowest for correct work with neg values
			ASSERT_GT(shardDataDistrib.at(shard).count(jsonNodeValue), 0) << "Id = " << jsonNodeValue << "; Slice -" << wrser.Slice();
		}
	};

	for (size_t shard = 0; shard < kShards; ++shard) {
		checkDataDistrib(shard);
	}
}

template <typename T>
void ShardingApi::fillShard(int shard, const std::set<T>& data, const std::string& kNsName, const std::string& kFieldId) {
	auto rx = svc_[shard][0].Get()->api.reindexer;

	NamespaceDef nsDef{kNsName};

	static_assert(std::is_integral_v<T> || std::is_floating_point_v<T> || std::is_same_v<T, std::string>,
				  "Unsupported type for keys of sharding");

	if constexpr (std::is_integral_v<T>) {
		nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
	} else if constexpr (std::is_floating_point_v<T>) {
		nsDef.AddIndex(kFieldId, "tree", "double", IndexOpts().PK());
	} else if constexpr (std::is_same_v<T, std::string>) {
		nsDef.AddIndex(kFieldId, "hash", "string", IndexOpts().PK());
	}

	Error err = rx->AddNamespace(nsDef);
	ASSERT_TRUE(err.ok()) << err.what();

	for (const auto& value : data) {
		WrSerializer wrser;
		auto item = rx->NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put(kFieldId, value);
		jsonBuilder.End();

		err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx->Upsert(kNsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

template <typename T>
void ShardingApi::fillShards(const std::map<int, std::set<T>>& shardDataDistrib, const std::string& kNsName, const std::string& kFieldId) {
	TestCout() << "Filling in the data" << std::endl;
	std::set<T> data;
	for (const auto& p : shardDataDistrib) {
		data.insert(p.second.begin(), p.second.end());
	}
	fillShard(0, data, kNsName, kFieldId);
}

TEST_F(ShardingApi, ShardingInvalidTxTest) {
	const std::string kNsName = "invalid_tx_ns";
	const std::string kSharingIdx = "sharding_key";

	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;	 // Only one node in the cluster is needed for the test
	cfg.additionalNss.emplace_back(kNsName);
	cfg.additionalNss[0].indexName = kSharingIdx;
	constexpr auto kShardKeyTmplt = "Shard{}Key{}";
	cfg.additionalNss[0].keyValuesNodeCreation = [kShardKeyTmplt](int shard) {
		ShardingConfig::Key key;
		for (int i = 0; i < 3; ++i) {
			key.values.emplace_back(Variant(fmt::format(kShardKeyTmplt, shard, i)));
		}
		key.shardId = shard;
		return key;
	};

	Init(std::move(cfg));

	NamespaceDef nsDef{kNsName};
	nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
	nsDef.AddIndex(kSharingIdx, "hash", "string", IndexOpts());

	std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;

	Error err = rx->AddNamespace(nsDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::client::Transaction tr = rx->NewTransaction(kNsName);
	ASSERT_TRUE(tr.Status().ok()) << tr.Status().what();

	const auto itemTmpltBase = fmt::format("{{{{\"{}\": {{}}, \"{}\": \"{}\"}}}}", kFieldId, kSharingIdx, kShardKeyTmplt);
	const auto itemTmplt = fmt::runtime(itemTmpltBase);
	auto makeItem = [&tr](std::string_view rawItem) {
		client::Item item = tr.NewItem();
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		auto err = item.FromJSON(rawItem);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		return item;
	};

	for (const auto& rawItem : {fmt::format(itemTmplt, 0, 2, 0), fmt::format(itemTmplt, 1, 2, 1), fmt::format(itemTmplt, 2, 2, 2)}) {
		err = tr.Upsert(makeItem(rawItem));
		ASSERT_TRUE(err.ok()) << err.what();
	}

	err = tr.Upsert(makeItem(fmt::format(itemTmplt, 3, 1, 1)));
	ASSERT_FALSE(err.ok()) << err.what();
	ASSERT_STREQ(err.what(),
				 "Transaction query to a different shard: 1 (2 is expected); First tx shard key - 'Shard2Key0', current tx shard key - "
				 "'Shard1Key1'");
}

TEST_F(ShardingApi, BaseApiTestsetForRanges) {
	const std::map<int, std::set<int>> shardDataDistrib{
		{0, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 20, 26, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39}},
		{1, {11, 12, 13, 14, 15, 17, 18, 19}},
		{2, {21, 22, 23, 24, 25, 27, 28, 29}}};
	const std::string kNsName = "ns_for_ranges";

	InitShardingConfig cfg;
	cfg.additionalNss.emplace_back(kNsName);
	cfg.additionalNss[0].indexName = "id";
	cfg.additionalNss[0].keyValuesNodeCreation = [](int shard) {
		// resulted key template for i shard
		// 10i + 1
		// [10i + 2, 10i + 5]
		// [10i + 7, 10i + 9]
		//
		// 10i and 10i + 6 goes to default shard
		ShardingConfig::Key key;

		key.values.emplace_back(Variant(10 * shard + 1));
		key.values.emplace_back(Variant(10 * shard + 2));
		key.values.emplace_back(Variant(10 * shard + 3));
		key.values.emplace_back(sharding::Segment{Variant(10 * shard + 2), Variant(10 * shard + 5)});
		key.values.emplace_back(Variant(10 * shard + 8));
		key.values.emplace_back(Variant(10 * shard + 9));
		key.values.emplace_back(sharding::Segment{Variant(10 * shard + 7), Variant(10 * shard + 9)});

		key.shardId = shard;
		key.algorithmType = ShardingAlgorithmType::ByRange;
		return key;
	};
	Init(std::move(cfg));

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	CheckServerIDs(svc_);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
}

TEST_F(ShardingApi, SelectTestForRangesWithDoubleKeys) {
	const std::map<int, std::set<double>> shardDataDistrib{
		{0, {-2.61, 3.2, 7.09}}, {1, {-2.5, -2.0, 0, 1.9, 2.0, 3.0, 3.1}}, {2, {4.1, 5.25, 6.0}}, {3, {7.1, 7.2}}};
	const std::string kNsName = "ns_for_ranges";

	InitShardingConfig cfg;
	cfg.additionalNss.emplace_back(kNsName);
	cfg.additionalNss[0].indexName = "id";
	cfg.additionalNss[0].keyValuesNodeCreation = [](int shard) {
		ShardingConfig::Key key;
		switch (shard) {
			case 1: {
				key.values.emplace_back(Variant(2.0));
				key.values.emplace_back(sharding::Segment{Variant(2.1), Variant(3.1)});
				key.values.emplace_back(sharding::Segment{Variant(2.0), Variant(-2.5)});
				break;
			}
			case 2: {
				key.values.emplace_back(Variant(5.25));
				key.values.emplace_back(sharding::Segment{Variant(6.0), Variant(4.1)});
				break;
			}
			case 3: {
				key.values.emplace_back(Variant(7.1));
				key.values.emplace_back(sharding::Segment{Variant(7.1), Variant(7.1)});
				key.values.emplace_back(sharding::Segment{Variant(7.2), Variant(7.2)});
				break;
			}
			default:
				break;
		}
		key.shardId = shard;
		key.algorithmType = ShardingAlgorithmType::ByRange;
		return key;
	};
	cfg.shards = 4;
	Init(std::move(cfg));

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
}

template <typename T>
ShardingApi::ShardingConfig ShardingApi::makeShardingConfigByDistrib(std::string_view nsName,
																	 const std::map<int, std::set<T>>& shardDataDistrib, int shards,
																	 int nodes) const {
	using Cfg = ShardingConfig;
	constexpr auto algType =
		std::is_same_v<T, sharding::Segment<Variant>> ? ShardingAlgorithmType::ByRange : ShardingAlgorithmType::ByValue;

	std::vector<Cfg::Key> keys;
	keys.reserve(shardDataDistrib.size());

	for (const auto& [shardID, set] : shardDataDistrib) {
		std::vector<sharding::Segment<Variant>> values;
		values.reserve(set.size());

		if constexpr (algType == ShardingAlgorithmType::ByRange) {
			values = {set.begin(), set.end()};
		} else {
			for (const auto& v : set) {
				values.emplace_back(Variant(v));
			}
		}
		keys.emplace_back(Cfg::Key{shardID, algType, std::move(values)});
	}

	std::map<int, std::vector<DSN>> shardsMap;
	for (int shard = 0; shard < shards; ++shard) {
		shardsMap[shard].reserve(nodes);
		const size_t startId = shard * nodes;
		for (size_t i = startId; i < startId + nodes; ++i) {
			shardsMap[shard].emplace_back(
				MakeDsn(reindexer_server::UserRole::kRoleSharding, i, GetDefaults().defaultRpcPort + i, "shard" + std::to_string(i)));
		}
	}

	Cfg cfg{{{std::string(nsName), "id", std::move(keys), 0}}, std::move(shardsMap), -1};
	cfg.proxyConnCount = 3;
	cfg.proxyConnThreads = 2;
	cfg.proxyConnConcurrency = 4;
	cfg.configRollbackTimeout = std::chrono::seconds(10);
	return cfg;
}

Error ShardingApi::applyNewShardingConfig(client::Reindexer& rx, const ShardingConfig& config, ApplyType type,
										  std::optional<int64_t> sourceId) const {
	reindexer::client::Item item = rx.NewItem(kConfigNamespace);
	if (!item.Status().ok()) {
		return item.Status();
	}

	{
		WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser);
		jsonBuilder.Put("type", "action");
		auto action = jsonBuilder.Object("action");
		action.Put("command", "apply_sharding_config");
		if (type == ApplyType::Local) {
			assertrx(sourceId.has_value());
			action.Put("locally", true);
			action.Put("source_id", *sourceId);
		} else {
			assertrx(!sourceId.has_value());
		}
		auto configNode = action.Object("config");
		config.GetJSON(configNode, cluster::MaskingDSN::Disabled);
		configNode.End();
		action.End();
		jsonBuilder.End();
		Error err = item.FromJSON(wrser.Slice());
		if (!err.ok()) {
			return err;
		}
	}

	auto err = rx.Upsert(kConfigNamespace, item);
	if (!item.Status().ok()) {
		return item.Status();
	}

	return err;
}

void ShardingApi::checkMaskedDSNsInConfig(int shardId) {
	auto config = getShardingConfigFrom(*svc_[shardId][0].Get()->api.reindexer);
	assertrx(config.has_value());
	for (const auto& [shard, dsns] : config->shards) {
		int nodeId = 0;
		for (const auto& dsn : dsns) {
			auto serverId = shard * kNodesInCluster + nodeId++;
			auto expected = MakeDsn(reindexer_server::UserRole::kRoleSharding, serverId, GetDefaults().defaultRpcPort + serverId,
									"shard" + std::to_string(serverId));
			EXPECT_EQ(fmt::format("{}", expected), dsn);
		}
	}
}

TEST_F(ShardingApi, RuntimeShardingConfigTest) {
	const std::string kNsName = "ns";
	const std::map<int, std::set<int>> shardDataDistrib{{0, {0, 1, 2, 3, 4, 5}}, {1, {11, 12, 13, 14, 15}}, {2, {21, 22, 23, 24, 25}}};

	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;	 // Only one node in the cluster is needed for the test
	Init(std::move(cfg));

	auto& rx = *svc_[0][0].Get()->api.reindexer;
	auto err = rx.DropNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	auto newConfig = makeShardingConfigByDistrib(kNsName, shardDataDistrib, 3, 1);
	err = applyNewShardingConfig(rx, newConfig, ApplyType::Shared);
	ASSERT_TRUE(err.ok()) << err.what();

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
}

void ShardingApi::MultyThreadApplyConfigTest(ShardingApi::ApplyType type) {
	const std::string kNsName = "ns";
	const std::string kNonShardedNs = "nonShardedNs";
	const int shardsCount = 4;
	const int parallelSelectsCount = shardsCount;
	std::map<int, std::set<int>> shardDataDistrib;
	for (int i = 0; i < shardsCount; ++i) {
		shardDataDistrib[i] = {10 * i, 10 * i + 1, 10 * i + 2, 10 * i + 3, 10 * i + 4, 10 * i + 5};
	}
	constexpr int64_t kLocalSourceId = 11111;  // Use this value in the local config for consistancy

	kShards = shardsCount;
	kNodesInCluster = 1;

	svc_.resize(kShards);
	for (int i = 0; i < shardsCount; ++i) {
		svc_[i].resize(kNodesInCluster);
		StartByIndex(i);
	}

	auto fillDataLocalNss = [this, &kNonShardedNs](int shardId) {
		auto& rx = *svc_[shardId][0].Get()->api.reindexer;

		NamespaceDef nsDef{kNonShardedNs};
		nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
		Error err = rx.AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();

		WrSerializer wrser;
		for (int i = 0; i < 10; ++i) {
			wrser.Reset();
			auto item = rx.NewItem(kNonShardedNs);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, i);
			jsonBuilder.End();

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Insert(kNonShardedNs, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	};
	for (int shardId = 0; shardId < parallelSelectsCount; ++shardId) {
		fillDataLocalNss(shardId);
	}

	auto config = makeShardingConfigByDistrib(kNsName, shardDataDistrib, shardsCount, 1);

	// to avoid timeout-related errors in client rpc coroutines
	// when requesting statuses from multiple threads when calling LocatorService::Start
	config.reconnectTimeout = std::chrono::milliseconds(6'000);

	std::vector<std::future<Error>> results;
	results.reserve(shardsCount);

	std::vector<std::thread> parallelSelectsPerShard;
	parallelSelectsPerShard.reserve(parallelSelectsCount);
	std::atomic<bool> stopSelects = false;

	for (int i = 0; i < parallelSelectsCount; ++i) {
		parallelSelectsPerShard.emplace_back(
			[this, &stopSelects, &kNonShardedNs](int shardId) {
				do {
					client::QueryResults qr;
					Error err = svc_[shardId][0].Get()->api.reindexer->Select(Query(kNonShardedNs), qr);

					ASSERT_TRUE(err.ok()) << err.what() << "node index = " << shardId;
					ASSERT_EQ(qr.Count(), 10);
					unsigned cnt = 0;
					for (auto& it : qr) {
						auto item = it.GetItem();
						ASSERT_TRUE(item.Status().ok()) << item.Status().what() << "node index = " << shardId;
						[[maybe_unused]] auto json = item.GetJSON();
						++cnt;
					}
					ASSERT_EQ(cnt, qr.Count());
				} while (!stopSelects.load());
			},
			i);
	}

	for (int i = 0; i < shardsCount; ++i) {
		results.emplace_back(std::async(
			std::launch::async,
			[this, &config, type, kLocalSourceId](int shardId) {
				if (type == ApplyType::Local) {
					return applyNewShardingConfig(*svc_[shardId][0].Get()->api.reindexer, config, type, kLocalSourceId);
				}
				return applyNewShardingConfig(*svc_[shardId][0].Get()->api.reindexer, config, type);
			},
			i));
	}

	for (auto& res : results) {
		auto err = res.get();
		if (type == ApplyType::Local) {
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			if (!err.ok()) {
				ASSERT_THAT(err.what(), testing::MatchesRegex(".*(Config candidate is busy already|Attempt to apply|Attempt to reset).*"))
					<< err.what();
			}
		}
	}

	TestCout() << "Parallel applies sharding configs from different shards done." << std::endl;

	if (type == ApplyType::Shared) {
		auto err = applyNewShardingConfig(*svc_[0][0].Get()->api.reindexer, config, ApplyType::Shared);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);

	stopSelects.store(true);
	for (auto& selectsThread : parallelSelectsPerShard) {
		selectsThread.join();
	}
}

TEST_F(ShardingApi, RuntimeShardingConfigMultyApplyTest) { MultyThreadApplyConfigTest(ApplyType::Shared); }
TEST_F(ShardingApi, RuntimeShardingConfigLocallyApplyTest) { MultyThreadApplyConfigTest(ApplyType::Local); }

TEST_F(ShardingApi, RuntimeShardingConfigLocallyResetTest) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	cfg.shards = 4;
	Init(std::move(cfg));

	auto& rx = *svc_[0][0].Get()->api.reindexer;
	auto shCfg = getShardingConfigFrom(rx);
	assertrx(shCfg.has_value());

	reindexer::client::Item item = rx.NewItem(kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	{
		WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser);
		jsonBuilder.Put("type", "action");
		auto action = jsonBuilder.Object("action");
		action.Put("command", "apply_sharding_config");
		action.Put("locally", true);
		action.End();
		jsonBuilder.End();
		auto err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
	}

	auto err = rx.Upsert(kConfigNamespace, item);
	ASSERT_TRUE(err.ok()) << err.what();

	shCfg = getShardingConfigFrom(rx);
	assertrx(!shCfg.has_value());
}

void ShardingApi::checkConfig(const ServerControl::Interface::Ptr& server, const cluster::ShardingConfig& config) {
	ASSERT_NO_THROW(checkConfigThrow(server, config));
}
// It is necessary to duplicate the implementation of the ShardingConfig operator==
// so that the operator== redefined in the "auth_tools.h" is used when comparing DSNs
static bool CompareShardingConfigs(const cluster::ShardingConfig& lhs, const cluster::ShardingConfig& rhs) {
	return lhs.namespaces == rhs.namespaces && lhs.thisShardId == rhs.thisShardId && lhs.shards == rhs.shards &&
		   lhs.reconnectTimeout == rhs.reconnectTimeout && lhs.shardsAwaitingTimeout == rhs.shardsAwaitingTimeout &&
		   lhs.configRollbackTimeout == rhs.configRollbackTimeout && lhs.proxyConnCount == rhs.proxyConnCount &&
		   lhs.proxyConnConcurrency == rhs.proxyConnConcurrency && rhs.proxyConnThreads == lhs.proxyConnThreads &&
		   rhs.sourceId == lhs.sourceId;
}

void ShardingApi::checkConfigThrow(const ServerControl::Interface::Ptr& server, const cluster::ShardingConfig& config) {
	Error error;
	auto fromConfigNs = getShardingConfigFrom(*server->api.reindexer);
	assertrx(fromConfigNs.has_value());
	if (!CompareShardingConfigs(config, *fromConfigNs)) {
		error = std::logic_error(
			fmt::format("The equality of config and fromConfigNs is expected, but:\nconfig:\n\t{}\nfromConfigNs:\n\t{}\n",
						config.GetJSON(cluster::MaskingDSN::Disabled), fromConfigNs->GetJSON(cluster::MaskingDSN::Disabled)));
	}

	std::string configYAML;
	auto res = fs::ReadFile(server->GetShardingConfigFilePath(), configYAML);
	ASSERT_GT(res, 0);
	ShardingConfig fromFileConfig;
	auto err = fromFileConfig.FromYAML(configYAML);
	ASSERT_TRUE(err.ok()) << err.what();
	if (!CompareShardingConfigs(config, fromFileConfig)) {
		error = std::logic_error(
			fmt::format("{}The equality of config and fromFileConfig is expected, but:\nconfig:\n\t{}\nfromFileConfig:\n\t{}\n",
						!error.ok() ? error.whatStr() + '\n' : "", config.GetJSON(cluster::MaskingDSN::Disabled),
						fromFileConfig.GetJSON(cluster::MaskingDSN::Disabled)));
	}

	if (!error.ok()) {
		throw error;
	}
}

int64_t ShardingApi::getSourceIdFrom(const ServerControl::Interface::Ptr& server) {
	auto cfg = getShardingConfigFrom(*server->api.reindexer);
	return cfg.has_value() ? cfg->sourceId : int64_t(ShardingSourceId::NotSet);
}

std::optional<ShardingApi::ShardingConfig> ShardingApi::getShardingConfigFrom(reindexer::client::Reindexer& rx) {
	client::QueryResults qr;
	Query q = Query(kConfigNamespace).Where("type", CondEq, "sharding");
	auto err = rx.Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	if (qr.Count() == 0) {
		return std::nullopt;
	}
	EXPECT_EQ(qr.Count(), 1);

	WrSerializer wser;
	err = qr.begin().GetJSON(wser, false);
	EXPECT_TRUE(err.ok()) << err.what();
	gason::JsonParser parser;
	auto readConfigJson = parser.Parse(giftStr(wser.Slice()))["sharding"];

	ShardingConfig cfg;
	err = cfg.FromJSON(readConfigJson);
	EXPECT_TRUE(err.ok()) << err.what();
	return std::optional<ShardingConfig>(std::move(cfg));
}

void ShardingApi::changeClusterLeader(int shardId) {
	SCOPED_TRACE(fmt::format("node index: {}", shardId));

	auto qrReplStat = svc_[shardId][0].Get()->api.ExecSQL("select * from #replicationstats where type = 'cluster'");
	ASSERT_TRUE(qrReplStat.Status().ok()) << qrReplStat.Status().what();
	ASSERT_EQ(qrReplStat.Count(), 1);

	auto itemRS = qrReplStat.begin().GetItem();
	gason::JsonParser parserRS;
	auto nodes = parserRS.Parse(itemRS.GetJSON())["nodes"];
	ASSERT_TRUE(!nodes.empty());
	int leaderId = -1;
	for (auto& node : nodes) {
		if (node["role"].As<std::string_view>() == "leader") {
			leaderId = node["server_id"].As<int>();
			break;
		}
	}
	ASSERT_GT(leaderId, -1);

	auto newLeaderServerId = (leaderId - shardId * kNodesInCluster + 1) % kNodesInCluster + shardId * kNodesInCluster;
	auto item = svc_[shardId][0].Get()->CreateClusterChangeLeaderItem(newLeaderServerId);
	svc_[shardId][0].Get()->api.Update(kConfigNamespace, item);
}

TEST_F(ShardingApi, RuntimeUpdateShardingCfgWithClusterTest) {
	const std::string kNsName = "ns";
	const std::string kNonShardedNs = "nonShardedNs";
	const int shardsCount = 3;
	const int nodesInCluster = 3;
	std::map<int, std::set<int>> shardDataDistrib;
	for (int i = 0; i < shardsCount; ++i) {
		shardDataDistrib[i] = {10 * i, 10 * i + 1, 10 * i + 2, 10 * i + 3, 10 * i + 4, 10 * i + 5};
	}

	InitShardingConfig cfg;
	cfg.needFillDefaultNs = false;
	cfg.shards = shardsCount;
	cfg.nodesInCluster = nodesInCluster;
	Init(std::move(cfg));

	const int parallelSelectsCount = shardsCount;

	auto fillDataLocalNss = [this, &kNonShardedNs](int shardId) {
		auto& rx = *svc_[shardId][0].Get()->api.reindexer;

		NamespaceDef nsDef{kNonShardedNs};
		nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
		Error err = rx.AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();

		for (int i = 0; i < 10; ++i) {
			WrSerializer wrser;
			auto item = rx.NewItem(kNonShardedNs);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, i);
			jsonBuilder.End();

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Insert(kNonShardedNs, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	};
	for (int shardId = 0; shardId < parallelSelectsCount; ++shardId) {
		fillDataLocalNss(shardId);
	}

	waitSync(kNonShardedNs);

	std::vector<std::thread> parallelSelectsPerShard;
	parallelSelectsPerShard.reserve(parallelSelectsCount);
	std::atomic<bool> stopSelects = false;

	for (int i = 0; i < parallelSelectsCount; ++i) {
		parallelSelectsPerShard.emplace_back(
			[this, &stopSelects, &kNonShardedNs](int shardId) {
				do {
					client::QueryResults qr;
					Error err = svc_[shardId][0].Get()->api.reindexer->Select(Query(kNonShardedNs), qr);

					ASSERT_TRUE(err.ok()) << err.what() << "; node index = " << shardId;
					ASSERT_EQ(qr.Count(), 10);
					unsigned cnt = 0;
					WrSerializer ser;
					for (auto& it : qr) {
						err = it.GetJSON(ser, false);
						ASSERT_TRUE(err.ok()) << err.what() << "node index = " << shardId;
						++cnt;
					}
					ASSERT_EQ(cnt, qr.Count());
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				} while (!stopSelects.load());
			},
			i);
	}

	auto config = makeShardingConfigByDistrib(kNsName, shardDataDistrib, shardsCount, nodesInCluster);

	// to avoid timeout-related errors in client rpc coroutines
	// when requesting statuses from multiple threads when calling LocatorService::Start
	config.reconnectTimeout = std::chrono::milliseconds(6'000);

	std::vector<std::thread> parallelChangeLeaders;
	parallelChangeLeaders.reserve(shardsCount);
	std::atomic<bool> stopChangeLeader = false;

	// On 0-cluster don't change leader because sending new config from it
	for (int shardId = 1; shardId < shardsCount; ++shardId) {
		parallelChangeLeaders.emplace_back(
			[this, &stopChangeLeader](int shardId) {
				do {
					changeClusterLeader(shardId);
					std::this_thread::yield();
				} while (!stopChangeLeader.load());
			},
			shardId);
	}

	auto err = applyNewShardingConfig(*svc_[0][0].Get()->api.reindexer, config, ApplyType::Shared);

	stopChangeLeader.store(true);
	for (auto& chLeaderThread : parallelChangeLeaders) {
		chLeaderThread.join();
	}

	if (!err.ok()) {
		ASSERT_THAT(err.what(), testing::MatchesRegex(".*(Role was switched to|Request was proxied to follower node).*")) << err.what();

		TEST_COUT << err.what() << std::endl;

		// Applying without changing leader must be done correctly
		do {
			err = applyNewShardingConfig(*svc_[0][0].Get()->api.reindexer, config, ApplyType::Shared);
			if (err.ok()) {
				break;
			}

			ASSERT_THAT(err.what(), testing::MatchesRegex(".*Config candidate is busy already.*")) << err.what();
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		} while (true);
	}

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);

	TestCout() << "Comparison of sharding configs on cluster nodes" << std::endl;
	config.sourceId = getSourceIdFrom(svc_[0][0].Get());
	for (size_t i = 0; i < svc_.size(); ++i) {
		config.thisShardId = i;
		for (auto& node : svc_[i]) {
			checkConfig(node.Get(), config);
		}
	}

	stopSelects.store(true);
	for (auto& selectsThread : parallelSelectsPerShard) {
		selectsThread.join();
	}
}

TEST_F(ShardingApi, RuntimeUpdateShardingWithDisabledNodesTest) {
	const std::string kNsName = "ns";
	const int shardsCount = 3;
	const int nodesInCluster = 3;
	std::map<int, std::set<int>> shardDataDistrib;
	for (int i = 0; i < shardsCount; ++i) {
		shardDataDistrib[i] = {10 * i, 10 * i + 1, 10 * i + 2, 10 * i + 3, 10 * i + 4, 10 * i + 5};
	}

	InitShardingConfig cfg;
	cfg.needFillDefaultNs = false;
	cfg.shards = shardsCount;
	cfg.nodesInCluster = nodesInCluster;
	Init(std::move(cfg));

	auto config = makeShardingConfigByDistrib(kNsName, shardDataDistrib, shardsCount, nodesInCluster);

	// to avoid timeout-related errors in client rpc coroutines
	// when requesting statuses from multiple threads when calling LocatorService::Start
	config.reconnectTimeout = std::chrono::milliseconds(6'000);

	std::vector<std::thread> parallelDisablingNodes;
	parallelDisablingNodes.reserve(shardsCount);
	std::atomic<bool> stopDisableNodes = false;

	auto startServer = [this](ServerControl& server, int idx) {
		if (server.IsRunning()) {
			return false;
		}
		auto [shard, _] = getSCIdxs(idx);
		std::string pathToDb = fs::JoinPath(GetDefaults().baseTestsetDbPath, "shard" + std::to_string(shard) + "/" + std::to_string(idx));
		std::string dbName = "shard" + std::to_string(idx);
		const bool asProcess = kTestServersInSeparateProcesses;
		ServerControlConfig cfg(idx, GetDefaults().defaultRpcPort + idx, GetDefaults().defaultHttpPort + idx, std::move(pathToDb),
								std::move(dbName), true, 0, asProcess);
		server.InitServer(std::move(cfg));
		return true;
	};

	auto stopServer = [](ServerControl& server) {
		if (!server.Get()) {
			return false;
		}
		server.Stop();
		server.Drop();
		const auto kMaxServerStartTime = std::chrono::seconds(15);
		auto now = std::chrono::milliseconds(0);
		const auto pause = std::chrono::milliseconds(10);
		while (server.IsRunning()) {
			now += pause;
			EXPECT_TRUE(now < kMaxServerStartTime);
			assert(now < kMaxServerStartTime);

			std::this_thread::sleep_for(pause);
		}
		return true;
	};

	for (int shardId = 1; shardId < shardsCount; ++shardId) {
		parallelDisablingNodes.emplace_back(
			[this, &stopDisableNodes, &stopServer, &startServer](int shardId) {
				do {
					auto qr = svc_[shardId][0].Get()->api.ExecSQL("select * from #replicationstats where type = 'cluster'");
					ASSERT_EQ(qr.Count(), 1);

					auto item = qr.begin().GetItem();
					gason::JsonParser parserRS;
					auto nodes = parserRS.Parse(item.GetJSON())["nodes"];
					ASSERT_TRUE(!nodes.empty());

					int leaderId = -1;
					for (auto& node : nodes) {
						if (node["role"].As<std::string_view>() == "leader") {
							leaderId = node["server_id"].As<int>();
							break;
						}
					}
					ASSERT_GT(leaderId, -1);

					auto nodeId = (leaderId - shardId * nodesInCluster + 1) % nodesInCluster + shardId * nodesInCluster;
					auto srvId = (leaderId - shardId * nodesInCluster + 1) % nodesInCluster;
					ASSERT_TRUE(stopServer(svc_[shardId][srvId]));
					ASSERT_TRUE(startServer(svc_[shardId][srvId], nodeId));

				} while (!stopDisableNodes.load());
			},
			shardId);
	}

	auto err = applyNewShardingConfig(*svc_[0][0].Get()->api.reindexer, config, ApplyType::Shared);

	stopDisableNodes.store(true);
	for (auto& th : parallelDisablingNodes) {
		th.join();
	}

	if (!err.ok()) {
		// Applying without changing leader must be done correctly
		err = applyNewShardingConfig(*svc_[0][0].Get()->api.reindexer, config, ApplyType::Shared);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);

	TestCout() << "Comparison of sharding configs on cluster nodes" << std::endl;
	config.sourceId = getSourceIdFrom(svc_[0][0].Get());
	for (size_t i = 0; i < svc_.size(); ++i) {
		config.thisShardId = i;
		for (auto& node : svc_[i]) {
			checkConfig(node.Get(), config);
		}
	}
}

TEST_F(ShardingApi, RuntimeUpdateShardingWithActualConfigTest) {
	const std::string kNsName = "ns";
	const auto deadline = std::chrono::seconds(15);
	const auto delay = std::chrono::milliseconds(500);
	const int shardsCount = 3;
	const int nodesInCluster = 3;
	std::map<int, std::set<int>> shardDataDistrib;
	for (int i = 0; i < shardsCount; ++i) {
		shardDataDistrib[i] = {10 * i, 10 * i + 1, 10 * i + 2, 10 * i + 3, 10 * i + 4, 10 * i + 5};
	}

	InitShardingConfig cfg;
	cfg.needFillDefaultNs = false;
	cfg.shards = shardsCount;
	cfg.nodesInCluster = nodesInCluster;
	constexpr int64_t kSourceId = 919191919;
	Init(std::move(cfg));

	auto config = makeShardingConfigByDistrib(kNsName, shardDataDistrib, shardsCount, nodesInCluster);
	auto configIncorrect = config;
	configIncorrect.namespaces.begin()->ns += "_incorrect";

	std::vector<int> nodeIds(nodesInCluster);
	std::iota(nodeIds.begin(), nodeIds.end(), 0);
	for (int shardId = 0; shardId < shardsCount; ++shardId) {
		std::next_permutation(nodeIds.begin(), nodeIds.end());
		auto& shard = svc_[shardId];
		std::ignore = shard[0].Get()->GetReplicationStats("cluster");  // Await replication startup
		for (size_t i = 0; i < nodeIds.size(); ++i) {
			const bool useIncorrectCfg = (i != nodeIds.size() - 1);
			auto& localCfg = useIncorrectCfg ? configIncorrect : config;
			localCfg.thisShardId = shardId;
			localCfg.sourceId = useIncorrectCfg ? kSourceId : kSourceId + 1;
			auto err = applyNewShardingConfig(*shard[nodeIds[i]].Get()->api.reindexer, localCfg, ApplyType::Local, localCfg.sourceId);
			ASSERT_TRUE(err.ok()) << err.what();
			checkConfig(shard[nodeIds[i]].Get(), localCfg);
		}
	}

	for (int shardId = 0; shardId < shardsCount; ++shardId) {
		changeClusterLeader(shardId);
	}

	TestCout() << "Comparison of sharding configs on cluster nodes" << std::endl;
	// NOLINTNEXTLINE
	auto start = system_clock_w::now();
	for (bool needContinue = true; needContinue; std::this_thread::sleep_for(delay)) {
		try {
			for (size_t i = 0; i < svc_.size(); ++i) {
				config.thisShardId = i;
				for (auto& node : svc_[i]) {
					checkConfigThrow(node.Get(), config);
				}
			}
			break;
		} catch (...) {
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
		needContinue = system_clock_w::now() - start < deadline;
		ASSERT_TRUE(needContinue) << "Time limit for applying config by leaders exceeded";
	}

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
}

TEST_F(ShardingApi, RuntimeUpdateShardingWithFilledNssTest) {
	const std::string kNsName = "ns";
	const int shardsCount = 4;

	std::map<int, std::set<int>> shardDataDistrib;
	std::map<int, std::set<sharding::Segment<Variant>>> shardDataDistribBySegment;
	for (int i = 0; i < shardsCount; ++i) {
		shardDataDistrib[i] = {10 * i, 10 * i + 1, 10 * i + 2, 10 * i + 3, 10 * i + 4, 10 * i + 5};
		shardDataDistribBySegment[i] = {sharding::Segment{Variant(10 * i)}, sharding::Segment{Variant(10 * i + 1), Variant(10 * i + 4)},
										sharding::Segment{Variant(10 * i + 5)}};
	}

	std::set<int> dataForDefault{6, 7, 8, 16, 17, 18, 27, 38, 50, 99};
	shardDataDistrib[0].insert(dataForDefault.begin(), dataForDefault.end());

	InitShardingConfig cfg;
	cfg.shards = shardsCount;
	Init(std::move(cfg));
	auto oldConfigOpt = getShardingConfigFrom(*svc_[0][0].Get()->api.reindexer);
	ASSERT_TRUE(oldConfigOpt);
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
	ShardingConfig oldConfig = *oldConfigOpt;

	auto config = makeShardingConfigByDistrib(kNsName, shardDataDistribBySegment, shardsCount);

	for (int shard = 0; shard < shardsCount; ++shard) {
		fillShard(shard, shardDataDistrib[shard], kNsName, kFieldId);
	}

	// Filling the namespaces of the shards with values that should not be on these shards, and trying to apply the config
	std::map<int, int> wrongValsForShards{{0, 10}, {1, 99}, {2, 30}, {3, 20}};
	for (const auto& [shardId, val] : wrongValsForShards) {
		auto rx = svc_[shardId][0].Get()->api.reindexer;

		auto fillItem = [&, val = val](client::Item& item) {
			WrSerializer wrser;
			item = rx->NewItem(kNsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, val);
			jsonBuilder.End();

			auto err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
		};

		client::Item insertItem, deleteItem;
		fillItem(insertItem);
		fillItem(deleteItem);

		auto err = rx->Upsert(kNsName, insertItem);
		ASSERT_TRUE(err.ok()) << err.what();

		err = applyNewShardingConfig(*rx, config, ApplyType::Shared);
		ASSERT_FALSE(err.ok());

		ASSERT_EQ(fmt::format("Namespace 'ns' on the shard {} contains keys unrelated to the config(e.g. {})", shardId, val), err.what())
			<< err.what();

		err = rx->Delete(kNsName, deleteItem);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// Checking that the original config has not changed on all nodes
	for (size_t shard = 0; shard < kShards; ++shard) {
		oldConfig.thisShardId = shard;
		for (size_t node = 0; node < kNodesInCluster; ++node) {
			checkConfig(svc_[shard][node].Get(), oldConfig);
		}
	}

	// Setting a new config, its application should be successful
	// Then checking the basic sharding functionality with the new config
	auto err = applyNewShardingConfig(*svc_[0][0].Get()->api.reindexer, config, ApplyType::Shared);
	ASSERT_TRUE(err.ok()) << err.what();

	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
}

TEST_F(ShardingApi, NsNamesCaseInsensitivityTest) {
	const std::map<int, std::set<int>> shardDataDistrib{
		{0, {0, 1, 2, 3, 4}}, {1, {10, 11, 12, 13, 14}}, {2, {20, 21, 22, 23, 24}}, {3, {30, 31, 32, 33, 34}}};

	const std::string kNsName = "NameSPaCe1";

	InitShardingConfig cfg;
	cfg.additionalNss.emplace_back(kNsName);
	cfg.additionalNss[0].indexName = kFieldId;
	cfg.additionalNss[0].keyValuesNodeCreation = [](int shard) {
		ShardingConfig::Key key;
		for (int i = 0; i < 5; ++i) {
			key.values.emplace_back(Variant(10 * shard + i));
		}

		key.shardId = shard;
		return key;
	};
	cfg.shards = 4;

	Init(std::move(cfg));

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runSelectTest(toLower(kNsName), shardDataDistrib);
}

TEST_F(ShardingApi, CheckUpdCfgNsAfterApplySharingCfg) {
	const std::string kNsName = "ns";
	const std::map<int, std::set<int>> shardDataDistrib{{0, {0, 1, 2, 3, 4, 5}}, {1, {11, 12, 13, 14, 15}}, {2, {21, 22, 23, 24, 25}}};

	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;	 // Only one node in the cluster required for correct work runtime sharding config
	cfg.needFillDefaultNs = false;
	Init(std::move(cfg));

	auto& rx = *svc_[0][0].Get()->api.reindexer;
	auto newConfig = makeShardingConfigByDistrib(kNsName, shardDataDistrib, 3, 1);
	auto err = applyNewShardingConfig(rx, newConfig, ApplyType::Shared);
	ASSERT_TRUE(err.ok()) << err.what();

	auto readConfig = getShardingConfigFrom(rx);
	assertrx(readConfig.has_value());
	readConfig->thisShardId = -1;  // it's necessary for the correct comparison of both configs

	EXPECT_NE(readConfig->sourceId, ShardingSourceId::NotSet);
	readConfig->sourceId = ShardingSourceId::NotSet;
	ASSERT_TRUE(CompareShardingConfigs(newConfig, *readConfig)) << "expected:\n"
																<< newConfig.GetJSON(cluster::MaskingDSN::Disabled) << "\nobtained:\n"
																<< readConfig->GetJSON(cluster::MaskingDSN::Disabled);
}

TEST_F(ShardingApi, PartialClusterNodesSetInShardingCfgTest) {
	// Сhecking the ability to connect not only to cluster leaders,
	// if not all of clusters nodes are listed in the sharding config
	const std::string kNsName = "ns";
	const std::map<int, std::set<int>> shardDataDistrib{{0, {0, 1, 2, 3, 4, 5}}, {1, {11, 12, 13, 14, 15}}, {2, {21, 22, 23, 24, 25}}};
	const int knodesInCluster = 7;	//  in order to reduce the probability of connecting to the leader
	InitShardingConfig cfg;

	cfg.additionalNss.emplace_back(kNsName);
	cfg.additionalNss[0].indexName = "id";
	cfg.additionalNss[0].keyValuesNodeCreation = [](int shard) {
		ShardingConfig::Key key;
		for (int i = 0; i < 6; ++i) {
			key.values.emplace_back(Variant(10 * shard + i));
		}
		key.shardId = shard;
		key.algorithmType = ShardingAlgorithmType::ByRange;
		return key;
	};

	std::map<int, std::vector<DSN>> shardsMap;
	for (int shard = 0; shard < cfg.shards; ++shard) {
		// must comply with node numbering given by in Init function
		// take only one node for shard from cluster
		auto nodeId = shard * knodesInCluster;
		shardsMap[shard].emplace_back(MakeDsn(reindexer_server::UserRole::kRoleSharding, nodeId, GetDefaults().defaultRpcPort + nodeId,
											  "shard" + std::to_string(nodeId)));
	}
	// replace shards part in sharding config due to in one shard was only one of 7 nodes of sync cluster
	cfg.shardsMap = std::make_shared<std::map<int, std::vector<DSN>>>(std::move(shardsMap));
	cfg.nodesInCluster = knodesInCluster;
	Init(std::move(cfg));

	fillShards(shardDataDistrib, kNsName, kFieldId);
	waitSync(kNsName);
	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
	runTransactionsTest(kNsName, shardDataDistrib);
	waitSync(kNsName);

	TestCout() << "Checking correct work of selects after apply of transactions" << std::endl;

	runLocalSelectTest(kNsName, shardDataDistrib);
	runSelectTest(kNsName, shardDataDistrib);
}

TEST_F(ShardingApi, CheckSortQueryResultsByShardID) {
	// Check if query results sorted by sharding ID if ordering is not specified in query
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;	 // Only one node in the cluster is enough for the test
	const int count = cfg.rowsInTableOnShard * cfg.shards;
	Init(std::move(cfg));

	// For the test, it is enough to use default_namespace, which will contain `count` items
	const auto q = Query(default_namespace);
	for (size_t i = 0; i < NodesCount(); ++i) {
		std::shared_ptr<client::Reindexer> rx = getNode(i)->api.reindexer;
		client::QueryResults qr(kResultsWithShardId);
		Error err = rx->Select(q, qr);

		ASSERT_TRUE(err.ok()) << err.what() << "; node index = ";
		ASSERT_EQ(qr.Count(), count);

		int curShardId = -1;
		WrSerializer wrser;

		// Checking on each node that the output is sorted by shard ID
		for (auto& res : qr) {
			wrser.Reset();
			err = res.GetJSON(wrser);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_GE(res.GetShardID(), curShardId) << "Query results not sorted by shardID";
			curShardId = res.GetShardID();
		}
	}
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
	TestCout() << "Init done" << std::endl;

	std::vector<unsigned> threadsCountVec = {24};
	const Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key2").Limit(20);
	for (auto thCnt : threadsCountVec) {
		std::atomic<unsigned> requests = {0};
		std::vector<std::thread> threads;
		reindexer::condition_variable cv;
		reindexer::mutex mtx;
		bool ready = false;
		for (unsigned i = 0; i < thCnt; ++i) {
			std::shared_ptr<client::Reindexer> rx = std::make_shared<client::Reindexer>();
			Error err = rx->Connect(getNode(0)->kRPCDsn);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx->Status(true);
			ASSERT_TRUE(err.ok()) << err.what();

			threads.emplace_back([&q, rx, &requests, &cv, &mtx, &ready] {
				reindexer::unique_lock lck(mtx);
				cv.wait(lck, [&ready] { return ready; });
				lck.unlock();
				while (++requests < kTotalRequestsCount) {
					client::QueryResults qr;
					Error err = rx->Select(q, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				}
			});
		}

		// <Start profiling here>
		reindexer::unique_lock lck(mtx);
		ready = true;
		const auto beg = steady_clock_w::now();
		cv.notify_all();
		lck.unlock();
		TestCout() << fmt::format("Start with {} threads", thCnt) << std::endl;
		for (auto& th : threads) {
			th.join();
		}
		// <Stop profiling here>
		const auto diff = steady_clock_w::now() - beg;
		TestCout() << fmt::format("Done with {} threads in {} usec", thCnt, diff.count() / 1000) << std::endl;
	}
}

TEST_F(ShardingApi, Aggregations) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	const auto itemsCount = cfg.rowsInTableOnShard * cfg.shards;
	Init(std::move(cfg));
	std::shared_ptr<client::Reindexer> rx = getNode(0)->api.reindexer;
	client::QueryResults qr;
	Query q = Query(default_namespace).Aggregate(AggSum, {kFieldId}).Aggregate(AggMin, {kFieldId}).Aggregate(AggMax, {kFieldId});
	Error err = rx->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.GetAggregationResults().size(), 3);
	EXPECT_EQ(qr.GetAggregationResults()[0].GetType(), AggSum);
	EXPECT_TRUE(qr.GetAggregationResults()[0].GetValue());
	EXPECT_EQ(qr.GetAggregationResults()[0].GetValueOrZero(), (itemsCount - 1) * itemsCount / 2);
	EXPECT_EQ(qr.GetAggregationResults()[1].GetType(), AggMin);
	EXPECT_TRUE(qr.GetAggregationResults()[1].GetValue());
	EXPECT_EQ(qr.GetAggregationResults()[1].GetValueOrZero(), 0);
	EXPECT_EQ(qr.GetAggregationResults()[2].GetType(), AggMax);
	EXPECT_TRUE(qr.GetAggregationResults()[2].GetValue());
	EXPECT_EQ(qr.GetAggregationResults()[2].GetValueOrZero(), itemsCount - 1);
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
		EXPECT_STREQ(err.what(), "Duplication of shard key condition in the query");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Or().Where(kFieldId, CondEq, 0);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be connected with other conditions by operator OR");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldId, CondEq, 0).Or().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be connected with other conditions by operator OR");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Not().Where(kFieldLocation, CondEq, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be negative");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondGt, "key1");
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition can only be 'Eq'");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).WhereBetweenFields(kFieldLocation, CondEq, kFieldData);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key cannot be compared with another field");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket();
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).OpenBracket().OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket().CloseBracket();
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be included in bracket");
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
		EXPECT_STREQ(err.what(), "Join query must contain shard key");
	}
	{
		client::QueryResults qr;
		Query q =
			Query(default_namespace).InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Query to all shard can't contain JOIN, MERGE or SUBQUERY");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key from other node");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Not().Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be negative");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .InnerJoin(kFieldId, kFieldId, CondEq, Query{default_namespace}.Not().Where(kFieldLocation, CondLike, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition can only be 'Eq'");
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
		EXPECT_STREQ(err.what(), "Merge query must contain shard key");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Query to all shard can't contain JOIN, MERGE or SUBQUERY");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key from other node");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Merge(Query{default_namespace}.OpenBracket().Where(kFieldLocation, CondEq, "key1").CloseBracket());
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be included in bracket");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Distinct(kFieldId);
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Query to all shard can't contain aggregations AVG, Facet or Distinct");
	}
	for (const auto agg : {AggAvg, AggFacet, AggDistinct}) {
		client::QueryResults qr;
		Query q = Query(default_namespace).Aggregate(agg, {kFieldId});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Query to all shard can't contain aggregations AVG, Facet or Distinct");
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

	// Subqueries
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(kFieldId, CondEq, Query{default_namespace}.Select({kFieldId}).Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"), CondAny, VariantArray{});
		Error err = rx->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(kFieldId, CondEq, Query{default_namespace}.Select({kFieldId}));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Subquery must contain shard key");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").Where(Query{default_namespace}, CondAny, VariantArray{});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Subquery must contain shard key");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldId, CondEq, Query{default_namespace}.Select({kFieldId}).Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Query to all shard can't contain JOIN, MERGE or SUBQUERY");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace).Where(Query{default_namespace}.Where(kFieldLocation, CondEq, "key1"), CondAny, VariantArray{});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Query to all shard can't contain JOIN, MERGE or SUBQUERY");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(kFieldId, CondEq, Query{default_namespace}.Select({kFieldId}).Where(kFieldLocation, CondEq, "key2"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key from other node");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(Query{default_namespace}.Where(kFieldLocation, CondEq, "key2"), CondAny, VariantArray{});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key from other node");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(kFieldId, CondEq, Query{default_namespace}.Select({kFieldId}).Not().Where(kFieldLocation, CondEq, "key1"));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be negative");
	}
	{
		client::QueryResults qr;
		Query q =
			Query(default_namespace)
				.Where(kFieldLocation, CondEq, "key1")
				.Where(kFieldId, CondEq, Query{default_namespace}.Select({kFieldId}).Not().Where(kFieldLocation, CondAny, VariantArray{}));
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Sharding key value cannot be empty or an array");
	}
	{
		client::QueryResults qr;
		Query q = Query(default_namespace)
					  .Where(kFieldLocation, CondEq, "key1")
					  .Where(Query{default_namespace}.Not().Where(kFieldLocation, CondEq, "key1"), CondAny, VariantArray{});
		Error err = rx->Select(q, qr);
		ASSERT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Shard key condition cannot be negative");
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
		for (auto& expNs : kExpectedNss[shard]) {
			const auto found = std::find_if(nss.begin(), nss.end(), [&expNs](const NamespaceDef& def) { return def.name == expNs; });
			ASSERT_TRUE(found != nss.end()) << expNs << "; shard = " << shard;
		}
	}
	// Check if we can drop local namespace and can't drop other shard local namespace
	{
		size_t shard = 0;
		auto rx = svc_[shard][0].Get()->api.reindexer;
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
		for (auto& expNs : kExpectedNss[shard]) {
			const auto found = std::find_if(nss.begin(), nss.end(), [&expNs](const NamespaceDef& def) { return def.name == expNs; });
			ASSERT_TRUE(found != nss.end()) << expNs << "; shard = " << shard;
		}
	}
}

const std::string_view ShardingApi::kConfigTemplate = R"(version: 1
namespaces:
  - namespace: namespace1
    default_shard: 0
    index: count
    keys:
      - shard_id: 1
        values:
          ${0}
      - shard_id: 2
        values:
          ${1}
      - shard_id: 3
        values:
          ${2}
shards:
  - shard_id: 0
    dsns:
      - cproto://127.0.0.1:19000/shard0
  - shard_id: 1
    dsns:
      - cproto://127.0.0.1:19010/shard1
  - shard_id: 2
    dsns:
      - cproto://127.0.0.1:19020/shard2
  - shard_id: 3
    dsns:
      - cproto://127.0.0.1:19030/shard3
this_shard_id: 0
reconnect_timeout_msec: 3000
shards_awaiting_timeout_sec: 30
config_rollback_timeout_sec: 30
proxy_conn_count: 8
proxy_conn_concurrency: 8
proxy_conn_threads: 4
)";

TEST_F(ShardingApi, ConfigYaml) {
	using namespace std::string_literals;
	using Cfg = ShardingConfig;
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

	struct [[nodiscard]] TestCase {
		std::string yaml;
		std::variant<Cfg, Error> expected;
		std::optional<std::string> yamlForCompare = std::nullopt;
	};

	auto substRangesInTemplate = [](const std::vector<std::string>& values) {
		std::string res(kConfigTemplate);
		for (size_t i = 0; i < 3; ++i) {
			auto tmplt = fmt::format("${{{}}}", i);
			res.replace(res.find(tmplt), tmplt.size(), values[i]);
		}
		return res;
	};

	// clang-format off
	std::vector rangesTestCases{
		TestCase{substRangesInTemplate({
	   R"(- 1
          - 2
          - 3
          - 4
          - [3, 5]
          - [10, 15])",
	   R"(- 16
          - [36, 40]
          - [40, 65]
          - [100, 150])",
	   R"(- 93
          - 25
          - 33
          - [24, 35]
          - [88, 95])"}), Cfg{{{"namespace1",
							 "count",
							 {Cfg::Key{1, ByRange, {sharding::Segment(Variant(1)), sharding::Segment(Variant(2)), sharding::Segment{Variant(3), Variant(5)}, sharding::Segment{Variant(10), Variant(15)}}},
							  {2, ByRange, {sharding::Segment(Variant(16)), sharding::Segment{Variant(36), Variant(65)}, sharding::Segment{Variant(100), Variant(150)}}},
							  {3, ByRange, {sharding::Segment{Variant(24), Variant(35)}, sharding::Segment{Variant(88), Variant(95)}}}},
							 0}},

						   {{0, {DSN("cproto://127.0.0.1:19000/shard0")}},
							{1, {DSN("cproto://127.0.0.1:19010/shard1")}},
							{2, {DSN("cproto://127.0.0.1:19020/shard2")}},
							{3, {DSN("cproto://127.0.0.1:19030/shard3")}}},
						   0},
		substRangesInTemplate({
	   R"(- 1
          - 2
          - [3, 5]
          - [10, 15])",
	   R"(- 16
          - [36, 65]
          - [100, 150])",
	   R"(- [24, 35]
          - [88, 95])"})},
		TestCase{substRangesInTemplate({
	   R"(- 1
          - 2
          - 3
          - [0, 11]
          - [10, 15])",
	   R"(- 18
          - [40, 65]
          - [45, 57]
          - [100, 150])",
	   R"(- 17
          - 22
          - 33
          - [24, 35]
          - [39, 27]
          - [88, 95])"}),Cfg{{{"namespace1",
							 "count",
							 {Cfg::Key{1, ByRange, {sharding::Segment{Variant(0), Variant(15)}}},
							  {2, ByRange, {sharding::Segment(Variant(18)), sharding::Segment{Variant(40), Variant(65)}, sharding::Segment{Variant(100), Variant(150)}}},
							  {3, ByRange, {sharding::Segment(Variant(17)), sharding::Segment(Variant(22)), sharding::Segment{Variant(24), Variant(39)}, sharding::Segment{Variant(88), Variant(95)}}}},
							 0}},

						   {{0, {DSN("cproto://127.0.0.1:19000/shard0")}},
							{1, {DSN("cproto://127.0.0.1:19010/shard1")}},
							{2, {DSN("cproto://127.0.0.1:19020/shard2")}},
							{3, {DSN("cproto://127.0.0.1:19030/shard3")}}},
						   0},
		substRangesInTemplate({
	   R"(- [0, 15])",
	   R"(- 18
          - [40, 65]
          - [100, 150])",
	   R"(- 17
          - 22
          - [24, 39]
          - [88, 95])"})},
		TestCase{substRangesInTemplate({
	   R"(- 1
          - 2
          - 3
          - [10, 15])",
	   R"(- 29
          - [40, 65]
          - [4, 5]
          - [100, 150])",
	   R"(- 143
          - 12)"}), Error{errParams, "Incorrect value '143'. Value already in use."}},
		TestCase{substRangesInTemplate({
	   R"(- "abd"
          - 2
          - 3
          - [10, 15])",
	   R"(- 29
          - [40, 65]
          - [4, 5]
          - [100, 150])",
	   R"(- 143
          - 12)"}), Error{errParams, "Incorrect value '2'. Type of first value is 'string', current type is 'int64'"}},
		TestCase{substRangesInTemplate({
	   R"(- 2
          - ["string1", "string2"])",
	   R"(- 29
          - [40, 65]
          - [4, 5]
          - [100, 150])",
	   R"(- 143
          - 12)"}), Error{errParams, "Incorrect value 'string1'. Type of first value is 'int64', current type is 'string'"}},
		TestCase{substRangesInTemplate({
	   R"(- 1.0
          - 2
          - 3
          - [10, 15])",
	   R"(- 29
          - [40, 65]
          - [4, 5]
          - [100, 150])",
	   R"(- 143
          - 12)"}), Error{errParams, "Incorrect value '2'. Type of first value is 'double', current type is 'int64'"}},
		TestCase{substRangesInTemplate({
	   R"(- [1, 3]
          - [2.3, 3]
          - 3
          - [10, 15])",
	   R"(- 29
          - [40, 65]
          - [4, 5]
          - [100, 150])",
	   R"(- 143
          - 12)"}), Error{errParams, "Incorrect segment '[2.3, 3]'. Type of left value is 'double', right type is 'int64'"}},
		TestCase{substRangesInTemplate({
	   R"(- [1.4, "string"]
          - 3
          - [10, 15])",
	   R"(- 29
          - [40, 65]
          - [4, 5]
          - [100, 150])",
	   R"(- 143
          - 12)"}), Error{errParams, "Incorrect segment '[1.4, string]'. Type of left value is 'double', right type is 'string'"}}};
	// clang-format on

	std::vector<TestCase> testCases{
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
config_rollback_timeout_sec: 30
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
config_rollback_timeout_sec: 30
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
config_rollback_timeout_sec: 30
proxy_conn_count: 15
)"s,
		 Error{errParams, "Scheme of sharding dsn must be cproto or cprotos: 127.0.0.1:19001/shard1"}},
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
config_rollback_timeout_sec: 30
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
config_rollback_timeout_sec: 30
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
config_rollback_timeout_sec: 30
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
config_rollback_timeout_sec: 30
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
config_rollback_timeout_sec: 17
proxy_conn_count: 15
proxy_conn_concurrency: 10
proxy_conn_threads: 5
)"s,
		 Cfg{{{"best_namespace",
			   "location",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant("south")), sharding::Segment(Variant("west"))}},
				{2, ByValue, {sharding::Segment(Variant("north"))}}},
			   0}},
			 {{0, {DSN("cproto://127.0.0.1:19000/shard0")}},
			  {1, {DSN("cproto://127.0.0.1:19001/shard1")}},
			  {2, {DSN("cproto://127.0.0.2:19002/shard2")}}},
			 0,
			 std::chrono::milliseconds(5000),
			 std::chrono::seconds(25),
			 std::chrono::seconds(17),
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
config_rollback_timeout_sec: 30
proxy_conn_count: 8
proxy_conn_concurrency: 8
proxy_conn_threads: 4
)"s,
		 Cfg{{{"namespace1",
			   "count",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant(0)), sharding::Segment(Variant(10)), sharding::Segment(Variant(20))}}},
			   0},
			  {"namespace2",
			   "city",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant("Moscow"))}},
				{2, ByValue, {sharding::Segment(Variant("London"))}},
				{3, ByValue, {sharding::Segment(Variant("Paris"))}}},
			   1}},
			 {{0, {DSN("cproto://127.0.0.1:19000/shard0"), DSN("cproto://127.0.0.1:19001/shard0"), DSN("cproto://127.0.0.1:19002/shard0")}},
			  {1, {DSN("cproto://127.0.0.1:19010/shard1"), DSN("cproto://127.0.0.1:19011/shard1")}},
			  {2, {DSN("cproto://127.0.0.2:19020/shard2")}},
			  {3,
			   {DSN("cproto://127.0.0.2:19030/shard3"), DSN("cproto://127.0.0.2:19031/shard3"), DSN("cproto://127.0.0.2:19032/shard3"),
				DSN("cproto://127.0.0.2:19033/shard3")}}},
			 0}},
		{sample.str(),
		 Cfg{{{"namespace1",
			   "count",
			   {Cfg::Key{1,
						 ByRange,
						 {sharding::Segment(Variant(0)), sharding::Segment{Variant(7), Variant(10)},
						  sharding::Segment{Variant(13), Variant(21)}}},
				{2,
				 ByValue,
				 {sharding::Segment(Variant(1)), sharding::Segment(Variant(2)), sharding::Segment(Variant(3)),
				  sharding::Segment(Variant(4))}},
				{3, ByValue, {sharding::Segment(Variant(11))}}},
			   0},
			  {"namespace2",
			   "city",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant("Moscow"))}},
				{2, ByValue, {sharding::Segment(Variant("London"))}},
				{3, ByValue, {sharding::Segment(Variant("Paris"))}}},
			   3}},
			 {{0, {DSN("cproto://127.0.0.1:19000/shard0"), DSN("cproto://127.0.0.1:19001/shard0"), DSN("cproto://127.0.0.1:19002/shard0")}},
			  {1, {DSN("cproto://127.0.0.1:19010/shard1"), DSN("cproto://127.0.0.1:19011/shard1")}},
			  {2, {DSN("cproto://127.0.0.2:19020/shard2")}},
			  {3,
			   {DSN("cproto://127.0.0.2:19030/shard3"), DSN("cproto://127.0.0.2:19031/shard3"), DSN("cproto://127.0.0.2:19032/shard3"),
				DSN("cproto://127.0.0.2:19033/shard3")}}},
			 0}}};

	testCases.insert(testCases.end(), rangesTestCases.begin(), rangesTestCases.end());

	for (const auto& [yaml, expected, yaml4Cmp] : testCases) {
		const Error err = config.FromYAML(yaml);
		if (std::holds_alternative<Cfg>(expected)) {
			const auto& cfg{std::get<Cfg>(expected)};
			EXPECT_TRUE(err.ok()) << err.what() << "\nYAML:\n" << yaml;
			if (err.ok()) {
				EXPECT_EQ(config, cfg) << yaml << "\nexpected:\n" << cfg.GetYAML();
			}
			const auto generatedYml = cfg.GetYAML();
			EXPECT_EQ(generatedYml, yaml4Cmp ? yaml4Cmp.value() : yaml);
		} else {
			EXPECT_FALSE(err.ok());
			EXPECT_EQ(err.whatStr(), std::get<Error>(expected).whatStr());
		}
	}
}

TEST_F(ShardingApi, ConfigJson) {
	using namespace std::string_literals;
	using Cfg = ShardingConfig;
	Cfg config;

	struct {
		std::string json;
		Cfg expected;
		std::optional<std::string> json4compare = std::nullopt;
	} testCases[]{
		{R"({"version":1,"namespaces":[{"namespace":"best_namespace","default_shard":0,"index":"location","keys":[{"shard_id":1,"values":["south","west"]},{"shard_id":2,"values":["north"]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19001/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.2:19002/shard2"]}],"this_shard_id":0,"reconnect_timeout_msec":5000,"shards_awaiting_timeout_sec":20,"config_rollback_timeout_sec":33,"proxy_conn_count":4,"proxy_conn_concurrency":8,"proxy_conn_threads":4})"s,
		 Cfg{{{"best_namespace",
			   "location",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant("south")), sharding::Segment(Variant("west"))}},
				{2, ByValue, {sharding::Segment(Variant("north"))}}},
			   0}},
			 {{0, {DSN("cproto://127.0.0.1:19000/shard0")}},
			  {1, {DSN("cproto://127.0.0.1:19001/shard1")}},
			  {2, {DSN("cproto://127.0.0.2:19002/shard2")}}},
			 0,
			 std::chrono::milliseconds(5000),
			 std::chrono::seconds(20),
			 std::chrono::seconds(33),
			 4}},
		{R"({"version":1,"namespaces":[{"namespace":"namespace1","default_shard":0,"index":"count","keys":[{"shard_id":1,"values":[0,10,20]},{"shard_id":2,"values":[1,5,7,9]},{"shard_id":3,"values":[100]}]},{"namespace":"namespace2","default_shard":3,"index":"city","keys":[{"shard_id":1,"values":["Moscow"]},{"shard_id":2,"values":["London"]},{"shard_id":3,"values":["Paris"]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0","cproto://127.0.0.1:19001/shard0","cproto://127.0.0.1:19002/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19010/shard1","cproto://127.0.0.1:19011/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.2:19020/shard2"]},{"shard_id":3,"dsns":["cproto://127.0.0.2:19030/shard3","cproto://127.0.0.2:19031/shard3","cproto://127.0.0.2:19032/shard3","cproto://127.0.0.2:19033/shard3"]}],"this_shard_id":0,"reconnect_timeout_msec":4000,"shards_awaiting_timeout_sec":30,"config_rollback_timeout_sec":30,"proxy_conn_count":3,"proxy_conn_concurrency":5,"proxy_conn_threads":2})"s,
		 Cfg{{{"namespace1",
			   "count",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant(0)), sharding::Segment(Variant(10)), sharding::Segment(Variant(20))}},
				{2,
				 ByValue,
				 {sharding::Segment(Variant(1)), sharding::Segment(Variant(5)), sharding::Segment(Variant(7)),
				  sharding::Segment(Variant(9))}},
				{3, ByValue, {sharding::Segment(Variant(100))}}},
			   0},
			  {"namespace2",
			   "city",
			   {Cfg::Key{1, ByValue, {sharding::Segment(Variant("Moscow"))}},
				{2, ByValue, {sharding::Segment(Variant("London"))}},
				{3, ByValue, {sharding::Segment(Variant("Paris"))}}},
			   3}},
			 {{0, {DSN("cproto://127.0.0.1:19000/shard0"), DSN("cproto://127.0.0.1:19001/shard0"), DSN("cproto://127.0.0.1:19002/shard0")}},
			  {1, {DSN("cproto://127.0.0.1:19010/shard1"), DSN("cproto://127.0.0.1:19011/shard1")}},
			  {2, {DSN("cproto://127.0.0.2:19020/shard2")}},
			  {3,
			   {DSN("cproto://127.0.0.2:19030/shard3"), DSN("cproto://127.0.0.2:19031/shard3"), DSN("cproto://127.0.0.2:19032/shard3"),
				DSN("cproto://127.0.0.2:19033/shard3")}}},
			 0,
			 std::chrono::milliseconds(4000),
			 std::chrono::seconds(30),
			 std::chrono::seconds(30),
			 3,
			 5,
			 2}},
		{R"({"version":1,"namespaces":[{"namespace":"namespace1","default_shard":0,"index":"count","keys":[{"shard_id":1,"values":[1,2,3,4,{"range":[3,5]},{"range":[10,15]}]},{"shard_id":2,"values":[16,{"range":[36,40]},{"range":[40,65]},{"range":[100,150]}]},{"shard_id":3,"values":[93,25,33,{"range":[24,35]},{"range":[88,95]}]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19010/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.1:19020/shard2"]},{"shard_id":3,"dsns":["cproto://127.0.0.1:19030/shard3"]}],"this_shard_id":0,"reconnect_timeout_msec":4000,"shards_awaiting_timeout_sec":30,"config_rollback_timeout_sec":30,"proxy_conn_count":3,"proxy_conn_concurrency":5,"proxy_conn_threads":2})",
		 Cfg{{{"namespace1",
			   "count",
			   {Cfg::Key{1,
						 ByRange,
						 {sharding::Segment(Variant(1)), sharding::Segment(Variant(2)), sharding::Segment{Variant(3), Variant(5)},
						  sharding::Segment{Variant(10), Variant(15)}}},
				{2,
				 ByRange,
				 {sharding::Segment(Variant(16)), sharding::Segment{Variant(36), Variant(65)},
				  sharding::Segment{Variant(100), Variant(150)}}},
				{3, ByRange, {sharding::Segment{Variant(24), Variant(35)}, sharding::Segment{Variant(88), Variant(95)}}}},
			   0}},

			 {{0, {DSN("cproto://127.0.0.1:19000/shard0")}},
			  {1, {DSN("cproto://127.0.0.1:19010/shard1")}},
			  {2, {DSN("cproto://127.0.0.1:19020/shard2")}},
			  {3, {DSN("cproto://127.0.0.1:19030/shard3")}}},
			 0,
			 std::chrono::milliseconds(4000),
			 std::chrono::seconds(30),
			 std::chrono::seconds(30),
			 3,
			 5,
			 2},
		 R"({"version":1,"namespaces":[{"namespace":"namespace1","default_shard":0,"index":"count","keys":[{"shard_id":1,"values":[1,2,{"range":[3,5]},{"range":[10,15]}]},{"shard_id":2,"values":[16,{"range":[36,65]},{"range":[100,150]}]},{"shard_id":3,"values":[{"range":[24,35]},{"range":[88,95]}]}]}],"shards":[{"shard_id":0,"dsns":["cproto://127.0.0.1:19000/shard0"]},{"shard_id":1,"dsns":["cproto://127.0.0.1:19010/shard1"]},{"shard_id":2,"dsns":["cproto://127.0.0.1:19020/shard2"]},{"shard_id":3,"dsns":["cproto://127.0.0.1:19030/shard3"]}],"this_shard_id":0,"reconnect_timeout_msec":4000,"shards_awaiting_timeout_sec":30,"config_rollback_timeout_sec":30,"proxy_conn_count":3,"proxy_conn_concurrency":5,"proxy_conn_threads":2})"}};

	for (const auto& [json, cfg, json4cmp] : testCases) {
		const auto generatedJson = cfg.GetJSON(cluster::MaskingDSN::Disabled);
		EXPECT_EQ(generatedJson, json4cmp ? json4cmp.value() : json);

		const Error err = config.FromJSON(std::string(json));
		EXPECT_TRUE(err.ok()) << err.what();
		if (err.ok()) {
			EXPECT_EQ(config, cfg) << json << "\nexpected:\n" << cfg.GetJSON(cluster::MaskingDSN::Disabled);
		}
	}
}

TEST_F(ShardingApi, ConfigKeyValues) {
	struct [[nodiscard]] shardInfo {
		bool result;
		struct [[nodiscard]] Key {
			Key(const char* c_str) : left(std::string(c_str)) {}
			Key(std::string&& left, std::string&& right) : left(std::move(left)), right(std::move(right)) {}
			std::string left, right;
			operator std::string() const { return right.empty() ? left : fmt::format("{{\"range\": [{}, {}]}}", left, right); }
		};
		using ShardKeys = std::vector<Key>;
		std::vector<ShardKeys> shards;
	};

	// NOLINTBEGIN (performance-inefficient-string-concatenation)
	auto generateConfigYaml = [](const shardInfo& info) -> std::string {
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
				kY["values"] = YAML::Node(YAML::NodeType::Sequence);
				for (const auto& [left, right] : info.shards[i]) {
					if (right.empty()) {
						kY["values"].push_back(left);
					} else {
						auto segmentNode = YAML::Node(YAML::NodeType::Sequence);
						segmentNode.SetStyle(YAML::EmitterStyle::Flow);
						segmentNode.push_back(left);
						segmentNode.push_back(right);
						kY["values"].push_back(std::move(segmentNode));
					};
				}
				nsY["keys"].push_back(std::move(kY));
			}
			y["namespaces"].push_back(std::move(nsY));
		}

		for (size_t i = 0; i <= info.shards.size(); i++) {
			YAML::Node sY;
			sY["shard_id"] = i;
			sY["dsns"].push_back(fmt::format("cproto://127.0.0.1:1900{}/shard{}", i, i));
			y["shards"].push_back(std::move(sY));
		}
		return YAML::Dump(y);
	};

	auto generateConfigJson = [](const shardInfo& info) -> std::string {
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
				conf += "                    " + std::string(info.shards[i][j]) + (j == info.shards[i].size() - 1 ? "\n" : ",\n");
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

									{true, {{"\"key1\"", "\"key2\"","\"key1\""}, {"\"key3\""}}},

									{true, {{"1", "2", "3", "4", {"3", "5"},  {"10", "15"}}, {"16",  {"36", "40"},  {"40", "65"},  {"100", "150"}},  {"93", "25", "33", {"24", "35"},  {"88", "95"}}}},
									{true, {{"1", "2",  {"0", "11"},  {"10", "15"}}, {"18", {"40", "65"},  {"45", "57"},  {"100", "150"}},  {"17","22", "33", {"24", "35"},  {"39", "27"}, {"88", "95"}}}},
									{false, {{"1", "2",  {"10", "15"}}, {"29",  {"40", "65"},  {"4", "5"},  {"100", "150"}},   {"143", "12"}}}

								};
	// clang-format on

	for (const auto& test : tests) {
		std::string conf = generateConfigJson(test);
		ShardingConfig config;
		Error err = config.FromJSON(std::span(conf));
		EXPECT_TRUE(err.ok() == test.result) << err.what() << "\nconfig:\n" << conf;
	}

	tests.push_back({true, {{"key1", "key2"}, {"key3"}}});
	tests.push_back({true, {{"key1", "\"key2\""}, {"\"key3\""}}});
	tests.push_back({false, {{"1", "2"}, {"3", "string"}}});
	tests.push_back({false, {{"key1", "key2"}, {"key1", "key3"}}});

	for (const auto& test : tests) {
		std::string conf = generateConfigYaml(test);
		ShardingConfig config;
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
		auto err = rx->Select(Query::FromSQL(fmt::format("select * from {} where {}<'key3'", default_namespace, kFieldLocation)), qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::QueryResults qr;
		// key3 - proxy node
		auto err = rx->Select(Query::FromSQL(fmt::format("select * from {} where {}='key3' and {}='key2'", default_namespace,
														 kFieldLocation, kFieldLocation)),
							  qr);
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		client::QueryResults qr;
		auto err = rx->Select(Query::FromSQL(fmt::format("select * from {} where {}='key1' and {}='key2'", default_namespace,
														 kFieldLocation, kFieldLocation)),
							  qr);
		ASSERT_EQ(err.code(), errLogic);
	}

	{
		client::QueryResults qr;
		auto err = rx->Select(
			Query::FromSQL(fmt::format("select * from {} where {}='key1' or {}='key2'", default_namespace, kFieldLocation, kFieldLocation)),
			qr);
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
static void CheckTotalCount(bool cached, client::Reindexer& rx, const std::string& ns, size_t expected) {
	client::QueryResults qr;
	Error err;
	if (cached) {
		err = rx.Select(Query(ns).CachedTotal().Limit(0), qr);
	} else {
		err = rx.Select(Query(ns).ReqTotal().Limit(0), qr);
	}
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.TotalCount(), expected);
}

static void CheckCachedCountAggregations(client::Reindexer& rx, const std::string& ns, size_t dataPerShard, size_t shardsCount,
										 const std::string& fieldLocation) {
	{
		// Check distributed query without offset
		client::QueryResults qr;
		Error err = rx.Select(Query(ns).CachedTotal().Limit(0), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), shardsCount * dataPerShard);
		auto& agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].GetType(), AggCountCached);
		ASSERT_TRUE(agg[0].GetValue());
		ASSERT_EQ(agg[0].GetValueOrZero(), qr.TotalCount());
	}
	{
		// Check distributed query with offset
		client::QueryResults qr;
		Error err = rx.Select(Query(ns).CachedTotal().Limit(0).Offset(1), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), shardsCount * dataPerShard);
		auto& agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].GetType(), AggCount);	// Here agg type was change to 'AggCount' by internal proxy
		ASSERT_TRUE(agg[0].GetValue());
		ASSERT_EQ(agg[0].GetValueOrZero(), qr.TotalCount());
	}
	{
		// Check single shard query without offset
		client::QueryResults qr;
		Error err = rx.Select(Query(ns).Where(fieldLocation, CondEq, "key2").CachedTotal().Limit(0), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), dataPerShard);
		auto& agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].GetType(), AggCountCached);
		ASSERT_TRUE(agg[0].GetValue());
		ASSERT_EQ(agg[0].GetValueOrZero(), qr.TotalCount());
	}
	{
		// Check single shard query with offset
		client::QueryResults qr;
		Error err = rx.Select(Query(ns).Where(fieldLocation, CondEq, "key2").CachedTotal().Limit(0).Offset(1), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.TotalCount(), dataPerShard);
		auto& agg = qr.GetAggregationResults();
		ASSERT_EQ(agg.size(), 1);
		ASSERT_EQ(agg[0].GetType(), AggCountCached);
		ASSERT_TRUE(agg[0].GetValue());
		ASSERT_EQ(agg[0].GetValueOrZero(), qr.TotalCount());
	}
}

template <typename T>
T getField(std::string_view field, reindexer::client::QueryResults::Iterator& it) {
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

	auto& rx = *svc_[0][0].Get()->api.reindexer;

	const unsigned long kMaxCountOnShard = 40;
	Fill(kNsName, rx, "key3", 0, kMaxCountOnShard);
	Fill(kNsName, rx, "key1", kMaxCountOnShard, kMaxCountOnShard);
	Fill(kNsName, rx, "key2", kMaxCountOnShard * 2, kMaxCountOnShard);

	struct [[nodiscard]] LimitOffsetCase {
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

	auto execVariant = [&rx, this](const LimitOffsetCase& v, bool checkCount) {
		Query q = Query(default_namespace).Limit(v.limit).Offset(v.offset);
		if (checkCount) {
			q.ReqTotal();
		}
		client::QueryResults qr;
		Error err = rx.Select(q, qr);
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

	for (const auto& v : variants) {
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
class [[nodiscard]] CompareFields<T> {
public:
	CompareFields(std::string_view f, std::vector<T> v = {}) : field_{f}, forcedValues_{std::move(v)} {}
	int operator()(reindexer::client::QueryResults::Iterator& it) {
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
	std::tuple<T> GetValues(reindexer::client::QueryResults::Iterator& it) const { return std::make_tuple(getField<T>(field_, it)); }
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
class [[nodiscard]] CompareFields<T, Ts...> : private CompareFields<Ts...> {
	using Base = CompareFields<Ts...>;
	template <typename>
	using string_view = std::string_view;

public:
	CompareFields(std::string_view f, string_view<Ts>... args, std::vector<std::tuple<T, Ts...>> v = {})
		: Base{args...}, impl_{f}, forcedValues_{std::move(v)} {}
	int operator()(reindexer::client::QueryResults::Iterator& it) {
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
	std::tuple<T, Ts...> GetValues(reindexer::client::QueryResults::Iterator& it) const {
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
class [[nodiscard]] CompareExpr {
public:
	CompareExpr(std::vector<std::string>&& fields, std::function<double(Ts...)>&& f) : fields_{std::move(fields)}, func_{std::move(f)} {}
	int operator()(reindexer::client::QueryResults::Iterator& it) { return impl(it, std::index_sequence_for<Ts...>{}); }

private:
	template <size_t... I>
	int impl(reindexer::client::QueryResults::Iterator& it, std::index_sequence<I...>) {
		const double r = func_(getField<Ts>(fields_[I], it)...);
		int result = 0;
		if (prevResult_ && !fp::EqualWithinULPs(*prevResult_, r)) {
			result = *prevResult_ < r ? 1 : -1;
		}
		prevResult_ = r;
		return result;
	}
	std::optional<double> prevResult_;
	std::vector<std::string> fields_;
	std::function<double(Ts...)> func_;
};

class [[nodiscard]] ShardingApi::CompareShardId {
public:
	CompareShardId(const ShardingApi& api) noexcept : api_{api} {}
	int operator()(reindexer::client::QueryResults::Iterator& it) {
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
	const ShardingApi& api_;
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

	struct [[nodiscard]] SortCase {
		std::string expression;
		std::variant<std::vector<int>, std::vector<std::string>, std::vector<std::tuple<int, std::string>>,
					 std::vector<std::tuple<std::string, int>>>
			forcedValues;
		std::function<int(reindexer::client::QueryResults::Iterator&)> test;
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
		{kIndexDataInt + " / 16 + " + kFieldNestedRand,
		 {},
		 CompareExpr<int, int>{{kFieldDataInt, kFieldNestedRand}, [](int data, int rand) { return data / 16.0 + rand; }}},
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
	struct [[nodiscard]] TestCase {
		TestCase(const SortCase& sc, bool d = ((rand() % 2) == 0)) : sort{sc}, desc{d} {}
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
			if (i != 0 && !std::visit([](const auto& c) { return c.empty(); }, sortCases[i].forcedValues)) {
				continue;
			}
			if (compositeIndexes.find(sortCases[i].expression) == compositeIndexes.end()) {
				if (usedFields.find(sortCases[i].expression) != usedFields.end()) {
					continue;
				}
				usedFields.insert(sortCases[i].expression);
				if (const auto it = synonymIndexes.find(sortCases[i].expression); it != synonymIndexes.end()) {
					if (usedFields.find(it->second) != usedFields.end()) {
						continue;
					}
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
				if (found) {
					continue;
				}
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
		for (const auto& tc : testCases) {
			std::visit(
				[&](const auto& c) {
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
							if (id % kShards == s) {
								--id;
							}
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
							if (id % kShards == s) {
								++id;
							}
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
					SCOPED_TRACE(fmt::format("desc: {}; result: {}", i->desc, result));
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

TEST_F(ShardingApi, OrderBySortHash) {
	InitShardingConfig cfg;
	cfg.rowsInTableOnShard = 1000;
	cfg.nodesInCluster = 1;
	cfg.shards = 3;
	cfg.createAdditionalIndexes = false;
	Init(std::move(cfg));

	auto getIds = [&](const std::string& sortExpr, std::vector<int>& ids) {
		Query q{default_namespace};
		q.Sort(sortExpr, true);
		client::QueryResults qr;
		std::shared_ptr<client::Reindexer> rx = svc_[0][0].Get()->api.reindexer;
		Error err = rx->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		for (auto& it : qr) {
			auto item = it.GetItem();
			std::string_view json = item.GetJSON();
			gason::JsonParser parser;
			auto Node = parser.Parse(json);
			int id = Node["id"].As<int>();
			ids.emplace_back(id);
		}
	};
	{
		const std::string expr("hash()");
		std::vector<int> ids1;
		getIds(expr, ids1);
		std::vector<int> ids2;
		getIds(expr, ids2);
		ASSERT_NE(ids1, ids2);
	}
	{
		int seed = std::rand() % 10000;
		const std::string expr("hash(" + std::to_string(seed) + ")");
		std::vector<int> ids1;
		getIds(expr, ids1);
		std::vector<int> ids2;
		getIds(expr, ids2);
		ASSERT_EQ(ids1, ids2);
	}
}

TEST_F(ShardingApi, TestCsvQrDistributedQuery) {
	const std::map<int, std::set<int>> shardDataDistrib{{0, {0, 1}}, {1, {2, 3}}, {2, {4, 5}}, {3, {6, 7}}};

	const std::string kNsName = "csv_test";

	InitShardingConfig cfg;
	cfg.additionalNss.emplace_back(kNsName);
	cfg.additionalNss[0].indexName = kFieldId;
	cfg.additionalNss[0].keyValuesNodeCreation = [&shardDataDistrib](int shard) {
		ShardingConfig::Key key;
		for (int v : shardDataDistrib.at(shard)) {
			key.values.emplace_back(Variant(v));
		}

		key.shardId = shard;
		return key;
	};
	cfg.shards = 4;

	Init(std::move(cfg));

	auto rx = svc_[0][0].Get()->api.reindexer;
	NamespaceDef nsDef{kNsName};
	nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
	Error err = rx->AddNamespace(nsDef);
	ASSERT_TRUE(err.ok()) << err.what();

	int count = 0;

	for (auto& [_, values] : shardDataDistrib) {
		(void)_;
		for (const auto& value : values) {
			WrSerializer wrser;
			auto item = rx->NewItem(kNsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
			jsonBuilder.Put(kFieldId, value);
			const auto& valStr = std::to_string(value);
			jsonBuilder.Put("Field" + valStr, "data" + valStr);
			jsonBuilder.End();

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx->Upsert(kNsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
			count++;
		}
	}

	waitSync(kNsName);

	const std::string jsonschema = R"!(
	{
		"required":
		[
			"id",
			"Field0",
			"Field1",
			"Field2",
			"Field3",
			"Field4",
			"Field5",
			"Field6",
			"Field7"
		],
		"properties":
		{
			"id": { "type": "int" },
			"Field0": { "type": "string" },
			"Field1": { "type": "string" },
			"Field2": { "type": "string" },
			"Field3": { "type": "string" },
			"Field4": { "type": "string" },
			"Field5": { "type": "string" },
			"Field6": { "type": "string" },
			"Field7": { "type": "string" }
		},
		"additionalProperties": false,
		"type": "object"
	})!";

	err = rx->SetSchema(kNsName, jsonschema);
	ASSERT_TRUE(err.ok()) << err.what();

	Query q = Query{kNsName};
	client::QueryResults qr;
	err = rx->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), count);

	auto csv2jsonSchema = std::vector<std::string>{"id", "Field0", "Field1", "Field2", "Field3", "Field4", "Field5", "Field6", "Field7"};

	std::vector<reindexer::TagName> orderingVec;
	const auto& tm = qr.GetTagsMatcher(0);
	for (const auto& tagName : csv2jsonSchema) {
		auto tag = tm.name2tag(tagName);
		EXPECT_FALSE(tag.IsEmpty());
		orderingVec.emplace_back(tag);
	}

	auto ordering = reindexer::CsvOrdering{orderingVec};

	reindexer::WrSerializer serCsv, serJson;
	for (auto& q : qr) {
		err = q.GetCSV(serCsv, ordering);
		ASSERT_TRUE(err.ok()) << err.what();

		err = q.GetJSON(serJson, false);
		ASSERT_TRUE(err.ok()) << err.what();

		gason::JsonParser parserCsv, parserJson;
		auto converted = parserCsv.Parse(std::string_view(reindexer::csv2json(serCsv.Slice(), csv2jsonSchema)));
		auto orig = parserJson.Parse(serJson.Slice());

		for (const auto& fieldName : csv2jsonSchema) {
			if (converted[fieldName].empty() || orig[fieldName].empty()) {
				EXPECT_TRUE(converted[fieldName].empty() && orig[fieldName].empty()) << "fieldName: " << fieldName;
				continue;
			}

			if (orig[fieldName].value.getTag() == gason::JsonTag::NUMBER) {
				EXPECT_EQ(orig[fieldName].As<int>(), converted[fieldName].As<int>());
			} else if (orig[fieldName].value.getTag() == gason::JsonTag::STRING) {
				EXPECT_EQ(orig[fieldName].As<std::string>(), converted[fieldName].As<std::string>());
			}
		}

		serCsv.Reset();
		serJson.Reset();
	}
}
