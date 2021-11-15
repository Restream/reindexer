#ifdef WITH_SHARDING

#include <future>
#include "core/itemimpl.h"
#include "sharding_api.h"

TEST_F(ShardingApi, Select) {
	Init();
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key);
			q.InnerJoin(kFieldId, kFieldId, CondEq, Query(default_namespace)).Where(kFieldLocation, CondEq, key);
			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; location = " << key << std::endl;
			ASSERT_TRUE(qr.Count() == 40) << qr.Count();
			for (auto it : qr) {
				auto item = it.GetItem();
				std::string_view json = item.GetJSON();
				ASSERT_TRUE(json.find("\"location\":\"" + key + "\"") != std::string_view::npos) << json;

				const auto &joinedData = it.GetJoined();
				for (size_t joinedField = 0; joinedField < joinedData.size(); ++joinedField) {
					QueryResults qrJoined;
					const auto &joinedItems = joinedData[joinedField];
					for (const auto &itemData : joinedItems) {
						ItemImpl itemimpl = ItemImpl(qr.GetPayloadType(1), qr.GetTagsMatcher(1));
						itemimpl.Unsafe(true);
						err = itemimpl.FromCJSON(itemData.data);
						ASSERT_TRUE(err.ok()) << err.what();

						std::string_view joinedJson = itemimpl.GetJSON();
						ASSERT_TRUE(joinedJson == json);
					}
				}
			}
		}
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
	for (size_t i = 0; i < svc_.size(); i += 2) {
		size_t shard = 1;
		const std::string updated = "updated_" + RandString();
		reindexer::client::Item item = svc_[i].Get()->api.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(item.Status().ok());

		WrSerializer wrser;
		buildItem(wrser, i, std::string("key" + std::to_string(shard)), updated, RandString());

		Error err = item.FromJSON(wrser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();

		err = svc_[i].Get()->api.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; shard = " << shard;

		if (i != (svc_.size() - 1)) {
			wrser.Reset();
			buildItem(wrser, i, std::string("key" + std::to_string(shard + 1)), updated, RandString());

			err = item.FromJSON(wrser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();

			err = svc_[i + 1].Get()->api.reindexer->Upsert(default_namespace, item);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; shard = " << shard;
		}
	}
}

TEST_F(ShardingApi, Update) {
	Init();
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
		for (size_t shard = 0; shard <= kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			const std::string updated = "updated_" + RandString();
			client::SyncCoroQueryResults qr;
			Query q = Query(default_namespace).Set(kFieldData, updated);
			if (shard != kShards) {
				q.Where(kFieldLocation, CondEq, key);
			}
			Error err = rx->Update(q, qr);
			ASSERT_TRUE(err.ok()) << err.what() << "; location = " << key << std::endl;
			std::string toFind;
			if (shard == kShards) {
				// Joint query from all shards
				ASSERT_TRUE(qr.Count() == 120) << qr.Count();
				toFind = ",\"data\":\"" + updated + "\"";
			} else {
				toFind = "\"location\":\"" + key + "\",\"data\":\"" + updated + "\"";
				ASSERT_TRUE(qr.Count() == 40) << qr.Count();
			}
			for (auto it : qr) {
				auto item = it.GetItem();
				std::string_view json = item.GetJSON();
				ASSERT_TRUE(json.find(toFind) != std::string_view::npos) << json << "; expected {" << toFind << "}";
			}
		}
	}
}

TEST_F(ShardingApi, Delete) {
	const int count = 120;
	int pos = count;
	Init();
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			const std::string key = "key" + std::to_string(shard + 1);
			client::SyncCoroQueryResults qr;
			std::string sql = "delete from " + default_namespace + " where " + kFieldLocation + " = \"" + key + "\"";
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
		Fill(i, pos, count);
		pos += count;
	}
}

TEST_F(ShardingApi, Meta) {
	Init();
	const std::string keyPrefix = "key";
	const std::string keyDataPrefix = "key_data";
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
		for (size_t shard = 0; shard < kShards; ++shard) {
			if ((i == 0) && (shard == 0)) {
				for (size_t i = 0; i < 10; ++i) {
					Error err = rx->PutMeta(default_namespace, keyPrefix + std::to_string(i), keyDataPrefix + std::to_string(i));
					ASSERT_TRUE(err.ok()) << err.what();
				}
				ClusterizationApi::Cluster::doWaitSync(default_namespace, svc_);
			}
			for (size_t i = 0; i < 10; ++i) {
				std::string actualValue;
				const std::string properValue = keyDataPrefix + std::to_string(i);
				Error err = rx->GetMeta(default_namespace, keyPrefix + std::to_string(i), actualValue);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(properValue == actualValue) << actualValue.size() << "; i = " << i << "; shard = " << shard;
			}
		}
	}
}

TEST_F(ShardingApi, Serial) {
	Init();
	Error err;
	const size_t kItemsCount = 10;
	const std::string kSerialFieldName = "linearValues";
	std::unordered_map<std::string, int> shardsUniqueItems;
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
		if (i == 0) {
			err = rx->AddIndex(default_namespace, IndexDef{kSerialFieldName, "hash", "int", IndexOpts()});
			ASSERT_TRUE(err.ok()) << err.what();
			ClusterizationApi::Cluster::doWaitSync(default_namespace, svc_);
		}
		for (size_t shard = 0; shard < kShards; ++shard) {
			size_t startId = 0;
			const std::string key = "key" + std::to_string(shard + 1);
			auto itKey = shardsUniqueItems.find(key);
			if (itKey == shardsUniqueItems.end()) {
				shardsUniqueItems[key] = 0;
			} else {
				startId = shardsUniqueItems[key];
			}
			for (size_t j = 0; j < kItemsCount; ++j) {
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

				err = rx->Upsert(default_namespace, item);
				ASSERT_TRUE(err.ok()) << err.what();

				Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(j));
				client::SyncCoroQueryResults qr;
				err = rx->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_TRUE(qr.Count() == 1) << qr.Count() << "; i = " << i << "; shard = " << shard << "; location = " << key << std::endl;
				size_t correctSerial = startId + j + 1;
				for (auto it : qr) {
					reindexer::client::Item itm = it.GetItem();
					std::string_view json = itm.GetJSON();
					std::string_view::size_type pos = json.find("linearValues\":" + std::to_string(correctSerial));
					ASSERT_TRUE(pos != std::string_view::npos) << correctSerial;
				}
			}
			shardsUniqueItems[key] += kItemsCount;
		}
	}
}

TEST_F(ShardingApi, UpdateItems) {
	Init();
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
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
	std::shared_ptr<client::SyncCoroReindexer> rx = svc_.front().Get()->api.reindexer;
	for (size_t shard = 0; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard + 1);
		std::string itemJson;
		{
			Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key).Where(kFieldId, CondEq, int(shard));
			client::SyncCoroQueryResults qr;
			Error err = rx->Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(qr.Count() == 1);
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
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
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
	std::shared_ptr<client::SyncCoroReindexer> rx = svc_.front().Get()->api.reindexer;
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

TEST_F(ShardingApi, Transactions) {
	Init();
	const int rowsInTr = 10;
	for (size_t i = 0; i < svc_.size(); ++i) {
		std::shared_ptr<client::SyncCoroReindexer> rx = svc_[i].Get()->api.reindexer;
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
		}
	}
}

TEST_F(ShardingApi, Reconnect) {
	const int nodesInCluster = 3, shards = 3;
	Init(shards, nodesInCluster);
	enum Mode : int { ModeRead = 0, ModeWrite = 1 };
	std::shared_ptr<client::SyncCoroReindexer> rx = svc_.front().Get()->api.reindexer;
	for (size_t shard = 1; shard < kShards; ++shard) {
		for (int mode = ModeRead; mode <= ModeWrite; ++mode) {
			StopByIndex(shard * nodesInCluster + mode);
			Error err;
			const std::string location = "key" + std::to_string(shard);
			client::SyncCoroQueryResults qr;
			if (mode == ModeRead) {
				Query q = Query(default_namespace).Where(kFieldLocation, CondEq, location);
				err = rx->Select(q, qr);
				ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << shard;
				ASSERT_TRUE(qr.Count() == 40) << qr.Count();
			} else if (mode == ModeWrite) {
				const std::string newValue = "most probably updated";
				qr = client::SyncCoroQueryResults();
				err = rx->Update(Query(default_namespace).Set(kFieldData, newValue).Where(kFieldLocation, CondEq, location), qr);
				ASSERT_TRUE(!err.ok()) << shard;
			}
		}
	}
	svc_.clear();
}

TEST_F(ShardingApi, MultithreadedReconnect) {
	const int nodesInCluster = 3, shards = 3;
	Init(shards, nodesInCluster);
	std::shared_ptr<client::SyncCoroReindexer> rx = svc_.front().Get()->api.reindexer;
	for (size_t shard = 1; shard < kShards; ++shard) {
		const std::string key = "key" + std::to_string(shard);
		// The last node won't be stopped because (in this case)
		// there will be no other nodes to reconnect to.
		for (size_t server = shard * nodesInCluster; server < shard * nodesInCluster + nodesInCluster - 1; ++server) {
			StopByIndex(server);
			std::vector<std::thread> threads;
			for (size_t i = 0; i < 10; ++i) {
				/*threads.emplace_back(std::thread([&, node = server, index = i]() {
					NamespaceDef nsDef(default_namespace + "_shard" + std::to_string(node) + "_" + std::to_string(index));
					nsDef.AddIndex(kFieldId, "hash", "int", IndexOpts().PK());
					nsDef.AddIndex(kFieldLocation, "hash", "string", IndexOpts());
					nsDef.AddIndex(kFieldData, "hash", "string", IndexOpts());
					Error err = rx->AddNamespace(nsDef, lsn_t());
					ASSERT_TRUE(err.ok()) << err.what();
				}));*/
				threads.emplace_back(std::thread([&, index = i]() {
					client::Item item = rx->NewItem(default_namespace);
					ASSERT_TRUE(item.Status().ok());

					const string key = string("key" + std::to_string(kShards));

					WrSerializer wrser;
					reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
					jsonBuilder.Put(kFieldId, int(index));
					jsonBuilder.Put(kFieldLocation, key);
					jsonBuilder.Put(kFieldData, RandString());
					jsonBuilder.End();

					Error err = item.FromJSON(wrser.Slice());
					ASSERT_TRUE(err.ok()) << err.what();

					err = rx->Upsert(default_namespace, item);
					ASSERT_TRUE(err.ok()) << err.what() << "; index = " << index << "; location = " << key;
				}));
				threads.emplace_back(std::thread([&, currShard = shard]() {
					client::SyncCoroQueryResults qr;
					Query q = Query(default_namespace).Where(kFieldLocation, CondEq, key);
					Error err = rx->Select(q, qr);
					ASSERT_TRUE(err.ok()) << err.what() << "; shard = " << currShard;
					ASSERT_TRUE(qr.Count() == 40) << qr.Count();
				}));
				/*threads.emplace_back(std::thread([&]() {
					client::SyncCoroQueryResults qr;
					Query q = Query(default_namespace).Set(kFieldData, std::string("NEEW!")).Where(kFieldLocation, CondEq, key);
					Error err = rx->Update(q, qr);
					if (i) {
						ASSERT_TRUE(err.ok()) << err.what();
						ASSERT_TRUE(qr.Count() == 40) << qr.Count();
					} else {
						ASSERT_TRUE(!err.ok());
					}
				}));*/
			}
			for (size_t i = 0; i < threads.size(); ++i) {
				threads[i].join();
			}
		}
	}
	svc_.clear();
}

#endif	// WITH_SHARDING
