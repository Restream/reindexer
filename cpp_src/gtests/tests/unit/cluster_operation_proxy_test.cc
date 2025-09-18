#include "cluster_operation_proxy.h"
#include "client/cororeindexer.h"
#include "client/queryresults.h"
#include "cluster/consts.h"
#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "estl/mutex.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "vendor/gason/gason.h"

using namespace reindexer;

static std::string itemData(int id, std::string_view valueData, std::string_view modifyValueData) {
	WrSerializer ser;
	JsonBuilder jb(ser);
	jb.Put("id", id);
	if (!valueData.empty()) {
		jb.Put("value", valueData);
	}
	if (!modifyValueData.empty()) {
		jb.Put("modifydata", modifyValueData);
	}
	jb.End();
	return std::string(ser.Slice());
}

TEST_F(ClusterOperationProxyApi, Transaction) {
	// transaction metod test
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		// waiting cluster synchonization, get leader and foollower id
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		const int followerId = (leaderId + 1) % kClusterSize;

		// create test ns
		const std::string kNsName = "ns1";
		Error err = cluster.GetNode(leaderId)->api.reindexer->OpenNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster.GetNode(leaderId)->api.reindexer->AddIndex(kNsName, {"id", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		cluster.WaitSync(kNsName);

		{
			// create transaction with Insert and Upsert rows on follower node
			client::Transaction tx = cluster.GetNode(followerId)->api.reindexer->NewTransaction(kNsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
			int iIn = 0;
			for (iIn = 0; iIn < 10; iIn++) {
				client::Item item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();
				err = item.FromJSON(itemData(iIn, "valuedata", ""));
				ASSERT_TRUE(err.ok()) << err.what();
				err = tx.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			for (iIn = 10; iIn < 20; iIn++) {
				client::Item item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();
				err = item.FromJSON(itemData(iIn, "valuedata", ""));
				ASSERT_TRUE(err.ok()) << err.what();
				err = tx.Upsert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}

			// commit transaction
			BaseApi::QueryResultsType qrTx;
			err = cluster.GetNode(followerId)->api.reindexer->CommitTransaction(tx, qrTx);
			ASSERT_TRUE(err.ok()) << err.what();

			// check data in ns on leader node
			auto leaderNode = cluster.GetNode(leaderId);
			auto qr = leaderNode->api.ExecSQL("select * from " + kNsName + " order by id");
			int i = 0;
			for (auto it = qr.begin(); it != qr.end(); ++it, ++i) {
				auto item = it.GetItem();
				auto data = item.GetJSON();
				ASSERT_EQ(data, itemData(i, "valuedata", ""));
			}
		}

		{
			// create transaction with Update and Delete rows on follower node
			client::Transaction tx = cluster.GetNode(followerId)->api.reindexer->NewTransaction(kNsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
			int iIn = 0;
			for (; iIn < 10; iIn++) {
				client::Item item = tx.NewItem();
				err = item.FromJSON(itemData(iIn, "valuedata", "modifydata" + std::to_string(iIn)));
				ASSERT_TRUE(err.ok()) << err.what();
				auto err = tx.Update(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			for (; iIn < 15; iIn++) {
				client::Item item = tx.NewItem();
				err = item.FromJSON(itemData(iIn, "valuedata", ""));
				ASSERT_TRUE(err.ok()) << err.what();
				auto err = tx.Delete(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			// remove items using SQL query
			err = tx.Modify(Query::FromSQL("delete from " + kNsName + " where id>=10"));
			ASSERT_TRUE(err.ok()) << err.what();

			// commit transaction
			BaseApi::QueryResultsType qrTx;
			err = cluster.GetNode(followerId)->api.reindexer->CommitTransaction(tx, qrTx);
			ASSERT_TRUE(err.ok()) << err.what();

			// check data in ns on leader node
			auto leaderNode = cluster.GetNode(leaderId);
			auto qr = leaderNode->api.ExecSQL("select * from " + kNsName + " order by id");
			int i = 0;
			for (auto it = qr.begin(); it != qr.end(); ++it, ++i) {
				auto item = it.GetItem();
				auto data = item.GetJSON();
				ASSERT_EQ(data, itemData(i, "valuedata", "modifydata" + std::to_string(i)));
			}
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, RollbackFollowerTransaction) {
	// transaction metod test
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		// waiting cluster synchonization, get leader and foollower id
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		const int followerId = (leaderId + 1) % kClusterSize;

		// create test ns
		const std::string kNsName = "ns1";
		Error err = cluster.GetNode(leaderId)->api.reindexer->OpenNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster.GetNode(leaderId)->api.reindexer->AddIndex(kNsName, {"id", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		cluster.WaitSync(kNsName);

		{
			// create transaction with Insert and Upsert rows on follower node
			client::Transaction tx = cluster.GetNode(followerId)->api.reindexer->NewTransaction(kNsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
			int iIn = 0;
			for (iIn = 0; iIn < 10; iIn++) {
				client::Item item = tx.NewItem();
				err = item.FromJSON(itemData(iIn, "valuedata", ""));
				ASSERT_TRUE(err.ok()) << err.what();
				err = tx.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}

			// rollback transaction
			err = cluster.GetNode(followerId)->api.reindexer->RollBackTransaction(tx);
			ASSERT_TRUE(err.ok()) << err.what();

			// trying to commit tx
			BaseApi::QueryResultsType qrTx;
			err = cluster.GetNode(followerId)->api.reindexer->CommitTransaction(tx, qrTx);
			ASSERT_FALSE(err.ok());

			// check data in ns on leader node
			auto leaderNode = cluster.GetNode(leaderId);
			auto qr = leaderNode->api.ExecSQL("select * from " + kNsName + " order by id");
			ASSERT_EQ(qr.Count(), 0);
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ParallelTransaction) {
	// checking parallel transactions work correct
	const size_t kClusterSize = 4;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		// waiting cluster synchonization, get leader and create foollowers id array
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		int followerId[kClusterSize - 1];
		unsigned int nodeCount = kClusterSize - 1;
		for (unsigned int i = 1; i < nodeCount + 1; i++) {
			followerId[i - 1] = (leaderId + i) % kClusterSize;
		}

		// create test ns
		const std::string kNsName = "ns1";
		Error err = cluster.GetNode(leaderId)->api.reindexer->OpenNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster.GetNode(leaderId)->api.reindexer->AddIndex(kNsName, {"id", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		cluster.WaitSync(kNsName);
		// creating transactions on different nodes
		std::vector<client::Transaction> txs;
		for (unsigned int n = 0; n < nodeCount; n++) {
			client::Transaction tx = cluster.GetNode(followerId[n])->api.reindexer->NewTransaction(kNsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
			int iIn = 0;
			for (iIn = 0; iIn < 10; iIn++) {
				client::Item item = tx.NewItem();
				err = item.FromJSON(itemData(iIn, "valuedata" + std::to_string(n), ""));
				ASSERT_TRUE(err.ok()) << err.what();
				err = tx.Upsert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			txs.push_back(std::move(tx));
		}
		// commit transactions
		for (unsigned int n = 0; n < nodeCount; n++) {
			int sId = followerId[nodeCount - 1 - n];
			BaseApi::QueryResultsType qrTx;
			err = cluster.GetNode(sId)->api.reindexer->CommitTransaction(txs[nodeCount - 1 - n], qrTx);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		// check data in ns on leader node
		leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		auto leaderNode = cluster.GetNode(leaderId);
		auto qr = leaderNode->api.ExecSQL("select * from " + kNsName + " order by id");
		int i = 0;
		for (auto it = qr.begin(); it != qr.end(); ++it, ++i) {
			auto item = it.GetItem();
			auto data = item.GetJSON();
			std::string pattern(itemData(i, "valuedata" + std::to_string(0), ""));
			ASSERT_EQ(data, pattern);
		}
	}));

	loop.run();
}

#if !defined(REINDEX_WITH_TSAN) && !defined(REINDEX_WITH_ASAN)
TEST_F(ClusterOperationProxyApi, StressTest) {
	// Check correct insertion ordering with proxied concurrent transactions
	const size_t kClusterSize = 3;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		// waiting cluster synchonization, get leader and create foollowers id array
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		int followerId = (leaderId + 1) % kClusterSize;
		const std::string dsn =
			fmt::format("cproto://127.0.0.1:{}/{}", cluster.GetNode(followerId)->RpcPort(), cluster.GetNode(followerId)->DbName());

		struct [[nodiscard]] NsData {
			std::string name;
			unsigned dataCount;
		};

		std::atomic<int> nsCounter{0};
		reindexer::mutex mtx;
		std::vector<NsData> namespaces;

		auto txInsertions = exceptionWrapper([&] {
			net::ev::dynamic_loop thLoop;
			thLoop.spawn(exceptionWrapper([&] {
				client::CoroReindexer rx;
				auto err = rx.Connect(dsn, thLoop);
				ASSERT_TRUE(err.ok()) << err.what();
				std::string nsName = fmt::format("ns_tx_{}", nsCounter.fetch_add(1));
				{
					lock_guard lck(mtx);
					namespaces.emplace_back(NsData{nsName, 3000});
				}
				err = rx.DropNamespace(nsName);
				(void)err;
				err = rx.OpenNamespace(nsName, StorageOpts().Enabled());
				ASSERT_TRUE(err.ok()) << err.what();
				err = rx.AddIndex(nsName, IndexDef("id", "hash", "int", IndexOpts().PK()));
				ASSERT_TRUE(err.ok()) << err.what();
				unsigned id = 0;
				for (unsigned i = 0; i < 30; ++i) {
					auto tx = rx.NewTransaction(nsName);
					for (unsigned i = 0; i < 100; ++i) {
						auto item = tx.NewItem();
						err = item.FromJSON(fmt::format(R"j({{"id":{}, "value":{}}})j", id++, rand()));
						ASSERT_TRUE(err.ok()) << err.what();
						err = tx.Upsert(std::move(item));
						ASSERT_TRUE(err.ok()) << err.what();
					}
					client::CoroQueryResults qr;
					err = rx.CommitTransaction(tx, qr);
					ASSERT_TRUE(err.ok()) << err.what();
				}
			}));
			thLoop.run();
		});

		auto singleInsertions = exceptionWrapper([&] {
			net::ev::dynamic_loop thLoop;
			thLoop.spawn(exceptionWrapper([&] {
				client::CoroReindexer rx;
				auto err = rx.Connect(dsn, thLoop);
				ASSERT_TRUE(err.ok()) << err.what();
				std::string nsName = fmt::format("ns_{}", nsCounter.fetch_add(1));
				{
					lock_guard lck(mtx);
					namespaces.emplace_back(NsData{nsName, 4000});
				}
				err = rx.DropNamespace(nsName);
				(void)err;
				err = rx.OpenNamespace(nsName, StorageOpts().Enabled());
				ASSERT_TRUE(err.ok()) << err.what();
				err = rx.AddIndex(nsName, IndexDef("id", "hash", "int", IndexOpts().PK()));
				ASSERT_TRUE(err.ok()) << err.what();
				unsigned id = 0;
				for (unsigned i = 0; i < 4'000; ++i) {
					auto item = rx.NewItem(nsName);
					err = item.FromJSON(fmt::format(R"j({{"id":{}, "value":{}}})j", id++, rand()));
					ASSERT_TRUE(err.ok()) << err.what();
					err = rx.Upsert(nsName, item);
					ASSERT_TRUE(err.ok()) << err.what();
				}
			}));
			thLoop.run();
		});

		std::vector<std::thread> threads;
		threads.emplace_back(txInsertions);
		threads.emplace_back(singleInsertions);
		threads.emplace_back(txInsertions);
		threads.emplace_back(singleInsertions);
		threads.emplace_back(txInsertions);
		threads.emplace_back(singleInsertions);
		threads.emplace_back(singleInsertions);
		for (auto& th : threads) {
			th.join();
		}
		WrSerializer ser;
		for (auto& ns : namespaces) {
			SCOPED_TRACE(ns.name);
			auto rx = cluster.GetNode(followerId)->api.reindexer;
			client::QueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
			auto err = rx->Select(Query(ns.name), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), ns.dataCount);
			for (auto& it : qr) {
				ser.Reset();
				err = it.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				gason::JsonParser parser;
				auto root = parser.Parse(ser.Slice());
				EXPECT_EQ(root["id"].As<int>(), it.GetID());
			}
		}
	}));

	loop.run();
}
#endif	// !defined(REINDEX_WITH_TSAN) && !defined(REINDEX_WITH_ASAN)

TEST_F(ClusterOperationProxyApi, TransactionStopLeader) {
	const size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		// waiting cluster synchonization, get leader and foollower id
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		const int followerId = (leaderId + 1) % kClusterSize;

		// create test ns
		const std::string kNsName = "ns1";
		Error err = cluster.GetNode(leaderId)->api.reindexer->OpenNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster.GetNode(leaderId)->api.reindexer->AddIndex(kNsName, {"id", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		cluster.WaitSync(kNsName);

		{
			// create transaction
			client::Transaction tx = cluster.GetNode(followerId)->api.reindexer->NewTransaction(kNsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
			int iIn = 0;
			bool isLeaderChanged = false;
			bool isTxInvalid = false;
			// inserting items
			for (iIn = 0; iIn < 10; iIn++) {
				client::Item item = tx.NewItem();
				if (isTxInvalid) {
					if (!item.Status().ok()) {
						break;
					}
				} else {
					ASSERT_TRUE(item.Status().ok()) << iIn << ":" << err.what();
				}
				err = item.FromJSON(itemData(iIn, "valuedata", ""));
				ASSERT_TRUE(err.ok()) << err.what();
				err = tx.Insert(std::move(item));
				if (!isLeaderChanged) {
					ASSERT_TRUE(err.ok()) << iIn << ":" << err.what();
				} else {
					isTxInvalid = true;
				}
				if (iIn == 5) {
					// stop leader
					cluster.StopServer(leaderId);
					isLeaderChanged = true;
				}
			}
			BaseApi::QueryResultsType qrTx;
			err = cluster.GetNode(followerId)->api.reindexer->CommitTransaction(tx, qrTx);
			ASSERT_FALSE(err.ok());
		}
	}));

	loop.run();
}

static void CreateTestNs(const std::string& nsName, int node, ClusterOperationApi::Cluster& cluster) {
	Error err = cluster.GetNode(node)->api.reindexer->OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = cluster.GetNode(node)->api.reindexer->AddIndex(nsName, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
}

static void CheckDropNamespace(ClusterOperationApi::Cluster& cluster, int node, int leader) {
	// check DropNamespace
	const std::string kNsName = "nsDrop";
	CreateTestNs(kNsName, node, cluster);

	auto item = cluster.GetNode(node)->api.reindexer->NewItem(kNsName);
	int id = 100;
	Error err = item.FromJSON(itemData(id, "string" + std::to_string(id), ""));
	ASSERT_TRUE(err.ok()) << err.what();
	err = cluster.GetNode(node)->api.reindexer->Insert(kNsName, item);
	ASSERT_TRUE(err.ok()) << err.what();
	err = cluster.GetNode(node)->api.reindexer->DropNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = cluster.GetNode(node)->api.reindexer->AddIndex(kNsName, {"value", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.code() == errNotFound);
	err = cluster.GetNode(leader)->api.reindexer->AddIndex(kNsName, {"value", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.code() == errNotFound);
}

static void CheckAddUpdateDropIndex(ClusterOperationApi::Cluster& cluster, int node, int leaderId) {
	// check index operations (add, update, drop)

	// create test ns
	const std::string kNsName = "nsIndex";
	CreateTestNs(kNsName, node, cluster);

	// add index
	std::string indxName = "index";
	Error err = cluster.GetNode(node)->api.reindexer->AddIndex(kNsName, {indxName, "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
	// update index
	IndexDef newDef{indxName, "tree", "int", IndexOpts()};
	err = cluster.GetNode(node)->api.reindexer->UpdateIndex(kNsName, newDef);

	auto getIndexDefByName = [&cluster](const std::string& nsName, int node, std::string& indxName) -> std::optional<IndexDef> {
		std::vector<NamespaceDef> defs;
		Error err = cluster.GetNode(node)->api.reindexer->EnumNamespaces(defs, EnumNamespacesOpts().HideSystem().HideTemporary());
		EXPECT_TRUE(err.ok()) << err.what();
		for (const auto& def : defs) {
			if (def.name != nsName) {
				continue;
			}
			for (const auto& indxDef : def.indexes) {
				if (indxDef.Name() == indxName) {
					return indxDef;
				}
			}
		}
		return {};
	};

	{
		// check index is correct on leader and follower node
		const auto def = getIndexDefByName(kNsName, node, indxName);
		ASSERT_TRUE(def.has_value()) << "index not found";
		ASSERT_EQ(def->IndexTypeStr(), "tree");	 // NOLINT(bugprone-unchecked-optional-access)
		const auto defL = getIndexDefByName(kNsName, leaderId, indxName);
		ASSERT_TRUE(defL.has_value()) << "index not found";
		ASSERT_EQ(defL->IndexTypeStr(), "tree");  // NOLINT(bugprone-unchecked-optional-access)
	}
	{
		// drop index and check on leader and follower
		err = cluster.GetNode(node)->api.reindexer->DropIndex(kNsName, newDef);
		ASSERT_TRUE(err.ok()) << err.what();
		const auto def = getIndexDefByName(kNsName, node, indxName);
		ASSERT_FALSE(def.has_value()) << "index found";
		const auto defL = getIndexDefByName(kNsName, leaderId, indxName);
		ASSERT_FALSE(defL.has_value()) << "index found";
	}
}

static void CheckSetGetShema(ClusterOperationApi::Cluster& cluster, int nodeSet, int nodeRead) {
	// checking shema operations
	// create test ns
	const std::string kNsName = "nsShema";
	CreateTestNs(kNsName, nodeSet, cluster);

	// set shema
	// clang-format off
	const std::string jsonschema = R"xxx(
	{
	  "required": [
		"Countries",
	  ],
	  "properties": {
		"Countries": {
		  "items": {
			"type": "string"
		  },
		  "type": "array"
		},
	  },
	  "additionalProperties": true,
	  "type": "object"
	})xxx";
	// clang-format on

	// set shema
	Error err = cluster.GetNode(nodeSet)->api.reindexer->SetSchema(kNsName, jsonschema);
	ASSERT_TRUE(err.ok()) << err.what();
	// checking shema is set
	std::string schemaGet;
	err = cluster.GetNode(nodeRead)->api.reindexer->GetSchema(kNsName, JsonSchemaType, schemaGet);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(jsonschema.compare(0, jsonschema.size() - 1, schemaGet, 0, jsonschema.size() - 1) == 0);
}

static void CheckSetGetEnumDeleteMeta(ClusterOperationApi::Cluster& cluster, int nodeSet, int nodeRead) {
	// checking meta operation
	// create test ns
	const std::string kNsName = "nsMeta";
	CreateTestNs(kNsName, nodeSet, cluster);

	// set meta
	const std::string metaKey = "testMeta";
	const std::string metaData = "testMetaData";
	Error err = cluster.GetNode(nodeSet)->api.reindexer->PutMeta(kNsName, metaKey, metaData);
	ASSERT_TRUE(err.ok()) << err.what();
	// read meta
	std::string readMetaData;
	err = cluster.GetNode(nodeRead)->api.reindexer->GetMeta(kNsName, metaKey, readMetaData);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(readMetaData, "testMetaData");

	// get all meta keys
	std::vector<std::string> keys;
	err = cluster.GetNode(nodeRead)->api.reindexer->EnumMeta(kNsName, keys);
	bool found = false;
	ASSERT_TRUE(err.ok()) << err.what();
	for (const auto& k : keys) {
		if (k == metaKey) {
			found = true;
			break;
		}
	}
	ASSERT_TRUE(found) << "EnumMeta: key not found.";

	// delete meta key
	err = cluster.GetNode(nodeRead)->api.reindexer->DeleteMeta(kNsName, metaKey);
	ASSERT_TRUE(err.ok()) << err.what();

	err = cluster.GetNode(nodeRead)->api.reindexer->EnumMeta(kNsName, keys);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(keys.empty()) << "DeleteMeta: key not deleted";
}

static void SelectHelper(int node, const std::string& nsName, ClusterOperationApi::Cluster& cluster, const std::string& itemJson,
						 int id = -1) {
	reindexer::Query q(nsName);
	BaseApi::QueryResultsType qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
	Error err = cluster.GetNode(node)->api.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1);
	auto itsel = qr.begin().GetItem();
	ASSERT_TRUE(itsel.GetJSON() == itemJson) << itsel.GetJSON();
	if (id >= 0) {
		ASSERT_EQ(id, itsel.GetID());
	}
}

static void Select0Helper(int node, const std::string& nsName, ClusterOperationApi::Cluster& cluster) {
	reindexer::Query q(nsName);
	BaseApi::QueryResultsType qr;
	Error err = cluster.GetNode(node)->api.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 0);
}

static void CheckInsertUpsertUpdateDelete(ClusterOperationApi::Cluster& cluster, int leaderId, int followerId) {
	const std::string kNsName = "nsInsert";
	// create test ns
	CreateTestNs(kNsName, followerId, cluster);

	int pk = 10;
	{
		// insert new item
		auto item = cluster.GetNode(followerId)->api.reindexer->NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		std::string itemJson = itemData(pk, "string" + std::to_string(pk), "");
		Error err = item.FromJSON(itemData(pk, "string" + std::to_string(pk), ""));
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster.GetNode(followerId)->api.reindexer->Insert(kNsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.GetLSN().isEmpty());
		// check the correctness of the insert
		SelectHelper(followerId, kNsName, cluster, itemJson);
		SelectHelper(leaderId, kNsName, cluster, itemJson);

		// upsert item (change)
		std::string itemJsonUp = itemData(pk, "string_up" + std::to_string(pk), "");
		err = item.FromJSON(itemJsonUp);
		err = cluster.GetNode(followerId)->api.reindexer->Upsert(kNsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.GetLSN().isEmpty());
		// check the correctness of the upsert
		SelectHelper(followerId, kNsName, cluster, itemJsonUp);
		SelectHelper(leaderId, kNsName, cluster, itemJsonUp);
	}

	{
		// update item
		BaseApi::QueryResultsType qres(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
		Error err =
			cluster.GetNode(followerId)->api.reindexer->ExecSQL("select * from " + kNsName + " where id=" + std::to_string(pk), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qres.Count(), 1);
		auto itsel = qres.begin().GetItem();
		ASSERT_FALSE(itsel.GetLSN().isEmpty());
		std::string itemJson = itemData(pk, "string_update" + std::to_string(pk), "");
		err = itsel.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();
		err = cluster.GetNode(followerId)->api.reindexer->Update(kNsName, itsel);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(itsel.GetLSN().isEmpty());
		// check the correctness of the update
		SelectHelper(followerId, kNsName, cluster, itemJson);
		SelectHelper(leaderId, kNsName, cluster, itemJson);
	}

	{
		// delete item
		BaseApi::QueryResultsType qres(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
		Error err =
			cluster.GetNode(followerId)->api.reindexer->ExecSQL("select * from " + kNsName + " where id=" + std::to_string(pk), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qres.Count(), 1);
		auto itsel = qres.begin().GetItem();
		ASSERT_FALSE(itsel.GetLSN().isEmpty());
		err = cluster.GetNode(followerId)->api.reindexer->Delete(kNsName, itsel);
		ASSERT_TRUE(err.ok()) << err.what();
		// check the correctness of the delete
		Select0Helper(followerId, kNsName, cluster);
		Select0Helper(leaderId, kNsName, cluster);
		ASSERT_TRUE(itsel.GetLSN().isEmpty());
	}
}

static void CheckInsertUpsertUpdateDeleteItemQR(ClusterOperationApi::Cluster& cluster, int leaderId, int followerId) {
	const std::string kNsName = "nsInsertQr";
	// create test ns
	CreateTestNs(kNsName, followerId, cluster);

	int pk = 10;
	{
		// insert new item
		auto item = cluster.GetNode(followerId)->api.reindexer->NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		std::string itemJson = itemData(pk, "string" + std::to_string(pk), "");
		Error err = item.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();
		client::QueryResults qrInsert;
		err = cluster.GetNode(followerId)->api.reindexer->Insert(kNsName, item, qrInsert);
		ASSERT_TRUE(err.ok()) << err.what();
		{
			ASSERT_TRUE(qrInsert.Count() == 1) << qrInsert.Count();
			auto itemQR = qrInsert.begin().GetItem();
			const auto id = itemQR.GetID();
			ASSERT_GE(id, 0);
			ASSERT_TRUE(itemQR.GetLSN().isEmpty());
			// check the correctness of the insert
			SelectHelper(followerId, kNsName, cluster, itemJson, id);
			SelectHelper(leaderId, kNsName, cluster, itemJson, id);
		}

		// upsert item (change)
		std::string itemJsonUp = itemData(pk, "string_up" + std::to_string(pk), "");
		err = item.FromJSON(itemJsonUp);
		client::QueryResults qrUpsert;
		err = cluster.GetNode(followerId)->api.reindexer->Upsert(kNsName, item, qrUpsert);
		ASSERT_TRUE(err.ok()) << err.what();
		{
			ASSERT_TRUE(qrUpsert.Count() == 1);
			auto itemQR = qrUpsert.begin().GetItem();
			const auto id = itemQR.GetID();
			ASSERT_GE(id, 0);
			ASSERT_TRUE(itemQR.GetLSN().isEmpty());
			// check the correctness of the upsert
			SelectHelper(followerId, kNsName, cluster, itemJsonUp, id);
			SelectHelper(leaderId, kNsName, cluster, itemJsonUp, id);
		}
	}

	{
		// update item
		BaseApi::QueryResultsType qres(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
		Error err = cluster.GetNode(followerId)->api.reindexer->Select(Query(kNsName).Where("id", CondEq, std::to_string(pk)), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qres.Count(), 1);
		auto itsel = qres.begin().GetItem();
		ASSERT_FALSE(itsel.GetLSN().isEmpty());
		std::string itemJson = itemData(pk, "string_update" + std::to_string(pk), "");
		err = itsel.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();
		client::QueryResults qrUpdate;
		err = cluster.GetNode(followerId)->api.reindexer->Update(kNsName, itsel, qrUpdate);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qrUpdate.Count() == 1);
		auto itemQR = qrUpdate.begin().GetItem();
		const auto id = itemQR.GetID();
		ASSERT_GE(id, 0);
		ASSERT_TRUE(itemQR.GetLSN().isEmpty());
		// check the correctness of the update
		SelectHelper(followerId, kNsName, cluster, itemJson, id);
		SelectHelper(leaderId, kNsName, cluster, itemJson, id);
	}

	{
		// delete item
		BaseApi::QueryResultsType qres(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
		Error err = cluster.GetNode(followerId)->api.reindexer->Select(Query(kNsName).Where("id", CondEq, std::to_string(pk)), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qres.Count(), 1);
		auto itsel = qres.begin().GetItem();
		ASSERT_FALSE(itsel.GetLSN().isEmpty());
		client::QueryResults qrDelete;
		err = cluster.GetNode(followerId)->api.reindexer->Delete(kNsName, itsel, qrDelete);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qrDelete.Count() == 1);
		auto itemQR = qrDelete.begin().GetItem();
		const auto id = itemQR.GetID();
		ASSERT_GE(id, 0);
		ASSERT_TRUE(itemQR.GetLSN().isEmpty());
		// check the correctness of the delete
		Select0Helper(followerId, kNsName, cluster);
		Select0Helper(leaderId, kNsName, cluster);
	}
}

static void CheckInsertUpsertUpdateItemQRSerial(ClusterOperationApi::Cluster& cluster, int leaderId, int followerId) {
	const std::string kNsName = "nsInsertQRSerial";
	// create test ns
	CreateTestNs(kNsName, followerId, cluster);

	Error err = cluster.GetNode(followerId)->api.reindexer->AddIndex(kNsName, {"int", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	int pk = 111;
	{
		// insert new item
		auto item = cluster.GetNode(followerId)->api.reindexer->NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		WrSerializer ser;
		JsonBuilder jb(ser);
		jb.Put("id", pk);
		jb.Put("int", 0);
		jb.End();
		std::string_view itemJson = ser.Slice();
		Error err = item.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();

		item.SetPrecepts({"int=SERIAL()"});

		client::QueryResults qrInsert;
		err = cluster.GetNode(followerId)->api.reindexer->Insert(kNsName, item, qrInsert);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string itemJsonCheck;
		{
			WrSerializer ser;
			JsonBuilder jb(ser);
			jb.Put("id", pk);
			jb.Put("int", 1);
			jb.End();
			itemJsonCheck = ser.Slice();
		}

		{
			ASSERT_EQ(qrInsert.Count(), 1);
			auto itemQR = qrInsert.begin().GetItem();
			ASSERT_EQ(itemQR.GetJSON(), itemJsonCheck);
			ASSERT_FALSE(itemQR.GetLSN().isEmpty());
		}

		// check the correctness of the insert
		SelectHelper(followerId, kNsName, cluster, std::string(itemJsonCheck));
		SelectHelper(leaderId, kNsName, cluster, std::string(itemJsonCheck));
	}
	{  // upsert item (change)
		auto item = cluster.GetNode(followerId)->api.reindexer->NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		WrSerializer ser;
		JsonBuilder jb(ser);
		jb.Put("id", pk);
		jb.Put("int", 0);
		jb.End();
		std::string_view itemJsonUp = ser.Slice();

		Error err = item.FromJSON(itemJsonUp);

		item.SetPrecepts({"int=SERIAL()"});

		client::QueryResults qrUpsert;
		err = cluster.GetNode(followerId)->api.reindexer->Upsert(kNsName, item, qrUpsert);
		ASSERT_TRUE(err.ok()) << err.what();

		// check the correctness of the upsert
		std::string itemJsonUpCheck;
		{
			WrSerializer ser;
			JsonBuilder jb(ser);
			jb.Put("id", pk);
			jb.Put("int", 2);
			jb.End();
			itemJsonUpCheck = ser.Slice();
		}
		{
			ASSERT_EQ(qrUpsert.Count(), 1);
			auto itemQR = qrUpsert.begin().GetItem();
			ASSERT_EQ(itemQR.GetJSON(), itemJsonUpCheck);
			ASSERT_FALSE(itemQR.GetLSN().isEmpty());
		}
		// check the correctness of the upsert

		SelectHelper(followerId, kNsName, cluster, std::string(itemJsonUpCheck));
		SelectHelper(leaderId, kNsName, cluster, std::string(itemJsonUpCheck));
	}

	{
		// update item
		BaseApi::QueryResultsType qres(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
		Error err = cluster.GetNode(followerId)->api.reindexer->Select(Query(kNsName).Where("id", CondEq, std::to_string(pk)), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qres.Count(), 1);
		auto itsel = qres.begin().GetItem();
		const auto initialLSN = itsel.GetLSN();
		ASSERT_FALSE(initialLSN.isEmpty());
		std::string itemJson;
		{
			WrSerializer ser;
			JsonBuilder jb(ser);
			jb.Put("id", pk);
			jb.Put("int", 200);
			jb.End();
			itemJson = ser.Slice();
		}
		err = itsel.FromJSON(itemJson);
		ASSERT_TRUE(err.ok()) << err.what();
		itsel.SetPrecepts({"int=SERIAL()"});
		client::QueryResults qrUpdate;
		err = cluster.GetNode(followerId)->api.reindexer->Update(kNsName, itsel, qrUpdate);

		std::string itemJsonCheck;
		{
			WrSerializer ser;
			JsonBuilder jb(ser);
			jb.Put("id", pk);
			jb.Put("int", 3);
			jb.End();
			itemJsonCheck = ser.Slice();
		}

		ASSERT_TRUE(err.ok()) << err.what();
		{
			ASSERT_EQ(qrUpdate.Count(), 1);
			auto itemQR = qrUpdate.begin().GetItem();
			ASSERT_EQ(itemQR.GetJSON(), itemJsonCheck);
			const auto resultLSN = itemQR.GetLSN();
			ASSERT_FALSE(resultLSN.isEmpty());
			ASSERT_NE(initialLSN, resultLSN);
		}

		// check the correctness of the update
		SelectHelper(followerId, kNsName, cluster, std::string(itemJsonCheck));
		SelectHelper(leaderId, kNsName, cluster, std::string(itemJsonCheck));
	}
	{
		// delete item
		BaseApi::QueryResultsType qres(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);
		Error err = cluster.GetNode(followerId)->api.reindexer->Select(Query(kNsName).Where("id", CondEq, std::to_string(pk)), qres);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qres.Count(), 1);
		auto itsel = qres.begin().GetItem();
		const auto initialLSN = itsel.GetLSN();
		ASSERT_FALSE(initialLSN.isEmpty());
		itsel.SetPrecepts({"int=SERIAL()"});
		client::QueryResults qrDelete;
		err = cluster.GetNode(followerId)->api.reindexer->Delete(kNsName, itsel, qrDelete);
		ASSERT_TRUE(err.ok()) << err.what();
		{
			ASSERT_EQ(qrDelete.Count(), 1);
			auto itemQR = qrDelete.begin().GetItem();
			ASSERT_EQ(itemQR.GetJSON(), itsel.GetJSON());
			const auto resultLSN = itemQR.GetLSN();
			ASSERT_FALSE(resultLSN.isEmpty());
			ASSERT_NE(initialLSN, resultLSN);
		}

		// check the correctness of the delete
		Select0Helper(followerId, kNsName, cluster);
		Select0Helper(leaderId, kNsName, cluster);
	}
}

static void CheckTruncate(ClusterOperationApi::Cluster& cluster, int followerId, int leaderId) {
	const std::string kNsName = "nsTruncate";
	// create test ns
	CreateTestNs(kNsName, followerId, cluster);

	// insert item
	int pk = 1;
	auto item = cluster.GetNode(followerId)->api.reindexer->NewItem(kNsName);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	Error err = item.FromJSON(itemData(pk, "string" + std::to_string(pk), ""));
	ASSERT_TRUE(err.ok()) << err.what();
	err = cluster.GetNode(followerId)->api.reindexer->Insert(kNsName, item);
	ASSERT_TRUE(err.ok()) << err.what();

	// truncate namespace
	err = cluster.GetNode(followerId)->api.reindexer->TruncateNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();
	// check the correctness of the truncate
	Select0Helper(followerId, kNsName, cluster);
	Select0Helper(leaderId, kNsName, cluster);
}

static void CheckSQL(ClusterOperationApi::Cluster& cluster, int followerId, int leaderId) {
	const std::string kNsName = "nsSQL";
	// create test ns
	CreateTestNs(kNsName, followerId, cluster);

	int pk = 1;
	// insert item
	auto item = cluster.GetNode(followerId)->api.reindexer->NewItem(kNsName);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	Error err = item.FromJSON(itemData(pk, "string" + std::to_string(pk), ""));
	ASSERT_TRUE(err.ok()) << err.what();

	err = cluster.GetNode(followerId)->api.reindexer->Insert(kNsName, item);
	ASSERT_TRUE(err.ok()) << err.what();
	(void)leaderId;
	// select all (one) items from namespace
	{
		BaseApi::QueryResultsType qresSelectTmp;
		err = cluster.GetNode(followerId)->api.reindexer->Select(Query(kNsName), qresSelectTmp);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qresSelectTmp.Count() == 1) << "select count = " << qresSelectTmp.Count();
	}
	// update item
	std::string q = "explain update " + kNsName + " set value='up_name' where id=" + std::to_string(pk);
	auto qr = cluster.GetNode(followerId)->api.ExecSQL(q);
	ASSERT_TRUE(qr.Count() > 0);
	for (auto it : qr) {
		ASSERT_FALSE(it.GetLSN().isEmpty());
	}
	ASSERT_TRUE(!qr.GetExplainResults().empty());
	ASSERT_TRUE(err.ok()) << err.what();
	{
		// check the correctness of the update
		BaseApi::QueryResultsType qresSelect;
		err = cluster.GetNode(followerId)->api.reindexer->Select(Query(kNsName), qresSelect);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qresSelect.Count() == 1) << "select count = " << qresSelect.Count();

		auto it = qresSelect.begin();
		auto itm = it.GetItem();
		std::string itemString = itemData(pk, "up_name", "");
		ASSERT_TRUE(itm.GetJSON() == itemString) << itm.GetJSON();
	}

	// delete item
	auto qresDel = cluster.GetNode(followerId)->api.ExecSQL("delete from " + kNsName);
	for (auto it : qresDel) {
		ASSERT_FALSE(it.GetLSN().isEmpty());
	}
	// check the correctness of the delete
	Select0Helper(followerId, kNsName, cluster);
	Select0Helper(leaderId, kNsName, cluster);
}

TEST_F(ClusterOperationProxyApi, ApiTest) {
	// Test All Api functions
	const size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
				   Cluster cluster(loop, 0, kClusterSize, ports);
				   // waiting cluster synchonization, get leader and foollower id
				   auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
				   ASSERT_NE(leaderId, -1);
				   const int followerId = (leaderId + 1) % kClusterSize;

				   CheckInsertUpsertUpdateDelete(cluster, leaderId, followerId);

				   CheckInsertUpsertUpdateDeleteItemQR(cluster, leaderId, followerId);

				   CheckInsertUpsertUpdateItemQRSerial(cluster, leaderId, followerId);

				   CheckTruncate(cluster, followerId, leaderId);
				   CheckSQL(cluster, followerId, leaderId);

				   CheckDropNamespace(cluster, followerId, leaderId);
				   CheckDropNamespace(cluster, leaderId, leaderId);

				   CheckAddUpdateDropIndex(cluster, followerId, leaderId);
				   CheckAddUpdateDropIndex(cluster, followerId, followerId);
				   CheckAddUpdateDropIndex(cluster, leaderId, leaderId);

				   CheckSetGetShema(cluster, followerId, leaderId);
				   CheckSetGetShema(cluster, followerId, followerId);

				   CheckSetGetEnumDeleteMeta(cluster, followerId, leaderId);
			   }),
			   1024 * 1024);

	loop.run();
}

TEST_F(ClusterOperationProxyApi, DeleteSelect) {
	// insert, delete, select data from follower node
	const size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	const std::string kNsName = "ns1";
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &kNsName, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		int followerId = (leaderId + 1) % kClusterSize;
		// create test data
		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);
		for (int i = 0; i < 10; i++) {
			int nodeNum = rand() % kClusterSize;
			cluster.AddRow(nodeNum, kNsName, i);
		}
		{
			// check correctness of the data
			BaseApi::QueryResultsType qr;
			auto err = cluster.GetNode(leaderId)->api.reindexer->Select(Query(kNsName), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 10);
		}

		cluster.WaitSync(kNsName);

		auto followerNode = cluster.GetNode(followerId);
		const auto selQ = Query(kNsName).Where("id", CondLt, 5);
		for (int k = 0; k < 10; k++) {
			// delete rows
			{
				BaseApi::QueryResultsType delResult;
				Error err = followerNode->api.reindexer->Select(selQ, delResult);
				ASSERT_EQ(delResult.Count(), 5) << "incorect count for delete";
				for (auto& it : delResult) {
					auto item = it.GetItem();
					err = followerNode->api.reindexer->Delete(kNsName, item);
					ASSERT_TRUE(err.ok()) << err.what();
				}
			}
			// check delete correctness
			{
				BaseApi::QueryResultsType selResult;
				Error err = followerNode->api.reindexer->Select(selQ, selResult);
				ASSERT_TRUE(selResult.Count() == 0) << "incorrect count =" << selResult.Count();
			}
			{
				for (int i = 0; i < 5; i++) {
					cluster.AddRow(followerId, kNsName, i);
				}
			}
		}
		followerNode.reset();
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ClusterStatsErrorHandling) {
	// Check incorrect queries to #replicationstats
	const size_t kClusterSize = 3;
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		std::vector<Query> queries = {Query(kReplicationStatsNamespace),
									  Query(kReplicationStatsNamespace)
										  .Where("type", CondEq, cluster::kAsyncReplStatsType)
										  .Or()
										  .Where("type", CondEq, cluster::kClusterReplStatsType),
									  Query(kReplicationStatsNamespace).Not().Where("type", CondEq, cluster::kAsyncReplStatsType)};
		for (size_t nodeId = 0; nodeId < kClusterSize; ++nodeId) {
			for (auto& q : queries) {
				BaseApi::QueryResultsType qr;
				Error err = cluster.GetNode(nodeId)->api.reindexer->Select(q, qr);
				ASSERT_EQ(err.code(), errParams) << q.GetSQL();
				ASSERT_STREQ(err.what(),
							 "Query to #replicationstats has to contain one of the following conditions: type='async' or type='cluster'")
					<< q.GetSQL();
			}
			for (auto& q : queries) {
				std::string sql = q.GetSQL();
				BaseApi::QueryResultsType qr;
				Error err = cluster.GetNode(nodeId)->api.reindexer->ExecSQL(sql, qr);
				ASSERT_EQ(err.code(), errParams) << sql;
				ASSERT_STREQ(err.what(),
							 "Query to #replicationstats has to contain one of the following conditions: type='async' or type='cluster'")
					<< sql;
			}
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ChangeLeaderOfflineNodeAndNotExistNode) {
	const size_t kClusterSize = 5;
	const int kNotExistServerNode = 100;
	net::ev::dynamic_loop loop;
	const std::string kNsName = "ns1";
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &kNsName, &kNotExistServerNode, &ports, this] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);
		for (int v = 0; v < 10; v++) {
			{
				auto item = cluster.GetNode(leaderId)->CreateClusterChangeLeaderItem(kNotExistServerNode);
				Error err = cluster.GetNode(leaderId)->api.reindexer->Update(kConfigNamespace, item);
				Error errPattern(errLogic, "Cluster config. Cannot find node index for ServerId({})", kNotExistServerNode);
				ASSERT_EQ(err.code(), errPattern.code());
				ASSERT_EQ(err.whatStr(), errPattern.whatStr());
				int leaderNew = cluster.AwaitLeader(kMaxElectionsTime);
				ASSERT_EQ(leaderId, leaderNew);
			}
			{
				const int stopFollowerId = GetRandFollower(kClusterSize, leaderId);
				const auto stopDsn = MakeDsn(reindexer_server::UserRole::kRoleReplication, cluster.GetNode(stopFollowerId));
				cluster.StopServer(stopFollowerId);
				auto item = cluster.GetNode(leaderId)->CreateClusterChangeLeaderItem(stopFollowerId);
				Error err = cluster.GetNode(leaderId)->api.reindexer->Update(kConfigNamespace, item);
				ASSERT_FALSE(err.ok());
				ASSERT_EQ(err.whatStr(), fmt::format("Target node {} is not available.", stopDsn));
				int leaderNew = cluster.AwaitLeader(kMaxElectionsTime);
				ASSERT_EQ(leaderId, leaderNew);
				cluster.StartServer(stopFollowerId);
				leaderNew = cluster.AwaitLeader(kMaxElectionsTime);
				ASSERT_EQ(leaderId, leaderNew);
			}
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ChangeLeader) {
	const size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	const std::string kNsName = "ns1";
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &kNsName, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		// create test data
		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);
		for (int i = 0; i < 10; i++) {
			int nodeNum = rand() % kClusterSize;
			cluster.AddRow(nodeNum, kNsName, i);
		}

		cluster.WaitSync(kNsName);

		for (int i = 0; i < 3; i++) {
			for (unsigned int k = 0; k < kClusterSize; k++) {  // -V756
				for (int j = 0; j < 2; j++) {
					cluster.GetNode(0)->SetClusterLeader(k);
					leaderId = cluster.AwaitLeader(kMaxElectionsTime);
					ASSERT_EQ(leaderId, k) << "iteration: " << i;
				}
			}
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, Shutdown) {
	const size_t kClusterSize = 5;
	net::ev::dynamic_loop loop;
	const std::string kNsName = "ns1";
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &kNsName, &ports] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		// create test data
		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);

		std::vector<std::thread> threads;
		std::atomic<int> counter{0};
		std::atomic<bool> done = {false};
		constexpr auto kSleepTime = std::chrono::milliseconds(1);

		auto addItemFn = [&counter, &cluster, &done, kSleepTime](int nodeId, std::string_view nsName) noexcept {
			while (!done) {
				auto err = cluster.AddRowWithErr(nodeId, nsName, counter++, Cluster::DataParam{});
				(void)err;	// ignored; Error are expected)
				std::this_thread::sleep_for(kSleepTime);
			}
		};
		threads.reserve(kClusterSize);
		for (size_t i = 0; i < kClusterSize; ++i) {
			threads.emplace_back(addItemFn, i % kClusterSize, kNsName);
		}
		for (size_t i = 0; i < kClusterSize; ++i) {
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
			TestCout() << "Stopping " << i << std::endl;
			cluster.StopServer(i);
		}
		done = true;
		for (auto& th : threads) {
			th.join();
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ChangeLeaderAndWrite) {
	constexpr size_t kClusterSize = 5;
	constexpr int kInserdThreadCount = 5;
	constexpr int kInserdTxThreadCount = 5;
	constexpr int kItemsPerTx = 20;
	const std::string kNsName = "ns1";

	net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, &kNsName, this] {
		const auto ports = GetDefaults();
		Cluster cluster(loop, 0, kClusterSize, ports);
		std::atomic<bool> done = {false};
		constexpr auto kSleepTime = std::chrono::milliseconds(1);
		ItemTracker itemTracker;
		std::atomic<int> counter{0};
		std::atomic<int> txCounter{0};
		std::vector<std::thread> threads;

		auto addItemFun = [&counter, &cluster, &done, kSleepTime, &itemTracker](int nodeId, std::string_view nsName, int tid) noexcept {
			const std::string threadName = "Upsert_" + std::to_string(tid);
			while (!done) {
				int id = counter++;
				std::string json;
				ItemTracker::ItemInfo info(id, nodeId, threadName);
				// Do not create float vectors here to simplify items comparsion
				auto err = cluster.AddRowWithErr(nodeId, nsName, id, Cluster::DataParam{.emptyVector = true}, &json);
				if (err.code() == errUpdateReplication) {
					itemTracker.AddUnknown(std::move(json), std::move(info));
				} else if (err.code() == errAlreadyProxied) {
					itemTracker.AddError(std::move(json), std::move(info));
				} else {
					ASSERT_TRUE(err.ok()) << err.what();
					itemTracker.AddCommited(std::move(json), std::move(info));
				}
				std::this_thread::sleep_for(kSleepTime);
			}
		};

		auto addItemItemInTxFun = [&counter, &txCounter, &cluster, &done, &itemTracker, kSleepTime](int nodeId, std::string_view nsName,
																									int tid) noexcept {
			auto client = cluster.GetNode(nodeId)->api.reindexer;
			auto& api = cluster.GetNode(nodeId)->api;
			const std::string threadName = "Tx_" + std::to_string(tid);
			while (!done) {
				auto txNum = txCounter++;
				auto txStart = ItemTracker::ClockT::now();
				auto tx = client->NewTransaction(nsName);
				ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
				std::vector<std::pair<std::string, ItemTracker::ItemInfo>> items;
				for (int j = 0; j < kItemsPerTx; ++j) {
					auto item = tx.NewItem();
					auto err = item.Status();
					if (err.code() == errTxInvalidLeader || err.code() == errWrongReplicationData || err.code() == errAlreadyProxied) {
						break;
					} else {
						ASSERT_TRUE(err.ok()) << err.what();
					}
					cluster.FillItem(api, item, counter++, Cluster::DataParam{.emptyVector = true});
					ItemTracker::ItemInfo info(counter, txNum, tx.GetTransactionId(), nodeId, txStart, threadName);
					items.emplace_back(item.GetJSON(), info);
					err = tx.Upsert(std::move(item));
					if (err.code() == errTxInvalidLeader || err.code() == errWrongReplicationData || err.code() == errAlreadyProxied) {
						items.pop_back();
					} else {
						ASSERT_TRUE(err.ok()) << err.what();
					}
				}
				BaseApi::QueryResultsType qrTx;

				auto timeBeforeCommit = ItemTracker::ClockT::now();
				int64_t txId = tx.GetTransactionId();
				auto err = client->CommitTransaction(tx, qrTx);
				ItemTracker::ItemInfo txInfoAfterCommit(txNum, txId, nodeId, txStart, timeBeforeCommit, ItemTracker::ClockT::now(),
														threadName);
				txInfoAfterCommit.txBeforeCommit = timeBeforeCommit;
				if (err.code() == errTxInvalidLeader || err.code() == errWrongReplicationData || err.code() == errAlreadyProxied) {
					itemTracker.AddErrorTx(items, txNum, std::move(txInfoAfterCommit));
				} else if (err.code() == errUpdateReplication) {
					// This data may still be replicated
					itemTracker.AddUnknownTx(items, txNum, std::move(txInfoAfterCommit));
				} else {
					ASSERT_TRUE(err.ok()) << err.what();
					itemTracker.AddCommitedTx(items, txNum, std::move(txInfoAfterCommit));
				}
				std::this_thread::sleep_for(kSleepTime);
			}
		};

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);

		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);
		for (int k = 0; k < kInserdThreadCount; ++k) {
			int nodeNum = k % kClusterSize;
			threads.emplace_back(addItemFun, nodeNum, kNsName, k);
		}

		for (int k = 0; k < kInserdTxThreadCount; ++k) {
			const int nodeNum = k % kClusterSize;
			threads.emplace_back(addItemItemInTxFun, nodeNum, kNsName, k);
		}

		for (int i = 0; i < 10; ++i) {
			const int newLeaderId = GetRandFollower(kClusterSize, leaderId);
			TestCout() << leaderId << " -> " << newLeaderId << std::endl;
			cluster.ChangeLeader(leaderId, newLeaderId);
			loop.sleep(std::chrono::milliseconds(100));
		}

		done = true;

		for (auto& t : threads) {
			t.join();
		}
		cluster.WaitSync(kNsName);
		{
			leaderId = cluster.AwaitLeader(kMaxElectionsTime);
			auto leaderClient = cluster.GetNode(leaderId)->api.reindexer;
			reindexer::client::QueryResults qr;
			auto err = leaderClient->ExecSQL("select *, vectors() from ns1 order by id", qr);
			ASSERT_TRUE(err.ok()) << err.what();
			itemTracker.Validate(qr);
		}
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ChangeLeaderAndWriteSimple) {
	const size_t kClusterSize = 5;
	const int kInserdThreadCount = 10;
	const std::string kNsName = "ns1";
	net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, &kNsName, this] {
		const auto ports = GetDefaults();
		std::vector<std::thread> threads;
		Cluster cluster(loop, 0, kClusterSize, ports);
		std::atomic<int> counter{0};
		std::atomic<bool> stopInsert{false};

		auto addItemFun = [&counter, &stopInsert, &cluster](int nodeId, std::string_view nsName) {
			while (!stopInsert) {
				int id = counter++;
				auto err = cluster.AddRowWithErr(nodeId, nsName, id, Cluster::DataParam{});
				if (err.code() == errUpdateReplication || err.code() == errAlreadyProxied) {
				} else {
					ASSERT_TRUE(err.ok()) << err.what();
				}
				int maxCounter = 1000;
				if (counter >= maxCounter) {
					counter = 0;
				}
			}
		};

		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		// create test data
		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);
		for (int k = 0; k < kInserdThreadCount; k++) {
			int nodeNum = rand() % kClusterSize;
			threads.emplace_back(addItemFun, nodeNum, kNsName);
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		for (size_t i = 0; i < kClusterSize; i++) {	 // -V756
			for (int k = 0; k < 2; k++) {
				cluster.GetNode(0)->SetClusterLeader(i);
				leaderId = cluster.AwaitLeader(kMaxElectionsTime);
				ASSERT_EQ(leaderId, i);
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}

		stopInsert = true;

		for (auto& t : threads) {
			t.join();
		}
		cluster.WaitSync(kNsName);
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, ChangeLeaderTimeout) {
	const size_t kClusterSize = 5;

	net::ev::dynamic_loop loop;
	const std::string kNsName = "ns1";
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &kNsName, &ports, this] {
		Cluster cluster(loop, 0, kClusterSize, ports);
		auto leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		// create test data
		cluster.InitNs(leaderId, kNsName);
		cluster.WaitSync(kNsName);
		for (int i = 0; i < 10; i++) {
			int nodeNum = rand() % kClusterSize;
			cluster.AddRow(nodeNum, kNsName, i);
		}

		cluster.WaitSync(kNsName);

		int newLeaderId = GetRandFollower(kClusterSize, leaderId);
		cluster.GetNode(leaderId)->SetClusterLeader(newLeaderId);
		cluster.StopServer(newLeaderId);

		leaderId = cluster.AwaitLeader(cluster::kDesiredLeaderTimeout * 2);
		ASSERT_NE(leaderId, -1);
		ASSERT_NE(leaderId, newLeaderId);
	}));

	loop.run();
}

TEST_F(ClusterOperationProxyApi, SelectFromStatsTimeout) {
	// Check error on attempt to reset cluster namespace role
	net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&loop, &ports] {
		constexpr size_t kClusterSize = 3;
		Cluster cluster(loop, 0, kClusterSize, ports);
		cluster.StopServers({0, 1});
		client::QueryResults qr;
		auto err = cluster.GetNode(2)->api.reindexer->ExecSQL("select * from #replicationstats where type='cluster'", qr);
		ASSERT_EQ(err.code(), errTimeout);
		ASSERT_STREQ(err.what(), "Unable to get cluster's leader: Context was canceled or timed out (condition variable)");
	}));

	loop.run();
}
