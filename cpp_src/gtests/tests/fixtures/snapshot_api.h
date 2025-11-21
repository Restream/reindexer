#pragma once

#include <tools/fsops.h>
#include "client/snapshot.h"
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/system_ns_names.h"
#include "rpcclient_api.h"
#include "vendor/gason/gason.h"

class [[nodiscard]] SnapshotTestApi : public RPCClientTestApi {
protected:
	struct [[nodiscard]] NsDataState {
		lsn_t lsn;
		lsn_t nsVersion;
		uint64_t dataHash = 0;
		int64_t dataCount = 0;
	};

	void SetUp() {
		std::ignore = fs::RmDirAll(kBaseTestsetDbPath);
		StartServer();
	}
	void TearDown() {
		[[maybe_unused]] auto err = RPCClientTestApi::StopAllServers();
		assertf(err.ok(), "{}", err.what());
		std::ignore = fs::RmDirAll(kBaseTestsetDbPath);
	}

	void StartServer() {
		const std::string dbPath = std::string(kBaseTestsetDbPath) + "/" + std::to_string(kDefaultRPCPort);
		RPCClientTestApi::AddRealServer(dbPath, kDefaultRPCServerAddr, kDefaultHttpPort);
		RPCClientTestApi::StartServer(kDefaultRPCServerAddr);
	}

	template <typename RxT>
	void InitNS(RxT& rx, std::string_view nsName) {
		Error err = rx.OpenNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"int", {"int"}, "tree", "int", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"string", {"string"}, "hash", "string", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
	}

	template <typename RxT>
	void FillData(RxT& rx, std::string_view nsName, size_t from, int upsertItemsCount = 500) {
		const int kUpsertItemsCnt = upsertItemsCount;
		const int kUpdateItemsCnt = kUpsertItemsCnt / 10;
		const int kDeleteItemsCnt = kUpsertItemsCnt / 10;
		const int kTxItemsCnt = kUpsertItemsCnt;

		Error err;
		for (int i = 0; i < kUpsertItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(i + from) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		for (int i = 0; i < kUpdateItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(from + i * 5) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		for (int i = 0; i < kDeleteItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(from + i * 7) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Delete(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		auto tx = rx.NewTransaction(nsName);
		ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
		for (int i = 0; i < kTxItemsCnt; ++i) {
			auto item = rx.NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			// clang-format off
			err = item.FromJSON("{"
								"\"id\": " + std::to_string(from + i * 2) + "," ""
								"\"int\": " + std::to_string(rand()) + ","
								"\"string\": \"" + randStringAlph(15) + "\""
								"}");
			// clang-format on
			ASSERT_TRUE(err.ok()) << err.what();
			if (i % 2) {
				err = tx.Upsert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			} else {
				err = tx.Delete(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		commitTx(rx, tx);
	}

	template <typename RxT>
	NsDataState GetNsDataState(RxT& rx, const std::string& ns) {
		Query qr = Query(kMemStatsNamespace).Where("name", CondEq, ns);
		typename RxT::QueryResultsT res;
		auto err = rx.Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();
		NsDataState state;
		for (auto it : res) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			EXPECT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			state.nsVersion.FromJSON(root["replication"]["ns_version"]);
			state.lsn.FromJSON(root["replication"]["last_lsn_v2"]);
			state.dataCount = root["replication"]["data_count"].As<int64_t>();
			state.dataHash = root["replication"]["data_hash"].As<uint64_t>();
		}
		return state;
	}

	void Connect(net::ev::dynamic_loop& loop, reindexer::client::CoroReindexer& rxClient, reindexer::Reindexer& localRx) {
		Connect(localRx);
		Connect(loop, rxClient);
	}
	void Connect(net::ev::dynamic_loop& loop, reindexer::client::CoroReindexer& rxClient) {
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		auto err = rxClient.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void Connect(reindexer::Reindexer& localRx) {
		auto err = localRx.Connect("builtin://" + kLocalDbPath + "/db1");
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void CompareData(reindexer::client::CoroReindexer& rxClient, reindexer::Reindexer& localRx) {
		auto remoteState = GetNsDataState(rxClient, kNsName);
		auto localState = GetNsDataState(localRx, kNsName);
		EXPECT_EQ(remoteState.dataHash, localState.dataHash);
		EXPECT_EQ(remoteState.dataCount, localState.dataCount);
		EXPECT_EQ(remoteState.lsn, localState.lsn);
		EXPECT_EQ(remoteState.nsVersion, localState.nsVersion);
	}

	void CompareWalSnapshots(reindexer::client::CoroReindexer& rxClient, reindexer::Reindexer& localRx) {
		auto clientNsData = GetNsDataState(rxClient, kNsName);
		auto localNsData = GetNsDataState(localRx, kNsName);
		ASSERT_EQ(clientNsData.nsVersion, localNsData.nsVersion);
		client::Snapshot csn;
		auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(clientNsData.nsVersion, lsn_t(0, 0))), csn);
		ASSERT_TRUE(err.ok()) << err.what();
		Snapshot sn;
		err = localRx.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(localNsData.nsVersion, lsn_t(0, 0))), sn);
		ASSERT_TRUE(err.ok()) << err.what();
		auto it2 = sn.begin();
		EXPECT_FALSE(csn.HasRawData());
		EXPECT_FALSE(sn.HasRawData());
		for (auto it1 = csn.begin(); it1 != csn.end() || it2 != sn.end(); ++it1, ++it2) {
			ASSERT_TRUE(it1 != csn.end());
			ASSERT_TRUE(it2 != sn.end());
			auto ch1 = it1.Chunk();
			auto ch2 = it2.Chunk();
			EXPECT_EQ(ch1.Records().size(), ch2.Records().size());
			EXPECT_EQ(ch1.IsTx(), ch2.IsTx());
			ASSERT_TRUE(ch1.IsWAL());
			ASSERT_TRUE(ch2.IsWAL());
			EXPECT_EQ(ch1.IsShallow(), ch2.IsShallow());
		}
	}

	size_t GetWALItemsCount(client::Snapshot& sn) {
		size_t count = 0;
		for (auto& ch : sn) {
			auto chunk = ch.Chunk();
			if (chunk.IsWAL()) {
				count += chunk.Records().size();
			}
		}
		return count;
	}

	void SetWalSize(reindexer::client::CoroReindexer& rxClient, size_t value) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);

		jb.Put("type", "namespaces");
		auto nsArray = jb.Array("namespaces");
		auto ns = nsArray.Object();
		ns.Put("namespace", kNsName);
		ns.Put("wal_size", value);

		ns.End();
		nsArray.End();
		jb.End();

		auto item = rxClient.NewItem(reindexer::kConfigNamespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();

		err = rxClient.Upsert(reindexer::kConfigNamespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	const std::string kBaseTestsetDbPath = fs::JoinPath(fs::GetTempDir(), "rx_test/SnapshotApi");
	static const uint16_t kDefaultRPCPort = 25685;
	const uint16_t kDefaultHttpPort = 33433;
	static const std::string kDefaultRPCServerAddr;
	const std::string kLocalDbPath = std::string(kBaseTestsetDbPath) + "/local";
	const std::string kNsName = "snapshot_test_ns";

private:
	void commitTx(client::CoroReindexer& rx, client::CoroTransaction& tx) {
		client::CoroQueryResults qrTx;
		auto err = rx.CommitTransaction(tx, qrTx);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void commitTx(Reindexer& rx, Transaction& tx) {
		QueryResults qr;
		auto err = rx.CommitTransaction(tx, qr);
		ASSERT_TRUE(err.ok()) << err.what();
	}
};
