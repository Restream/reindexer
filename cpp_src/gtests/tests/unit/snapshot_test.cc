#include "core/namespace/namespacestat.h"
#include "core/type_consts.h"
#include "gtests/tools.h"
#include "net/cproto/cproto.h"
#include "net/ev/ev.h"
#include "snapshot_api.h"

const std::string SnapshotTestApi::kDefaultRPCServerAddr = std::string("127.0.0.1:") + std::to_string(SnapshotTestApi::kDefaultRPCPort);

// ASAN randomly breaks on coroutines here
#ifndef REINDEX_WITH_ASAN
TEST_F(SnapshotTestApi, ForceSyncFromLocalToRemote) {
	// Check if we can apply snapshot from local rx instance to remote rx instance via RPC
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::Reindexer localRx;
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient, localRx);

		InitNS(localRx, kNsName);
		FillData(localRx, kNsName, 0);

		constexpr auto kTimeout = std::chrono::seconds(15);

		// Checking full snapshot
		{
			Snapshot crsn;
			auto err = localRx.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(crsn.HasRawData());

			std::string tmpNsName;
			err = rxClient.WithLSN(lsn_t(0, 0))
					  .WithTimeout(kTimeout)
					  .CreateTemporaryNamespace(kNsName, tmpNsName, StorageOpts().Enabled(false));
			ASSERT_TRUE(err.ok()) << err.what();

			for (auto& it : crsn) {
				auto ch = it.Chunk();
				err = rxClient.WithLSN(lsn_t(0, 0)).WithTimeout(kTimeout).ApplySnapshotChunk(tmpNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}
			err = rxClient.WithLSN(lsn_t(0, 0)).WithTimeout(kTimeout).RenameNamespace(tmpNsName, kNsName);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);

		// Checking WAL snapshot
		FillData(localRx, kNsName, 1000);
		{
			auto remoteState = GetNsDataState(rxClient, kNsName);
			Snapshot crsn;
			auto err = localRx.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(remoteState.nsVersion, remoteState.lsn)), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_FALSE(crsn.HasRawData());
			for (auto& it : crsn) {
				auto ch = it.Chunk();
				ASSERT_TRUE(ch.IsWAL());
				ASSERT_FALSE(ch.IsShallow());
				err = rxClient.WithLSN(lsn_t(0, 0)).WithTimeout(kTimeout).ApplySnapshotChunk(kNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);
	}));
	loop.run();
}

TEST_F(SnapshotTestApi, ForceSyncFromRemoteToLocal) {
	// Check if we can apply snapshot from remote rx instance to local rx instance via RPC
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::Reindexer localRx;
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient, localRx);

		InitNS(rxClient, kNsName);
		FillData(rxClient, kNsName, 0);

		constexpr auto kTimeout = std::chrono::seconds(15);

		// Checking full snapshot
		{
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(crsn.HasRawData());
			ASSERT_TRUE(crsn.ClusterOperationStat().has_value());
			ASSERT_EQ(crsn.ClusterOperationStat()->leaderId, -1);
			ASSERT_EQ(crsn.ClusterOperationStat()->role, reindexer::ClusterOperationStatus::Role::None);

			std::string tmpNsName;
			err = localRx.WithLSN(lsn_t(0, 0))
					  .WithTimeout(kTimeout)
					  .CreateTemporaryNamespace(kNsName, tmpNsName, StorageOpts().Enabled(false));
			ASSERT_TRUE(err.ok()) << err.what();

			for (auto& it : crsn) {
				auto& ch = it.Chunk();
				err = localRx.WithLSN(lsn_t(0, 0)).WithTimeout(kTimeout).ApplySnapshotChunk(tmpNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}

			err = localRx.WithLSN(lsn_t(0, 0)).WithTimeout(kTimeout).RenameNamespace(tmpNsName, kNsName);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);

		// Checking WAL snapshot
		FillData(rxClient, kNsName, 1000);
		{
			auto remoteState = GetNsDataState(localRx, kNsName);
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(remoteState.nsVersion, remoteState.lsn)), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_FALSE(crsn.HasRawData());
			ASSERT_TRUE(crsn.ClusterOperationStat().has_value());
			ASSERT_EQ(crsn.ClusterOperationStat()->leaderId, -1);
			ASSERT_EQ(crsn.ClusterOperationStat()->role, reindexer::ClusterOperationStatus::Role::None);
			for (auto& it : crsn) {
				auto& ch = it.Chunk();
				ASSERT_TRUE(ch.IsWAL());
				ASSERT_FALSE(ch.IsShallow());
				err = localRx.WithLSN(lsn_t(0, 0)).WithTimeout(kTimeout).ApplySnapshotChunk(kNsName, ch);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		}
		CompareData(rxClient, localRx);
		CompareWalSnapshots(rxClient, localRx);
	}));
	loop.run();
}
#endif	// REINDEX_WITH_ASAN

TEST_F(SnapshotTestApi, ConcurrentSnapshotsLimit) {
	// Check if concurrent snapshots limit is actually works
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient);

		InitNS(rxClient, kNsName);
		FillData(rxClient, kNsName, 0);

		std::vector<client::Snapshot> snapshots;
		for (size_t i = 0; i < reindexer::net::cproto::kMaxConcurentSnapshots; ++i) {
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			snapshots.emplace_back(std::move(crsn));
		}
		{
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_EQ(err.code(), errLogic) << err.what();
		}
		// Check if snapshot will be freed on server side after iterating over it
		for (auto& it : snapshots.back()) {
			(void)it;
		}
		{
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
			client::Snapshot crsn1;
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_EQ(err.code(), errLogic) << err.what();
		}
		// Check if snapshot will be freed on server side after destructor call
		{
			client::Snapshot crsn;
			auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), crsn);
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}));
	loop.run();
}

TEST_F(SnapshotTestApi, SnapshotInvalidation) {
	// Check if snapshot will be invalidated after reconnect
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient);

		InitNS(rxClient, kNsName);
		FillData(rxClient, kNsName, 0);

		client::Snapshot sn;
		auto err = rxClient.GetSnapshot(kNsName, SnapshotOpts(), sn);
		ASSERT_TRUE(err.ok()) << err.what();

		rxClient.Stop();
		Connect(loop, rxClient);
		err = rxClient.Status(true);
		ASSERT_TRUE(err.ok()) << err.what();

		try {
			for (auto& it : sn) {
				(void)it;
			}
			EXPECT_TRUE(false) << "Exception was expected";
		} catch (Error& e) {
			EXPECT_EQ(e.code(), errNetwork);
			EXPECT_STREQ(e.what(), "Connection was broken and all associated snapshots, queryresults and transaction were invalidated");
		}
	}));
	loop.run();
}

TEST_F(SnapshotTestApi, MaxWALDepth) {
	// Check if max wal depth option for snapshots is actually works
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient);

		InitNS(rxClient, kNsName);
		FillData(rxClient, kNsName, 0);

		constexpr int64_t kValidLimit = 100;
		constexpr int64_t kNegativeLimit = -1;
		constexpr int64_t kTooLargeLimit = 10000;
		client::CoroQueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		ReplicationStateV2 replState;
		auto err = rxClient.GetReplState(kNsName, replState);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_GT(replState.lastLsn.Counter(), kValidLimit);

		{
			// Check valid limit for client snapshot
			client::Snapshot sn;
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(), kValidLimit), sn);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(GetWALItemsCount(sn), kValidLimit + 1);
		}
		{
			// Check negative and large limits for client snapshot
			client::Snapshot sn1, sn2;
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(), kNegativeLimit), sn1);
			const auto sn1WalItemsCount = GetWALItemsCount(sn1);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(), kTooLargeLimit), sn2);
			ASSERT_TRUE(err.ok()) << err.what();
			const auto sn2WalItemsCount = GetWALItemsCount(sn2);
			ASSERT_EQ(sn1WalItemsCount, sn2WalItemsCount);
			ASSERT_EQ(sn1WalItemsCount, replState.lastLsn.Counter() + 1 + 1);
		}
	}));
	loop.run();
}

TEST_F(SnapshotTestApi, MaxWALDepthWithWALOverflow) {
	// Check if max wal depth option for snapshots is works, when WAL ring-buffer is overflowed
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::client::CoroReindexer rxClient;
		Connect(loop, rxClient);
		constexpr auto kWALSize = 500;
		constexpr auto kUpsertsCount = 1200;

		InitNS(rxClient, kNsName);
		SetWalSize(rxClient, kWALSize);
		FillData(rxClient, kNsName, 0, kUpsertsCount);

		constexpr int64_t kValidLimit = 100;
		constexpr int64_t kNegativeLimit = -1;
		constexpr int64_t kTooLargeLimit = 10000;
		client::CoroQueryResults qr(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		ReplicationStateV2 replState;
		auto err = rxClient.GetReplState(kNsName, replState);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_GT(replState.lastLsn.Counter(), kValidLimit);
		ASSERT_GT(replState.lastLsn.Counter(), kWALSize);

		{
			// Check valid limit for client snapshot
			client::Snapshot sn;
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(), kValidLimit), sn);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(GetWALItemsCount(sn), kValidLimit + 1);
		}
		{
			// Check negative and large limits for client snapshot
			client::Snapshot sn1, sn2;
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(), kNegativeLimit), sn1);
			const auto sn1WalItemsCount = GetWALItemsCount(sn1);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rxClient.GetSnapshot(kNsName, SnapshotOpts(ExtendedLsn(), kTooLargeLimit), sn2);
			ASSERT_TRUE(err.ok()) << err.what();
			const auto sn2WalItemsCount = GetWALItemsCount(sn2);
			ASSERT_EQ(sn1WalItemsCount, sn2WalItemsCount);
			ASSERT_EQ(sn1WalItemsCount, kWALSize + 1);
		}
	}));
	loop.run();
}
