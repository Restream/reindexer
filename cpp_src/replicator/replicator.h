#pragma once

#include <string>
#include <thread>
#include "core/dbconfig.h"
#include "core/namespace/namespace.h"
#include "core/namespace/namespacestat.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/fast_hash_map.h"
#include "net/ev/ev.h"
#include "tools/errors.h"
#include "updatesobserver.h"
#include "vendor/hopscotch/hopscotch_set.h"

namespace reindexer {
namespace client {
class Reindexer;
class QueryResults;
}  // namespace client

class ReindexerImpl;

class Replicator : public IUpdatesObserver {
public:
	Replicator(ReindexerImpl *slave);
	~Replicator();
	bool Configure(const ReplicationConfigData &config);
	Error Start();
	void Stop();
	void Enable() { enabled_.store(true, std::memory_order_release); }

protected:
	struct SyncStat {
		ReplicationState masterState;
		Error lastError;
		int updated = 0, deleted = 0, errors = 0, updatedIndexes = 0, deletedIndexes = 0, updatedMeta = 0, processed = 0, schemasSet = 0;
		WrSerializer &Dump(WrSerializer &ser);
	};
	struct NsErrorMsg {
		Error err;
		uint64_t count = 0;
	};
	class SyncQueue {
	public:
		SyncQueue(const std::mutex &replicatorMtx) noexcept : replicatorMtx_(replicatorMtx) {}
		void Push(const std::string &nsName, NamespaceDef &&nsDef, bool force);
		bool Get(NamespaceDef &def, bool &force) const;
		bool Pop(std::string_view nsName, const std::unique_lock<std::mutex> &replicatorLock) noexcept;
		size_t Size() const noexcept { return size_.load(std::memory_order_acquire); }
		bool Contains(std::string_view nsName) noexcept;
		void Clear() noexcept;

	private:
		struct recordData {
			recordData() = default;
			recordData(NamespaceDef &&_def, bool _forced) : def(std::move(_def)), forced(_forced) {}

			NamespaceDef def;
			bool forced = false;
		};
		fast_hash_map<std::string, recordData, nocase_hash_str, nocase_equal_str, nocase_less_str> queue_;
		std::atomic<size_t> size_ = {0};
		mutable std::mutex mtx_;
		const std::mutex &replicatorMtx_;
	};

	void run();
	void stop();
	// Sync single namespace
	Error syncNamespace(const NamespaceDef &ns, std::string_view forceSyncReason, SyncQueue *sourceQueue);
	// Sync database
	Error syncDatabase();
	// Read and apply WAL from master
	Error syncNamespaceByWAL(const NamespaceDef &ns);
	// Apply WAL from master to namespace
	Error applyWAL(Namespace::Ptr &slaveNs, client::QueryResults &qr, const NamespaceDef *nsDef = nullptr);
	// Sync indexes of namespace
	Error syncIndexesForced(Namespace::Ptr &slaveNs, const NamespaceDef &masterNsDef);
	// Sync namespace schema
	Error syncSchemaForced(Namespace::Ptr &slaveNs, const NamespaceDef &masterNsDef);
	// Forced sync of namespace
	Error syncNamespaceForced(const NamespaceDef &ns, std::string_view reason);
	// Sync meta data
	Error syncMetaForced(Namespace::Ptr &slaveNs, std::string_view nsName);
	// Apply single WAL record
	Error applyWALRecord(LSNPair LSNs, std::string_view nsName, Namespace::Ptr &ns, const WALRecord &wrec, SyncStat &stat,
						 const NamespaceDef * = nullptr);
	// Apply single transaction WAL record
	Error applyTxWALRecord(LSNPair LSNs, std::string_view nsName, Namespace::Ptr &ns, const WALRecord &wrec);
	void checkNoOpenedTransaction(std::string_view nsName, Namespace::Ptr &slaveNs);
	// Apply single cjson item
	Error modifyItem(LSNPair LSNs, Namespace::Ptr &ns, std::string_view cjson, int modifyMode, const TagsMatcher &tm, SyncStat &stat);
	static Error unpackItem(Item &, lsn_t, std::string_view cjson, const TagsMatcher &tm);
	// Push update to the queue to apply it later
	void pushPendingUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord &wrec);

	void OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord &wrec) override final;
	void onWALUpdateImpl(LSNPair LSNs, std::string_view nsName, const WALRecord &wrec);
	void OnUpdatesLost(std::string_view nsName) override final;
	void OnConnectionState(const Error &err) override final;

	bool canApplyUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord &wrec);
	bool isSyncEnabled(std::string_view nsName);
	bool retryIfNetworkError(const Error &err);
	void subscribeUpdatesIfRequired(const std::string &nsName);

	std::unique_ptr<client::Reindexer> master_;
	ReindexerImpl *slave_;

	net::ev::dynamic_loop loop_;
	std::thread thread_;
	net::ev::async stop_;
	net::ev::async resync_;
	net::ev::timer resyncTimer_;
	net::ev::async walSyncAsync_;
	net::ev::async resyncUpdatesLostAsync_;
	std::atomic_bool resyncUpdatesLostFlag_;

	ReplicationConfigData config_;

	std::atomic<bool> terminate_;
	enum State { StateInit, StateSyncing, StateIdle };
	std::atomic<State> state_;

	using UpdatesContainer = std::vector<std::pair<LSNPair, PackedWALRecord>>;
	struct UpdatesData {
		UpdatesContainer container;
		bool UpdatesLost = false;
	};

	fast_hash_map<std::string, UpdatesData, nocase_hash_str, nocase_equal_str, nocase_less_str> pendedUpdates_;
	tsl::hopscotch_set<std::string, nocase_hash_str, nocase_equal_str> syncedNamespaces_;
	std::string currentSyncNs_;

	std::mutex syncMtx_;
	std::mutex masterMtx_;
	std::atomic<bool> enabled_;

	const RdxContext dummyCtx_;
	std::unordered_map<const Namespace *, Transaction> transactions_;
	fast_hash_map<std::string, NsErrorMsg, nocase_hash_str, nocase_equal_str, nocase_less_str> lastNsErrMsg_;
	SyncQueue syncQueue_;
};

}  // namespace reindexer
