#pragma once

#include <string>
#include <thread>
#include "core/dbconfig.h"
#include "core/namespacestat.h"
#include "estl/fast_hash_map.h"
#include "net/ev/ev.h"
#include "tools/errors.h"
#include "updatesobserver.h"

namespace reindexer {
using std::string;
namespace client {
class Reindexer;
class QueryResults;
}  // namespace client

class ReindexerImpl;
struct NamespaceDef;
class Namespace;
class TagsMatcher;

class Replicator : public IUpdatesObserver {
public:
	Replicator(ReindexerImpl *slave);
	~Replicator();
	bool Configure(ReplicationConfigData config);
	Error Start();
	void Stop(bool withLock = true);
	void ShutDown();

protected:
	struct SyncStat {
		ReplicationState masterState;
		Error lastError;
		int updated = 0, deleted = 0, errors = 0, updatedIndexes = 0, deletedIndexes = 0, updatedMeta = 0, processed = 0;
		WrSerializer &Dump(WrSerializer &ser);
	};

	struct InternalConfig {
		void FromReplicationConfig(ReplicationConfigData &&config, bool threadSafeOnly) noexcept {
			if (!threadSafeOnly) {
				role = config.role;
				masterDSN = std::move(config.masterDSN);
				clusterID = config.clusterID;
				connPoolSize = config.connPoolSize;
				workerThreads = config.workerThreads;
				namespaces = std::move(config.namespaces);
			}
			forceSyncOnLogicError.store(config.forceSyncOnLogicError, std::memory_order_release);
			forceSyncOnWrongDataHash.store(config.forceSyncOnWrongDataHash, std::memory_order_release);
		}

		ReplicationRole role{ReplicationNone};
		std::string masterDSN;
		int connPoolSize{1};
		int workerThreads{1};
		int clusterID{1};
		std::atomic<bool> forceSyncOnLogicError{false};
		std::atomic<bool> forceSyncOnWrongDataHash{false};
		fast_hash_set<string, nocase_hash_str, nocase_equal_str> namespaces;
	};

	void run();
	// Sync database
	Error syncDatabase();
	// Read and apply WAL from master
	Error syncNamespaceByWAL(const NamespaceDef &ns);
	// Apply WAL from master to namespace
	Error applyWAL(string_view nsName, client::QueryResults &qr);
	// Sync indexes of namespace
	Error syncIndexesForced(const NamespaceDef &ns);
	// Forced sync of namespace
	Error syncNamespaceForced(const NamespaceDef &ns, string_view reason);
	// Sync meta data
	Error syncMetaForced(string_view nsName);
	// Apply single WAL record
	Error applyWALRecord(int64_t lsn, string_view nsName, std::shared_ptr<Namespace> ns, const WALRecord &wrec, SyncStat &stat);
	// Apply single cjson item
	Error applyItemCJson(int64_t, std::shared_ptr<Namespace> ns, string_view cjson, int modifyMode, const TagsMatcher &tm, SyncStat &stat);

	void OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &walRec) override final;
	void OnConnectionState(const Error &err) override final;

	bool canApplyUpdate(int64_t lsn, string_view nsName);
	bool isSyncEnabled(string_view nsName);

	std::unique_ptr<client::Reindexer> master_;
	ReindexerImpl *slave_;

	net::ev::dynamic_loop loop_;
	std::thread thread_;
	net::ev::async stop_;
	net::ev::async resync_;
	InternalConfig config_;

	std::atomic<bool> terminate_;
	enum State { StateInit, StateSyncing, StateIdle };
	std::atomic<State> state_;
	fast_hash_map<string, int64_t, nocase_hash_str, nocase_equal_str> maxLsns_;

	std::mutex syncMtx_;
	std::mutex masterMtx_;
};

}  // namespace reindexer
