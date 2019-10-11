#pragma once

#include <string>
#include <thread>
#include "core/dbconfig.h"
#include "core/namespace.h"
#include "core/namespacestat.h"
#include "estl/atomic_unique_ptr.h"
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
		int updated = 0, deleted = 0, errors = 0, updatedIndexes = 0, deletedIndexes = 0, updatedMeta = 0, processed = 0;
		WrSerializer &Dump(WrSerializer &ser);
	};

	void run();
	void stop();
	// Sync database
	Error syncDatabase();
	// Read and apply WAL from master
	Error syncNamespaceByWAL(const NamespaceDef &ns);
	// Apply WAL from master to namespace
	Error applyWAL(string_view nsName, client::QueryResults &qr);
	// Apply WAL from master to namespace
	Error applyWAL(Namespace::Ptr slaveNs, client::QueryResults &qr);
	// Sync indexes of namespace
	Error syncIndexesForced(Namespace::Ptr slaveNs, const NamespaceDef &masterNsDef);
	// Forced sync of namespace
	Error syncNamespaceForced(const NamespaceDef &ns, string_view reason);
	// Sync meta data
	Error syncMetaForced(reindexer::Namespace::Ptr slaveNs);
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
	ReplicationConfigData config_;

	std::atomic<bool> terminate_;
	enum State { StateInit, StateSyncing, StateIdle };
	std::atomic<State> state_;
	fast_hash_map<string, int64_t, nocase_hash_str, nocase_equal_str> maxLsns_;

	std::mutex syncMtx_;
	std::mutex masterMtx_;
	std::atomic<bool> enabled_;

	const RdxContext dummyCtx_;
};

}  // namespace reindexer
