#pragma once

#include "client/connectopts.h"
#include "client/coroqueryresults.h"
#include "client/corotransaction.h"
#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/reindexerconfig.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/shardedmeta.h"
#include "net/ev/ev.h"

namespace reindexer {
class SnapshotChunk;
struct SnapshotOpts;
struct ReplicationStateV2;
struct ClusterOperationStatus;
class DSN;

namespace sharding {
struct ShardingControlRequestData;
struct ShardingControlResponseData;
}  // namespace sharding

namespace client {

class RPCClient;
class Snapshot;

/// The main Reindexer interface. Holds database object<br>
/// *Thread safety*: None of the methods are threadsafe <br>
/// CoroReindexer should be used via multiple coroutines in single thread, while ev-loop is running.
/// *Resources lifetime*: All resources acquired from Reindexer, e.g Item or QueryResults are uses Copy-On-Write
/// semantics, and have independent lifetime<br>
class [[nodiscard]] CoroReindexer {
public:
	using ConnectionStateHandlerT = std::function<void(const Error&)>;

	/// Create Reindexer database object
	explicit CoroReindexer(const ReindexerConfig& = ReindexerConfig());
	/// Destroy Reindexer database object
	~CoroReindexer();
	CoroReindexer(const CoroReindexer&) = delete;
	CoroReindexer(CoroReindexer&&) noexcept;
	CoroReindexer& operator=(const CoroReindexer&) = delete;
	CoroReindexer& operator=(CoroReindexer&&) noexcept;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname` or
	/// `ucproto://user@password:/tmp/reindexer.sock:/dbname`
	/// @param loop - event loop for connections and coroutines handling
	/// @param opts - Connect options. May contain any of <br>
	Error Connect(const std::string& dsn, net::ev::dynamic_loop& loop, const client::ConnectOpts& opts = client::ConnectOpts()) noexcept;
	Error Connect(const DSN& dsn, net::ev::dynamic_loop& loop, const client::ConnectOpts& opts = client::ConnectOpts()) noexcept;
	/// Stop - shutdown connector
	void Stop() noexcept;
	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @param replOpts - Namespace replication options (for replication purposes, should not be user elsewhere)
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts& replOpts = NsReplicationOpts()) noexcept;
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	/// @param replOpts - Namespace replication options (for replication purposes, should not be user elsewhere)
	Error AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts = NsReplicationOpts()) noexcept;
	/// Close namespace. Will free all memory resources, associated with namespace. Forces sync changes to disk
	/// @param nsName - Name of namespace
	Error CloseNamespace(std::string_view nsName) noexcept;
	/// Drop namespace. Will free all memory resources, associated with namespace and erase all files from disk
	/// @param nsName - Name of namespace
	Error DropNamespace(std::string_view nsName) noexcept;
	/// Create new temporary namespace with randomized name
	/// Temporary namespace will be automatically removed on next startup
	/// @param baseName - base name, which will be used in result name
	/// @param resultName - name of created namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// @param version - Namespace version. Should be left empty for leader reindexer instances
	Error CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts = StorageOpts().Enabled(),
								   lsn_t version = lsn_t()) noexcept;
	/// Delete all items in namespace
	/// @param nsName - Name of namespace
	Error TruncateNamespace(std::string_view nsName) noexcept;
	/// Rename namespace. If namespace with dstNsName exists, then it is replaced.
	/// @param srcNsName  - Name of namespace
	/// @param dstNsName  - desired name of namespace
	Error RenameNamespace(std::string_view srcNsName, std::string_view dstNsName) noexcept;
	/// Add index to namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error AddIndex(std::string_view nsName, const IndexDef& index) noexcept;
	/// Update index in namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error UpdateIndex(std::string_view nsName, const IndexDef& index) noexcept;
	/// Drop index from namespace
	/// @param nsName - Name of namespace
	/// @param index - index name
	Error DropIndex(std::string_view nsName, const IndexDef& index) noexcept;
	/// Set fields schema for namespace
	/// @param nsName - Name of namespace
	/// @param schema - JSON in JsonSchema format
	Error SetSchema(std::string_view nsName, std::string_view schema) noexcept;
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaces
	/// @param opts - Enumeration options
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) noexcept;
	/// Gets a list of available databases for a certain server.
	/// @param dbList - list of DB names
	Error EnumDatabases(std::vector<std::string>& dbList) noexcept;
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(std::string_view nsName, Item& item) noexcept;
	/// Insert new Item to namespace.
	/// NOTICE: This method expects, that cjson is compatible with internal client's tagsmatcher
	/// @param nsName - Name of namespace
	/// @param cjson - document's cjson
	Error Insert(std::string_view nsName, std::string_view cjson) noexcept;
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(std::string_view nsName, Item& item) noexcept;
	/// Update Item in namespace.
	/// NOTICE: This method expects, that cjson is compatible with internal client's tagsmatcher
	/// @param nsName - Name of namespace
	/// @param cjson - document's cjson
	Error Update(std::string_view nsName, std::string_view cjson) noexcept;
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(std::string_view nsName, Item& item) noexcept;
	/// Update or Insert Item in namespace.
	/// NOTICE: This method expects, that cjson is compatible with internal client's tagsmatcher
	/// @param nsName - Name of namespace
	/// @param cjson - document's cjson
	Error Upsert(std::string_view nsName, std::string_view cjson) noexcept;
	/// Updates all items in namespace, that satisfy provided query.
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of deleted items.
	Error Update(const Query& query, CoroQueryResults& result) noexcept;
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(std::string_view nsName, Item& item) noexcept;
	/// Delete all items from namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query& query, CoroQueryResults& result) noexcept;
	/// Delete Item from namespace.
	/// NOTICE: This method expects, that cjson is compatible with internal client's tagsmatcher
	/// @param nsName - Name of namespace
	/// @param cjson - document's cjson
	Error Delete(std::string_view nsName, std::string_view cjson) noexcept;
	/// Execute SQL Query and return results
	/// @param query - SQL query.
	/// @param result - QueryResults with found items
	Error ExecSQL(std::string_view query, CoroQueryResults& result) noexcept;
	/// Execute Query and return results
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	Error Select(const Query& query, CoroQueryResults& result) noexcept;
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and further Upsert/Insert/Delete/Update call
	Item NewItem(std::string_view nsName) noexcept;
	/// Get metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with metadata
	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data) noexcept;
	/// Get sharded metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output vector with metadata from different shards
	Error GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data) noexcept;
	/// Put meta data to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with metadata
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data) noexcept;
	/// Delete metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	Error DeleteMeta(std::string_view nsName, const std::string& key) noexcept;
	/// Get list of all meta data keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys) noexcept;
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions) noexcept;
	/// Get current connection status
	/// @param forceCheck - forces to check status immediately (otherwise result of periodic check will be returned)
	Error Status(bool forceCheck = false) noexcept;
	/// Get version of the Reindexer server that is connected to
	Error Version(std::string&) noexcept;
	/// Allocate new transaction for namespace
	/// @param nsName - Name of namespace
	CoroTransaction NewTransaction(std::string_view nsName) noexcept;
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	/// @param result - QueryResults with IDs of items changed by tx.
	Error CommitTransaction(CoroTransaction& tr, CoroQueryResults& result) noexcept;
	/// RollBack transaction - transaction will be deleted after rollback
	/// @param tr - transaction to rollback
	Error RollBackTransaction(CoroTransaction& tr) noexcept;
	/// Get namespace's replication state
	/// @param nsName - Name of namespace
	/// @param state - result state
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state) noexcept;
	/// Set namespace's cluster opertation status
	/// @param nsName - Name of namespace
	/// @param status - new status
	Error SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status) noexcept;
	/// Get namespace snapshot
	/// @param nsName - Name of namespace
	/// @param opts - Snapshot options:
	/// LSN value. Starting point for snapshot. If this LSN is still available, snapshot will contain only diff (wal records)
	/// MaxWALDepth. Max number of WAL-records in force-sync snapshot
	/// @param snapshot - Result snapshot
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot) noexcept;
	/// Apply record from snapshot chunk
	/// @param nsName - Name of namespace
	/// @param ch - Snapshot chunk to apply
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch) noexcept;
	/// Set tags matcher for replica namespace. Current tagsmatcher and new tagsmatcher have to have the same state tokens.
	/// This request is for replicator only.
	/// @param nsName - Name of namespace
	/// @param tm - New tagsmatcher
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm) noexcept;

	/// Add observer for client's connection state. This callback will be called on each connect and disconnect
	/// @param callback - callback functor
	/// @return callback's ID. This id should be used to remove callback
	Expected<int64_t> AddConnectionStateObserver(ConnectionStateHandlerT callback) noexcept;
	/// Remove observer by it's ID
	/// @param id - observer's ID
	Error RemoveConnectionStateObserver(int64_t id) noexcept;

	/// Execute sharding control request during the sharding config change
	/// @param request - control params
	/// @param response - control response
	Error ShardingControlRequest(const sharding::ShardingControlRequestData& request,
								 sharding::ShardingControlResponseData& response) noexcept;

	/// Add cancelable context
	/// @param cancelCtx - context pointer
	CoroReindexer WithContext(const IRdxCancelContext* cancelCtx) const noexcept { return {impl_, ctx_.WithCancelContext(cancelCtx)}; }
	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	CoroReindexer WithTimeout(milliseconds timeout) const noexcept { return {impl_, ctx_.WithTimeout(timeout)}; }
	/// Add lsn info
	/// @param lsn - next operation lsn
	CoroReindexer WithLSN(lsn_t lsn) const noexcept { return {impl_, ctx_.WithLSN(lsn)}; }
	/// Add shard info
	/// @param id - shard id
	CoroReindexer WithShardId(int id, bool parallel) const noexcept { return {impl_, ctx_.WithShardId(id, parallel)}; }
	/// Add emitter server id
	/// @param serverId - emitter server id
	CoroReindexer WithEmitterServerId(int serverId) const noexcept { return {impl_, ctx_.WithEmitterServerId(serverId)}; }

	typedef CoroQueryResults QueryResultsT;
	typedef Item ItemT;
	typedef ReindexerConfig ConfigT;
	typedef CoroTransaction TransactionT;
	typedef ConnectOpts ConnectOptsT;

private:
	CoroReindexer(RPCClient* impl, InternalRdxContext&& ctx) noexcept : impl_(impl), ctx_(std::move(ctx)) {}
	RPCClient* impl_ = nullptr;
	bool owner_ = false;
	InternalRdxContext ctx_;
};

}  // namespace client
}  // namespace reindexer
