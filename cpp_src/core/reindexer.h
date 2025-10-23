#pragma once

#include <chrono>
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"
#include "core/rdxcontext.h"
#include "core/reindexerconfig.h"
#include "core/shardedmeta.h"
#include "core/transaction/transaction.h"

namespace reindexer {
using std::chrono::milliseconds;

class ShardingProxy;
struct ReplicationStateV2;
struct ClusterOperationStatus;
class SnapshotChunk;
class Snapshot;
struct SnapshotOpts;
struct ClusterControlRequestData;
class IEventsObserver;		  // TODO: Make this class accessible to the external user #1714
class EventSubscriberConfig;  // TODO: Make this class accessible to the external user #1714

namespace cluster {
struct NodeData;
struct RaftInfo;
}  // namespace cluster

namespace sharding {
struct ShardingControlRequestData;
struct ShardingControlResponseData;
}  // namespace sharding

/// The main Reindexer interface. Holds database object<br>
/// *Thread safety*: All methods of Reindexer are thread safe. <br>
/// *Resources lifetime*: All resources acquired from Reindexer, e.g Item or QueryResults are uses Copy-On-Write
/// semantics, and have independent lifetime. Also them are thread safe against Reindexer<br>
/// *Fork behavior*: Reindexer creates few service threads: for data flushing and normalization in background.
/// If application calls `fork` after using Reindexer, then resources associated with that threads will leak
/// Therefore it is strongly recommended, *do not* call fork after Reindexer creation.
class [[nodiscard]] Reindexer {
public:
	/// Completion routine
	using Completion = std::function<void(const Error& err)>;
	using ItemType = Item;
	using TransactionType = Transaction;

	/// Create Reindexer database object
	/// @param cfg - general database options
	explicit Reindexer(ReindexerConfig cfg = ReindexerConfig());

	/// Destroy Reindexer database object
	~Reindexer();
	/// Create not holding copy
	Reindexer(const Reindexer&) noexcept;
	Reindexer(Reindexer&&) noexcept;
	Reindexer& operator=(const Reindexer&) = delete;
	Reindexer& operator=(Reindexer&&) = delete;

	/// Connect - connect to reindexer database in embedded mode. It has to be called before most other calls.
	/// Cancellation context doesn't affect this call
	/// First Connect is NOT THREAD-SAFE. I.e. it must be called before any other DB's methods.
	/// If Connect has succeed, any subsequent connects (even with different DSN) will not take any effect.
	/// @param dsn - uri of database, like: `builtin:///var/lib/reindexer/dbname` or just `/var/lib/reindexer/dbname`.
	/// Use `builtin://` for no-storage setup
	/// @param opts - Connect options. May contain any of <br>
	/// ConnectOpts::AllowNamespaceErrors() - true: Ignore errors during existing NS's load; false: Return error occurred during NS's load
	/// ConnectOpts::OpenNamespaces() - true: Need to open all the namespaces; false: Don't open namespaces
	Error Connect(const std::string& dsn, ConnectOpts opts = ConnectOpts()) noexcept;

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
	/// Delete all items from namespace
	/// @param nsName - Name of namespace
	Error TruncateNamespace(std::string_view nsName) noexcept;
	/// Rename namespace. If namespace with dstNsName exists, then it is replaced.
	/// @param srcNsName  - Name of namespace
	/// @param dstNsName  - desired name of namespace
	Error RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) noexcept;
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
	/// Get fields schema for namespace
	/// @param nsName - Name of namespace
	/// @param format - type of Schema: JSON or Protobuf
	/// @param schema - text representation of schema
	Error GetSchema(std::string_view nsName, int format, std::string& schema) noexcept;
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaces
	/// @param opts - Enumeration options
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) noexcept;
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(std::string_view nsName, Item& item) noexcept;
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success inserted item will be added to the result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with inserted item.
	Error Insert(std::string_view nsName, Item& item, QueryResults& result) noexcept;
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(std::string_view nsName, Item& item) noexcept;
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success updated item will be added to result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with updated item.
	Error Update(std::string_view nsName, Item& item, QueryResults& result) noexcept;
	/// Updates all items in namespace, that satisfy provided query
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of updated items.
	Error Update(const Query& query, QueryResults& result) noexcept;
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(std::string_view nsName, Item& item) noexcept;
	/// Update or Insert Item in namespace.
	/// On success upserted item will be added to result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with upserted item.
	Error Upsert(std::string_view nsName, Item& item, QueryResults& result) noexcept;
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(std::string_view nsName, Item& item) noexcept;
	/// Delete Item from namespace.
	/// On success deleted item will be added to result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with deleted item.
	Error Delete(std::string_view nsName, Item& item, QueryResults& result) noexcept;
	/// Delete all items from namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query& query, QueryResults& result) noexcept;
	/// Execute SQL Query and return results
	/// May be used with completion
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	/// @param proxyFetchLimit - Fetch limit for proxied query
	Error ExecSQL(std::string_view query, QueryResults& result, unsigned proxyFetchLimit = 10000) noexcept;
	/// Execute Query and return results
	/// May be used with completion
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	/// @param proxyFetchLimit - Fetch limit for proxied query
	Error Select(const Query& query, QueryResults& result, unsigned proxyFetchLimit = 10000) noexcept;
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and further Upsert/Insert/Delete/Update call
	Item NewItem(std::string_view nsName) noexcept;
	/// Allocate new transaction for namespace
	/// Cancellation context doesn't affect this call
	/// @param nsName - Name of namespace
	Transaction NewTransaction(std::string_view nsName) noexcept;
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	/// @param result - QueryResults with IDs of items changed by tx.
	Error CommitTransaction(Transaction& tr, QueryResults& result) noexcept;
	/// RollBack transaction - transaction will be deleted after rollback
	/// Cancellation context doesn't affect this call
	/// @param tr - transaction to rollback
	Error RollBackTransaction(Transaction& tr) noexcept;
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
	/// Put metadata to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with metadata
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data) noexcept;
	/// Get list of all metadata keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys) noexcept;
	/// Delete metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	Error DeleteMeta(std::string_view nsName, const std::string& key) noexcept;
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// Cancellation context doesn't affect this call
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions) noexcept;
	/// Get current connection status
	Error Status() noexcept;
	/// Get version of the Reindexer
	Error Version(std::string&) const noexcept;
	/// Builds Protobuf schema in ser.
	/// @param ser - schema output buffer
	/// @param namespaces - list of namespaces to be embedded in .proto
	Error GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces) noexcept;
	/// Get namespace's replication state
	/// @param nsName - Name of namespace
	/// @param state - result state
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state) noexcept;
	/// Set namespace's cluster operation status
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

	/// Suggest new leader info to cluster node
	/// @param suggestion - suggested elections data
	/// @param response - local elections data
	Error SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) noexcept;
	/// Ping RAFT node, to notify it about cluster leader status
	/// @param data - elections data
	Error LeadersPing(const cluster::NodeData& data) noexcept;
	/// Get RAFT information about rx node
	/// @param info - result RAFT info (out param)
	Error GetRaftInfo(cluster::RaftInfo& info) noexcept;
	/// Execute cluster control request
	/// @param request - control params
	Error ClusterControlRequest(const ClusterControlRequestData& request) noexcept;
	/// Set tags matcher for replica namespace. Current tagsmatcher and new tagsmatcher have to have the same state tokens.
	/// This request is for replicator only.
	/// @param nsName - Name of namespace
	/// @param tm - New tagsmatcher
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm) noexcept;

	/// Execute sharding control request
	/// @param request - control params
	/// @param response - control response
	Error ShardingControlRequest(const sharding::ShardingControlRequestData& request,
								 sharding::ShardingControlResponseData& response) noexcept;

	/// Add cancelable context
	/// @param ctx - context pointer
	Reindexer WithContext(const IRdxCancelContext* ctx) const { return {impl_, ctx_.WithCancelParent(ctx)}; }
	/// Add execution timeout to the next query
	/// @param timeout - Execution timeout
	Reindexer WithTimeout(milliseconds timeout) const { return {impl_, ctx_.WithTimeout(timeout)}; }
	/// Add lsn info
	/// @param lsn - next operation lsn
	Reindexer WithLSN(lsn_t lsn) const { return {impl_, ctx_.WithLSN(lsn)}; }
	/// Add emitter server id
	/// @param id - emitter server id
	Reindexer WithEmitterServerId(unsigned int id) const { return {impl_, ctx_.WithEmitterServerId(id)}; }
	/// Add shard id
	/// @param id - shard id
	/// @param distributed - 'true' means, that we are executing distributed sharding query part
	Reindexer WithShardId(unsigned int id, bool distributed) const { return {impl_, ctx_.WithShardId(id, distributed)}; }
	/// Add completion
	/// @param cmpl - Optional async completion routine. If nullptr function will work synchronous
	Reindexer WithCompletion(Completion cmpl) const { return {impl_, ctx_.WithCompletion(std::move(cmpl))}; }
	/// Add activityTracer
	/// This With* should be the last one, because it creates strings, that will be copied on each next With* statement
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	/// @param connectionId - unique identifier for the connection
	Reindexer WithActivityTracer(std::string_view activityTracer, std::string&& user, int connectionId) const {
		return {impl_, ctx_.WithActivityTracer(activityTracer, std::move(user), connectionId)};
	}
	Reindexer WithActivityTracer(std::string_view activityTracer, std::string&& user) const {
		return {impl_, ctx_.WithActivityTracer(activityTracer, std::move(user))};
	}
	/// Allows to set multiple context params at once
	/// @param timeout - Execution timeout
	/// @param lsn - origin LSN value (required for replicated requests)
	/// @param emitterServerId - server ID of the emitter node (required for synchronously replicated requests)
	/// @param shardId - expected shard ID for this node (non empty for proxied sharding requests)
	/// @param distributed - 'true' means, that we are executing distributed sharding query part
	/// @param replToken - replication token of current node
	/// @param needMaskingDSN - flag for masking user credentials in DSNs when requesting config data
	Reindexer WithContextParams(milliseconds timeout, lsn_t lsn, int emitterServerId, int shardId, bool distributed, key_string replToken,
								NeedMaskingDSN needMaskingDSN) const {
		return {impl_, ctx_.WithContextParams(timeout, lsn, emitterServerId, shardId, distributed, std::move(replToken), needMaskingDSN)};
	}
	/// Allows to set multiple context params at once
	/// @param timeout - Execution timeout
	/// @param lsn - origin LSN value (required for replicated requests)
	/// @param emitterServerId - server ID of the emitter node (required for synchronously replicated requests)
	/// @param shardId - expected shard ID for this node (non empty for proxied sharding requests)
	/// @param distributed - 'true' means, that we are executing distributed sharding query part
	/// @param replToken - replication token of current node
	/// @param needMaskingDSN - flag for masking user credentials in DSNs when requesting config data
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	/// @param connectionId - unique identifier for the connection
	Reindexer WithContextParams(milliseconds timeout, lsn_t lsn, int emitterServerId, int shardId, bool distributed, key_string replToken,
								NeedMaskingDSN needMaskingDSN, std::string_view activityTracer, std::string user, int connectionId) const {
		return {impl_, ctx_.WithContextParams(timeout, lsn, emitterServerId, shardId, distributed, std::move(replToken), needMaskingDSN,
											  activityTracer, std::move(user), connectionId)};
	}
	/// Allows to set multiple context params at once
	/// @param timeout - Execution timeout
	///  @param needMaskingDSN - flag for masking user credentials in DSNs when requesting config data
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	Reindexer WithContextParams(milliseconds timeout, NeedMaskingDSN needMaskingDSN, std::string_view activityTracer,
								std::string user) const {
		return {impl_, ctx_.WithContextParams(timeout, needMaskingDSN, activityTracer, std::move(user))};
	}

	/// Set activityTracer to current DB
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	/// @param connectionId - unique identifier for the connection
	void SetActivityTracer(std::string&& activityTracer, std::string&& user, int connectionId) noexcept {
		ctx_.SetActivityTracer(std::move(activityTracer), std::move(user), connectionId);
	}
	void SetActivityTracer(std::string&& activityTracer, std::string&& user) noexcept {
		ctx_.SetActivityTracer(std::move(activityTracer), std::move(user));
	}

	Error ShutdownCluster() noexcept;

	bool NeedTraceActivity() const noexcept;

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;
	typedef Transaction TransactionT;
	typedef ReindexerConfig ConfigT;
	typedef ConnectOpts ConnectOptsT;

	Error DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index) noexcept;

	/// Subscribe to updates of database
	/// Cancelation context doesn't affect this call
	/// @param observer - Observer interface, which will receive updates
	/// @param cfg - Subscription config
	/// Each subscriber may have multiple event streams. Reindexer will create streamsMask for each event to route it to the proper stream.
	Error SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg) noexcept;
	/// Unsubscribe from updates of database
	/// Cancelation context doesn't affect this call
	/// @param observer - Observer interface, which will be unsubscribed updates
	Error UnsubscribeUpdates(IEventsObserver& observer) noexcept;

private:
	Reindexer(ShardingProxy* impl, InternalRdxContext&& ctx) noexcept : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}

	template <typename FnT, typename T = typename std::invoke_result<FnT>::type>
	T callWithConnectCheck(FnT&& f) noexcept;

	ShardingProxy* impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace reindexer
