#pragma once

#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"
#include "core/rdxcontext.h"
#include "core/reindexerconfig.h"
#include "core/shardedmeta.h"
#include "core/transaction/transaction.h"

#include <chrono>

namespace reindexer {
using std::chrono::milliseconds;

class ShardingProxy;
class ProtobufSchema;
struct ReplicationStateV2;
struct ClusterizationStatus;
class SnapshotChunk;
class Snapshot;
struct SnapshotOpts;
struct ClusterControlRequestData;

// REINDEX_WITH_V3_FOLLOWERS
class IUpdatesObserver;
class UpdatesFilters;

namespace cluster {
struct NodeData;
struct RaftInfo;
}  // namespace cluster

namespace sharding {
struct ShardingControlRequestData;
}

/// The main Reindexer interface. Holds database object<br>
/// *Thread safety*: All methods of Reindexer are thread safe. <br>
/// *Resources lifetime*: All resources aquired from Reindexer, e.g Item or QueryResults are uses Copy-On-Write
/// semantics, and have independent lifetime. Also them are thread safe against Reindexer<br>
/// *Fork behavior*: Reindexer creates few service threads: for data flushing and normalization in background.
/// If application calls `fork` after using Reindexer, then resources associated with that threads will leak
/// Therefore it is strongly recommended, *do not* call fork after Reindexer creation.
class Reindexer {
public:
	/// Completion routine
	using Completion = std::function<void(const Error &err)>;

	/// Create Reindexer database object
	/// @param cfg - general database options
	Reindexer(ReindexerConfig cfg = ReindexerConfig());
	/// Destrory Reindexer database object
	~Reindexer();
	/// Create not holding copy
	Reindexer(const Reindexer &) noexcept;
	Reindexer(Reindexer &&) noexcept;
	Reindexer &operator=(const Reindexer &) = delete;
	Reindexer &operator=(Reindexer &&) = delete;

	/// Connect - connect to reindexer database in embeded mode
	/// Cancelation context doesn't affect this call
	/// First Connect is NOT THREAD-SAFE. I.e. it must be called before any other DB's methods.
	/// If Connect has succeed, any subsequent connects (even with different DSN) will not take any effect.
	/// @param dsn - uri of database, like: `builtin:///var/lib/reindexer/dbname` or just `/var/lib/reindexer/dbname`
	/// @param opts - Connect options. May contaion any of <br>
	/// ConnectOpts::AllowNamespaceErrors() - true: Ignore errors during existing NS's load; false: Return error occured during NS's load
	/// ConnectOpts::OpenNamespaces() - true: Need to open all the namespaces; false: Don't open namespaces
	Error Connect(const std::string &dsn, ConnectOpts opts = ConnectOpts());

	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @param replOpts - Namespace replication options (for replication purposes, should not be user elsewhere)
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts &replOpts = NsReplicationOpts());
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	/// @param replOpts - Namespace replication options (for replication purposes, should not be user elsewhere)
	Error AddNamespace(const NamespaceDef &nsDef, const NsReplicationOpts &replOpts = NsReplicationOpts());
	/// Close namespace. Will free all memory resorces, associated with namespace. Forces sync changes to disk
	/// @param nsName - Name of namespace
	Error CloseNamespace(std::string_view nsName);
	/// Drop namespace. Will free all memory resorces, associated with namespace and erase all files from disk
	/// @param nsName - Name of namespace
	Error DropNamespace(std::string_view nsName);
	/// Create new temporary namespace with randomized name
	/// Temporary namespace will be automatically removed on next startup
	/// @param baseName - base name, which will be used in result name
	/// @param resultName - name of created namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// @param version - Namespace version. Should be left empty for leader reindexer instances
	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts = StorageOpts().Enabled(),
								   lsn_t version = lsn_t());
	/// Delete all items from namespace
	/// @param nsName - Name of namespace
	Error TruncateNamespace(std::string_view nsName);
	/// Rename namespace. If namespace with dstNsName exists, then it is replaced.
	/// @param srcNsName  - Name of namespace
	/// @param dstNsName  - desired name of namespace
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName);
	/// Add index to namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error AddIndex(std::string_view nsName, const IndexDef &index);
	/// Update index in namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error UpdateIndex(std::string_view nsName, const IndexDef &index);
	/// Drop index from namespace
	/// @param nsName - Name of namespace
	/// @param index - index name
	Error DropIndex(std::string_view nsName, const IndexDef &index);
	/// Set fields schema for namespace
	/// @param nsName - Name of namespace
	/// @param schema - JSON in JsonSchema format
	Error SetSchema(std::string_view nsName, std::string_view schema);
	/// Get fields schema for namespace
	/// @param nsName - Name of namespace
	/// @param format - type of Schema: JSON or Protobuf
	/// @param schema - text representation of schema
	Error GetSchema(std::string_view nsName, int format, std::string &schema);
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaves
	/// @param opts - Enumeration options
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(std::string_view nsName, Item &item);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success inserted item will be added to the result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with inserted item.
	Error Insert(std::string_view nsName, Item &item, QueryResults &result);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(std::string_view nsName, Item &item);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success updated item will be added to result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with updated item.
	Error Update(std::string_view nsName, Item &item, QueryResults &result);
	/// Updates all items in namespace, that satisfy provided query
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of updated items.
	Error Update(const Query &query, QueryResults &result);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(std::string_view nsName, Item &item);
	/// Update or Insert Item in namespace.
	/// On success upserted item will be added to result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with upserted item.
	Error Upsert(std::string_view nsName, Item &item, QueryResults &result);
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(std::string_view nsName, Item &item);
	/// Delete Item from namespace.
	/// On success deleted item will be added to result and item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with deleted item.
	Error Delete(std::string_view nsName, Item &item, QueryResults &result);
	/// Delete all items froms namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query &query, QueryResults &result);
	/// Execute SQL Query and return results
	/// May be used with completion
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	/// @param proxyFetchLimit - Fetch limit for proxied query
	Error Select(std::string_view query, QueryResults &result, unsigned proxyFetchLimit = 10000);
	/// Execute Query and return results
	/// May be used with completion
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	/// @param proxyFetchLimit - Fetch limit for proxied query
	Error Select(const Query &query, QueryResults &result, unsigned proxyFetchLimit = 10000);
	/// Flush changes to storage
	/// Cancelation context doesn't affect this call
	/// @param nsName - Name of namespace
	Error Commit(std::string_view nsName);
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and futher Upsert/Insert/Delete/Update call
	Item NewItem(std::string_view nsName);
	/// Allocate new transaction for namespace
	/// Cancelation context doesn't affect this call
	/// @param nsName - Name of namespace
	Transaction NewTransaction(std::string_view nsName);
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	/// @param result - QueryResults with IDs of items changed by tx.
	Error CommitTransaction(Transaction &tr, QueryResults &result);
	/// RollBack transaction - transaction will be deleted after rollback
	/// Cancelation context doesn't affect this call
	/// @param tr - transaction to rollback
	Error RollBackTransaction(Transaction &tr);
	/// Get meta data from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with meta data
	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data);
	/// Get sharded meta data from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output vector with meta data from different shards
	Error GetMeta(std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data);
	/// Put meta data to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with meta data
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data);
	/// Get list of all meta data keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys);
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// Cancelation context doesn't affect this call
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions);
	/// Get curret connection status
	Error Status() noexcept;

	/// Init system namepaces, and load config from config namespace
	/// Cancelation context doesn't affect this call
	Error InitSystemNamespaces();

	/// Builds Protobuf schema in ser.
	/// @param ser - schema output buffer
	/// @param namespaces - list of namespaces to be embedded in .proto
	Error GetProtobufSchema(WrSerializer &ser, std::vector<std::string> &namespaces);
	/// Get namespace's replication state
	/// @param nsName - Name of namespace
	/// @param state - result state
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state);
	/// Set namespace's clusterization status
	/// @param nsName - Name of namespace
	/// @param status - new status
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status);
	/// Get namespace snapshot
	/// @param nsName - Name of namespace
	/// @param opts - Snapshot options:
	/// LSN value. Starting point for snapshot. If this LSN is still available, snapshot will contain only diff (wal records)
	/// MaxWALDepth. Max number of WAL-records in force-sync snapshot
	/// @param snapshot - Result snapshot
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot);
	/// Apply record from snapshot chunk
	/// @param nsName - Name of namespace
	/// @param ch - Snapshot chunk to apply
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch);

	/// Suggest new leader info to cluster node
	/// @param suggestion - suggested elections data
	/// @param response - local elections data
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	/// Ping RAFT node, to notify it about cluster leader status
	/// @param data - elections data
	Error LeadersPing(const cluster::NodeData &data);
	/// Get RAFT information about rx node
	/// @param info - result RAFT info (out param)
	Error GetRaftInfo(cluster::RaftInfo &info);
	/// Execute cluster control request
	/// @param request - control params
	Error ClusterControlRequest(const ClusterControlRequestData &request);
	/// Set tags matcher for replica namespace. Current tagsmatcher and new tagsmatcher have to have the same state tokens.
	/// This request is for replicator only.
	/// @param nsName - Name of namespace
	/// @param tm - New tagsmatcher
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm);

	/// Execute sharding control request
	/// @param request - control params
	[[nodiscard]] Error ShardingControlRequest(const sharding::ShardingControlRequestData &request) noexcept;

	/// Add cancelable context
	/// @param ctx - context pointer
	Reindexer WithContext(const IRdxCancelContext *ctx) const { return Reindexer(impl_, ctx_.WithCancelParent(ctx)); }
	/// Add execution timeout to the next query
	/// @param timeout - Execution timeout
	Reindexer WithTimeout(milliseconds timeout) const { return Reindexer(impl_, ctx_.WithTimeout(timeout)); }
	/// Add lsn info
	/// @param lsn - next operation lsn
	Reindexer WithLSN(lsn_t lsn) { return Reindexer(impl_, ctx_.WithLSN(lsn)); }
	/// Add emmiter server id
	/// @param id - emmiter server id
	Reindexer WithEmmiterServerId(unsigned int id) { return Reindexer(impl_, ctx_.WithEmmiterServerId(id)); }
	/// Add shard id
	/// @param id - shard id
	/// @param distributed - 'true' means, that we are executing distributed sharding query part
	Reindexer WithShardId(unsigned int id, bool distributed) { return Reindexer(impl_, ctx_.WithShardId(id, distributed)); }
	/// Add completion
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Reindexer WithCompletion(Completion cmpl) const { return Reindexer(impl_, ctx_.WithCompletion(std::move(cmpl))); }
	/// Add activityTracer
	/// This With* should be the last one, because it creates strings, that will be copied on each next With* statement
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	/// @param connectionId - unique identifier for the connection
	Reindexer WithActivityTracer(std::string_view activityTracer, std::string &&user, int connectionId) const {
		return Reindexer(impl_, ctx_.WithActivityTracer(activityTracer, std::move(user), connectionId));
	}
	Reindexer WithActivityTracer(std::string_view activityTracer, std::string &&user) const {
		return Reindexer(impl_, ctx_.WithActivityTracer(activityTracer, std::move(user)));
	}
	/// Allows to set multiple context params at once
	/// @param timeout - Execution timeout
	/// @param lsn - origin LSN value (required for replicated requests)
	/// @param emmiterServerId - server ID of the emmiter node (required for synchronously replicated requests)
	/// @param shardId - expected shard ID for this node (non empty for proxied sharding requests)
	/// @param distributed - 'true' means, that we are executing distributed sharding query part
	Reindexer WithContextParams(milliseconds timeout, lsn_t lsn, int emmiterServerId, int shardId, bool distributed) const {
		return Reindexer(impl_, ctx_.WithContextParams(timeout, lsn, emmiterServerId, shardId, distributed));
	}
	/// Allows to set multiple context params at once
	/// @param timeout - Execution timeout
	/// @param lsn - origin LSN value (required for replicated requests)
	/// @param emmiterServerId - server ID of the emmiter node (required for synchronously replicated requests)
	/// @param shardId - expected shard ID for this node (non empty for proxied sharding requests)
	/// @param distributed - 'true' means, that we are executing distributed sharding query part
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	/// @param connectionId - unique identifier for the connection
	Reindexer WithContextParams(milliseconds timeout, lsn_t lsn, int emmiterServerId, int shardId, bool distributed,
								std::string_view activityTracer, std::string user, int connectionId) const {
		return Reindexer(impl_, ctx_.WithContextParams(timeout, lsn, emmiterServerId, shardId, distributed, activityTracer, std::move(user),
													   connectionId));
	}
	/// Allows to set multiple context params at once
	/// @param timeout - Execution timeout
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	Reindexer WithContextParams(milliseconds timeout, std::string_view activityTracer, std::string user) const {
		return Reindexer(impl_, ctx_.WithContextParams(timeout, activityTracer, std::move(user)));
	}

	/// Set activityTracer to current DB
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	/// @param connectionId - unique identifier for the connection
	void SetActivityTracer(std::string &&activityTracer, std::string &&user, int connectionId) {
		ctx_.SetActivityTracer(std::move(activityTracer), std::move(user), connectionId);
	}
	void SetActivityTracer(std::string &&activityTracer, std::string &&user) {
		ctx_.SetActivityTracer(std::move(activityTracer), std::move(user));
	}

	void ShutdownCluster();

	bool NeedTraceActivity() const noexcept;

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;

	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index);

	/// REINDEX_WITH_V3_FOLLOWERS
	/// THIS METHOD IS TEMPORARY AND WILL BE REMOVED
	/// Subscribe to updates of database
	/// @param observer - Observer interface, which will receive updates
	/// @param filters - Subscription filters set
	/// @param opts - Subscription options (allows to either add new filters or reset them)
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts = SubscriptionOpts());
	/// THIS METHOD IS TEMPORARY AND WILL BE REMOVED
	/// Unsubscribe from updates of database
	/// Cancelation context doesn't affect this call
	/// @param observer - Observer interface, which will be unsubscribed updates
	Error UnsubscribeUpdates(IUpdatesObserver *observer);

private:
	Reindexer(ShardingProxy *impl, InternalRdxContext &&ctx) noexcept : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}

	ShardingProxy *impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace reindexer
