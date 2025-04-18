#pragma once

#include "client/connectopts.h"
#include "client/queryresults.h"
#include "client/reindexerconfig.h"
#include "client/transaction.h"
#include "core/indexdef.h"
#include "core/namespacedef.h"
#include "core/shardedmeta.h"
#include "internalrdxcontext.h"

namespace reindexer {

struct ReplicationStateV2;
class DSN;

namespace client {

class ReindexerImpl;

class Reindexer {
public:
	/// Completion routine
	using Completion = std::function<void(const Error& err)>;

	/// Create Reindexer database object
	explicit Reindexer(const ReindexerConfig& = ReindexerConfig(), uint32_t connCount = 0, uint32_t threads = 0);
	Reindexer(Reindexer&& rdx) noexcept = default;
	/// Destroy Reindexer database object
	~Reindexer();
	Reindexer(const Reindexer&) = delete;
	Reindexer& operator=(const Reindexer&) = delete;
	Reindexer& operator=(Reindexer&& rdx) noexcept = default;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname` or
	/// `ucproto://user@password:/tmp/reindexer.sock:/dbname`
	/// @param opts - Connect options. May contain any of <br>
	Error Connect(const std::string& dsn, const client::ConnectOpts& opts = client::ConnectOpts());
	Error Connect(const DSN& dsn, const client::ConnectOpts& opts = client::ConnectOpts());
	/// Stop - shutdown connector
	void Stop();
	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @param replOpts - Namespace version. Should be left empty for leader reindexer instances
	/// @return errOK - On success
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts& replOpts = NsReplicationOpts());
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	/// @param replOpts - Namespace version. Should be left empty for leader reindexer instances
	Error AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts = NsReplicationOpts());
	/// Close namespace. Will free all memory resources, associated with namespace. Forces sync changes to disk
	/// @param nsName - Name of namespace
	Error CloseNamespace(std::string_view nsName);
	/// Drop namespace. Will free all memory resources, associated with namespace and erase all files from disk
	/// @param nsName - Name of namespace
	Error DropNamespace(std::string_view nsName);
	/// Delete all items in namespace
	/// @param nsName - Name of namespace
	Error TruncateNamespace(std::string_view nsName);
	/// Rename namespace. If namespace with dstNsName exists, then it is replaced.
	/// @param srcNsName  - Name of namespace
	/// @param dstNsName  - desired name of namespace
	Error RenameNamespace(std::string_view srcNsName, const std::string& dstNsName);
	/// Add index to namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error AddIndex(std::string_view nsName, const IndexDef& index);
	/// Update index in namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error UpdateIndex(std::string_view nsName, const IndexDef& index);
	/// Drop index from namespace
	/// @param nsName - Name of namespace
	/// @param index - index name
	Error DropIndex(std::string_view nsName, const IndexDef& index);
	/// Set fields schema for namespace
	/// @param nsName - Name of namespace
	/// @param schema - JSON in JsonSchema format
	Error SetSchema(std::string_view nsName, std::string_view schema);
	/// Get fields schema for namespace
	/// @param nsName - Name of namespace
	/// @param format - type of Schema: JSON or Protobuf
	/// @param schema - text representation of schema
	Error GetSchema(std::string_view nsName, int format, std::string& schema);
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaces
	/// @param opts - Enumeration options
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts);
	/// Gets a list of available databases for a certain server.
	/// @param dbList - list of DB names
	Error EnumDatabases(std::vector<std::string>& dbList);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(std::string_view nsName, Item& item);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success inserted item will be added to the result and item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with inserted item.
	Error Insert(std::string_view nsName, Item& item, QueryResults& result);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(std::string_view nsName, Item& item);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success updated item will be added to result and item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with updated item.
	Error Update(std::string_view nsName, Item& item, QueryResults& result);
	/// Updates all items in namespace, that satisfy provided query.
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of deleted items.
	Error Update(const Query& query, QueryResults& result);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(std::string_view nsName, Item& item);
	/// Update or Insert Item in namespace.
	/// On success upserted item will be added to result and item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with upserted item.
	Error Upsert(std::string_view nsName, Item& item, QueryResults& result);
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(std::string_view nsName, Item& item);
	/// Delete Item from namespace.
	/// On success deleted item will be added to result and item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param result - QueryResults with deleted item.
	Error Delete(std::string_view nsName, Item& item, QueryResults& result);
	/// Delete all items from namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query& query, QueryResults& result);
	/// Execute SQL Query and return results
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	Error Select(std::string_view query, QueryResults& result);
	/// Execute Query and return resuchCommandlts
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	Error Select(const Query& query, QueryResults& result);

	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and further Upsert/Insert/Delete/Update call
	Item NewItem(std::string_view nsName);
	/// Get metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with metadata
	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data);
	/// Get sharded metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output vector with metadata from different shards
	Error GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data);
	/// Put metadata to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with metadata
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data);
	/// Get list of all metadata keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys);
	/// Delete metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	Error DeleteMeta(std::string_view nsName, const std::string& key);
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions);
	/// Get current connection status
	/// WARNING: Status() request may call completion in current thread. Beware of possible deadlocks, using mutexes in this completion
	/// @param forceCheck - forces to check status immediately (otherwise result of periodic check will be returned)
	Error Status(bool forceCheck = false);
	/// Allocate new transaction for namespace
	/// @param nsName - Name of namespace
	Transaction NewTransaction(std::string_view nsName);
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	/// @param result - QueryResults with IDs of items changed by tx.
	Error CommitTransaction(Transaction& tr, QueryResults& result);
	/// RollBack transaction - transaction will be deleted after rollback
	/// @param tr - transaction to rollback
	Error RollBackTransaction(Transaction& tr);
	/// Get namespace's replication state
	/// @param nsName - Name of namespace
	/// @param state - result state
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state);

	/// Process new sharding config
	/// @param config - New sharding config
	/// @param sourceId - Unique identifier for operations with a specific config candidate
	Error SaveNewShardingConfig(std::string_view config, int64_t sourceId) noexcept;
	/// Resetting the old sharding config before applying the new one
	Error ResetOldShardingConfig(int64_t sourceId) noexcept;
	/// Resetting sharding config candidates if there were errors when saving of candidates on other nodes
	Error ResetShardingConfigCandidate(int64_t sourceId) noexcept;
	/// Rollback config candidate if there were errors when trying to apply candidates on other nodes
	Error RollbackShardingConfigCandidate(int64_t sourceId) noexcept;
	/// Apply new sharding config on all shards
	Error ApplyNewShardingConfig(int64_t sourceId) noexcept;

	/// Add cancelable context
	/// @param cancelCtx - context pointer
	Reindexer WithContext(const IRdxCancelContext* cancelCtx) { return {impl_, ctx_.WithCancelContext(cancelCtx)}; }
	/// Add async completion. It will be executed from internal connection thread
	/// (completion for Status() call may be executed from current thread)
	/// @param cmpl - completion callback
	Reindexer WithCompletion(Completion cmpl) { return {impl_, ctx_.WithCompletion(std::move(cmpl))}; }

	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	Reindexer WithTimeout(milliseconds timeout) { return {impl_, ctx_.WithTimeout(timeout)}; }
	Reindexer WithLSN(lsn_t lsn) { return {impl_, ctx_.WithLSN(lsn)}; }
	Reindexer WithEmmiterServerId(int sId) { return {impl_, ctx_.WithEmmiterServerId(sId)}; }
	Reindexer WithShardId(int id, bool parallel) { return {impl_, ctx_.WithShardId(id, parallel)}; }
	Reindexer WithShardingParallelExecution(bool parallel) { return {impl_, ctx_.WithShardingParallelExecution(parallel)}; }

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;
	typedef Transaction TransactionT;
	typedef ReindexerConfig ConfigT;

private:
	friend QueryResults;
	Reindexer(std::shared_ptr<ReindexerImpl> impl, InternalRdxContext&& ctx) noexcept : impl_(std::move(impl)), ctx_(std::move(ctx)) {}
	std::shared_ptr<ReindexerImpl> impl_;

	InternalRdxContext ctx_;
};
}  // namespace client
}  // namespace reindexer
