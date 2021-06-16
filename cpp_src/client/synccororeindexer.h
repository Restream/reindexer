#pragma once

#include "client/synccoroqueryresults.h"
#include "client/synccorotransaction.h"
#include "core/indexdef.h"
#include "core/namespacedef.h"
#include "internalrdxcontext.h"
#include "net/ev/ev.h"
#include "reindexerconfig.h"

namespace reindexer {
namespace client {

class SyncCoroReindexerImpl;

class SyncCoroReindexer {
public:
	/// Create Reindexer database object
	SyncCoroReindexer(const ReindexerConfig & = ReindexerConfig());
	SyncCoroReindexer(SyncCoroReindexer &&rdx) noexcept;
	/// Destrory Reindexer database object
	~SyncCoroReindexer();
	SyncCoroReindexer(const SyncCoroReindexer &) = delete;
	SyncCoroReindexer &operator=(const SyncCoroReindexer &) = delete;
	SyncCoroReindexer &operator=(SyncCoroReindexer &&rdx) noexcept;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname`
	/// @param opts - Connect options. May contaion any of <br>
	Error Connect(const std::string &dsn, const client::ConnectOpts &opts = client::ConnectOpts());
	/// Stop - shutdown connector
	Error Stop();
	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @return errOK - On success
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	Error AddNamespace(const NamespaceDef &nsDef);
	/// Close namespace. Will free all memory resorces, associated with namespace. Forces sync changes to disk
	/// @param nsName - Name of namespace
	Error CloseNamespace(std::string_view nsName);
	/// Drop namespace. Will free all memory resorces, associated with namespace and erase all files from disk
	/// @param nsName - Name of namespace
	Error DropNamespace(std::string_view nsName);
	/// Delete all items in namespace
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
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaves
	/// @param opts - Enumerartion options
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts);
	/// Gets a list of available databases for a certain server.
	/// @param dbList - list of DB names
	Error EnumDatabases(std::vector<std::string> &dbList);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(std::string_view nsName, Item &item);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(std::string_view nsName, Item &item);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(std::string_view nsName, Item &item);
	/// Updates all items in namespace, that satisfy provided query.
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of deleted items.
	Error Update(const Query &query, SyncCoroQueryResults &result);
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(std::string_view nsName, Item &item);
	/// Delete all items froms namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query &query, SyncCoroQueryResults &result);
	/// Execute SQL Query and return results
	/// May be used with completion
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	Error Select(std::string_view query, SyncCoroQueryResults &result);
	/// Execute Query and return resuchCommandlts
	/// May be used with completion
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	Error Select(const Query &query, SyncCoroQueryResults &result);

	/// Flush changes to storage
	/// @param nsName - Name of namespace
	Error Commit(std::string_view nsName);
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and futher Upsert/Insert/Delete/Update call
	Item NewItem(std::string_view nsName);
	/// Get meta data from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with meta data
	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data);
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
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(const std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions);
	/// Get curret connection status
	Error Status();
	/// Allocate new transaction for namespace
	/// @param nsName - Name of namespace
	SyncCoroTransaction NewTransaction(std::string_view nsName);
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	Error CommitTransaction(SyncCoroTransaction &tr);
	/// RollBack transaction - transaction will be deleted after rollback
	/// @param tr - transaction to rollback
	Error RollBackTransaction(SyncCoroTransaction &tr);

	/// Add cancelable context
	/// @param cancelCtx - context pointer
	SyncCoroReindexer WithContext(const IRdxCancelContext *cancelCtx) {
		return SyncCoroReindexer(impl_, ctx_.WithCancelContext(cancelCtx));
	}

	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	SyncCoroReindexer WithTimeout(milliseconds timeout) { return SyncCoroReindexer(impl_, ctx_.WithTimeout(timeout)); }

	typedef SyncCoroQueryResults QueryResultsT;
	typedef Item ItemT;

private:
	friend SyncCoroQueryResults;
	SyncCoroReindexer(SyncCoroReindexerImpl *impl, InternalRdxContext &&ctx) : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}
	SyncCoroReindexerImpl *impl_;
	bool owner_;
	InternalRdxContext ctx_;
};
}  // namespace client
}  // namespace reindexer
