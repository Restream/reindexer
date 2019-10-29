#pragma once

#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/queryresults.h"
#include "client/reindexerconfig.h"
#include "core/namespacedef.h"
#include "core/query/query.h"

#include <chrono>

namespace reindexer {
class IUpdatesObserver;

namespace client {
using std::vector;
using std::string;
using std::chrono::milliseconds;

class RPCClient;

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
	typedef std::function<void(const Error &err)> Completion;

	/// Create Reindexer database object
	Reindexer(const ReindexerConfig & = ReindexerConfig());
	/// Destrory Reindexer database object
	~Reindexer();
	Reindexer(const Reindexer &) = delete;
	Reindexer(Reindexer &&) noexcept;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname`
	Error Connect(const string &dsn);
	/// Stop - shutdown connector
	Error Stop();
	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @return errOK - On success
	Error OpenNamespace(string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	Error AddNamespace(const NamespaceDef &nsDef);
	/// Close namespace. Will free all memory resorces, associated with namespace. Forces sync changes to disk
	/// @param nsName - Name of namespace
	Error CloseNamespace(string_view nsName);
	/// Drop namespace. Will free all memory resorces, associated with namespace and erase all files from disk
	/// @param nsName - Name of namespace
	Error DropNamespace(string_view nsName);
	/// Delete all items in namespace
	/// @param nsName - Name of namespace
	Error TruncateNamespace(string_view nsName);
	/// Add index to namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error AddIndex(string_view nsName, const IndexDef &index);
	/// Update index in namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error UpdateIndex(string_view nsName, const IndexDef &index);
	/// Drop index from namespace
	/// @param nsName - Name of namespace
	/// @param index - index name
	Error DropIndex(string_view nsName, const IndexDef &index);
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaves
	/// @param bEnumAll - Also include currenty not opened, but exists on disk namespaces
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);
	/// Gets a list of available databases for a certain server.
	/// @param dbList - list of DB names
	Error EnumDatabases(vector<string> &dbList);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(string_view nsName, Item &item);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(string_view nsName, Item &item);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(string_view nsName, Item &item);
	/// Updates all items in namespace, that satisfy provided query.
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of deleted items.
	Error Update(const Query &query, QueryResults &result);
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(string_view nsName, Item &item);
	/// Delete all items froms namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query &query, QueryResults &result);
	/// Execute SQL Query and return results
	/// May be used with completion
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	Error Select(string_view query, QueryResults &result);
	/// Execute Query and return results
	/// May be used with completion
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	Error Select(const Query &query, QueryResults &result);
	/// Flush changes to storage
	/// @param nsName - Name of namespace
	Error Commit(string_view nsName);
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and futher Upsert/Insert/Delete/Update call
	Item NewItem(string_view nsName);
	/// Get meta data from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with meta data
	Error GetMeta(string_view nsName, const string &key, string &data);
	/// Put meta data to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with meta data
	Error PutMeta(string_view nsName, const string &key, const string_view &data);
	/// Get list of all meta data keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(string_view nsName, vector<string> &keys);
	/// Subscribe to updates of database
	/// @param observer - Observer interface, which will receive updates
	/// @param subscribe - true: subscribe, false: unsubscribe
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions);

	/// Add cancelable context
	/// @param cancelCtx - context pointer
	Reindexer WithContext(const IRdxCancelContext *cancelCtx) { return Reindexer(impl_, ctx_.WithCancelContext(cancelCtx)); }

	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	Reindexer WithTimeout(milliseconds timeout) { return Reindexer(impl_, ctx_.WithTimeout(timeout)); }
	/// Add completion
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Reindexer WithCompletion(Completion cmpl) { return Reindexer(impl_, ctx_.WithCompletion(cmpl)); }

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;

private:
	Reindexer(RPCClient *impl, InternalRdxContext &&ctx) : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}

	RPCClient *impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace client
}  // namespace reindexer
