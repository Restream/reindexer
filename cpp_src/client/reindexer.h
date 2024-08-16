#pragma once

#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/queryresults.h"
#include "client/reindexerconfig.h"
#include "client/transaction.h"
#include "core/namespacedef.h"
#include "core/query/query.h"

#include <chrono>

namespace reindexer {
class IUpdatesObserver;
class UpdatesFilters;

namespace client {
using std::chrono::milliseconds;

class RPCClient;

/// The main Reindexer interface. Holds database object<br>
/// *Thread safety*: All methods of Reindexer are thread safe. <br>
/// *Resources lifetime*: All resources acquired from Reindexer, e.g Item or QueryResults are uses Copy-On-Write
/// semantics, and have independent lifetime. Also them are thread safe against Reindexer<br>
/// *Fork behavior*: Reindexer creates few service threads: for data flushing and normalization in background.
/// If application calls `fork` after using Reindexer, then resources associated with that threads will leak
/// Therefore it is strongly recommended, *do not* call fork after Reindexer creation.
class Reindexer {
public:
	/// Completion routine
	typedef std::function<void(const Error& err)> Completion;

	/// Create Reindexer database object
	Reindexer(const ReindexerConfig& = ReindexerConfig());
	/// Destroy Reindexer database object
	~Reindexer();
	Reindexer(const Reindexer&) = delete;
	Reindexer(Reindexer&&) noexcept;
	Reindexer& operator=(const Reindexer&) = delete;
	Reindexer& operator=(Reindexer&&) noexcept;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname` or
	/// `ucproto://user@password:/tmp/reindexer.sock:/dbname`
	/// @param opts - Connect options. May contain any of <br>
	Error Connect(const std::string& dsn, const client::ConnectOpts& opts = client::ConnectOpts());
	/// Connect - connect to reindexer server
	/// @param connectData - list of server dsn + it's ConnectOpts
	Error Connect(const std::vector<std::pair<std::string, client::ConnectOpts>>& connectData);
	/// Stop - shutdown connector
	void Stop();
	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @return errOK - On success
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts = StorageOpts().Enabled().CreateIfMissing());
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	Error AddNamespace(const NamespaceDef& nsDef);
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
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaces
	/// @param opts - Enumeration options
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts);
	/// Gets a list of available databases for a certain server.
	/// @param dbList - list of DB names
	Error EnumDatabases(std::vector<std::string>& dbList);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(std::string_view nsName, Item& item);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(std::string_view nsName, Item& item);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(std::string_view nsName, Item& item);
	/// Updates all items in namespace, that satisfy provided query.
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of deleted items.
	Error Update(const Query& query, QueryResults& result);
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Delete(std::string_view nsName, Item& item);
	/// Delete all items from namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query& query, QueryResults& result);
	/// Execute SQL Query and return results
	/// May be used with completion
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	Error Select(std::string_view query, QueryResults& result);
	/// Execute Query and return results
	/// May be used with completion
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	Error Select(const Query& query, QueryResults& result);
	/// Flush changes to storage
	/// @param nsName - Name of namespace
	Error Commit(std::string_view nsName);
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and further Upsert/Insert/Delete/Update call
	Item NewItem(std::string_view nsName);
	/// Get metadata from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with metadata
	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data);
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
	/// Subscribe to updates of database
	/// @param observer - Observer interface, which will receive updates
	/// @param filters - Subscription filters set
	/// @param opts - Subscription options (allows to either add new filters or reset them)
	Error SubscribeUpdates(IUpdatesObserver* observer, const UpdatesFilters& filters, SubscriptionOpts opts = SubscriptionOpts());
	/// Unsubscribe from updates of database
	/// Cancellation context doesn't affect this call
	/// @param observer - Observer interface, which will be unsubscribed updates
	Error UnsubscribeUpdates(IUpdatesObserver* observer);
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions);
	/// Get current connection status
	Error Status();
	/// Allocate new transaction for namespace
	/// @param nsName - Name of namespace
	Transaction NewTransaction(std::string_view nsName);
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	Error CommitTransaction(Transaction& tr);
	/// RollBack transaction - transaction will be deleted after rollback
	/// @param tr - transaction to rollback
	Error RollBackTransaction(Transaction& tr);

	/// Add cancelable context
	/// @param cancelCtx - context pointer
	Reindexer WithContext(const IRdxCancelContext* cancelCtx) { return Reindexer(impl_, ctx_.WithCancelContext(cancelCtx)); }

	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	Reindexer WithTimeout(milliseconds timeout) { return Reindexer(impl_, ctx_.WithTimeout(timeout)); }
	/// Add completion
	/// @param cmpl - Optional async completion routine. If nullptr function will work synchronously
	Reindexer WithCompletion(Completion cmpl) { return Reindexer(impl_, ctx_.WithCompletion(std::move(cmpl))); }

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;

private:
	Reindexer(RPCClient* impl, InternalRdxContext&& ctx) : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}
	RPCClient* impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace client
}  // namespace reindexer
