#pragma once

#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"
#include "core/rdxcontext.h"
#include "core/transaction.h"

#include <chrono>

namespace reindexer {
using std::vector;
using std::string;
using std::chrono::milliseconds;

class ReindexerImpl;
class IUpdatesObserver;

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
	Reindexer();
	/// Destrory Reindexer database object
	~Reindexer();
	/// Create not holding copy
	Reindexer(const Reindexer &) noexcept;
	Reindexer(Reindexer &&) noexcept;
	Reindexer &operator=(const Reindexer &) = delete;
	Reindexer &operator=(Reindexer &&) = delete;

	/// Connect - connect to reindexer database in embeded mode
	/// Cancelation context doesn't affect this call
	/// @param dsn - uri of database, like: `builtin:///var/lib/reindexer/dbname` or just `/var/lib/reindexer/dbname`
	/// @param opts - Connect options. May contaion any of <br>
	/// ConnectOpts::AllowNamespaceErrors() - true: Ignore errors during existing NS's load; false: Return error occured during NS's load
	/// ConnectOpts::OpenNamespaces() - true: Need to open all the namespaces; false: Don't open namespaces
	Error Connect(const string &dsn, ConnectOpts opts = ConnectOpts());

	/// Enable storage. Must be called before InitSystemNamespaces
	/// @param storagePath - file system path to database storage
	/// @param skipPlaceholderCheck - If set, then reindexer will not check folder for placeholder
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false);

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
	/// Delete all items from namespace
	/// @param nsName - Name of namespace
	Error TruncateNamespace(string_view nsName);
	/// Rename namespace. If namespace with dstNsName exists, then it is replaced.
	/// @param srcNsName  - Name of namespace
	/// @param dstNsName  - desired name of namespace
	Error RenameNamespace(string_view srcNsName, const std::string &dstNsName);
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
	/// @param opts - Enumeration options
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Insert(string_view nsName, Item &item);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// May be used with completion
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Update(string_view nsName, Item &item);
	/// Updates all items in namespace, that satisfy provided query
	/// @param query - Query to define items set for update.
	/// @param result - QueryResults with IDs of deleted items.
	Error Update(const Query &query, QueryResults &result);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// May be used with completion
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	Error Upsert(string_view nsName, Item &item);
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
	/// Cancelation context doesn't affect this call
	/// @param nsName - Name of namespace
	Error Commit(string_view nsName);
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and futher Upsert/Insert/Delete/Update call
	Item NewItem(string_view nsName);
	/// Allocate new transaction for namespace
	/// Cancelation context doesn't affect this call
	/// @param nsName - Name of namespace
	Transaction NewTransaction(string_view nsName);
	/// Commit transaction - transaction will be deleted after commit
	/// @param tr - transaction to commit
	/// @param result - QueryResults with IDs of changed by tx items.
	Error CommitTransaction(Transaction &tr, QueryResults &result);
	/// RollBack transaction - transaction will be deleted after rollback
	/// Cancelation context doesn't affect this call
	/// @param tr - transaction to rollback
	Error RollBackTransaction(Transaction &tr);
	/// Get meta data from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with meta data
	Error GetMeta(string_view nsName, const string &key, string &data);
	/// Put meta data to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with meta data
	Error PutMeta(string_view nsName, const string &key, string_view data);
	/// Get list of all meta data keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(string_view nsName, vector<string> &keys);
	/// Get possible suggestions for token (set by 'pos') in Sql query.
	/// Cancelation context doesn't affect this call
	/// @param sqlQuery - sql query.
	/// @param pos - position in sql query for suggestions.
	/// @param suggestions - all the suggestions for 'pos' position in query.
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions);
	/// Get curret connection status
	Error Status();

	/// Init system namepaces, and load config from config namespace
	/// Cancelation context doesn't affect this call
	Error InitSystemNamespaces();

	/// Subscribe to updates of database
	/// Cancelation context doesn't affect this call
	/// @param observer - Observer interface, which will receive updates
	/// @param subscribe - true: subscribe, false: unsubscribe
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);

	/// Add cancelable context
	/// @param ctx - context pointer
	Reindexer WithContext(const IRdxCancelContext *ctx) const { return Reindexer(impl_, ctx_.WithCancelParent(ctx)); }
	/// Add execution timeout to the next query
	/// @param timeout - Execution timeout
	Reindexer WithTimeout(milliseconds timeout) const { return Reindexer(impl_, ctx_.WithTimeout(timeout)); }
	/// Add completion
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Reindexer WithCompletion(Completion cmpl) const { return Reindexer(impl_, ctx_.WithCompletion(cmpl)); }
	/// Add activityTracer
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	Reindexer WithActivityTracer(string_view activityTracer, string_view user) const {
		return Reindexer(impl_, ctx_.WithActivityTracer(activityTracer, user));
	}
	/// Set activityTracer to current DB
	/// @param activityTracer - name of activity tracer
	/// @param user - user identifying information
	void SetActivityTracer(string_view activityTracer, string_view user) { ctx_.SetActivityTracer(activityTracer, user); }
	bool NeedTraceActivity() const;

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;

private:
	Reindexer(ReindexerImpl *impl, InternalRdxContext &&ctx) : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}

	ReindexerImpl *impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace reindexer
