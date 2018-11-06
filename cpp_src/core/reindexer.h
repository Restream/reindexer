#pragma once

#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/query/queryresults.h"

namespace reindexer {
using std::vector;
using std::string;

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
	typedef std::function<void(const Error &err)> Completion;

	/// Create Reindexer database object
	Reindexer();
	/// Destrory Reindexer database object
	~Reindexer();
	Reindexer(const Reindexer &) = delete;

	/// Connect - connect to reindexer database in embeded mode
	/// @param dsn - uri of database, like: `builtin:///var/lib/reindexer/dbname` or just `/var/lib/reindexer/dbname`
	Error Connect(const string &dsn);

	/// Enable storage. Must be called before InitSystemNamespaces
	/// @param storagePath - file system path to database storage
	/// @param skipPlaceholderCheck - If set, then reindexer will not check folder for placeholder
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false);

	/// Open or create namespace
	/// @param nsName - Name of namespace
	/// @param opts - Storage options. Can be one of <br>
	/// StorageOpts::Enabled() - Enable storage. If storage is disabled, then namespace will be completely in-memory<br>
	/// StorageOpts::CreateIfMissing () - Storage will be created, if missing
	/// @param cacheMode - caching politcs to this namesapce
	/// @return errOK - On success
	Error OpenNamespace(const string &nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						CacheMode cacheMode = CacheMode::CacheModeOn);
	/// Create new namespace. Will fail, if namespace already exists
	/// @param nsDef - NamespaceDef with namespace initial parameters
	Error AddNamespace(const NamespaceDef &nsDef);
	/// Close namespace. Will free all memory resorces, associated with namespace. Forces sync changes to disk
	/// @param nsName - Name of namespace
	Error CloseNamespace(const string &nsName);
	/// Drop namespace. Will free all memory resorces, associated with namespace and erase all files from disk
	/// @param nsName - Name of namespace
	Error DropNamespace(const string &nsName);
	/// Add index to namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error AddIndex(const string &nsName, const IndexDef &index);
	/// Update index in namespace
	/// @param nsName - Name of namespace
	/// @param index - IndexDef with index name and parameters
	Error UpdateIndex(const string &nsName, const IndexDef &index);
	/// Drop index from namespace
	/// @param nsName - Name of namespace
	/// @param index - index name
	Error DropIndex(const string &nsName, const string &index);
	/// Get list of all available namespaces
	/// @param defs - std::vector of NamespaceDef of available namespaves
	/// @param bEnumAll - Also include currenty not opened, but exists on disk namespaces
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);
	/// Insert new Item to namespace. If item with same PK is already exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Error Insert(const string &nsName, Item &item, Completion cmpl = nullptr);
	/// Update Item in namespace. If item with same PK is not exists, when item.GetID will
	/// return -1, on success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Error Update(const string &nsName, Item &item, Completion cmpl = nullptr);
	/// Update or Insert Item in namespace. On success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Error Upsert(const string &nsName, Item &item, Completion cmpl = nullptr);
	/// Delete Item from namespace. On success item.GetID() will return internal Item ID
	/// @param nsName - Name of namespace
	/// @param item - Item, obtained by call to NewItem of the same namespace
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Error Delete(const string &nsName, Item &item, Completion cmpl = nullptr);
	/// Delete all items froms namespace, which matches provided Query
	/// @param query - Query with conditions
	/// @param result - QueryResults with IDs of deleted items
	Error Delete(const Query &query, QueryResults &result);
	/// Execute SQL Query and return results
	/// @param query - SQL query. Only "SELECT" semantic is supported
	/// @param result - QueryResults with found items
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Error Select(const string_view &query, QueryResults &result, Completion cmpl = nullptr);
	/// Execute Query and return results
	/// @param query - Query object with query attributes
	/// @param result - QueryResults with found items
	/// @param cmpl - Optional async completion routine. If nullptr function will work syncronius
	Error Select(const Query &query, QueryResults &result, Completion cmpl = nullptr);
	/// Flush changes to storage
	/// @param nsName - Name of namespace
	Error Commit(const string &nsName);
	/// Allocate new item for namespace
	/// @param nsName - Name of namespace
	/// @return Item ready for filling and futher Upsert/Insert/Delete/Update call
	Item NewItem(const string &nsName);
	/// Get meta data from storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - output string with meta data
	Error GetMeta(const string &nsName, const string &key, string &data);
	/// Put meta data to storage by key
	/// @param nsName - Name of namespace
	/// @param key - string with meta key
	/// @param data - string with meta data
	Error PutMeta(const string &nsName, const string &key, const string_view &data);
	/// Get list of all meta data keys
	/// @param nsName - Name of namespace
	/// @param keys - std::vector filled with meta keys
	Error EnumMeta(const string &nsName, vector<string> &keys);

	/// Init system namepaces, and load config from config namespace
	Error InitSystemNamespaces();

	// Subsribe to updates of database
	// @param observer - Observer interface, which will receive updates
	// @param subsctibe - true: subsribe, false: unsubsrcibe
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);

	typedef QueryResults QueryResultsT;
	typedef Item ItemT;

private:
	ReindexerImpl *impl_;
};

}  // namespace reindexer
