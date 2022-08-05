#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "core/namespace/namespace.h"
#include "core/nsselecter/nsselecter.h"
#include "core/rdxcontext.h"
#include "dbconfig.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/smart_lock.h"
#include "querystat.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"
#include "tools/filecontentwatcher.h"
#include "transaction.h"

namespace reindexer {

class Replicator;
class IClientsStats;
class ProtobufSchema;

class ReindexerImpl {
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Reindexer>;
	using StorageMutex = MarkedMutex<shared_timed_mutex, MutexMark::ReindexerStorage>;
	struct NsLockerItem {
		NsLockerItem(NamespaceImpl::Ptr ins = {}) : ns(std::move(ins)), count(1) {}
		NamespaceImpl::Ptr ns;
		NamespaceImpl::Locker::RLockT nsLck;
		unsigned count = 1;
	};
	template <bool needUpdateSystemNs, typename MakeCtxStrFn, typename MemFnType, MemFnType Namespace::*MemFn, typename Arg,
			  typename... Args>
	Error applyNsFunction(std::string_view nsName, const InternalRdxContext &ctx, const MakeCtxStrFn &makeCtxStr, Arg arg, Args... args);
	template <auto MemFn, typename MakeCtxStrFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const InternalRdxContext &ctx, const MakeCtxStrFn &makeCtxStr, Arg &, Args...);

public:
	using Completion = std::function<void(const Error &err)>;

	ReindexerImpl(IClientsStats *clientsStats = nullptr);

	~ReindexerImpl();

	Error Connect(const string &dsn, ConnectOpts opts = ConnectOpts());
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false, const InternalRdxContext &ctx = InternalRdxContext());
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						const InternalRdxContext &ctx = InternalRdxContext());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx = InternalRdxContext());
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error DropNamespace(std::string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx = InternalRdxContext());
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx = InternalRdxContext());
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx = InternalRdxContext());
	Error UpdateIndex(std::string_view nsName, const IndexDef &indexDef, const InternalRdxContext &ctx = InternalRdxContext());
	Error DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx = InternalRdxContext());
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx = InternalRdxContext());
	Error Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Insert(std::string_view nsName, Item &item, QueryResults &, const InternalRdxContext &ctx = InternalRdxContext());
	Error Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Update(std::string_view nsName, Item &item, QueryResults &, const InternalRdxContext &ctx = InternalRdxContext());
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Upsert(std::string_view nsName, Item &item, QueryResults &, const InternalRdxContext &ctx = InternalRdxContext());
	Error Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Delete(std::string_view nsName, Item &item, QueryResults &, const InternalRdxContext &ctx = InternalRdxContext());
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Select(std::string_view query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Commit(std::string_view nsName);
	Item NewItem(std::string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());

	Transaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error RollBackTransaction(Transaction &tr);

	Error GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx = InternalRdxContext());
	Error PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx = InternalRdxContext());
	Error EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx = InternalRdxContext());
	Error InitSystemNamespaces();
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts);
	Error UnsubscribeUpdates(IUpdatesObserver *observer);
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, vector<string> &suggestions,
							const InternalRdxContext &ctx = InternalRdxContext());
	Error GetProtobufSchema(WrSerializer &ser, vector<string> &namespaces);
	Error Status();

	bool NeedTraceActivity() { return configProvider_.GetProfilingConfig().activityStats; }

	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index,
					const InternalRdxContext &ctx = InternalRdxContext());

protected:
	typedef contexted_shared_lock<Mutex, const RdxContext> SLock;
	typedef contexted_unique_lock<Mutex, const RdxContext> ULock;
	typedef contexted_shared_lock<StorageMutex, const RdxContext> SStorageLock;
	typedef contexted_unique_lock<StorageMutex, const RdxContext> UStorageLock;

	template <typename Context>
	class NsLocker : private h_vector<NsLockerItem, 4> {
	public:
		NsLocker(const Context &context) : context_(context) {}
		~NsLocker() {
			// Unlock first
			for (auto it = rbegin(); it != rend(); ++it) {
				// Some of the namespaces may be in unlocked statet in case of the exception during Lock() call
				if (it->nsLck.owns_lock()) {
					it->nsLck.unlock();
				} else {
					assertrx(!locked_);
				}
			}
			// Clean (ns may releases, if locker holds last ref)
		}

		void Add(NamespaceImpl::Ptr ns) {
			assertrx(!locked_);
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					++(it->count);
					return;
				}
			}

			emplace_back(std::move(ns));
			return;
		}
		void Delete(const NamespaceImpl::Ptr &ns) {
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					if (!--(it->count)) erase(it);
					return;
				}
			}
			assertrx(0);
		}
		void Lock() {
			std::sort(begin(), end(), [](const NsLockerItem &lhs, const NsLockerItem &rhs) { return lhs.ns.get() < rhs.ns.get(); });
			for (auto it = begin(); it != end(); ++it) {
				it->nsLck = it->ns->rLock(context_);
			}
			locked_ = true;
		}

		NamespaceImpl::Ptr Get(const string &name) {
			for (auto it = begin(); it != end(); it++) {
				if (iequals(it->ns->name_, name)) return it->ns;
			}
			return nullptr;
		}

	protected:
		bool locked_ = false;
		const Context &context_;
	};
	template <typename T>
	void doSelect(const Query &q, QueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func, const RdxContext &ctx);
	struct QueryResultsContext;
	template <typename T>
	JoinedSelectors prepareJoinedSelectors(const Query &q, QueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func,
										   vector<QueryResultsContext> &, const RdxContext &ctx);
	void prepareJoinResults(const Query &q, QueryResults &result);
	static bool isPreResultValuesModeOptimizationAvailable(const Query &jItemQ, const NamespaceImpl::Ptr &jns);

	void syncSystemNamespaces(std::string_view sysNsName, std::string_view filterNsName, const RdxContext &ctx);
	void createSystemNamespaces();
	void updateToSystemNamespace(std::string_view nsName, Item &, const RdxContext &ctx);
	void updateConfigProvider(const gason::JsonNode &config);
	void updateReplicationConfFile();
	void onProfiligConfigLoad();
	Error tryLoadReplicatorConfFromFile();
	Error tryLoadReplicatorConfFromYAML(const std::string &yamlReplConf);

	void backgroundRoutine();
	void storageFlushingRoutine();
	Error closeNamespace(std::string_view nsName, const RdxContext &ctx, bool dropStorage, bool enableDropSlave = false);

	Error syncDownstream(std::string_view nsName, bool force, const InternalRdxContext &ctx = InternalRdxContext());

	Namespace::Ptr getNamespace(std::string_view nsName, const RdxContext &ctx);
	Namespace::Ptr getNamespaceNoThrow(std::string_view nsName, const RdxContext &ctx);

	std::vector<std::pair<string, Namespace::Ptr>> getNamespaces(const RdxContext &ctx);
	std::vector<string> getNamespacesNames(const RdxContext &ctx);
	Error renameNamespace(std::string_view srcNsName, const std::string &dstNsName, bool fromReplication = false,
						  const InternalRdxContext &ctx = InternalRdxContext());

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	Mutex mtx_;
	string storagePath_;

	std::thread backgroundThread_;
	std::thread storageFlushingThread_;
	std::atomic<bool> stopBackgroundThreads_;

	QueriesStatTracer queriesStatTracker_;
	UpdatesObservers observers_;
	std::unique_ptr<Replicator> replicator_;
	DBConfigProvider configProvider_;
	FileContetWatcher replConfigFileChecker_;
	bool hasReplConfigLoadError_;

	ActivityContainer activities_;

	StorageMutex storageMtx_;
	StorageType storageType_;
	bool autorepairEnabled_ = false;
	bool replicationEnabled_ = true;
	std::atomic<bool> connected_;

	IClientsStats *clientsStats_ = nullptr;

	friend class Replicator;
	friend class TransactionImpl;
};

}  // namespace reindexer
