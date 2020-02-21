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

using std::shared_ptr;
using std::string;

namespace reindexer {

class Replicator;
class ReindexerImpl {
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Reindexer>;
	using StorageMutex = MarkedMutex<shared_timed_mutex, MutexMark::ReindexerStorage>;
	struct NsLockerItem {
		NsLockerItem(NamespaceImpl::Ptr ins = {}) : ns(std::move(ins)), count(1) {}
		NamespaceImpl::Ptr ns;
		NamespaceImpl::Locker::RLockT nsLck;
		unsigned count = 1;
	};

public:
	using Completion = std::function<void(const Error &err)>;

	ReindexerImpl();
	~ReindexerImpl();

	Error Connect(const string &dsn, ConnectOpts opts = ConnectOpts());
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false, const InternalRdxContext &ctx = InternalRdxContext());
	Error OpenNamespace(string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						const InternalRdxContext &ctx = InternalRdxContext());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx = InternalRdxContext());
	Error CloseNamespace(string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error DropNamespace(string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error TruncateNamespace(string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx = InternalRdxContext());
	Error UpdateIndex(string_view nsName, const IndexDef &indexDef, const InternalRdxContext &ctx = InternalRdxContext());
	Error DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx = InternalRdxContext());
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx = InternalRdxContext());
	Error Insert(string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Update(string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Upsert(string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Delete(string_view nsName, Item &item, const InternalRdxContext &ctx = InternalRdxContext());
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Select(string_view query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());

	Transaction NewTransaction(string_view nsName, const InternalRdxContext &ctx = InternalRdxContext());
	Error CommitTransaction(Transaction &tr, QueryResults &result, const InternalRdxContext &ctx = InternalRdxContext());
	Error RollBackTransaction(Transaction &tr);

	Error GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx = InternalRdxContext());
	Error PutMeta(string_view nsName, const string &key, string_view data, const InternalRdxContext &ctx = InternalRdxContext());
	Error EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx = InternalRdxContext());
	Error InitSystemNamespaces();
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions,
							const InternalRdxContext &ctx = InternalRdxContext());
	Error Status();

	bool NeedTraceActivity() { return configProvider_.GetProfilingConfig().activityStats; }

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
			while (size()) {
				pop_back();
			}
		}

		void Add(NamespaceImpl::Ptr ns) {
			assert(!locked_);
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					++(it->count);
					return;
				}
			}

			emplace_back(ns);
			return;
		}
		void Delete(NamespaceImpl::Ptr ns) {
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					if (!--(it->count)) erase(it);
					return;
				}
			}
			assert(0);
		}
		void Lock() {
			std::sort(begin(), end(), [](const NsLockerItem &lhs, const NsLockerItem &rhs) { return lhs.ns.get() < rhs.ns.get(); });
			for (auto it = begin(); it != end(); ++it) {
				it->nsLck = it->ns->rLock(context_);
			}
			locked_ = true;
		}

		NamespaceImpl::Ptr Get(const string &name) {
			for (auto it = begin(); it != end(); it++)
				if (iequals(it->ns->name_, name)) return it->ns;
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

	void ensureDataLoaded(Namespace::Ptr &ns, const RdxContext &ctx);

	void syncSystemNamespaces(string_view sysNsName, string_view filterNsName, const RdxContext &ctx);
	void createSystemNamespaces();
	void updateToSystemNamespace(string_view nsName, Item &, const RdxContext &ctx);
	void updateConfigProvider(const gason::JsonNode &config);
	void updateReplicationConfFile();
	void onProfiligConfigLoad();
	Error tryLoadReplicatorConfFromFile();
	Error tryLoadReplicatorConfFromYAML(const std::string &yamlReplConf);

	void backgroundRoutine();
	Error closeNamespace(string_view nsName, const RdxContext &ctx, bool dropStorage, bool enableDropSlave = false);

	Namespace::Ptr getNamespace(string_view nsName, const RdxContext &ctx);
	Namespace::Ptr getNamespaceNoThrow(string_view nsName, const RdxContext &ctx);

	std::vector<std::pair<string, Namespace::Ptr>> getNamespaces(const RdxContext &ctx);
	std::vector<string> getNamespacesNames(const RdxContext &ctx);

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	Mutex mtx_;
	string storagePath_;

	std::thread backgroundThread_;
	std::atomic<bool> stopBackgroundThread_;

	QueriesStatTracer queriesStatTracker_;
	UpdatesObservers observers_;
	std::unique_ptr<Replicator> replicator_;
	DBConfigProvider configProvider_;
	FileContetWatcher replConfigFileChecker_;
	bool hasReplConfigLoadError_;

	ActivityContainer activities_;

	StorageMutex storageMtx_;
	StorageType storageType_;
	bool autorepairEnabled_;
	std::atomic<bool> connected_;

	friend class Replicator;
	friend class TransactionImpl;
};

}  // namespace reindexer
