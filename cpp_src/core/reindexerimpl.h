#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "core/namespace.h"
#include "core/nsselecter/nsselecter.h"
#include "core/rdxcontext.h"
#include "dbconfig.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/smart_lock.h"
#include "namespacecloner.h"
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

public:
	using Completion = Transaction::Completion;

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
	Error AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx = InternalRdxContext());
	Error UpdateIndex(string_view nsName, const IndexDef &indexDef, const InternalRdxContext &ctx = InternalRdxContext());
	Error DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx = InternalRdxContext());
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll, const InternalRdxContext &ctx = InternalRdxContext());
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

	Transaction NewTransaction(const string &nsName);
	Error CommitTransaction(Transaction &tr, const InternalRdxContext &ctx = InternalRdxContext());
	Error RollBackTransaction(Transaction &tr);

	Error GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx = InternalRdxContext());
	Error PutMeta(string_view nsName, const string &key, string_view data, const InternalRdxContext &ctx = InternalRdxContext());
	Error EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx = InternalRdxContext());
	Error InitSystemNamespaces();
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions);

	bool NeedTraceActivity() { return configProvider_.GetProfilingConfig().activityStats; }

protected:
	typedef contexted_shared_lock<Mutex, const RdxContext> SLock;
	typedef contexted_lock_guard<Mutex, const RdxContext> ULock;
	typedef contexted_shared_lock<StorageMutex, const RdxContext> SStorageLock;
	typedef contexted_lock_guard<StorageMutex, const RdxContext> UStorageLock;

	template <typename Context>
	class NsLocker : public h_vector<pair<Namespace::Ptr, smart_lock<Namespace::Mutex>>, 4> {
	public:
		NsLocker(const Context &context) : context_(context) {}
		~NsLocker() {
			while (size()) {
				pop_back();
			}
		}

		void Add(Namespace::Ptr ns) {
			assert(!locked_);
			for (auto it = begin(); it != end(); it++)
				if (it->first.get() == ns.get()) return;

			push_back({ns, smart_lock<Namespace::Mutex>()});
			return;
		}
		void Lock() {
			std::sort(begin(), end(),
					  [](const pair<Namespace::Ptr, smart_lock<Namespace::Mutex>> &lhs,
						 const pair<Namespace::Ptr, smart_lock<Namespace::Mutex>> &rhs) { return lhs.first.get() < rhs.first.get(); });
			for (auto it = begin(); it != end(); ++it) {
				it->second = smart_lock<Namespace::Mutex>(it->first->mtx_, context_, false);
			}
			locked_ = true;
		}

		Namespace::Ptr Get(const string &name) {
			for (auto it = begin(); it != end(); it++)
				if (iequals(it->first->name_, name)) return it->first;
			return nullptr;
		}

	protected:
		bool locked_ = false;
		const Context &context_;
	};
	template <typename T>
	void doSelect(const Query &q, QueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func, const RdxContext &ctx);
	template <typename T>
	JoinedSelectors prepareJoinedSelectors(const Query &q, QueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func,
										   const RdxContext &ctx);

	void ensureDataLoaded(ClonableNamespace &ns, const RdxContext &ctx);
	void ensureDataLoaded(Namespace::Ptr &ns, const RdxContext &ctx);

	void syncSystemNamespaces(string_view nsName, const RdxContext &ctx);
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
#if ATOMIC_NS_CLONE
	ClonableNamespace getClonableNamespace(string_view nsName, const RdxContext &ctx, size_t actionsSize = 1);
#else
	Namespace::Ptr getClonableNamespace(string_view nsName, const RdxContext &ctx, size_t actionsSize = 1) {
		(void)actionsSize;
		return getNamespace(nsName, ctx);
	}
#endif

	std::vector<Namespace::Ptr> getNamespaces(const RdxContext &ctx);
	std::vector<string> getNamespacesNames(const RdxContext &ctx);

	fast_hash_map<string, NamespaceCloner::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

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
	friend class Replicator;
	friend class TransactionImpl;
};

}  // namespace reindexer
