#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include "core/namespace.h"
#include "core/nsselecter/nsselecter.h"
#include "dbconfig.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/shared_mutex.h"
#include "query/querycache.h"
#include "querystat.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"
#include "transaction.h"

using std::shared_ptr;
using std::string;

namespace reindexer {

class Replicator;
class ReindexerImpl {
public:
	using Completion = Transaction::Completion;

	ReindexerImpl();
	~ReindexerImpl();

	Error Connect(const string &dsn);
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false);
	Error OpenNamespace(string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef);
	Error CloseNamespace(string_view nsName);
	Error DropNamespace(string_view nsName);
	Error AddIndex(string_view nsName, const IndexDef &index);
	Error UpdateIndex(string_view nsName, const IndexDef &index);
	Error DropIndex(string_view nsName, const IndexDef &index);
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);
	Error Insert(string_view nsName, Item &item, Completion cmpl = nullptr);
	Error Update(string_view nsName, Item &item, Completion cmpl = nullptr);
	Error Update(const Query &query, QueryResults &result);
	Error Upsert(string_view nsName, Item &item, Completion cmpl = nullptr);
	Error Delete(string_view nsName, Item &item, Completion cmpl = nullptr);
	Error Delete(const Query &query, QueryResults &result);
	Error Select(string_view query, QueryResults &result, Completion cmpl = nullptr);
	Error Select(const Query &query, QueryResults &result, Completion cmpl = nullptr);
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName);

	Transaction NewTransaction(const string &nsName);
	Error CommitTransaction(Transaction &tr);
	Error RollBackTransaction(Transaction &tr);

	Error GetMeta(string_view nsName, const string &key, string &data);
	Error PutMeta(string_view nsName, const string &key, string_view data);
	Error EnumMeta(string_view nsName, vector<string> &keys);
	Error InitSystemNamespaces();
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);
	Error GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions);

protected:
	class NsLocker : public h_vector<pair<Namespace::Ptr, smart_lock<shared_timed_mutex>>, 4> {
	public:
		~NsLocker() {
			while (size()) {
				pop_back();
			}
		}

		void Add(Namespace::Ptr ns) {
			assert(!locked_);
			for (auto it = begin(); it != end(); it++)
				if (it->first.get() == ns.get()) return;

			push_back({ns, smart_lock<shared_timed_mutex>()});
			return;
		}
		void Lock() {
			std::sort(begin(), end(),
					  [](const pair<Namespace::Ptr, smart_lock<shared_timed_mutex>> &lhs,
						 const pair<Namespace::Ptr, smart_lock<shared_timed_mutex>> &rhs) { return lhs.first.get() < rhs.first.get(); });
			for (auto it = begin(); it != end(); it++) it->second = smart_lock<shared_timed_mutex>(it->first->mtx_, false);
			locked_ = true;
		}

		Namespace::Ptr Get(const string &name) {
			for (auto it = begin(); it != end(); it++)
				if (iequals(it->first->name_, name)) return it->first;
			return nullptr;
		}

	protected:
		bool locked_ = false;
	};
	void doSelect(const Query &q, QueryResults &res, NsLocker &locker, SelectFunctionsHolder &func);
	JoinedSelectors prepareJoinedSelectors(const Query &q, QueryResults &result, NsLocker &locks, h_vector<Query, 4> &queries,
										   SelectFunctionsHolder &func);

	void ensureDataLoaded(Namespace::Ptr &ns);
	void syncSystemNamespaces(string_view nsName);
	void createSystemNamespaces();
	Error updateDbFromConfig(string_view configNsName, Item &configItem);
	void updateConfigProvider(Item &configItem);
	void onProfiligConfigLoad();
	void tryLoadReplicatorConfFromFile();

	void backgroundRoutine();
	Error closeNamespace(string_view nsName, bool dropStorage, bool enableDropSlave = false);
	Namespace::Ptr getNamespace(string_view nsName);
	std::vector<Namespace::Ptr> getNamespaces();
	std::vector<string> getNamespacesNames();

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	shared_timed_mutex mtx_;
	string storagePath_;

	std::thread backgroundThread_;
	std::atomic<bool> stopBackgroundThread_;

	QueriesStatTracer queriesStatTracker_;
	UpdatesObservers observers_;
	std::unique_ptr<Replicator> replicator_;
	DBConfigProvider configProvider_;

	shared_timed_mutex storageMtx_;
	friend class Replicator;
};

}  // namespace reindexer
