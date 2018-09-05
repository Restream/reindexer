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
#include "tools/errors.h"

using std::shared_ptr;
using std::string;

namespace reindexer {

class ReindexerImpl {
public:
	ReindexerImpl();
	~ReindexerImpl();

	Error Connect(const string &dsn);
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false);
	Error OpenNamespace(const string &_namespace, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						CacheMode cacheMode = CacheMode::CacheModeOn);
	Error AddNamespace(const NamespaceDef &nsDef);
	Error CloseNamespace(const string &_namespace);
	Error DropNamespace(const string &_namespace);
	Error AddIndex(const string &_namespace, const IndexDef &index);
	Error UpdateIndex(const string &_namespace, const IndexDef &index);
	Error DropIndex(const string &_namespace, const string &index);
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);
	Error ConfigureIndex(const string &_namespace, const string &index, const string &config);
	Error Insert(const string &_namespace, Item &item);
	Error Update(const string &_namespace, Item &item);
	Error Upsert(const string &_namespace, Item &item);
	Error Delete(const string &_namespace, Item &item);
	Error Delete(const Query &query, QueryResults &result);
	Error Select(const string &query, QueryResults &result);
	Error Select(const Query &query, QueryResults &result);
	Error Commit(const string &namespace_);
	Item NewItem(const string &_namespace);
	Error GetMeta(const string &_namespace, const string &key, string &data);
	Error PutMeta(const string &_namespace, const string &key, const string_view &data);
	Error EnumMeta(const string &_namespace, vector<string> &keys);
	Error InitSystemNamespaces();

protected:
	class NsLocker : public SelectLockUpgrader, h_vector<pair<Namespace::Ptr, smart_lock<shared_timed_mutex>>, 4> {
	public:
		~NsLocker() {
			while (size()) {
				pop_back();
			}
		}
		virtual void Upgrade() override {
			assert(locked_);
			if (upgraded_) return;
			for (auto it = rbegin(); it != rend(); it++) it->second = smart_lock<shared_timed_mutex>();
			for (auto it = begin(); it != end(); it++) it->second = smart_lock<shared_timed_mutex>(it->first->mtx_, true);
			upgraded_ = true;
			if (size() > 1) {
				throw Error(errWasRelock, "Internal - was lock upgrade, need retry");
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
		bool upgraded_ = false;
	};
	void doSelect(const Query &q, QueryResults &res, NsLocker &locker, SelectFunctionsHolder &func);
	JoinedSelectors prepareJoinedSelectors(const Query &q, QueryResults &result, NsLocker &locks, h_vector<Query, 4> &queries,
										   SelectFunctionsHolder &func);

	void syncSystemNamespaces(const string &nsName);
	void createSystemNamespaces();
	void updateSystemNamespace(const string &nsName, Item &item);
	Error applyConfig();

	void flusherThread();
	Error closeNamespace(const string &_namespace, bool dropStorage);
	Namespace::Ptr getNamespace(const string &_namespace);
	std::vector<Namespace::Ptr> getNamespaces();
	std::vector<string> getNamespacesNames();

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	shared_timed_mutex mtx_;
	string storagePath_;

	std::thread flusher_;
	std::atomic<bool> stopFlusher_;

	QueriesStatTracer queriesStatTracker_;
	std::shared_ptr<DBProfilingConfig> profConfig_;
	std::mutex profCfgMtx_;
};

}  // namespace reindexer
