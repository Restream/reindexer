#pragma once

#include <memory>
#include <string>
#include <thread>
#include "core/namespace.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/shared_mutex.h"
#include "query/querycache.h"
#include "tools/errors.h"

using std::shared_ptr;
using std::string;

namespace reindexer {

class Reindexer {
public:
	Reindexer();
	~Reindexer();

	// Enable storage
	Error EnableStorage(const string &storagePath, bool skipPlaceholderCheck = false);

	Error OpenNamespace(const string &_namespace, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef);
	Error CloseNamespace(const string &_namespace);
	Error DropNamespace(const string &_namespace);
	Error CloneNamespace(const string &src, const string &dst);
	Error RenameNamespace(const string &src, const string &dst);
	Error AddIndex(const string &_namespace, const IndexDef &index);
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);

	Error ConfigureIndex(const string &_namespace, const string &index, const string &config);

	Error Insert(const string &_namespace, Item *item);
	Error Update(const string &_namespace, Item *item);
	Error Upsert(const string &_namespace, Item *item);
	Error Delete(const string &_namespace, Item *item);
	Error Delete(const Query &query, QueryResults &result);
	Error Select(const string &query, QueryResults &result);
	Error Select(const Query &query, QueryResults &result);
	Error Commit(const string &namespace_);

	// Alloc new item for namespace
	Item *NewItem(const string &_namespace);

	// Get meta data from storage by key
	Error GetMeta(const string &_namespace, const string &key, string &data);
	// Put meta data to storage by key
	Error PutMeta(const string &_namespace, const string &key, const Slice &data);

	Error EnumMeta(const string &_namespace, vector<string> &keys);

	Error ResetStats();
	Error GetStats(reindexer_stat &stat);

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

		void Upgrade() {
			assert(locked_);
			if (upgraded_) return;
			for (auto it = rbegin(); it != rend(); it++) it->second = smart_lock<shared_timed_mutex>();
			for (auto it = begin(); it != end(); it++) it->second = smart_lock<shared_timed_mutex>(it->first->mtx_, true);
			upgraded_ = true;
		}

		Namespace::Ptr Get(const string &name) {
			for (auto it = begin(); it != end(); it++)
				if (it->first->name == name) return it->first;
			return nullptr;
		}

	protected:
		bool locked_ = false;
		bool upgraded_ = false;
	};

	void flusherThread();
	Error closeNamespace(const string &_namespace, bool dropStorage);
	Namespace::Ptr getNamespace(const string &_namespace);

	fast_hash_map<string, Namespace::Ptr> namespaces;

	shared_timed_mutex ns_mutex;
	string storagePath_;

	std::thread flusher_;
	bool stopFlusher_;
};

}  // namespace reindexer
