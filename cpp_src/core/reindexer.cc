#include "core/reindexer.h"
#include <stdio.h>
#include <chrono>
#include <thread>
#include "core/cjson/jsondecoder.h"
#include "kx/kxsort.h"
#include "namespacedef.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::lock_guard;
using std::string;
using std::vector;

namespace reindexer {

#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ < 8
static __thread reindexer_stat local_stat;
#else
static thread_local reindexer_stat local_stat;
#endif

#ifndef REINDEX_NOTIMIG
#define STAT_FUNC(name)                                                                     \
	class __stat {                                                                          \
	public:                                                                                 \
		__stat() { tmStart = high_resolution_clock::now(); }                                \
		~__stat() {                                                                         \
			auto tmEnd = high_resolution_clock::now();                                      \
			local_stat.time_##name += duration_cast<microseconds>(tmEnd - tmStart).count(); \
			local_stat.count_##name++;                                                      \
		}                                                                                   \
		std::chrono::time_point<std::chrono::high_resolution_clock> tmStart;                \
	} __stater
#else
#define STAT_FUNC(name)
#endif

Reindexer::Reindexer() { stopFlusher_ = false; }

Reindexer::~Reindexer() {
	if (storagePath_.length()) {
		stopFlusher_ = true;
		flusher_.join();
	}
}

const char* kStoragePlaceholderFilename = ".reindexer.storage";

Error Reindexer::EnableStorage(const string& storagePath, bool skipPlaceholderCheck) {
	storagePath_.clear();
	if (storagePath.empty()) return errOK;
	if (MkDirAll(storagePath) < 0) {
		return Error(errParams, "Can't create directory '%s' for reindexer storage - reason %s", storagePath.c_str(), strerror(errno));
	}

	vector<reindexer::DirEntry> dirEntries;
	bool isEmpty = true;
	if (ReadDir(storagePath, dirEntries) < 0) {
		return Error(errParams, "Can't read contents of directory '%s' for reindexer storage - reason %s", storagePath.c_str(),
					 strerror(errno));
	}
	for (auto& entry : dirEntries) {
		if (entry.name != "." && entry.name != ".." && entry.name != kStoragePlaceholderFilename) {
			isEmpty = false;
		}
	}

	if (!isEmpty && !skipPlaceholderCheck) {
		FILE* f = fopen(JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "r");
		if (f) {
			fclose(f);
		} else {
			return Error(errParams, "Cowadly refusing to use directory '%s' - it's not empty, and doesn't contains reindexer placeholder",
						 storagePath.c_str());
		}
	} else {
		FILE* f = fopen(JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "w");
		if (f) {
			fwrite("leveldb", 7, 1, f);
			fclose(f);
		} else {
			return Error(errParams, "Can't create placeholder in directory '%s' for reindexer storage - reason %s", storagePath.c_str(),
						 strerror(errno));
		}
	}

	storagePath_ = storagePath;
	flusher_ = std::thread([this]() { this->flusherThread(); });

	return errOK;
}

Error Reindexer::AddNamespace(const NamespaceDef& nsDef) {
	shared_ptr<Namespace> ns;
	try {
		{
#ifndef REINDEX_SINGLETHREAD
			lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
			if (namespaces.find(nsDef.name) != namespaces.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name.c_str());
			}
		}
		for (;;) {
			ns = std::make_shared<Namespace>(nsDef.name);
			if (nsDef.storage.IsEnabled() && !storagePath_.empty()) {
				ns->EnableStorage(storagePath_, nsDef.storage);
			}

			for (auto& idx : nsDef.indexes) {
				vector<string> jPaths;
				if (idx.jsonPath.empty()) {
					ns->AddIndex(idx.name, "", idx.Type(), idx.opts);
				} else {
					for (auto& p : split(idx.jsonPath, ",", true, jPaths)) {
						ns->AddIndex(idx.name, p, idx.Type(), idx.opts);
					}
				}
			}
			break;
		}
		if (nsDef.storage.IsEnabled()) {
			ns->LoadFromStorage();
		}
#ifndef REINDEX_SINGLETHREAD
		lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
		namespaces.insert({nsDef.name, ns});
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
Error Reindexer::OpenNamespace(const string& name, const StorageOpts& storage) {
	shared_ptr<Namespace> ns;
	try {
		{
#ifndef REINDEX_SINGLETHREAD
			lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
			if (namespaces.find(name) != namespaces.end()) {
				return 0;
			}
		}
		ns = std::make_shared<Namespace>(name);
		if (storage.IsEnabled() && !storagePath_.empty()) {
			ns->EnableStorage(storagePath_, storage);
			ns->LoadFromStorage();
		}
#ifndef REINDEX_SINGLETHREAD
		lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
		namespaces.insert({name, ns});
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::DropNamespace(const string& _namespace) { return closeNamespace(_namespace, true); }
Error Reindexer::CloseNamespace(const string& _namespace) { return closeNamespace(_namespace, false); }

Error Reindexer::closeNamespace(const string& _namespace, bool dropStorage) {
	shared_ptr<Namespace> ns;
	try {
#ifndef REINDEX_SINGLETHREAD
		lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
		auto nsIt = namespaces.find(_namespace);

		if (nsIt == namespaces.end()) {
			return Error(errParams, "Namespace '%s' does not exist", _namespace.c_str());
		}

		// Temporary save namespace. This will call destructor without lock
		ns = nsIt->second;
		namespaces.erase(nsIt);
		if (dropStorage) {
			ns->DeleteStorage();
		} else {
			ns->FlushStorage();
		}
	} catch (const Error& err) {
		ns = nullptr;
		return err;
	}
	// Here will called destructor
	ns = nullptr;
	return 0;
}

// Atomically clone namespace. If dst NS exits, error will throwns and NS will not cloned
// Thread safe
Error Reindexer::CloneNamespace(const string& src, const string& dst) {
	Namespace::Ptr srcNamespace;

	{
#ifndef REINDEX_SINGLETHREAD
		lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif

		auto srcIt = namespaces.find(src);

		if (srcIt == namespaces.end()) {
			return Error(errParams, "Namespace '%s' does not exist", src.c_str());
		}

		if (namespaces.find(dst) != namespaces.end()) {
			return Error(errParams, "Namespace '%s' already exists", dst.c_str());
		}

		srcNamespace = srcIt->second;
	}
	// Clone data without lock
	shared_ptr<Namespace> dstNamespace;
	dstNamespace.reset(Namespace::Clone(srcNamespace));
	srcNamespace->LockSnapshot();

	{
// Lock again
#ifndef REINDEX_SINGLETHREAD
		lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
		namespaces.insert({dst, move(dstNamespace)});
	}
	return 0;
}

// Atomically rename namespace. If dst NS exists, it will be removed
// Thread safe
Error Reindexer::RenameNamespace(const string& src, const string& dst) {
	shared_ptr<Namespace> dstNamespace;
#ifndef REINDEX_SINGLETHREAD
	lock_guard<shared_timed_mutex> lock(ns_mutex);
#endif
	auto srcIt = namespaces.find(src);

	if (srcIt == namespaces.end()) {
		ns_mutex.unlock();
		return Error(errParams, "Namespace '%s' ", src.c_str());
	}

	auto dstIt = namespaces.find(dst);

	if (dstIt != namespaces.end()) {
		// If dst NS already exists - prepare to remove it
		// save pointer to dst NS. It will be fried on this function exit without lock
		dstNamespace = dstIt->second;
		namespaces.erase(dstIt);
		// Update iterator. It can be invalidated
		srcIt = namespaces.find(src);
		assert(srcIt != namespaces.end());
	}

	// Save pointer to src NS
	shared_ptr<Namespace> srcNamespace = srcIt->second;
	// Erase src NS iterator from namespaces map
	namespaces.erase(srcIt);
	// Put src NS with new (dst) name
	namespaces.insert({dst, move(srcNamespace)});
	return 0;
}

Error Reindexer::Insert(const string& _namespace, Item* item) {
	STAT_FUNC(insert);
	try {
		auto ns = getNamespace(_namespace);
		ns->Insert(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Update(const string& _namespace, Item* item) {
	STAT_FUNC(update);
	try {
		auto ns = getNamespace(_namespace);
		ns->Update(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Upsert(const string& _namespace, Item* item) {
	STAT_FUNC(upsert);
	try {
		auto ns = getNamespace(_namespace);
		ns->Upsert(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Item* Reindexer::NewItem(const string& _namespace) {
	Item* item;
	try {
		getNamespace(_namespace)->NewItem(&item);
	} catch (const Error& err) {
		item = new ItemImpl(err);
	}
	return item;
}

// Get meta data from storage by key
Error Reindexer::GetMeta(const string& _namespace, const string& key, string& data) {
	try {
		data = getNamespace(_namespace)->GetMeta(key);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
// Put meta data to storage by key
Error Reindexer::PutMeta(const string& _namespace, const string& key, const Slice& data) {
	try {
		getNamespace(_namespace)->PutMeta(key, data);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::EnumMeta(const string& _namespace, vector<string>& keys) {
	try {
		keys = getNamespace(_namespace)->EnumMeta();
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Delete(const string& _namespace, Item* item) {
	STAT_FUNC(delete);
	try {
		auto ns = getNamespace(_namespace);
		ns->Delete(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
Error Reindexer::Delete(const Query& q, QueryResults& result) {
	STAT_FUNC(delete);
	try {
		auto ns = getNamespace(q._namespace);
		ns->Delete(q, result);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Select(const string& query, QueryResults& result) {
	STAT_FUNC(select);
	try {
		Query q;
		q.Parse(query);
		return Select(q, result);

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

struct ItemRefLess {
	bool operator()(const ItemRef& lhs, const ItemRef& rhs) const {
		if (lhs.proc == rhs.proc) {
			if (lhs.nsid == rhs.nsid) {
				return lhs.id < rhs.id;
			}
			return lhs.nsid < rhs.nsid;
		}
		return lhs.proc > rhs.proc;
	}
};

Error Reindexer::Select(const Query& q, QueryResults& result) {
	NsLocker locks;

	auto lockUpgrader = [&locks]() { locks.Upgrade(); };

	try {
		if (q.describe) {
			auto namespaceNames = q.namespacesNames_;
			if (namespaceNames.empty()) {
				vector<NamespaceDef> nsDefs;
				EnumNamespaces(nsDefs, false);
				for (auto& nsDef : nsDefs) {
					namespaceNames.push_back(nsDef.name);
				}
			}

			for (auto& name : namespaceNames) {
				auto ns = getNamespace(name);
				ns->Describe(result);
			}

			return 0;
		}

		if (!q.joinQueries_.empty() && !q.mergeQueries_.empty()) {
			throw Error(errParams, "Merge and join can't be in same query");
		}

		// Loockup and lock namespaces
		locks.Add(getNamespace(q._namespace));
		for (auto& jq : q.joinQueries_) locks.Add(getNamespace(jq._namespace));
		for (auto& mq : q.mergeQueries_) locks.Add(getNamespace(mq._namespace));
		locks.Lock();

		auto ns = locks.Get(q._namespace);
		if (!ns) {
			throw Error(errParams, "Namespace '%s' is not exists", q._namespace.c_str());
		}
		JoinedSelectors joinedSelectors;

		h_vector<Query, 4> queries;
		queries.reserve(q.joinQueries_.size());

		// For each joined queries
		for (auto& jq : q.joinQueries_) {
			// Get common results from joined namespaces
			auto jns = locks.Get(jq._namespace);
			IdSet preResult;

			if (jq.entries.size()) {
				QueryResults jr;
				Query jjq(jq);
				jjq.sortDirDesc = false;
				jjq.Limit(INT_MAX);
				SelectCtx ctx(jjq, lockUpgrader);
				ctx.rsltAsSrtOrdrs = true;
				jns->Select(jr, ctx);
				preResult.reserve(jr.size());
				for (auto it : jr) preResult.Add(it.id, IdSet::Unordered);
			}
			// Do join for each item in main result
			size_t pos = joinedSelectors.size();
			Query jItemQ(jq._namespace);
			jItemQ.Debug(jq.debugLevel);
			jItemQ.Limit(jq.count);
			jItemQ.Sort(jq.sortBy.c_str(), jq.sortDirDesc);
			jItemQ.entries.reserve(jq.joinEntries_.size());

			// Construct join conditions
			for (auto& je : jq.joinEntries_) {
				QueryEntry qe;
				qe.op = je.op_;
				qe.condition = je.condition_;
				qe.index = je.joinIndex_;
				qe.idxNo = jns->getIndexByName(je.joinIndex_);
				jItemQ.entries.push_back(qe);
			}
			queries.push_back(jItemQ);
			Query* pjItemQ = &queries.back();

			auto joinedSelector = [&result, jq, ns, jns, preResult, pos, pjItemQ, lockUpgrader](IdType id, ConstPayload payload,
																								bool match) {

				local_stat.count_join++;  // Do not measure each join time (expensive). Just give count
				KeyRefs krefs;
				// Put values to join conditions
				int cnt = 0;
				for (auto& je : jq.joinEntries_) {
					payload.Get(je.index_, krefs);
					pjItemQ->entries[cnt].values.resize(0);
					pjItemQ->entries[cnt].values.reserve(krefs.size());
					for (auto kref : krefs) pjItemQ->entries[cnt].values.push_back(KeyValue(kref));
					cnt++;
				}
				pjItemQ->Limit(match ? jq.count : 1);
				QueryResults joinItemR;

				SelectCtx ctx(*pjItemQ, lockUpgrader);
				if (jq.entries.size()) ctx.preResult = &preResult;
				jns->Select(joinItemR, ctx);

				bool found = joinItemR.size();
				if (match && found) {
					auto& jres = result.joined_.emplace(id, vector<QueryResults>()).first->second;

					if (pos >= jres.size()) jres.resize(pos + 1);

					jres[pos] = std::move(joinItemR);
				}
				return found;
			};
			joinedSelectors.push_back({jq.joinType, joinedSelector});
		}
		{
			STAT_FUNC(select);
			SelectCtx ctx(q, lockUpgrader);
			ctx.joinedSelectors = &joinedSelectors;
			ctx.nsid = 0;
			ctx.isForceAll = !q.mergeQueries_.empty() || !q.forcedSortOrder.empty();
			ns->Select(result, ctx);
		}
		if (!q.mergeQueries_.empty()) {
			uint8_t counter = 0;

			if (!q.joinQueries_.empty()) {
				throw Error(errParams, "Merge and join can't be in same query");
			}

			for (auto& mq : q.mergeQueries_) {
				auto mns = locks.Get(mq._namespace);
				STAT_FUNC(select);
				SelectCtx ctx(mq, lockUpgrader);
				ctx.nsid = ++counter;
				ctx.isForceAll = true;
				mns->Select(result, ctx);
			}

			if (static_cast<size_t>(q.start) >= result.size()) {
				result.clear();
				return 0;
			}

			std::sort(result.begin(), result.end(), ItemRefLess());
			if (q.calcTotal) {
				result.totalCount = result.size();
			}

			if (q.start > 0) {
				auto end = q.start < result.size() ? result.begin() + q.start : result.end();
				result.erase(result.begin(), end);
			}

			if (result.size() > q.count) {
				result.erase(result.begin() + q.count, result.end());
			}

			if (q.start) {
				result.erase(result.begin(), result.begin() + q.start);
			}
			if (static_cast<size_t>(q.count) < result.size()) {
				result.erase(result.begin() + q.count, result.end());
			}
		}

		// dummy selects for put ctx-es
		for (auto& jq : q.joinQueries_) {
			auto jns = locks.Get(jq._namespace);
			Query tmpq(jq._namespace);
			tmpq.Limit(0);
			SelectCtx ctx(tmpq, lockUpgrader);
			jns->Select(result, ctx);
		}

	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Commit(const string& _namespace) {
	try {
		getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

Error Reindexer::ConfigureIndex(const string& _namespace, const string& index, const string& config) {
	try {
		getNamespace(_namespace)->ConfigureIndex(index, config);

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

shared_ptr<Namespace> Reindexer::getNamespace(const string& _namespace) {
#ifndef REINDEX_SINGLETHREAD
	shared_lock<shared_timed_mutex> lock(ns_mutex);
#endif
	auto nsIt = namespaces.find(_namespace);

	if (nsIt == namespaces.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", _namespace.c_str());
	}

	assert(nsIt->second);
	return nsIt->second;
}
Error Reindexer::ResetStats() {
	memset(&local_stat, 0, sizeof(local_stat));
	return Error(errOK);
}
Error Reindexer::GetStats(reindexer_stat& stat) {
	stat = local_stat;
	return Error(errOK);
}

Error Reindexer::AddIndex(const string& _namespace, const IndexDef& idx) {
	try {
		auto ns = getNamespace(_namespace);
		bool res = ns->AddIndex(idx.name, idx.jsonPath, idx.Type(), idx.opts);
		if (res) {
			logPrintf(LogInfo, "Reloading namespace %s, index '%s' jsonPath=%s changed\n", _namespace.c_str(), idx.name.c_str(),
					  idx.jsonPath.c_str());
			// TODO: do not reload from disk, just do smart rebuild in-memory
			ns = nullptr;
			auto err = CloseNamespace(_namespace);
			assertf(err.ok(), "%s", err.what().c_str());
			err = OpenNamespace(_namespace, StorageOpts().Enabled());
			return err;
		}

	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) {
#ifndef REINDEX_SINGLETHREAD
	shared_lock<shared_timed_mutex> lock(ns_mutex);
#endif

	for (auto& ns : namespaces) {
		defs.push_back(ns.second->GetDefinition());
	}

	if (bEnumAll && !storagePath_.empty()) {
		vector<DirEntry> dirs;
		if (reindexer::ReadDir(storagePath_, dirs) != 0) return Error(errLogic, "Could not read database dir");

		for (auto& d : dirs) {
			if (d.isDir && d.name != "." && d.name != ".." && namespaces.find(d.name) == namespaces.end()) {
				string dbpath = JoinPath(storagePath_, d.name);
				unique_ptr<Namespace> tmpNs(new Namespace(d.name));
				try {
					tmpNs->EnableStorage(storagePath_, StorageOpts());
					defs.push_back(tmpNs->GetDefinition());
				} catch (reindexer::Error) {
				}
			}
		}
	}
	return 0;
}

void Reindexer::flusherThread() {
	vector<string> nsarray;
	while (!stopFlusher_) {
		nsarray.clear();
		{
			shared_lock<shared_timed_mutex> lock(ns_mutex);
			for (auto ns : namespaces) nsarray.push_back(ns.first);
		}

		for (auto name : nsarray) {
			try {
				auto ns = getNamespace(name);
				ns->FlushStorage();
			} catch (...) {
			}
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}
}  // namespace reindexer
