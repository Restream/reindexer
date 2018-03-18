#include "core/reindexerimpl.h"
#include <stdio.h>
#include <chrono>
#include <thread>
#include "core/cjson/jsondecoder.h"
#include "core/selectfunc/selectfunc.h"
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

static thread_local reindexer_stat local_stat;

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

ReindexerImpl::ReindexerImpl() { stopFlusher_ = false; }

ReindexerImpl::~ReindexerImpl() {
	if (storagePath_.length()) {
		stopFlusher_ = true;
		flusher_.join();
	}
}

const char* kStoragePlaceholderFilename = ".reindexer.storage";

Error ReindexerImpl::EnableStorage(const string& storagePath, bool skipPlaceholderCheck) {
	if (!storagePath_.empty()) {
		return Error(errParams, "Storage already enabled\n");
	}

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

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef) {
	shared_ptr<Namespace> ns;
	try {
		{
			lock_guard<shared_timed_mutex> lock(ns_mutex);
			if (namespaces.find(nsDef.name) != namespaces.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name.c_str());
			}
		}
		if (!validateObjectName(nsDef.name.c_str())) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
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
		lock_guard<shared_timed_mutex> lock(ns_mutex);
		namespaces.insert({nsDef.name, ns});
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
Error ReindexerImpl::OpenNamespace(const string& name, const StorageOpts& storage) {
	shared_ptr<Namespace> ns;
	try {
		{
			lock_guard<shared_timed_mutex> lock(ns_mutex);
			if (namespaces.find(name) != namespaces.end()) {
				return 0;
			}
		}
		if (!validateObjectName(name.c_str())) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
		}
		ns = std::make_shared<Namespace>(name);
		if (storage.IsEnabled() && !storagePath_.empty()) {
			ns->EnableStorage(storagePath_, storage);
			ns->LoadFromStorage();
		}
		lock_guard<shared_timed_mutex> lock(ns_mutex);
		namespaces.insert({name, ns});
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::DropNamespace(const string& _namespace) { return closeNamespace(_namespace, true); }
Error ReindexerImpl::CloseNamespace(const string& _namespace) { return closeNamespace(_namespace, false); }

Error ReindexerImpl::closeNamespace(const string& _namespace, bool dropStorage) {
	shared_ptr<Namespace> ns;
	try {
		lock_guard<shared_timed_mutex> lock(ns_mutex);
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

Error ReindexerImpl::Insert(const string& _namespace, Item& item) {
	STAT_FUNC(insert);
	try {
		auto ns = getNamespace(_namespace);
		ns->Insert(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::Update(const string& _namespace, Item& item) {
	STAT_FUNC(update);
	try {
		auto ns = getNamespace(_namespace);
		ns->Update(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::Upsert(const string& _namespace, Item& item) {
	STAT_FUNC(upsert);
	try {
		auto ns = getNamespace(_namespace);
		ns->Upsert(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Item ReindexerImpl::NewItem(const string& _namespace) {
	try {
		return getNamespace(_namespace)->NewItem();
	} catch (const Error& err) {
		return Item(err);
	}
}

Error ReindexerImpl::GetMeta(const string& _namespace, const string& key, string& data) {
	try {
		data = getNamespace(_namespace)->GetMeta(key);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::PutMeta(const string& _namespace, const string& key, const Slice& data) {
	try {
		getNamespace(_namespace)->PutMeta(key, data);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::EnumMeta(const string& _namespace, vector<string>& keys) {
	try {
		keys = getNamespace(_namespace)->EnumMeta();
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::Delete(const string& _namespace, Item& item) {
	STAT_FUNC(delete);
	try {
		auto ns = getNamespace(_namespace);
		ns->Delete(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
Error ReindexerImpl::Delete(const Query& q, QueryResults& result) {
	STAT_FUNC(delete);
	try {
		auto ns = getNamespace(q._namespace);
		ns->Delete(q, result);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error ReindexerImpl::Select(const string& query, QueryResults& result) {
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

Error ReindexerImpl::Select(const Query& q, QueryResults& result) {
	NsLocker locks;

	if (!q.joinQueries_.empty() && !q.mergeQueries_.empty()) {
		return Error(errParams, "Merge and join can't be in same query");
	}

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
			result.lockResults();
			for (auto& name : namespaceNames) {
				getNamespace(name)->Describe(result);
			}
			return 0;
		}

		// Loockup and lock namespaces
		locks.Add(getNamespace(q._namespace));
		for (auto& jq : q.joinQueries_) locks.Add(getNamespace(jq._namespace));
		for (auto& mq : q.mergeQueries_) locks.Add(getNamespace(mq._namespace));
		locks.Lock();
	} catch (const Error& err) {
		return err;
	}

	for (;;) {
		try {
			SelectFunctionsHolder func;
			h_vector<Query, 4> queries;
			JoinedSelectors joinedSelectors = prepareJoinedSelectors(q, result, locks, queries, func);
			doSelect(q, result, joinedSelectors, locks, func);
			result.lockResults();
			func.Process(result);

			break;
		} catch (const Error& err) {
			if (err.code() == errWasRelock) {
				result = QueryResults();
				logPrintf(LogInfo, "Was lock upgrade in multi namespaces query. Retrying");
				continue;
			} else {
				return err;
			}
		}
	}
	return 0;
}

JoinedSelectors ReindexerImpl::prepareJoinedSelectors(const Query& q, QueryResults& result, NsLocker& locks, h_vector<Query, 4>& queries,
													  SelectFunctionsHolder& func) {
	JoinedSelectors joinedSelectors;
	queries.reserve(q.joinQueries_.size());
	auto ns = locks.Get(q._namespace);

	if (!q.joinQueries_.empty()) {
		result.joined_.reset(new unordered_map<IdType, QRVector>());
	}
	// For each joined queries
	for (auto& jq : q.joinQueries_) {
		// Get common results from joined namespaces
		auto jns = locks.Get(jq._namespace);

		Query jjq(jq);

		SelectCtx::PreResult::Ptr preResult;
		if (jjq.entries.size()) {
			QueryResults jr;
			jjq.sortDirDesc = false;
			jjq.Limit(UINT_MAX);
			SelectCtx ctx(jjq, &locks);
			ctx.preResult = preResult = std::make_shared<SelectCtx::PreResult>();
			ctx.preResult->mode = SelectCtx::PreResult::ModeBuild;
			ctx.functions = &func;
			jns->Select(jr, ctx);
			assert(ctx.preResult->mode != SelectCtx::PreResult::ModeBuild);
		}

		// Do join for each item in main result
		size_t pos = joinedSelectors.size();
		Query jItemQ(jq._namespace);
		jItemQ.Debug(jq.debugLevel).Limit(jq.count).Sort(jjq.sortBy.c_str(), jq.sortDirDesc);

		jItemQ.entries.reserve(jq.joinEntries_.size());

		// Construct join conditions
		for (auto& je : jq.joinEntries_) {
			QueryEntry qe(je.op_, je.condition_, je.joinIndex_, jns->getIndexByName(je.joinIndex_));
			const_cast<QueryJoinEntry&>(je).idxNo = ns->getIndexByName(je.index_);
			jItemQ.entries.push_back(qe);
		}
		queries.push_back(std::move(jItemQ));
		Query* pjItemQ = &queries.back();

		auto joinedSelector = [&result, &jq, jns, preResult, pos, pjItemQ, &locks, &func](IdType id, ConstPayload payload, bool match) {
			local_stat.count_join++;  // Do not measure each join time (expensive). Just give count
			// Put values to join conditions
			int cnt = 0;
			for (auto& je : jq.joinEntries_) {
				payload.Get(je.idxNo, pjItemQ->entries[cnt].values);
				cnt++;
			}
			pjItemQ->Limit(match ? jq.count : 0);
			QueryResults joinItemR;

			SelectCtx ctx(*pjItemQ, &locks);
			if (jq.entries.size()) ctx.preResult = preResult;
			ctx.matchedAtLeastOnce = false;
			ctx.reqMatchedOnceFlag = true;
			ctx.skipIndexesLookup = true;
			ctx.functions = &func;
			jns->Select(joinItemR, ctx);

			bool found = joinItemR.size();
			if (match && found) {
				auto& jres = result.joined_->emplace(id, QRVector()).first->second;

				if (pos >= jres.size()) jres.resize(pos + 1);

				jres[pos] = std::move(joinItemR);
			}
			return ctx.matchedAtLeastOnce;
		};
		joinedSelectors.push_back({jq.joinType, jq.count == 0, joinedSelector});
	}
	return joinedSelectors;
}

void ReindexerImpl::doSelect(const Query& q, QueryResults& result, JoinedSelectors& joinedSelectors, NsLocker& locks,
							 SelectFunctionsHolder& func) {
	auto ns = locks.Get(q._namespace);
	if (!ns) {
		throw Error(errParams, "Namespace '%s' is not exists", q._namespace.c_str());
	}

	{
		STAT_FUNC(select);
		SelectCtx ctx(q, &locks);
		ctx.functions = &func;
		ctx.joinedSelectors = &joinedSelectors;
		ctx.nsid = 0;
		ctx.isForceAll = !q.mergeQueries_.empty() || !q.forcedSortOrder.empty();
		ns->Select(result, ctx);
	}
	if (!q.mergeQueries_.empty()) {
		uint8_t counter = 0;

		for (auto& mq : q.mergeQueries_) {
			auto mns = locks.Get(mq._namespace);
			STAT_FUNC(select);
			SelectCtx ctx(mq, &locks);
			ctx.nsid = ++counter;
			ctx.isForceAll = true;
			ctx.functions = &func;

			mns->Select(result, ctx);
		}

		if (static_cast<size_t>(q.start) >= result.size()) {
			result.Erase(result.begin(), result.end());
			return;
		}

		std::sort(result.begin(), result.end(), ItemRefLess());
		if (q.calcTotal) {
			result.totalCount = result.size();
		}

		if (q.start > 0) {
			auto end = q.start < result.size() ? result.begin() + q.start : result.end();
			result.Erase(result.begin(), end);
		}

		if (result.size() > q.count) {
			result.Erase(result.begin() + q.count, result.end());
		}

		if (q.start) {
			result.Erase(result.begin(), result.begin() + q.start);
		}
		if (static_cast<size_t>(q.count) < result.size()) {
			result.Erase(result.begin() + q.count, result.end());
		}
	}
	// dummy selects for put ctx-es
	for (auto& jq : q.joinQueries_) {
		auto jns = locks.Get(jq._namespace);
		Query tmpq(jq._namespace);
		tmpq.Limit(0);
		SelectCtx ctx(tmpq, &locks);
		ctx.functions = &func;
		jns->Select(result, ctx);
	}
}

Error ReindexerImpl::Commit(const string& _namespace) {
	try {
		getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

Error ReindexerImpl::ConfigureIndex(const string& _namespace, const string& index, const string& config) {
	try {
		getNamespace(_namespace)->ConfigureIndex(index, config);

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

shared_ptr<Namespace> ReindexerImpl::getNamespace(const string& _namespace) {
	shared_lock<shared_timed_mutex> lock(ns_mutex);
	auto nsIt = namespaces.find(_namespace);

	if (nsIt == namespaces.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", _namespace.c_str());
	}

	assert(nsIt->second);
	return nsIt->second;
}
Error ReindexerImpl::ResetStats() {
	memset(&local_stat, 0, sizeof(local_stat));
	return Error(errOK);
}
Error ReindexerImpl::GetStats(reindexer_stat& stat) {
	stat = local_stat;
	return Error(errOK);
}

Error ReindexerImpl::AddIndex(const string& _namespace, const IndexDef& idx) {
	try {
		auto ns = getNamespace(_namespace);
		ns->AddIndex(idx.name, idx.jsonPath, idx.Type(), idx.opts);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::DropIndex(const string& _namespace, const string& index) {
	try {
		auto ns = getNamespace(_namespace);
		ns->DropIndex(index);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) {
	shared_lock<shared_timed_mutex> lock(ns_mutex);

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

void ReindexerImpl::flusherThread() {
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
