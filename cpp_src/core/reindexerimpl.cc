#include "core/reindexerimpl.h"
#include <stdio.h>
#include <chrono>
#include <thread>
#include "core/cjson/jsondecoder.h"
#include "core/namespacedef.h"
#include "core/selectfunc/selectfunc.h"
#include "kx/kxsort.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"
using std::lock_guard;
using std::string;
using std::vector;
using namespace std::placeholders;

const char* kPerfStatsNamespace = "#perfstats";
const char* kQueriesPerfStatsNamespace = "#queriesperfstats";
const char* kMemStatsNamespace = "#memstats";
const char* kNamespacesNamespace = "#namespaces";
const char* kConfigNamespace = "#config";
const char* kStoragePlaceholderFilename = ".reindexer.storage";

namespace reindexer {

ReindexerImpl::ReindexerImpl() : profConfig_(std::make_shared<DBProfilingConfig>()) { stopFlusher_ = false; }

ReindexerImpl::~ReindexerImpl() {
	if (storagePath_.length()) {
		stopFlusher_ = true;
		flusher_.join();
	}
}

Error ReindexerImpl::EnableStorage(const string& storagePath, bool skipPlaceholderCheck) {
	if (!storagePath_.empty()) {
		return Error(errParams, "Storage already enabled\n");
	}

	storagePath_.clear();
	if (storagePath.empty()) return errOK;
	if (fs::MkDirAll(storagePath) < 0) {
		return Error(errParams, "Can't create directory '%s' for reindexer storage - reason %s", storagePath.c_str(), strerror(errno));
	}

	vector<fs::DirEntry> dirEntries;
	bool isEmpty = true;
	if (fs::ReadDir(storagePath, dirEntries) < 0) {
		return Error(errParams, "Can't read contents of directory '%s' for reindexer storage - reason %s", storagePath.c_str(),
					 strerror(errno));
	}
	for (auto& entry : dirEntries) {
		if (entry.name != "." && entry.name != ".." && entry.name != kStoragePlaceholderFilename) {
			isEmpty = false;
		}
	}

	if (!isEmpty && !skipPlaceholderCheck) {
		FILE* f = fopen(fs::JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "r");
		if (f) {
			fclose(f);
		} else {
			return Error(errParams, "Cowadly refusing to use directory '%s' - it's not empty, and doesn't contains reindexer placeholder",
						 storagePath.c_str());
		}
	} else {
		FILE* f = fopen(fs::JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "w");
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
			lock_guard<shared_timed_mutex> lock(mtx_);
			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name.c_str());
			}
		}
		if (!validateObjectName(nsDef.name.c_str())) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
		}
		for (;;) {
			ns = std::make_shared<Namespace>(nsDef.name, nsDef.cacheMode);
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
		if (nsDef.storage.IsEnabled() && !storagePath_.empty()) {
			ns->LoadFromStorage();
		}
		lock_guard<shared_timed_mutex> lock(mtx_);
		namespaces_.insert({nsDef.name, ns});
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::OpenNamespace(const string& name, const StorageOpts& storage, CacheMode cacheMode) {
	shared_ptr<Namespace> ns;
	try {
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			auto it = namespaces_.find(name);
			if (it != namespaces_.end()) {
				it->second->SetCacheMode(cacheMode);
				return 0;
			}
		}
		if (!validateObjectName(name.c_str())) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
		}
		ns = std::make_shared<Namespace>(name, cacheMode);
		if (storage.IsEnabled() && !storagePath_.empty()) {
			ns->EnableStorage(storagePath_, storage);
			ns->LoadFromStorage();
		}
		lock_guard<shared_timed_mutex> lock(mtx_);
		namespaces_.insert({name, ns});
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::DropNamespace(const string& _namespace) { return closeNamespace(_namespace, true); }
Error ReindexerImpl::CloseNamespace(const string& _namespace) { return closeNamespace(_namespace, false); }

Error ReindexerImpl::closeNamespace(const string& _namespace, bool dropStorage) {
	shared_ptr<Namespace> ns;
	try {
		lock_guard<shared_timed_mutex> lock(mtx_);
		auto nsIt = namespaces_.find(_namespace);

		if (nsIt == namespaces_.end()) {
			return Error(errParams, "Namespace '%s' does not exist", _namespace.c_str());
		}

		// Temporary save namespace. This will call destructor without lock
		ns = nsIt->second;
		namespaces_.erase(nsIt);
		if (dropStorage) {
			ns->DeleteStorage();
		} else {
			ns->CloseStorage();
		}
	} catch (const Error& err) {
		ns = nullptr;
		return err;
	}
	// Here will called destructor
	ns = nullptr;
	return errOK;
}

Error ReindexerImpl::Insert(const string& _namespace, Item& item) {
	try {
		auto ns = getNamespace(_namespace);
		ns->Insert(item);
		updateSystemNamespace(_namespace, item);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Update(const string& _namespace, Item& item) {
	try {
		auto ns = getNamespace(_namespace);
		ns->Update(item);
		updateSystemNamespace(_namespace, item);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Upsert(const string& _namespace, Item& item) {
	try {
		auto ns = getNamespace(_namespace);
		ns->Upsert(item);
		updateSystemNamespace(_namespace, item);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
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
	return errOK;
}

Error ReindexerImpl::PutMeta(const string& _namespace, const string& key, const string_view& data) {
	try {
		getNamespace(_namespace)->PutMeta(key, data);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::EnumMeta(const string& _namespace, vector<string>& keys) {
	try {
		keys = getNamespace(_namespace)->EnumMeta();
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Delete(const string& _namespace, Item& item) {
	try {
		auto ns = getNamespace(_namespace);
		ns->Delete(item);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}
Error ReindexerImpl::Delete(const Query& q, QueryResults& result) {
	try {
		auto ns = getNamespace(q._namespace);
		ns->Delete(q, result);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Select(const string& query, QueryResults& result) {
	try {
		Query q;
		q.Parse(query);
		return Select(q, result);

	} catch (const Error& err) {
		return err;
	}

	return errOK;
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

	Namespace::Ptr mainNs;

	try {
		mainNs = getNamespace(q._namespace);
	} catch (const Error& err) {
		return err;
	}

	mtx_.lock_shared();
	auto profCfg = profConfig_;
	mtx_.unlock_shared();

	PerfStatCalculatorMT calc(mainNs->selectPerfCounter_, mainNs->enablePerfCounters_);  // todo more accurate detect joined queries
	auto& tracker = queriesStatTracker_;
	QueryStatCalculator statCalculator(
		[&q, &tracker](bool lockHit, std::chrono::microseconds time) {
			if (lockHit)
				tracker.LockHit(q, time);
			else
				tracker.Hit(q, time);
		},
		std::chrono::microseconds(profCfg->queriedThresholdUS), profCfg->queriesPerfStats);

	try {
		if (q._namespace.size() && q._namespace[0] == '#') syncSystemNamespaces(q._namespace);
		// Loockup and lock namespaces_
		locks.Add(mainNs);

		for (auto& jq : q.joinQueries_) locks.Add(getNamespace(jq._namespace));
		for (auto& mq : q.mergeQueries_) locks.Add(getNamespace(mq._namespace));
		locks.Lock();
	} catch (const Error& err) {
		return err;
	}
	calc.LockHit();
	statCalculator.LockHit();
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
				logPrintf(LogInfo, "Was lock upgrade in multi namespaces_ query. Retrying");
				continue;
			} else {
				return err;
			}
		}
	}
	return errOK;
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
		// Get common results from joined namespaces_
		auto jns = locks.Get(jq._namespace);

		Query jjq(jq);

		SelectCtx::PreResult::Ptr preResult;
		size_t pos = joinedSelectors.size();

		JoinCacheRes joinRes;
		joinRes.key.SetData(0, jq);
		jns->GetFromJoinCache(joinRes);
		Query* pjItemQ = nullptr;
		if (jjq.entries.size() && !joinRes.haveData) {
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
		if (joinRes.haveData) {
			preResult = joinRes.it.val.preResult;
		} else if (joinRes.needPut) {
			jns->PutToJoinCache(joinRes, preResult);
		}

		// Do join for each item in main result
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
		pjItemQ = &queries.back();

		auto joinedSelector = [&result, &jq, jns, preResult, pos, pjItemQ, &locks, &func, ns](JoinCacheRes& joinRes, IdType id,
																							  ConstPayload payload, bool match) {
			QueryResults joinItemR;

			JoinCacheRes finalJoinRes;

			// Put values to join conditions
			int cnt = 0;
			for (auto& je : jq.joinEntries_) {
				payload.Get(je.idxNo, pjItemQ->entries[cnt].values);
				cnt++;
			}
			pjItemQ->Limit(match ? jq.count : 0);

			bool found = false;
			bool matchedAtLeastOnce = false;
			JoinCacheRes joinResLong;
			joinResLong.key.SetData(jq, *pjItemQ);
			jns->GetFromJoinCache(joinResLong);

			jns->GetIndsideFromJoinCache(joinRes);
			if (joinRes.needPut) {
				jns->PutToJoinCache(joinRes, preResult);
			}
			if (joinResLong.haveData) {
				found = joinResLong.it.val.ids_->size();
				matchedAtLeastOnce = joinResLong.it.val.matchedAtLeastOnce;
				jns->FillResult(joinItemR, joinResLong.it.val.ids_, pjItemQ->selectFilter_);
			} else {
				SelectCtx ctx(*pjItemQ, &locks);
				if (jq.entries.size()) ctx.preResult = preResult;
				ctx.matchedAtLeastOnce = false;
				ctx.reqMatchedOnceFlag = true;
				ctx.skipIndexesLookup = true;
				ctx.functions = &func;
				jns->Select(joinItemR, ctx);

				found = joinItemR.size();
				matchedAtLeastOnce = ctx.matchedAtLeastOnce;
			}
			if (joinResLong.needPut) {
				JoinCacheVal val;
				val.ids_ = std::make_shared<IdSet>();
				val.matchedAtLeastOnce = matchedAtLeastOnce;
				for (auto& r : joinItemR) {
					val.ids_->Add(r.id, IdSet::Unordered);
				}
				jns->PutToJoinCache(joinResLong, val);
			}
			if (match && found) {
				auto& jres = result.joined_->emplace(id, QRVector()).first->second;

				if (pos >= jres.size()) jres.resize(pos + 1);

				jres[pos] = std::move(joinItemR);
			}
			return matchedAtLeastOnce;
		};
		auto cache_func_selector = std::bind(joinedSelector, std::move(joinRes), _1, _2, _3);

		joinedSelectors.push_back({jq.joinType, jq.count == 0, cache_func_selector, 0, 0, jns->name_});
	}
	return joinedSelectors;
}

void ReindexerImpl::doSelect(const Query& q, QueryResults& result, JoinedSelectors& joinedSelectors, NsLocker& locks,
							 SelectFunctionsHolder& func) {
	auto ns = locks.Get(q._namespace);
	if (!ns) {
		throw Error(errParams, "Namespace '%s' is not exists", q._namespace.c_str());
	}
	SelectCtx ctx(q, &locks);

	{
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
		tmpq.selectFilter_ = jq.selectFilter_;
		SelectCtx jctx(tmpq, &locks);
		jctx.functions = &func;
		jns->Select(result, jctx);
	}
}

Error ReindexerImpl::Commit(const string& _namespace) {
	try {
		getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::ConfigureIndex(const string& _namespace, const string& index, const string& config) {
	try {
		getNamespace(_namespace)->ConfigureIndex(index, config);

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

shared_ptr<Namespace> ReindexerImpl::getNamespace(const string& _namespace) {
	shared_lock<shared_timed_mutex> lock(mtx_);
	auto nsIt = namespaces_.find(_namespace);

	if (nsIt == namespaces_.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", _namespace.c_str());
	}

	assert(nsIt->second);
	return nsIt->second;
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
std::vector<Namespace::Ptr> ReindexerImpl::getNamespaces() {
	shared_lock<shared_timed_mutex> lock(mtx_);
	std::vector<Namespace::Ptr> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.push_back(ns.second);
	}
	return ret;
}

std::vector<string> ReindexerImpl::getNamespacesNames() {
	shared_lock<shared_timed_mutex> lock(mtx_);
	std::vector<string> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) ret.push_back(ns.first);

	return ret;
}

Error ReindexerImpl::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) {
	auto nsarray = getNamespaces();
	for (auto& ns : nsarray) {
		defs.push_back(ns->GetDefinition());
	}

	if (bEnumAll && !storagePath_.empty()) {
		vector<fs::DirEntry> dirs;
		if (fs::ReadDir(storagePath_, dirs) != 0) return Error(errLogic, "Could not read database dir");

		for (auto& d : dirs) {
			if (d.isDir && d.name != "." && d.name != ".." && namespaces_.find(d.name) == namespaces_.end()) {
				string dbpath = fs::JoinPath(storagePath_, d.name);
				unique_ptr<Namespace> tmpNs(new Namespace(d.name, CacheMode::CacheModeOn));
				try {
					tmpNs->EnableStorage(storagePath_, StorageOpts());
					defs.push_back(tmpNs->GetDefinition());
				} catch (reindexer::Error) {
				}
			}
		}
	}
	return errOK;
}

void ReindexerImpl::flusherThread() {
	auto nsFlush = [&]() {
		auto nsarray = getNamespacesNames();
		for (auto name : nsarray) {
			try {
				auto ns = getNamespace(name);
				ns->FlushStorage();
			} catch (...) {
			}
		}
	};

	while (!stopFlusher_) {
		nsFlush();
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	nsFlush();
}

void ReindexerImpl::createSystemNamespaces() {
	AddNamespace(NamespaceDef(kPerfStatsNamespace, StorageOpts())
					 .AddIndex("name", "name", "hash", "string", IndexOpts().PK())
					 .AddIndex("updates.total_queries_count", "updates.total_queries_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("updates.total_avg_latency_us", "updates.total_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("updates.last_sec_qps", "updates.last_sec_qps", "-", "int64", IndexOpts().Dense())
					 .AddIndex("updates.last_sec_avg_latency_us", "updates.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.total_queries_count", "selects.total_queries_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.total_avg_latency_us", "selects.total_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.last_sec_qps", "selects.last_sec_qps", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.last_sec_avg_latency_us", "selects.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense()));

	AddNamespace(NamespaceDef(kQueriesPerfStatsNamespace, StorageOpts())
					 .AddIndex("query", "query", "hash", "string", IndexOpts().PK())
					 .AddIndex("total_queries_count", "total_queries_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total_avg_latency_us", "total_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total_avg_lock_time_us", "total_avg_lock_time_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("last_sec_qps", "last_sec_qps", "-", "int64", IndexOpts().Dense())
					 .AddIndex("last_sec_avg_latency_us", "last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("last_sec_avg_lock_time_us", "last_sec_avg_lock_time_us", "-", "int64", IndexOpts().Dense()));

	AddNamespace(NamespaceDef(kNamespacesNamespace, StorageOpts()).AddIndex("name", "name", "hash", "string", IndexOpts().PK()));

	AddNamespace(NamespaceDef(kPerfStatsNamespace, StorageOpts()).AddIndex("name", "name", "hash", "string", IndexOpts().PK()));

	AddNamespace(NamespaceDef(kMemStatsNamespace, StorageOpts())
					 .AddIndex("name", "name", "hash", "string", IndexOpts().PK())
					 .AddIndex("items_count", "items_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("data_size", "data_size", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total.data_size", "total.data_size", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total.indexes_size", "total.indexes_size", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total.cache_size", "total.cache_size", "-", "int64", IndexOpts().Dense()));

	AddNamespace(NamespaceDef(kConfigNamespace, StorageOpts().Enabled().CreateIfMissing())
					 .AddIndex("type", "type", "hash", "string", IndexOpts().PK()));
}

std::vector<string> defDBConfig = {
	R"json({
		"type":"profiling", 
		"profiling":{
			"queriesperfstats":false,
			"queries_threshold_us":10,
			"perfstats":false,
			"memstats":true
		}
	})json",
	R"json({
		"type":"log_queries", 
		"log_queries":[
			{"namespace":"*","log_level":"none"}
	]})json",
};

Error ReindexerImpl::InitSystemNamespaces() {
	createSystemNamespaces();

	Query q(kConfigNamespace);
	QueryResults results;
	auto err = Select(q, results);
	if (!err.ok()) return err;

	if (results.size() == 0) {
		for (auto conf : defDBConfig) {
			Item item = NewItem(kConfigNamespace);
			if (!item.Status().ok()) return item.Status();
			err = item.FromJSON(conf);
			if (!err.ok()) return err;
			err = Insert(kConfigNamespace, item);
			if (!err.ok()) return err;
		}
		results = QueryResults();
		err = Select(q, results);
		if (!err.ok()) return err;
	}

	for (int i = 0; i < int(results.size()); i++) {
		auto item = results.GetItem(i);
		updateSystemNamespace(kConfigNamespace, item);
	}
	return errOK;
}

void ReindexerImpl::syncSystemNamespaces(const string& name) {
	auto nsarray = getNamespaces();
	WrSerializer ser;

	auto forEachNS = [&](Namespace::Ptr sysNs, std::function<void(Namespace::Ptr ns)> filler) {
		for (auto& ns : nsarray) {
			ser.Reset();
			filler(ns);
			auto item = sysNs->NewItem();
			auto err = item.FromJSON(ser.Slice());
			if (!err.ok()) throw err;
			sysNs->Upsert(item);
		}
	};
	mtx_.lock_shared();
	auto profCfg = profConfig_;
	mtx_.unlock_shared();

	if (profCfg->perfStats && (name.empty() || name == kPerfStatsNamespace)) {
		forEachNS(getNamespace(kPerfStatsNamespace), [&](Namespace::Ptr ns) { ns->GetPerfStat().GetJSON(ser); });
	}

	if (profCfg->memStats && (name.empty() || name == kMemStatsNamespace)) {
		forEachNS(getNamespace(kMemStatsNamespace), [&](Namespace::Ptr ns) { ns->GetMemStat().GetJSON(ser); });
	}

	if (name.empty() || name == kNamespacesNamespace) {
		forEachNS(getNamespace(kNamespacesNamespace), [&](Namespace::Ptr ns) { ns->GetDefinition().GetJSON(ser, true); });
	}

	if (profCfg->queriesPerfStats && (name.empty() || name == kQueriesPerfStatsNamespace)) {
		auto queriesperfstatsNs = getNamespace(kQueriesPerfStatsNamespace);
		auto data = queriesStatTracker_.Data();
		for (auto& stat : data) {
			ser.Reset();
			stat.GetJSON(ser);
			auto item = queriesperfstatsNs->NewItem();
			auto err = item.FromJSON(ser.Slice());
			if (!err.ok()) throw err;
			queriesperfstatsNs->Upsert(item);
		}
	}
}

void ReindexerImpl::updateSystemNamespace(const string& nsName, Item& item) {
	if (nsName == kConfigNamespace) {
		auto json = item.GetJSON();
		JsonAllocator jalloc;
		JsonValue jvalue;
		char* endp;

		int status = jsonParse(const_cast<char*>(json.data()), &endp, &jvalue, jalloc);
		if (status != JSON_OK) {
			throw Error(errParseJson, "Malformed JSON with config");
		}
		for (auto elem : jvalue) {
			if (!strcmp(elem->key, "profiling")) {
				// reset profiling namespaces_
				QueryResults qr1, qr2, qr3;
				Delete(Query(kMemStatsNamespace), qr2);
				Delete(Query(kQueriesPerfStatsNamespace), qr3);
				Delete(Query(kPerfStatsNamespace), qr1);

				auto cfg = std::make_shared<DBProfilingConfig>();
				auto err = cfg->FromJSON(elem->value);
				if (!err.ok()) throw err;
				mtx_.lock();
				profConfig_ = cfg;
				mtx_.unlock();

				auto nsarray = getNamespaces();
				for (auto& ns : nsarray) {
					ns->EnablePerfCounters(cfg->perfStats);
				}

			} else if (!strcmp(elem->key, "log_queries")) {
				DBLoggingConfig cfg;
				auto err = cfg.FromJSON(elem->value);
				if (!err.ok()) throw err;
				LogLevel defLogLevel = LogNone;
				if (cfg.logQueries.find("*") != cfg.logQueries.end()) {
					defLogLevel = LogLevel(cfg.logQueries.find("*")->second);
				}

				auto nsarray = getNamespaces();
				for (auto& ns : nsarray) {
					LogLevel logLevel = defLogLevel;
					if (cfg.logQueries.find(ns->GetName()) != cfg.logQueries.end()) {
						logLevel = LogLevel(cfg.logQueries.find(ns->GetName())->second);
					}
					ns->SetQueriesLogLevel(logLevel);
				}
			}
		}
	};
}

}  // namespace reindexer
