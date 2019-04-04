#include "core/reindexerimpl.h"
#include <stdio.h>
#include <chrono>
#include <thread>
#include "cjson/jsonbuilder.h"
#include "core/cjson/jsondecoder.h"
#include "core/index/index.h"
#include "core/itemimpl.h"
#include "core/namespacedef.h"
#include "core/selectfunc/selectfunc.h"
#include "kx/kxsort.h"
#include "replicator/replicator.h"
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
const char* kReplicationConfFilename = "replication.conf";

namespace reindexer {

ReindexerImpl::ReindexerImpl() : replicator_(new Replicator(this)) {
	stopBackgroundThread_ = false;
	configProvider_.setHandler(ProfilingConf, std::bind(&ReindexerImpl::onProfiligConfigLoad, this));
	backgroundThread_ = std::thread([this]() { this->backgroundRoutine(); });
}

ReindexerImpl::~ReindexerImpl() {
	replicator_->Stop();
	stopBackgroundThread_ = true;
	backgroundThread_.join();
}

Error ReindexerImpl::EnableStorage(const string& storagePath, bool skipPlaceholderCheck) {
	if (!storagePath_.empty()) {
		return Error(errParams, "Storage already enabled");
	}

	storagePath_.clear();
	if (storagePath.empty()) return errOK;
	if (fs::MkDirAll(storagePath) < 0) {
		return Error(errParams, "Can't create directory '%s' for reindexer storage - reason %s", storagePath, strerror(errno));
	}

	vector<fs::DirEntry> dirEntries;
	bool isEmpty = true;
	bool isHaveConfig = false;
	if (fs::ReadDir(storagePath, dirEntries) < 0) {
		return Error(errParams, "Can't read contents of directory '%s' for reindexer storage - reason %s", storagePath, strerror(errno));
	}
	for (auto& entry : dirEntries) {
		if (entry.name != "." && entry.name != ".." && entry.name != kStoragePlaceholderFilename) {
			isEmpty = false;
		}
		if (entry.name == kConfigNamespace) isHaveConfig = true;
	}

	if (!isEmpty && !skipPlaceholderCheck) {
		FILE* f = fopen(fs::JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "r");
		if (f) {
			fclose(f);
		} else {
			return Error(errParams, "Cowadly refusing to use directory '%s' - it's not empty, and doesn't contains reindexer placeholder",
						 storagePath);
		}
	} else {
		FILE* f = fopen(fs::JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "w");
		if (f) {
			fwrite("leveldb", 7, 1, f);
			fclose(f);
		} else {
			return Error(errParams, "Can't create placeholder in directory '%s' for reindexer storage - reason %s", storagePath,
						 strerror(errno));
		}
	}

	storagePath_ = storagePath;
	if (isHaveConfig) return OpenNamespace(kConfigNamespace);

	return errOK;
}

Error ReindexerImpl::Connect(const string& dsn) {
	string path = dsn;
	if (dsn.compare(0, 10, "builtin://") == 0) {
		path = dsn.substr(10);
	}

	auto err = EnableStorage(path);
	if (!err.ok()) return err;

	vector<reindexer::fs::DirEntry> foundNs;
	if (fs::ReadDir(path, foundNs) < 0) {
		return Error(errParams, "Can't read database dir %s", path);
	}

	InitSystemNamespaces();

	int maxLoadWorkers = std::min(int(std::thread::hardware_concurrency()), 8);
	std::unique_ptr<std::thread[]> thrs(new std::thread[maxLoadWorkers]);

	for (int i = 0; i < maxLoadWorkers; i++) {
		thrs[i] = std::thread(
			[&](int i) {
				for (int j = i; j < int(foundNs.size()); j += maxLoadWorkers) {
					auto& de = foundNs[j];
					if (de.isDir && validateObjectName(de.name)) {
						auto status = OpenNamespace(de.name, StorageOpts().Enabled());
						if (!status.ok()) {
							logPrintf(LogError, "Failed to open namespace '%s' - %s", de.name, status.what());
						}
					}
				}
			},
			i);
	}
	for (int i = 0; i < maxLoadWorkers; i++) thrs[i].join();
	bool needStart = replicator_->Configure(configProvider_.GetReplicationConfig());
	return needStart ? replicator_->Start() : errOK;
}

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef) {
	shared_ptr<Namespace> ns;
	try {
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name);
			}
		}
		if (!validateObjectName(nsDef.name)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
		}
		bool readyToLoadStorage = (nsDef.storage.IsEnabled() && !storagePath_.empty());
		ns = std::make_shared<Namespace>(nsDef.name, observers_);
		if (readyToLoadStorage) {
			ns->EnableStorage(storagePath_, nsDef.storage);
		}
		ns->onConfigUpdated(configProvider_);
		if (readyToLoadStorage) {
			if (!ns->getStorageOpts().IsLazyLoad()) ns->LoadFromStorage();
		}
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			namespaces_.insert({nsDef.name, ns});
		}
		observers_.OnWALUpdate(0, nsDef.name, WALRecord(WalNamespaceAdd));
		for (auto& indexDef : nsDef.indexes) ns->AddIndex(indexDef);

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::OpenNamespace(string_view name, const StorageOpts& storageOpts) {
	shared_ptr<Namespace> ns;
	try {
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			auto it = namespaces_.find(name);
			if (it != namespaces_.end()) {
				it->second->SetStorageOpts(storageOpts);
				return 0;
			}
		}
		if (!validateObjectName(name)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
		}
		auto nameStr = name.ToString();
		ns = std::make_shared<Namespace>(nameStr, observers_);
		if (storageOpts.IsEnabled() && !storagePath_.empty()) {
			ns->EnableStorage(storagePath_, storageOpts);
			ns->onConfigUpdated(configProvider_);
			if (!ns->getStorageOpts().IsLazyLoad()) ns->LoadFromStorage();
		}
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			namespaces_.insert({nameStr, ns});
		}
		observers_.OnWALUpdate(0, name, WALRecord(WalNamespaceAdd));
	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::DropNamespace(string_view nsName) { return closeNamespace(nsName, true); }
Error ReindexerImpl::CloseNamespace(string_view nsName) { return closeNamespace(nsName, false); }

Error ReindexerImpl::closeNamespace(string_view nsName, bool dropStorage, bool enableDropSlave) {
	shared_ptr<Namespace> ns;
	try {
		lock_guard<shared_timed_mutex> lock(mtx_);
		auto nsIt = namespaces_.find(nsName);

		if (nsIt == namespaces_.end()) {
			return Error(errNotFound, "Namespace '%s' does not exist", nsName);
		}
		// Temporary save namespace. This will call destructor without lock
		ns = nsIt->second;
		if (ns->GetReplState().slaveMode && !enableDropSlave) {
			return Error(errLogic, "Can't modify slave ns '%s'", nsName);
		}

		namespaces_.erase(nsIt);
		if (dropStorage) {
			ns->DeleteStorage();
		} else {
			ns->CloseStorage();
		}
		if (dropStorage) observers_.OnWALUpdate(0, nsName, WALRecord(WalNamespaceDrop));

	} catch (const Error& err) {
		ns = nullptr;
		return err;
	}
	// Here will called destructor
	ns = nullptr;
	return errOK;
}

Error ReindexerImpl::Insert(string_view nsName, Item& item, Completion cmpl) {
	Error err;
	try {
		auto ns = getNamespace(nsName);
		ns->Insert(item);
		if (item.GetID() != -1) {
			updateDbFromConfig(nsName, item);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (cmpl) cmpl(err);
	return err;
}

Error ReindexerImpl::Update(string_view nsName, Item& item, Completion cmpl) {
	Error err;
	try {
		auto ns = getNamespace(nsName);
		ns->Update(item);
		if (item.GetID() != -1) {
			updateDbFromConfig(nsName, item);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (cmpl) cmpl(err);
	return err;
}

Error ReindexerImpl::Update(const Query& q, QueryResults& result) {
	try {
		auto ns = getNamespace(q._namespace);
		ensureDataLoaded(ns);
		ns->Update(q, result);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Upsert(string_view nsName, Item& item, Completion cmpl) {
	Error err;
	try {
		auto ns = getNamespace(nsName);
		ns->Upsert(item);
		if (item.GetID() != -1) {
			updateDbFromConfig(nsName, item);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (cmpl) cmpl(err);
	return err;
}

Item ReindexerImpl::NewItem(string_view nsName) {
	try {
		auto ns = getNamespace(nsName);
		auto item = ns->NewItem();
		item.impl_->SetNamespace(ns);
		return item;
	} catch (const Error& err) {
		return Item(err);
	}
}
Transaction ReindexerImpl::NewTransaction(const std::string& _namespace) {
	TransactionAccessor tr(_namespace, this);

	return std::move(tr);
}

Error ReindexerImpl::CommitTransaction(Transaction& tr) {
	Error err = errOK;
	auto trAccessor = static_cast<TransactionAccessor*>(&tr);

	Namespace::Ptr ns;
	try {
		ns = getNamespace(trAccessor->GetName());

		ns->StartTransaction();
		for (auto& step : tr.impl_->steps_) {
			ns->ApplyTransactionStep(step);
			if (step.item_.GetID() != -1) {
				updateDbFromConfig(trAccessor->GetName(), step.item_);
			}
		}

	} catch (const Error& e) {
		err = e;
	}
	if (ns) ns->EndTransaction();

	if (trAccessor->GetCmpl()) trAccessor->GetCmpl()(err);
	return err;
}
Error ReindexerImpl::RollBackTransaction(Transaction& tr) {
	Error err = errOK;
	auto trAccessor = static_cast<TransactionAccessor*>(&tr);

	trAccessor->GetSteps().clear();

	return err;
}

Error ReindexerImpl::GetMeta(string_view nsName, const string& key, string& data) {
	try {
		data = getNamespace(nsName)->GetMeta(key);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::PutMeta(string_view nsName, const string& key, string_view data) {
	try {
		getNamespace(nsName)->PutMeta(key, data);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::EnumMeta(string_view nsName, vector<string>& keys) {
	try {
		keys = getNamespace(nsName)->EnumMeta();
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Delete(string_view nsName, Item& item, Completion cmpl) {
	Error err;
	try {
		auto ns = getNamespace(nsName);
		ns->Delete(item);
	} catch (const Error& e) {
		err = e;
	}
	if (cmpl) cmpl(err);
	return err;
}
Error ReindexerImpl::Delete(const Query& q, QueryResults& result) {
	try {
		auto ns = getNamespace(q._namespace);
		ensureDataLoaded(ns);
		ns->Delete(q, result);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Select(string_view query, QueryResults& result, Completion cmpl) {
	Error err = errOK;
	try {
		Query q;
		q.FromSQL(query);
		switch (q.type_) {
			case QuerySelect:
				err = Select(q, result);
				break;
			case QueryDelete:
				err = Delete(q, result);
				break;
			case QueryUpdate:
				err = Update(q, result);
				break;
			default:
				throw Error(errParams, "Error unsupported query type %d", q.type_);
		}
	} catch (const Error& e) {
		err = e;
	}

	if (cmpl) cmpl(err);
	return err;
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

Error ReindexerImpl::Select(const Query& q, QueryResults& result, Completion cmpl) {
	NsLocker locks;

	Namespace::Ptr mainNs;

	try {
		mainNs = getNamespace(q._namespace);
	} catch (const Error& err) {
		if (cmpl) cmpl(err);
		return err;
	}

	ProfilingConfigData profilingCfg = configProvider_.GetProfilingConfig();
	PerfStatCalculatorMT calc(mainNs->selectPerfCounter_, mainNs->enablePerfCounters_);  // todo more accurate detect joined queries
	auto& tracker = queriesStatTracker_;
	QueryStatCalculator statCalculator(
		[&q, &tracker](bool lockHit, std::chrono::microseconds time) {
			if (lockHit)
				tracker.LockHit(q, time);
			else
				tracker.Hit(q, time);
		},
		std::chrono::microseconds(profilingCfg.queriedThresholdUS), profilingCfg.queriesPerfStats);

	try {
		if (q._namespace.size() && q._namespace[0] == '#') syncSystemNamespaces(q._namespace);
		// Lookup and lock namespaces_
		ensureDataLoaded(mainNs);
		mainNs->updateSelectTime();
		locks.Add(mainNs);
		q.WalkNested(false, true, [this, &locks](const Query q) {
			auto ns = getNamespace(q._namespace);
			ensureDataLoaded(ns);
			ns->updateSelectTime();
			locks.Add(ns);
		});

		locks.Lock();
	} catch (const Error& err) {
		if (cmpl) cmpl(err);
		return err;
	}
	calc.LockHit();
	statCalculator.LockHit();
	try {
		SelectFunctionsHolder func;
		if (!q.joinQueries_.empty()) {
			result.joined_.resize(1 + q.mergeQueries_.size());
		}

		doSelect(q, result, locks, func);
		func.Process(result);
	} catch (const Error& err) {
		if (cmpl) cmpl(err);
		return err;
	}
	if (cmpl) cmpl(errOK);
	return errOK;
}

JoinedSelectors ReindexerImpl::prepareJoinedSelectors(const Query& q, QueryResults& result, NsLocker& locks, SelectFunctionsHolder& func) {
	JoinedSelectors joinedSelectors;
	if (q.joinQueries_.empty()) return joinedSelectors;
	auto ns = locks.Get(q._namespace);

	// For each joined queries
	for (auto& jq : q.joinQueries_) {
		// Get common results from joined namespaces_
		auto jns = locks.Get(jq._namespace);

		Query jjq(jq);

		JoinPreResult::Ptr preResult = std::make_shared<JoinPreResult>();
		size_t pos = joinedSelectors.size();

		JoinCacheRes joinRes;
		joinRes.key.SetData(jq);
		jns->GetFromJoinCache(joinRes);
		if (jjq.entries.size() && !joinRes.haveData) {
			QueryResults jr;
			jjq.Limit(UINT_MAX);
			SelectCtx ctx(jjq);
			ctx.preResult = preResult;
			ctx.preResult->mode = JoinPreResult::ModeBuild;
			ctx.functions = &func;
			jns->Select(jr, ctx);
			assert(ctx.preResult->mode != JoinPreResult::ModeBuild);
		}
		if (joinRes.haveData) {
			preResult = joinRes.it.val.preResult;
		} else if (joinRes.needPut) {
			jns->PutToJoinCache(joinRes, preResult);
		}

		// Do join for each item in main result
		Query jItemQ(jq._namespace);
		jItemQ.Debug(jq.debugLevel).Limit(jq.count);
		for (size_t i = 0; i < jjq.sortingEntries_.size(); ++i) {
			jItemQ.Sort(jjq.sortingEntries_[i].column, jq.sortingEntries_[i].desc);
		}

		jItemQ.entries.reserve(jq.joinEntries_.size());

		// Construct join conditions
		for (auto& je : jq.joinEntries_) {
			int joinIdx = IndexValueType::NotSet;
			if (!jns->getIndexByName(je.joinIndex_, joinIdx)) {
				joinIdx = IndexValueType::SetByJsonPath;
			}
			QueryEntry qe(je.op_, je.condition_, je.joinIndex_, joinIdx);
			if (!ns->getIndexByName(je.index_, const_cast<QueryJoinEntry&>(je).idxNo)) {
				const_cast<QueryJoinEntry&>(je).idxNo = IndexValueType::SetByJsonPath;
			}
			jItemQ.entries.push_back(qe);
		}

		auto joinedSelector = [&result, &jq, jns, preResult, pos, &func, ns](JoinedSelector* js, IdType id, int nsId, ConstPayload payload,
																			 bool match) {
			QueryResults joinItemR;
			JoinCacheRes finalJoinRes;

			// Put values to join conditions
			int cnt = 0;
			for (auto& je : jq.joinEntries_) {
				bool nonIndexedField = (je.idxNo == IndexValueType::SetByJsonPath);
				bool isIndexSparse = !nonIndexedField && ns->indexes_[je.idxNo]->Opts().IsSparse();
				if (nonIndexedField || isIndexSparse) {
					VariantArray& values = js->query.entries[cnt].values;
					KeyValueType type = values.empty() ? KeyValueUndefined : values[0].Type();
					payload.GetByJsonPath(je.index_, ns->tagsMatcher_, values, type);
				} else {
					payload.Get(je.idxNo, js->query.entries[cnt].values);
				}
				cnt++;
			}
			js->query.Limit(match ? jq.count : 0);

			bool found = false;
			bool matchedAtLeastOnce = false;
			JoinCacheRes joinResLong;
			joinResLong.key.SetData(jq, js->query);
			jns->GetFromJoinCache(joinResLong);

			jns->GetIndsideFromJoinCache(js->joinRes);
			if (js->joinRes.needPut) {
				jns->PutToJoinCache(js->joinRes, preResult);
			}
			if (joinResLong.haveData) {
				found = joinResLong.it.val.ids_->size();
				matchedAtLeastOnce = joinResLong.it.val.matchedAtLeastOnce;
				jns->FillResult(joinItemR, joinResLong.it.val.ids_, js->query.selectFilter_);
			} else {
				SelectCtx ctx(js->query);
				ctx.preResult = preResult;
				ctx.matchedAtLeastOnce = false;
				ctx.reqMatchedOnceFlag = true;
				ctx.skipIndexesLookup = true;
				ctx.functions = &func;
				jns->Select(joinItemR, ctx);

				found = joinItemR.Count();
				matchedAtLeastOnce = ctx.matchedAtLeastOnce;
			}
			if (joinResLong.needPut) {
				JoinCacheVal val;
				val.ids_ = std::make_shared<IdSet>();
				val.matchedAtLeastOnce = matchedAtLeastOnce;
				for (auto& r : joinItemR.Items()) {
					val.ids_->Add(r.id, IdSet::Unordered, 0);
				}
				jns->PutToJoinCache(joinResLong, val);
			}
			if (match && found) {
				auto& jres = result.joined_[nsId].emplace(id, QRVector()).first->second;

				if (pos >= jres.size()) jres.resize(pos + 1);

				jres[pos] = std::move(joinItemR);
			}
			return matchedAtLeastOnce;
		};
		joinedSelectors.push_back({jq.joinType, jq.count == 0, joinedSelector, 0, 0, jns->name_, std::move(joinRes), jItemQ});
	}
	return joinedSelectors;
}

void ReindexerImpl::doSelect(const Query& q, QueryResults& result, NsLocker& locks, SelectFunctionsHolder& func) {
	auto ns = locks.Get(q._namespace);
	if (!ns) {
		throw Error(errParams, "Namespace '%s' is not exists", q._namespace);
	}
	SelectCtx ctx(q);
	int jqCount = q.joinQueries_.size();

	for (auto& mq : q.mergeQueries_) {
		jqCount += mq.joinQueries_.size();
	}

	{
		JoinedSelectors joinedSelectors = prepareJoinedSelectors(q, result, locks, func);
		ctx.functions = &func;
		ctx.joinedSelectors = joinedSelectors.size() ? &joinedSelectors : nullptr;
		ctx.nsid = 0;
		ctx.isForceAll = !q.mergeQueries_.empty() || !q.forcedSortOrder.empty();
		ns->Select(result, ctx);
	}

	if (!q.mergeQueries_.empty()) {
		uint8_t counter = 0;

		for (auto& mq : q.mergeQueries_) {
			auto mns = locks.Get(mq._namespace);
			SelectCtx mctx(mq);
			mctx.nsid = ++counter;
			mctx.isForceAll = true;
			mctx.functions = &func;
			JoinedSelectors joinedSelectors = prepareJoinedSelectors(mq, result, locks, func);
			mctx.joinedSelectors = joinedSelectors.size() ? &joinedSelectors : nullptr;

			mns->Select(result, mctx);
		}

		ItemRefVector& itemRefVec = result.Items();
		if (static_cast<size_t>(q.start) >= itemRefVec.size()) {
			result.Erase(itemRefVec.begin(), itemRefVec.end());
			return;
		}

		std::sort(itemRefVec.begin(), itemRefVec.end(), ItemRefLess());
		if (q.calcTotal) {
			result.totalCount = itemRefVec.size();
		}

		if (q.start > 0) {
			auto end = q.start < itemRefVec.size() ? itemRefVec.begin() + q.start : itemRefVec.end();
			result.Erase(itemRefVec.begin(), end);
		}

		if (itemRefVec.size() > q.count) {
			result.Erase(itemRefVec.begin() + q.count, itemRefVec.end());
		}

		if (q.start) {
			result.Erase(itemRefVec.begin(), itemRefVec.begin() + q.start);
		}
		if (static_cast<size_t>(q.count) < itemRefVec.size()) {
			result.Erase(itemRefVec.begin() + q.count, itemRefVec.end());
		}
	}
	// dummy selects for put ctx-es
	q.WalkNested(false, false, [&locks, &result](const Query& nestesQuery) {
		auto jns = locks.Get(nestesQuery._namespace);
		Query tmpq(nestesQuery._namespace);
		tmpq.Limit(0);
		SelectCtx jctx(tmpq);
		jns->Select(result, jctx);
	});
	result.lockResults();
}

Error ReindexerImpl::Commit(string_view /*_namespace*/) {
	try {
		// getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

shared_ptr<Namespace> ReindexerImpl::getNamespace(string_view nsName) {
	shared_lock<shared_timed_mutex> lock(mtx_);
	auto nsIt = namespaces_.find(nsName);

	if (nsIt == namespaces_.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", nsName);
	}

	assert(nsIt->second);
	return nsIt->second;
}

Error ReindexerImpl::AddIndex(string_view nsName, const IndexDef& indexDef) {
	try {
		auto ns = getNamespace(nsName);
		ns->AddIndex(indexDef);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::UpdateIndex(string_view nsName, const IndexDef& indexDef) {
	try {
		auto ns = getNamespace(nsName);
		ns->UpdateIndex(indexDef);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::DropIndex(string_view nsName, const IndexDef& indexDef) {
	try {
		auto ns = getNamespace(nsName);
		ns->DropIndex(indexDef);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

void ReindexerImpl::ensureDataLoaded(Namespace::Ptr& ns) {
	shared_lock<shared_timed_mutex> readlock(storageMtx_);
	if (ns->needToLoadData()) {
		readlock.unlock();
		lock_guard<shared_timed_mutex> writelock(storageMtx_);
		if (ns->needToLoadData()) ns->LoadFromStorage();
	}
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
			if (d.isDir && d.name != "." && d.name != "..") {
				{
					shared_lock<shared_timed_mutex> lk(mtx_);
					if (namespaces_.find(d.name) != namespaces_.end()) continue;
				}
				unique_ptr<Namespace> tmpNs(new Namespace(d.name, observers_));
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

void ReindexerImpl::backgroundRoutine() {
	auto nsFlush = [&]() {
		auto nsarray = getNamespacesNames();
		for (auto name : nsarray) {
			try {
				auto ns = getNamespace(name);
				ns->tryToReload();
				ns->BackgroundRoutine();
			} catch (Error err) {
				logPrintf(LogWarning, "flusherThread() failed: %s", err.what());
			} catch (...) {
				logPrintf(LogWarning, "flusherThread() failed with ns: %s", name);
			}
		}
	};

	while (!stopBackgroundThread_) {
		nsFlush();
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	nsFlush();
}

void ReindexerImpl::createSystemNamespaces() {
	AddNamespace(NamespaceDef(kConfigNamespace, StorageOpts().Enabled().CreateIfMissing().DropOnFileFormatError())
					 .AddIndex("type", "hash", "string", IndexOpts().PK()));

	AddNamespace(NamespaceDef(kPerfStatsNamespace, StorageOpts())
					 .AddIndex("name", "hash", "string", IndexOpts().PK())
					 .AddIndex("updates.total_queries_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("updates.total_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("updates.last_sec_qps", "-", "int64", IndexOpts().Dense())
					 .AddIndex("updates.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.total_queries_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.total_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.last_sec_qps", "-", "int64", IndexOpts().Dense())
					 .AddIndex("selects.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense()));

	AddNamespace(NamespaceDef(kQueriesPerfStatsNamespace, StorageOpts())
					 .AddIndex("query", "hash", "string", IndexOpts().PK())
					 .AddIndex("total_queries_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total_avg_lock_time_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("last_sec_qps", "-", "int64", IndexOpts().Dense())
					 .AddIndex("last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("last_sec_avg_lock_time_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("latency_stddev", "-", "double", IndexOpts().Dense()));

	AddNamespace(NamespaceDef(kNamespacesNamespace, StorageOpts()).AddIndex("name", "hash", "string", IndexOpts().PK()));

	AddNamespace(NamespaceDef(kPerfStatsNamespace, StorageOpts()).AddIndex("name", "hash", "string", IndexOpts().PK()));

	AddNamespace(NamespaceDef(kMemStatsNamespace, StorageOpts())
					 .AddIndex("name", "hash", "string", IndexOpts().PK())
					 .AddIndex("items_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("data_size", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total.data_size", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total.indexes_size", "-", "int64", IndexOpts().Dense())
					 .AddIndex("total.cache_size", "-", "int64", IndexOpts().Dense()));
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
        "type":"namespaces",
        "namespaces":[
            {
				"namespace":"*",
                "log_level":"none",
				"lazyload":false,
				"unload_idle_threshold":0,
				"join_cache_mode":"off"
			}
    	]
	})json",
	R"json({
        "type":"replication",
        "replication":{
			"role":"none",
			"master_dsn":"cproto://127.0.0.1:6534/db",
			"cluster_id":2,
			"force_sync_on_logic_error": false,
			"force_sync_on_wrong_data_hash": false,
			"namespaces":[]
		}
    })json"};

Error ReindexerImpl::InitSystemNamespaces() {
	createSystemNamespaces();

	QueryResults results;
	auto err = Select(Query(kConfigNamespace), results);
	if (!err.ok()) return err;

	if (results.Count() == 0) {
		// Set default config
		for (auto conf : defDBConfig) {
			Item item = NewItem(kConfigNamespace);
			if (!item.Status().ok()) return item.Status();
			err = item.FromJSON(conf);
			if (!err.ok()) return err;
			err = Insert(kConfigNamespace, item);
			if (!err.ok()) return err;
		}
	} else {
		// Load config from namespace #config
		QueryResults results;
		auto err = Select(Query(kConfigNamespace), results);
		if (!err.ok()) return err;
		for (auto it : results) {
			auto item = it.GetItem();
			try {
				updateConfigProvider(item);
			} catch (const Error& err) {
				return err;
			}
		}
	}

	tryLoadReplicatorConfFromFile();
	return errOK;
}

void ReindexerImpl::tryLoadReplicatorConfFromFile() {
	std::string yamlReplConf;
	int res = fs::ReadFile(fs::JoinPath(storagePath_, kReplicationConfFilename), yamlReplConf);
	ReplicationConfigData replConf;
	if (res > 0) {
		Error err = replConf.FromYML(yamlReplConf);
		if (!err.ok()) {
			logPrintf(LogError, "Error parsing replication config YML: %s", err.what());
		} else {
			WrSerializer ser;
			JsonBuilder jb(ser);
			jb.Put("type", "replication");
			auto replNode = jb.Object("replication");
			replConf.GetJSON(replNode);
			replNode.End();
			jb.End();

			Item item = NewItem(kConfigNamespace);
			if (item.Status().ok()) err = item.FromJSON(ser.Slice());
			if (err.ok()) err = Upsert(kConfigNamespace, item);
		}
	}
}

Error ReindexerImpl::updateDbFromConfig(string_view configNsName, Item& configItem) {
	if (configNsName == kConfigNamespace) {
		try {
			updateConfigProvider(configItem);
			bool needStart = replicator_->Configure(configProvider_.GetReplicationConfig());
			for (auto& ns : getNamespaces()) {
				ns->onConfigUpdated(configProvider_);
			}
			if (needStart) return replicator_->Start();
		} catch (const Error& err) {
			return err;
		}
	}
	return errOK;
}

void ReindexerImpl::updateConfigProvider(Item& configItem) {
	JsonAllocator jalloc;
	JsonValue jvalue;
	char* endp;

	string_view json = configItem.GetJSON();
	int status = jsonParse(const_cast<char*>(json.data()), &endp, &jvalue, jalloc);
	if (status != JSON_OK) {
		throw Error(errParseJson, "Malformed JSON with config");
	}

	Error err = configProvider_.FromJSON(jvalue);
	if (!err.ok()) throw err;
}

void ReindexerImpl::syncSystemNamespaces(string_view name) {
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

	ProfilingConfigData profilingCfg = configProvider_.GetProfilingConfig();

	if (profilingCfg.perfStats && (name.empty() || name == kPerfStatsNamespace)) {
		forEachNS(getNamespace(kPerfStatsNamespace), [&](Namespace::Ptr ns) { ns->GetPerfStat().GetJSON(ser); });
	}

	if (profilingCfg.memStats && (name.empty() || name == kMemStatsNamespace)) {
		forEachNS(getNamespace(kMemStatsNamespace), [&](Namespace::Ptr ns) { ns->GetMemStat().GetJSON(ser); });
	}

	if (name.empty() || name == kNamespacesNamespace) {
		forEachNS(getNamespace(kNamespacesNamespace), [&](Namespace::Ptr ns) { ns->GetDefinition().GetJSON(ser, kIndexJSONWithDescribe); });
	}

	if (profilingCfg.queriesPerfStats && (name.empty() || name == kQueriesPerfStatsNamespace)) {
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

void ReindexerImpl::onProfiligConfigLoad() {
	QueryResults qr1, qr2, qr3;
	Delete(Query(kMemStatsNamespace), qr2);
	Delete(Query(kQueriesPerfStatsNamespace), qr3);
	Delete(Query(kPerfStatsNamespace), qr1);
}

Error ReindexerImpl::SubscribeUpdates(IUpdatesObserver* observer, bool subscribe) {
	if (subscribe) {
		return observers_.Add(observer);
	} else {
		return observers_.Delete(observer);
	}
}

Error ReindexerImpl::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string>& suggestions) {
	Query query;
	suggestions = query.GetSuggestions(sqlQuery, pos, namespaces_);
	return errOK;
}

}  // namespace reindexer
