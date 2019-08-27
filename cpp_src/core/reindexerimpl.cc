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
#include "estl/contexted_locks.h"
#include "replicator/replicator.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"

using std::lock_guard;
using std::string;
using std::vector;
using namespace std::placeholders;

const char* const kPerfStatsNamespace = "#perfstats";
const char* const kQueriesPerfStatsNamespace = "#queriesperfstats";
const char* const kMemStatsNamespace = "#memstats";
const char* const kNamespacesNamespace = "#namespaces";
const char* const kConfigNamespace = "#config";
const char* const kActivityStatsNamespace = "#activitystats";
const char* const kStoragePlaceholderFilename = ".reindexer.storage";
const char* const kReplicationConfFilename = "replication.conf";

namespace reindexer {

ReindexerImpl::ReindexerImpl() : replicator_(new Replicator(this)), hasReplConfigLoadError_(false), storageType_(StorageType::LevelDB) {
	stopBackgroundThread_ = false;
	configProvider_.setHandler(ProfilingConf, std::bind(&ReindexerImpl::onProfiligConfigLoad, this));
	backgroundThread_ = std::thread([this]() { this->backgroundRoutine(); });
}

ReindexerImpl::~ReindexerImpl() {
	stopBackgroundThread_ = true;
	backgroundThread_.join();
	replicator_->Stop();
}

Error ReindexerImpl::EnableStorage(const string& storagePath, bool skipPlaceholderCheck, const InternalRdxContext& ctx) {
	if (!storagePath_.empty()) {
		return Error(errParams, "Storage already enabled");
	}

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
		std::string content;
		int res = fs::ReadFile(fs::JoinPath(storagePath, kStoragePlaceholderFilename), content);
		if (res > 0) {
			auto currentStorageType = StorageType::LevelDB;
			try {
				currentStorageType = reindexer::datastorage::StorageTypeFromString(content);
			} catch (const Error&) {
				return Error(errParams,
							 "Cowadly refusing to use directory '%s' - it's not empty and contains reindexer placeholder with unexpected "
							 "content: \"%s\"",
							 storagePath, content);
			}
			if (storageType_ != currentStorageType) {
				logPrintf(LogWarning, "Placeholder content doesn't correspond to chosen storage type. Forcing \"%s\"", content);
				storageType_ = currentStorageType;
			}
		} else {
			return Error(errParams, "Cowadly refusing to use directory '%s' - it's not empty and doesn't contain reindexer placeholder",
						 storagePath);
		}
	} else {
		FILE* f = fopen(fs::JoinPath(storagePath, kStoragePlaceholderFilename).c_str(), "w");
		if (f) {
			auto storageName = datastorage::StorageTypeToString(storageType_);
			int res = fwrite(storageName.c_str(), storageName.size(), 1, f);
			if (res != 1) {
				return Error(errParams, "Can't create placeholder in directory '%s' for reindexer storage - reason %s", storagePath,
							 strerror(errno));
			}
			fclose(f);
		} else {
			return Error(errParams, "Can't create placeholder in directory '%s' for reindexer storage - reason %s", storagePath,
						 strerror(errno));
		}
	}

	storagePath_ = storagePath;
	Error res = errOK;
	if (isHaveConfig) {
		res = OpenNamespace(kConfigNamespace, StorageOpts().Enabled().CreateIfMissing(), ctx);
	}
	replConfigFileChecker_.SetFilepath(fs::JoinPath(storagePath_, kReplicationConfFilename));

	return res;
}

Error ReindexerImpl::Connect(const string& dsn, ConnectOpts opts) {
	string path = dsn;
	if (dsn.compare(0, 10, "builtin://") == 0) {
		path = dsn.substr(10);
	}

	vector<reindexer::fs::DirEntry> foundNs;

	switch (opts.StorageType()) {
		case kStorageTypeOptLevelDB:
			storageType_ = StorageType::LevelDB;
			break;
		case kStorageTypeOptRocksDB:
			storageType_ = StorageType::RocksDB;
			break;
	}

	bool enableStorage = (path.length() > 0 && path != "/");
	if (enableStorage) {
		auto err = EnableStorage(path);
		if (!err.ok()) return err;
		if (fs::ReadDir(path, foundNs) < 0) {
			return Error(errParams, "Can't read database dir %s", path);
		}
	}

	InitSystemNamespaces();

	if (enableStorage && opts.IsOpenNamespaces()) {
		int maxLoadWorkers = std::min(int(std::thread::hardware_concurrency()), 8);
		std::unique_ptr<std::thread[]> thrs(new std::thread[maxLoadWorkers]);
		std::atomic_flag hasNsErrors{false};
		for (int i = 0; i < maxLoadWorkers; i++) {
			thrs[i] = std::thread(
				[&](int i) {
					for (int j = i; j < int(foundNs.size()); j += maxLoadWorkers) {
						auto& de = foundNs[j];
						if (de.isDir && validateObjectName(de.name)) {
							auto status = OpenNamespace(de.name, StorageOpts().Enabled());
							if (!status.ok()) {
								logPrintf(LogError, "Failed to open namespace '%s' - %s", de.name, status.what());
								hasNsErrors.test_and_set(std::memory_order_relaxed);
							}
						}
					}
				},
				i);
		}
		for (int i = 0; i < maxLoadWorkers; i++) thrs[i].join();

		if (!opts.IsAllowNamespaceErrors() && hasNsErrors.test_and_set(std::memory_order_relaxed)) {
			return Error(errNotValid, "Namespaces load error");
		}
	}

	bool needStart = replicator_->Configure(configProvider_.GetReplicationConfig());
	Error err = needStart ? replicator_->Start() : errOK;
	if (!err.ok()) {
		return err;
	}
	return replConfigFileChecker_.Enable();
}

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx) {
	shared_ptr<Namespace> ns;
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE NAMESPACE " << nsDef.name).Slice() : ""_sv, activities_);
		{
			ULock lock(mtx_, rdxCtx);
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
			ns->EnableStorage(storagePath_, nsDef.storage, storageType_, rdxCtx);
		}
		ns->onConfigUpdated(configProvider_, rdxCtx);
		if (readyToLoadStorage) {
			if (!ns->getStorageOpts(rdxCtx).IsLazyLoad()) ns->LoadFromStorage(rdxCtx);
		}
		{
			ULock lock(mtx_, rdxCtx);
			NamespaceCloner::Ptr wr = std::make_shared<NamespaceCloner>(ns);
			namespaces_.insert({nsDef.name, wr});
		}
		observers_.OnWALUpdate(0, nsDef.name, WALRecord(WalNamespaceAdd));
		for (auto& indexDef : nsDef.indexes) ns->AddIndex(indexDef, rdxCtx);

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::OpenNamespace(string_view name, const StorageOpts& storageOpts, const InternalRdxContext& ctx) {
	shared_ptr<Namespace> ns;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "OPEN NAMESPACE " << name).Slice() : ""_sv, activities_);
		{
			SLock lock(mtx_, &rdxCtx);
			auto nsIt = namespaces_.find(name);
			if (nsIt != namespaces_.end() && nsIt->second) {
				nsIt->second->GetOriginNs()->SetStorageOpts(storageOpts, rdxCtx);
				return 0;
			}
		}
		if (!validateObjectName(name)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-, are allowed");
		}
		string nameStr(name);
		ns = std::make_shared<Namespace>(nameStr, observers_);
		if (storageOpts.IsEnabled() && !storagePath_.empty()) {
			ns->EnableStorage(storagePath_, storageOpts, storageType_, rdxCtx);
			ns->onConfigUpdated(configProvider_, rdxCtx);
			if (!ns->getStorageOpts(rdxCtx).IsLazyLoad()) ns->LoadFromStorage(rdxCtx);
		}
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			NamespaceCloner::Ptr nmWrapper = std::make_shared<NamespaceCloner>(ns);
			namespaces_.insert({nameStr, nmWrapper});
		}
		observers_.OnWALUpdate(0, name, WALRecord(WalNamespaceAdd));
	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::DropNamespace(string_view nsName, const InternalRdxContext& ctx) {
	WrSerializer ser;
	return closeNamespace(nsName,
						  ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "DROP NAMESPACE " << nsName).Slice() : ""_sv, activities_),
						  true, false);
}
Error ReindexerImpl::CloseNamespace(string_view nsName, const InternalRdxContext& ctx) {
	WrSerializer ser;
	return closeNamespace(
		nsName, ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CLOSE NAMESPACE " << nsName).Slice() : ""_sv, activities_), false,
		false);
}

Error ReindexerImpl::closeNamespace(string_view nsName, const RdxContext& ctx, bool dropStorage, bool enableDropSlave) {
	NamespaceCloner::Ptr nsw;
	try {
		ULock lock(mtx_, ctx);
		auto nsIt = namespaces_.find(nsName);

		if (nsIt == namespaces_.end()) {
			return Error(errNotFound, "Namespace '%s' does not exist", nsName);
		}
		// Temporary save namespace. This will call destructor without lock
		nsw = nsIt->second;
		auto ns = ClonableNamespace(1, nsw);
		if (ns->GetReplState(ctx).slaveMode && !enableDropSlave) {
			return Error(errLogic, "Can't modify slave ns '%s'", nsName);
		}

		namespaces_.erase(nsIt);
		if (dropStorage) {
			ns->DeleteStorage(ctx);
		} else {
			ns->CloseStorage(ctx);
		}
		if (dropStorage) observers_.OnWALUpdate(0, nsName, WALRecord(WalNamespaceDrop));

	} catch (const Error& err) {
		nsw.reset();
		return err;
	}
	// Here will called destructor
	nsw.reset();
	return errOK;
}

Error ReindexerImpl::TruncateNamespace(string_view nsName, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "TRUNCATE " << nsName).Slice() : ""_sv, activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->Truncate(rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

Error ReindexerImpl::Insert(string_view nsName, Item& item, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "INSERT INTO " << nsName).Slice() : ""_sv, activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->Insert(item, rdxCtx);
		updateToSystemNamespace(nsName, item, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

static WrSerializer& printPkFields(const Item& item, WrSerializer& ser) {
	const FieldsSet fields = item.PkFields();
	for (auto it = fields.begin(); it != fields.end(); ++it) {
		if (it != fields.begin()) ser << " AND ";
		const Item::FieldRef f = item[*it];
		ser << f.Name() << " = ";
		Variant(f).Dump(ser);
	}
	return ser;
}

Error ReindexerImpl::Update(string_view nsName, Item& item, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "UPDATE " << nsName << " WHERE ", printPkFields(item, ser)).Slice() : ""_sv, activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);

		ns->Update(item, rdxCtx);
		updateToSystemNamespace(nsName, item, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

Error ReindexerImpl::Update(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? q.GetSQL(ser).Slice() : ""_sv, activities_, result);
		auto ns = getClonableNamespace(q._namespace, rdxCtx);
		ensureDataLoaded(ns, rdxCtx);
		ns->Update(q, result, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Upsert(string_view nsName, Item& item, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "UPSERT INTO " << nsName << " WHERE ", printPkFields(item, ser)).Slice() : ""_sv,
			activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->Upsert(item, rdxCtx);
		updateToSystemNamespace(nsName, item, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

Item ReindexerImpl::NewItem(string_view nsName, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE ITEM FOR " << nsName).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		auto item = ns->NewItem(rdxCtx);
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

Error ReindexerImpl::CommitTransaction(Transaction& tr, const InternalRdxContext& ctx) {
	Error err = errOK;
	auto trAccessor = static_cast<TransactionAccessor*>(&tr);

#if ATOMIC_NS_CLONE
	ClonableNamespace ns;
#else
	Namespace::Ptr ns;
#endif
	try {
		const RdxContext rdxCtx = ctx.CreateRdxContext("COMMIT TRANSACTION", activities_);
		ns = getClonableNamespace(trAccessor->GetName(), rdxCtx, trAccessor->impl_->steps_.size());
		ns->StartTransaction(rdxCtx);
		RdxActivityContext* const actCtx = rdxCtx.Activity();
		for (auto& step : tr.impl_->steps_) {
			ns->ApplyTransactionStep(step, actCtx);
			updateToSystemNamespace(trAccessor->GetName(), step.item_, rdxCtx);
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

Error ReindexerImpl::GetMeta(string_view nsName, const string& key, string& data, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "SELECT META FROM " << nsName << " WHERE KEY = '" << key << '\'').Slice() : ""_sv,
			activities_);
		data = getNamespace(nsName, rdxCtx)->GetMeta(key, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::PutMeta(string_view nsName, const string& key, string_view data, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "UPDATE " << nsName << " SET META = '" << data << "' WHERE KEY = '" << key << '\'').Slice()
									: ""_sv,
			activities_);
		getClonableNamespace(nsName, rdxCtx)->PutMeta(key, data, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::EnumMeta(string_view nsName, vector<string>& keys, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "SELECT META FROM " << nsName).Slice() : ""_sv, activities_);
		keys = getNamespace(nsName, rdxCtx)->EnumMeta(rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Delete(string_view nsName, Item& item, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "DELETE FROM " << nsName << " WHERE ", printPkFields(item, ser)).Slice() : ""_sv,
			activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->Delete(item, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}
Error ReindexerImpl::Delete(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? q.GetSQL(ser).Slice() : "", activities_, result);
		auto ns = getClonableNamespace(q._namespace, rdxCtx);
		ensureDataLoaded(ns, rdxCtx);
		ns->Delete(q, result, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Select(string_view query, QueryResults& result, const InternalRdxContext& ctx) {
	Error err = errOK;
	try {
		Query q;
		q.FromSQL(query);
		switch (q.type_) {
			case QuerySelect:
				err = Select(q, result, ctx);
				break;
			case QueryDelete:
				err = Delete(q, result, ctx);
				break;
			case QueryUpdate:
				err = Update(q, result, ctx);
				break;
			case QueryTruncate:
				err = TruncateNamespace(q._namespace, ctx);
				break;
			default:
				throw Error(errParams, "Error unsupported query type %d", q.type_);
		}
	} catch (const Error& e) {
		err = e;
	}

	if (ctx.Compl()) ctx.Compl()(err);
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

Error ReindexerImpl::Select(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? q.GetSQL(ser).Slice() : "", activities_, result);
		NsLocker<const RdxContext> locks(rdxCtx);

		Namespace::Ptr mainNs;

		mainNs = getNamespace(q._namespace, rdxCtx);

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

		if (q._namespace.size() && q._namespace[0] == '#') syncSystemNamespaces(q._namespace, rdxCtx);
		// Lookup and lock namespaces_
		ensureDataLoaded(mainNs, rdxCtx);
		mainNs->updateSelectTime();
		locks.Add(mainNs);
		q.WalkNested(false, true, [this, &locks, &rdxCtx](const Query q) {
			auto ns = getNamespace(q._namespace, rdxCtx);
			ensureDataLoaded(ns, rdxCtx);
			ns->updateSelectTime();
			locks.Add(ns);
		});

		locks.Lock();

		calc.LockHit();
		statCalculator.LockHit();
		SelectFunctionsHolder func;
		if (!q.joinQueries_.empty()) {
			result.joined_.resize(1 + q.mergeQueries_.size());
		}

		doSelect(q, result, locks, func, rdxCtx);
		func.Process(result);
	} catch (const Error& err) {
		if (ctx.Compl()) ctx.Compl()(err);
		return err;
	}
	if (ctx.Compl()) ctx.Compl()(errOK);
	return errOK;
}

template <typename T>
JoinedSelectors ReindexerImpl::prepareJoinedSelectors(const Query& q, QueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func,
													  const RdxContext& rdxCtx) {
	JoinedSelectors joinedSelectors;
	if (q.joinQueries_.empty()) return joinedSelectors;
	auto ns = locks.Get(q._namespace);

	// For each joined queries
	int joinedSelectorsCount = q.joinQueries_.size();
	for (auto& jq : q.joinQueries_) {
		// Get common results from joined namespaces_
		auto jns = locks.Get(jq._namespace);

		Query jjq(jq);

		JoinPreResult::Ptr preResult = std::make_shared<JoinPreResult>();
		size_t joinedFieldIdx = joinedSelectors.size();

		JoinCacheRes joinRes;
		joinRes.key.SetData(jq);
		jns->GetFromJoinCache(joinRes);
		if (!jjq.entries.Empty() && !joinRes.haveData) {
			QueryResults jr;
			jjq.Limit(UINT_MAX);
			SelectCtx ctx(jjq);
			ctx.preResult = preResult;
			ctx.preResult->mode = JoinPreResult::ModeBuild;
			ctx.functions = &func;
			jns->Select(jr, ctx, rdxCtx);
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

		jItemQ.entries.Reserve(jq.joinEntries_.size());

		// Construct join conditions
		for (auto& je : jq.joinEntries_) {
			int joinIdx = IndexValueType::NotSet;
			if (!jns->getIndexByName(je.joinIndex_, joinIdx)) {
				joinIdx = IndexValueType::SetByJsonPath;
			}
			QueryEntry qe(je.condition_, je.joinIndex_, joinIdx);
			if (!ns->getIndexByName(je.index_, const_cast<QueryJoinEntry&>(je).idxNo)) {
				const_cast<QueryJoinEntry&>(je).idxNo = IndexValueType::SetByJsonPath;
			}
			jItemQ.entries.Append(je.op_, std::move(qe));
		}

		auto joinedSelector = [&result, &jq, jns, preResult, joinedFieldIdx, &func, ns, joinedSelectorsCount, &rdxCtx](
								  JoinedSelector* js, IdType id, int nsId, ConstPayload payload, bool match) {
			QueryResults joinItemR;

			// Put values to join conditions
			size_t index = 0;
			for (auto& je : jq.joinEntries_) {
				bool nonIndexedField = (je.idxNo == IndexValueType::SetByJsonPath);
				bool isIndexSparse = !nonIndexedField && ns->indexes_[je.idxNo]->Opts().IsSparse();
				if (nonIndexedField || isIndexSparse) {
					VariantArray& values = js->query.entries[index].values;
					KeyValueType type = values.empty() ? KeyValueUndefined : values[0].Type();
					payload.GetByJsonPath(je.index_, ns->tagsMatcher_, values, type);
				} else {
					payload.Get(je.idxNo, js->query.entries[index].values);
				}
				++index;
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
				jns->FillResult(joinItemR, joinResLong.it.val.ids_);
			} else {
				SelectCtx ctx(js->query);
				ctx.preResult = preResult;
				ctx.matchedAtLeastOnce = false;
				ctx.reqMatchedOnceFlag = true;
				ctx.skipIndexesLookup = true;
				ctx.functions = &func;
				jns->Select(joinItemR, ctx, rdxCtx);

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
				if (nsId >= static_cast<int>(result.joined_.size())) {
					result.joined_.resize(nsId + 1);
				}
				joins::NamespaceResults& nsJoinRes = result.joined_[nsId];
				nsJoinRes.SetJoinedSelectorsCount(joinedSelectorsCount);
				nsJoinRes.Insert(id, joinedFieldIdx, std::move(joinItemR));
			}
			return matchedAtLeastOnce;
		};
		joinedSelectors.push_back({jq.joinType, jq.count == 0, joinedSelector, 0, 0, jns->name_, std::move(joinRes), jItemQ});
		ThrowOnCancel(rdxCtx);
	}
	return joinedSelectors;
}

template JoinedSelectors ReindexerImpl::prepareJoinedSelectors(const Query&, QueryResults&, NsLocker<RdxContext>&, SelectFunctionsHolder&,
															   const RdxContext&);

template <typename T>
void ReindexerImpl::doSelect(const Query& q, QueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func, const RdxContext& ctx) {
	auto ns = locks.Get(q._namespace);
	if (!ns) {
		throw Error(errParams, "Namespace '%s' is not exists", q._namespace);
	}
	SelectCtx selCtx(q);
	selCtx.contextCollectingMode = true;
	int jqCount = q.joinQueries_.size();

	for (auto& mq : q.mergeQueries_) {
		jqCount += mq.joinQueries_.size();
	}

	{
		JoinedSelectors joinedSelectors = prepareJoinedSelectors(q, result, locks, func, ctx);
		selCtx.functions = &func;
		selCtx.joinedSelectors = joinedSelectors.size() ? &joinedSelectors : nullptr;
		selCtx.nsid = 0;
		selCtx.isForceAll = !q.mergeQueries_.empty() || !q.forcedSortOrder.empty();
		ns->Select(result, selCtx, ctx);
	}

	if (!q.mergeQueries_.empty()) {
		uint8_t counter = 0;

		for (auto& mq : q.mergeQueries_) {
			auto mns = locks.Get(mq._namespace);
			SelectCtx mctx(mq);
			mctx.nsid = ++counter;
			mctx.isForceAll = true;
			mctx.functions = &func;
			mctx.contextCollectingMode = true;
			JoinedSelectors joinedSelectors = prepareJoinedSelectors(mq, result, locks, func, ctx);
			mctx.joinedSelectors = joinedSelectors.size() ? &joinedSelectors : nullptr;

			mns->Select(result, mctx, ctx);
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
	// Adding context to QueryResults
	if (!q.joinQueries_.empty() || !q.mergeQueries_.empty()) {
		q.WalkNested(false, false, [&locks, &result, &ctx](const Query& nestedQuery) {
			Query q = Query(nestedQuery._namespace, 0, 0);
			SelectCtx jctx(q);
			jctx.contextCollectingMode = true;
			Namespace::Ptr ns = locks.Get(nestedQuery._namespace);
			ns->Select(result, jctx, ctx);
		});
	}
	result.lockResults();
}

template void ReindexerImpl::doSelect(const Query&, QueryResults&, NsLocker<RdxContext>&, SelectFunctionsHolder&, const RdxContext&);

Error ReindexerImpl::Commit(string_view /*_namespace*/) {
	try {
		// getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Namespace::Ptr ReindexerImpl::getNamespace(string_view nsName, const RdxContext& ctx) {
	SLock lock(mtx_, &ctx);
	auto nsIt = namespaces_.find(nsName);

	if (nsIt == namespaces_.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", nsName);
	}

	assert(nsIt->second);
	return nsIt->second->GetOriginNs();
}

#if ATOMIC_NS_CLONE
ClonableNamespace ReindexerImpl::getClonableNamespace(string_view nsName, const RdxContext& ctx, size_t actionsSize) {
	SLock lock(mtx_, ctx);
	auto nsIt = namespaces_.find(nsName);

	if (nsIt == namespaces_.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", nsName);
	}

	assert(nsIt->second);

	ClonableNamespace some(actionsSize, nsIt->second);
	return some;
}
#endif

Error ReindexerImpl::AddIndex(string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "CREATE INDEX " << indexDef.name_ << " ON " << nsName).Slice() : ""_sv, activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->AddIndex(indexDef, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::UpdateIndex(string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "UPDATE INDEX " << indexDef.name_ << " ON " << nsName).Slice() : ""_sv, activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->UpdateIndex(indexDef, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::DropIndex(string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "DROP INDEX " << indexDef.name_ << " ON " << nsName).Slice() : ""_sv, activities_);
		auto ns = getClonableNamespace(nsName, rdxCtx);
		ns->DropIndex(indexDef, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}
void ReindexerImpl::ensureDataLoaded(Namespace::Ptr& ns, const RdxContext& ctx) {
	SStorageLock readlock(storageMtx_, &ctx);
	if (ns->needToLoadData(ctx)) {
		readlock.unlock();
		UStorageLock writelock(storageMtx_, ctx);
		if (ns->needToLoadData(ctx)) ns->LoadFromStorage(ctx);
	}
}

#if ATOMIC_NS_CLONE
void ReindexerImpl::ensureDataLoaded(ClonableNamespace& ns, const RdxContext& ctx) {
	SStorageLock readlock(storageMtx_, &ctx);
	if (ns->needToLoadData()) {
		readlock.unlock();
		UStorageLock writelock(storageMtx_, ctx);
		if (ns->needToLoadData()) ns->LoadFromStorage(ctx);
	}
}
#endif

std::vector<Namespace::Ptr> ReindexerImpl::getNamespaces(const RdxContext& ctx) {
	SLock lock(mtx_, &ctx);
	std::vector<Namespace::Ptr> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.push_back(ns.second->GetOriginNs());
	}
	return ret;
}

std::vector<string> ReindexerImpl::getNamespacesNames(const RdxContext& ctx) {
	SLock lock(mtx_, &ctx);
	std::vector<string> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) ret.push_back(ns.first);
	return ret;
}

Error ReindexerImpl::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll, const InternalRdxContext& ctx) {
	try {
		const auto rdxCtx = ctx.CreateRdxContext("SELECT NAMESPACES", activities_);
		auto nsarray = getNamespaces(rdxCtx);
		for (auto& ns : nsarray) {
			defs.push_back(ns->GetDefinition(rdxCtx));
		}

		if (bEnumAll && !storagePath_.empty()) {
			vector<fs::DirEntry> dirs;
			if (fs::ReadDir(storagePath_, dirs) != 0) return Error(errLogic, "Could not read database dir");

			for (auto& d : dirs) {
				if (d.isDir && d.name != "." && d.name != "..") {
					{
						SLock lock(mtx_, &rdxCtx);
						if (namespaces_.find(d.name) != namespaces_.end()) continue;
					}
					unique_ptr<Namespace> tmpNs(new Namespace(d.name, observers_));
					try {
						tmpNs->EnableStorage(storagePath_, StorageOpts(), storageType_, rdxCtx);
						defs.push_back(tmpNs->GetDefinition(rdxCtx));
					} catch (reindexer::Error) {
					}
				}
			}
		}
	} catch (reindexer::Error err) {
		return err.code();
	}
	return errOK;
}

void ReindexerImpl::backgroundRoutine() {
	static const RdxContext dummyCtx;
	auto nsFlush = [&]() {
		auto nsarray = getNamespacesNames(dummyCtx);
		for (auto name : nsarray) {
			try {
				auto ns = getClonableNamespace(name, dummyCtx);
				ns->tryToReload(dummyCtx);
				ns->BackgroundRoutine(nullptr);
			} catch (Error err) {
				logPrintf(LogWarning, "flusherThread() failed: %s", err.what());
			} catch (...) {
				logPrintf(LogWarning, "flusherThread() failed with ns: %s", name);
			}
		}
		std::string yamlReplConf;
		bool replConfigWasModified = replConfigFileChecker_.ReadIfFileWasModified(yamlReplConf);
		if (replConfigWasModified) {
			hasReplConfigLoadError_ = !(tryLoadReplicatorConfFromYAML(yamlReplConf).ok());
		} else if (hasReplConfigLoadError_) {
			// Retry to read error config once
			// This logic adds delay between write and read, which allows writer to finish all his writes
			hasReplConfigLoadError_ = false;
			tryLoadReplicatorConfFromFile();
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

	AddNamespace(NamespaceDef(kActivityStatsNamespace, StorageOpts())
					 .AddIndex("query_id", "hash", "int", IndexOpts().PK())
					 .AddIndex("client", "-", "string", IndexOpts().Dense())
					 .AddIndex("query", "-", "string", IndexOpts().Dense())
					 .AddIndex("query_start", "-", "string", IndexOpts().Dense())
					 .AddIndex("blocked", "-", "bool", IndexOpts().Dense())
					 .AddIndex("description", "-", "string", IndexOpts().Sparse()));

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
				"join_cache_mode":"off",
				"start_copy_politics_count":10000,
				"merge_limit_count":20000
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
    })json",
	R"json({
        "type":"action",
        "action":{
			"command":""
		}
    })json"};

Error ReindexerImpl::InitSystemNamespaces() {
	createSystemNamespaces();

	QueryResults results;
	auto err = Select(Query(kConfigNamespace), results);
	if (!err.ok()) return err;

	bool hasReplicatorConfig = false;
	if (results.Count() == 0) {
		// Set default config
		for (const auto& conf : defDBConfig) {
			if (!hasReplicatorConfig) {
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(string_view(conf));
				if (configJson["type"].As<std::string>() == "replication") {
					hasReplicatorConfig = true;
					if (tryLoadReplicatorConfFromFile().ok()) {
						continue;
					}
				}
			}

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
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(item.GetJSON());
				updateConfigProvider(configJson);
			} catch (const Error& err) {
				return err;
			}
		}
	}

	if (!hasReplicatorConfig) {
		tryLoadReplicatorConfFromFile();
	}

	return errOK;
}

Error ReindexerImpl::tryLoadReplicatorConfFromFile() {
	std::string yamlReplConf;
	int res = fs::ReadFile(fs::JoinPath(storagePath_, kReplicationConfFilename), yamlReplConf);
	if (res > 0) {
		return tryLoadReplicatorConfFromYAML(yamlReplConf);
	}
	return Error(errNotFound);
}

Error ReindexerImpl::tryLoadReplicatorConfFromYAML(const std::string& yamlReplConf) {
	if (yamlReplConf.empty()) {
		return Error(errNotFound);
	}

	ReplicationConfigData replConf;
	Error err = replConf.FromYML(yamlReplConf);
	if (!err.ok()) {
		logPrintf(LogError, "Error parsing replication config YML: %s", err.what());
		return Error(errParams, "Error parsing replication config YML: %s", err.what());
	} else {
		WrSerializer ser;
		JsonBuilder jb(ser);
		jb.Put("type", "replication");
		auto replNode = jb.Object("replication");
		replConf.GetJSON(replNode);
		replNode.End();
		jb.End();

		Item item = NewItem(kConfigNamespace);
		if (!item.Status().ok()) {
			return item.Status();
		}
		err = item.FromJSON(ser.Slice());
		if (!err.ok()) {
			return err;
		}
		return Upsert(kConfigNamespace, item);
	}
}

void ReindexerImpl::updateToSystemNamespace(string_view nsName, Item& item, const RdxContext& ctx) {
	if (item.GetID() != -1 && nsName == kConfigNamespace) {
		gason::JsonParser parser;
		gason::JsonNode configJson = parser.Parse(item.GetJSON());

		updateConfigProvider(configJson);

		bool needStartReplicator = false;
		if (!configJson["replication"].empty()) {
			updateReplicationConfFile();
			needStartReplicator = replicator_->Configure(configProvider_.GetReplicationConfig());
		}
		for (auto& ns : getNamespaces(ctx)) {
			ns->onConfigUpdated(configProvider_, ctx);
		}
		auto& actionNode = configJson["action"];
		if (!actionNode.empty()) {
			string_view command = actionNode["command"].As<string_view>();
			if (command == "restart_replication"_sv) {
				replicator_->Stop();
				needStartReplicator = true;
			}
		}
		if (needStartReplicator && !stopBackgroundThread_) {
			if (Error err = replicator_->Start()) throw err;
		}

	} else if (nsName == kQueriesPerfStatsNamespace) {
		queriesStatTracker_.Reset();
	} else if (nsName == kPerfStatsNamespace) {
		for (auto& ns : getNamespaces(ctx)) ns->ResetPerfStat(ctx);
	}
}

void ReindexerImpl::updateConfigProvider(const gason::JsonNode& config) {
	Error err;
	try {
		err = configProvider_.FromJSON(config);
	} catch (const gason::Exception& ex) {
		err = Error(errParseJson, "updateConfigProvider: %s", ex.what());
	}
	if (!err.ok()) throw err;
}

void ReindexerImpl::updateReplicationConfFile() {
	WrSerializer ser;
	auto oldReplConf = configProvider_.GetReplicationConfig();
	oldReplConf.GetYAML(ser);
	auto err = replConfigFileChecker_.RewriteFile(std::string(ser.Slice()), [&oldReplConf](const string& content) {
		ReplicationConfigData replConf;
		Error err = replConf.FromYML(content);
		if (err.ok()) {
			return replConf == oldReplConf;
		}
		return false;
	});
	if (!err.ok()) {
		throw err;
	}
}

void ReindexerImpl::syncSystemNamespaces(string_view name, const RdxContext& ctx) {
	auto nsarray = getNamespaces(ctx);
	WrSerializer ser;
	const auto activityCtx = ctx.OnlyActivity();

	auto forEachNS = [&](Namespace::Ptr sysNs, bool withSystem, std::function<void(Namespace::Ptr ns)> filler) {
		std::vector<Item> items;
		items.reserve(nsarray.size());
		for (auto& ns : nsarray) {
			if (ns->isSystem() && !withSystem) continue;
			ser.Reset();
			filler(ns);
			items.push_back(sysNs->NewItem(ctx));
			auto err = items.back().FromJSON(ser.Slice());
			if (!err.ok()) throw err;
		}
		const Namespace::WLock lock(sysNs->mtx_, &ctx);
		sysNs->Truncate(ctx, -1, true);
		for (Item& i : items) {
			sysNs->Upsert(i, activityCtx, true, true);
		}
	};

	ProfilingConfigData profilingCfg = configProvider_.GetProfilingConfig();

	if (profilingCfg.perfStats && (name.empty() || name == kPerfStatsNamespace)) {
		forEachNS(getNamespace(kPerfStatsNamespace, ctx), false, [&](Namespace::Ptr ns) { ns->GetPerfStat(ctx).GetJSON(ser); });
	}

	if (profilingCfg.memStats && (name.empty() || name == kMemStatsNamespace)) {
		forEachNS(getNamespace(kMemStatsNamespace, ctx), false, [&](Namespace::Ptr ns) { ns->GetMemStat(ctx).GetJSON(ser); });
	}

	if (name.empty() || name == kNamespacesNamespace) {
		forEachNS(getNamespace(kNamespacesNamespace, ctx), true,
				  [&](Namespace::Ptr ns) { ns->GetDefinition(ctx).GetJSON(ser, kIndexJSONWithDescribe); });
	}

	if (profilingCfg.queriesPerfStats && (name.empty() || name == kQueriesPerfStatsNamespace)) {
		const auto data = queriesStatTracker_.Data();
		std::vector<Item> items;
		items.reserve(data.size());
		auto queriesperfstatsNs = getNamespace(kQueriesPerfStatsNamespace, ctx);
		for (const auto& stat : data) {
			ser.Reset();
			stat.GetJSON(ser);
			items.push_back(queriesperfstatsNs->NewItem(ctx));
			auto err = items.back().FromJSON(ser.Slice());
			if (!err.ok()) throw err;
		}
		const Namespace::WLock lock(queriesperfstatsNs->mtx_, &ctx);
		queriesperfstatsNs->Truncate(ctx, -1, true);
		for (Item& i : items) {
			queriesperfstatsNs->Upsert(i, activityCtx, true, true);
		}
	}

	if (name.empty() || name == kActivityStatsNamespace) {
		const auto data = activities_.List();
		std::vector<Item> items;
		items.reserve(data.size());
		auto activityNs = getNamespace(kActivityStatsNamespace, ctx);
		for (const auto& act : data) {
			ser.Reset();
			act.GetJSON(ser);
			items.push_back(activityNs->NewItem(ctx));
			auto err = items.back().FromJSON(ser.Slice());
			if (!err.ok()) throw err;
		}
		const Namespace::WLock lock(activityNs->mtx_, &ctx);
		activityNs->Truncate(ctx, -1, true);
		for (Item& i : items) {
			activityNs->Upsert(i, activityCtx, true, true);
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
	shared_lock<shared_timed_mutex> lock(mtx_);

	suggestions = query.GetSuggestions(sqlQuery, pos, namespaces_);
	return errOK;
}

}  // namespace reindexer
