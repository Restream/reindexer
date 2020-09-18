#include "core/reindexerimpl.h"
#include <stdio.h>
#include <chrono>
#include <thread>
#include "cjson/jsonbuilder.h"
#include "core/cjson/jsondecoder.h"
#include "core/iclientsstats.h"
#include "core/index/index.h"
#include "core/itemimpl.h"
#include "core/namespacedef.h"
#include "core/query/sql/sqlsuggester.h"
#include "core/selectfunc/selectfunc.h"
#include "estl/contexted_locks.h"
#include "queryresults/joinresults.h"
#include "replicator/replicator.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"

#include "debug/terminate_handler.h"

reindexer::SetTerminateHandler sth;

using std::lock_guard;
using std::string;
using std::vector;
using namespace std::placeholders;

namespace reindexer {

constexpr char kPerfStatsNamespace[] = "#perfstats";
constexpr char kQueriesPerfStatsNamespace[] = "#queriesperfstats";
constexpr char kMemStatsNamespace[] = "#memstats";
constexpr char kNamespacesNamespace[] = "#namespaces";
constexpr char kConfigNamespace[] = "#config";
constexpr char kActivityStatsNamespace[] = "#activitystats";
constexpr char kClientsStatsNamespace[] = "#clientsstats";
constexpr char kStoragePlaceholderFilename[] = ".reindexer.storage";
constexpr char kReplicationConfFilename[] = "replication.conf";

ReindexerImpl::ReindexerImpl(IClientsStats* clientsStats)
	: replicator_(new Replicator(this)),
	  hasReplConfigLoadError_(false),
	  storageType_(StorageType::LevelDB),
	  autorepairEnabled_(false),
	  connected_(false),
	  clientsStats_(clientsStats) {
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
			int errnoSv = errno;
			fclose(f);
			if (res != 1) {
				return Error(errParams, "Can't create placeholder in directory '%s' for reindexer storage - reason %s", storagePath,
							 strerror(errnoSv));
			}

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
	auto checkReplConf = [this](const ConnectOpts& opts) {
		if (opts.HasExpectedClusterID()) {
			auto replConfig = configProvider_.GetReplicationConfig();
			if (replConfig.role == ReplicationNone) {
				return Error(errReplParams, "Reindexer has replication state 'none' on this DSN.");
			}
			if (replConfig.clusterID != opts.ExpectedClusterID()) {
				return Error(errReplParams, "Expected master's clusted ID(%d) is not equal to actual clusted ID(%d)",
							 opts.ExpectedClusterID(), replConfig.clusterID);
			}
		}
		return Error();
	};
	if (connected_.load(std::memory_order_relaxed)) {
		return checkReplConf(opts);
	}
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

	autorepairEnabled_ = opts.IsAutorepair();

	bool enableStorage = (path.length() > 0 && path != "/");
	if (enableStorage) {
		auto err = EnableStorage(path);
		if (!err.ok()) return err;
		if (fs::ReadDir(path, foundNs) < 0) {
			return Error(errParams, "Can't read database dir %s", path);
		}
	}

	Error err = InitSystemNamespaces();
	if (!err.ok()) return err;

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
							if (status.ok()) {
								const RdxContext dummyCtx;
								if (getNamespace(de.name, dummyCtx)->IsTemporary(dummyCtx)) {
									logPrintf(LogWarning, "Droping tmp namespace '%s'", de.name);
									status = closeNamespace(de.name, dummyCtx, true, true);
								}
							}
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

	err = checkReplConf(opts);
	if (!err.ok()) return err;

	replicator_->Enable();
	bool needStart = replicator_->Configure(configProvider_.GetReplicationConfig());
	err = needStart ? replicator_->Start() : errOK;
	if (!err.ok()) {
		return err;
	}
	if (!storagePath_.empty()) {
		err = replConfigFileChecker_.Enable();
	}

	if (err.ok()) {
		connected_.store(true, std::memory_order_release);
	}
	return err;
}

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx) {
	Namespace::Ptr ns;
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE NAMESPACE " << nsDef.name).Slice() : ""_sv, activities_);
		{
			ULock lock(mtx_, &rdxCtx);
			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name);
			}
		}
		if (!validateObjectName(nsDef.name)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		bool readyToLoadStorage = (nsDef.storage.IsEnabled() && !storagePath_.empty());
		ns = std::make_shared<Namespace>(nsDef.name, observers_);
		if (nsDef.isTemporary) {
			ns->awaitMainNs(rdxCtx)->setTemporary();
		}
		if (readyToLoadStorage) {
			ns->EnableStorage(storagePath_, nsDef.storage, storageType_, rdxCtx);
		}
		ns->OnConfigUpdated(configProvider_, rdxCtx);
		if (readyToLoadStorage) {
			ns->LoadFromStorage(rdxCtx);
		}
		{
			ULock lock(mtx_, &rdxCtx);
			namespaces_.insert({nsDef.name, ns});
		}
		if (!nsDef.isTemporary) observers_.OnWALUpdate(LSNPair(), nsDef.name, WALRecord(WalNamespaceAdd));
		for (auto& indexDef : nsDef.indexes) ns->AddIndex(indexDef, rdxCtx);
		ns->SetSchema(nsDef.schemaJson, rdxCtx);
		if (nsDef.storage.IsSlaveMode()) ns->setSlaveMode(rdxCtx);

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::OpenNamespace(string_view name, const StorageOpts& storageOpts, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "OPEN NAMESPACE " << name).Slice() : ""_sv, activities_);
		{
			SLock lock(mtx_, &rdxCtx);
			auto nsIt = namespaces_.find(name);
			if (nsIt != namespaces_.end() && nsIt->second) {
				if (storageOpts.IsSlaveMode()) nsIt->second->setSlaveMode(rdxCtx);
				return 0;
			}
		}
		if (!validateObjectName(name)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		string nameStr(name);
		auto ns = std::make_shared<Namespace>(nameStr, observers_);
		if (storageOpts.IsSlaveMode()) ns->setSlaveMode(rdxCtx);
		if (storageOpts.IsEnabled() && !storagePath_.empty()) {
			auto opts = storageOpts;
			ns->EnableStorage(storagePath_, opts.Autorepair(autorepairEnabled_), storageType_, rdxCtx);
			ns->OnConfigUpdated(configProvider_, rdxCtx);
			ns->LoadFromStorage(rdxCtx);
		} else {
			ns->OnConfigUpdated(configProvider_, rdxCtx);
		}
		{
			lock_guard<shared_timed_mutex> lock(mtx_);
			namespaces_.insert({nameStr, ns});
		}
		observers_.OnWALUpdate(LSNPair(), name, WALRecord(WalNamespaceAdd));
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
	Namespace::Ptr ns;
	try {
		ULock lock(mtx_, &ctx);
		auto nsIt = namespaces_.find(nsName);

		if (nsIt == namespaces_.end()) {
			return Error(errNotFound, "Namespace '%s' does not exist", nsName);
		}
		// Temporary save namespace. This will call destructor without lock
		ns = nsIt->second;
		if (ns->GetReplState(ctx).slaveMode && !enableDropSlave) {
			return Error(errLogic, "Can't modify slave ns '%s'", nsName);
		}

		namespaces_.erase(nsIt);
		if (dropStorage) {
			ns->DeleteStorage(ctx);
		} else {
			ns->CloseStorage(ctx);
		}
		if (dropStorage) {
			if (!nsIt->second->GetDefinition(ctx).isTemporary) observers_.OnWALUpdate(LSNPair(), nsName, WALRecord(WalNamespaceDrop));
		}

	} catch (const Error& err) {
		ns.reset();
		return err;
	}
	// Here will called destructor
	ns.reset();
	return errOK;
}

Error ReindexerImpl::forceSyncDownstream(string_view nsName, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "FORCESYNCDOWNSTREAM " << nsName).Slice() : ""_sv, activities_);
		NamespaceDef nsDef = getNamespace(nsName, rdxCtx)->GetDefinition(rdxCtx);
		nsDef.GetJSON(ser);
		WALRecord wrec(WalForceSync, ser.Slice());
		observers_.OnWALUpdate(LSNPair(), nsName, wrec);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::TruncateNamespace(string_view nsName, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "TRUNCATE " << nsName).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		ns->Truncate(rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

Error ReindexerImpl::RenameNamespace(string_view srcNsName, const std::string& dstNsName, const InternalRdxContext& ctx) {
	Namespace::Ptr dstNs, srcNs;
	try {
		if (dstNsName == srcNsName.data()) return errOK;
		const char kSystemNamespacePrefix = '#';
		if (!srcNsName.empty() && srcNsName[0] == kSystemNamespacePrefix) {
			return Error(errParams, "Can't rename system namespace (%s)", srcNsName);
		}
		if (dstNsName.empty()) {
			return Error(errParams, "Can't rename namespace to empty name");
		}
		if (!validateObjectName(dstNsName)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed (%s)", dstNsName);
		}

		if (dstNsName[0] == kSystemNamespacePrefix) {
			return Error(errParams, "Can't rename to system namespace name (%s)", dstNsName);
		}

		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "RENAME " << srcNsName << " to " << dstNsName).Slice() : ""_sv, activities_);

		ULock lock(mtx_, &rdxCtx);
		auto srcIt = namespaces_.find(srcNsName);
		if (srcIt == namespaces_.end()) {
			return Error(errParams, "Namespace '%s' doesn't exist", srcNsName);
		}
		srcNs = srcIt->second;
		assert(srcNs != nullptr);

		auto dstIt = namespaces_.find(dstNsName);
		auto needWalUpdate = !srcNs->GetDefinition(rdxCtx).isTemporary;
		if (dstIt != namespaces_.end()) {
			dstNs = dstIt->second;
			assert(dstNs != nullptr);
			srcNs->Rename(dstNs, storagePath_, rdxCtx);
		} else {
			srcNs->Rename(dstNsName, storagePath_, rdxCtx);
		}
		if (needWalUpdate) observers_.OnWALUpdate(LSNPair(), srcNsName, WALRecord(WalNamespaceRename, dstNsName));

		auto srcNamespace = srcIt->second;
		namespaces_.erase(srcIt);
		namespaces_[dstNsName] = std::move(srcNamespace);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Insert(string_view nsName, Item& item, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "INSERT INTO " << nsName).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		ns->Insert(item, rdxCtx);
		updateToSystemNamespace(nsName, item, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

static void printPkValue(const Item::FieldRef& f, WrSerializer& ser) {
	ser << f.Name() << " = ";
	Variant(f).Dump(ser);
}

static WrSerializer& printPkFields(const Item& item, WrSerializer& ser) {
	size_t jsonPathIdx = 0;
	const FieldsSet fields = item.PkFields();
	for (auto it = fields.begin(); it != fields.end(); ++it) {
		if (it != fields.begin()) ser << " AND ";
		int field = *it;
		if (field == IndexValueType::SetByJsonPath) {
			assert(jsonPathIdx < fields.getTagsPathsLength());
			printPkValue(item[fields.getJsonPath(jsonPathIdx++)], ser);
		} else {
			printPkValue(item[field], ser);
		}
	}
	return ser;
}

Error ReindexerImpl::Update(string_view nsName, Item& item, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "UPDATE " << nsName << " WHERE ", printPkFields(item, ser)).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);

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
		auto ns = getNamespace(q._namespace, rdxCtx);
		ns->Update(q, result, rdxCtx);
		if (ns->IsSystem(rdxCtx)) {
			for (auto it = result.begin(); it != result.end(); ++it) {
				auto item = it.GetItem();
				updateToSystemNamespace(ns->GetName(), item, rdxCtx);
			}
		}
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
		auto ns = getNamespace(nsName, rdxCtx);
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
Transaction ReindexerImpl::NewTransaction(string_view _namespace, const InternalRdxContext& ctx) {
	try {
		const RdxContext rdxCtx = ctx.CreateRdxContext("START TRANSACTION"_sv, activities_);
		return getNamespace(_namespace, rdxCtx)->NewTransaction(rdxCtx);
	} catch (const Error& err) {
		return Transaction(err);
	}
}

Error ReindexerImpl::CommitTransaction(Transaction& tr, QueryResults& result, const InternalRdxContext& ctx) {
	Error err = errOK;
	try {
		WrSerializer ser;
		const RdxContext rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "COMMIT TRANSACTION "_sv << tr.GetName()).Slice() : ""_sv, activities_);
		// for (auto& step : tr.GetSteps()) updateToSystemNamespace(tr.GetName(), step.item_, rdxCtx);
		getNamespace(tr.GetName(), rdxCtx)->CommitTransaction(tr, result, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}

	return err;
}
Error ReindexerImpl::RollBackTransaction(Transaction& tr) {
	tr.GetSteps().clear();

	return errOK;
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
		getNamespace(nsName, rdxCtx)->PutMeta(key, data, rdxCtx);
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
		auto ns = getNamespace(nsName, rdxCtx);
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
		auto ns = getNamespace(q._namespace, rdxCtx);
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
		if (lhs.Proc() == rhs.Proc()) {
			if (lhs.Nsid() == rhs.Nsid()) {
				return lhs.Id() < rhs.Id();
			}
			return lhs.Nsid() < rhs.Nsid();
		}
		return lhs.Proc() > rhs.Proc();
	}
};

Error ReindexerImpl::Select(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	try {
		WrSerializer normalizedSQL, nonNormalizedSQL;
		if (ctx.NeedTraceActivity()) q.GetSQL(nonNormalizedSQL, false);
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? nonNormalizedSQL.Slice() : "", activities_, result);
		NsLocker<const RdxContext> locks(rdxCtx);

		auto mainNsWrp = getNamespace(q._namespace, rdxCtx);
		auto mainNs = q.IsWALQuery() ? mainNsWrp->awaitMainNs(rdxCtx) : mainNsWrp->getMainNs();

		ProfilingConfigData profilingCfg = configProvider_.GetProfilingConfig();
		PerfStatCalculatorMT calc(mainNs->selectPerfCounter_, mainNs->enablePerfCounters_);	 // todo more accurate detect joined queries
		auto& tracker = queriesStatTracker_;
		if (profilingCfg.queriesPerfStats) {
			q.GetSQL(normalizedSQL, true);
			if (!ctx.NeedTraceActivity()) q.GetSQL(nonNormalizedSQL, false);
		}
		const QueriesStatTracer::QuerySQL sql{normalizedSQL.Slice(), nonNormalizedSQL.Slice()};
		QueryStatCalculator statCalculator(
			[&sql, &tracker](bool lockHit, std::chrono::microseconds time) {
				if (lockHit)
					tracker.LockHit(sql, time);
				else
					tracker.Hit(sql, time);
			},
			std::chrono::microseconds(profilingCfg.queriedThresholdUS), profilingCfg.queriesPerfStats);

		if (q._namespace.size() && q._namespace[0] == '#') {
			string filterNsName;
			if (q.entries.Size() == 1 && q.entries.IsValue(0) && q.entries[0].condition == CondEq && q.entries[0].values.size() == 1)
				filterNsName = q.entries[0].values[0].As<string>();

			syncSystemNamespaces(q._namespace, filterNsName, rdxCtx);
		}
		// Lookup and lock namespaces_
		mainNs->updateSelectTime();
		locks.Add(mainNs);
		q.WalkNested(false, true, [this, &locks, &rdxCtx](const Query& q) {
			auto nsWrp = getNamespace(q._namespace, rdxCtx);
			auto ns = q.IsWALQuery() ? nsWrp->awaitMainNs(rdxCtx) : nsWrp->getMainNs();
			ns->updateSelectTime();
			locks.Add(ns);
		});

		locks.Lock();

		calc.LockHit();
		statCalculator.LockHit();

		SelectFunctionsHolder func;
		doSelect(q, result, locks, func, rdxCtx);
		func.Process(result);
	} catch (const Error& err) {
		if (ctx.Compl()) ctx.Compl()(err);
		return err;
	}
	if (ctx.Compl()) ctx.Compl()(errOK);
	return errOK;
}

struct ReindexerImpl::QueryResultsContext {
	QueryResultsContext() {}
	QueryResultsContext(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet& fieldsFilter)
		: type_(type), tagsMatcher_(tagsMatcher), fieldsFilter_(fieldsFilter) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsSet fieldsFilter_;
};

bool ReindexerImpl::isPreResultValuesModeOptimizationAvailable(const Query& jItemQ, const NamespaceImpl::Ptr& jns) {
	bool result = true;
	jItemQ.entries.ForEachEntry([&jns, &result](const QueryEntry& qe) {
		if (qe.idxNo >= 0) {
			assert(jns->indexes_.size() > static_cast<size_t>(qe.idxNo));
			const IndexType indexType = jns->indexes_[qe.idxNo]->Type();
			if (isComposite(indexType) || isFullText(indexType)) result = false;
		}
	});
	return result;
}

void ReindexerImpl::prepareJoinResults(const Query& q, QueryResults& result) {
	bool thereAreJoins = !q.joinQueries_.empty();
	if (!thereAreJoins) {
		for (const Query& mq : q.mergeQueries_) {
			if (!mq.joinQueries_.empty()) {
				thereAreJoins = true;
				break;
			}
		}
	}
	if (thereAreJoins) {
		result.joined_.resize(1 + q.mergeQueries_.size());
	}
}

template <typename T>
JoinedSelectors ReindexerImpl::prepareJoinedSelectors(const Query& q, QueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func,
													  vector<QueryResultsContext>& queryResultsContexts, const RdxContext& rdxCtx) {
	JoinedSelectors joinedSelectors;
	if (q.joinQueries_.empty()) return joinedSelectors;
	auto ns = locks.Get(q._namespace);
	assert(ns);

	// For each joined queries
	int joinedSelectorsCount = q.joinQueries_.size();
	for (auto& jq : q.joinQueries_) {
		// Get common results from joined namespaces_
		auto jns = locks.Get(jq._namespace);
		assert(jns);

		// Do join for each item in main result
		Query jItemQ(jq._namespace);
		jItemQ.explain_ = q.explain_;
		jItemQ.Debug(jq.debugLevel).Limit(jq.count);
		jItemQ.Strict(q.strictMode);
		for (size_t i = 0; i < jq.sortingEntries_.size(); ++i) {
			jItemQ.Sort(jq.sortingEntries_[i].expression, jq.sortingEntries_[i].desc);
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

		Query jjq(jq);
		JoinPreResult::Ptr preResult = std::make_shared<JoinPreResult>();
		size_t joinedFieldIdx = joinedSelectors.size();
		JoinCacheRes joinRes;
		joinRes.key.SetData(jq);
		jns->getFromJoinCache(joinRes);
		jjq.explain_ = q.explain_;
		jjq.Strict(q.strictMode);
		if (!jjq.entries.Empty() && !joinRes.haveData) {
			QueryResults jr;
			jjq.Limit(UINT_MAX);
			SelectCtx ctx(jjq);
			ctx.preResult = preResult;
			ctx.preResult->executionMode = JoinPreResult::ModeBuild;
			ctx.preResult->enableStoredValues = isPreResultValuesModeOptimizationAvailable(jItemQ, jns);
			ctx.functions = &func;
			jns->Select(jr, ctx, rdxCtx);
			assert(ctx.preResult->executionMode == JoinPreResult::ModeExecute);
		}
		if (joinRes.haveData) {
			preResult = joinRes.it.val.preResult;
		} else if (joinRes.needPut) {
			jns->putToJoinCache(joinRes, preResult);
		}

		queryResultsContexts.emplace_back(jns->payloadType_, jns->tagsMatcher_, FieldsSet(jns->tagsMatcher_, jq.selectFilter_));

		if (preResult->dataMode == JoinPreResult::ModeValues) {
			jItemQ.entries.ForEachEntry([&jns](QueryEntry& qe) {
				if (jns->indexes_[qe.idxNo]->Opts().IsSparse()) qe.idxNo = IndexValueType::SetByJsonPath;
			});
			if (!preResult->values.Locked()) preResult->values.Lock();	// If not from cache
			locks.Delete(jns);
			jns.reset();
		}
		joinedSelectors.push_back({jq.joinType, ns, std::move(jns), std::move(joinRes), std::move(jItemQ), result, jq, preResult,
								   joinedFieldIdx, func, joinedSelectorsCount, rdxCtx});
		ThrowOnCancel(rdxCtx);
	}
	return joinedSelectors;
}

template <typename T>
void ReindexerImpl::doSelect(const Query& q, QueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func, const RdxContext& ctx) {
	auto ns = locks.Get(q._namespace);
	assert(ns);
	if (!ns) {
		throw Error(errParams, "Namespace '%s' is not exists", q._namespace);
	}
	vector<QueryResultsContext> joinQueryResultsContexts;
	// should be destroyed after results.lockResults()
	JoinedSelectors mainJoinedSelectors = prepareJoinedSelectors(q, result, locks, func, joinQueryResultsContexts, ctx);
	prepareJoinResults(q, result);
	{
		SelectCtx selCtx(q);
		selCtx.joinedSelectors = mainJoinedSelectors.size() ? &mainJoinedSelectors : nullptr;
		selCtx.contextCollectingMode = true;
		selCtx.functions = &func;
		selCtx.nsid = 0;
		selCtx.isForceAll = !q.mergeQueries_.empty();
		ns->Select(result, selCtx, ctx);
	}

	// should be destroyed after results.lockResults()
	vector<JoinedSelectors> mergeJoinedSelectors;
	if (!q.mergeQueries_.empty()) {
		mergeJoinedSelectors.reserve(q.mergeQueries_.size());
		uint8_t counter = 0;

		for (auto& mq : q.mergeQueries_) {
			auto mns = locks.Get(mq._namespace);
			assert(mns);
			SelectCtx mctx(mq);
			mctx.nsid = ++counter;
			mctx.isForceAll = true;
			mctx.functions = &func;
			mctx.contextCollectingMode = true;
			mergeJoinedSelectors.emplace_back(prepareJoinedSelectors(mq, result, locks, func, joinQueryResultsContexts, ctx));
			mctx.joinedSelectors = mergeJoinedSelectors.back().size() ? &mergeJoinedSelectors.back() : nullptr;

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
	for (const auto& jctx : joinQueryResultsContexts) result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_);
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
	return nsIt->second;
}

Namespace::Ptr ReindexerImpl::getNamespaceNoThrow(string_view nsName, const RdxContext& ctx) {
	SLock lock(mtx_, &ctx);
	auto nsIt = namespaces_.find(nsName);
	return (nsIt == namespaces_.end()) ? nullptr : nsIt->second;
}

Error ReindexerImpl::AddIndex(string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "CREATE INDEX " << indexDef.name_ << " ON " << nsName).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		ns->AddIndex(indexDef, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::SetSchema(string_view nsName, string_view schema, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "SET SCHEMA ON " << nsName).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		ns->SetSchema(schema, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

Error ReindexerImpl::GetSchema(string_view nsName, std::string& schema, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "GET SCHEMA ON " << nsName).Slice() : ""_sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		ns->GetSchema(schema, rdxCtx);
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
		auto ns = getNamespace(nsName, rdxCtx);
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
		auto ns = getNamespace(nsName, rdxCtx);
		ns->DropIndex(indexDef, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return Error(errOK);
}

std::vector<std::pair<std::string, Namespace::Ptr>> ReindexerImpl::getNamespaces(const RdxContext& ctx) {
	SLock lock(mtx_, &ctx);
	std::vector<std::pair<std::string, Namespace::Ptr>> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.emplace_back(ns.first, ns.second);
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

Error ReindexerImpl::EnumNamespaces(vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx) {
	logPrintf(LogTrace, "ReindexerImpl::EnumNamespaces (%d,%s)", opts.options_, opts.filter_);
	try {
		const auto rdxCtx = ctx.CreateRdxContext("SELECT NAMESPACES", activities_);
		auto nsarray = getNamespaces(rdxCtx);
		for (auto& nspair : nsarray) {
			if (!opts.MatchFilter(nspair.first)) continue;
			NamespaceDef nsDef(nspair.first);
			if (!opts.IsOnlyNames()) {
				nsDef = nspair.second->GetDefinition(rdxCtx);
			}
			if (nsDef.name == nspair.first) {
				defs.emplace_back(std::move(nsDef));
			}
		}

		if (opts.IsWithClosed() && !storagePath_.empty()) {
			vector<fs::DirEntry> dirs;
			if (fs::ReadDir(storagePath_, dirs) != 0) return Error(errLogic, "Could not read database dir");

			for (auto& d : dirs) {
				if (d.isDir && d.name != "." && d.name != ".." && opts.MatchFilter(d.name)) {
					{
						SLock lock(mtx_, &rdxCtx);
						if (namespaces_.find(d.name) != namespaces_.end()) continue;
					}
					unique_ptr<NamespaceImpl> tmpNs(new NamespaceImpl(d.name, observers_));
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
				auto ns = getNamespace(name, dummyCtx);
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
					 .AddIndex("selects.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("transactions.total_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("transactions.total_copy_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("transactions.avg_steps_count", "-", "int64", IndexOpts().Dense())
					 .AddIndex("transactions.avg_prepare_time_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("transactions.avg_commit_time_us", "-", "int64", IndexOpts().Dense())
					 .AddIndex("transactions.avg_copy_time_us", "-", "int64", IndexOpts().Dense()));

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

	AddNamespace(NamespaceDef(kClientsStatsNamespace, StorageOpts())
					 .AddIndex("connection_id", "hash", "int", IndexOpts().PK())
					 .AddIndex("ip", "-", "string", IndexOpts().Dense())
					 .AddIndex("user_name", "-", "string", IndexOpts().Dense())
					 .AddIndex("user_rights", "-", "string", IndexOpts().Dense())
					 .AddIndex("db_name", "-", "string", IndexOpts().Dense())
					 .AddIndex("current_activity", "-", "string", IndexOpts().Dense())
					 .AddIndex("start_time", "-", "int64", IndexOpts().Dense())
					 .AddIndex("send_bytes", "-", "int64", IndexOpts().Dense())
					 .AddIndex("recv_bytes", "-", "int64", IndexOpts().Dense()));
}

std::vector<string> defDBConfig = {
	R"json({
		"type":"profiling", 
		"profiling":{
            "queriesperfstats":false,
			"queries_threshold_us":10,
			"perfstats":false,
			"memstats":true,
			"activitystats":false
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
				"start_copy_policy_tx_size":10000
				"copy_policy_multiplier":5
				"tx_size_to_always_copy":100000,
				"optimization_timeout_ms":800,
				"optimization_sort_workers":4,
				"wal_size":4000000,
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
			ns.second->OnConfigUpdated(configProvider_, ctx);
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
		for (auto& ns : getNamespaces(ctx)) ns.second->ResetPerfStat(ctx);
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

void ReindexerImpl::syncSystemNamespaces(string_view sysNsName, string_view filterNsName, const RdxContext& ctx) {
	logPrintf(LogTrace, "ReindexerImpl::syncSystemNamespaces (%s,%s)", sysNsName, filterNsName);
	auto nsarray = getNamespaces(ctx);
	WrSerializer ser;
	const auto activityCtx = ctx.OnlyActivity();

	auto forEachNS = [&](Namespace::Ptr sysNs, bool withSystem, std::function<bool(std::pair<string, Namespace::Ptr> & nspair)> filler) {
		std::vector<Item> items;
		items.reserve(nsarray.size());
		for (auto& nspair : nsarray) {
			if (!filterNsName.empty() && filterNsName != nspair.first) continue;
			if (nspair.second->IsSystem(activityCtx) && !withSystem) continue;
			ser.Reset();
			if (filler(nspair)) {
				items.emplace_back(sysNs->NewItem(ctx));
				auto err = items.back().FromJSON(ser.Slice());
				if (!err.ok()) {
					throw err;
				}
			}
		}
		sysNs->Refill(items, NsContext(ctx));
	};

	ProfilingConfigData profilingCfg = configProvider_.GetProfilingConfig();

	if (profilingCfg.perfStats && sysNsName == kPerfStatsNamespace) {
		forEachNS(getNamespace(kPerfStatsNamespace, ctx), false, [&](std::pair<string, Namespace::Ptr>& nspair) {
			auto stats = nspair.second->GetPerfStat(ctx);
			bool notRenamed = (stats.name == nspair.first);
			if (notRenamed) stats.GetJSON(ser);
			return notRenamed;
		});
	}

	if (profilingCfg.memStats && sysNsName == kMemStatsNamespace) {
		forEachNS(getNamespace(kMemStatsNamespace, ctx), false, [&](std::pair<string, Namespace::Ptr>& nspair) {
			auto stats = nspair.second->GetMemStat(ctx);
			bool notRenamed = (stats.name == nspair.first);
			if (notRenamed) stats.GetJSON(ser);
			return notRenamed;
		});
	}

	if (sysNsName == kNamespacesNamespace) {
		forEachNS(getNamespace(kNamespacesNamespace, ctx), true, [&](std::pair<string, Namespace::Ptr>& nspair) {
			auto stats = nspair.second->GetDefinition(ctx);
			bool notRenamed = (stats.name == nspair.first);
			if (notRenamed) stats.GetJSON(ser, kIndexJSONWithDescribe);
			return notRenamed;
		});
	}

	if (profilingCfg.queriesPerfStats && sysNsName == kQueriesPerfStatsNamespace) {
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
		queriesperfstatsNs->Refill(items, NsContext(ctx));
	}

	if (sysNsName == kActivityStatsNamespace) {
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
		activityNs->Refill(items, NsContext(ctx));
	}
	if (sysNsName == kClientsStatsNamespace) {
		if (clientsStats_) {
			std::vector<ClientStat> clientInf;
			clientsStats_->GetClientInfo(clientInf);
			auto observers = observers_.Get();
			auto clientsNs = getNamespace(kClientsStatsNamespace, ctx);
			std::vector<Item> items;
			items.reserve(clientInf.size());
			for (auto& i : clientInf) {
				ser.Reset();
				Activity activ;
				bool isExist = activities_.ActivityForIpConnection(i.connectionId, activ);
				if (isExist) i.currentActivity = activ.query;
				for (auto obsIt = observers.begin(); obsIt != observers.end(); ++obsIt) {
					if (obsIt->ptr == i.updatesPusher) {
						i.isSubscribed = true;
						i.updatesFilters = std::move(obsIt->filters);
						observers.erase(obsIt);
						break;
					}
				}
				i.GetJSON(ser);
				items.emplace_back(clientsNs->NewItem(ctx));
				auto err = items.back().FromJSON(ser.Slice());
				if (!err.ok()) throw err;
			}
			clientsNs->Refill(items, NsContext(ctx));
		}
	}
}

void ReindexerImpl::onProfiligConfigLoad() {
	QueryResults qr1, qr2, qr3;
	Delete(Query(kMemStatsNamespace), qr2);
	Delete(Query(kQueriesPerfStatsNamespace), qr3);
	Delete(Query(kPerfStatsNamespace), qr1);
}

Error ReindexerImpl::SubscribeUpdates(IUpdatesObserver* observer, const UpdatesFilters& filters, SubscriptionOpts opts) {
	return observers_.Add(observer, filters, opts);
}

Error ReindexerImpl::UnsubscribeUpdates(IUpdatesObserver* observer) { return observers_.Delete(observer); }

Error ReindexerImpl::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string>& suggestions, const InternalRdxContext& ctx) {
	Query query;
	SQLSuggester suggester(query);
	std::vector<NamespaceDef> nses;

	suggestions = suggester.GetSuggestions(
		sqlQuery, pos,
		[&, this](EnumNamespacesOpts opts) {
			EnumNamespaces(nses, opts, ctx);
			return nses;
		},
		[&ctx, this](string_view ns) {
			auto rdxCtx = ctx.CreateRdxContext(""_sv, activities_);
			auto nsPtr = getNamespaceNoThrow(ns, rdxCtx);
			if (nsPtr) {
				return nsPtr->getMainNs()->GetSchemaPtr(rdxCtx);
			}
			return std::shared_ptr<const Schema>();
		});
	return errOK;
}

Error ReindexerImpl::Status() {
	if (connected_.load(std::memory_order_acquire)) {
		return errOK;
	}
	return Error(errNotValid, "DB is not connected"_sv);
}

}  // namespace reindexer
