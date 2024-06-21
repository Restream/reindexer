#include "reindexerimpl.h"

#include <stdio.h>
#include <chrono>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/defnsconfigs.h"
#include "core/iclientsstats.h"
#include "core/index/index.h"
#include "core/itemimpl.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/query/sql/sqlsuggester.h"
#include "core/queryresults/joinresults.h"
#include "core/selectfunc/selectfunc.h"
#include "core/type_consts_helpers.h"
#include "debug/crashqueryreporter.h"
#include "estl/defines.h"
#include "replicator/replicator.h"
#include "rx_selector.h"
#include "server/outputparameters.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"

#include "debug/backtrace.h"
#include "debug/terminate_handler.h"

static std::once_flag initTerminateHandlerFlag;

using namespace std::placeholders;
using namespace std::string_view_literals;

namespace reindexer {

constexpr char kStoragePlaceholderFilename[] = ".reindexer.storage";
constexpr char kReplicationConfFilename[] = "replication.conf";
constexpr unsigned kStorageLoadingThreads = 6;

static unsigned ConcurrentNamespaceLoaders() noexcept {
	const auto hwConc = std::thread::hardware_concurrency();
	if (hwConc <= 4) {
		return 1;
	} else if (hwConc < 8) {  // '<' is not a typo
		return 2;
	} else if (hwConc <= 16) {
		return 3;
	} else if (hwConc <= 24) {
		return 4;
	} else if (hwConc < 32) {  // '<' is not a typo
		return 5;
	} else if (hwConc <= 42) {
		return 6;
	}
	return 10;
}

ReindexerImpl::ReindexerImpl(ReindexerConfig cfg)
	: replicator_(new Replicator(this)), storageType_(StorageType::LevelDB), clientsStats_(cfg.clientsStats) {
	configProvider_.setHandler(ProfilingConf, std::bind(&ReindexerImpl::onProfiligConfigLoad, this));
	backgroundThread_.Run([this](net::ev::dynamic_loop& loop) { this->backgroundRoutine(loop); });

#ifdef REINDEX_WITH_GPERFTOOLS
	if (alloc_ext::TCMallocIsAvailable()) {
		heapWatcher_ = TCMallocHeapWathcher(alloc_ext::instance(), cfg.allocatorCacheLimit, cfg.allocatorCachePart);
	}
#endif

	storageFlushingThread_.Run([this](net::ev::dynamic_loop& loop) { this->storageFlushingRoutine(loop); });
	std::call_once(initTerminateHandlerFlag, []() {
		debug::terminate_handler_init();
		debug::backtrace_set_crash_query_reporter(&reindexer::PrintCrashedQuery);
	});
}

ReindexerImpl::~ReindexerImpl() {
	for (auto& ns : namespaces_) {
		// Add extra checks to avoid GCC 13 warnings in Release build. Actually namespaces are never null
		if (ns.second) {
			if (auto mainNs = ns.second->getMainNs(); mainNs) {
				mainNs->SetDestroyFlag();
			}
		}
	}

	dbDestroyed_ = true;
	backgroundThread_.Stop();
	storageFlushingThread_.Stop();
	replicator_->Stop();
}

ReindexerImpl::StatsLocker::StatsLocker() {
	mtxMap_.try_emplace(kMemStatsNamespace);
	mtxMap_.try_emplace(kPerfStatsNamespace);
	mtxMap_.try_emplace(kNamespacesNamespace);
	// kQueriesPerfStatsNamespace - lock is not required
	// kActivityStatsNamespace - lock is not required
	// kClientsStatsNamespace - lock is not required
}

ReindexerImpl::StatsLocker::StatsLockT ReindexerImpl::StatsLocker::LockIfRequired(std::string_view sysNsName, const RdxContext& ctx) {
	auto found = mtxMap_.find(sysNsName);
	if (found != mtxMap_.end()) {
		return StatsLockT(found->second, ctx);
	}
	// Do not create any lock, if namespace does not preset in the map
	return StatsLockT();
}

Error ReindexerImpl::EnableStorage(const std::string& storagePath, bool skipPlaceholderCheck, const InternalRdxContext& ctx) {
	if (!storagePath_.empty()) {
		return Error(errParams, "Storage already enabled");
	}

	if (storagePath.empty()) return errOK;
	if (fs::MkDirAll(storagePath) < 0) {
		return Error(errParams, "Can't create directory '%s' for reindexer storage - reason %s", storagePath, strerror(errno));
	}

	std::vector<fs::DirEntry> dirEntries;
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
		if (res >= 0) {
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
		try {
			WrSerializer ser;
			const auto rdxCtx =
				ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "OPEN NAMESPACE " << kConfigNamespace).Slice() : ""sv, activities_);
			res = openNamespace(kConfigNamespace, StorageOpts().Enabled().CreateIfMissing(), rdxCtx);
		} catch (const Error& err) {
			return err;
		}
	}
	replConfigFileChecker_.SetFilepath(fs::JoinPath(storagePath_, kReplicationConfFilename));

	return res;
}

Error ReindexerImpl::Connect(const std::string& dsn, ConnectOpts opts) {
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
	std::string path = dsn;
	if (dsn.compare(0, 10, "builtin://") == 0) {
		path = dsn.substr(10);
	}

	std::vector<reindexer::fs::DirEntry> foundNs;

	switch (opts.StorageType()) {
		case kStorageTypeOptLevelDB:
			storageType_ = StorageType::LevelDB;
			break;
		case kStorageTypeOptRocksDB:
			storageType_ = StorageType::RocksDB;
			break;
	}

	autorepairEnabled_ = opts.IsAutorepair();
	replicationEnabled_ = !opts.IsReplicationDisabled();

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
		boost::sort::pdqsort_branchless(foundNs.begin(), foundNs.end(), [](const fs::DirEntry& ld, const fs::DirEntry& rd) noexcept {
			return ld.internalFilesCount > rd.internalFilesCount;
		});
		const size_t maxLoadWorkers = ConcurrentNamespaceLoaders();
		std::unique_ptr<std::thread[]> thrs(new std::thread[maxLoadWorkers]);
		std::atomic_flag hasNsErrors = ATOMIC_FLAG_INIT;
		std::atomic<unsigned> nsIdx = {0};
		for (size_t i = 0; i < maxLoadWorkers; i++) {
			thrs[i] = std::thread([&] {
				for (unsigned idx = nsIdx.fetch_add(1, std::memory_order_relaxed); idx < foundNs.size();
					 idx = nsIdx.fetch_add(1, std::memory_order_relaxed)) {
					auto& de = foundNs[idx];
					if (de.isDir && validateObjectName(de.name, true)) {
						if (de.name[0] == kTmpNsPrefix) {
							const std::string tmpPath = fs::JoinPath(storagePath_, de.name);
							logPrintf(LogWarning, "Dropping tmp namespace '%s'", de.name);
							if (fs::RmDirAll(tmpPath) < 0) {
								logPrintf(LogWarning, "Failed to remove '%s' temporary namespace from filesystem, path: %s", de.name,
										  tmpPath);
								hasNsErrors.test_and_set(std::memory_order_relaxed);
							}
							continue;
						}
						const RdxContext dummyCtx;
						auto status = openNamespace(de.name, StorageOpts().Enabled(), dummyCtx);
						if (status.ok()) {
							if (getNamespace(de.name, dummyCtx)->IsTemporary(dummyCtx)) {
								logPrintf(LogWarning, "Dropping tmp namespace '%s'", de.name);
								status = closeNamespace(de.name, dummyCtx, true, true);
							}
						}
						if (!status.ok()) {
							logPrintf(LogError, "Failed to open namespace '%s' - %s", de.name, status.what());
							hasNsErrors.test_and_set(std::memory_order_relaxed);
						}
					}
				}
			});
		}
		for (size_t i = 0; i < maxLoadWorkers; ++i) thrs[i].join();

		if (!opts.IsAllowNamespaceErrors() && hasNsErrors.test_and_set(std::memory_order_relaxed)) {
			return Error(errNotValid, "Namespaces load error");
		}
	}

	if (replicationEnabled_) {
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
	}

	if (err.ok()) {
		connected_.store(true, std::memory_order_release);
	}
	return err;
}

Error ReindexerImpl::OpenNamespace(std::string_view nsName, const StorageOpts& opts, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "OPEN NAMESPACE " << nsName).Slice() : ""sv, activities_);
		{
			SLock lock(mtx_, rdxCtx);
			auto it = namespaces_.find(nsName);
			if (it == namespaces_.end()) {	// create new namespace
				if (!validateUserNsName(nsName)) {
					return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
				}
			}
		}
		return openNamespace(nsName, opts, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
}

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx) {
	if (!validateUserNsName(nsDef.name)) {
		return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
	}
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE NAMESPACE " << nsDef.name).Slice() : ""sv, activities_);
		return addNamespace(nsDef, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
}

Error ReindexerImpl::addNamespace(const NamespaceDef& nsDef, const RdxContext& rdxCtx) {
	Namespace::Ptr ns;
	try {
		{
			SLock lock(mtx_, rdxCtx);
			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name);
			}
		}
		if (!validateObjectName(nsDef.name, nsDef.isTemporary)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		bool readyToLoadStorage = (nsDef.storage.IsEnabled() && !storagePath_.empty());
		ns = std::make_shared<Namespace>(nsDef.name, observers_, bgDeleter_);
		if (nsDef.isTemporary) {
			ns->awaitMainNs(rdxCtx)->setTemporary();
		}
		if (readyToLoadStorage) {
			ns->EnableStorage(storagePath_, nsDef.storage, storageType_, rdxCtx);
		}
		ns->OnConfigUpdated(configProvider_, rdxCtx);
		if (readyToLoadStorage) {
			ns->LoadFromStorage(kStorageLoadingThreads, rdxCtx);
		}
		{
			ULock lock(mtx_, rdxCtx);
			namespaces_.insert({nsDef.name, ns});
		}
		if (!nsDef.isTemporary) observers_.OnWALUpdate(LSNPair(), nsDef.name, WALRecord(WalNamespaceAdd));
		for (auto& indexDef : nsDef.indexes) {
			ns->AddIndex(indexDef, rdxCtx);
		}
		ns->SetSchema(nsDef.schemaJson, rdxCtx);
		if (nsDef.storage.IsSlaveMode()) ns->setSlaveMode(rdxCtx);

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::openNamespace(std::string_view name, const StorageOpts& storageOpts, const RdxContext& rdxCtx) {
	try {
		{
			SLock lock(mtx_, rdxCtx);
			auto nsIt = namespaces_.find(name);
			if (nsIt != namespaces_.end() && nsIt->second) {
				if (storageOpts.IsSlaveMode()) nsIt->second->setSlaveMode(rdxCtx);
				return {};
			}
		}
		if (!validateObjectName(name, false)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		std::string nameStr(name);
		auto ns = std::make_shared<Namespace>(nameStr, observers_, bgDeleter_);
		if (storageOpts.IsSlaveMode()) ns->setSlaveMode(rdxCtx);
		if (storageOpts.IsEnabled() && !storagePath_.empty()) {
			auto opts = storageOpts;
			ns->EnableStorage(storagePath_, opts.Autorepair(autorepairEnabled_), storageType_, rdxCtx);
			ns->OnConfigUpdated(configProvider_, rdxCtx);
			ns->LoadFromStorage(kStorageLoadingThreads, rdxCtx);
		} else {
			ns->OnConfigUpdated(configProvider_, rdxCtx);
		}
		{
			std::lock_guard<shared_timed_mutex> lock(mtx_);
			namespaces_.insert({nameStr, ns});
		}
		observers_.OnWALUpdate(LSNPair(), name, WALRecord(WalNamespaceAdd));
	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::DropNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	WrSerializer ser;
	return closeNamespace(nsName,
						  ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "DROP NAMESPACE " << nsName).Slice() : ""sv, activities_),
						  true, false);
}
Error ReindexerImpl::CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	WrSerializer ser;
	return closeNamespace(nsName,
						  ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CLOSE NAMESPACE " << nsName).Slice() : ""sv, activities_),
						  false, false);
}

Error ReindexerImpl::closeNamespace(std::string_view nsName, const RdxContext& ctx, bool dropStorage, bool enableDropSlave) {
	Namespace::Ptr ns;
	Error err;
	try {
		ULock lock(mtx_, ctx);
		auto nsIt = namespaces_.find(nsName);
		if (nsIt == namespaces_.end()) {
			return Error(errNotFound, "Namespace '%s' does not exist", nsName);
		}
		// Temporary save namespace. This will call destructor without lock
		ns = nsIt->second;
		if (ns->GetReplState(ctx).slaveMode && !enableDropSlave) {
			return Error(errLogic, "Can't modify slave ns '%s'", nsName);
		}

		if (isSystemNamespaceNameStrict(nsName)) {
			return Error(errLogic, "Can't delete system ns '%s'", nsName);
		}
		namespaces_.erase(nsIt);

		if (dropStorage) {
			ns->DeleteStorage(ctx);
		} else {
			ns->CloseStorage(ctx);
		}
		if (dropStorage) {
			if (!nsIt->second->GetDefinition(ctx).isTemporary) {
				observers_.OnWALUpdate(LSNPair(), nsName, WALRecord(WalNamespaceDrop));
			}
		}

	} catch (const Error& e) {
		err = e;
	}
	if (ns) {
		bgDeleter_.Add(ns->atomicLoadMainNs());
	}
	return err;
}

Error ReindexerImpl::syncDownstream(std::string_view nsName, bool force, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "SYNCDOWNSTREAM " << nsName).Slice() : ""sv, activities_);
		NamespaceDef nsDef = getNamespace(nsName, rdxCtx)->GetDefinition(rdxCtx);
		nsDef.GetJSON(ser);
		ser.PutBool(true);
		observers_.OnWALUpdate(LSNPair(), nsName, WALRecord(force ? WalForceSync : WalWALSync, ser.Slice()));
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "TRUNCATE " << nsName).Slice() : ""sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		ns->Truncate(rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

Error ReindexerImpl::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const InternalRdxContext& ctx) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "RENAME " << srcNsName << " to " << dstNsName).Slice() : ""sv, activities_);
		{
			SLock lock(mtx_, rdxCtx);
			auto srcIt = namespaces_.find(srcNsName);
			if (srcIt == namespaces_.end()) {
				return Error(errParams, "Namespace '%s' doesn't exist", srcNsName);
			}
			Namespace::Ptr srcNs = srcIt->second;
			assertrx(srcNs != nullptr);

			if (srcNs->IsTemporary(rdxCtx)) {
				return Error(errParams, "Can't rename temporary namespace '%s'", srcNsName);
			}
		}
		err = renameNamespace(srcNsName, dstNsName, false, ctx);
	} catch (const Error& e) {
		return e;
	}
	return err;
}

Error ReindexerImpl::renameNamespace(std::string_view srcNsName, const std::string& dstNsName, bool fromReplication,
									 const InternalRdxContext& ctx) {
	Namespace::Ptr dstNs, srcNs;
	try {
		if (dstNsName == srcNsName.data()) {
			return {};
		}
		if (isSystemNamespaceNameStrict(srcNsName)) {
			return Error(errParams, "Can't rename system namespace (%s)", srcNsName);
		}
		if (dstNsName.empty()) {
			return Error(errParams, "Can't rename namespace to empty name");
		}
		if (!validateUserNsName(dstNsName)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}

		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(
			ctx.NeedTraceActivity() ? (ser << "RENAME " << srcNsName << " to " << dstNsName).Slice() : ""sv, activities_);

		{
			// Perform namespace flushes to minimize chances of the flush under lock
			SLock lock(mtx_, rdxCtx);
			auto srcIt = namespaces_.find(srcNsName);
			srcNs = (srcIt != namespaces_.end()) ? srcIt->second : Namespace::Ptr();
			lock.unlock();

			if (srcNs) {
				auto err = srcNs->awaitMainNs(rdxCtx)->FlushStorage(rdxCtx);
				if (!err.ok()) {
					return Error(err.code(), "Unable to flush storage before rename: %s", err.what());
				}
				srcNs.reset();
			}
		}

		ULock lock(mtx_, rdxCtx);
		auto srcIt = namespaces_.find(srcNsName);
		if (srcIt == namespaces_.end()) {
			return Error(errParams, "Namespace '%s' doesn't exist", srcNsName);
		}
		srcNs = srcIt->second;
		assertrx(srcNs != nullptr);

		auto replState = srcNs->GetReplState(rdxCtx);

		if (fromReplication || !replState.slaveMode) {	// rename from replicator forced temporary ns
			auto dstIt = namespaces_.find(dstNsName);
			auto needWalUpdate = !srcNs->GetDefinition(rdxCtx).isTemporary;
			if (dstIt != namespaces_.end()) {
				dstNs = dstIt->second;
				assertrx(dstNs != nullptr);
				srcNs->Rename(dstNs, storagePath_, rdxCtx);
			} else {
				srcNs->Rename(dstNsName, storagePath_, rdxCtx);
			}
			namespaces_.erase(srcIt);
			namespaces_[dstNsName] = std::move(srcNs);
			if (needWalUpdate) observers_.OnWALUpdate(LSNPair(), srcNsName, WALRecord(WalNamespaceRename, dstNsName));
		} else {
			return Error(errLogic, "Can't rename namespace in slave mode '%s'", srcNsName);
		}
	} catch (const Error& err) {
		return err;
	}
	return {};
}

template <bool needUpdateSystemNs, typename MakeCtxStrFn, typename MemFnType, MemFnType Namespace::*MemFn, typename Arg, typename... Args>
Error ReindexerImpl::applyNsFunction(std::string_view nsName, const InternalRdxContext& ctx, const MakeCtxStrFn& makeCtxStr, Arg arg,
									 Args... args) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? makeCtxStr(ser).Slice() : ""sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		(*ns.*MemFn)(arg, args..., rdxCtx);
		if constexpr (needUpdateSystemNs) {
			updateToSystemNamespace(nsName, arg, rdxCtx);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

template <typename>
struct IsVoidReturn;

template <typename R, typename... Args>
struct IsVoidReturn<R (Namespace::*)(Args...)> : public std::false_type {};

template <typename... Args>
struct IsVoidReturn<void (Namespace::*)(Args...)> : public std::true_type {};

template <auto MemFn, typename MakeCtxStrFn, typename Arg, typename... Args>
Error ReindexerImpl::applyNsFunction(std::string_view nsName, const InternalRdxContext& ctx, const MakeCtxStrFn& makeCtxStr, Arg& arg,
									 Args... args) {
	Error err;
	try {
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? makeCtxStr(ser).Slice() : ""sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		if constexpr (IsVoidReturn<decltype(MemFn)>::value) {
			(*ns.*MemFn)(arg, args..., rdxCtx);
		} else {
			arg = (*ns.*MemFn)(args..., rdxCtx);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

#define APPLY_NS_FUNCTION1(needUpdateSys, memFn, arg)                                                                                      \
	return applyNsFunction<needUpdateSys, decltype(makeCtxStr), void(decltype(arg), const RdxContext&), &Namespace::memFn, decltype(arg)>( \
		nsName, ctx, makeCtxStr, arg)

#define APPLY_NS_FUNCTION2(needUpdateSys, memFn, arg1, arg2)                                                             \
	return applyNsFunction<needUpdateSys, decltype(makeCtxStr), void(decltype(arg1), decltype(arg2), const RdxContext&), \
						   &Namespace::memFn, decltype(arg1), decltype(arg2)>(nsName, ctx, makeCtxStr, arg1, arg2)

Error ReindexerImpl::Insert(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName](WrSerializer& ser) -> WrSerializer& { return ser << "INSERT INTO " << nsName; };
	APPLY_NS_FUNCTION1(true, Insert, item);
}

Error ReindexerImpl::Insert(std::string_view nsName, Item& item, QueryResults& qr, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName](WrSerializer& ser) -> WrSerializer& { return ser << "INSERT INTO " << nsName; };
	APPLY_NS_FUNCTION2(true, Insert, item, qr);
}

static void printPkValue(const Item::FieldRef& f, WrSerializer& ser) {
	ser << f.Name() << " = ";
	f.operator Variant().Dump(ser);
}

static WrSerializer& printPkFields(const Item& item, WrSerializer& ser) {
	size_t jsonPathIdx = 0;
	const FieldsSet fields = item.PkFields();
	for (auto it = fields.begin(); it != fields.end(); ++it) {
		if (it != fields.begin()) ser << " AND ";
		int field = *it;
		if (field == IndexValueType::SetByJsonPath) {
			assertrx(jsonPathIdx < fields.getTagsPathsLength());
			printPkValue(item[fields.getJsonPath(jsonPathIdx++)], ser);
		} else {
			printPkValue(item[field], ser);
		}
	}
	return ser;
}

Error ReindexerImpl::Update(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &item](WrSerializer& ser) -> WrSerializer& {
		ser << "UPDATE " << nsName << " WHERE ";
		return printPkFields(item, ser);
	};
	APPLY_NS_FUNCTION1(true, Update, item);
}

Error ReindexerImpl::Update(std::string_view nsName, Item& item, QueryResults& qr, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &item](WrSerializer& ser) -> WrSerializer& {
		ser << "UPDATE " << nsName << " WHERE ";
		return printPkFields(item, ser);
	};
	APPLY_NS_FUNCTION2(true, Update, item, qr);
}

Error ReindexerImpl::Update(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	try {
		q.VerifyForUpdate();
		WrSerializer ser;
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? q.GetSQL(ser).Slice() : ""sv, activities_, result);
		auto ns = getNamespace(q.NsName(), rdxCtx);
		ns->Update(q, result, rdxCtx);
		if (ns->IsSystem(rdxCtx)) {
			const std::string kNsName = ns->GetName(rdxCtx);
			for (auto it = result.begin(); it != result.end(); ++it) {
				auto item = it.GetItem(false);
				updateToSystemNamespace(kNsName, item, rdxCtx);
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Upsert(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &item](WrSerializer& ser) -> WrSerializer& {
		ser << "UPSERT INTO " << nsName << " WHERE ";
		return printPkFields(item, ser);
	};
	APPLY_NS_FUNCTION1(true, Upsert, item);
}

Error ReindexerImpl::Upsert(std::string_view nsName, Item& item, QueryResults& qr, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &item](WrSerializer& ser) -> WrSerializer& {
		ser << "UPSERT INTO " << nsName << " WHERE ";
		return printPkFields(item, ser);
	};
	APPLY_NS_FUNCTION2(true, Upsert, item, qr);
}

Item ReindexerImpl::NewItem(std::string_view nsName, const InternalRdxContext& ctx) {
	try {
		WrSerializer ser;
		const auto rdxCtx =
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "CREATE ITEM FOR " << nsName).Slice() : ""sv, activities_);
		auto ns = getNamespace(nsName, rdxCtx);
		auto item = ns->NewItem(rdxCtx);
		item.impl_->SetNamespace(ns);
		return item;
	} catch (const Error& err) {
		return Item(err);
	}
}
Transaction ReindexerImpl::NewTransaction(std::string_view _namespace, const InternalRdxContext& ctx) {
	try {
		const RdxContext rdxCtx = ctx.CreateRdxContext("START TRANSACTION"sv, activities_);
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
			ctx.CreateRdxContext(ctx.NeedTraceActivity() ? (ser << "COMMIT TRANSACTION "sv << tr.GetName()).Slice() : ""sv, activities_);
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

Error ReindexerImpl::GetMeta(std::string_view nsName, const std::string& key, std::string& data, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &key](WrSerializer& ser) -> WrSerializer& {
		return ser << "SELECT META FROM " << nsName << " WHERE KEY = '" << key << '\'';
	};
	return applyNsFunction<&Namespace::GetMeta>(nsName, ctx, makeCtxStr, data, key);
}

Error ReindexerImpl::PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, data, &key](WrSerializer& ser) -> WrSerializer& {
		return ser << "UPDATE " << nsName << " SET META = '" << data << "' WHERE KEY = '" << key << '\'';
	};
	return applyNsFunction<&Namespace::PutMeta>(nsName, ctx, makeCtxStr, key, data);
}

Error ReindexerImpl::EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName](WrSerializer& ser) -> WrSerializer& { return ser << "SELECT META FROM " << nsName; };
	return applyNsFunction<&Namespace::EnumMeta>(nsName, ctx, makeCtxStr, keys);
}

Error ReindexerImpl::DeleteMeta(std::string_view nsName, const std::string& key, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &key](WrSerializer& ser) -> WrSerializer& {
		return ser << "DELETE META FROM " << nsName << " WHERE KEY = '" << key << '\'';
	};
	return applyNsFunction<&Namespace::DeleteMeta>(nsName, ctx, makeCtxStr, key);
}

Error ReindexerImpl::Delete(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &item](WrSerializer& ser) -> WrSerializer& {
		ser << "DELETE FROM " << nsName << " WHERE ";
		return printPkFields(item, ser);
	};
	APPLY_NS_FUNCTION1(false, Delete, item);
}

Error ReindexerImpl::Delete(std::string_view nsName, Item& item, QueryResults& qr, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &item](WrSerializer& ser) -> WrSerializer& {
		ser << "DELETE FROM " << nsName << " WHERE ";
		return printPkFields(item, ser);
	};
	APPLY_NS_FUNCTION2(false, Delete, item, qr);
}

Error ReindexerImpl::Delete(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	q.VerifyForUpdate();
	const auto makeCtxStr = [&q](WrSerializer& ser) -> WrSerializer& { return q.GetSQL(ser); };
	const std::string_view nsName = q.NsName();
	APPLY_NS_FUNCTION2(false, Delete, q, result);
}

Error ReindexerImpl::Select(std::string_view query, QueryResults& result, const InternalRdxContext& ctx) {
	Error err;
	try {
		Query q = Query::FromSQL(query);
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
				err = TruncateNamespace(q.NsName(), ctx);
				break;
			default:
				err = Error(errParams, "Error unsupported query type %d", q.type_);
		}
	} catch (const Error& e) {
		err = e;
	}

	if (ctx.Compl()) ctx.Compl()(err);
	return err;
}

Error ReindexerImpl::Select(const Query& q, QueryResults& result, const InternalRdxContext& ctx) {
	try {
		WrSerializer normalizedSQL, nonNormalizedSQL;
		if (ctx.NeedTraceActivity()) q.GetSQL(nonNormalizedSQL, false);
		const auto rdxCtx = ctx.CreateRdxContext(ctx.NeedTraceActivity() ? nonNormalizedSQL.Slice() : "", activities_, result);
		RxSelector::NsLocker<const RdxContext> locks(rdxCtx);

		auto mainNsWrp = getNamespace(q.NsName(), rdxCtx);
		auto mainNs = q.IsWALQuery() ? mainNsWrp->awaitMainNs(rdxCtx) : mainNsWrp->getMainNs();

		const auto queriesPerfStatsEnabled = configProvider_.QueriesPerfStatsEnabled();
		const auto queriesThresholdUS = configProvider_.QueriesThresholdUS();
		PerfStatCalculatorMT calc(mainNs->selectPerfCounter_, mainNs->enablePerfCounters_);	 // todo more accurate detect joined queries
		auto& tracker = queriesStatTracker_;
		if (queriesPerfStatsEnabled) {
			q.GetSQL(normalizedSQL, true);
			if (!ctx.NeedTraceActivity()) q.GetSQL(nonNormalizedSQL, false);
		}
		const QueriesStatTracer::QuerySQL sql{normalizedSQL.Slice(), nonNormalizedSQL.Slice()};

		auto hitter = queriesPerfStatsEnabled
		? [&sql, &tracker](bool lockHit, std::chrono::microseconds time) {
			if (lockHit)
				tracker.LockHit(sql, time);
			else
				tracker.Hit(sql, time);
		}
		: std::function<void(bool, std::chrono::microseconds)>{};

		const bool isSystemNsRequest = isSystemNamespaceNameFast(q.NsName());
		QueryStatCalculator statCalculator(
			std::move(hitter), std::chrono::microseconds(queriesThresholdUS),
			queriesPerfStatsEnabled || configProvider_.GetSelectLoggingParams().thresholdUs >= 0,
			long_actions::MakeLogger<QueryType::QuerySelect>(
				q, isSystemNsRequest ? LongQueriesLoggingParams{} : configProvider_.GetSelectLoggingParams()));

		StatsLocker::StatsLockT statsSelectLck;
		if (isSystemNsRequest) {
			statsSelectLck = syncSystemNamespaces(q.NsName(), detectFilterNsNames(q), rdxCtx);
		}
		// Lookup and lock namespaces_
		mainNs->updateSelectTime();
		locks.Add(std::move(mainNs));
		q.WalkNested(false, true, true, [this, &locks, &rdxCtx](const Query& q) {
			auto nsWrp = getNamespace(q.NsName(), rdxCtx);
			auto ns = q.IsWALQuery() ? nsWrp->awaitMainNs(rdxCtx) : nsWrp->getMainNs();
			ns->updateSelectTime();
			locks.Add(std::move(ns));
		});

		locks.Lock();
		calc.LockHit();
		statCalculator.LockHit();

		if (statsSelectLck.owns_lock()) {
			// Allow concurrent Refill's preparation for system namespaces during Select execution
			statsSelectLck.unlock();
		}

		SelectFunctionsHolder func;
		RxSelector::DoSelect(q, result, locks, func, rdxCtx, statCalculator);
		func.Process(result);
	} catch (const Error& err) {
		if (ctx.Compl()) ctx.Compl()(err);
		return err;
	}
	if (ctx.Compl()) ctx.Compl()(Error());
	return Error();
}

Error ReindexerImpl::Commit(std::string_view /*_namespace*/) {
	try {
		// getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Namespace::Ptr ReindexerImpl::getNamespace(std::string_view nsName, const RdxContext& ctx) {
	SLock lock(mtx_, ctx);
	auto nsIt = namespaces_.find(nsName);
	if (nsIt == namespaces_.end()) {
		throw Error(errParams, "Namespace '%s' does not exist", nsName);
	}

	assertrx(nsIt->second);
	return nsIt->second;
}

Namespace::Ptr ReindexerImpl::getNamespaceNoThrow(std::string_view nsName, const RdxContext& ctx) {
	SLock lock(mtx_, ctx);
	auto nsIt = namespaces_.find(nsName);
	return (nsIt == namespaces_.end()) ? nullptr : nsIt->second;
}

Error ReindexerImpl::AddIndex(std::string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &indexDef](WrSerializer& ser) -> WrSerializer& {
		return ser << "CREATE INDEX " << indexDef.name_ << " ON " << nsName;
	};
	return applyNsFunction<&Namespace::AddIndex>(nsName, ctx, makeCtxStr, indexDef);
}

Error ReindexerImpl::DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, index](WrSerializer& ser) -> WrSerializer& {
		return ser << "DUMP INDEX " << index << " ON " << nsName;
	};
	return applyNsFunction<&Namespace::DumpIndex>(nsName, ctx, makeCtxStr, os, index);
}

Error ReindexerImpl::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName](WrSerializer& ser) -> WrSerializer& { return ser << "SET SCHEMA ON " << nsName; };
	return applyNsFunction<&Namespace::SetSchema>(nsName, ctx, makeCtxStr, schema);
}

Error ReindexerImpl::GetSchema(std::string_view nsName, int format, std::string& schema, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName](WrSerializer& ser) -> WrSerializer& { return ser << "GET SCHEMA ON " << nsName; };
	return applyNsFunction<&Namespace::GetSchema>(nsName, ctx, makeCtxStr, schema, format);
}

Error ReindexerImpl::UpdateIndex(std::string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &indexDef](WrSerializer& ser) -> WrSerializer& {
		return ser << "UPDATE INDEX " << indexDef.name_ << " ON " << nsName;
	};
	return applyNsFunction<&Namespace::UpdateIndex>(nsName, ctx, makeCtxStr, indexDef);
}

Error ReindexerImpl::DropIndex(std::string_view nsName, const IndexDef& indexDef, const InternalRdxContext& ctx) {
	const auto makeCtxStr = [nsName, &indexDef](WrSerializer& ser) -> WrSerializer& {
		return ser << "DROP INDEX " << indexDef.name_ << " ON " << nsName;
	};
	return applyNsFunction<&Namespace::DropIndex>(nsName, ctx, makeCtxStr, indexDef);
}

std::vector<std::pair<std::string, Namespace::Ptr>> ReindexerImpl::getNamespaces(const RdxContext& ctx) {
	SLock lock(mtx_, ctx);
	std::vector<std::pair<std::string, Namespace::Ptr>> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.emplace_back(ns.first, ns.second);
	}
	return ret;
}

std::vector<std::string> ReindexerImpl::getNamespacesNames(const RdxContext& ctx) {
	std::vector<std::string> ret;
	SLock lock(mtx_, ctx);
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.emplace_back();
		reindexer::deepCopy(ret.back(), ns.first);	// Forced copy to avoid races with COW strings on centos7
	}
	return ret;
}

Error ReindexerImpl::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx) {
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
			std::vector<fs::DirEntry> dirs;
			if (fs::ReadDir(storagePath_, dirs) != 0) return Error(errLogic, "Could not read database dir");

			for (auto& d : dirs) {
				if (d.isDir && d.name != "." && d.name != ".." && opts.MatchFilter(d.name)) {
					{
						SLock lock(mtx_, rdxCtx);
						if (namespaces_.find(d.name) != namespaces_.end()) continue;
					}
					std::unique_ptr<NamespaceImpl> tmpNs{new NamespaceImpl(d.name, observers_)};
					try {
						tmpNs->EnableStorage(storagePath_, StorageOpts(), storageType_, rdxCtx);
						defs.push_back(tmpNs->GetDefinition(rdxCtx));
						// NOLINTBEGIN(bugprone-empty-catch)
					} catch (const Error&) {
					}
					// NOLINTEND(bugprone-empty-catch)
				}
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

void ReindexerImpl::backgroundRoutine(net::ev::dynamic_loop& loop) {
	static const RdxContext dummyCtx;
	auto nsBackground = [&]() {
		bgDeleter_.DeleteUnique();
		auto nsarray = getNamespacesNames(dummyCtx);
		for (const auto& name : nsarray) {
			try {
				auto ns = getNamespace(name, dummyCtx);
				ns->BackgroundRoutine(nullptr);
			} catch (Error err) {
				logPrintf(LogWarning, "backgroundRoutine() failed: %s", err.what());
			} catch (...) {
				logPrintf(LogWarning, "backgroundRoutine() failed with ns: %s", name);
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
			auto err = tryLoadReplicatorConfFromFile();
			(void)err;	// ignore
		}
	};

	net::ev::periodic t;
	t.set(loop);
	t.set([&nsBackground](net::ev::timer&, int) noexcept {
		try {
			nsBackground();
		} catch (Error err) {
			logPrintf(LogError, "Unexpected exception in background thread: %s", err.what());
		} catch (std::exception& e) {
			logPrintf(LogError, "Unexpected exception in background thread: %s", e.what());
		} catch (...) {
			logPrintf(LogError, "Unexpected exception in background thread: ???");
		}
	});
	t.start(0.1, 0.1);

	while (!dbDestroyed_.load(std::memory_order_relaxed)) {
		loop.run();
	}
	nsBackground();
}

void ReindexerImpl::storageFlushingRoutine(net::ev::dynamic_loop& loop) {
	static const RdxContext dummyCtx;
	struct ErrorInfo {
		Error lastError;
		uint64_t skipedErrorMsgs = 0;
	};
	std::unordered_map<std::string, ErrorInfo> errors;
	auto nsFlush = [&]() {
		auto nsarray = getNamespacesNames(dummyCtx);
		std::unordered_map<std::string, ErrorInfo> newErrors;
		for (const auto& name : nsarray) {
			try {
				auto ns = getNamespace(name, dummyCtx);
				ns->StorageFlushingRoutine();
			} catch (Error& err) {
				bool printMsg = false;
				auto found = errors.find(name);
				ErrorInfo* errInfo = nullptr;
				if (found != errors.end()) {
					auto bucket = errors.extract(found);
					errInfo = &bucket.mapped();
					newErrors.insert(std::move(bucket));
				} else {
					errInfo = &newErrors[name];
				}
				if (errInfo->lastError != err) {
					printMsg = true;
					errInfo->lastError = std::move(err);
				} else if (++errInfo->skipedErrorMsgs % 1000 == 0) {
					printMsg = true;
				}
				if (printMsg) {
					if (errInfo->skipedErrorMsgs) {
						logPrintf(LogWarning, "storageFlushingRoutine() failed: '%s' (%d successive errors on ns '%s')",
								  errInfo->lastError.what(), errInfo->skipedErrorMsgs + 1, name);
					} else {
						logPrintf(LogWarning, "storageFlushingRoutine() failed: '%s'", errInfo->lastError.what(), name);
					}
				}
			} catch (...) {
				logPrintf(LogWarning, "storageFlushingRoutine() failed with ns: '%s'", name);
			}
		}
		errors = std::move(newErrors);
	};

	net::ev::periodic t;
	t.set(loop);
	t.set([this, &nsFlush](net::ev::timer&, int) noexcept {
		try {
			nsFlush();
			(void)this;
#ifdef REINDEX_WITH_GPERFTOOLS
			this->heapWatcher_.CheckHeapUsagePeriodic();
#endif
		} catch (Error err) {
			logPrintf(LogError, "Unexpected exception in flushing thread: %s", err.what());
		} catch (std::exception& e) {
			logPrintf(LogError, "Unexpected exception in flushing thread: %s", e.what());
		} catch (...) {
			logPrintf(LogError, "Unexpected exception in flushing thread: ???");
		}
	});
	t.start(0.1, 0.1);

	while (!dbDestroyed_.load(std::memory_order_relaxed)) {
		loop.run();
	}
	nsFlush();
}

void ReindexerImpl::createSystemNamespaces() {
	const RdxContext dummyCtx;
	for (const auto& nsDef : kSystemNsDefs) {
		auto err = addNamespace(nsDef, dummyCtx);
		if (!err.ok()) {
			logPrintf(LogWarning, "Unable to create system namespace '%s': %s", nsDef.name, err.what());
		}
	}
}

Error ReindexerImpl::InitSystemNamespaces() {
	createSystemNamespaces();

	QueryResults results;
	auto err = Select(Query(kConfigNamespace), results);
	if (!err.ok()) return err;

	bool hasReplicatorConfig = false;
	if (results.Count() == 0) {
		// Set default config
		for (const auto& conf : kDefDBConfig) {
			if (!hasReplicatorConfig) {
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(std::string_view(conf));
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
			auto item = it.GetItem(false);
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
		auto err = tryLoadReplicatorConfFromFile();
		(void)err;	// ignore
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
		err = item.Unsafe().FromJSON(ser.Slice());
		if (!err.ok()) {
			return err;
		}
		return Upsert(kConfigNamespace, item);
	}
}

void ReindexerImpl::updateToSystemNamespace(std::string_view nsName, Item& item, const RdxContext& ctx) {
	if (item.GetID() != -1 && nsName == kConfigNamespace) {
		try {
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
				std::string_view command = actionNode["command"].As<std::string_view>();
				if (command == "restart_replication"sv) {
					replicator_->Stop();
					needStartReplicator = true;
				}
			}
			if (replicationEnabled_ && needStartReplicator && !dbDestroyed_) {
				if (Error err = replicator_->Start()) throw err;
			}
		} catch (gason::Exception& e) {
			throw Error(errParseJson, "JSON parsing error: %s", e.what());
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
	auto err = replConfigFileChecker_.RewriteFile(std::string(ser.Slice()), [&oldReplConf](const std::string& content) {
		ReplicationConfigData replConf;
		Error err = replConf.FromYML(content);
		return err.ok() && (replConf == oldReplConf);
	});
	if (!err.ok()) {
		throw err;
	}
}

ReindexerImpl::FilterNsNamesT ReindexerImpl::detectFilterNsNames(const Query& q) {
	FilterNsNamesT res;
	struct BracketRange {
		uint32_t begin;
		uint32_t end;
	};

	h_vector<BracketRange, 4> notBrackets;
	const auto& entries = q.Entries();
	for (uint32_t i = 0, sz = entries.Size(); i < sz; ++i) {
		const auto op = entries.GetOperation(i);
		if (op == OpOr) {
			return std::nullopt;
		}
		if (entries.Is<QueryEntry>(i)) {
			auto& qe = entries.Get<QueryEntry>(i);
			if (qe.FieldName() == kNsNameField) {
				if (op == OpNot) {
					return std::nullopt;
				}
				if (std::find_if(notBrackets.begin(), notBrackets.end(),
								 [i](const BracketRange& br) noexcept { return i >= br.begin && i < br.end; }) != notBrackets.end()) {
					return std::nullopt;
				}
				if (qe.Condition() != CondSet && qe.Condition() != CondEq) {
					return std::nullopt;
				}
				if (res.has_value()) {
					return std::nullopt;
				}
				res.emplace();
				res->reserve(qe.Values().size());
				for (auto& v : qe.Values()) {
					if (!v.Type().Is<KeyValueType::String>()) {
						return std::nullopt;
					}
					res->emplace_back(v.As<std::string>());
				}
			}
		} else if (entries.Is<BetweenFieldsQueryEntry>(i)) {
			auto& qe = entries.Get<BetweenFieldsQueryEntry>(i);
			if (qe.LeftFieldName() == kNsNameField || qe.RightFieldName() == kNsNameField) {
				return std::nullopt;
			}
		} else if (op == OpNot && entries.IsSubTree(i)) {
			notBrackets.emplace_back(BracketRange{.begin = i, .end = uint32_t(entries.Size(i))});
		}
	}
	for (auto& jq : q.GetJoinQueries()) {
		if (jq.joinType == OrInnerJoin) {
			return std::nullopt;
		}
	}
	return res;
}

[[nodiscard]] ReindexerImpl::StatsLocker::StatsLockT ReindexerImpl::syncSystemNamespaces(std::string_view sysNsName,
																						 const FilterNsNamesT& filterNsNames,
																						 const RdxContext& ctx) {
	logPrintf(
		LogTrace, "ReindexerImpl::syncSystemNamespaces (%s,%s)", sysNsName,
		filterNsNames.has_value() ? (filterNsNames->size() == 1 ? (*filterNsNames)[0] : std::to_string(filterNsNames->size())) : "<all>");

	StatsLocker::StatsLockT resultLock;

	auto forEachNS = [&](const Namespace::Ptr& sysNs, bool withSystem,
						 const std::function<bool(std::string_view nsName, const Namespace::Ptr&, WrSerializer&)>& filler) {
		const auto nsarray = getNamespaces(ctx);
		WrSerializer ser;
		std::vector<Item> items;
		items.reserve(nsarray.size());
		const auto activityCtx = ctx.OnlyActivity();
		for (auto& nspair : nsarray) {
			if (filterNsNames.has_value()) {
				if (std::find(filterNsNames->cbegin(), filterNsNames->cend(), nspair.first) == filterNsNames->cend()) {
					continue;
				}
			}
			if (isSystemNamespaceNameFast(nspair.first) && !withSystem) continue;
			ser.Reset();
			if (filler(nspair.first, nspair.second, ser)) {
				auto& item = items.emplace_back(sysNs->NewItem(ctx));
				if (!item.Status().ok()) {
					throw item.Status();
				}
				auto err = item.FromJSON(ser.Slice());
				if (!err.ok()) {
					throw err;
				}
			}
		}
		resultLock = statsLocker_.LockIfRequired(sysNsName, ctx);
		sysNs->Refill(items, ctx);
	};

	if (sysNsName == kPerfStatsNamespace) {
		if (configProvider_.PerfStatsEnabled()) {
			forEachNS(getNamespace(kPerfStatsNamespace, ctx), false,
					  [&ctx](std::string_view nsName, const Namespace::Ptr& nsPtr, WrSerializer& ser) {
						  auto stats = nsPtr->GetPerfStat(ctx);
						  bool notRenamed = (stats.name == nsName);
						  if (notRenamed) stats.GetJSON(ser);
						  return notRenamed;
					  });
		}
	} else if (sysNsName == kMemStatsNamespace) {
		if (configProvider_.MemStatsEnabled()) {
			forEachNS(getNamespace(kMemStatsNamespace, ctx), false,
					  [&ctx](std::string_view nsName, const Namespace::Ptr& nsPtr, WrSerializer& ser) {
						  auto stats = nsPtr->GetMemStat(ctx);
						  bool notRenamed = (stats.name == nsName);
						  if (notRenamed) stats.GetJSON(ser);
						  return notRenamed;
					  });
		}
	} else if (sysNsName == kNamespacesNamespace) {
		forEachNS(getNamespace(kNamespacesNamespace, ctx), true,
				  [&ctx](std::string_view nsName, const Namespace::Ptr& nsPtr, WrSerializer& ser) {
					  auto stats = nsPtr->GetDefinition(ctx);
					  bool notRenamed = (stats.name == nsName);
					  if (notRenamed) stats.GetJSON(ser, kIndexJSONWithDescribe);
					  return notRenamed;
				  });
	} else if (sysNsName == kQueriesPerfStatsNamespace) {
		if (configProvider_.QueriesPerfStatsEnabled()) {
			const auto data = queriesStatTracker_.Data();
			WrSerializer ser;
			std::vector<Item> items;
			items.reserve(data.size());
			auto queriesperfstatsNs = getNamespace(kQueriesPerfStatsNamespace, ctx);
			for (const auto& stat : data) {
				ser.Reset();
				stat.GetJSON(ser);
				auto& item = items.emplace_back(queriesperfstatsNs->NewItem(ctx));
				if (!item.Status().ok()) {
					throw item.Status();
				}
				auto err = item.FromJSON(ser.Slice());
				if (!err.ok()) throw err;
			}
			queriesperfstatsNs->Refill(items, ctx);
		}
	} else if (sysNsName == kActivityStatsNamespace) {
		const auto data = activities_.List();
		WrSerializer ser;
		std::vector<Item> items;
		items.reserve(data.size());
		auto activityNs = getNamespace(kActivityStatsNamespace, ctx);
		for (const auto& act : data) {
			ser.Reset();
			act.GetJSON(ser);
			auto& item = items.emplace_back(activityNs->NewItem(ctx));
			if (!item.Status().ok()) {
				throw item.Status();
			}
			auto err = item.FromJSON(ser.Slice());
			if (!err.ok()) throw err;
		}
		activityNs->Refill(items, ctx);
	} else if (sysNsName == kClientsStatsNamespace) {
		if (clientsStats_) {
			std::vector<ClientStat> clientInf;
			WrSerializer ser;
			clientsStats_->GetClientInfo(clientInf);
			auto observers = observers_.Get();
			auto clientsNs = getNamespace(kClientsStatsNamespace, ctx);
			std::vector<Item> items;
			items.reserve(clientInf.size());
			for (auto& i : clientInf) {
				if (auto query = activities_.QueryForIpConnection(i.connectionId); query) {
					i.currentActivity = std::move(*query);
				}
				for (auto obsIt = observers.begin(); obsIt != observers.end(); ++obsIt) {
					if (obsIt->ptr == i.updatesPusher) {
						i.isSubscribed = true;
						i.updatesFilters = std::move(obsIt->filters);
						observers.erase(obsIt);
						break;
					}
				}
				ser.Reset();
				i.GetJSON(ser);
				auto& item = items.emplace_back(clientsNs->NewItem(ctx));
				if (!item.Status().ok()) {
					throw item.Status();
				}
				auto err = item.FromJSON(ser.Slice());
				if (!err.ok()) throw err;
			}
			clientsNs->Refill(items, ctx);
		}
	}
	return resultLock;
}

void ReindexerImpl::onProfiligConfigLoad() {
	QueryResults qr1, qr2, qr3;
	auto err = Delete(Query(kMemStatsNamespace), qr2);
	err = Delete(Query(kQueriesPerfStatsNamespace), qr3);
	err = Delete(Query(kPerfStatsNamespace), qr1);
	(void)err;	// ignore
}

Error ReindexerImpl::SubscribeUpdates(IUpdatesObserver* observer, const UpdatesFilters& filters, SubscriptionOpts opts) {
	observers_.Add(observer, filters, opts);
	return {};
}

Error ReindexerImpl::UnsubscribeUpdates(IUpdatesObserver* observer) { return observers_.Delete(observer); }

Error ReindexerImpl::GetSqlSuggestions(const std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions,
									   const InternalRdxContext& ctx) {
	std::vector<NamespaceDef> nses;

	suggestions = SQLSuggester::GetSuggestions(
		sqlQuery, pos,
		[&, this](EnumNamespacesOpts opts) {
			auto err = EnumNamespaces(nses, opts, ctx);
			(void)err;	// ignore
			return nses;
		},
		[&ctx, this](std::string_view ns) {
			auto rdxCtx = ctx.CreateRdxContext(""sv, activities_);
			auto nsPtr = getNamespaceNoThrow(ns, rdxCtx);
			if (nsPtr) {
				return nsPtr->GetSchemaPtr(rdxCtx);
			}
			return std::shared_ptr<const Schema>();
		});
	return errOK;
}

Error ReindexerImpl::GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces) {
	struct NsInfo {
		std::string nsName, objName;
		int nsNumber;
	};

	std::vector<NsInfo> nses;
	nses.reserve(namespaces.size());
	for (const std::string& ns : namespaces) {
		nses.push_back({ns, std::string(), 0});
	}

	ser << "// Autogenerated by reindexer server - do not edit!\n";
	SchemaFieldsTypes fieldsTypes;
	ProtobufSchemaBuilder schemaBuilder(&ser, &fieldsTypes, ObjType::TypePlain);

	const std::string_view kMessage = "message"sv;

	for (auto& ns : nses) {
		std::string nsProtobufSchema;
		Error status = GetSchema(ns.nsName, ProtobufSchemaType, nsProtobufSchema);
		if (!status.ok()) return status;
		ser << "// Message with document schema from namespace " << ns.nsName << "\n";
		ser << nsProtobufSchema;
		std::string_view objName = nsProtobufSchema;
		auto pos1 = objName.find(kMessage);
		if (pos1 != std::string_view::npos) {
			objName = objName.substr(pos1 + kMessage.length() + 1);
			auto pos2 = objName.find(' ');
			if (pos2 != std::string_view::npos) {
				objName = objName.substr(0, pos2);
			}
		} else {
			objName = ns.nsName;
		}
		ns.objName = std::string(objName);
		QueryResults qr;
		status = Select(Query(ns.nsName).Limit(0), qr);
		if (!status.ok()) return status;
		ns.nsNumber = qr.getNsNumber(0) + 1;
	}

	ser << "// Possible item schema variants in QueryResults or in ModifyResults\n";
	schemaBuilder.Object(0, "ItemsUnion", false, [&](ProtobufSchemaBuilder& obj) {
		ser << "oneof item {\n";
		for (auto& ns : nses) {
			obj.Field(ns.nsName, ns.nsNumber, FieldProps{KeyValueType::Tuple{}, false, false, false, ns.objName});
		}
		ser << "}\n";
	});

	ser << "// The QueryResults message is schema of http API methods response:\n";
	ser << "// - GET api/v1/db/:db/namespaces/:ns/items\n";
	ser << "// - GET/POST api/v1/db/:db/query\n";
	ser << "// - GET/POST api/v1/db/:db/sqlquery\n";
	schemaBuilder.Object(0, "QueryResults", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamItems, kProtoQueryResultsFields.at(kParamItems),
				  FieldProps{KeyValueType::Tuple{}, true, false, false, "ItemsUnion"});
		obj.Field(kParamNamespaces, kProtoQueryResultsFields.at(kParamNamespaces), FieldProps{KeyValueType::String{}, true});
		obj.Field(kParamCacheEnabled, kProtoQueryResultsFields.at(kParamCacheEnabled), FieldProps{KeyValueType::Bool{}});
		obj.Field(kParamExplain, kProtoQueryResultsFields.at(kParamExplain), FieldProps{KeyValueType::String{}});
		obj.Field(kParamTotalItems, kProtoQueryResultsFields.at(kParamTotalItems), FieldProps{KeyValueType::Int{}});
		obj.Field(kParamQueryTotalItems, kProtoQueryResultsFields.at(kParamQueryTotalItems), FieldProps{KeyValueType::Int{}});

		obj.Object(kProtoQueryResultsFields.at(kParamColumns), "Columns", false, [](ProtobufSchemaBuilder& obj) {
			obj.Field(kParamName, kProtoColumnsFields.at(kParamName), FieldProps{KeyValueType::String{}});
			obj.Field(kParamWidthPercents, kProtoColumnsFields.at(kParamWidthPercents), FieldProps{KeyValueType::Double{}});
			obj.Field(kParamMaxChars, kProtoColumnsFields.at(kParamMaxChars), FieldProps{KeyValueType::Int{}});
			obj.Field(kParamWidthChars, kProtoColumnsFields.at(kParamWidthChars), FieldProps{KeyValueType::Int{}});
		});

		obj.Field(kParamColumns, kProtoQueryResultsFields.at(kParamColumns),
				  FieldProps{KeyValueType::Tuple{}, true, false, false, "Columns"});

		AggregationResult::GetProtobufSchema(obj);
		obj.Field(kParamAggregations, kProtoQueryResultsFields.at(kParamAggregations),
				  FieldProps{KeyValueType::Tuple{}, true, false, false, "AggregationResults"});
	});

	ser << "// The ModifyResults message is schema of http API methods response:\n";
	ser << "// - PUT/POST/DELETE api/v1/db/:db/namespaces/:ns/items\n";
	schemaBuilder.Object(0, "ModifyResults", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamItems, kProtoModifyResultsFields.at(kParamItems),
				  FieldProps{KeyValueType::Tuple{}, true, false, false, "ItemsUnion"});
		obj.Field(kParamUpdated, kProtoModifyResultsFields.at(kParamUpdated), FieldProps{KeyValueType::Int{}});
		obj.Field(kParamSuccess, kProtoModifyResultsFields.at(kParamSuccess), FieldProps{KeyValueType::Bool{}});
	});

	ser << "// The ErrorResponse message is schema of http API methods response on error condition \n";
	ser << "// With non 200 http status code\n";
	schemaBuilder.Object(0, "ErrorResponse", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamSuccess, kProtoErrorResultsFields.at(kParamSuccess), FieldProps{KeyValueType::Bool{}});
		obj.Field(kParamResponseCode, kProtoErrorResultsFields.at(kParamResponseCode), FieldProps{KeyValueType::Int{}});
		obj.Field(kParamDescription, kProtoErrorResultsFields.at(kParamDescription), FieldProps{KeyValueType::String{}});
	});
	schemaBuilder.End();
	return errOK;
}

[[nodiscard]] bool ReindexerImpl::isSystemNamespaceNameStrict(std::string_view name) noexcept {
	return std::find_if(kSystemNsDefs.begin(), kSystemNsDefs.end(),
						[name](const NamespaceDef& nsDef) { return iequals(nsDef.name, name); }) != kSystemNsDefs.end();
}

Error ReindexerImpl::Status() {
	if (connected_.load(std::memory_order_acquire)) {
		return errOK;
	}
	return Error(errNotValid, "DB is not connected"sv);
}

}  // namespace reindexer
