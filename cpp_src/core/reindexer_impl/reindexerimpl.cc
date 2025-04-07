#include "reindexerimpl.h"

#include <stdio.h>
#include <chrono>
#include <thread>
#include "cluster/clustercontrolrequest.h"
#include "cluster/clusterizator.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/defnsconfigs.h"
#include "core/iclientsstats.h"
#include "core/index/index.h"
#include "core/nsselecter/querypreprocessor.h"
#include "core/query/sql/sqlsuggester.h"
#include "core/selectfunc/selectfunc.h"
#include "debug/crashqueryreporter.h"
#include "rx_selector.h"
#include "server/outputparameters.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/catch_and_return.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "vendor/gason/gason.h"

#include "debug/backtrace.h"
#include "debug/terminate_handler.h"

static std::once_flag initTerminateHandlerFlag;

using namespace std::placeholders;
using reindexer::updates::UpdateRecord;
using namespace std::string_view_literals;

namespace reindexer {

constexpr char kStoragePlaceholderFilename[] = ".reindexer.storage";
const std::string kReplicationConfFilename = "replication.conf";
const std::string kAsyncReplicationConfFilename = "async_replication.conf";
constexpr char kClusterConfFilename[] = "cluster.conf";
constexpr char kShardingConfFilename[] = "sharding.conf";
constexpr char kReplicationConfigType[] = "replication";
constexpr char kAsyncReplicationConfigType[] = "async_replication";
constexpr char kShardingConfigType[] = "sharding";
constexpr char kActionConfigType[] = "action";

constexpr unsigned kStorageLoadingThreads = 6;

static unsigned ConcurrentNamespaceLoaders() noexcept {
	const auto hwConc = hardware_concurrency();
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

ReindexerImpl::ReindexerImpl(ReindexerConfig cfg, ActivityContainer& activities, CallbackMap&& proxyCallbacks)
	: clusterizator_(std::make_unique<cluster::Clusterizator>(*this, cfg.maxReplUpdatesSize)),
	  nsLock_(*clusterizator_, *this),
	  activities_(activities),
	  storageType_(StorageType::LevelDB),
	  config_(std::move(cfg)),
	  proxyCallbacks_(std::move(proxyCallbacks)),
	  observers_(config_.dbName, *clusterizator_, config_.maxReplUpdatesSize) {
	configProvider_.setHandler(ProfilingConf, std::bind(&ReindexerImpl::onProfiligConfigLoad, this));
	replCfgHandlerID_ =
		configProvider_.setHandler([this](const ReplicationConfigData& cfg) noexcept { observers_.SetEventsServerID(cfg.serverID); });
	shardingConfig_.setHandled([this](const ShardinConfigPtr& cfg) noexcept {
		observers_.SetEventsShardID(cfg ? cfg->thisShardId : ShardingKeyType::NotSetShard);
	});

	configWatchers_.emplace_back(
		kReplicationConfFilename,
		[this](const std::string& yaml) { return tryLoadConfFromYAML<kReplicationConfigType, ReplicationConfigData>(yaml); },
		[this](const std::string& filename) { return tryLoadConfFromFile<kReplicationConfigType, ReplicationConfigData>(filename); });
	configWatchers_.emplace_back(
		kAsyncReplicationConfFilename,
		[this](const std::string& yaml) { return tryLoadConfFromYAML<kAsyncReplicationConfigType, cluster::AsyncReplConfigData>(yaml); },
		[this](const std::string& filename) {
			return tryLoadConfFromFile<kAsyncReplicationConfigType, cluster::AsyncReplConfigData>(filename);
		});
	backgroundThread_.Run([this](net::ev::dynamic_loop& loop) { this->backgroundRoutine(loop); });

#ifdef REINDEX_WITH_GPERFTOOLS
	if (alloc_ext::TCMallocIsAvailable()) {
		heapWatcher_ = TCMallocHeapWathcher(alloc_ext::instance(), config_.allocatorCacheLimit, config_.allocatorCachePart);
	}
#endif

	annCachingThread_.Run([this](net::ev::dynamic_loop& loop) { this->annCachingRoutine(loop); });
	storageFlushingThread_.Run([this](net::ev::dynamic_loop& loop) { this->storageFlushingRoutine(loop); });
	std::call_once(initTerminateHandlerFlag, []() {
		debug::terminate_handler_init();
		debug::backtrace_set_crash_query_reporter(&reindexer::PrintCrashedQuery);
	});
}

ReindexerImpl::~ReindexerImpl() {
	if (replCfgHandlerID_.has_value()) {
		configProvider_.unsetHandler(*replCfgHandlerID_);
	}
	for (auto& ns : namespaces_) {
		// Add extra checks to avoid GCC 13 warnings in Release build. Actually namespaces are never null
		if (ns.second) {
			if (auto mainNs = ns.second->getMainNs(); mainNs) {
				mainNs->SetDestroyFlag();
			}
		}
	}

	dbDestroyed_ = true;
	clusterizator_->Stop();
	backgroundThread_.Stop();
	annCachingThread_.Stop();
	storageFlushingThread_.Stop();
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

Error ReindexerImpl::enableStorage(const std::string& storagePath) {
	if (!storagePath_.empty()) {
		return Error(errParams, "Storage already enabled");
	}

	if (storagePath.empty()) {
		return {};
	}
	if (fs::MkDirAll(storagePath) < 0) {
		return Error(errParams, "Can't create directory '{}' for reindexer storage - reason {}", storagePath, strerror(errno));
	}

	std::vector<fs::DirEntry> dirEntries;
	bool isEmpty = true;
	bool isHaveConfig = false;
	if (fs::ReadDir(storagePath, dirEntries) < 0) {
		return Error(errParams, "Can't read contents of directory '{}' for reindexer storage - reason {}", storagePath, strerror(errno));
	}
	for (auto& entry : dirEntries) {
		if (entry.name != "." && entry.name != ".." && entry.name != kStoragePlaceholderFilename) {
			isEmpty = false;
		}
		if (entry.name == kConfigNamespace) {
			isHaveConfig = true;
		}
	}

	if (!isEmpty) {
		std::string content;
		int res = fs::ReadFile(fs::JoinPath(storagePath, kStoragePlaceholderFilename), content);
		if (res >= 0) {
			auto currentStorageType = StorageType::LevelDB;
			try {
				currentStorageType = reindexer::datastorage::StorageTypeFromString(content);
			} catch (const Error&) {
				return Error(errParams,
							 "Cowardly refusing to use directory '{}' - it's not empty and contains reindexer placeholder with unexpected "
							 "content: \"{}\"",
							 storagePath, content);
			}
			if (storageType_ != currentStorageType) {
				logFmt(LogWarning, "Placeholder content doesn't correspond to chosen storage type. Forcing \"{}\"", content);
				storageType_ = currentStorageType;
			}
		} else {
			return Error(errParams, "Cowardly refusing to use directory '{}' - it's not empty and doesn't contain reindexer placeholder",
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
				return Error(errParams, "Can't create placeholder in directory '{}' for reindexer storage - reason {}", storagePath,
							 strerror(errnoSv));
			}

		} else {
			return Error(errParams, "Can't create placeholder in directory '{}' for reindexer storage - reason {}", storagePath,
						 strerror(errno));
		}
	}

	storagePath_ = storagePath;

	Error res;
	if (isHaveConfig) {
		res = openNamespace(kConfigNamespace, IsDBInitCall_True, StorageOpts().Enabled().CreateIfMissing(), {}, RdxContext());
	}
	for (auto& watcher : configWatchers_) {
		watcher.SetDirectory(storagePath_);
	}
	auto err = readClusterConfigFile();
	if (!err.ok() && res.ok()) {
		res = Error(err.code(), "Failed to read cluster config file: '{}'", err.what());
	}
	err = readShardingConfigFile();
	if (!err.ok() && res.ok()) {
		res = Error(err.code(), "Failed to read sharding config file: '{}'", err.what());
	}

	return res;
}

Error ReindexerImpl::Connect(const std::string& dsn, ConnectOpts opts) {
	auto checkReplConf = [this](const ConnectOpts& opts) {
		if (opts.HasExpectedClusterID()) {
			auto replConfig = configProvider_.GetReplicationConfig();
			if (replConfig.clusterID != opts.ExpectedClusterID()) {
				return Error(errReplParams, "Expected leader's cluster ID({}) is not equal to actual cluster ID({})",
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

	switch (opts.StorageType()) {
		case kStorageTypeOptLevelDB:
			storageType_ = StorageType::LevelDB;
			break;
		case kStorageTypeOptRocksDB:
			storageType_ = StorageType::RocksDB;
			break;
	}

	replicationEnabled_ = !opts.IsReplicationDisabled();

	std::vector<reindexer::fs::DirEntry> foundNs;
	const bool storageEnable = (path.length() > 0 && path != "/");
	if (storageEnable) {
		auto err = enableStorage(path);
		if (!err.ok()) {
			return err;
		}
		if (fs::ReadDir(path, foundNs) < 0) {
			return Error(errParams, "Can't read database dir {}", path);
		}
	}

	Error err = InitSystemNamespaces();
	if (!err.ok()) {
		return err;
	}

	if (storageEnable && opts.IsOpenNamespaces()) {
		boost::sort::pdqsort_branchless(foundNs.begin(), foundNs.end(), [](const fs::DirEntry& ld, const fs::DirEntry& rd) noexcept {
			return ld.internalFilesCount > rd.internalFilesCount;
		});
		const size_t maxLoadWorkers = ConcurrentNamespaceLoaders();
		std::unique_ptr<std::thread[]> thrs(new std::thread[maxLoadWorkers]);
		std::atomic_flag hasNsErrors = ATOMIC_FLAG_INIT;
		std::atomic<unsigned> nsIdx = {0};
		std::atomic<int64_t> maxNsVersion = -1;
		for (size_t i = 0; i < maxLoadWorkers; i++) {
			thrs[i] = std::thread([&] {
				for (unsigned idx = nsIdx.fetch_add(1, std::memory_order_relaxed); idx < foundNs.size();
					 idx = nsIdx.fetch_add(1, std::memory_order_relaxed)) {
					auto& de = foundNs[idx];
					if (de.isDir && validateObjectName(de.name, true)) {
						if (de.name[0] == kTmpNsPrefix) {
							const std::string tmpPath = fs::JoinPath(storagePath_, de.name);
							logFmt(LogWarning, "Dropping tmp namespace '{}'", de.name);
							if (fs::RmDirAll(tmpPath) < 0) {
								logFmt(LogWarning, "Failed to remove '{}' temporary namespace from filesystem, path: {}", de.name, tmpPath);
								hasNsErrors.test_and_set(std::memory_order_relaxed);
							}
							continue;
						}
						RdxContext dummyCtx;
						auto status = openNamespace(de.name, IsDBInitCall_True, StorageOpts().Enabled(), {}, dummyCtx);
						if (status.ok()) {
							auto ns = getNamespace(de.name, dummyCtx);
							auto replState = ns->GetReplStateV2(dummyCtx);
							int64_t maxVer = maxNsVersion.load();
							int64_t ver = replState.nsVersion.Counter();
							do {
								if (replState.nsVersion.isEmpty() || ver <= maxVer) {
									break;
								}
							} while (!maxNsVersion.compare_exchange_strong(maxVer, ver));
						} else {
							logFmt(LogError, "Failed to open namespace '{}' - {}", de.name, status.what());
							hasNsErrors.test_and_set(std::memory_order_relaxed);
						}
					}
				}
			});
		}
		for (size_t i = 0; i < maxLoadWorkers; ++i) {
			thrs[i].join();
		}
		nsVersion_.UpdateCounter(maxNsVersion);

		if (!opts.IsAllowNamespaceErrors() && hasNsErrors.test_and_set(std::memory_order_relaxed)) {
			return Error(errNotValid, "Namespaces load error");
		}

		RdxContext dummyCtx;
		auto nss = getNamespaces(dummyCtx);
		for (auto& ns : nss) {
			const auto replState = ns.second->GetReplState(dummyCtx);
			if (replState.nsVersion.isEmpty()) {
				// Do not set version for the v3 follower namespaces to guarantee force sync for them
				if (!replState.wasV3ReplicatedNS) {
					const auto nsVersion = nsVersion_.GetNext();
					logFmt(LogTrace, "{}: Ns version was empty. Generating new one: {}", ns.first, nsVersion);
					ns.second->SetNsVersion(nsVersion, dummyCtx);
				} else {
					logFmt(LogTrace, "{}: Ns version was empty (namespaces was replicated in v3). Keeping empty value", ns.first);
				}
			}
		}

		logFmt(LogTrace, "{}: All of the namespaces were opened", storagePath_);
	}

	if (replicationEnabled_) {
		err = checkReplConf(opts);
		if (!err.ok()) {
			return err;
		}

		const auto replConfig = configProvider_.GetReplicationConfig();
		clusterizator_->Enable();
		clusterizator_->Configure(replConfig);

		clusterizator_->Configure(configProvider_.GetAsyncReplicationConfig());
		err = clusterizator_->IsExpectingAsyncReplStartup() ? clusterizator_->StartAsyncRepl() : errOK;
		if (!err.ok()) {
			return err;
		}

		if (!storagePath_.empty()) {
			for (auto& watcher : configWatchers_) {
				err = watcher.Enable();
				if (!err.ok()) {
					return err;
				}
			}
		}
		auto clusterConfig = clusterConfig_.get();
		if (clusterConfig) {
			clusterizator_->Configure(*clusterConfig);
			if (clusterizator_->IsExpectingClusterStartup()) {
				logFmt(LogTrace, "{}: Clusterizator was started after connect", storagePath_);
				err = clusterizator_->StartClusterRepl();
			}
			if (!err.ok()) {
				return err;
			}
		}
	}

	connected_.store(err.ok(), std::memory_order_release);
	return err;
}

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef, std::optional<NsReplicationOpts> replOpts, const RdxContext& rdxCtx) {
	NsCreationLockerT::Locks nsCreationLock;  // In case of error this lock should be destroyed after Namespace pointer
	Namespace::Ptr ns;
	try {
		{
			auto rlck = nsLock_.RLock(rdxCtx);
			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '{}' already exists", nsDef.name);
			}
		}
		if (!validateObjectName(nsDef.name, nsDef.isTemporary)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		assertrx(clusterizator_);
		ns = std::make_shared<Namespace>(nsDef.name, replOpts.has_value() ? replOpts->tmStateToken : std::optional<int32_t>(),
										 *clusterizator_, bgDeleter_, observers_);
		if (nsDef.isTemporary) {
			ns->awaitMainNs(rdxCtx)->setTemporary();
		}

		rdxCtx.WithNoWaitSync(nsDef.isTemporary || ns->IsSystem(rdxCtx) || !clusterizator_->NamespaceIsInClusterConfig(nsDef.name));
		nsCreationLock = nsLock_.CreationLock(nsDef.name, rdxCtx);
		if (nsDef.storage.IsEnabled() && !storagePath_.empty()) {
			{
				auto rlck = nsLock_.RLock(rdxCtx);
				if (namespaces_.find(nsDef.name) != namespaces_.end()) {
					return Error(errParams, "Namespace '{}' already exists", nsDef.name);
				}
			}
			ns->EnableStorage(storagePath_, nsDef.storage, storageType_, rdxCtx);
			ns->OnConfigUpdated(configProvider_, rdxCtx);
			ns->LoadFromStorage(kStorageLoadingThreads, rdxCtx);
		} else {
			ns->OnConfigUpdated(configProvider_, rdxCtx);
		}

		if (!rdxCtx.GetOriginLSN().isEmpty()) {
			// TODO: It may be a simple replica
			auto err = ns->SetClusterizationStatus(
				ClusterizationStatus{rdxCtx.GetOriginLSN().Server(), ClusterizationStatus::Role::ClusterReplica}, rdxCtx);
			if (!err.ok()) {
				return err;
			}
		}
		const int64_t stateToken = ns->NewItem(rdxCtx).GetStateToken();
		{
			auto wlck = nsLock_.DataWLock(rdxCtx);

			checkClusterRole(nsDef.name, rdxCtx.GetOriginLSN());

			auto [it, inserted] = namespaces_.insert({nsDef.name, ns});
			if (!inserted) {
				return Error(errParams, "Namespace '{}' already exists", nsDef.name);
			}
			// Unlock, when all storage and namespace map operations are done
			nsCreationLock.UnlockIfOwns();

			const lsn_t version = setNsVersion(ns, replOpts, rdxCtx);
			if (!nsDef.isTemporary) {
				NamespaceDef def;
				def.name = nsDef.name;
				def.storage = nsDef.storage;  // Indexes and schema will be replicate later
#ifdef REINDEX_WITH_V3_FOLLOWERS
				observers_.OnWALUpdate(LSNPair(), nsDef.name, WALRecord(WalNamespaceAdd));
#endif	// REINDEX_WITH_V3_FOLLOWERS
				auto err = observers_.SendUpdate(
					{updates::URType::AddNamespace, it.value()->GetName(RdxContext()), version, rdxCtx.EmmiterServerId(), std::move(def),
					 stateToken},
					[&wlck] {
						assertrx(wlck.isClusterLck());
						wlck.unlock();
					},
					rdxCtx);
				if (!err.ok()) {
					return err;
				}
			}
		}
		try {
			rdxCtx.WithNoWaitSync(nsDef.isTemporary || ns->IsSystem(rdxCtx));
			for (auto& indexDef : nsDef.indexes) {
				ns->AddIndex(indexDef, rdxCtx);
			}
			if (nsDef.HasSchema()) {
				ns->SetSchema(nsDef.schemaJson, rdxCtx);
			}
		} catch (const Error& err) {
			if (rdxCtx.GetOriginLSN().isEmpty() && err.code() == errWrongReplicationData) {
				auto replState = ns->GetReplStateV2(rdxCtx);
				if (replState.clusterStatus.role == ClusterizationStatus::Role::SimpleReplica ||
					replState.clusterStatus.role == ClusterizationStatus::Role::ClusterReplica) {
					return Error();	 // In this case we have leader, who concurrently creates indexes and so on
				}
			}
			return err;
		}
	} catch (std::exception& err) {
		return err;
	}

	return {};
}

Error ReindexerImpl::OpenNamespace(std::string_view name, const StorageOpts& storageOpts, const NsReplicationOpts& replOpts,
								   const RdxContext& ctx) {
	return openNamespace(name, IsDBInitCall_False, storageOpts, {replOpts}, ctx);
}

Error ReindexerImpl::DropNamespace(std::string_view nsName, const RdxContext& rdxCtx) {
	rdxCtx.WithNoWaitSync(!clusterizator_->NamespaceIsInClusterConfig(nsName));
	return closeNamespace(nsName, rdxCtx, true);
}

std::string ReindexerImpl::generateTemporaryNamespaceName(std::string_view baseName) {
	return '@' + std::string(baseName) + "_tmp_" + randStringAlph(kTmpNsPostfixLen);
}

Error ReindexerImpl::CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts, lsn_t nsVersion,
											  const RdxContext& rdxCtx) {
	NamespaceDef tmpNsDef;
	tmpNsDef.isTemporary = true;
	tmpNsDef.storage = opts;
	tmpNsDef.storage.CreateIfMissing();
	if (resultName.empty()) {
		tmpNsDef.name = generateTemporaryNamespaceName(baseName);
		resultName = tmpNsDef.name;
	} else {
		tmpNsDef.name = resultName;
	}
	return AddNamespace(tmpNsDef, {NsReplicationOpts{{}, nsVersion}}, rdxCtx);
}

Error ReindexerImpl::CloseNamespace(std::string_view nsName, const RdxContext& rdxCtx) {
	rdxCtx.WithNoWaitSync(!clusterizator_->NamespaceIsInClusterConfig(nsName));
	return closeNamespace(nsName, rdxCtx, false);
}

Error ReindexerImpl::closeNamespace(std::string_view nsName, const RdxContext& ctx, bool dropStorage) {
	Namespace::Ptr ns;
	Error err;
	try {
		auto nsCreationLock = nsLock_.CreationLock(nsName, ctx);
		auto wlck = nsLock_.DataWLock(ctx);

		auto nsIt = namespaces_.find(nsName);
		if (nsIt == namespaces_.end()) {
			return Error(errNotFound, "Namespace '{}' does not exist", nsName);
		}
		if (isSystemNamespaceNameStrict(nsName)) {
			return Error(errLogic, "Can't delete system ns '{}'", nsName);
		}
		// Temporary save namespace. This will call destructor without lock
		ns = nsIt->second;
		auto replState = ns->GetReplState(ctx);
		const bool isTemporary = replState.temporary;

		if (!isTemporary) {
			checkClusterRole(nsName, ctx.GetOriginLSN());
		}

		if (dropStorage) {
			ns->DeleteStorage(ctx);
		} else {
			if (clusterizator_->NamespaceIsInClusterConfig(nsName)) {
				return Error(errLogic, "Unable to close cluster namespace without storage drop");
			}
			ns->CloseStorage(ctx);
		}

		namespaces_.erase(nsIt);

		// Unlock, when all storage and namespace map operations are done
		nsCreationLock.UnlockIfOwns();

		if (!isTemporary) {
#ifdef REINDEX_WITH_V3_FOLLOWERS
			if (dropStorage) {
				observers_.OnWALUpdate(LSNPair(), nsName, WALRecord(WalNamespaceDrop));
			}
#endif	// REINDEX_WITH_V3_FOLLOWERS
			err = observers_.SendUpdate(
				{dropStorage ? updates::URType::DropNamespace : updates::URType::CloseNamespace, ns->GetName(RdxContext()), lsn_t(0, 0),
				 lsn_t(0, 0), ctx.EmmiterServerId()},
				[&wlck] {
					assertrx(wlck.isClusterLck());
					wlck.unlock();
				},
				ctx);
			if (!err.ok()) {
				return err;
			}
		}
	} catch (std::exception& e) {
		err = std::move(e);
	}
	if (ns) {
		bgDeleter_.Add(ns->atomicLoadMainNs());
	}
	return err;
}

Error ReindexerImpl::openNamespace(std::string_view name, IsDBInitCall isDBInitCall, const StorageOpts& storageOpts,
								   std::optional<NsReplicationOpts> replOpts, const RdxContext& rdxCtx) {
	NsCreationLockerT::Locks nsCreationLock;  // In case of error this lock should be destroyed after Namespace pointer
	auto awaitReplication = [this, &rdxCtx, &nsCreationLock](const Namespace::Ptr& ns, auto&& lock) -> Error {
		lock.unlock();
		nsCreationLock.UnlockIfOwns();
		if (rdxCtx.HasEmmiterServer()) {
			return observers_.SendUpdate(UpdateRecord{updates::URType::EmptyUpdate, ns->GetName(rdxCtx), rdxCtx.EmmiterServerId()},
										 std::function<void()>(), rdxCtx);
		}
		return {};
	};

	try {
		{
			auto rlck = nsLock_.RLock(rdxCtx);
			if (auto nsIt = namespaces_.find(name); nsIt != namespaces_.end() && nsIt->second) {
				return awaitReplication(nsIt->second, std::move(rlck));
			}
		}
		if (!isDBInitCall && !validateUserNsName(name)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		NamespaceDef nsDef(std::string(name), storageOpts);
		assertrx(clusterizator_);
		auto ns = std::make_shared<Namespace>(nsDef.name, replOpts.has_value() ? replOpts->tmStateToken : std::optional<int32_t>(),
											  *clusterizator_, bgDeleter_, observers_);

		rdxCtx.WithNoWaitSync(ns->IsSystem(rdxCtx) || !clusterizator_->NamespaceIsInClusterConfig(nsDef.name));
		// Do not use this lock on database initialization
		nsCreationLock = isDBInitCall ? NsCreationLockerT::Locks() : nsLock_.CreationLock(name, rdxCtx);
		if (storageOpts.IsEnabled() && !storagePath_.empty()) {
			{
				auto rlck = nsLock_.RLock(rdxCtx);
				if (auto nsIt = namespaces_.find(name); nsIt != namespaces_.end() && nsIt->second) {
					return awaitReplication(nsIt->second, std::move(rlck));
				}
			}
			ns->EnableStorage(storagePath_, storageOpts, storageType_, rdxCtx);
			ns->OnConfigUpdated(configProvider_, rdxCtx);
			ns->LoadFromStorage(kStorageLoadingThreads, rdxCtx);
		} else {
			ns->OnConfigUpdated(configProvider_, rdxCtx);
		}
		if (!rdxCtx.GetOriginLSN().isEmpty()) {
			ClusterizationStatus clStatus;
			clStatus.role = ClusterizationStatus::Role::ClusterReplica;	 // TODO: It may be a simple replica
			clStatus.leaderId = rdxCtx.GetOriginLSN().Server();
			auto err = ns->SetClusterizationStatus(std::move(clStatus), rdxCtx);
			if (!err.ok()) {
				return err;
			}
		}
		const int64_t stateToken = ns->NewItem(rdxCtx).GetStateToken();
		{
			auto wlck = nsLock_.DataWLock(rdxCtx);

			if (auto nsIt = namespaces_.find(name); nsIt != namespaces_.end() && nsIt->second) {
				return awaitReplication(nsIt->second, std::move(wlck));
			}

			checkClusterRole(name, rdxCtx.GetOriginLSN());

			const lsn_t version = setNsVersion(ns, replOpts, rdxCtx);

			auto [nsIt, inserted] = namespaces_.insert({nsDef.name, std::move(ns)});
			(void)inserted;
			// Unlock, when all storage and namespace map operations are done
			nsCreationLock.UnlockIfOwns();
#ifdef REINDEX_WITH_V3_FOLLOWERS
			if (!nsDef.isTemporary) {
				observers_.OnWALUpdate(LSNPair(), nsDef.name, WALRecord(WalNamespaceAdd));
			}
#endif	// REINDEX_WITH_V3_FOLLOWERS
			auto err = observers_.SendUpdate(
				{updates::URType::AddNamespace, nsIt.value()->GetName(RdxContext()), version, rdxCtx.EmmiterServerId(), std::move(nsDef),
				 stateToken},
				[&wlck] {
					assertrx(wlck.isClusterLck());
					wlck.unlock();
				},
				rdxCtx);
			if (!err.ok()) {
				throw err;
			}
		}
	} catch (std::exception& err) {
		return err;
	}

	return {};
}

Error ReindexerImpl::TruncateNamespace(std::string_view nsName, const RdxContext& rdxCtx) {
	Error err;
	try {
		auto ns = getNamespace(nsName, rdxCtx);
		ns->Truncate(rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (rdxCtx.Compl()) {
		rdxCtx.Compl()(err);
	}
	return err;
}

Error ReindexerImpl::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const RdxContext& rdxCtx) {
	try {
		{
			auto rlck = nsLock_.RLock(rdxCtx);
			auto srcIt = namespaces_.find(srcNsName);
			if (srcIt == namespaces_.end()) {
				return Error(errParams, "Namespace '{}' doesn't exist", srcNsName);
			}
			Namespace::Ptr srcNs = srcIt->second;
			assertrx(srcNs != nullptr);

			if (srcNs->IsTemporary(rdxCtx) && rdxCtx.GetOriginLSN().isEmpty()) {
				return Error(errParams, "Can't rename temporary namespace '{}'", srcNsName);
			}
		}
		return renameNamespace(srcNsName, dstNsName, !rdxCtx.GetOriginLSN().isEmpty(), false, rdxCtx);
	} catch (std::exception& e) {
		return e;
	}
}

Error ReindexerImpl::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::SetTagsMatcher>(nsName, ctx, std::move(tm));
}

void ReindexerImpl::ShutdownCluster() { clusterizator_->Stop(true); }

bool ReindexerImpl::NamespaceIsInClusterConfig(std::string_view nsName) {
	return clusterizator_ && clusterizator_->NamespaceIsInClusterConfig(nsName);
}

Error ReindexerImpl::SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg) {
	return observers_.AddOrUpdate(observer, std::move(cfg));
}

Error ReindexerImpl::UnsubscribeUpdates(IEventsObserver& observer) { return observers_.Remove(observer); }

Error ReindexerImpl::SubscribeUpdates([[maybe_unused]] IUpdatesObserverV3* observer, [[maybe_unused]] const UpdatesFilters& filters,
									  [[maybe_unused]] SubscriptionOpts opts) {
#ifdef REINDEX_WITH_V3_FOLLOWERS
	observers_.Add(observer, filters, opts);
	return {};
#else	// REINDEX_WITH_V3_FOLLOWERS
	return Error(errForbidden, "Reindexer was built without v3 followers compatibility");
#endif	// REINDEX_WITH_V3_FOLLOWERS
}

Error ReindexerImpl::UnsubscribeUpdates([[maybe_unused]] IUpdatesObserverV3* observer) {
#ifdef REINDEX_WITH_V3_FOLLOWERS
	return observers_.Remove(observer);
#else	// REINDEX_WITH_V3_FOLLOWERS
	return Error(errForbidden, "Reindexer was built without v3 followers compatibility");
#endif	// REINDEX_WITH_V3_FOLLOWERS
}

Error ReindexerImpl::renameNamespace(std::string_view srcNsName, const std::string& dstNsName, bool fromReplication, bool skipResync,
									 const RdxContext& rdxCtx) {
	Namespace::Ptr dstNs, srcNs;
	try {
		if (std::string_view(dstNsName) == srcNsName) {
			return {};
		}
		if (isSystemNamespaceNameStrict(srcNsName)) {
			return Error(errParams, "Can't rename system namespace ({})", srcNsName);
		}
		if (dstNsName.empty()) {
			return Error(errParams, "Can't rename namespace to empty name");
		}
		if (!validateUserNsName(dstNsName)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed ({})", dstNsName);
		}

		{
			// Perform namespace flushes to minimize chances of the flush under lock
			auto rlock = nsLock_.RLock(rdxCtx);
			auto srcIt = namespaces_.find(srcNsName);
			srcNs = (srcIt != namespaces_.end()) ? srcIt->second : Namespace::Ptr();
			auto dstIt = namespaces_.find(dstNsName);
			dstNs = (dstIt != namespaces_.end()) ? dstIt->second : Namespace::Ptr();
			rlock.unlock();

			auto flushNs = [&rdxCtx, &srcNsName, &dstNsName](Namespace::Ptr& targetNs, std::string_view targetNsName) {
				if (targetNs) {
					auto nsPtr = targetNs->awaitMainNs(rdxCtx);
					logFmt(LogInfo, "[rename] Performing '{}' storage flush before namespace renaming ('{}' -> '{}')...", targetNsName,
						   srcNsName, dstNsName);
					auto err = nsPtr->FlushStorage(rdxCtx);
					logFmt(LogInfo, "[rename] Flush done for '{}'", targetNsName);
					if (!err.ok()) {
						throw Error(err.code(), "Unable to flush storage before rename: {}", err.what());
					}
					targetNs.reset();
				}
			};
			flushNs(srcNs, srcNsName);
			flushNs(dstNs, dstNsName);
		}

		rdxCtx.WithNoWaitSync(fromReplication);

		auto nsCreationLock = nsLock_.CreationLock(srcNsName, dstNsName, rdxCtx);
		auto wlck = nsLock_.DataWLock(rdxCtx);

		checkClusterRole(srcNsName, rdxCtx.GetOriginLSN());

		auto srcIt = namespaces_.find(srcNsName);
		if (srcIt == namespaces_.end()) {
			return Error(errParams, "Namespace '{}' doesn't exist", srcNsName);
		}
		srcNs = srcIt->second;
		assertrx(srcNs != nullptr);

		auto replState = srcNs->GetReplState(rdxCtx);

		if (!replState.temporary &&
			(clusterizator_->NamesapceIsInReplicationConfig(srcNsName) || clusterizator_->NamesapceIsInReplicationConfig(dstNsName))) {
			return Error(errParams, "Unable to rename namespace: rename replication is not supported");
		}

		auto dstIt = namespaces_.find(dstNsName);
		auto needWalUpdate = !replState.temporary;
		if (dstIt != namespaces_.end()) {
			dstNs = dstIt->second;
			assertrx(dstNs != nullptr);
		}
		namespaces_.erase(srcIt);
		namespaces_[dstNsName] = srcNs;
		std::function<void(std::function<void()>)> replicateCb;
		if (needWalUpdate) {
			replicateCb = [this, &srcNsName, &dstNsName, &rdxCtx /*, &wlck*/](const std::function<void()>& /*unlockCb*/) {
				// TODO: Implement rename replication
				//					auto err = clusterizator_->Replicate(
				//						{UpdateRecord::Type::RenameNamespace, std::string(srcNsName), lsn_t(0, 0),
				// rdxCtx.emmiterServerId_,
				// dstNsName},
				//						[&lock, &unlockCb] {
				//							unlockCb();
				//							lock.unlock();
				//						},
				//						rdxCtx);
				//					if (!err.ok()) {
				//						throw err;
				//					}

				// Temporary solution. Don't replicate rename command, but still sending rename event
				observers_.SendAsyncEventOnly({updates::URType::RenameNamespace, NamespaceName(srcNsName), lsn_t(0, 0), lsn_t(0, 0),
											   rdxCtx.EmmiterServerId(), dstNsName});
			};
		} else if (!skipResync) {
			replicateCb = [this, &dstNsName, &rdxCtx](const std::function<void()>&) {
				auto err = observers_.SendAsyncUpdate(
					{updates::URType::ResyncNamespaceGeneric, NamespaceName(dstNsName), lsn_t(0, 0), lsn_t(0, 0), rdxCtx.EmmiterServerId()},
					rdxCtx);
				if (!err.ok()) {
					throw err;
				}
			};
		}
		try {
			// unlocking namespace's map for concurrent reading/writing
			wlck.unlock();

			if (dstNs) {
				srcNs->Rename(dstNs, storagePath_, replicateCb, rdxCtx);
			} else {
				srcNs->Rename(dstNsName, storagePath_, replicateCb, rdxCtx);
			}
#ifdef REINDEX_WITH_V3_FOLLOWERS
			if (needWalUpdate) {
				observers_.OnWALUpdate(LSNPair(), srcNsName, WALRecord(WalNamespaceRename, dstNsName));
			} else if (!skipResync) {
				WrSerializer ser;
				auto nsDef = srcNs->GetDefinition(rdxCtx);
				nsDef.GetJSON(ser);
				ser.PutBool(true);
				observers_.OnWALUpdate(LSNPair(), dstNsName, WALRecord(WalForceSync, ser.Slice()));
			}
#endif	// REINDEX_WITH_V3_FOLLOWERS
		} catch (...) {
			if (auto actualName = srcNs->GetName(RdxContext()); !iequals(actualName, dstNsName)) {
				// Can not cancel here
				wlck.lock(ignore_cancel_ctx{});

				namespaces_.erase(dstNsName);
				namespaces_[std::string(actualName)] = srcNs;
			}
			throw;
		}
	} catch (std::exception& err) {
		return err;
	}
	return {};
}

Error ReindexerImpl::readClusterConfigFile() {
	auto path = fs::JoinPath(storagePath_, kClusterConfFilename);
	std::string content;
	auto res = fs::ReadFile(path, content);
	if (res < 0) {
		return Error();
	}
	cluster::ClusterConfigData conf;
	Error err = conf.FromYAML(content);
	if (err.ok()) {
		std::unique_ptr<cluster::ClusterConfigData> confPtr(new cluster::ClusterConfigData(std::move(conf)));
		if (clusterConfig_.compare_exchange_strong(nullptr, confPtr.get())) {
			confPtr.release();	// NOLINT(bugprone-unused-return-value) Moved to clusterConfig_ ptr
			clusterizator_->Configure(*clusterConfig_);
		}
	} else {
		logFmt(LogError, "Error parsing cluster config YML: {}", err.what());
	}
	return err;
}

Error ReindexerImpl::readShardingConfigFile() {
	auto path = fs::JoinPath(storagePath_, kShardingConfFilename);
	std::string content;
	auto res = fs::ReadFile(path, content);
	if (res < 0) {
		return Error();
	}
	cluster::ShardingConfig conf;
	Error err = conf.FromYAML(content);
	if (err.ok()) {
		shardingConfig_.Set(std::move(conf));
	} else {
		logFmt(LogError, "Error parsing sharding config YML: {}", err.what());
	}
	return err;
}

void ReindexerImpl::saveNewShardingConfigFile(const cluster::ShardingConfig& config) const {
	auto path = fs::JoinPath(storagePath_, kShardingConfFilename);

	auto res = fs::WriteFile(path, config.GetYAML());
	if (res < 0) {
		throw Error(errParams, "Error during saving sharding config candidate file. Returned with code: {}", res);
	}
}

void ReindexerImpl::checkClusterRole(std::string_view nsName, lsn_t originLsn) const {
	if (!clusterizator_->NamespaceIsInClusterConfig(nsName)) {
		return;
	}

	switch (clusterStatus_.role) {
		case ClusterizationStatus::Role::None:
			if (!originLsn.isEmpty()) {
				throw Error(errWrongReplicationData, "Can't modify database with 'None' replication status from node {}",
							originLsn.Server());
			}
			break;
		case ClusterizationStatus::Role::SimpleReplica:
			assertrx(false);  // This role is unavailable for database
			break;
		case ClusterizationStatus::Role::ClusterReplica:
			if (originLsn.isEmpty() || originLsn.Server() != clusterStatus_.leaderId) {
				throw Error(errWrongReplicationData, "Can't modify cluster database replica with incorrect origin LSN: ({}) (s1:{} s2:{})",
							originLsn, originLsn.Server(), clusterStatus_.leaderId);
			}
			break;
	}
}

void ReindexerImpl::setClusterizationStatus(ClusterizationStatus&& status, const RdxContext& ctx) {
	auto wlck = nsLock_.SimpleWLock(ctx);
	clusterStatus_ = std::move(status);
}

template <bool needUpdateSystemNs, typename MemFnType, MemFnType Namespace::* MemFn, typename Arg, typename... Args>
Error ReindexerImpl::applyNsFunction(std::string_view nsName, const RdxContext& rdxCtx, Arg arg, Args&&... args) {
	Error err;
	try {
		auto ns = getNamespace(nsName, rdxCtx);
		(*ns.*MemFn)(arg, std::forward<Args>(args)..., rdxCtx);
		if constexpr (needUpdateSystemNs) {
			RdxContext rdxCtxNoCancel = rdxCtx.NoCancel();
			rdxCtxNoCancel.WithNoWaitSync(true);
			updateToSystemNamespace(nsName, arg, rdxCtxNoCancel);
		}
	} catch (std::exception& e) {
		err = std::move(e);
	}
	if (rdxCtx.Compl()) {
		rdxCtx.Compl()(err);
	}
	return err;
}

template <typename>
struct IsVoidReturn;

template <typename R, typename... Args>
struct IsVoidReturn<R (Namespace::*)(Args...)> : public std::false_type {};

template <typename... Args>
struct IsVoidReturn<void (Namespace::*)(Args...)> : public std::true_type {};

template <auto MemFn, typename Arg, typename... Args>
Error ReindexerImpl::applyNsFunction(std::string_view nsName, const RdxContext& rdxCtx, Arg&& arg, Args&&... args) {
	Error err;
	try {
		auto ns = getNamespace(nsName, rdxCtx);
		if constexpr (IsVoidReturn<decltype(MemFn)>::value) {
			(*ns.*MemFn)(std::forward<Arg>(arg), std::forward<Args>(args)..., rdxCtx);
		} else {
			arg = (*ns.*MemFn)(std::forward<Args>(args)..., rdxCtx);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (rdxCtx.Compl()) {
		rdxCtx.Compl()(err);
	}
	return err;
}

#define APPLY_NS_FUNCTION1(needUpdateSys, memFn, arg) \
	return applyNsFunction<needUpdateSys, void(decltype(arg), const RdxContext&), &Namespace::memFn, decltype(arg)>(nsName, ctx, arg)

#define APPLY_NS_FUNCTION2(needUpdateSys, memFn, arg1, arg2)                                                                          \
	return applyNsFunction<needUpdateSys, void(decltype(arg1), decltype(arg2), const RdxContext&), &Namespace::memFn, decltype(arg1), \
						   decltype(arg2)>(nsName, ctx, arg1, arg2)

Error ReindexerImpl::Insert(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(true, Insert, item); }

Error ReindexerImpl::insertDontUpdateSystemNS(std::string_view nsName, Item& item, const RdxContext& ctx) {
	APPLY_NS_FUNCTION1(false, Insert, item);
}

Error ReindexerImpl::Insert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(true, Insert, item, qr);
}

Error ReindexerImpl::Update(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(true, Update, item); }

Error ReindexerImpl::Update(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(true, Update, item, qr);
}

Error ReindexerImpl::Update(const Query& q, LocalQueryResults& result, const RdxContext& rdxCtx) {
	try {
		q.VerifyForUpdate();
		const std::string& nsName = q.NsName();
		auto ns = getNamespace(nsName, rdxCtx);
		ns->Update(q, result, rdxCtx);
		if (isSystemNamespaceNameFast(nsName)) {
			RdxContext rdxCtxNoCancel = rdxCtx.NoCancel();
			rdxCtxNoCancel.WithNoWaitSync(true);
			for (auto& it : result) {
				auto item = it.GetItem(false);
				updateToSystemNamespace(nsName, item, rdxCtxNoCancel);
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return {};
}

Error ReindexerImpl::Upsert(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(true, Upsert, item); }

Error ReindexerImpl::Upsert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(true, Upsert, item, qr);
}

Item ReindexerImpl::NewItem(std::string_view nsName, const RdxContext& rdxCtx) {
	try {
		auto ns = getNamespace(nsName, rdxCtx);
		auto item = ns->NewItem(rdxCtx);
		item.impl_->SetNamespace(ns);
		return item;
	} catch (const Error& err) {
		return Item(err);
	}
}
LocalTransaction ReindexerImpl::NewTransaction(std::string_view _namespace, const RdxContext& rdxCtx) {
	try {
		return getNamespace(_namespace, rdxCtx)->NewTransaction(rdxCtx);
	} catch (const Error& err) {
		return LocalTransaction(err);
	}
}

Error ReindexerImpl::CommitTransaction(LocalTransaction& tr, LocalQueryResults& result, const RdxContext& rdxCtx) {
	Error err = errOK;
	try {
		getNamespace(tr.GetNsName(), rdxCtx)->CommitTransaction(tr, result, rdxCtx);
	} catch (const Error& e) {
		err = e;
	}

	return err;
}

Error ReindexerImpl::GetMeta(std::string_view nsName, const std::string& key, std::string& data, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::GetMeta>(nsName, ctx, data, key);
}

Error ReindexerImpl::PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::PutMeta>(nsName, ctx, key, data);
}

Error ReindexerImpl::EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::EnumMeta>(nsName, ctx, keys);
}

Error ReindexerImpl::DeleteMeta(std::string_view nsName, const std::string& key, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::DeleteMeta>(nsName, ctx, key);
}

Error ReindexerImpl::Delete(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(false, Delete, item); }

Error ReindexerImpl::Delete(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(false, Delete, item, qr);
}

Error ReindexerImpl::Delete(const Query& q, LocalQueryResults& result, const RdxContext& ctx) {
	q.VerifyForUpdate();
	const std::string_view nsName = q.NsName();
	APPLY_NS_FUNCTION2(false, Delete, q, result);
}

Error ReindexerImpl::Select(const Query& q, LocalQueryResults& result, const RdxContext& rdxCtx) {
	try {
		RxSelector::NsLocker<const RdxContext> locks(rdxCtx);

		auto mainNsWrp = getNamespace(q.NsName(), rdxCtx);
		const bool isWalQuery = q.IsWALQuery();
		auto mainNs = isWalQuery ? mainNsWrp->awaitMainNs(rdxCtx) : mainNsWrp->getMainNs();

		const auto queriesPerfStatsEnabled = configProvider_.QueriesPerfStatsEnabled();
		const auto queriesThresholdUS = configProvider_.QueriesThresholdUS();
		PerfStatCalculatorMT calc(mainNs->selectPerfCounter_, mainNs->enablePerfCounters_);	 // todo more accurate detect joined queries
		auto& tracker = queriesStatTracker_;
		WrSerializer normalizedSQL, nonNormalizedSQL;
		if (queriesPerfStatsEnabled) {
			q.GetSQL(normalizedSQL, true);

			if (rdxCtx.Activity()) {
				q.GetSQL(nonNormalizedSQL, false);
			}
		}
		const QueriesStatTracer::QuerySQL sql{normalizedSQL.Slice(), nonNormalizedSQL.Slice()};

		auto hitter = queriesPerfStatsEnabled
			? [&sql, &tracker](bool lockHit, std::chrono::microseconds time) {
				if (lockHit) {
					tracker.LockHit(sql, time);
				} else {
					tracker.Hit(sql, time);
				}
			} : std::function<void(bool, std::chrono::microseconds)>{};

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
		struct {
			bool isWalQuery;
			RxSelector::NsLocker<const RdxContext>& locks;
			const RdxContext& ctx;
		} refs{isWalQuery, locks, rdxCtx};
		q.WalkNested(false, true, true, [this, &refs](const Query& q) {
			auto nsWrp = getNamespace(q.NsName(), refs.ctx);
			auto ns = refs.isWalQuery ? nsWrp->awaitMainNs(refs.ctx) : nsWrp->getMainNs();
			ns->updateSelectTime();
			refs.locks.Add(std::move(ns));
		});

		locks.Lock();
		calc.LockHit();
		statCalculator.LockHit();

		if (statsSelectLck.owns_lock()) {
			// Allow concurrent Refill's preparation for system namespaces during Select execution
			statsSelectLck.unlock();
		}

		const auto ward = rdxCtx.BeforeSimpleState(Activity::InProgress);
		SelectFunctionsHolder func;
		RxSelector::DoSelect(q, result, locks, func, rdxCtx, statCalculator);
		func.Process(result);
	} catch (const Error& err) {
		if (auto cmpl = rdxCtx.Compl(); cmpl) {
			cmpl(err);
		}
		return err;
	}
	if (auto cmpl = rdxCtx.Compl(); cmpl) {
		cmpl(Error());
	}
	return Error();
}

std::set<std::string> ReindexerImpl::getFTIndexes(std::string_view nsName) {
	const RdxContext rdxCtx;
	auto rlck = nsLock_.RLock(rdxCtx);
	auto it = namespaces_.find(nsName);
	if (it == namespaces_.end()) {
		return {};
	} else {
		return it->second->GetFTIndexes(rdxCtx);
	}
}

PayloadType ReindexerImpl::getPayloadType(std::string_view nsName) {
	const RdxContext rdxCtx;
	auto rlck = nsLock_.RLock(rdxCtx);
	auto it = namespaces_.find(nsName);
	if (it == namespaces_.end()) {
		static const PayloadType pt;
		return pt;
	}
	return it->second->getPayloadType(rdxCtx);
}

Namespace::Ptr ReindexerImpl::getNamespace(std::string_view nsName, const RdxContext& ctx) {
	auto rlck = nsLock_.RLock(ctx);
	auto nsIt = namespaces_.find(nsName);
	if (nsIt == namespaces_.end()) {
		throw Error(errNotFound, "Namespace '{}' does not exist", nsName);
	}

	assertrx(nsIt->second);
	return nsIt->second;
}

Namespace::Ptr ReindexerImpl::getNamespaceNoThrow(std::string_view nsName, const RdxContext& ctx) {
	auto rlck = nsLock_.RLock(ctx);
	const auto nsIt = namespaces_.find(nsName);
	return (nsIt == namespaces_.end()) ? nullptr : nsIt->second;
}

lsn_t ReindexerImpl::setNsVersion(Namespace::Ptr& ns, const std::optional<NsReplicationOpts>& replOpts, const RdxContext& ctx) {
	lsn_t version;
	if (replOpts.has_value()) {
		const auto curVer = ns->GetReplState(ctx).nsVersion;
		if (replOpts->nsVersion.isEmpty()) {
			if (curVer.isEmpty()) {
				version = nsVersion_.GetNext();
			} else {
				version = curVer;
			}
		} else {
			if (!curVer.isEmpty() && replOpts->nsVersion != curVer) {
				throw Error(errWrongReplicationData, "Namespace version missmatch. Expected version: {}, actual version: {}",
							replOpts->nsVersion, curVer);
			}
			nsVersion_.UpdateCounter(replOpts->nsVersion.Counter());
			version = replOpts->nsVersion;
		}
		if (version != curVer) {
			ns->SetNsVersion(version, ctx);
		}
	}
	return version;
}

Error ReindexerImpl::AddIndex(std::string_view nsName, const IndexDef& indexDef, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::AddIndex>(nsName, ctx, indexDef);
}

Error ReindexerImpl::DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::DumpIndex>(nsName, ctx, os, index);
}

Error ReindexerImpl::SetSchema(std::string_view nsName, std::string_view schema, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::SetSchema>(nsName, ctx, schema);
}

Error ReindexerImpl::GetSchema(std::string_view nsName, int format, std::string& schema, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::GetSchema>(nsName, ctx, schema, format);
}

Error ReindexerImpl::UpdateIndex(std::string_view nsName, const IndexDef& indexDef, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::UpdateIndex>(nsName, ctx, indexDef);
}

Error ReindexerImpl::DropIndex(std::string_view nsName, const IndexDef& indexDef, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::DropIndex>(nsName, ctx, indexDef);
}

std::vector<std::pair<std::string, Namespace::Ptr>> ReindexerImpl::getNamespaces(const RdxContext& ctx) {
	auto rlck = nsLock_.RLock(ctx);
	std::vector<std::pair<std::string, Namespace::Ptr>> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.emplace_back(ns.first, ns.second);
	}
	return ret;
}

std::vector<std::string> ReindexerImpl::getNamespacesNames(const RdxContext& ctx) {
	std::vector<std::string> ret;
	auto rlck = nsLock_.RLock(ctx);
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) {
		ret.emplace_back();
		reindexer::deepCopy(ret.back(), ns.first);	// Forced copy to avoid races with COW strings on centos7
	}
	return ret;
}

Error ReindexerImpl::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const RdxContext& rdxCtx) {
	logFmt(LogTrace, "ReindexerImpl::EnumNamespaces ({},{})", opts.options_, opts.filter_);
	try {
		auto nsarray = getNamespaces(rdxCtx);
		for (auto& nspair : nsarray) {
			if (!opts.MatchFilter(nspair.first, nspair.second, rdxCtx)) {
				continue;
			}
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
			if (fs::ReadDir(storagePath_, dirs) != 0) {
				return Error(errLogic, "Could not read database dir");
			}

			for (auto& d : dirs) {
				if (d.isDir && d.name != "." && d.name != ".." && opts.MatchNameFilter(d.name)) {
					{
						auto rlck = nsLock_.RLock(rdxCtx);
						if (namespaces_.find(d.name) != namespaces_.end()) {
							continue;
						}
					}
					assertrx(clusterizator_);
					auto tmpNs = std::make_unique<NamespaceImpl>(d.name, std::optional<int32_t>(), *clusterizator_, observers_);
					try {
						tmpNs->EnableStorage(storagePath_, StorageOpts(), storageType_, rdxCtx);
						if (opts.IsHideTemporary() && tmpNs->IsTemporary(rdxCtx)) {
							continue;
						}
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
	return {};
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
			} catch (std::exception& err) {
				logFmt(LogWarning, "backgroundRoutine() failed with ns '{}': {}", name, err.what());
			} catch (...) {
				logFmt(LogWarning, "backgroundRoutine() failed with ns '{}': unknown exception", name);
			}
		}
		for (auto& watcher : configWatchers_) {
			watcher.Check();
		}
	};

	net::ev::periodic t;
	t.set(loop);
	t.set([&nsBackground](net::ev::timer&, int) noexcept {
		try {
			nsBackground();
		} catch (std::exception& e) {
			logFmt(LogError, "Unexpected exception in background thread: {}", e.what());
		} catch (...) {
			logFmt(LogError, "Unexpected exception in background thread: ???");
		}
	});
	t.start(0.1, 0.1);

	while (!dbDestroyed_.load(std::memory_order_relaxed)) {
		loop.run();
	}
	nsBackground();
}

void ReindexerImpl::annCachingRoutine(net::ev::dynamic_loop& loop) {
	static const RdxContext dummyCtx;
	auto updateAnnStorageCache = [&]() {
		auto nsarray = getNamespacesNames(dummyCtx);
		for (const auto& name : nsarray) {
			try {
				auto ns = getNamespace(name, dummyCtx);
				ns->ANNCachingRoutine();
			} catch (std::exception& err) {
				logFmt(LogWarning, "annCachingRoutine() failed: '{}'", err.what(), name);
			} catch (...) {
				logFmt(LogWarning, "annCachingRoutine() failed with ns: '{}'", name);
			}
		}
	};

	net::ev::periodic t;
	t.set(loop);
	t.set([&updateAnnStorageCache](net::ev::timer&, int) noexcept {
		try {
			updateAnnStorageCache();
		} catch (std::exception& e) {
			logFmt(LogError, "Unexpected exception in ann storage cache thread: {}", e.what());
		} catch (...) {
			logFmt(LogError, "Unexpected exception in ann storage cache thread: ???");
		}
	});
	t.start(1.0, 1.0);

	while (!dbDestroyed_.load(std::memory_order_relaxed)) {
		loop.run();
	}
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
						logFmt(LogWarning, "storageFlushingRoutine() failed: '{}' ({} successive errors on ns '{}')",
							   errInfo->lastError.what(), errInfo->skipedErrorMsgs + 1, name);
					} else {
						logFmt(LogWarning, "storageFlushingRoutine() failed: '{}'", errInfo->lastError.what(), name);
					}
				}
			} catch (...) {
				logFmt(LogWarning, "storageFlushingRoutine() failed with ns: '{}'", name);
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
		} catch (std::exception& e) {
			logFmt(LogError, "Unexpected exception in flushing thread: {}", e.what());
		} catch (...) {
			logFmt(LogError, "Unexpected exception in flushing thread: ???");
		}
	});
	t.start(0.1, 0.1);

	while (!dbDestroyed_.load(std::memory_order_relaxed)) {
		loop.run();
	}
	nsFlush();
}

void ReindexerImpl::createSystemNamespaces() {
	for (const auto& nsDef : kSystemNsDefs) {
		auto err = AddNamespace(nsDef);
		if (!err.ok()) {
			logFmt(LogWarning, "Unable to create system namespace '{}': {}", nsDef.name, err.what());
		}
	}
}

Error ReindexerImpl::tryLoadShardingConf(const RdxContext& ctx) noexcept {
	try {
		Item item = NewItem(kConfigNamespace, ctx);
		if (!item.Status().ok()) {
			return item.Status();
		}
		auto config = shardingConfig_.Get();
		WrSerializer ser;
		{
			JsonBuilder jb{ser};
			jb.Put("type", kShardingConfigType);
			auto shardCfgObj = jb.Object(kShardingConfigType);
			if (config) {
				config->GetJSON(shardCfgObj, cluster::MaskingDSN::Enabled);
			}
		}
		Error err = item.FromJSON(ser.Slice());
		if (!err.ok()) {
			return err;
		}
		return config ? Upsert(kConfigNamespace, item, ctx) : Delete(kConfigNamespace, item, ctx);
	}
	CATCH_AND_RETURN
}

void ReindexerImpl::handleDropANNCacheAction(const gason::JsonNode& action, const RdxContext& ctx) {
	auto& nsNode = action["namespace"];
	auto& indexNode = action["index"];
	std::string_view indexName;
	if (!indexNode.empty() && indexNode.As<std::string_view>() != "*") {
		indexName = indexNode.As<std::string_view>();
	}
	if (nsNode.empty() || nsNode.As<std::string_view>() == "*") {
		for (auto& ns : getNamespaces(ctx)) {
			ns.second->DropANNStorageCache(indexName, ctx);
		}
	} else if (auto ns = getNamespaceNoThrow(nsNode.As<std::string_view>(), ctx); ns) {
		ns->DropANNStorageCache(indexName, ctx);
	}
}

void ReindexerImpl::handleRebuildIVFIndexAction(const gason::JsonNode& action, const RdxContext& ctx) {
	auto& nsNode = action["namespace"];
	auto& indexNode = action["index"];
	auto& dataPartNode = action["data_part"];
	std::string_view indexName;
	float dataPart = 1.0;
	if (!indexNode.empty() && indexNode.As<std::string_view>() != "*") {
		indexName = indexNode.As<std::string_view>();
	}
	if (!dataPartNode.empty()) {
		dataPart = dataPartNode.As<float>();
	}
	if (nsNode.empty() || nsNode.As<std::string_view>() == "*") {
		for (auto& ns : getNamespaces(ctx)) {
			ns.second->RebuildIVFIndex(indexName, dataPart, ctx);
		}
	} else if (auto ns = getNamespaceNoThrow(nsNode.As<std::string_view>(), ctx); ns) {
		ns->RebuildIVFIndex(indexName, dataPart, ctx);
	}
}

Error ReindexerImpl::InitSystemNamespaces() {
	createSystemNamespaces();

	LocalQueryResults results;
	auto err = Select(Query(kConfigNamespace), results, RdxContext());
	if (!err.ok()) {
		return err;
	}

	bool hasBaseReplicationConfig = false;
	bool hasAsyncReplicationConfig = false;
	bool hasShardingConfig = false;

	// Fail earlier
	// Reading config files first
	{
		logFmt(LogInfo, "Attempting to load replication config from '{}'", kReplicationConfFilename);
		err = tryLoadConfFromFile<kReplicationConfigType, ReplicationConfigData>(kReplicationConfFilename);
		if (err.ok()) {
			hasBaseReplicationConfig = true;
			logFmt(LogInfo, "Replication config loaded from '{}'", kReplicationConfFilename);
		} else if (err.code() == errNotFound) {
			logFmt(LogInfo, "Not found '{}'", kReplicationConfFilename);
		} else {
			return Error(err.code(), "Failed to load general replication config file: '{}'", err.what());
		}
	}

	{
		logFmt(LogInfo, "Attempting to load async replication config from '{}'", kAsyncReplicationConfFilename);
		err = tryLoadConfFromFile<kAsyncReplicationConfigType, cluster::AsyncReplConfigData>(kAsyncReplicationConfFilename);
		if (err.ok()) {
			hasAsyncReplicationConfig = true;
			logFmt(LogInfo, "Async replication config loaded from '{}'", kAsyncReplicationConfFilename);
		} else if (err.code() == errNotFound) {
			logFmt(LogInfo, "Not found '{}'", kAsyncReplicationConfFilename);
		} else {
			return Error(err.code(), "Failed to load async replication config file: '{}'", err.what());
		}
	}

	if (shardingConfig_) {
		// Ignore error check here (it always returns fake error for now)
		[[maybe_unused]] auto _ = tryLoadShardingConf();
		if (err.ok() || (err.code() == errLogic)) {
			hasShardingConfig = true;
		}
	}

	// Filling rest of default config
	if (results.Count() == 0) {
		logFmt(LogInfo, "Initializing default DB config for missed sections");
		for (const auto& conf : kDefDBConfig) {
			if (!hasBaseReplicationConfig || !hasAsyncReplicationConfig || !hasShardingConfig) {
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(std::string_view(conf));
				const std::string_view type = configJson["type"].As<std::string_view>();
				if ((type == kReplicationConfigType && hasBaseReplicationConfig) ||
					(type == kAsyncReplicationConfigType && hasAsyncReplicationConfig) ||
					(type == kShardingConfigType && shardingConfig_ && hasShardingConfig)) {
					continue;
				}
			}

			Item item = NewItem(kConfigNamespace, RdxContext());
			if (!item.Status().ok()) {
				return item.Status();
			}
			err = item.FromJSON(conf);
			if (!err.ok()) {
				return err;
			}
			err = insertDontUpdateSystemNS(kConfigNamespace, item, RdxContext());
			if (!err.ok()) {
				return err;
			}
		}
	}

	// #config probably was updated, so we need to reload previous results
	results = LocalQueryResults();
	err = Select(Query(kConfigNamespace), results, RdxContext());
	if (!err.ok()) {
		return err;
	}

	{
		logFmt(LogInfo, "Reading configuration from namespace #config");
		for (auto it : results) {
			auto item = it.GetItem(false);
			try {
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(item.GetJSON());
				const std::string_view type = configJson["type"].As<std::string_view>();

				if ((type == kReplicationConfigType && hasBaseReplicationConfig) ||
					(type == kAsyncReplicationConfigType && hasAsyncReplicationConfig) ||
					(type == kShardingConfigType && shardingConfig_ && hasShardingConfig)) {
					continue;
				}

				updateConfigProvider(configJson, true);
			} catch (const Error& err) {
				return err;
			}
		}
		if (auto configLoadErrors = configProvider_.GetConfigParseErrors()) {
			logFmt(LogError, "Config load errors:\n{}", configLoadErrors.what());
		}
	}

	auto replConfig = configProvider_.GetReplicationConfig();
	nsVersion_.SetServer(replConfig.serverID);

	// Update nsVersion.serverID for system namespaces
	RdxContext ctx;
	auto namespaces = getNamespaces(ctx);
	for (auto& ns : namespaces) {
		if (ns.second->IsSystem(ctx)) {
			ns.second->OnConfigUpdated(configProvider_, ctx);
		}
	}
	return {};
}

template <const char* type, typename ConfigT>
Error ReindexerImpl::tryLoadConfFromFile(const std::string& filename) {
	std::string yamlConf;
	int res = fs::ReadFile(fs::JoinPath(storagePath_, filename), yamlConf);
	if (res > 0) {
		return tryLoadConfFromYAML<type, ConfigT>(yamlConf);
	}
	return Error(errNotFound);
}

template <const char* type, typename ConfigT>
Error ReindexerImpl::tryLoadConfFromYAML(const std::string& yamlConf) {
	if (yamlConf.empty()) {
		return Error(errNotFound);
	}

	ConfigT config;
	Error err = config.FromYAML(yamlConf);
	if (!err.ok()) {
		logFmt(LogError, "Error parsing config YML for {}: {}", type, err.what());
		return Error(err.code(), "Error parsing config YML for {}: {}", type, err.what());
	} else {
		WrSerializer ser;
		JsonBuilder jb(ser);
		jb.Put("type", type);
		auto jsonNode = jb.Object(type);
		if constexpr (std::is_same_v<ConfigT, cluster::AsyncReplConfigData>) {
			config.GetJSON(jsonNode, cluster::MaskingDSN::Disabled);
		} else {
			config.GetJSON(jsonNode);
		}
		jsonNode.End();
		jb.End();

		Item item = NewItem(kConfigNamespace, RdxContext());
		if (!item.Status().ok()) {
			return item.Status();
		}
		err = item.Unsafe().FromJSON(ser.Slice());
		if (!err.ok()) {
			return err;
		}
		return Upsert(kConfigNamespace, item, RdxContext());
	}
}

void ReindexerImpl::updateToSystemNamespace(std::string_view nsName, Item& item, const RdxContext& ctx) {
	if (item.GetID() != -1 && nsName == kConfigNamespace) {
		try {
			gason::JsonParser parser;
			gason::JsonNode configJson = parser.Parse(item.GetJSON());
			if (!ctx.NoWaitSync()) {
				throw Error(errLogic, "Expecting no wait sync flag for config update");
			}
			bool isChangedActivityStats = configProvider_.ActivityStatsEnabled();
			updateConfigProvider(configJson);

			isChangedActivityStats = isChangedActivityStats != configProvider_.ActivityStatsEnabled();
			if (isChangedActivityStats) {
				activities_.Reset();
			}

			if (!configJson[kReplicationConfigType].empty()) {
				auto replConf = configProvider_.GetReplicationConfig();
				updateConfFile(replConf, kReplicationConfFilename);
				{
					auto wlck = nsLock_.SimpleWLock(ctx);
					nsVersion_.SetServer(replConf.serverID);
				}
				clusterizator_->Configure(std::move(replConf));
			}
			if (!configJson[kAsyncReplicationConfigType].empty()) {
				auto asyncReplConf = configProvider_.GetAsyncReplicationConfig();
				updateConfFile(asyncReplConf, kAsyncReplicationConfFilename);
				clusterizator_->Configure(std::move(asyncReplConf));
			}
			if (!configJson[kShardingConfigType].empty()) {
				throw Error(errLogic, "Sharding configuration can not be updated directly. Use 'apply_sharding_config' action instead");
			}

			const auto namespaces = getNamespaces(ctx);
			for (auto& ns : namespaces) {
				ns.second->OnConfigUpdated(configProvider_, ctx);
			}
			const auto& actionNode = configJson[kActionConfigType];
			handleConfigAction(actionNode, namespaces, ctx);

			if (replicationEnabled_ && !dbDestroyed_) {
				if (clusterizator_->IsExpectingAsyncReplStartup()) {
					if (Error err = clusterizator_->StartAsyncRepl()) {
						throw err;
					}
				}
				if (clusterizator_->IsExpectingClusterStartup()) {
					if (Error err = clusterizator_->StartClusterRepl()) {
						throw err;
					}
				}
			}

			if (Error err = configProvider_.GetConfigParseErrors()) {
				throw err;
			}
		} catch (gason::Exception& e) {
			throw Error(errParseJson, "JSON parsing error: {}", e.what());
		}
	} else if (nsName == kQueriesPerfStatsNamespace) {
		queriesStatTracker_.Reset();
	} else if (nsName == kPerfStatsNamespace) {
		for (auto& ns : getNamespaces(ctx)) {
			ns.second->ResetPerfStat(ctx);
		}
	} else if (nsName == kClusterConfigNamespace) {
		//  TODO: reconfigure clusterization
		// Separate namespace is required, because it has to be replicated into all cluster nodes
		// and other system namespaces are not replicable
	}
}

void ReindexerImpl::handleConfigAction(const gason::JsonNode& action, const std::vector<std::pair<std::string, Namespace::Ptr>>& namespaces,
									   const RdxContext& ctx) {
	if (!action.empty()) {
		std::string_view command = action["command"].As<std::string_view>();
		if (command == "set_leader_node"sv) {
			if (!clusterConfig_) {
				throw Error(errLogic,
							"Cluster replicator is not configured. Command 'set_leader_node' is only available for cluster node.");
			}
			const int newLeaderId = action["server_id"].As<int>(-1);
			if (newLeaderId == -1) {
				throw Error(errLogic, "Expecting 'server_id' in 'set_leader_node' command");
			}
			cluster::RaftInfo info = clusterizator_->GetRaftInfo(false, ctx);  // current node leader or follower
			if (info.leaderId == newLeaderId) {
				return;
			}
			clusterConfig_->GetNodeIndexForServerId(newLeaderId);  // check if nextServerId in config (on error throw)

			const auto err = clusterizator_->SetDesiredLeaderId(newLeaderId, true);
			if (!err.ok()) {
				throw err;
			}
		} else if (command == "restart_replication"sv) {
			clusterizator_->StopAsyncRepl();
		} else if (command == "reset_replication_role"sv) {
			std::string_view name = action["namespace"].As<std::string_view>();
			if (name.empty()) {
				for (auto& ns : namespaces) {
					if (!clusterizator_->NamespaceIsInClusterConfig(ns.first)) {
						auto err = ns.second->SetClusterizationStatus(ClusterizationStatus(), ctx);
						if (!err.ok()) {
							throw err;
						}
					}
				}
			} else {
				if (clusterizator_->NamespaceIsInClusterConfig(name)) {
					throw Error(errLogic, "Role of the cluster namespace may not be dropped");
				}
				auto ns = getNamespaceNoThrow(name, ctx);
				if (ns) {
					auto err = ns->SetClusterizationStatus(ClusterizationStatus(), ctx);
					if (!err.ok()) {
						throw err;
					}
				}
			}
		} else if (command == "set_log_level"sv) {
			std::string_view type = action["type"].As<std::string_view>();
			const auto level = logLevelFromString(action["level"].As<std::string_view>("info"));
			if (type == "async_replication"sv) {
				clusterizator_->SetAsyncReplicatonLogLevel(level);
			} else if (type == "cluster"sv) {
				clusterizator_->SetClusterReplicatonLogLevel(level);
			} else {
				throw Error(errParams, "Unknown logs type in config-action: '{}'", type);
			}
		} else if (command == "drop_ann_storage_cache"sv) {
			handleDropANNCacheAction(action, ctx);
		} else if (command == "rebuild_ivf_index"sv) {
			handleRebuildIVFIndexAction(action, ctx);
		}

		if (const auto it = proxyCallbacks_.find({command, CallbackT::Type::User}); it != proxyCallbacks_.end()) {
			it->second(action, CallbackT::EmptyT{}, ctx);
		}
	}
}

void ReindexerImpl::updateConfigProvider(const gason::JsonNode& config, bool autoCorrect) {
	Error err;
	try {
		err = configProvider_.FromJSON(config, autoCorrect);
	} catch (const gason::Exception& ex) {
		err = Error(errParseJson, ex.what());
	}

	if (!err.ok()) {
		if (autoCorrect) {
			logFmt(LogError, "DBConfigProvider: Non fatal error {} \"{}\"", int(err.code()), err.what());
			return;
		}

		throw err;
	}
}

template <typename ConfigT>
void ReindexerImpl::updateConfFile(const ConfigT& newConf, std::string_view filename) {
	WrSerializer ser;
	newConf.GetYAML(ser);
	for (auto& watcher : configWatchers_) {
		if (watcher.Filename() != filename) {
			continue;
		}
		auto err = watcher.RewriteFile(std::string(ser.Slice()), [&newConf](const std::string& content) {
			ConfigT config;
			Error err = config.FromYAML(content);
			return err.ok() && (config == newConf);
		});
		if (!err.ok()) {
			throw err;
		}
		break;
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
	logFmt(
		LogTrace, "ReindexerImpl::syncSystemNamespaces ({},{})", sysNsName,
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
				if (std::find_if(filterNsNames->cbegin(), filterNsNames->cend(), [&nspair](std::string_view name) noexcept {
						return iequals(nspair.first, name);
					}) == filterNsNames->cend()) {
					continue;
				}
			}
			if (isSystemNamespaceNameFast(nspair.first) && !withSystem) {
				continue;
			}
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
						  bool notRenamed = iequals(stats.name, nsName);
						  if (notRenamed) {
							  stats.GetJSON(ser);
						  }
						  return notRenamed;
					  });
		}
	} else if (sysNsName == kMemStatsNamespace) {
		if (configProvider_.MemStatsEnabled()) {
			forEachNS(getNamespace(kMemStatsNamespace, ctx), false,
					  [&ctx](std::string_view nsName, const Namespace::Ptr& nsPtr, WrSerializer& ser) {
						  auto stats = nsPtr->GetMemStat(ctx);
						  bool notRenamed = iequals(stats.name, nsName);
						  if (notRenamed) {
							  stats.GetJSON(ser);
						  }
						  return notRenamed;
					  });
		}
	} else if (sysNsName == kNamespacesNamespace) {
		forEachNS(getNamespace(kNamespacesNamespace, ctx), true,
				  [&ctx](std::string_view nsName, const Namespace::Ptr& nsPtr, WrSerializer& ser) {
					  auto stats = nsPtr->GetDefinition(ctx);
					  bool notRenamed = iequals(stats.name, nsName);
					  if (notRenamed) {
						  stats.GetJSON(ser);
					  }
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
				if (!err.ok()) {
					throw err;
				}
			}
			queriesperfstatsNs->Refill(items, ctx);
		}
	} else if (sysNsName == kActivityStatsNamespace) {
		const auto data = activities_.List(configProvider_.GetReplicationConfig().serverID);
		std::vector<Item> items;
		WrSerializer ser;
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
			if (!err.ok()) {
				throw err;
			}
		}
		activityNs->Refill(items, ctx);
	} else if (sysNsName == kClientsStatsNamespace) {
		if (config_.clientsStats) {
			std::vector<ClientStat> clientInf;
			WrSerializer ser;
			config_.clientsStats->GetClientInfo(clientInf);
			auto clientsNs = getNamespace(kClientsStatsNamespace, ctx);
			std::vector<Item> items;
			items.reserve(clientInf.size());
			for (auto& i : clientInf) {
				if (auto query = activities_.QueryForIpConnection(i.connectionId); query) {
					i.currentActivity = std::move(*query);
				}
				ser.Reset();
				i.GetJSON(ser);
				auto& item = items.emplace_back(clientsNs->NewItem(ctx));
				if (!item.Status().ok()) {
					throw item.Status();
				}
				auto err = item.FromJSON(ser.Slice());
				if (!err.ok()) {
					throw err;
				}
			}
			clientsNs->Refill(items, ctx);
		}
	} else if (sysNsName == kReplicationStatsNamespace) {
		if (clusterizator_) {
			std::vector<Item> items;
			WrSerializer ser;
			items.reserve(2);
			std::array<cluster::ReplicationStats, 2> stats;
			stats[0] = clusterizator_->GetAsyncReplicationStats();
			stats[1] = clusterizator_->GetClusterReplicationStats();
			auto replStatsNs = getNamespace(kReplicationStatsNamespace, ctx);
			for (const auto& stat : stats) {
				ser.Reset();
				items.emplace_back(replStatsNs->NewItem(ctx));
				stat.GetJSON(ser);
				auto err = items.back().FromJSON(ser.Slice());
				if (!err.ok()) {
					throw err;
				}
			}
			replStatsNs->Refill(items, ctx);
		}
	}
	return resultLock;
}

void ReindexerImpl::onProfiligConfigLoad() {
	LocalQueryResults qr1, qr2, qr3;
	RdxContext ctx;
	auto err = Delete(Query(kMemStatsNamespace), qr2, ctx);
	err = Delete(Query(kQueriesPerfStatsNamespace), qr3, ctx);
	err = Delete(Query(kPerfStatsNamespace), qr1, ctx);
	(void)err;	// ignore
}

Error ReindexerImpl::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions,
									   const RdxContext& rdxCtx) {
	std::vector<NamespaceDef> nses;

	suggestions = SQLSuggester::GetSuggestions(
		sqlQuery, pos,
		[&, this](EnumNamespacesOpts opts) {
			auto err = EnumNamespaces(nses, opts, rdxCtx);
			(void)err;	// ignore
			return nses;
		},
		[&rdxCtx, this](std::string_view ns) {
			auto nsPtr = getNamespaceNoThrow(ns, rdxCtx);
			if (nsPtr) {
				return nsPtr->GetSchemaPtr(rdxCtx);
			}
			return std::shared_ptr<const Schema>();
		});
	return {};
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
		Error status = GetSchema(ns.nsName, ProtobufSchemaType, nsProtobufSchema, RdxContext());
		if (!status.ok()) {
			return status;
		}
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
		LocalQueryResults qr;
		status = Select(Query(ns.nsName).Limit(0), qr, RdxContext());
		if (!status.ok()) {
			return status;
		}
		ns.nsNumber = qr.getNsNumber(0) + 1;
	}

	ser << "// Possible item schema variants in LocalQueryResults or in ModifyResults\n";
	schemaBuilder.Object(TagName::Empty(), "ItemsUnion", false, [&](ProtobufSchemaBuilder& obj) {
		ser << "oneof item {\n";
		for (auto& ns : nses) {
			obj.Field(ns.nsName, TagName(ns.nsNumber), FieldProps{KeyValueType::Tuple{}, false, false, false, ns.objName});
		}
		ser << "}\n";
	});

	ser << "// The LocalQueryResults message is schema of http API methods response:\n";
	ser << "// - GET api/v1/db/:db/namespaces/:ns/items\n";
	ser << "// - GET/POST api/v1/db/:db/query\n";
	ser << "// - GET/POST api/v1/db/:db/sqlquery\n";
	schemaBuilder.Object(TagName::Empty(), "QueryResults", false, [](ProtobufSchemaBuilder& obj) {
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
	schemaBuilder.Object(TagName::Empty(), "ModifyResults", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamItems, kProtoModifyResultsFields.at(kParamItems),
				  FieldProps{KeyValueType::Tuple{}, true, false, false, "ItemsUnion"});
		obj.Field(kParamUpdated, kProtoModifyResultsFields.at(kParamUpdated), FieldProps{KeyValueType::Int{}});
		obj.Field(kParamSuccess, kProtoModifyResultsFields.at(kParamSuccess), FieldProps{KeyValueType::Bool{}});
	});

	ser << "// The ErrorResponse message is schema of http API methods response on error condition \n";
	ser << "// With non 200 http status code\n";
	schemaBuilder.Object(TagName::Empty(), "ErrorResponse", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamSuccess, kProtoErrorResultsFields.at(kParamSuccess), FieldProps{KeyValueType::Bool{}});
		obj.Field(kParamResponseCode, kProtoErrorResultsFields.at(kParamResponseCode), FieldProps{KeyValueType::Int{}});
		obj.Field(kParamDescription, kProtoErrorResultsFields.at(kParamDescription), FieldProps{KeyValueType::String{}});
	});
	schemaBuilder.End();
	return {};
}

Error ReindexerImpl::GetReplState(std::string_view nsName, ReplicationStateV2& state, const RdxContext& rdxCtx) noexcept {
	try {
		if (!nsName.empty()) {
			state = getNamespace(nsName, rdxCtx)->GetReplStateV2(rdxCtx);
		} else {
			state.lastLsn = lsn_t();
			state.dataHash = 0;
			auto rlck = nsLock_.RLock(rdxCtx);
			state.clusterStatus = clusterStatus_;
		}
	}
	CATCH_AND_RETURN;
	return {};
}

Error ReindexerImpl::SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus& status,
											 const RdxContext& rdxCtx) noexcept {
	try {
		return getNamespace(nsName, rdxCtx)->SetClusterizationStatus(ClusterizationStatus(status), rdxCtx);
	}
	CATCH_AND_RETURN;
	return {};
}

Error ReindexerImpl::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const RdxContext& rdxCtx) noexcept {
	try {
		getNamespace(nsName, rdxCtx)->GetSnapshot(snapshot, opts, rdxCtx);
	}
	CATCH_AND_RETURN;
	return {};
}

Error ReindexerImpl::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const RdxContext& rdxCtx) noexcept {
	try {
		getNamespace(nsName, rdxCtx)->ApplySnapshotChunk(ch, false, rdxCtx);
	}
	CATCH_AND_RETURN;
	return {};
}

bool ReindexerImpl::isSystemNamespaceNameStrict(std::string_view name) noexcept {
	return std::find_if(std::cbegin(kSystemNsDefs), std::cend(kSystemNsDefs),
						[name](const NamespaceDef& nsDef) { return iequals(nsDef.name, name); }) != std::cend(kSystemNsDefs);
}

Error ReindexerImpl::SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) {
	return clusterizator_->SuggestLeader(suggestion, response);
}

Error ReindexerImpl::LeadersPing(const cluster::NodeData& leader) { return clusterizator_->LeadersPing(leader); }

Error ReindexerImpl::GetRaftInfo(bool allowTransitState, cluster::RaftInfo& info, const RdxContext& rdxCtx) {
	try {
		info = clusterizator_->GetRaftInfo(allowTransitState, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return {};
}

Error ReindexerImpl::ClusterControlRequest(const ClusterControlRequestData& request) {
	switch (request.type) {
		case ClusterControlRequestData::Type::ChangeLeader:
			return clusterizator_->SetDesiredLeaderId(std::get<SetClusterLeaderCommand>(request.data).leaderServerId, false);
		case ClusterControlRequestData::Type::Empty:
			break;
	}
	return Error(errParams, "Unknown cluster command request. Command type [{}].", int(request.type));
}

Error ReindexerImpl::getLeaderDsn(DSN& dsn, unsigned short serverId, const cluster::RaftInfo& info) {
	try {
		if (!clusterConfig_ || !clusterizator_->Enabled()) {
			return Error(errLogic, "Cluster config not set.");
		}
		if (serverId == info.leaderId) {
			dsn = {};
			return {};
		}
		for (const auto& node : clusterConfig_->nodes) {
			if (node.serverId == info.leaderId) {
				dsn = node.GetRPCDsn();
				return {};
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return Error(errLogic, "Leader serverId is missing in the config.");
}

template <typename PreReplFunc, typename... Args>
Error ReindexerImpl::shardingConfigReplAction(const RdxContext& ctx, PreReplFunc func, Args&&... args) noexcept {
	try {
		auto wlck = nsLock_.DataWLock(ctx);
		if (!clusterizator_) {
			return Error();
		}

		return observers_.SendUpdate(
			std::apply([](auto&&... args) { return UpdateRecord{std::forward<decltype(args)>(args)...}; },
					   func(std::forward<Args>(args)...)),
			[&wlck] {
				assertrx(wlck.isClusterLck());
				wlck.unlock();
			},
			ctx);
	}
	CATCH_AND_RETURN
}

template <typename... Args>
Error ReindexerImpl::shardingConfigReplAction(const RdxContext& ctx, updates::URType type, Args&&... args) noexcept {
	return shardingConfigReplAction(
		ctx, [&ctx, &type](Args&&... aargs) { return std::make_tuple(type, ctx.EmmiterServerId(), std::forward<Args>(aargs)...); },
		std::forward<Args>(args)...);
}

Error ReindexerImpl::saveShardingCfgCandidate(std::string_view config, int64_t sourceId, const RdxContext& ctx) noexcept {
	auto preReplfunc = [this, &ctx](std::string_view config, int64_t sourceId) {
		cluster::ShardingConfig conf;
		auto err = conf.FromJSON(std::string_view(config));
		if (!err.ok()) {
			throw err;
		}

		const auto& hosts = conf.shards.at(conf.thisShardId);

		auto nodeStats = clusterizator_->GetClusterReplicationStats().nodeStats;
		if (!nodeStats.empty() && nodeStats.size() != hosts.size()) {
			throw Error(errLogic, "Not equal count of dsns in cluster and sharding config. Shard - {}", conf.thisShardId);
		}

		for (const auto& nodeStat : nodeStats) {
			if (auto it = std::find_if(hosts.begin(), hosts.end(), std::bind(&RelaxCompare, _1, nodeStat.dsn)); it == hosts.end()) {
				throw Error(errLogic, "Different sets of DSNs in cluster and sharding config");
			}
		}

		return std::make_tuple(updates::URType::SaveShardingConfig, ctx.EmmiterServerId(), std::string(config), sourceId);
	};

	return shardingConfigReplAction(ctx, std::move(preReplfunc), config, sourceId);
}

Error ReindexerImpl::applyShardingCfgCandidate(int64_t sourceId, const RdxContext& ctx) noexcept {
	return shardingConfigReplAction(ctx, updates::URType::ApplyShardingConfig, sourceId);
}

Error ReindexerImpl::resetOldShardingConfig(int64_t sourceId, const RdxContext& ctx) noexcept {
	return shardingConfigReplAction(ctx, updates::URType::ResetOldShardingConfig, sourceId);
}

Error ReindexerImpl::resetShardingConfigCandidate(int64_t sourceId, const RdxContext& ctx) noexcept {
	return shardingConfigReplAction(ctx, updates::URType::ResetCandidateConfig, sourceId);
}

Error ReindexerImpl::rollbackShardingConfigCandidate(int64_t sourceId, const RdxContext& ctx) noexcept {
	return shardingConfigReplAction(ctx, updates::URType::RollbackCandidateConfig, sourceId);
}

}  // namespace reindexer
