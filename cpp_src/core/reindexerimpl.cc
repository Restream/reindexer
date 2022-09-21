#include "core/reindexerimpl.h"
#include <stdio.h>
#include <chrono>
#include <thread>
#include "cjson/jsonbuilder.h"
#include "cluster/clustercontrolrequest.h"
#include "cluster/clusterizator.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/iclientsstats.h"
#include "core/index/index.h"
#include "core/itemimpl.h"
#include "core/nsselecter/crashqueryreporter.h"
#include "core/query/sql/sqlsuggester.h"
#include "core/selectfunc/selectfunc.h"
#include "core/type_consts_helpers.h"
#include "defnsconfigs.h"
#include "estl/contexted_locks.h"
#include "queryresults/joinresults.h"
#include "server/outputparameters.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"

#include "debug/backtrace.h"
#include "debug/terminate_handler.h"

std::once_flag initTerminateHandlerFlag;

using std::lock_guard;
using std::string;
using std::vector;
using namespace std::placeholders;
using reindexer::cluster::UpdateRecord;
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
	: clusterizator_(new cluster::Clusterizator(*this, cfg.maxReplUpdatesSize)),
	  nsLock_(*clusterizator_, *this),
	  storageType_(StorageType::LevelDB),
	  connected_(false),
	  config_(std::move(cfg)) {
	stopBackgroundThreads_ = false;
	configProvider_.setHandler(ProfilingConf, std::bind(&ReindexerImpl::onProfiligConfigLoad, this));

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

	backgroundThread_ = std::thread([this]() { this->backgroundRoutine(); });
	storageFlushingThread_ = std::thread([this]() { this->storageFlushingRoutine(); });
	std::call_once(initTerminateHandlerFlag, []() {
		debug::terminate_handler_init();
		debug::backtrace_set_crash_query_reporter(&reindexer::PrintCrashedQuery);
	});
}

ReindexerImpl::~ReindexerImpl() {
	stopBackgroundThreads_ = true;
	clusterizator_->Stop();
	backgroundThread_.join();
	storageFlushingThread_.join();
}

Error ReindexerImpl::EnableStorage(const string& storagePath, bool skipPlaceholderCheck, const RdxContext& ctx) {
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

	Error res;
	if (isHaveConfig) {
		res = openNamespace(kConfigNamespace, true, StorageOpts().Enabled().CreateIfMissing(), {}, ctx);
	}
	for (auto& watcher : configWatchers_) {
		watcher.SetDirectory(storagePath_);
	}
	auto err = readClusterConfigFile();
	if (res.ok()) res = Error(err.code(), "Failed to read cluster config file: '%s'", err.what());
	err = readShardingConfigFile();
	if (res.ok()) res = Error(err.code(), "Failed to read sharding config file: '%s'", err.what());

	return res;
}

Error ReindexerImpl::Connect(const string& dsn, ConnectOpts opts) {
	auto checkReplConf = [this](const ConnectOpts& opts) {
		if (opts.HasExpectedClusterID()) {
			auto replConfig = configProvider_.GetReplicationConfig();
			if (replConfig.clusterID != opts.ExpectedClusterID()) {
				return Error(errReplParams, "Expected leader's cluster ID(%d) is not equal to actual cluster ID(%d)",
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
		const size_t maxLoadWorkers = ConcurrentNamespaceLoaders();
		std::unique_ptr<std::thread[]> thrs(new std::thread[maxLoadWorkers]);
		std::atomic_flag hasNsErrors{false};
		std::atomic<int64_t> maxNsVersion = -1;
		for (size_t i = 0; i < maxLoadWorkers; i++) {
			thrs[i] = std::thread(
				[&](size_t begin) {
					for (size_t j = begin; j < foundNs.size(); j += maxLoadWorkers) {
						auto& de = foundNs[j];
						if (de.isDir && validateObjectName(de.name, true)) {
							if (de.name[0] == '@') {
								const std::string tmpPath = fs::JoinPath(storagePath_, de.name);
								logPrintf(LogWarning, "Dropping tmp namespace '%s'", de.name);
								if (fs::RmDirAll(tmpPath) < 0) {
									logPrintf(LogWarning, "Failed to remove '%s' temporary namespace from filesystem, path: %s", de.name,
											  tmpPath);
									hasNsErrors.test_and_set(std::memory_order_relaxed);
								}
								continue;
							}
							auto status = openNamespace(de.name, true, StorageOpts().Enabled(), {}, RdxContext());
							if (status.ok()) {
								RdxContext dummyCtx;
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
								logPrintf(LogError, "Failed to open namespace '%s' - %s", de.name, status.what());
								hasNsErrors.test_and_set(std::memory_order_relaxed);
							}
						}
					}
				},
				i);
		}
		for (size_t i = 0; i < maxLoadWorkers; i++) thrs[i].join();
		nsVersion_.UpdateCounter(maxNsVersion);

		if (!opts.IsAllowNamespaceErrors() && hasNsErrors.test_and_set(std::memory_order_relaxed)) {
			return Error(errNotValid, "Namespaces load error");
		}
		logPrintf(LogTrace, "%s: All of the namespaces were opened", storagePath_);
	}

	if (replicationEnabled_) {
		err = checkReplConf(opts);
		if (!err.ok()) return err;

		const auto replConfig = configProvider_.GetReplicationConfig();
		clusterizator_->Enable();
		clusterizator_->Configure(replConfig);

		clusterizator_->Configure(configProvider_.GetAsyncReplicationConfig());
		err = clusterizator_->IsExpectingAsyncReplStartup() ? clusterizator_->StartAsyncRepl() : errOK;
		if (!err.ok()) return err;

		if (!storagePath_.empty()) {
			for (auto& watcher : configWatchers_) {
				err = watcher.Enable();
				if (!err.ok()) return err;
			}
		}
		auto clusterConfig = clusterConfig_.get();
		if (clusterConfig) {
			if (config_.raftCluster) {
				clusterizator_->Configure(*clusterConfig);
				if (clusterizator_->IsExpectingClusterStartup()) {
					logPrintf(LogTrace, "%s: Clusterizator was started after connect", storagePath_);
					err = clusterizator_->StartClusterRepl();
				}
				if (!err.ok()) return err;
			} else {
				logPrintf(LogWarning, "%s: RAFT-cluster config exists, hovewer cluster is disabled in DB config", storagePath_);
			}
		}
	}

	if (err.ok()) {
		connected_.store(true, std::memory_order_release);
	}
	return err;
}

Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef, std::optional<NsReplicationOpts> replOpts, const RdxContext& rdxCtx) {
	Namespace::Ptr ns;
	try {
		{
			auto rlck = nsLock_.RLock(rdxCtx);
			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name);
			}
		}
		if (!validateObjectName(nsDef.name, nsDef.isTemporary)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		const bool readyToLoadStorage = (nsDef.storage.IsEnabled() && !storagePath_.empty());
		assertrx(clusterizator_);
		ns = std::make_shared<Namespace>(nsDef.name, replOpts.has_value() ? replOpts->tmStateToken : std::optional<int32_t>(),
										 clusterizator_.get());
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
		if (!rdxCtx.GetOriginLSN().isEmpty()) {
			// TODO: It may be a simple replica
			ns->SetClusterizationStatus(ClusterizationStatus{rdxCtx.GetOriginLSN().Server(), ClusterizationStatus::Role::ClusterReplica},
										rdxCtx);
		}
		rdxCtx.WithNoWaitSync(nsDef.isTemporary || ns->IsSystem(rdxCtx) || !clusterizator_->NamespaceIsInClusterConfig(nsDef.name));
		const int64_t stateToken = ns->NewItem(rdxCtx).GetStateToken();
		{
			auto wlck = nsLock_.DataWLock(rdxCtx);

			checkClusterRole(nsDef.name, rdxCtx.GetOriginLSN());

			if (namespaces_.find(nsDef.name) != namespaces_.end()) {
				return Error(errParams, "Namespace '%s' already exists", nsDef.name);
			}

			const lsn_t version = setNsVersion(ns, replOpts, rdxCtx);

			namespaces_.insert({nsDef.name, ns});
			if (!nsDef.isTemporary) {
				NamespaceDef def;
				def.name = nsDef.name;
				def.storage = nsDef.storage;  // Indexes and schema will be replicate later
				auto err = clusterizator_->Replicate(
					{UpdateRecord::Type::AddNamespace, nsDef.name, version, rdxCtx.EmmiterServerId(), std::move(def), stateToken},
					[&wlck] {
						assertrx(wlck.isClusterLck());
						wlck.unlock();
					},
					rdxCtx);
				if (!err.ok()) {
					throw err;
				}
			}
		}
		try {
			rdxCtx.WithNoWaitSync(nsDef.isTemporary || ns->IsSystem(rdxCtx));
			for (auto& indexDef : nsDef.indexes) ns->AddIndex(indexDef, rdxCtx);
			if (nsDef.HasSchema()) ns->SetSchema(nsDef.schemaJson, rdxCtx);
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
	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::OpenNamespace(std::string_view name, const StorageOpts& storageOpts, const NsReplicationOpts& replOpts,
								   const RdxContext& ctx) {
	return openNamespace(name, false, storageOpts, {replOpts}, ctx);
}

Error ReindexerImpl::DropNamespace(std::string_view nsName, const RdxContext& rdxCtx) {
	rdxCtx.WithNoWaitSync(!clusterizator_->NamespaceIsInClusterConfig(nsName));
	return closeNamespace(nsName, rdxCtx, true);
}

std::string ReindexerImpl::generateTemporaryNamespaceName(std::string_view baseName) {
	constexpr size_t kTmpNsPostfixLen = 20;
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
	try {
		auto wlck = nsLock_.DataWLock(ctx);

		auto nsIt = namespaces_.find(nsName);

		if (nsIt == namespaces_.end()) {
			return Error(errNotFound, "Namespace '%s' does not exist", nsName);
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
		if (!isTemporary) {
			auto err = clusterizator_->Replicate(
				{dropStorage ? UpdateRecord::Type::DropNamespace : UpdateRecord::Type::CloseNamespace, std::string(nsName), lsn_t(0, 0),
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
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::openNamespace(std::string_view name, bool skipNameCheck, const StorageOpts& storageOpts,
								   std::optional<NsReplicationOpts> replOpts, const RdxContext& rdxCtx) {
	try {
		{
			auto rlck = nsLock_.RLock(rdxCtx);
			auto nsIt = namespaces_.find(name);
			if (nsIt != namespaces_.end() && nsIt->second) {
				rlck.unlock();
				if (rdxCtx.HasEmmiterServer()) {
					return clusterizator_->Replicate(
						UpdateRecord{UpdateRecord::Type::EmptyUpdate, std::string(name), rdxCtx.EmmiterServerId()}, std::function<void()>(),
						rdxCtx);
				}
				return errOK;
			}
		}
		if (!skipNameCheck && !validateObjectName(name, false)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed");
		}
		NamespaceDef nsDef(std::string(name), storageOpts);
		assertrx(clusterizator_);
		auto ns = std::make_shared<Namespace>(nsDef.name, replOpts.has_value() ? replOpts->tmStateToken : std::optional<int32_t>(),
											  clusterizator_.get());
		if (storageOpts.IsEnabled() && !storagePath_.empty()) {
			auto opts = storageOpts;
			ns->EnableStorage(storagePath_, opts.Autorepair(autorepairEnabled_), storageType_, rdxCtx);
			ns->OnConfigUpdated(configProvider_, rdxCtx);
			ns->LoadFromStorage(kStorageLoadingThreads, rdxCtx);
		} else {
			ns->OnConfigUpdated(configProvider_, rdxCtx);
		}
		if (!rdxCtx.GetOriginLSN().isEmpty()) {
			ClusterizationStatus clStatus;
			clStatus.role = ClusterizationStatus::Role::ClusterReplica;	 // TODO: It may be a simple replica
			clStatus.leaderId = rdxCtx.GetOriginLSN().Server();
			ns->SetClusterizationStatus(std::move(clStatus), rdxCtx);
		}
		rdxCtx.WithNoWaitSync(ns->IsSystem(rdxCtx) || !clusterizator_->NamespaceIsInClusterConfig(nsDef.name));
		const int64_t stateToken = ns->NewItem(rdxCtx).GetStateToken();
		{
			auto wlck = nsLock_.DataWLock(rdxCtx);

			checkClusterRole(name, rdxCtx.GetOriginLSN());

			const lsn_t version = setNsVersion(ns, replOpts, rdxCtx);

			namespaces_.insert({nsDef.name, std::move(ns)});
			auto err = clusterizator_->Replicate(
				{UpdateRecord::Type::AddNamespace, nsDef.name, version, rdxCtx.EmmiterServerId(), std::move(nsDef), stateToken},
				[&wlck] {
					assertrx(wlck.isClusterLck());
					wlck.unlock();
				},
				rdxCtx);
			if (!err.ok()) {
				throw err;
			}
		}
	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

Error ReindexerImpl::TruncateNamespace(std::string_view nsName, const RdxContext& rdxCtx) {
	Error err;
	try {
		auto ns = getNamespace(nsName, rdxCtx);
		ns->Truncate(rdxCtx);
	} catch (const Error& e) {
		err = e;
	}
	if (rdxCtx.Compl()) rdxCtx.Compl()(err);
	return err;
}

Error ReindexerImpl::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const RdxContext& rdxCtx) {
	Error err;
	try {
		{
			auto rlck = nsLock_.RLock(rdxCtx);
			auto srcIt = namespaces_.find(srcNsName);
			if (srcIt == namespaces_.end()) {
				return Error(errParams, "Namespace '%s' doesn't exist", srcNsName);
			}
			Namespace::Ptr srcNs = srcIt->second;
			assertrx(srcNs != nullptr);

			if (srcNs->IsTemporary(rdxCtx) && rdxCtx.GetOriginLSN().isEmpty()) {
				return Error(errParams, "Can't rename temporary namespace '%s'", srcNsName);
			}
		}
		err = renameNamespace(srcNsName, dstNsName, !rdxCtx.GetOriginLSN().isEmpty(), false, rdxCtx);
	} catch (const Error& e) {
		return e;
	}
	return err;
}

Error ReindexerImpl::SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::SetTagsMatcher>(nsName, ctx, std::move(tm));
}

void ReindexerImpl::ShutdownCluster() { clusterizator_->Stop(true); }

Error ReindexerImpl::renameNamespace(std::string_view srcNsName, const std::string& dstNsName, bool fromReplication, bool skipResync,
									 const RdxContext& rdxCtx) {
	Namespace::Ptr dstNs, srcNs;
	try {
		if (std::string_view(dstNsName) == srcNsName) return errOK;
		const char kSystemNamespacePrefix = '#';
		if (!srcNsName.empty() && srcNsName[0] == kSystemNamespacePrefix) {
			return Error(errParams, "Can't rename system namespace (%s)", srcNsName);
		}
		if (dstNsName.empty()) {
			return Error(errParams, "Can't rename namespace to empty name");
		}
		if (!validateObjectName(dstNsName, false)) {
			return Error(errParams, "Namespace name contains invalid character. Only alphas, digits,'_','-', are allowed (%s)", dstNsName);
		}

		if (dstNsName[0] == kSystemNamespacePrefix) {
			return Error(errParams, "Can't rename to system namespace name (%s)", dstNsName);
		}

		rdxCtx.WithNoWaitSync(fromReplication);

		auto wlck = nsLock_.DataWLock(rdxCtx);

		checkClusterRole(srcNsName, rdxCtx.GetOriginLSN());

		auto srcIt = namespaces_.find(srcNsName);
		if (srcIt == namespaces_.end()) {
			return Error(errParams, "Namespace '%s' doesn't exist", srcNsName);
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
			replicateCb = [/*this, &srcNsName, &dstNsName, &rdxCtx, &wlck*/](std::function<void()> /*unlockCb*/) {
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
			};
		} else if (!skipResync) {
			replicateCb = [this, &dstNsName, &rdxCtx](std::function<void()>) {
				auto err = clusterizator_->ReplicateAsync(
					{UpdateRecord::Type::ResyncNamespaceGeneric, dstNsName, lsn_t(0, 0), lsn_t(0, 0), rdxCtx.EmmiterServerId()}, rdxCtx);
				if (!err.ok()) {
					throw err;
				}
			};
		}
		try {
			if (dstNs) {
				srcNs->Rename(dstNs, storagePath_, std::move(replicateCb), rdxCtx);
			} else {
				srcNs->Rename(dstNsName, storagePath_, std::move(replicateCb), rdxCtx);
			}
		} catch (...) {
			auto actualName = srcNs->GetName(rdxCtx);
			if (actualName != dstNsName) {
				namespaces_.erase(dstNsName);
				namespaces_[actualName] = srcNs;
			}
			throw;
		}
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::readClusterConfigFile() {
	auto path = fs::JoinPath(storagePath_, kClusterConfFilename);
	std::string content;
	auto res = fs::ReadFile(path, content);
	if (res < 0) {
		return Error();
	}
	cluster::ClusterConfigData conf;
	Error err = conf.FromYML(content);
	if (err.ok()) {
		std::unique_ptr<cluster::ClusterConfigData> confPtr(new cluster::ClusterConfigData(std::move(conf)));
		if (clusterConfig_.compare_exchange_strong(nullptr, confPtr.get())) {
			if (config_.raftCluster) {
				confPtr.release();
				clusterizator_->Configure(*clusterConfig_);
				if (clusterizator_->IsExpectingClusterStartup()) {
					logPrintf(LogTrace, "%s: Starting clusterizator right after config read", storagePath_);
					err = clusterizator_->StartClusterRepl();
					if (!err.ok()) {
						logPrintf(LogError, "Error on cluster start: %s", err.what());
					}
				}
			} else {
				logPrintf(LogWarning, "%s: RAFT-cluster config found, hovewer cluster is disabled in DB config", storagePath_);
			}
		}
	} else {
		logPrintf(LogError, "Error parsing cluster config YML: %s", err.what());
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
	Error err = conf.FromYML(content);
	if (err.ok()) {
		std::unique_ptr<cluster::ShardingConfig> confPtr(new cluster::ShardingConfig(std::move(conf)));
		if (shardingConfig_.compare_exchange_strong(nullptr, confPtr.get())) {
			confPtr.release();
		}
	} else {
		logPrintf(LogError, "Error parsing sharding config YML: %s", err.what());
	}
	return err;
}

void ReindexerImpl::checkClusterRole(std::string_view nsName, lsn_t originLsn) const {
	if (!clusterizator_->NamespaceIsInClusterConfig(nsName)) {
		return;
	}

	switch (clusterStatus_.role) {
		case ClusterizationStatus::Role::None:
			if (!originLsn.isEmpty()) {
				throw Error(errWrongReplicationData, "Can't modify database with 'None' replication status from node %d",
							originLsn.Server());
			}
			break;
		case ClusterizationStatus::Role::SimpleReplica:
			assertrx(false);  // This role is unavailable for database
			break;
		case ClusterizationStatus::Role::ClusterReplica:
			if (originLsn.isEmpty() || originLsn.Server() != clusterStatus_.leaderId) {
				throw Error(errWrongReplicationData, "Can't modify cluster database replica with incorrect origin LSN: (%d) (s1:%d s2:%d)",
							originLsn, originLsn.Server(), clusterStatus_.leaderId);
			}
			break;
	}
}

void ReindexerImpl::setClusterizationStatus(ClusterizationStatus&& status, const RdxContext& ctx) {
	auto wlck = nsLock_.SimpleWLock(ctx);
	clusterStatus_ = std::move(status);
}

template <bool needUpdateSystemNs, typename MemFnType, MemFnType Namespace::*MemFn, typename Arg, typename... Args>
Error ReindexerImpl::applyNsFunction(std::string_view nsName, const RdxContext& rdxCtx, Arg arg, Args&&... args) {
	Error err;
	try {
		auto ns = getNamespace(nsName, rdxCtx);
		(*ns.*MemFn)(arg, std::forward<Args>(args)..., rdxCtx);
		if constexpr (needUpdateSystemNs) {
			rdxCtx.WithNoWaitSync(true);
			updateToSystemNamespace(nsName, arg, rdxCtx);
		}
	} catch (const Error& e) {
		err = e;
	}
	if (rdxCtx.Compl()) rdxCtx.Compl()(err);
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
	if (rdxCtx.Compl()) rdxCtx.Compl()(err);
	return err;
}

#define APPLY_NS_FUNCTION1(needUpdateSys, memFn, arg) \
	return applyNsFunction<needUpdateSys, void(decltype(arg), const RdxContext&), &Namespace::memFn, decltype(arg)>(nsName, ctx, arg)

#define APPLY_NS_FUNCTION2(needUpdateSys, memFn, arg1, arg2)                                                                          \
	return applyNsFunction<needUpdateSys, void(decltype(arg1), decltype(arg2), const RdxContext&), &Namespace::memFn, decltype(arg1), \
						   decltype(arg2)>(nsName, ctx, arg1, arg2)

Error ReindexerImpl::Insert(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(true, Insert, item); }

Error ReindexerImpl::Insert(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(true, Insert, item, qr);
}

Error ReindexerImpl::Update(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(true, Update, item); }

Error ReindexerImpl::Update(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(true, Update, item, qr);
}

Error ReindexerImpl::Update(const Query& q, LocalQueryResults& result, const RdxContext& rdxCtx) {
	try {
		auto ns = getNamespace(q._namespace, rdxCtx);
		ns->Update(q, result, rdxCtx);
		if (ns->IsSystem(rdxCtx)) {
			const std::string kNsName = ns->GetName(rdxCtx);
			rdxCtx.WithNoWaitSync(true);
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

Error ReindexerImpl::GetMeta(std::string_view nsName, const string& key, string& data, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::GetMeta>(nsName, ctx, data, key);
}

Error ReindexerImpl::PutMeta(std::string_view nsName, const string& key, std::string_view data, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::PutMeta>(nsName, ctx, key, data);
}

Error ReindexerImpl::EnumMeta(std::string_view nsName, vector<string>& keys, const RdxContext& ctx) {
	return applyNsFunction<&Namespace::EnumMeta>(nsName, ctx, keys);
}

Error ReindexerImpl::Delete(std::string_view nsName, Item& item, const RdxContext& ctx) { APPLY_NS_FUNCTION1(false, Delete, item); }

Error ReindexerImpl::Delete(std::string_view nsName, Item& item, LocalQueryResults& qr, const RdxContext& ctx) {
	APPLY_NS_FUNCTION2(false, Delete, item, qr);
}

Error ReindexerImpl::Delete(const Query& q, LocalQueryResults& result, const RdxContext& ctx) {
	std::string_view nsName = q.Namespace();
	APPLY_NS_FUNCTION2(false, Delete, q, result);
}

Error ReindexerImpl::Select(std::string_view query, LocalQueryResults& result, const RdxContext& ctx) {
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

Error ReindexerImpl::Select(const Query& q, LocalQueryResults& result, const RdxContext& rdxCtx) {
	try {
		NsLocker<const RdxContext> locks(rdxCtx);

		auto mainNsWrp = getNamespace(q._namespace, rdxCtx);
		auto mainNs = q.IsWALQuery() ? mainNsWrp->awaitMainNs(rdxCtx) : mainNsWrp->getMainNs();

		ProfilingConfigData profilingCfg = configProvider_.GetProfilingConfig();
		PerfStatCalculatorMT calc(mainNs->selectPerfCounter_, mainNs->enablePerfCounters_);	 // todo more accurate detect joined queries
		auto& tracker = queriesStatTracker_;
		WrSerializer normalizedSQL, nonNormalizedSQL;
		if (profilingCfg.queriesPerfStats) {
			q.GetSQL(normalizedSQL, true);

			if (rdxCtx.Activity()) {
				q.GetSQL(nonNormalizedSQL, false);
			}
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
			if (q.entries.Size() == 1 && q.entries.HoldsOrReferTo<QueryEntry>(0)) {
				const QueryEntry entry = q.entries.Get<QueryEntry>(0);
				if (entry.condition == CondEq && entry.values.size() == 1) filterNsName = entry.values[0].As<string>();
			}

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

		const auto ward = rdxCtx.BeforeSimpleState(Activity::InProgress);
		SelectFunctionsHolder func;
		doSelect(q, result, locks, func, rdxCtx);
		func.Process(result);
	} catch (const Error& err) {
		if (rdxCtx.Compl()) rdxCtx.Compl()(err);
		return err;
	}
	if (rdxCtx.Compl()) rdxCtx.Compl()(errOK);
	return errOK;
}

struct ReindexerImpl::QueryResultsContext {
	QueryResultsContext() = default;
	QueryResultsContext(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet& fieldsFilter, std::shared_ptr<const Schema> schema)
		: type_(type), tagsMatcher_(tagsMatcher), fieldsFilter_(fieldsFilter), schema_(schema) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsSet fieldsFilter_;
	std::shared_ptr<const Schema> schema_;
};

bool ReindexerImpl::isPreResultValuesModeOptimizationAvailable(const Query& jItemQ, const NamespaceImpl::Ptr& jns) {
	bool result = true;
	jItemQ.entries.ExecuteAppropriateForEach(
		Skip<JoinQueryEntry, QueryEntriesBracket, AlwaysFalse>{},
		[&jns, &result](const QueryEntry& qe) {
			if (qe.idxNo >= 0) {
				assertrx(jns->indexes_.size() > static_cast<size_t>(qe.idxNo));
				const IndexType indexType = jns->indexes_[qe.idxNo]->Type();
				if (IsComposite(indexType) || IsFullText(indexType)) result = false;
			}
		},
		[&jns, &result](const BetweenFieldsQueryEntry& qe) {
			if (qe.firstIdxNo >= 0) {
				assertrx(jns->indexes_.size() > static_cast<size_t>(qe.firstIdxNo));
				const IndexType indexType = jns->indexes_[qe.firstIdxNo]->Type();
				if (IsComposite(indexType) || IsFullText(indexType)) result = false;
			}
			if (qe.secondIdxNo >= 0) {
				assertrx(jns->indexes_.size() > static_cast<size_t>(qe.secondIdxNo));
				if (IsComposite(jns->indexes_[qe.secondIdxNo]->Type())) result = false;
			}
		});
	return result;
}

void ReindexerImpl::prepareJoinResults(const Query& q, LocalQueryResults& result) {
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
JoinedSelectors ReindexerImpl::prepareJoinedSelectors(const Query& q, LocalQueryResults& result, NsLocker<T>& locks,
													  SelectFunctionsHolder& func, vector<QueryResultsContext>& queryResultsContexts,
													  const RdxContext& rdxCtx) {
	JoinedSelectors joinedSelectors;
	if (q.joinQueries_.empty()) return joinedSelectors;
	auto ns = locks.Get(q._namespace);
	assertrx(ns);

	// For each joined queries
	int joinedSelectorsCount = q.joinQueries_.size();
	for (auto& jq : q.joinQueries_) {
		// Get common results from joined namespaces_
		auto jns = locks.Get(jq._namespace);
		assertrx(jns);

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
			QueryEntry qe(InvertJoinCondition(je.condition_), je.joinIndex_, joinIdx);
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
			LocalQueryResults jr;
			jjq.Limit(UINT_MAX);
			SelectCtx ctx(jjq, &q);
			ctx.preResult = preResult;
			ctx.preResult->executionMode = JoinPreResult::ModeBuild;
			ctx.preResult->enableStoredValues = isPreResultValuesModeOptimizationAvailable(jItemQ, jns);
			ctx.functions = &func;
			ctx.requiresCrashTracking = true;
			jns->Select(jr, ctx, rdxCtx);
			assertrx(ctx.preResult->executionMode == JoinPreResult::ModeExecute);
		}
		if (joinRes.haveData) {
			preResult = joinRes.it.val.preResult;
		} else if (joinRes.needPut) {
			jns->putToJoinCache(joinRes, preResult);
		}

		queryResultsContexts.emplace_back(jns->payloadType_, jns->tagsMatcher_, FieldsSet(jns->tagsMatcher_, jq.selectFilter_),
										  jns->schema_);

		result.AddNamespace(jns, true, rdxCtx);
		if (preResult->dataMode == JoinPreResult::ModeValues) {
			jItemQ.entries.ExecuteAppropriateForEach(
				Skip<JoinQueryEntry, QueryEntriesBracket, AlwaysFalse>{},
				[&jns](QueryEntry& qe) {
					if (qe.idxNo != IndexValueType::SetByJsonPath) {
						assertrx(qe.idxNo >= 0 && static_cast<size_t>(qe.idxNo) < jns->indexes_.size());
						if (jns->indexes_[qe.idxNo]->Opts().IsSparse()) qe.idxNo = IndexValueType::SetByJsonPath;
					}
				},
				[&jns](BetweenFieldsQueryEntry& qe) {
					if (qe.firstIdxNo != IndexValueType::SetByJsonPath) {
						assertrx(qe.firstIdxNo >= 0 && static_cast<size_t>(qe.firstIdxNo) < jns->indexes_.size());
						if (jns->indexes_[qe.firstIdxNo]->Opts().IsSparse()) qe.firstIdxNo = IndexValueType::SetByJsonPath;
					}
					if (qe.secondIdxNo != IndexValueType::SetByJsonPath) {
						assertrx(qe.secondIdxNo >= 0 && static_cast<size_t>(qe.secondIdxNo) < jns->indexes_.size());
						if (jns->indexes_[qe.secondIdxNo]->Opts().IsSparse()) qe.secondIdxNo = IndexValueType::SetByJsonPath;
					}
				});
			if (!preResult->values.Locked()) preResult->values.Lock();	// If not from cache
			locks.Delete(jns);
			jns.reset();
		}
		joinedSelectors.emplace_back(jq.joinType, ns, std::move(jns), std::move(joinRes), std::move(jItemQ), result, jq, preResult,
									 joinedFieldIdx, func, joinedSelectorsCount, false, rdxCtx);
		ThrowOnCancel(rdxCtx);
	}
	return joinedSelectors;
}

template <typename T>
void ReindexerImpl::doSelect(const Query& q, LocalQueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func,
							 const RdxContext& ctx) {
	auto ns = locks.Get(q._namespace);
	assertrx(ns);
	if (!ns) {
		throw Error(errParams, "Namespace '%s' is not exists", q._namespace);
	}
	vector<QueryResultsContext> joinQueryResultsContexts;
	// should be destroyed after results.lockResults()
	JoinedSelectors mainJoinedSelectors = prepareJoinedSelectors(q, result, locks, func, joinQueryResultsContexts, ctx);
	prepareJoinResults(q, result);
	{
		SelectCtx selCtx(q, nullptr);
		selCtx.joinedSelectors = mainJoinedSelectors.size() ? &mainJoinedSelectors : nullptr;
		selCtx.contextCollectingMode = true;
		selCtx.functions = &func;
		selCtx.nsid = 0;
		selCtx.isForceAll = !q.mergeQueries_.empty();
		selCtx.requiresCrashTracking = true;
		ns->Select(result, selCtx, ctx);
		result.AddNamespace(ns, true, ctx);
	}

	// should be destroyed after results.lockResults()
	vector<JoinedSelectors> mergeJoinedSelectors;
	if (!q.mergeQueries_.empty()) {
		mergeJoinedSelectors.reserve(q.mergeQueries_.size());
		uint8_t counter = 0;

		for (auto& mq : q.mergeQueries_) {
			auto mns = locks.Get(mq._namespace);
			assertrx(mns);
			SelectCtx mctx(mq, &q);
			mctx.nsid = ++counter;
			mctx.isForceAll = true;
			mctx.functions = &func;
			mctx.contextCollectingMode = true;
			mergeJoinedSelectors.emplace_back(prepareJoinedSelectors(mq, result, locks, func, joinQueryResultsContexts, ctx));
			mctx.joinedSelectors = mergeJoinedSelectors.back().size() ? &mergeJoinedSelectors.back() : nullptr;
			mctx.requiresCrashTracking = true;

			mns->Select(result, mctx, ctx);
			result.AddNamespace(mns, true, ctx);
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
	// Adding context to LocalQueryResults
	for (const auto& jctx : joinQueryResultsContexts) result.addNSContext(jctx.type_, jctx.tagsMatcher_, jctx.fieldsFilter_, jctx.schema_);
}

template void ReindexerImpl::doSelect(const Query&, LocalQueryResults&, NsLocker<RdxContext>&, SelectFunctionsHolder&, const RdxContext&);

Error ReindexerImpl::Commit(std::string_view /*_namespace*/) {
	try {
		// getNamespace(_namespace)->FlushStorage();

	} catch (const Error& err) {
		return err;
	}

	return errOK;
}

std::set<std::string> ReindexerImpl::getFTIndexes(std::string_view nsName) {
	InternalRdxContext ctx;
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
	InternalRdxContext ctx;
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
		throw Error(errNotFound, "Namespace '%s' does not exist", nsName);
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
				throw Error(errWrongReplicationData, "Namespace version missmatch. Expected version: %s, actual version: %s",
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

std::vector<string> ReindexerImpl::getNamespacesNames(const RdxContext& ctx) {
	auto rlck = nsLock_.RLock(ctx);
	std::vector<string> ret;
	ret.reserve(namespaces_.size());
	for (auto& ns : namespaces_) ret.push_back(ns.first);
	return ret;
}

Error ReindexerImpl::EnumNamespaces(vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const RdxContext& rdxCtx) {
	logPrintf(LogTrace, "ReindexerImpl::EnumNamespaces (%d,%s)", opts.options_, opts.filter_);
	try {
		auto nsarray = getNamespaces(rdxCtx);
		for (auto& nspair : nsarray) {
			if (!opts.MatchFilter(nspair.first, nspair.second, rdxCtx)) continue;
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
				if (d.isDir && d.name != "." && d.name != ".." && opts.MatchNameFilter(d.name)) {
					{
						auto rlck = nsLock_.RLock(rdxCtx);
						if (namespaces_.find(d.name) != namespaces_.end()) continue;
					}
					assertrx(clusterizator_);
					std::unique_ptr<NamespaceImpl> tmpNs{new NamespaceImpl(d.name, {}, clusterizator_.get())};
					try {
						tmpNs->EnableStorage(storagePath_, StorageOpts(), storageType_, rdxCtx);
						if (opts.IsHideTemporary() && tmpNs->IsTemporary(rdxCtx)) {
							continue;
						}
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
	auto nsBackground = [&]() {
		auto nsarray = getNamespacesNames(dummyCtx);
		for (auto name : nsarray) {
			try {
				auto ns = getNamespace(name, dummyCtx);
				ns->BackgroundRoutine(nullptr);
			} catch (Error err) {
				logPrintf(LogWarning, "backgroundRoutine() failed with ns '%s': %s", name, err.what());
			} catch (...) {
				logPrintf(LogWarning, "backgroundRoutine() failed with ns '%s': unknown exception", name);
			}
		}
		for (auto& watcher : configWatchers_) {
			watcher.Check();
		}
	};

	while (!stopBackgroundThreads_) {
		nsBackground();
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	nsBackground();
}

void ReindexerImpl::storageFlushingRoutine() {
	static const RdxContext dummyCtx;
	auto nsFlush = [&]() {
		auto nsarray = getNamespacesNames(dummyCtx);
		for (auto name : nsarray) {
			try {
				auto ns = getNamespace(name, dummyCtx);
				ns->StorageFlushingRoutine();
			} catch (Error err) {
				logPrintf(LogWarning, "storageFlushingRoutine() failed with ns '%s': %s", name, err.what());
			} catch (...) {
				logPrintf(LogWarning, "storageFlushingRoutine() failed with ns '%s': unknown exception", name);
			}
		}
	};

	while (!stopBackgroundThreads_) {
		nsFlush();
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	nsFlush();
}

void ReindexerImpl::createSystemNamespaces() {
	for (const auto& nsDef : kSystemNsDefs) {
		AddNamespace(nsDef);
	}
}

Error ReindexerImpl::tryLoadShardingConf() {
	Item item = NewItem(kConfigNamespace, RdxContext());
	if (!item.Status().ok()) return item.Status();
	WrSerializer ser;
	{
		JsonBuilder jb{ser};
		jb.Put("type", kShardingConfigType);
		auto shardCfgObj = jb.Object(kShardingConfigType);
		shardingConfig_->GetJson(shardCfgObj);
	}
	const Error err = item.FromJSON(ser.Slice());
	if (!err.ok()) return err;
	return Insert(kConfigNamespace, item, RdxContext());
}

Error ReindexerImpl::InitSystemNamespaces() {
	createSystemNamespaces();

	LocalQueryResults results;
	auto err = Select(Query(kConfigNamespace), results, RdxContext());
	if (!err.ok()) return err;

	bool hasBaseReplicationConfig = false;
	bool hasAsyncReplicationConfig = false;
	bool hasShardingConfig = false;
	if (results.Count() == 0) {
		// Set default config
		for (const auto& conf : kDefDBConfig) {
			if (!hasBaseReplicationConfig || !hasAsyncReplicationConfig || !hasShardingConfig) {
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(std::string_view(conf));
				const std::string_view type = configJson["type"].As<std::string_view>();
				if (type == kReplicationConfigType) {
					hasBaseReplicationConfig = true;
					err = tryLoadConfFromFile<kReplicationConfigType, ReplicationConfigData>(kReplicationConfFilename);
					if (err.ok()) {
						continue;
					} else if (err.code() != errNotFound) {
						return Error(err.code(), "Failed to load general replication config file: '%s'", err.what());
					}
				} else if (type == kAsyncReplicationConfigType) {
					hasAsyncReplicationConfig = true;
					err = tryLoadConfFromFile<kAsyncReplicationConfigType, cluster::AsyncReplConfigData>(kAsyncReplicationConfFilename);
					if (err.ok()) {
						continue;
					} else if (err.code() != errNotFound) {
						return Error(err.code(), "Failed to load async replication config file: '%s'", err.what());
					}
				} else if (type == kShardingConfigType && shardingConfig_) {
					hasShardingConfig = true;
					// Ignore error check here (it always returns fake error for now)
					if (tryLoadShardingConf().ok()) {
						continue;
					}
				}
			}

			Item item = NewItem(kConfigNamespace, RdxContext());
			if (!item.Status().ok()) return item.Status();
			err = item.FromJSON(conf);
			if (!err.ok()) return err;
			err = Insert(kConfigNamespace, item, RdxContext());
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

	if (!hasBaseReplicationConfig) {
		tryLoadConfFromFile<kReplicationConfigType, ReplicationConfigData>(kReplicationConfFilename);
	}
	if (!hasAsyncReplicationConfig) {
		tryLoadConfFromFile<kAsyncReplicationConfigType, cluster::AsyncReplConfigData>(kAsyncReplicationConfFilename);
	}
	if (!hasShardingConfig && shardingConfig_) {
		tryLoadShardingConf();
	}

	nsVersion_.SetServer(configProvider_.GetReplicationConfig().serverID);

	return errOK;
}

template <char const* type, typename ConfigT>
Error ReindexerImpl::tryLoadConfFromFile(const std::string& filename) {
	std::string yamlConf;
	int res = fs::ReadFile(fs::JoinPath(storagePath_, filename), yamlConf);
	if (res > 0) {
		return tryLoadConfFromYAML<type, ConfigT>(yamlConf);
	}
	return Error(errNotFound);
}

template <char const* type, typename ConfigT>
Error ReindexerImpl::tryLoadConfFromYAML(const std::string& yamlConf) {
	if (yamlConf.empty()) {
		return Error(errNotFound);
	}

	ConfigT config;
	Error err = config.FromYML(yamlConf);
	if (!err.ok()) {
		logPrintf(LogError, "Error parsing config YML for %s: %s", type, err.what());
		return Error(errParams, "Error parsing replication config YML for %s: %s", type, err.what());
	} else {
		WrSerializer ser;
		JsonBuilder jb(ser);
		jb.Put("type", type);
		auto jsonNode = jb.Object(type);
		config.GetJSON(jsonNode);
		jsonNode.End();
		jb.End();

		Item item = NewItem(kConfigNamespace, RdxContext());
		if (!item.Status().ok()) {
			return item.Status();
		}
		err = item.FromJSON(ser.Slice());
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
			bool isChangedActivityStats = configProvider_.GetProfilingConfig().activityStats;
			updateConfigProvider(configJson);
			isChangedActivityStats = isChangedActivityStats != configProvider_.GetProfilingConfig().activityStats;
			if (isChangedActivityStats) {
				activities_.Reset();
			}

			if (!configJson[kReplicationConfigType].empty()) {
				auto replConf = configProvider_.GetReplicationConfig();
				{
					auto wlck = nsLock_.SimpleWLock(ctx);
					nsVersion_.SetServer(replConf.serverID);
				}
				updateConfFile(replConf, kReplicationConfFilename);
				clusterizator_->Configure(std::move(replConf));
			}
			if (!configJson[kAsyncReplicationConfigType].empty()) {
				auto asyncReplConf = configProvider_.GetAsyncReplicationConfig();
				updateConfFile(asyncReplConf, kAsyncReplicationConfFilename);
				clusterizator_->Configure(std::move(asyncReplConf));
			}
			if (!configJson[kShardingConfigType].empty()) {
				throw Error(errLogic, "Sharding configuration cannot be updated");
			}

			auto namespaces = getNamespaces(ctx);
			for (auto& ns : namespaces) {
				ns.second->OnConfigUpdated(configProvider_, ctx);
			}
			auto& actionNode = configJson["action"sv];
			if (!actionNode.empty()) {
				std::string_view command = actionNode["command"].As<std::string_view>();
				if (command == "set_leader_node"sv) {
					int newLeaderId = actionNode["server_id"].As<int>(-1);
					if (newLeaderId == -1) {
						throw Error(errLogic, "Expecting 'server_id' in 'set_leader_node' command");
					}
					cluster::RaftInfo info = clusterizator_->GetRaftInfo(false, ctx);  // current node leader or follower
					if (info.leaderId == newLeaderId) {
						return;
					}

					clusterConfig_->GetNodeIndexForServerId(newLeaderId);  // check if nextServerId in config (on error throw)

					auto err = clusterizator_->SetDesiredLeaderId(newLeaderId, true);
					if (!err.ok()) {
						throw err;
					}

				} else if (command == "restart_replication"sv) {
					clusterizator_->StopAsyncRepl();
				} else if (command == "reset_replication_role"sv) {
					std::string_view name = actionNode["namespace"].As<std::string_view>();
					if (name.empty()) {
						for (auto& ns : namespaces) {
							if (!clusterizator_->NamespaceIsInClusterConfig(ns.first)) {
								ns.second->SetClusterizationStatus(ClusterizationStatus(), ctx);
							}
						}
					} else {
						if (clusterizator_->NamespaceIsInClusterConfig(name)) {
							throw Error(errLogic, "Role of the cluster namespace may not be reseted");
						}
						auto ns = getNamespaceNoThrow(name, ctx);
						if (ns) {
							ns->SetClusterizationStatus(ClusterizationStatus(), ctx);
						}
					}
				}
			}
			if (replicationEnabled_ && !stopBackgroundThreads_ && clusterizator_->IsExpectingAsyncReplStartup()) {
				if (Error err = clusterizator_->StartAsyncRepl()) throw err;
			}
			if (replicationEnabled_ && !stopBackgroundThreads_ && clusterizator_->IsExpectingClusterStartup()) {
				if (Error err = clusterizator_->StartClusterRepl()) throw err;
			}
		} catch (gason::Exception& e) {
			throw Error(errParseJson, "JSON parsing error: %s", e.what());
		}
	} else if (nsName == kQueriesPerfStatsNamespace) {
		queriesStatTracker_.Reset();
	} else if (nsName == kPerfStatsNamespace) {
		for (auto& ns : getNamespaces(ctx)) ns.second->ResetPerfStat(ctx);
	} else if (nsName == kClusterConfigNamespace) {
		//  TODO: reconfigure clusterization
		// Separate namespace is required, because it has to be replicated into all cluster nodes
		// and other system namespaces are not replicatable
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

template <typename ConfigT>
void ReindexerImpl::updateConfFile(const ConfigT& newConf, std::string_view filename) {
	WrSerializer ser;
	newConf.GetYAML(ser);
	for (auto& watcher : configWatchers_) {
		if (watcher.Filename() != filename) {
			continue;
		}
		auto err = watcher.RewriteFile(std::string(ser.Slice()), [&newConf](const string& content) {
			ConfigT config;
			Error err = config.FromYML(content);
			if (err.ok()) {
				return config == newConf;
			}
			return false;
		});
		if (!err.ok()) {
			throw err;
		}
		break;
	}
}

void ReindexerImpl::syncSystemNamespaces(std::string_view sysNsName, std::string_view filterNsName, const RdxContext& ctx) {
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
		sysNs->Refill(items, ctx);
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
		queriesperfstatsNs->Refill(items, ctx);
	}

	if (sysNsName == kActivityStatsNamespace) {
		const auto data = activities_.List(configProvider_.GetReplicationConfig().serverID);
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
		activityNs->Refill(items, ctx);
	}
	if (sysNsName == kClientsStatsNamespace) {
		if (config_.clientsStats) {
			std::vector<ClientStat> clientInf;
			config_.clientsStats->GetClientInfo(clientInf);
			auto clientsNs = getNamespace(kClientsStatsNamespace, ctx);
			std::vector<Item> items;
			items.reserve(clientInf.size());
			for (auto& i : clientInf) {
				ser.Reset();
				if (auto query = activities_.QueryForIpConnection(i.connectionId); query) {
					i.currentActivity = std::move(*query);
				}
				i.GetJSON(ser);
				items.emplace_back(clientsNs->NewItem(ctx));
				auto err = items.back().FromJSON(ser.Slice());
				if (!err.ok()) throw err;
			}
			clientsNs->Refill(items, ctx);
		}
	}
	if (sysNsName == kReplicationStatsNamespace) {
		if (clusterizator_) {
			std::vector<Item> items;
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
				if (!err.ok()) throw err;
			}
			replStatsNs->Refill(items, ctx);
		}
	}
}

void ReindexerImpl::onProfiligConfigLoad() {
	LocalQueryResults qr1, qr2, qr3;
	Delete(Query(kMemStatsNamespace), qr2, RdxContext());
	Delete(Query(kQueriesPerfStatsNamespace), qr3, RdxContext());
	Delete(Query(kPerfStatsNamespace), qr1, RdxContext());
}

Error ReindexerImpl::GetSqlSuggestions(const std::string_view sqlQuery, int pos, vector<string>& suggestions, const RdxContext& rdxCtx) {
	Query query;
	SQLSuggester suggester(query);
	std::vector<NamespaceDef> nses;

	suggestions = suggester.GetSuggestions(
		sqlQuery, pos,
		[&, this](EnumNamespacesOpts opts) {
			EnumNamespaces(nses, opts, rdxCtx);
			return nses;
		},
		[&rdxCtx, this](std::string_view ns) {
			auto nsPtr = getNamespaceNoThrow(ns, rdxCtx);
			if (nsPtr) {
				return nsPtr->getMainNs()->GetSchemaPtr(rdxCtx);
			}
			return std::shared_ptr<const Schema>();
		});
	return errOK;
}

Error ReindexerImpl::GetProtobufSchema(WrSerializer& ser, vector<string>& namespaces) {
	struct NsInfo {
		std::string nsName, objName;
		int nsNumber;
	};

	std::vector<NsInfo> nses;
	for (const string& ns : namespaces) {
		nses.push_back({ns, "", 0});
	}

	ser << "// Autogenerated by reindexer server - do not edit!\n";
	SchemaFieldsTypes fieldsTypes;
	ProtobufSchemaBuilder schemaBuilder(&ser, &fieldsTypes, ObjType::TypePlain);

	const std::string_view kMessage = "message"sv;

	for (auto& ns : nses) {
		std::string nsProtobufSchema;
		Error status = GetSchema(ns.nsName, ProtobufSchemaType, nsProtobufSchema, RdxContext());
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
		LocalQueryResults qr;
		status = Select(Query(ns.nsName).Limit(0), qr, RdxContext());
		if (!status.ok()) return status;
		ns.nsNumber = qr.getNsNumber(0) + 1;
	}

	ser << "// Possible item schema variants in LocalQueryResults or in ModifyResults\n";
	schemaBuilder.Object(0, "ItemsUnion", false, [&](ProtobufSchemaBuilder& obj) {
		ser << "oneof item {\n";
		for (auto& ns : nses) {
			obj.Field(ns.nsName, ns.nsNumber, FieldProps{KeyValueTuple, false, false, false, ns.objName});
		}
		ser << "}\n";
	});

	ser << "// The LocalQueryResults message is schema of http API methods response:\n";
	ser << "// - GET api/v1/db/:db/namespaces/:ns/items\n";
	ser << "// - GET/POST api/v1/db/:db/query\n";
	ser << "// - GET/POST api/v1/db/:db/sqlquery\n";
	schemaBuilder.Object(0, "QueryResults", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamItems, kProtoQueryResultsFields.at(kParamItems), FieldProps{KeyValueTuple, true, false, false, "ItemsUnion"});
		obj.Field(kParamNamespaces, kProtoQueryResultsFields.at(kParamNamespaces), FieldProps{KeyValueString, true});
		obj.Field(kParamCacheEnabled, kProtoQueryResultsFields.at(kParamCacheEnabled), FieldProps{KeyValueBool});
		obj.Field(kParamExplain, kProtoQueryResultsFields.at(kParamExplain), FieldProps{KeyValueString});
		obj.Field(kParamTotalItems, kProtoQueryResultsFields.at(kParamTotalItems), FieldProps{KeyValueInt});
		obj.Field(kParamQueryTotalItems, kProtoQueryResultsFields.at(kParamQueryTotalItems), FieldProps{KeyValueInt});

		obj.Object(kProtoQueryResultsFields.at(kParamColumns), "Columns", false, [](ProtobufSchemaBuilder& obj) {
			obj.Field(kParamName, kProtoColumnsFields.at(kParamName), FieldProps{KeyValueString});
			obj.Field(kParamWidthPercents, kProtoColumnsFields.at(kParamWidthPercents), FieldProps{KeyValueDouble});
			obj.Field(kParamMaxChars, kProtoColumnsFields.at(kParamMaxChars), FieldProps{KeyValueInt});
			obj.Field(kParamWidthChars, kProtoColumnsFields.at(kParamWidthChars), FieldProps{KeyValueInt});
		});

		obj.Field(kParamColumns, kProtoQueryResultsFields.at(kParamColumns), FieldProps{KeyValueTuple, true, false, false, "Columns"});

		AggregationResult::GetProtobufSchema(obj);
		obj.Field(kParamAggregations, kProtoQueryResultsFields.at(kParamAggregations),
				  FieldProps{KeyValueTuple, true, false, false, "AggregationResults"});
	});

	ser << "// The ModifyResults message is schema of http API methods response:\n";
	ser << "// - PUT/POST/DELETE api/v1/db/:db/namespaces/:ns/items\n";
	schemaBuilder.Object(0, "ModifyResults", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamItems, kProtoModifyResultsFields.at(kParamItems), FieldProps{KeyValueTuple, true, false, false, "ItemsUnion"});
		obj.Field(kParamUpdated, kProtoModifyResultsFields.at(kParamUpdated), FieldProps{KeyValueInt});
		obj.Field(kParamSuccess, kProtoModifyResultsFields.at(kParamSuccess), FieldProps{KeyValueBool});
	});

	ser << "// The ErrorResponse message is schema of http API methods response on error condition \n";
	ser << "// With non 200 http status code\n";
	schemaBuilder.Object(0, "ErrorResponse", false, [](ProtobufSchemaBuilder& obj) {
		obj.Field(kParamSuccess, kProtoErrorResultsFields.at(kParamSuccess), FieldProps{KeyValueBool});
		obj.Field(kParamResponseCode, kProtoErrorResultsFields.at(kParamResponseCode), FieldProps{KeyValueInt});
		obj.Field(kParamDescription, kProtoErrorResultsFields.at(kParamDescription), FieldProps{KeyValueString});
	});
	schemaBuilder.End();
	return errOK;
}

Error ReindexerImpl::GetReplState(std::string_view nsName, ReplicationStateV2& state, const RdxContext& rdxCtx) {
	try {
		if (nsName.size()) {
			state = getNamespace(nsName, rdxCtx)->GetReplStateV2(rdxCtx);
		} else {
			state.lastLsn = lsn_t();
			state.dataHash = 0;
			auto rlck = nsLock_.RLock(rdxCtx);
			state.clusterStatus = clusterStatus_;
		}
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus& status, const RdxContext& rdxCtx) {
	try {
		return getNamespace(nsName, rdxCtx)->SetClusterizationStatus(ClusterizationStatus(status), rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const RdxContext& rdxCtx) {
	try {
		getNamespace(nsName, rdxCtx)->GetSnapshot(snapshot, opts, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const RdxContext& rdxCtx) {
	try {
		getNamespace(nsName, rdxCtx)->ApplySnapshotChunk(ch, false, rdxCtx);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

Error ReindexerImpl::Status() {
	if (connected_.load(std::memory_order_acquire)) {
		return errOK;
	}
	return Error(errNotValid, "DB is not connected"sv);
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
	return errOK;
}

Error ReindexerImpl::ClusterControlRequest(const ClusterControlRequestData& request) {
	switch (request.type) {
		case ClusterControlRequestData::Type::ChangeLeader:
			return clusterizator_->SetDesiredLeaderId(std::get<SetClusterLeaderCommand>(request.data).leaderServerId, false);
		default:;
	}
	return Error(errParams, "Unknown cluster command request. Command type [%d].", int(request.type));
}

Error ReindexerImpl::GetLeaderDsn(std::string& dsn, unsigned short serverId, const cluster::RaftInfo& info) {
	try {
		if (!clusterConfig_ || !clusterizator_->Enabled()) {
			return Error(errLogic, "Cluster config not set.");
		}
		if (serverId == info.leaderId) {
			dsn.clear();
			return errOK;
		}
		for (const auto& node : clusterConfig_->nodes) {
			if (node.serverId == info.leaderId) {
				dsn = node.GetRPCDsn();
				return errOK;
			}
		}
	} catch (const Error& err) {
		return err;
	}
	return Error(errLogic, "Leader serverId is missing in the config.");
}

}  // namespace reindexer
