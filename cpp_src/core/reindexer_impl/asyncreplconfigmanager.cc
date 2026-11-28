#include "asyncreplconfigmanager.h"
#include <cstdlib>
#include <new>
#include "cluster/clustercontrolrequest.h"
#include "cluster/clusterizator.h"
#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "reindexerimpl.h"
#include "tools/jsontools.h"
#include "tools/logger.h"
#include "vendor/gason/gason.h"

namespace reindexer {

using namespace std::string_view_literals;

const std::string kAsyncReplConfigSourceKey = std::string(kConfigSourceMetaPrefix) + kAsyncReplicationConfigType;

AsyncReplConfigManager::AsyncReplConfigManager(ReindexerImpl& owner)
	: owner_(owner), source_{std::make_shared<const AsyncReplConfigSource>()} {}

AsyncReplConfigManager::Lock::Lock(AsyncReplConfigManager& owner, const RdxContext& ctx)
	: owner_(owner), lck_(owner.mtx_, std::defer_lock, ctx) {
	if (owner_.writer_.load(std::memory_order_relaxed) == std::this_thread::get_id()) {
		throw Error(errLogic, "Async replication config is already being changed by this request");
	}
	lck_.lock();
	owner_.writer_.store(std::this_thread::get_id(), std::memory_order_relaxed);
}

AsyncReplConfigManager::Lock::~Lock() { Unlock(); }

void AsyncReplConfigManager::Lock::Unlock() noexcept {
	if (lck_.owns_lock()) {
		owner_.publish();
		UnlockPublished();
	}
}

void AsyncReplConfigManager::Lock::UnlockPublished() noexcept {
	owner_.writer_.store(std::thread::id(), std::memory_order_relaxed);
	lck_.unlock();
}

// The cluster's part as it was received, including the fields, which are unknown for this node
static std::string actionClusterConfigJson(const gason::JsonNode& config) {
	WrSerializer ser;
	{
		JsonBuilder jb(ser);
		for (const auto& field : config) {
			if (const std::string_view key(field.key); key != "role"sv) {
				jb.Json(key, stringifyJson(field));
			}
		}
	}
	return std::string(ser.Slice());
}

// FIXME: #2713 Fix masking for the action
void AsyncReplConfigManager::HandleAction(const gason::JsonNode& action, cluster::RaftInfo::Role role, const RdxContext& ctx) {
	const auto& configNode = action["config"];
	if (configNode.isEmpty()) {
		throw Error(errParams, "Expecting 'config' object in 'apply_async_replication_config' command");
	}
	cluster::AsyncReplConfigData config;
	if (auto err = config.FromJSON(configNode); !err.ok()) {
		throw err;
	}
	AsyncReplConfigSource source{.config = actionClusterConfigJson(configNode)};
	if (role != cluster::RaftInfo::Role::Leader) {
		// ClusterProxy proxies the follower's action to the leader, so role cannot be 'follower' or 'candidate' here
		assertrx_dbg(role == cluster::RaftInfo::Role::None);
		if (owner_.clusterManager_->IsRaftCluster()) {
			throw Error(errLogic, "Command 'apply_async_replication_config' requires the RAFT leader, but the node's role is '{}'",
						cluster::RaftInfo::RoleToStr(role));
		}
		Lock lck(*this, ctx);
		applyClusterPartLocked(source, ctx);
		return;
	}
	// The initial sync may wait for the lock to apply the newest config of the cluster
	owner_.clusterManager_->AwaitInitialSync(ctx);
	Lock lck(*this, ctx);
	// The leadership may be switched, while awaiting the lock. Then the new initial sync may apply the older config after the action
	if (!owner_.clusterManager_->IsInitialSyncDone()) {
		throw Error(errUpdateReplication,
					"RAFT leader was switched during the command 'apply_async_replication_config'. Repeat the command");
	}
	int64_t lastSourceId = loadSource(ctx).sourceId;
	if (const auto rejected = owner_.clusterManager_->GetRejectedAsyncReplConfig();
		rejected && cluster::ConfigSourceId::IsNewer(rejected->sourceId, lastSourceId)) {
		lastSourceId = rejected->sourceId;
	}
	source.sourceId = cluster::ConfigSourceId::Generate(owner_.configProvider_.GetReplicationConfig().serverID, lastSourceId);
	// Is applied before the queueing, so the followers, which connect meanwhile, get it via the push
	applyClusterPartLocked(source, ctx);
	owner_.clusterManager_->ResetRejectedAsyncReplConfig();
	// The lock is released under the lock of the updates queue
	publish();
	UpdatesContainer recs;
	recs.emplace_back(updates::URType::ApplyAsyncReplicationConfig, ctx.EmitterServerId(), std::move(source.config), source.sourceId);
	Error err;
	{
		auto wlck = owner_.nsLock_.DataWLock(ctx, "");
		const auto replWard = ctx.BeforeSimpleState(Activity::WaitReplication);
		err = owner_.clusterManager_->Replicate(
			std::move(recs),
			[&wlck, &lck]() RX_RELEASE(lck) {
				assertrx(wlck.isClusterLck());
				wlck.unlock();
				lck.UnlockPublished();
			},
			ctx);
	}
	// The thread safety analysis does not see the release in the callback, so the lock is released explicitly. If the callback has not
	// been called, this publishes and reads #config, so the namespaces lock has to be released before
	lck.Unlock();
	if (!err.ok()) {
		throw err;
	}
}

void AsyncReplConfigManager::ApplyConfigItem(Item& item, const RdxContext& rdxCtx) {
	if (!item.GetID().IsValid()) {
		return;
	}
	const RdxContext ctx = rdxCtx.NoCancel();
	ctx.WithNoWaitSync(true);
	try {
		gason::JsonParser parser;
		const auto& config = parser.Parse(item.GetJSON());
		if (config["type"].As<std::string_view>() != kAsyncReplicationConfigType) {
			throw Error(errLogic, "Expecting the #config item of the type '{}'", kAsyncReplicationConfigType);
		}
		owner_.updateConfigProvider(config);
		if (!config[kAsyncReplicationConfigType].isEmpty()) {
			auto asyncReplConf = owner_.configProvider_.GetAsyncReplicationConfig();
			owner_.updateConfFile(asyncReplConf, kAsyncReplicationConfFilename);
			revokeSource(asyncReplConf, ctx);
			owner_.clusterManager_->Configure(std::move(asyncReplConf));
		}
		owner_.completeConfigUpdate(ctx);
	} catch (const gason::Exception& e) {
		throw Error(errParseJson, "JSON parsing error: {}", e.what());
	}
}

void AsyncReplConfigManager::upsertConfig(const cluster::AsyncReplConfigData& config, const RdxContext& ctx) {
	WrSerializer ser;
	{
		JsonBuilder jb(ser);
		jb.Put("type", kAsyncReplicationConfigType);
		auto configObj = jb.Object(kAsyncReplicationConfigType);
		config.GetJSON(configObj, cluster::MaskingDSN::Disabled);
	}
	Item item = owner_.NewItem(kConfigNamespace, ctx);
	if (!item.Status().ok()) {
		throw item.Status();
	}
	if (auto err = item.Unsafe().FromJSON(ser.Slice()); !err.ok()) {
		throw err;
	}
	owner_.getNamespace(kConfigNamespace, ctx)->Upsert(item, ctx);
	ApplyConfigItem(item, ctx);
}

void AsyncReplConfigManager::ApplyClusterPart(const AsyncReplConfigSource& source, const RdxContext& lockCtx) {
	Lock lck(*this, lockCtx);
	applyClusterPartLocked(source, RdxContext());
}

void AsyncReplConfigManager::applyClusterPartLocked(const AsyncReplConfigSource& source, const RdxContext& ctx) {
	cluster::AsyncReplConfigData config;
	if (auto err = config.FromJSON(source.config); !err.ok()) {
		throw err;
	}
	const auto current = owner_.configProvider_.GetAsyncReplicationConfig();
	config.role = current.role;
	// The source is saved before the config, so applying of the config does not revoke it. The config without the source is applied
	// as the local change
	if (source.sourceId != ShardingSourceId::NotSet) {
		if (const auto saved = loadSource(ctx); saved.sourceId != source.sourceId || saved.rawConfig != source.config) {
			saveSource(source, ctx);
		}
	}
	if (config != current) {
		upsertConfig(config, ctx);
	}
}

std::shared_ptr<const AsyncReplConfigSource> AsyncReplConfigManager::GetSource() const {
	lock_guard lck(sourceMtx_);
	return source_;
}

void AsyncReplConfigManager::PublishInitial() {
	Lock lck(*this, RdxContext());
	publish();
	lck.UnlockPublished();
}

// The saved part is distributed as it was received, until the node changes it
void AsyncReplConfigManager::publish() noexcept {
	try {
		const auto saved = loadSource(RdxContext());
		const auto config = owner_.configProvider_.GetAsyncReplicationConfig();
		auto source = std::make_shared<const AsyncReplConfigSource>(AsyncReplConfigSource{
			.config = (saved.config && saved.config->HasSameClusterPart(config)) ? saved.rawConfig : config.GetClusterPartJSON(),
			.sourceId = saved.SourceIdOf(config)});
		lock_guard lck(sourceMtx_);
		source_.swap(source);
	} catch (const std::bad_alloc&) {
		logFmt(LogError, "Unable to publish the source of the async replication config: out of memory");
		std::abort();
	} catch (const std::exception& e) {
		// Is not expected by the current contracts: otherwise the calls above fail only on the broken invariants (e.g. missing #config)
		logFmt(LogError, "Unable to publish the source of the async replication config, unexpected error: {}", e.what());
		std::abort();
	}
}

// The change of 'nodes' revokes the source, so it does not revive, if 'nodes' are changed back
void AsyncReplConfigManager::revokeSource(const cluster::AsyncReplConfigData& applied, const RdxContext& ctx) {
	const auto saved = loadSource(ctx);
	if (saved.config && !saved.config->HasSameClusterNodes(applied)) {
		saveSource({.config = {}, .sourceId = saved.sourceId}, ctx);
	}
}

AsyncReplConfigManager::SavedConfig AsyncReplConfigManager::loadSource(const RdxContext& ctx) {
	SavedConfig saved;
	const auto json = owner_.getNamespace(kConfigNamespace, ctx)->GetMeta(kAsyncReplConfigSourceKey, ctx);
	if (json.empty()) {
		return saved;
	}
	// The invalid source is ignored, and the config is published without it. The lack of memory is not the invalid source
	const auto logInvalidSource = [](const std::exception& e) {
		logFmt(LogWarning, "Unable to load the source of the async replication config: {}", e.what());
	};
	try {
		gason::JsonParser parser;
		AsyncReplConfigSource source;
		source.FromJSON(parser.Parse(std::string_view(json)));
		saved.sourceId = source.sourceId;
		if (!source.config.empty()) {
			gason::JsonParser configParser;
			cluster::AsyncReplConfigData config;
			if (auto err = config.FromJSON(configParser.Parse(std::string_view(source.config))); !err.ok()) {
				throw err;
			}
			saved.config = std::move(config);
			saved.rawConfig = std::move(source.config);
		}
	} catch (const std::bad_alloc&) {
		throw;
	} catch (const gason::ParseException& e) {
		if (e.Code() == gason::JSON_ALLOCATION_FAILURE) {
			throw std::bad_alloc();
		}
		logInvalidSource(e);
	} catch (const std::exception& e) {
		logInvalidSource(e);
	}
	return saved;
}

void AsyncReplConfigManager::saveSource(const AsyncReplConfigSource& source, const RdxContext& ctx) {
	WrSerializer ser;
	{
		JsonBuilder jb(ser);
		source.GetJSON(jb);
	}
	owner_.getNamespace(kConfigNamespace, ctx)->PutMeta(kAsyncReplConfigSourceKey, ser.Slice(), ctx);
}

}  // namespace reindexer
