#include "replicationstats.h"
#include "cluster/consts.h"
#include "core/cjson/jsonbuilder.h"
#include "vendor/gason/gason.h"

namespace reindexer {
namespace cluster {

using namespace std::string_view_literals;

void SyncStats::FromJSON(const gason::JsonNode& root) {
	count = root["count"sv].As<size_t>(0);
	maxTimeUs = root["max_time_us"sv].As<size_t>(0);
	avgTimeUs = root["avg_time_us"sv].As<size_t>(0);
}

void SyncStats::GetJSON(JsonBuilder& builder) const {
	builder.Put("count"sv, count);
	builder.Put("max_time_us"sv, maxTimeUs);
	builder.Put("avg_time_us"sv, avgTimeUs);
}

void InitialSyncStats::FromJSON(const gason::JsonNode& root) {
	forceSyncs.FromJSON(root["force_syncs"sv]);
	walSyncs.FromJSON(root["wal_syncs"sv]);
	totalTimeUs = root["total_time_us"sv].As<size_t>(0);
}

void InitialSyncStats::GetJSON(JsonBuilder& builder) const {
	{
		auto obj = builder.Object("force_syncs"sv);
		forceSyncs.GetJSON(obj);
	}
	{
		auto obj = builder.Object("wal_syncs"sv);
		walSyncs.GetJSON(obj);
	}
	builder.Put("total_time_us"sv, totalTimeUs);
}

static std::string_view NodeStatusToStr(NodeStats::Status status) noexcept {
	switch (status) {
		case NodeStats::Status::Offline:
			return "offline"sv;
		case NodeStats::Status::Online:
			return "online"sv;
		case NodeStats::Status::RaftError:
			return "raft_error"sv;
		case NodeStats::Status::None:
			break;
	}
	return "none"sv;
}

static NodeStats::Status NodeStatusFromStr(std::string_view status) noexcept {
	if (status == "online"sv) {
		return NodeStats::Status::Online;
	} else if (status == "offline"sv) {
		return NodeStats::Status::Offline;
	} else if (status == "raft_error"sv) {
		return NodeStats::Status::RaftError;
	}
	return NodeStats::Status::None;
}

static std::string_view NodeSyncStateToStr(NodeStats::SyncState state) {
	using namespace std::string_view_literals;
	switch (state) {
		case NodeStats::SyncState::None:
			return "none"sv;
		case NodeStats::SyncState::Syncing:
			return "syncing"sv;
		case NodeStats::SyncState::AwaitingResync:
			return "awaiting_resync"sv;
		case NodeStats::SyncState::OnlineReplication:
			return "online_replication"sv;
		case NodeStats::SyncState::InitialLeaderSync:
			return "initial_leader_sync"sv;
		default:;
	}
	return "none"sv;
}

static NodeStats::SyncState NodeSyncStateFromStr(std::string_view state) {
	using namespace std::string_view_literals;
	if (state == "online_replication"sv) {
		return NodeStats::SyncState::OnlineReplication;
	} else if (state == "awaiting_resync"sv) {
		return NodeStats::SyncState::AwaitingResync;
	} else if (state == "syncing"sv) {
		return NodeStats::SyncState::Syncing;
	} else if (state == "initial_leader_sync"sv) {
		return NodeStats::SyncState::InitialLeaderSync;
	} else {
		return NodeStats::SyncState::None;
	}
}

static Error NodeErrorFromJson(const gason::JsonNode& lastErrorRoot) {
	int code = lastErrorRoot["code"sv].As<int>(0);
	std::string_view what = lastErrorRoot["message"sv].As<std::string_view>(""sv);
	return Error(ErrorCode(code), what);
}

void NodeStats::FromJSON(const gason::JsonNode& root) {
	dsn = DSN(root["dsn"sv].As<std::string>());
	serverId = root["server_id"sv].As<int>(-1);
	updatesCount = root["pending_updates_count"sv].As<int64_t>(0);
	status = NodeStatusFromStr(root["status"sv].As<std::string_view>("none"sv));
	role = RaftInfo::RoleFromStr(root["role"sv].As<std::string_view>("follower"sv));
	isSynchronized = root["is_synchronized"sv].As<bool>(false);
	syncState = NodeSyncStateFromStr(root["sync_state"sv].As<std::string_view>("none"sv));
	lastError = NodeErrorFromJson(root["last_error"sv]);
	for (auto& ns : root["namespaces"sv]) {
		namespaces.emplace_back(ns.As<std::string>());
	}
}

void NodeStats::GetJSON(JsonBuilder& builder) const {
	builder.Put("dsn"sv, dsn);
	builder.Put("server_id"sv, serverId);
	builder.Put("pending_updates_count"sv, updatesCount);
	builder.Put("status"sv, NodeStatusToStr(status));
	builder.Put("role"sv, RaftInfo::RoleToStr(role));
	builder.Put("sync_state"sv, NodeSyncStateToStr(syncState));
	builder.Put("is_synchronized"sv, isSynchronized);
	{
		auto lastErrorJsonBuilder = builder.Object("last_error"sv);
		lastErrorJsonBuilder.Put("code"sv, int(lastError.code()));
		lastErrorJsonBuilder.Put("message"sv, lastError.what());
	}
	{
		auto nsArray = builder.Array("namespaces"sv);
		for (auto& ns : namespaces) {
			nsArray.Put(TagName::Empty(), ns);
		}
	}
}

Error ReplicationStats::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "RaftInfo: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
}

Error ReplicationStats::FromJSON(const gason::JsonNode& root) {
	try {
		type = root["type"sv].As<std::string>(type);
		if (type == kClusterReplStatsType) {
			initialSync.FromJSON(root["initial_sync"sv]);
		}
		forceSyncs.FromJSON(root["force_syncs"sv]);
		walSyncs.FromJSON(root["wal_syncs"sv]);
		updateDrops = root["update_drops"sv].As<int64_t>(updateDrops);
		pendingUpdatesCount = root["pending_updates_count"sv].As<int64_t>(pendingUpdatesCount);
		allocatedUpdatesCount = root["allocated_updates_count"sv].As<int64_t>(allocatedUpdatesCount);
		allocatedUpdatesSizeBytes = root["allocated_updates_size"sv].As<int64_t>(allocatedUpdatesSizeBytes);
		logLevel = logLevelFromString(root["log_level"sv].As<std::string_view>("info"));
		nodeStats.clear();
		for (auto& nodeJson : root["nodes"sv]) {
			NodeStats nodeSt;
			nodeSt.FromJSON(nodeJson);
			nodeStats.emplace_back(std::move(nodeSt));
		}
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "RaftInfo: {}", ex.what());
	}
	return errOK;
}

void ReplicationStats::GetJSON(JsonBuilder& builder) const {
	builder.Put("type"sv, type);
	if (type == kClusterReplStatsType) {
		auto obj = builder.Object("initial_sync"sv);
		initialSync.GetJSON(obj);
	}
	{
		auto obj = builder.Object("force_syncs"sv);
		forceSyncs.GetJSON(obj);
	}
	{
		auto obj = builder.Object("wal_syncs"sv);
		walSyncs.GetJSON(obj);
	}
	builder.Put("update_drops"sv, updateDrops);
	builder.Put("pending_updates_count"sv, pendingUpdatesCount);
	builder.Put("allocated_updates_count"sv, allocatedUpdatesCount);
	builder.Put("allocated_updates_size"sv, allocatedUpdatesSizeBytes);
	builder.Put("log_level"sv, logLevelToString(logLevel));
	{
		auto arrNode = builder.Array("nodes"sv);
		for (auto& node : nodeStats) {
			auto obj = arrNode.Object();
			node.GetJSON(obj);
		}
	}
}

void ReplicationStats::GetJSON(WrSerializer& ser) const {
	JsonBuilder jb(ser);
	GetJSON(jb);
}

void SyncStatsCounter::Hit(std::chrono::microseconds time) noexcept {
	lock_guard lck(mtx_);
	totalTimeUs += time.count();
	++count;
	if (maxTimeUs < time.count()) {
		maxTimeUs = time.count();
	}
}

void SyncStatsCounter::Reset() noexcept {
	lock_guard lck(mtx_);
	count = 0;
	maxTimeUs = 0;
	totalTimeUs = 0;
}

SyncStats SyncStatsCounter::Get() const {
	SyncStats stats;
	{
		lock_guard lck(mtx_);
		stats.count = count;
		stats.maxTimeUs = maxTimeUs;
		stats.avgTimeUs = totalTimeUs / (count ? count : 1);
	}
	return stats;
}

void NodeStatsCounter::SaveLastError(const Error& err) noexcept {
	lock_guard lck(mtx_);
	lastError = err;
}

Error NodeStatsCounter::GetLastError() const {
	lock_guard lck(mtx_);
	return lastError;
}

NodeStats NodeStatsCounter::Get() const {
	NodeStats stats;
	stats.dsn = dsn;
	stats.namespaces = namespaces;
	stats.updatesCount = 0;
	stats.serverId = serverId.load(std::memory_order_relaxed);
	stats.status = status.load(std::memory_order_relaxed);
	stats.syncState = syncState.load(std::memory_order_relaxed);
	stats.lastError = GetLastError();
	return stats;
}

void ReplicationStatCounter::OnStatusChanged(size_t nodeId, NodeStats::Status status) const noexcept {
	shared_lock rlck(mtx_);
	auto found = nodeCounters_.find(nodeId);
	if (found != nodeCounters_.end()) {
		found->second->OnStatusChanged(status);
	}
}

void ReplicationStatCounter::OnSyncStateChanged(size_t nodeId, NodeStats::SyncState state) noexcept {
	shared_lock rlck(mtx_);
	if (nodeId == kLeaderUID && thisNode_.has_value()) {
		thisNode_->OnSyncStateChanged(state);
	} else {
		auto found = nodeCounters_.find(nodeId);
		if (found != nodeCounters_.end()) {
			found->second->OnSyncStateChanged(state);
		}
	}
}

void ReplicationStatCounter::OnServerIdChanged(size_t nodeId, int serverId) const noexcept {
	shared_lock rlck(mtx_);
	auto found = nodeCounters_.find(nodeId);
	if (found != nodeCounters_.end()) {
		found->second->OnServerIdChanged(serverId);
	}
}

void ReplicationStatCounter::SaveNodeError(size_t nodeId, const Error& lastError) noexcept {
	shared_lock rlck(mtx_);
	auto found = nodeCounters_.find(nodeId);
	if (found != nodeCounters_.end()) {
		found->second->SaveLastError(lastError);
	}
}

void ReplicationStatCounter::Clear() noexcept {
	walSyncs_.Reset();
	forceSyncs_.Reset();
	initialForceSyncs_.Reset();
	initialWalSyncs_.Reset();

	lock_guard lck(mtx_);
	nodeCounters_.clear();
	updatesDrops_.store(0, std::memory_order_relaxed);
	lastPushedUpdateId_.store(-1, std::memory_order_relaxed);
	lastErasedUpdateId_.store(-1, std::memory_order_relaxed);
	lastReplicatedUpdateId_.store(-1, std::memory_order_relaxed);
	allocatedUpdatesSizeBytes_.store(0, std::memory_order_relaxed);
	initialSyncTotalTimeUs_.store(0, std::memory_order_relaxed);
	thisNode_.reset();
}

ReplicationStats ReplicationStatCounter::Get() const {
	ReplicationStats stats;
	stats.type = type_;

	shared_lock rlck(mtx_);
	stats.updateDrops = updatesDrops_.load(std::memory_order_relaxed);
	stats.pendingUpdatesCount =
		getUpdatesCountById(lastPushedUpdateId_.load(std::memory_order_relaxed), lastReplicatedUpdateId_.load(std::memory_order_relaxed));
	stats.allocatedUpdatesCount =
		getUpdatesCountById(lastPushedUpdateId_.load(std::memory_order_relaxed), lastErasedUpdateId_.load(std::memory_order_relaxed));
	stats.allocatedUpdatesSizeBytes = allocatedUpdatesSizeBytes_.load(std::memory_order_relaxed);
	stats.walSyncs = walSyncs_.Get();
	stats.forceSyncs = forceSyncs_.Get();
	stats.initialSync.walSyncs = initialWalSyncs_.Get();
	stats.initialSync.forceSyncs = initialForceSyncs_.Get();
	stats.initialSync.totalTimeUs = initialSyncTotalTimeUs_.load(std::memory_order_relaxed);
	stats.nodeStats.reserve(thisNode_.has_value() ? nodeCounters_.size() + 1 : nodeCounters_.size());
	for (auto& nodeCounter : nodeCounters_) {
		stats.nodeStats.emplace_back(nodeCounter.second->Get());
		const auto lastAppliedId = nodeCounter.second->lastAppliedUpdateId_.load(std::memory_order_relaxed);
		auto& lastNode = stats.nodeStats.back();
		lastNode.updatesCount = getUpdatesCountById(lastPushedUpdateId_.load(std::memory_order_relaxed), lastAppliedId);
		lastNode.isSynchronized = false;
	}
	if (thisNode_.has_value()) {
		stats.nodeStats.emplace_back(thisNode_->Get());
	}
	return stats;
}

int64_t ReplicationStatCounter::getUpdatesCountById(int64_t lastPushedId, int64_t lastErasedId) noexcept {
	if (lastPushedId >= lastErasedId) {
		return lastPushedId - lastErasedId;
	} else if (lastPushedId < 0 || lastErasedId >= 0) {
		return 0;
	}
	return lastPushedId;
}

}  // namespace cluster
}  // namespace reindexer
