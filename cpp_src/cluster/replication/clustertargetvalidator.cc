#include "clustertargetvalidator.h"
#include "client/cororeindexer.h"
#include "cluster/consts.h"
#include "cluster/stats/replicationstats.h"
#include "core/query/query.h"
#include "core/system_ns_names.h"
#include "coroutine/waitgroup.h"
#include "estl/gift_str.h"
#include "replicationthread.h"
#include "tools/catch_and_return.h"
#include "tools/scope_guard.h"

namespace reindexer::cluster {

namespace {

struct [[nodiscard]] NodeProbe {
	explicit NodeProbe(const client::ReindexerConfig& config) : client(config) {}

	client::CoroReindexer client;
	bool reachable = false;
	Error err;
	RaftInfo info;
};

client::ReindexerConfig makeClientConfig(const ReplThreadConfig& config) {
	client::ReindexerConfig cfg;
	cfg.AppName = config.AppName;
	cfg.NetTimeout = std::chrono::seconds(config.UpdatesTimeoutSec);
	cfg.EnableCompression = config.EnableCompression;
	cfg.ReplToken = config.LeaderReplToken;
	return cfg;
}

// Expects the connected client
void probeNode(NodeProbe& probe, const DSN& dsn) noexcept {
	try {
		probe.err = probe.client.Status(true);
		if (!probe.err.ok()) {
			return;
		}
		probe.reachable = true;
		probe.err = CheckClusterTargetCompatibility(probe.client, dsn);
		if (probe.err.ok()) {
			probe.err = probe.client.GetRaftInfo(probe.info);
		}
	} catch (std::exception& e) {
		probe.err = Error(e);
	}
}

Expected<ReplicationStats> getClusterStats(client::CoroReindexer& leader) {
	client::CoroQueryResults qr;
	auto err = leader.Select(Query(kReplicationStatsNamespace).Where("type", CondEq, Variant(kClusterReplStatsType)), qr);
	if (!err.ok()) {
		return Unexpected(std::move(err));
	}
	if (qr.Count() != 1) {
		return Unexpected(Error(errNotValid, "Unexpected count of the RAFT cluster stats: {}", qr.Count()));
	}
	WrSerializer ser;
	err = qr.begin().GetJSON(ser, false);
	ReplicationStats stats;
	if (err.ok()) {
		err = stats.FromJSON(giftStr(ser.Slice()));
	}
	if (!err.ok()) {
		return Unexpected(std::move(err));
	}
	return stats;
}

Error checkClusterStats(const ReplicationStats& stats, std::span<const DSN> dsns, std::span<const NodeProbe> probes,
						size_t leaderIdx) noexcept {
	if (stats.nodeStats.size() != dsns.size()) {
		return Error(errParams, "Secondary cluster consists of {} nodes, but {} DSNs are configured", stats.nodeStats.size(), dsns.size());
	}
	size_t onlineCount = 0;
	for (const auto& node : stats.nodeStats) {
		const auto it = std::find_if(dsns.begin(), dsns.end(), [&node](const DSN& dsn) { return RelaxCompare(dsn, node.dsn); });
		if (it == dsns.end()) {
			return Error(errParams, "Secondary cluster's node '{}' is missing in the configured DSNs", node.dsn);
		}
		const size_t idx = it - dsns.begin();
		const bool isOnline = (node.status == NodeStats::Status::Online);
		// RAFT error means, that the node is reachable for the leader, but can not be its follower
		const bool isReachable = isOnline || node.status == NodeStats::Status::RaftError;
		if (isReachable != probes[idx].reachable) {
			return Error(errClusterConsensus,
						 "Availability of '{}' differs: it is {} for this node and {} for the secondary cluster's leader", node.dsn,
						 probes[idx].reachable ? "online" : "offline", isReachable ? "online" : "offline");
		}
		onlineCount += isOnline ? 1 : 0;
		if (idx == leaderIdx && (node.role != RaftInfo::Role::Leader || !node.isSynchronized)) {
			return Error(errClusterConsensus, "Secondary cluster's leader '{}' has not finished its initial sync yet", node.dsn);
		}
	}
	if (onlineCount < GetConsensusForN(dsns.size())) {
		return Error(errClusterConsensus, "Consensus is not available in the secondary cluster: {} of {} nodes are online", onlineCount,
					 dsns.size());
	}
	return {};
}

}  // namespace

Expected<size_t> ValidateClusterTarget(net::ev::dynamic_loop& loop, const ReplThreadConfig& config, std::span<const DSN> dsns) noexcept {
	try {
		const auto clientConfig = makeClientConfig(config);
		std::vector<NodeProbe> probes;
		probes.reserve(dsns.size());
		auto stopClients = MakeScopeGuard([&probes]() noexcept {
			for (auto& probe : probes) {
				probe.client.Stop();
			}
		});

		// Connect does not perform any network requests, so it is called before the probing coroutines' start
		const auto connectOpts = client::ConnectOpts().WithExpectedClusterID(config.ClusterID);
		for (const auto& dsn : dsns) {
			auto& probe = probes.emplace_back(clientConfig);
			probe.err = probe.client.Connect(dsn, loop, connectOpts);
		}
		coroutine::wait_group wg;
		for (size_t i = 0; i < dsns.size(); ++i) {
			if (probes[i].err.ok()) {
				loop.spawn(wg, [&probe = probes[i], &dsn = dsns[i]]() noexcept { probeNode(probe, dsn); });
			}
		}
		wg.wait();

		std::optional<size_t> leaderIdx;
		Error lastNetworkErr;
		for (size_t i = 0; i < dsns.size(); ++i) {
			if (!probes[i].reachable) {
				lastNetworkErr = probes[i].err;
				continue;
			}
			if (!probes[i].err.ok()) {
				return Unexpected(probes[i].err);
			}
			if (probes[i].info.role == RaftInfo::Role::Leader) {
				if (leaderIdx.has_value()) {
					return Unexpected(
						Error(errClusterConsensus, "Secondary cluster has several leaders: '{}' and '{}'", dsns[*leaderIdx], dsns[i]));
				}
				leaderIdx = i;
			}
		}
		if (!leaderIdx.has_value()) {
			return Unexpected(Error(errClusterConsensus, "There is no reachable leader in the secondary cluster{}{}",
									lastNetworkErr.ok() ? "" : ". Last network error: ", lastNetworkErr.what()));
		}

		auto stats = getClusterStats(probes[*leaderIdx].client);
		if (!stats) {
			return Unexpected(Error(stats.error().code(), "Unable to get secondary cluster's stats from '{}': {}", dsns[*leaderIdx],
									stats.error().what()));
		}
		if (auto err = checkClusterStats(*stats, dsns, probes, *leaderIdx); !err.ok()) {
			return Unexpected(std::move(err));
		}
		return *leaderIdx;
	} catch (std::exception& e) {
		return Unexpected(Error(e));
	}
}

Error CheckClusterTargetCompatibility(client::CoroReindexer& client, const DSN& dsn) noexcept {
	try {
		BindingCapabilities caps;
		if (auto err = client.GetServerCapabilities(caps); !err.ok()) {
			return err;
		}
		if (!caps.HasAsyncClusterReplication()) {
			return Error(errVersion, "Server '{}' does not support async replication to the RAFT cluster", dsn);
		}
	} CATCH_AND_RETURN
	return {};
}

}  // namespace reindexer::cluster
