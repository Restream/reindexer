#include "cluster/consts.h"
#include "cluster/replication/sharedsyncstate.h"
#include "gtest/gtest.h"
#include "tools/errors.h"

namespace reindexer_tests {

using namespace reindexer;

TEST(SharedSyncState, AwaitAsyncReplicationPermission) {
	// The node of the RAFT cluster replicates asynchronously, only being the leader, which has the newest config of the cluster.
	// Each change of the permission increments its epoch and notifies the observers synchronously
	using cluster::RaftInfo;
	const auto expectAllowed = [](const cluster::SharedSyncState& state) {
		const auto err = state.GetAsyncReplPermission().error;
		EXPECT_TRUE(err.ok()) << err.what();
	};
	const auto expectForbidden = [](const cluster::SharedSyncState& state, std::string_view reason) {
		const auto err = state.GetAsyncReplPermission().error;
		EXPECT_NE(err.whatStr().find(reason), std::string::npos) << err.what();
	};
	const auto switchRole = [](cluster::SharedSyncState& state, RaftInfo::Role role) {
		state.SetRole(RaftInfo{.role = role});
		std::ignore = state.TryTransitRole(RaftInfo{.role = role});
	};

	expectAllowed(cluster::SharedSyncState());

	cluster::SharedSyncState state;
	size_t notifications = 0;
	state.AddAsyncReplPermissionObserver(&notifications, [&notifications] { ++notifications; });
	// Both the notifications and the epoch count the changes of the permission since the previous check
	auto expectChanges = [&state, &notifications, epoch = state.GetAsyncReplPermission().epoch](size_t count) mutable {
		const auto currentEpoch = state.GetAsyncReplPermission().epoch;
		EXPECT_EQ(notifications, count);
		EXPECT_EQ(currentEpoch - epoch, count);
		notifications = 0;
		epoch = currentEpoch;
	};

	cluster::ClusterConfigData raftConfig;
	raftConfig.nodes.resize(cluster::kMinClusterSize);
	state.Configure(raftConfig);
	expectForbidden(state, "has role 'none'");
	expectChanges(1);
	state.Reset(1);
	expectForbidden(state, "has role 'none'");
	switchRole(state, RaftInfo::Role::Candidate);
	expectForbidden(state, "roles 'candidate:candidate'");
	state.SetRole(RaftInfo{.role = RaftInfo::Role::Leader});
	expectForbidden(state, "roles 'candidate:leader'");
	switchRole(state, RaftInfo::Role::Leader);
	expectForbidden(state, "Initial sync of the RAFT leader is not done yet");
	expectChanges(0);
	state.MarkSynchronized();
	expectAllowed(state);
	expectChanges(1);
	state.SetRejectedAsyncReplConfig({.sourceId = 1, .cause = Error(errParams, "rejected config")});
	expectForbidden(state, "is not applied: rejected config");
	expectChanges(1);
	state.ResetRejectedAsyncReplConfig();
	expectAllowed(state);
	expectChanges(1);

	// Leader's demotion removes the permission at once. Initial sync of the next term is required
	state.SetRole(RaftInfo{.role = RaftInfo::Role::Follower});
	expectForbidden(state, "roles 'leader:follower'");
	expectChanges(1);
	std::ignore = state.TryTransitRole(RaftInfo{.role = RaftInfo::Role::Follower});
	expectForbidden(state, "roles 'follower:follower'");
	switchRole(state, RaftInfo::Role::Leader);
	expectForbidden(state, "Initial sync of the RAFT leader is not done yet");
	expectChanges(0);
	state.MarkSynchronized();
	expectAllowed(state);
	expectChanges(1);

	// Role's change without the transition
	state.SetRole(RaftInfo{.role = RaftInfo::Role::Candidate});
	expectForbidden(state, "roles 'leader:candidate'");
	state.SetRole(RaftInfo{.role = RaftInfo::Role::Leader});
	expectAllowed(state);
	expectChanges(2);

	state.SetTerminated();
	expectForbidden(state, "has role 'none'");
	expectChanges(1);

	state.RemoveAsyncReplPermissionObserver(&notifications);
	const auto epoch = state.GetAsyncReplPermission().epoch;
	state.Configure(cluster::ClusterConfigData());
	expectAllowed(state);
	EXPECT_EQ(state.GetAsyncReplPermission().epoch, epoch + 1);
	EXPECT_EQ(notifications, 0);
}

}  // namespace reindexer_tests
