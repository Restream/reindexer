#include "cluster/config.h"
#include "cluster/logger.h"
#include "cluster/raftmanager.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/dbconfig.h"
#include "gtest/gtest.h"
#include "gtests/tools.h"
#include "net/ev/ev.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer_tests {

using reindexer::WrSerializer;
using reindexer::cluster::LeaderCommitState;
using reindexer::cluster::Logger;
using reindexer::cluster::NodeData;
using reindexer::cluster::RaftManager;
using reindexer::cluster::ReplicationStatsCollector;

namespace {

NodeData ParseNodeData(std::string json) {
	NodeData data;
	auto err = data.FromJSON(std::span<char>(json.data(), json.size()));
	EXPECT_TRUE(err.ok()) << err.what();
	return data;
}

NodeData MakePing(int serverId, int term, LeaderCommitState state) {
	NodeData ping;
	ping.serverId = serverId;
	ping.electionsTerm = term;
	ping.leaderCommitState = state;
	return ping;
}

}  // namespace

TEST(RaftLeaderPing, NodeDataLeaderCommittedJsonRoundtrip) {
	{
		NodeData src;
		src.serverId = 7;
		src.electionsTerm = 3;
		src.leaderCommitState = LeaderCommitState::Committed;

		WrSerializer ser;
		src.GetJSON(ser);
		EXPECT_NE(std::string(ser.Slice()).find(R"("leader_committed":true)"), std::string::npos);
		EXPECT_EQ(ParseNodeData(std::string(ser.Slice())).leaderCommitState, LeaderCommitState::Committed);
	}
	{
		NodeData src;
		src.serverId = 7;
		src.electionsTerm = 3;
		src.leaderCommitState = LeaderCommitState::Election;

		WrSerializer ser;
		src.GetJSON(ser);
		EXPECT_NE(std::string(ser.Slice()).find(R"("leader_committed":false)"), std::string::npos);
		EXPECT_EQ(ParseNodeData(std::string(ser.Slice())).leaderCommitState, LeaderCommitState::Election);
	}
	{
		NodeData src;
		src.serverId = 7;
		src.electionsTerm = 3;
		EXPECT_EQ(src.leaderCommitState, LeaderCommitState::Unspecified);

		WrSerializer ser;
		src.GetJSON(ser);
		EXPECT_EQ(std::string(ser.Slice()).find("leader_committed"), std::string::npos);
		EXPECT_EQ(ParseNodeData(std::string(ser.Slice())).leaderCommitState, LeaderCommitState::Unspecified);
	}
}

TEST(RaftLeaderPing, SplitAvailabilityAndCommittedOverride) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.FollowerStayReady(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Committed)));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	// Same-id Election must not be rejected / must not demote Committed (re-elect path).
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 2, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	EXPECT_THROW(mgr.LeadersPing(MakePing(2, 2, LeaderCommitState::Election)), reindexer::Error);
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	NodeData legacy;
	legacy.serverId = 1;
	legacy.electionsTerm = 3;
	ASSERT_NO_THROW(mgr.LeadersPing(legacy));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	EXPECT_THROW(mgr.LeadersPing(MakePing(2, 4, LeaderCommitState::Committed)), reindexer::Error);
}

TEST(RaftLeaderPing, ElectionLeaseOverriddenByCommitted) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(7, 5, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(6, 3, LeaderCommitState::Committed)));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 6);

	NodeData probe;
	probe.serverId = 9;
	probe.electionsTerm = 1;
	NodeData response;
	mgr.SuggestLeader(probe, response);
	EXPECT_EQ(response.serverId, 6);
	// Term must sync down to the accepted Committed ping (not max(local, ping)).
	EXPECT_EQ(response.electionsTerm, 3);
}

TEST(RaftLeaderPing, SuggestDoesNotInstallPingLease) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	NodeData suggestion;
	suggestion.serverId = 5;
	suggestion.electionsTerm = 10;
	NodeData response;
	mgr.SuggestLeader(suggestion, response);
	EXPECT_EQ(response.serverId, 5);
	EXPECT_EQ(mgr.GetLeaderId(), 5);
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.FollowerStayReady(RaftManager::ClockT::now()));
	EXPECT_THROW(mgr.LeadersPing(MakePing(9, 10, LeaderCommitState::Election)), reindexer::Error);
	EXPECT_EQ(mgr.GetLeaderId(), 5);

	NodeData rival;
	rival.serverId = 6;
	rival.electionsTerm = 11;
	mgr.SuggestLeader(rival, response);
	EXPECT_EQ(mgr.GetLeaderId(), 6);
	EXPECT_EQ(response.serverId, 6);
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(6, 11, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	NodeData late;
	late.serverId = 7;
	late.electionsTerm = 12;
	mgr.SuggestLeader(late, response);
	EXPECT_EQ(mgr.GetLeaderId(), 6);
	EXPECT_EQ(response.serverId, 6);

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(6, 11, LeaderCommitState::Committed)));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	NodeData hijack;
	hijack.serverId = 8;
	hijack.electionsTerm = 13;
	mgr.SuggestLeader(hijack, response);
	EXPECT_EQ(mgr.GetLeaderId(), 6);
	EXPECT_EQ(response.serverId, 6);
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
}

TEST(RaftLeaderPing, SuggestIdentityChangeDoesNotInstallGhostLease) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Committed)));
	ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	EXPECT_FALSE(mgr.SetDesiredLeaderId(2));
	EXPECT_EQ(mgr.GetLeaderId(), 1);
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));

	NodeData suggestion;
	suggestion.serverId = 9;
	suggestion.electionsTerm = 4;
	NodeData response;
	mgr.SuggestLeader(suggestion, response);
	EXPECT_EQ(response.serverId, 2);
	EXPECT_EQ(mgr.GetLeaderId(), 2);
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(2, 4, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	NodeData rival;
	rival.serverId = 3;
	rival.electionsTerm = 5;
	mgr.SuggestLeader(rival, response);
	EXPECT_EQ(mgr.GetLeaderId(), 2);
	EXPECT_EQ(response.serverId, 2);
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
}

TEST(RaftLeaderPing, SetDesiredLeaderIdClearsLease) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Committed)));
	ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	EXPECT_FALSE(mgr.SetDesiredLeaderId(2));
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);
	EXPECT_EQ(mgr.GetDesiredLeaderId(), 2);

	mgr.ClearDesiredLeaderId();
	EXPECT_EQ(mgr.GetDesiredLeaderId(), -1);
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
}

TEST(RaftLeaderPing, FormerLeaderAcceptsCommittedAfterDesired) {
	// SetDesired(other) must demote VoteData Leader, else LeadersPing is rejected as "leader itself".
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	reindexer::ReplicationConfigData base;
	base.serverID = 0;
	reindexer::cluster::ClusterConfigData cluster;
	mgr.Configure(base, cluster);

	std::optional<reindexer::cluster::RaftInfo::Role> elected;
	loop.spawn(reindexer_tests_tools::exceptionWrapper([&] {
		elected = mgr.RunElectionsRound();
		ASSERT_TRUE(elected.has_value());
		ASSERT_EQ(*elected, reindexer::cluster::RaftInfo::Role::Leader);
		ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		EXPECT_EQ(mgr.GetLeaderId(), 0);

		ASSERT_TRUE(mgr.SetDesiredLeaderId(2));
		EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		EXPECT_EQ(mgr.GetDesiredLeaderId(), 2);

		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(2, 5, LeaderCommitState::Committed)));
		EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		EXPECT_EQ(mgr.GetLeaderId(), 2);
		EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));
	}));
	loop.run();
}

TEST(RaftLeaderPing, SuggestDesiredOverrideRetainsLiveCommitted) {
	// desired=id + live Committed(id): higher-term Suggest must bind desired without demoting Committed.
	// SetDesired clears lease — restore Committed before Suggest (prod keeps it via leader pings).
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(3, 1, LeaderCommitState::Committed)));
	ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	EXPECT_FALSE(mgr.SetDesiredLeaderId(3));
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(3, 2, LeaderCommitState::Committed)));
	ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	NodeData suggestion;
	suggestion.serverId = 9;
	suggestion.electionsTerm = 3;
	NodeData response;
	mgr.SuggestLeader(suggestion, response);
	EXPECT_EQ(response.serverId, 3);
	EXPECT_EQ(mgr.GetLeaderId(), 3);
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));
}

TEST(RaftLeaderPing, StartNewTermDropsElectionLeaseButRetainsCommitted) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	reindexer::ReplicationConfigData base;
	base.serverID = 0;
	reindexer::cluster::ClusterConfigData cluster;
	reindexer::cluster::ClusterNodeConfig peer;
	peer.serverId = 1;
	peer.dsn = reindexer::DSN("cproto://127.0.0.1:1/db");
	cluster.nodes.push_back(peer);
	mgr.Configure(base, cluster);

	loop.spawn(reindexer_tests_tools::exceptionWrapper([&] {
		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Election)));
		ASSERT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
		ASSERT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

		// Unreachable peer -> no consensus. StartNewTerm must drop Election lease (leaderId → self).
		EXPECT_FALSE(mgr.RunElectionsRound().has_value());
		EXPECT_EQ(mgr.GetLeaderId(), 0);
		EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));

		// Candidate (leaderId=self) must still ACK a rival Election ping.
		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 99, LeaderCommitState::Election)));
		EXPECT_EQ(mgr.GetLeaderId(), 1);
		EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
		EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 2, LeaderCommitState::Committed)));
		ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

		// Committed remote lease is retained -> Follower of 1 without winning phase-2.
		const auto elected = mgr.RunElectionsRound();
		ASSERT_TRUE(elected.has_value());
		EXPECT_EQ(*elected, reindexer::cluster::RaftInfo::Role::Follower);
		EXPECT_EQ(mgr.GetLeaderId(), 1);
		EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	}));
	loop.run();
}

}  // namespace reindexer_tests
