#include <thread>
#include "cluster/config.h"
#include "cluster/logger.h"
#include "cluster/raftmanager.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/dbconfig.h"
#include "estl/gift_str.h"
#include "gtest/gtest.h"
#include "gtests/tools.h"
#include "net/cproto/dispatcher.h"
#include "net/cproto/serverconnection.h"
#include "net/ev/ev.h"
#include "net/listener.h"
#include "reindexer_version.h"
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

class [[nodiscard]] RaftPeerServer {
public:
	explicit RaftPeerServer(std::function<reindexer::Error()> onSecondPing) : onSecondPing_(std::move(onSecondPing)) {}
	~RaftPeerServer() { Stop(); }

	void Start(std::string addr) {
		thread_ = std::thread([this, addr = std::move(addr)] {
			stop_.set(loop_);
			stop_.set([](reindexer::net::ev::async& signal) { signal.loop.break_loop(); });
			stop_.start();
			dispatcher_.Register(reindexer::net::cproto::kCmdLogin, this, &RaftPeerServer::Login);
			dispatcher_.Register(reindexer::net::cproto::kCmdSuggestLeader, this, &RaftPeerServer::SuggestLeader);
			dispatcher_.Register(reindexer::net::cproto::kCmdLeadersPing, this, &RaftPeerServer::LeadersPing);
			listener_ = std::make_unique<reindexer::net::Listener<reindexer::net::ListenerType::Mixed>>(
				loop_, reindexer::net::cproto::ServerConnection::NewFactory(dispatcher_, false), nullptr);
			listener_->Bind(addr, reindexer::net::socket_domain::tcp);
			ready_ = true;
			while (!terminate_) {
				loop_.run();
			}
			ready_ = false;
			stop_.stop();
			listener_->Stop();
			listener_.reset();
		});
		while (!ready_) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}

	void Stop() {
		if (thread_.joinable()) {
			terminate_ = true;
			stop_.send();
			thread_.join();
		}
	}

private:
	reindexer::Error Login(reindexer::net::cproto::Context& ctx, reindexer::p_string, reindexer::p_string, reindexer::p_string) {
		const int64_t startTs =
			std::chrono::duration_cast<std::chrono::seconds>(reindexer::system_clock_w::now().time_since_epoch()).count();
		ctx.Return({reindexer::net::cproto::Arg(reindexer::p_string(REINDEX_VERSION)), reindexer::net::cproto::Arg(startTs)});
		return {};
	}

	reindexer::Error SuggestLeader(reindexer::net::cproto::Context& ctx, reindexer::p_string data) {
		NodeData suggestion;
		auto err = suggestion.FromJSON(reindexer::giftStr(data));
		if (!err.ok()) {
			return err;
		}
		WrSerializer ser;
		suggestion.GetJSON(ser);
		auto json = ser.Slice();
		ctx.Return({reindexer::net::cproto::Arg(reindexer::p_string(&json))});
		return {};
	}

	reindexer::Error LeadersPing(reindexer::net::cproto::Context&, reindexer::p_string) {
		return ++pingCount_ == 2 ? onSecondPing_() : reindexer::Error{};
	}

	std::function<reindexer::Error()> onSecondPing_;
	std::atomic<int> pingCount_{0};
	std::atomic<bool> terminate_{false};
	std::atomic<bool> ready_{false};
	reindexer::net::ev::dynamic_loop loop_;
	reindexer::net::ev::async stop_;
	reindexer::net::cproto::Dispatcher dispatcher_;
	std::unique_ptr<reindexer::net::IListener> listener_;
	std::thread thread_;
};

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

TEST(RaftLeaderPing, HigherTermDuringLeaderCommitPublishesFollower) {
	reindexer::net::ev::dynamic_loop loop;
	ReplicationStatsCollector stats;
	Logger log(LogNone);
	RaftManager mgr(loop, stats, log, [](uint32_t, bool) {});

	constexpr int kThisServerId = 3;
	constexpr int kNewLeaderId = 0;
	constexpr int kNewTerm = 2;
	const std::string peerAddr = "127.0.0.1:25699";
	RaftPeerServer peer([&mgr] {
		try {
			NodeData response;
			mgr.SuggestLeader(MakePing(kNewLeaderId, kNewTerm, LeaderCommitState::Unspecified), response);
			mgr.LeadersPing(MakePing(kNewLeaderId, kNewTerm, LeaderCommitState::Committed));
			return reindexer::Error{};
		} catch (const reindexer::Error& err) {
			return err;
		}
	});
	peer.Start(peerAddr);

	reindexer::ReplicationConfigData base;
	base.serverID = kThisServerId;
	base.clusterID = 1;
	reindexer::cluster::ClusterConfigData cluster;
	for (int serverId = 0; serverId < 5; ++serverId) {
		reindexer::cluster::ClusterNodeConfig node;
		node.serverId = serverId;
		node.dsn = reindexer::DSN("cproto://" + peerAddr + "/db");
		cluster.nodes.emplace_back(std::move(node));
	}
	mgr.Configure(base, cluster);

	loop.spawn(reindexer_tests_tools::exceptionWrapper([&] {
		const auto elected = mgr.RunElectionsRound();
		ASSERT_TRUE(elected.has_value());
		EXPECT_EQ(*elected, reindexer::cluster::RaftInfo::Role::Follower);
		EXPECT_EQ(mgr.GetLeaderId(), kNewLeaderId);
		EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

		mgr.SetTerminateFlag(true);
		mgr.AwaitTermination();
	}));
	loop.run();
}

}  // namespace reindexer_tests
