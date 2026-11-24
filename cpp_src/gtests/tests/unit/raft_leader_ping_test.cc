#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>
#include "cluster/config.h"
#include "cluster/logger.h"
#include "cluster/raftmanager.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/dbconfig.h"
#include "estl/gift_str.h"
#include "fmt/format.h"
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
using reindexer::cluster::RaftInfo;
using reindexer::cluster::RaftManager;
using reindexer::cluster::ReplicationStatsCollector;

namespace {

constexpr int kBasePort = 25700;

std::string PeerAddr(int serverId) { return fmt::format("127.0.0.1:{}", kBasePort + serverId); }

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

bool IsCommittedPingFrom(const NodeData& ping, int serverId) noexcept {
	return (ping.serverId == serverId) && (ping.leaderCommitState == LeaderCommitState::Committed);
}

reindexer::Error ForwardPing(RaftManager& mgr, const NodeData& ping) noexcept {
	try {
		mgr.LeadersPing(ping);
		return {};
	} catch (std::exception& err) {
		return std::move(err);
	}
}

// The manager skips its own entry, so every server id gets an address.
void ConfigureRaft(RaftManager& mgr, int serverId, int clusterSize = 0, const std::function<std::string(int)>& addrOf = {}) {
	reindexer::ReplicationConfigData base;
	base.serverID = serverId;
	reindexer::cluster::ClusterConfigData cluster;
	cluster.nodes.reserve(clusterSize);
	for (int id = 0; id < clusterSize; ++id) {
		auto& node = cluster.nodes.emplace_back();
		node.serverId = id;
		node.dsn = reindexer::DSN(fmt::format("cproto://{}/db", addrOf(id)));
	}
	mgr.Configure(base, cluster);
}

class [[nodiscard]] RaftPeerServer {
public:
	using SuggestHandler = std::function<reindexer::Error(const NodeData& suggestion, NodeData& response)>;
	using PingHandler = std::function<reindexer::Error(const NodeData& ping)>;

	RaftPeerServer(SuggestHandler onSuggest, PingHandler onPing) : onSuggest_(std::move(onSuggest)), onPing_(std::move(onPing)) {}
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
		NodeData response;
		err = onSuggest_(suggestion, response);
		if (!err.ok()) {
			return err;
		}
		WrSerializer ser;
		response.GetJSON(ser);
		auto json = ser.Slice();
		ctx.Return({reindexer::net::cproto::Arg(reindexer::p_string(&json))});
		return {};
	}

	reindexer::Error LeadersPing(reindexer::net::cproto::Context&, reindexer::p_string data) {
		NodeData ping;
		auto err = ping.FromJSON(reindexer::giftStr(data));
		return err.ok() ? onPing_(ping) : err;
	}

	SuggestHandler onSuggest_;
	PingHandler onPing_;
	std::atomic<bool> terminate_{false};
	std::atomic<bool> ready_{false};
	reindexer::net::ev::dynamic_loop loop_;
	reindexer::net::ev::async stop_;
	reindexer::net::cproto::Dispatcher dispatcher_;
	std::unique_ptr<reindexer::net::IListener> listener_;
	std::thread thread_;
};

}  // namespace

// Test coroutines and peer handlers are stored as std::function and capture more than the hot-path limit.
// NOLINTBEGIN(rx-perf-lambda-to-std-function-allocation)
class [[nodiscard]] RaftLeaderPing : public ::testing::Test {
protected:
	RaftManager makeManager() {
		return RaftManager(loop_, stats_, log_, [](uint32_t, bool) {});
	}

	static NodeData readVote(RaftManager& mgr) {
		// A stale suggestion reads the vote without advancing the term.
		NodeData response;
		mgr.SuggestLeader(MakePing(-1, -1, LeaderCommitState::Unspecified), response);
		return response;
	}

	void runAndStop(RaftManager& mgr, std::function<void()> body) {
		auto wrapped = reindexer_tests_tools::exceptionWrapper(std::move(body));
		// A fatal assertion returns from body, so cleanup belongs outside it.
		loop_.spawn([&mgr, &wrapped] {
			wrapped();
			mgr.SetTerminateFlag(true);
			mgr.AwaitTermination();
		});
		loop_.run();
	}

	reindexer::net::ev::dynamic_loop loop_;
	ReplicationStatsCollector stats_;
	Logger log_{LogNone};
};

TEST_F(RaftLeaderPing, NodeDataLeaderCommittedJsonRoundtrip) {
	{
		const auto src = MakePing(7, 3, LeaderCommitState::Committed);

		WrSerializer ser;
		src.GetJSON(ser);
		EXPECT_NE(std::string(ser.Slice()).find(R"("leader_committed":true)"), std::string::npos);
		EXPECT_EQ(ParseNodeData(std::string(ser.Slice())).leaderCommitState, LeaderCommitState::Committed);
	}
	{
		const auto src = MakePing(7, 3, LeaderCommitState::Election);

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

TEST_F(RaftLeaderPing, SplitAvailabilityAndCommittedOverride) {
	// An Election ping makes the leader available, but followers stay unpublished until the same leader commits.
	// That committed lease then sticks: same-id Election and legacy pings must not demote it, and another node can't replace it while it is
	// live.

	auto mgr = makeManager();

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

	// Re-election by the same leader must preserve its 'committed' state. A ping from the same leader must not be rejected.
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 2, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	// Ping from another candidate must be rejected, if node already has different leader's lease.
	EXPECT_THROW(mgr.LeadersPing(MakePing(2, 2, LeaderCommitState::Election)), reindexer::Error);
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	// Legacy ping from the same leader still won't be rejected and grants 'committed' leader availability.
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 3, LeaderCommitState::Unspecified)));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	EXPECT_THROW(mgr.LeadersPing(MakePing(2, 4, LeaderCommitState::Committed)), reindexer::Error);
}

TEST_F(RaftLeaderPing, ElectionLeaseOverriddenByCommitted) {
	auto mgr = makeManager();

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(7, 5, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(6, 3, LeaderCommitState::Committed)));
	EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 6);

	const auto response = readVote(mgr);
	EXPECT_EQ(response.serverId, 6);
	// Term must sync down to the accepted Committed ping, even if local term is higher.
	EXPECT_EQ(response.electionsTerm, 3);
}

TEST_F(RaftLeaderPing, SuggestDoesNotInstallPingLease) {
	auto mgr = makeManager();

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

TEST_F(RaftLeaderPing, SuggestIdentityChangeDoesNotInstallGhostLease) {
	// A suggestion that switches the voted leader must not carry the previous leader's lease onto the new id.
	// A suggestion must not promote a later Election ping to Committed either.

	auto mgr = makeManager();

	// Committed leader, whose lease the identity change could illegally reuse.
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Committed)));
	ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_EQ(mgr.GetLeaderId(), 1);

	// Desired-leader switch drops that lease and leaves the voted id unchanged.
	EXPECT_FALSE(mgr.SetDesiredLeaderId(2));
	EXPECT_EQ(mgr.GetLeaderId(), 1);
	EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));

	// Higher-term suggestion moves the vote to the desired leader.
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

	// First ping from the new leader.
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(2, 4, LeaderCommitState::Election)));
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

	// Higher-term suggestion from a third node while that ping lease is live.
	NodeData rival;
	rival.serverId = 3;
	rival.electionsTerm = 5;
	mgr.SuggestLeader(rival, response);
	EXPECT_EQ(mgr.GetLeaderId(), 2);
	EXPECT_EQ(response.serverId, 2);
	EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
	EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
}

TEST_F(RaftLeaderPing, SetDesiredLeaderIdClearsLease) {
	auto mgr = makeManager();

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

TEST_F(RaftLeaderPing, FormerLeaderAcceptsCommittedAfterDesired) {
	// SetDesired(other) must demote VoteData Leader, else LeadersPing is rejected as "leader itself".

	auto mgr = makeManager();
	ConfigureRaft(mgr, 0);

	runAndStop(mgr, [&] {
		const auto elected = mgr.RunElectionsRound();
		ASSERT_EQ(elected, RaftInfo::Role::Leader);
		ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		EXPECT_EQ(mgr.GetLeaderId(), 0);

		ASSERT_TRUE(mgr.SetDesiredLeaderId(2));
		EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		EXPECT_EQ(mgr.GetDesiredLeaderId(), 2);

		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(2, 5, LeaderCommitState::Committed)));
		EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		EXPECT_EQ(mgr.GetLeaderId(), 2);
		EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

		// The desired leader suppresses a new term, so the node follows 2 and the vote stays at term 5.
		EXPECT_EQ(mgr.RunElectionsRound(), RaftInfo::Role::Follower);
		EXPECT_EQ(mgr.GetLeaderId(), 2);
		EXPECT_EQ(readVote(mgr).electionsTerm, 5);
	});
}

TEST_F(RaftLeaderPing, SuggestDesiredOverrideRetainsLiveCommitted) {
	// A higher-term suggestion must preserve a live committed lease for the desired leader.

	auto mgr = makeManager();

	EXPECT_FALSE(mgr.SetDesiredLeaderId(3));
	// The committed ping installs the lease of the desired leader. SetDesired does not keep one.
	ASSERT_NO_THROW(mgr.LeadersPing(MakePing(3, 2, LeaderCommitState::Committed)));
	ASSERT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	EXPECT_TRUE(mgr.FollowerStayReady(RaftManager::ClockT::now()));

	// Other leader suggestion doesn't affect current leader's availability.
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

TEST_F(RaftLeaderPing, ElectionStartWaitsForElectionLeaseAndRetainsCommitted) {
	auto mgr = makeManager();
	ConfigureRaft(mgr, 0, 2, PeerAddr);

	runAndStop(mgr, [&] {
		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 1, LeaderCommitState::Election)));
		ASSERT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
		ASSERT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

		// Wait for node 1's Election lease to expire before starting another term.
		EXPECT_FALSE(mgr.RunElectionsRound().has_value());
		EXPECT_EQ(mgr.GetLeaderId(), 1);
		EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));

		// The next round votes for itself, but the unreachable peer prevents a majority.
		EXPECT_FALSE(mgr.RunElectionsRound().has_value());
		EXPECT_EQ(mgr.GetLeaderId(), 0);
		EXPECT_FALSE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));

		// A candidate may still acknowledge a rival's Election ping.
		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 99, LeaderCommitState::Election)));
		EXPECT_EQ(mgr.GetLeaderId(), 1);
		EXPECT_TRUE(mgr.LeaderIsAvailable(RaftManager::ClockT::now()));
		EXPECT_FALSE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));

		// Committed remote lease is retained, so node becomes Follower of 1 without phase-2.
		ASSERT_NO_THROW(mgr.LeadersPing(MakePing(1, 2, LeaderCommitState::Committed)));
		EXPECT_EQ(mgr.RunElectionsRound(), RaftInfo::Role::Follower);
		EXPECT_EQ(mgr.GetLeaderId(), 1);
	});

	// A previous Committed state must not mask legacy-ping handling.
	for (auto state : {LeaderCommitState::Committed, LeaderCommitState::Unspecified}) {
		SCOPED_TRACE(state == LeaderCommitState::Committed ? "Committed" : "Unspecified");
		auto follower = makeManager();
		ConfigureRaft(follower, 0, 2, PeerAddr);
		runAndStop(follower, [&] {
			ASSERT_NO_THROW(follower.LeadersPing(MakePing(1, 1, LeaderCommitState::Election)));
			ASSERT_FALSE(follower.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
			ASSERT_NO_THROW(follower.LeadersPing(MakePing(1, 2, state)));
			ASSERT_TRUE(follower.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
			EXPECT_TRUE(follower.FollowerStayReady(RaftManager::ClockT::now()));
			EXPECT_EQ(follower.RunElectionsRound(), RaftInfo::Role::Follower);
			EXPECT_EQ(follower.GetLeaderId(), 1);
			EXPECT_TRUE(follower.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
		});
	}
}

TEST_F(RaftLeaderPing, SuggestStartsTerm) {
	// A suggestion carries no ping lease, so the election-start wait must not block the next term.

	auto mgr = makeManager();
	ConfigureRaft(mgr, 0);

	runAndStop(mgr, [&] {
		NodeData response;
		mgr.SuggestLeader(MakePing(1, 4, LeaderCommitState::Election), response);
		EXPECT_EQ(mgr.RunElectionsRound(), RaftInfo::Role::Leader);
		EXPECT_EQ(mgr.GetLeaderId(), 0);
		EXPECT_EQ(readVote(mgr).electionsTerm, 5);
	});
}

TEST_F(RaftLeaderPing, NewTermStopsFormerLeaderPingRoutines) {
	auto mgr = makeManager();
	std::atomic<int> committedPings{0};
	RaftPeerServer peer(
		[](const NodeData& suggestion, NodeData& response) {
			response = suggestion;
			return reindexer::Error{};
		},
		[&committedPings](const NodeData& ping) {
			if (ping.leaderCommitState == LeaderCommitState::Committed) {
				++committedPings;
			}
			return reindexer::Error{};
		});
	peer.Start(PeerAddr(0));
	ConfigureRaft(mgr, 0, 2, [](int) { return PeerAddr(0); });
	runAndStop(mgr, [&] {
		EXPECT_EQ(mgr.RunElectionsRound(), RaftInfo::Role::Leader);
		// Let the old leader's ping routine start and yield in its RPC or interval sleep.
		loop_.granular_sleep(std::chrono::seconds(5), std::chrono::milliseconds(1), [&] { return committedPings.load() > 0; });
		EXPECT_GT(committedPings.load(), 0);
		EXPECT_EQ(mgr.RunElectionsRound(), RaftInfo::Role::Leader);
	});
}

TEST_F(RaftLeaderPing, HigherTermDuringLeaderCommitPublishesFollower) {
	// A leader commit overtaken by a higher term must follow the leader that term already committed.
	// Opening a new term instead would drop a vote the other leader has counted and leave two leaders.

	auto mgr = makeManager();

	constexpr int kThisServerId = 3;
	constexpr int kNewLeaderId = 0;
	constexpr int kNewTerm = 2;
	std::mutex pingMtx;
	int pingCount = 0;
	RaftPeerServer peer(
		[](const NodeData& suggestion, NodeData& response) {
			response = suggestion;
			return reindexer::Error{};
		},
		[&mgr, &pingMtx, &pingCount](const NodeData&) {
			// Later ACKs must not form a majority before the second ping installs the new term.
			std::lock_guard lock(pingMtx);
			if (++pingCount != 2) {
				return reindexer::Error{};
			}
			try {
				NodeData response;
				mgr.SuggestLeader(MakePing(kNewLeaderId, kNewTerm, LeaderCommitState::Unspecified), response);
				mgr.LeadersPing(MakePing(kNewLeaderId, kNewTerm, LeaderCommitState::Committed));
				return reindexer::Error{};
			} catch (const reindexer::Error& err) {
				return err;
			}
		});
	peer.Start(PeerAddr(0));
	ConfigureRaft(mgr, kThisServerId, 5, [](int) { return PeerAddr(0); });

	runAndStop(mgr, [&] {
		const auto elected = mgr.RunElectionsRound();
		EXPECT_EQ(elected, RaftInfo::Role::Follower);
		EXPECT_EQ(mgr.GetLeaderId(), kNewLeaderId);
		EXPECT_TRUE(mgr.LeaderIsCommittedAvailable(RaftManager::ClockT::now()));
	});
}

TEST_F(RaftLeaderPing, FollowerTimeoutWithElectionLeaseDoesNotStartRivalTerm) {
	// Model a new round after a follower wait: node 3 has acknowledged node 0's Election ping but has not received Committed.

	using reindexer::cluster::kMinLeaderAwaitInterval;
	constexpr int kClusterSize = 5;
	constexpr int kLeaderId = 0;
	constexpr int kLateNodeId = 3;

	std::mutex commitMtx;
	std::condition_variable commitCv;
	std::atomic<bool> releaseCommit{false};
	std::atomic<RaftManager::ClockT::time_point> electionAccepted{};
	std::vector<std::unique_ptr<RaftManager>> nodes;
	nodes.reserve(kClusterSize);
	std::vector<std::unique_ptr<RaftPeerServer>> servers;
	servers.reserve(kClusterSize);
	for (int serverId = 0; serverId < kClusterSize; ++serverId) {
		auto& mgr = *nodes.emplace_back(std::make_unique<RaftManager>(loop_, stats_, log_, [](uint32_t, bool) {}));
		ConfigureRaft(mgr, serverId, kClusterSize, PeerAddr);

		RaftPeerServer::PingHandler onPing;
		if (serverId == kLateNodeId) {
			onPing = [&mgr, &commitMtx, &commitCv, &releaseCommit, &electionAccepted](const NodeData& ping) {
				if (IsCommittedPingFrom(ping, kLeaderId)) {
					std::unique_lock lock(commitMtx);
					if (!commitCv.wait_for(lock, std::chrono::seconds(5), [&releaseCommit] { return releaseCommit.load(); })) {
						return reindexer::Error(errTimeout, "Test did not release Committed ping");
					}
				}
				const auto acceptedAt = RaftManager::ClockT::now();
				auto err = ForwardPing(mgr, ping);
				if (err.ok() && ping.serverId == kLeaderId && ping.leaderCommitState == LeaderCommitState::Election) {
					electionAccepted = acceptedAt;
				}
				return err;
			};
		} else {
			onPing = [&mgr, &releaseCommit](const NodeData& ping) {
				if (IsCommittedPingFrom(ping, kLeaderId) && !releaseCommit.load()) {
					return reindexer::Error(errNetwork, "Committed ping is still in flight");
				}
				return ForwardPing(mgr, ping);
			};
		}
		servers.emplace_back(std::make_unique<RaftPeerServer>(
			[&mgr](const NodeData& suggestion, NodeData& response) {
				mgr.SuggestLeader(suggestion, response);
				return reindexer::Error{};
			},
			std::move(onPing)));
		servers.back()->Start(PeerAddr(serverId));
	}

	// Node 0 must win term 2 with nodes 1 and 3; nodes 2 and 4 remain available for a rival's next term.
	NodeData response;
	nodes[0]->SuggestLeader(MakePing(0, 1, LeaderCommitState::Unspecified), response);
	nodes[1]->SuggestLeader(MakePing(1, 1, LeaderCommitState::Unspecified), response);
	nodes[2]->SuggestLeader(MakePing(2, 2, LeaderCommitState::Unspecified), response);
	nodes[4]->SuggestLeader(MakePing(2, 2, LeaderCommitState::Unspecified), response);

	loop_.spawn(reindexer_tests_tools::exceptionWrapper([&] {
		auto& leader = *nodes[kLeaderId];
		auto& late = *nodes[kLateNodeId];
		const auto leaderRole = leader.RunElectionsRound();
		const auto now = RaftManager::ClockT::now();
		const auto acceptedAt = electionAccepted.load();
		const bool liveAtEntry = (acceptedAt != RaftManager::ClockT::time_point{}) && (now < (acceptedAt + kMinLeaderAwaitInterval));
		const bool lateHoldsElectionLease =
			(late.GetLeaderId() == kLeaderId) && late.LeaderIsAvailable(now) && !late.LeaderIsCommittedAvailable(now);
		const auto before = readVote(late);

		bool observed = false;
		bool liveAtObservation = false;
		NodeData after;
		// spawn queues the observer: RunElectionsRound makes its local decision before its first coroutine wait.
		// Committed stays gated until the observer has checked the decision, including an erroneous reset.
		loop_.spawn([&] {
			liveAtObservation = RaftManager::ClockT::now() < (acceptedAt + kMinLeaderAwaitInterval);
			after = readVote(late);
			observed = true;
			{
				std::lock_guard lock(commitMtx);
				releaseCommit = true;
			}
			commitCv.notify_all();
		});
		const auto lateRole = late.RunElectionsRound();
		// Also run the observer if an erroneous implementation returned without yielding.
		while (!observed) {
			loop_.yield();
		}
		const bool leaderStillLeads = (leader.GetLeaderId() == kLeaderId) && leader.LeaderIsCommittedAvailable(RaftManager::ClockT::now());
		const int lateLeaderId = late.GetLeaderId();
		for (auto* mgr : {&leader, &late}) {
			mgr->SetTerminateFlag(true);
			mgr->AwaitTermination();
		}

		// Classify the schedule using the independently recorded ping time, never the possibly reset vote.
		if (liveAtEntry && liveAtObservation) {
			EXPECT_EQ(after.serverId, before.serverId);
			EXPECT_EQ(after.electionsTerm, before.electionsTerm);
		}
#if RX_WITH_SLOW_RUNTIME
		if (!liveAtEntry || !liveAtObservation || !leaderRole) {
			GTEST_SKIP() << "Live-lease election scenario was not established before observation"
						 << "; liveAtEntry=" << liveAtEntry << ", liveAtObservation=" << liveAtObservation
						 << ", hasElectionResult=" << leaderRole.has_value();
		}
#else	// !RX_WITH_SLOW_RUNTIME
		EXPECT_TRUE(liveAtEntry);
		EXPECT_TRUE(liveAtObservation);
#endif	// !RX_WITH_SLOW_RUNTIME
		ASSERT_EQ(leaderRole, RaftInfo::Role::Leader);
		ASSERT_TRUE(lateHoldsElectionLease);
		ASSERT_TRUE(leaderStillLeads);
		EXPECT_EQ(lateRole, RaftInfo::Role::Follower) << "node 3 must follow node 0, not start a rival term";
		EXPECT_EQ(lateLeaderId, kLeaderId);
	}));
	loop_.run();
}

// NOLINTEND(rx-perf-lambda-to-std-function-allocation)

}  // namespace reindexer_tests
