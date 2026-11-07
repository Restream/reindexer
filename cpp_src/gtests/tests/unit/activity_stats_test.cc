#if !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(REINDEX_WITH_UBSAN)

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include "client/reindexer.h"
#include "cluster_operation_api.h"
#include "core/activity/activity.h"
#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "gtests/tools.h"
#include "tools/serilize/wrserializer.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests {

using reindexer::IndexOpts;
using reindexer_tests_tools::exceptionWrapper;

namespace {

constexpr int kItemCount = 10'000;
// Each delete removes its own half of the items.
constexpr int kDeleteBatch = kItemCount / 2;
constexpr size_t kNoteLen = 128;
constexpr int kScanConditions = 8;
constexpr int kMaxRetries = 3;

std::string joinValues(const std::unordered_set<std::string>& values) {
	std::string out;
	for (const auto& value : values) {
		if (!out.empty()) {
			out += ", ";
		}
		out += value;
	}
	return out;
}

// Samples #activitystats on a separate connection to the leader. Its own selects are on #activitystats and are ignored below.
class [[nodiscard]] ActivitySampler {
public:
	ActivitySampler(reindexer::client::Reindexer& watcher, std::string ns) : watcher_(watcher), ns_(std::move(ns)) {}
	~ActivitySampler() { Stop(); }

	ActivitySampler(const ActivitySampler&) = delete;
	ActivitySampler& operator=(const ActivitySampler&) = delete;

	void Start() {
		thread_ = std::thread([this] {
			const reindexer::Query query{reindexer::kActivityStatsNamespace};
			while (!stop_.load(std::memory_order_relaxed)) {
				reindexer::client::QueryResults qr;
				const auto err = watcher_.Select(query, qr);
				polls_.fetch_add(1, std::memory_order_relaxed);
				if (!err.ok()) {
					lastError_ = err.what();
					continue;
				}
				lastError_.clear();
				for (auto it : qr) {
					reindexer::WrSerializer ser;
					if (!it.GetJSON(ser, false).ok()) {
						continue;
					}
					try {
						gason::JsonParser parser;
						const auto root = parser.Parse(ser.Slice());
						const auto queryText = root["query"].As<std::string_view>();
						if (queryText.find(ns_) == std::string_view::npos) {
							continue;
						}
						auto state = root["state"].As<std::string>();
						if (!state.empty()) {
							states_.emplace(std::move(state));
						}
					} catch (const std::exception& err) {
						lastError_ = err.what();
					}
				}
			}
		});
	}

	void WaitUntilPolling() {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (polls_.load(std::memory_order_relaxed) == 0 && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::yield();
		}
	}

	void Stop() {
		if (!thread_.joinable()) {
			return;
		}
		stop_.store(true, std::memory_order_relaxed);
		thread_.join();
	}

	int Polls() const noexcept { return polls_.load(std::memory_order_relaxed); }
	const std::string& LastError() const noexcept { return lastError_; }
	const std::unordered_set<std::string>& States() const noexcept { return states_; }

private:
	reindexer::client::Reindexer& watcher_;
	std::string ns_;
	std::atomic<bool> stop_{false};
	std::atomic<int> polls_{0};
	std::string lastError_;
	std::unordered_set<std::string> states_;
	std::thread thread_;
};

template <typename F>
std::unordered_set<std::string> observeActivity(reindexer::client::Reindexer& watcher, const std::string& ns, F&& op) {
	ActivitySampler sampler(watcher, ns);
	sampler.Start();
	sampler.WaitUntilPolling();
	std::forward<F>(op)();
	sampler.Stop();
	EXPECT_GT(sampler.Polls(), 0) << "select from #activitystats did not complete: " << sampler.LastError();
	return sampler.States();
}

void expectState(const std::unordered_set<std::string>& seen, reindexer::Activity::State state) {
	const auto name = std::string(reindexer::Activity::DescribeState(state));
	EXPECT_TRUE(seen.find(name) != seen.end()) << '\'' << name << "' was not observed, seen: [" << joinValues(seen) << ']';
}

// 'note' is not indexed. This allows to achive 'select_loop' phase
Query matchingItems(std::string_view ns, const std::string& note) {
	Query q{ns};
	for (int i = 0; i < kScanConditions; ++i) {
		q.Where("note", CondEq, note);
	}
	return q;
}

// Deletes by 'part', which is not indexed: its comparator costs as much as note's and is checked after them,
// so every item is scanned with all conditions.
Query itemsOfPart(std::string_view ns, const std::string& note, int part) {
	auto q = matchingItems(ns, note);
	q.Where("part", CondEq, part);
	q.Delete();
	return q;
}

// A phase may fall between #activitystats polls, then the query is repeated: the update sets the same values again,
// the delete gets its items back first.
template <typename Exec, typename Restore>
void expectPhases(reindexer::client::Reindexer& watcher, const std::string& ns, Exec&& exec, const Query& query, size_t expectedCount,
				  reindexer::Activity::State itemsPhase, Restore&& restore) {
	const std::array phases{reindexer::Activity::SelectLoop, itemsPhase, reindexer::Activity::WaitReplication};
	const auto run = [&] { return observeActivity(watcher, ns, [&] { EXPECT_EQ(exec(query), expectedCount) << query.GetSQL(); }); };
	auto seen = run();
	const auto allSeen = [&] {
		return std::ranges::all_of(phases,
								   [&](auto phase) { return seen.contains(std::string(reindexer::Activity::DescribeState(phase))); });
	};
	for (int i = 0; i < kMaxRetries && !allSeen(); ++i) {
		restore();
		seen.merge(run());
	}
	for (auto phase : phases) {
		expectState(seen, phase);
	}
}

void insertItems(BaseApi& api, std::string_view ns, const std::string& note) {
	auto tx = api.NewTransaction(ns);
	reindexer::WrSerializer ser;
	for (int id = 0; id < kItemCount; ++id) {
		ser.Reset();
		{
			reindexer::JsonBuilder builder{ser};
			builder.Put("id", id);
			builder.Put("note", std::string_view{note});
			builder.Put("part", id / kDeleteBatch);
			builder.Put("flag", 0);
			builder.End();
		}
		auto item = tx.NewItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		err = tx.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
	}
	const auto qr = api.CommitTransaction(tx);
	ASSERT_EQ(qr.Count(), static_cast<size_t>(kItemCount));
}

}  // namespace

TEST_F(ClusterOperationApi, ActivityPhasesOfUpdateDeleteAndTransactions) {
	// Update and delete queries on the raft leader, direct and in a transaction, show their phases in #activitystats.
	reindexer::net::ev::dynamic_loop loop;
	auto ports = GetDefaults();
	loop.spawn(exceptionWrapper([&] {
		const std::string ns = "activity_phases";
		const std::string note(kNoteLen, 'n');
		constexpr size_t kClusterSize = 3;
		Cluster cluster(loop, 0, kClusterSize, ports, {ns}, std::chrono::milliseconds(3000), 1);
		const int leaderId = cluster.AwaitLeader(kMaxElectionsTime);
		ASSERT_NE(leaderId, -1);
		auto leader = cluster.GetNode(leaderId);
		ASSERT_TRUE(leader);
		auto& api = leader->api;

		api.OpenNamespace(ns);
		api.DefineNamespaceDataset(
			ns, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"flag", "-", "int", IndexOpts(), 0}});
		insertItems(api, ns, note);
		cluster.WaitSync(ns);

		const auto cfg = api.ExecSQL("UPDATE #config SET profiling.activitystats=true WHERE type='profiling'");
		ASSERT_EQ(cfg.Count(), 1);

		// A second connection: the leader's own client is busy inside the operation being sampled.
		reindexer::client::Reindexer watcher;
		const auto connectErr = watcher.Connect(leader->kRPCDsn);
		ASSERT_TRUE(connectErr.ok()) << connectErr.what();

		const auto updateItems = [&api](const Query& q) { return api.Update(q); };
		const auto deleteItems = [&api](const Query& q) { return api.Delete(q); };
		const auto inTx = [&api, &ns](const Query& q) -> size_t {
			auto tx = api.NewTransaction(ns);
			const auto err = tx.Modify(Query{q});
			EXPECT_TRUE(err.ok()) << err.what();
			return err.ok() ? api.CommitTransaction(tx).Count() : 0;
		};
		const auto noRestore = [] {};
		const auto restoreItems = [&] { insertItems(api, ns, note); };

		{
			auto q = matchingItems(ns, note);
			q.Set("flag", 1);
			expectPhases(watcher, ns, updateItems, q, kItemCount, reindexer::Activity::ItemsModification, noRestore);
		}
		{
			auto q = matchingItems(ns, note);
			q.Set("flag", 2);
			expectPhases(watcher, ns, inTx, q, kItemCount, reindexer::Activity::ItemsModification, noRestore);
		}
		expectPhases(watcher, ns, deleteItems, itemsOfPart(ns, note, 0), kDeleteBatch, reindexer::Activity::ItemsDeletion, restoreItems);
		expectPhases(watcher, ns, inTx, itemsOfPart(ns, note, 1), kDeleteBatch, reindexer::Activity::ItemsDeletion, restoreItems);
	}));
	loop.run();
}

}  // namespace reindexer_tests

#endif	// !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN) && !defined(REINDEX_WITH_UBSAN)
