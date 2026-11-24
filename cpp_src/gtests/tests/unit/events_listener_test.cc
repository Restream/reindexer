#include <future>
#include <iostream>
#include <thread>
#include "events/listener.h"
#include "gtest/gtest.h"
#include "tools/clock.h"

using reindexer::EventsListener;

namespace {

constexpr auto kTimeout = std::chrono::seconds(10);
constexpr int64_t kUnlimited = 1'000'000;
constexpr size_t kQueueSizeLimit = 1024 * 1024;
constexpr std::string_view kAllEventsConfig = R"({"version":1,"streams":[{"id":0}]})";

class [[nodiscard]] TestObserver final : public reindexer::IEventsObserver {
public:
	explicit TestObserver(int64_t budget) noexcept : budget_(budget) {}

	size_t AvailableEventsSpace() noexcept override {
		const auto budget = budget_.load();
		return budget > 0 ? size_t(budget) : 0;
	}
	void SendEvent(uint32_t streamsMask, const reindexer::EventsSerializationOpts&, const reindexer::EventRecord& rec) override {
		budget_.fetch_sub(1);
		if (rec.IsResyncOnUpdatesDropRecord()) {
			drops_.fetch_add(1);
			dropStreams_.fetch_or(streamsMask);
		} else {
			events_.fetch_add(1);
		}
	}

	void SetBudget(int64_t budget) noexcept { budget_.store(budget); }
	int64_t Events() const noexcept { return events_.load(); }
	int64_t Drops() const noexcept { return drops_.load(); }
	uint32_t DropStreams() const noexcept { return dropStreams_.load(); }

private:
	std::atomic<int64_t> budget_;
	std::atomic<int64_t> events_ = {0};
	std::atomic<int64_t> drops_ = {0};
	std::atomic<uint32_t> dropStreams_ = {0};
};

void subscribe(EventsListener& listener, TestObserver& observer, std::string_view config = kAllEventsConfig) {
	std::string json(config);
	reindexer::EventSubscriberConfig cfg;
	auto err = cfg.FromJSON(std::span(json.data(), json.size()));
	ASSERT_TRUE(err.ok()) << err.what();
	err = listener.AddOrUpdate(observer, std::move(cfg));
	ASSERT_TRUE(err.ok()) << err.what();
}

void unsubscribe(EventsListener& listener, TestObserver& observer) {
	auto result = std::make_shared<std::promise<reindexer::Error>>();
	auto future = result->get_future();
	std::thread([&listener, &observer, result] { result->set_value(listener.Remove(observer)); }).detach();
	if (future.wait_for(kTimeout) != std::future_status::ready) {
		// Stuck Remove() can not be interrupted and the listener's destructor would hang on it too
		std::cerr << "EventsListener::Remove() has not finished in " << kTimeout.count() << "s" << std::endl;
		std::abort();
	}
	const auto err = future.get();
	ASSERT_TRUE(err.ok()) << err.what();
}

void pushEvents(EventsListener& listener, size_t count, size_t valueSize = 8) {
	using namespace reindexer;
	for (size_t i = 0; i < count; ++i) {
		EventsContainer recs;
		recs.emplace_back(
			updates::UpdateRecord(updates::URType::PutMeta, NamespaceName("ns"), lsn_t(), lsn_t(), -1, "key", std::string(valueSize, 'v')));
		const auto err = listener.SendEvents(std::move(recs));
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

template <typename F>
bool waitFor(F&& condition) {
	const auto deadline = reindexer::steady_clock_w::now() + kTimeout;
	while (!condition()) {
		if (reindexer::steady_clock_w::now() > deadline) {
			return false;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	return true;
}

}  // namespace

TEST(EventsListener, UnsubscribeSlowestWhileOthersAreUpToDate) {
	TestObserver fast(kUnlimited), slow(10);
	EventsListener listener("test_db", 0);
	subscribe(listener, fast);
	subscribe(listener, slow);
	pushEvents(listener, 100);
	ASSERT_TRUE(waitFor([&] { return fast.Events() == 100 && slow.Events() == 10; }));

	unsubscribe(listener, slow);
	unsubscribe(listener, fast);
}

TEST(EventsListener, UnsubscribeSlowestWhileOthersLag) {
	TestObserver lagging(30), slowest(10);
	EventsListener listener("test_db", 0);
	subscribe(listener, lagging);
	subscribe(listener, slowest);
	pushEvents(listener, 100);
	ASSERT_TRUE(waitFor([&] { return lagging.Events() == 30 && slowest.Events() == 10; }));

	unsubscribe(listener, slowest);
	lagging.SetBudget(kUnlimited);
	EXPECT_TRUE(waitFor([&] { return lagging.Events() == 100; })) << lagging.Events();
	unsubscribe(listener, lagging);
}

TEST(EventsListener, UnsubscribeLastLaggingDuringWrites) {
	TestObserver lagging(10);
	EventsListener listener("test_db", 0);
	subscribe(listener, lagging);
	pushEvents(listener, 100);
	ASSERT_TRUE(waitFor([&] { return lagging.Events() == 10; }));

	std::atomic<bool> stop = {false};
	std::thread writer([&] {
		while (!stop.load()) {
			pushEvents(listener, 1);
		}
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	unsubscribe(listener, lagging);
	stop.store(true);
	writer.join();
}

TEST(EventsListener, LateSubscriptionDoesNotLoseEvents) {
	TestObserver early(250), late(kUnlimited);
	EventsListener listener("test_db", 0);
	subscribe(listener, early);
	pushEvents(listener, 300);
	ASSERT_TRUE(waitFor([&] { return early.Events() == 250; }));

	subscribe(listener, late);
	pushEvents(listener, 10);
	ASSERT_TRUE(waitFor([&] { return late.Events() == 10; }));

	early.SetBudget(kUnlimited);
	EXPECT_TRUE(waitFor([&] { return early.Events() == 310; })) << early.Events();
	unsubscribe(listener, late);
	unsubscribe(listener, early);
}

TEST(EventsListener, UnsubscribeAfterLateSubscription) {
	TestObserver early(250), late(kUnlimited);
	EventsListener listener("test_db", 0);
	subscribe(listener, early);
	pushEvents(listener, 300);
	ASSERT_TRUE(waitFor([&] { return early.Events() == 250; }));

	subscribe(listener, late);
	// The late subscriber does not receive older events, so there is nothing to wait for
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	unsubscribe(listener, late);
	unsubscribe(listener, early);
}

TEST(EventsListener, UnsubscribeOfNotSlowestDoesNotLeakQueue) {
	TestObserver fast(kUnlimited), middle(300), slowest(100);
	EventsListener listener("test_db", kQueueSizeLimit);
	subscribe(listener, fast);
	subscribe(listener, middle);
	subscribe(listener, slowest);
	pushEvents(listener, 1001);
	ASSERT_TRUE(waitFor([&] { return fast.Events() == 1001 && middle.Events() == 300 && slowest.Events() == 100; }));

	unsubscribe(listener, middle);
	slowest.SetBudget(kUnlimited);
	int64_t expected = 1001;
	for (int i = 0; i < 3; ++i) {
		pushEvents(listener, 333, 256);
		expected += 333;
		ASSERT_TRUE(waitFor([&] { return fast.Events() == expected && slowest.Events() == expected; }));
	}

	// Unread events alone fit into the queue limit, but not together with the leaked (already read) ones
	fast.SetBudget(0);
	slowest.SetBudget(0);
	pushEvents(listener, 1000, 256);
	expected += 1000;
	fast.SetBudget(kUnlimited);
	slowest.SetBudget(kUnlimited);
	EXPECT_TRUE(waitFor([&] { return fast.Events() == expected && slowest.Events() == expected; }))
		<< fast.Events() << " " << slowest.Events() << " / " << expected;
	EXPECT_EQ(fast.Drops(), 0);
	EXPECT_EQ(slowest.Drops(), 0);
}

TEST(EventsListener, QueueOverflowIsReportedOnce) {
	TestObserver observer(0);
	EventsListener listener("test_db", kQueueSizeLimit);
	subscribe(listener, observer);
	pushEvents(listener, 3000, 1024);

	observer.SetBudget(1000);
	EXPECT_TRUE(waitFor([&] { return observer.Events() > 0; }));
	EXPECT_EQ(observer.Drops(), 1);
	unsubscribe(listener, observer);
}

TEST(EventsListener, SubscriptionAfterQueueOverflow) {
	TestObserver stuck(0), late(kUnlimited);
	EventsListener listener("test_db", kQueueSizeLimit);
	subscribe(listener, stuck);
	pushEvents(listener, 3000, 1024);

	subscribe(listener, late);
	pushEvents(listener, 10);
	EXPECT_TRUE(waitFor([&] { return late.Events() == 10; })) << late.Events();
	EXPECT_EQ(late.Drops(), 0);
	unsubscribe(listener, late);
}

TEST(EventsListener, QueueOverflowIsReportedToAllStreams) {
	TestObserver observer(0);
	EventsListener listener("test_db", kQueueSizeLimit);
	subscribe(listener, observer,
			  R"({"version":1,"streams":[{"id":0,"event_types":["PutMeta"]},)"
			  R"({"id":1,"namespaces":[{"name":"other_ns"}],"event_types":["ItemUpsert"]}]})");
	pushEvents(listener, 3000, 1024);

	observer.SetBudget(1000);
	EXPECT_TRUE(waitFor([&] { return observer.Events() > 0; }));
	EXPECT_EQ(observer.Drops(), 1);
	EXPECT_EQ(observer.DropStreams(), 0b11u);
	unsubscribe(listener, observer);
}

TEST(EventsListener, HasListenersForRespectsNamespaces) {
	using reindexer::NamespaceName;
	TestObserver first(kUnlimited), second(kUnlimited), third(kUnlimited);
	EventsListener listener("test_db", 0);
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("ns_a")));

	subscribe(listener, first, R"({"version":1,"streams":[{"id":0,"namespaces":[{"name":"ns_a"}]}]})");
	EXPECT_TRUE(listener.HasListenersFor(NamespaceName("ns_a")));
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("ns_b")));

	subscribe(listener, second, R"({"version":1,"streams":[{"id":0,"namespaces":[{"name":"ns_b"}]}]})");
	EXPECT_TRUE(listener.HasListenersFor(NamespaceName("ns_a")));
	EXPECT_TRUE(listener.HasListenersFor(NamespaceName("ns_b")));
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("ns_c")));

	subscribe(listener, third);
	EXPECT_TRUE(listener.HasListenersFor(NamespaceName("ns_c")));

	unsubscribe(listener, third);
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("ns_c")));
	unsubscribe(listener, second);
	unsubscribe(listener, first);
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("ns_a")));
}

TEST(EventsListener, HasListenersForConfigNamespaceWithNamespacesFilter) {
	using reindexer::NamespaceName;
	TestObserver observer(kUnlimited);
	EventsListener listener("test_db", 0);
	subscribe(listener, observer, R"({"version":1,"streams":[{"id":0,"namespaces":[{"name":"ns_a"}]}]})");
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("#config")));

	subscribe(listener, observer, R"({"version":1,"streams":[{"id":0,"namespaces":[{"name":"ns_a"}],"with_config_namespace":true}]})");
	EXPECT_TRUE(listener.HasListenersFor(NamespaceName("#config")));
	EXPECT_TRUE(listener.HasListenersFor(NamespaceName("ns_a")));
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("ns_b")));
	EXPECT_FALSE(listener.HasListenersFor(NamespaceName("#memstats")));
	unsubscribe(listener, observer);
}
