#include "reindexer_api.h"
#include "core/system_ns_names.h"

using namespace reindexer;

void ReindexerApi::AwaitIndexOptimization(std::string_view nsName) {
	bool optimization_completed = false;
	unsigned waitForIndexOptimizationCompleteIterations = 0;
	while (!optimization_completed) {
		ASSERT_LT(waitForIndexOptimizationCompleteIterations++, 200) << "Too long index optimization";
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		auto qr = rt.Select(Query(kMemStatsNamespace).Where("name", CondEq, nsName));
		ASSERT_EQ(1, qr.Count());
		optimization_completed = qr.begin().GetItem(false)["optimization_completed"].Get<bool>();
	}
}

void ReindexerApi::initializeDefaultNs() {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled());
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
}

reindexer::Item ReindexerApi::getMemStat(Reindexer& rx, std::string_view ns) {
	QueryResults qr;
	auto err = rx.Select(Query(kMemStatsNamespace).Where("name", CondEq, ns), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1);
	return qr.begin().GetItem(false);
}
