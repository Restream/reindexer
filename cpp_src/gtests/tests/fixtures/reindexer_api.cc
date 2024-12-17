#include "reindexer_api.h"

void ReindexerApi::DefineNamespaceDataset(std::string_view ns, std::initializer_list<const IndexDeclaration> fields) {
	rt.DefineNamespaceDataset(ns, fields);
}

void ReindexerApi::DefineNamespaceDataset(std::string_view ns, const std::vector<IndexDeclaration>& fields) {
	rt.DefineNamespaceDataset(ns, fields);
}

void ReindexerApi::DefineNamespaceDataset(Reindexer& rx, const std::string& ns, std::initializer_list<const IndexDeclaration> fields) {
	rt.DefineNamespaceDataset(rx, ns, fields);
}

void ReindexerApi::AwaitIndexOptimization(const std::string& nsName) {
	bool optimization_completed = false;
	unsigned waitForIndexOptimizationCompleteIterations = 0;
	while (!optimization_completed) {
		ASSERT_LT(waitForIndexOptimizationCompleteIterations++, 200) << "Too long index optimization";
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		reindexer::QueryResults qr;
		Error err = rt.reindexer->Select(Query("#memstats").Where("name", CondEq, nsName), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(1, qr.Count());
		optimization_completed = qr.begin().GetItem(false)["optimization_completed"].Get<bool>();
	}
}

void ReindexerApi::initializeDefaultNs() {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled());
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
}

reindexer::Item ReindexerApi::getMemStat(Reindexer& rx, const std::string& ns) {
	QueryResults qr;
	auto err = rx.Select(Query("#memstats").Where("name", CondEq, ns), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1);
	return qr.begin().GetItem(false);
}
