#include "reindexer_api.h"
#include "core/system_ns_names.h"

namespace reindexer_tests {

using namespace reindexer;

void ReindexerApi::initializeDefaultNs() {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled());
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
}

Item ReindexerApi::getMemStat(Reindexer& rx, std::string_view ns) {
	QueryResults qr;
	auto err = rx.Select(Query(kMemStatsNamespace).Where("name", CondEq, ns), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1);
	return qr.begin().GetItem(false);
}

}  // namespace reindexer_tests
