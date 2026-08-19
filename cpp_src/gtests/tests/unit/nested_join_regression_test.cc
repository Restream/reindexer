#include <limits>
#include <memory>
#include <set>
#include <string_view>

#include "core/nsselecter/joins/results.h"
#include "gtests/tools.h"
#include "join_selects_api.h"
#include "net/ev/ev.h"
#include "rpcclient_api.h"
#include "sharding_api.h"

namespace reindexer_tests {

using namespace reindexer;
using reindexer_tests_tools::exceptionWrapper;

class [[nodiscard]] NestedJoinRegressionApi : public JoinSelectsApi {
protected:
	static constexpr std::string_view kRootNs = "nested_regression_root";
	static constexpr std::string_view kMiddleNs = "nested_regression_middle";
	static constexpr std::string_view kLeafNs = "nested_regression_leaf";

	void PrepareThreeLevelData() {
		rt.OpenNamespace(kRootNs);
		DefineNamespaceDataset(kRootNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
										 IndexDeclaration{"join_key", "hash", "int", IndexOpts(), 0}});

		rt.OpenNamespace(kMiddleNs);
		DefineNamespaceDataset(kMiddleNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
										   IndexDeclaration{"outer_key", "hash", "int", IndexOpts(), 0},
										   IndexDeclaration{"leaf_key", "hash", "int", IndexOpts(), 0}});

		rt.OpenNamespace(kLeafNs);
		DefineNamespaceDataset(
			kLeafNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"kind", "hash", "int", IndexOpts(), 0}});

		rt.UpsertJSON(kRootNs, R"json({"id":1,"join_key":7})json");
		rt.UpsertJSON(kMiddleNs, R"json({"id":10,"outer_key":7,"leaf_key":100})json");
		rt.UpsertJSON(kMiddleNs, R"json({"id":11,"outer_key":7,"leaf_key":200})json");
		rt.UpsertJSON(kLeafNs, R"json({"id":100,"kind":1})json");
		rt.UpsertJSON(kLeafNs, R"json({"id":200,"kind":2})json");
	}

	std::set<int> JoinedMiddleIds(QueryResults& qr) {
		std::set<int> ids;
		if (qr.Count() != 1) {
			ADD_FAILURE() << "Expected exactly one root item, got " << qr.Count();
			return ids;
		}

		auto rootIt = qr.begin();
		auto rootCtx = rootIt.GetJoinedContext();
		if (rootCtx.iterator.GetFieldsCount() != 1) {
			ADD_FAILURE() << "Expected exactly one joined field, got " << rootCtx.iterator.GetFieldsCount();
			return ids;
		}

		auto middleQr = rootCtx.iterator.At(0).ToQueryResults(rootCtx);
		for (auto it : middleQr) {
			auto item = it.GetItem(false);
			ids.emplace(item["id"].As<int>());
		}
		return ids;
	}
};

// A middle row may satisfy "local condition OR nested INNER JOIN" even when the
// nested join itself has no match. Materializing joined data must not turn this
// expression into an unconditional AND over every child processor and every row.
TEST_F(NestedJoinRegressionApi, NestedLogicalOrDoesNotDropWholeParentBatch) {
	PrepareThreeLevelData();

	Query middle = Query(kMiddleNs).Where("id", CondEq, 11).OrInnerJoin("leaf_key", "id", CondEq, Query(kLeafNs).Where("kind", CondEq, 1));
	Query query = Query(kRootNs).InnerJoin("join_key", "outer_key", CondEq, std::move(middle));

	auto qr = rt.Select(query);
	EXPECT_EQ(JoinedMiddleIds(qr), (std::set<int>{10, 11}));
}

// Both queries have the same middle WHERE/ON part, but their nested predicates
// select different middle rows. A cache key which omits the nested query tree
// incorrectly reuses the first result for the second query.
TEST_F(NestedJoinRegressionApi, JoinCacheKeyIncludesNestedQueryTree) {
	PrepareThreeLevelData();
	TurnOnJoinCache(std::string{kMiddleNs});
	AwaitIndexOptimization(kMiddleNs);

	auto selectForLeafKind = [this](int kind) {
		Query middle = Query(kMiddleNs).InnerJoin("leaf_key", "id", CondEq, Query(kLeafNs).Where("kind", CondEq, kind));
		return rt.Select(Query(kRootNs).InnerJoin("join_key", "outer_key", CondEq, std::move(middle)));
	};

	auto kind1 = selectForLeafKind(1);
	ASSERT_EQ(JoinedMiddleIds(kind1), (std::set<int>{10}));
	// Ensure the first key has gone through both cache population and lookup.
	auto kind1Cached = selectForLeafKind(1);
	ASSERT_EQ(JoinedMiddleIds(kind1Cached), (std::set<int>{10}));

	auto kind2 = selectForLeafKind(2);
	EXPECT_EQ(JoinedMiddleIds(kind2), (std::set<int>{11}));
}

TEST(NestedJoinRegression, MutableQueryOptionsReachEveryJoinDepth) {
	Query level1 = Query("nested_options_level_1").InnerJoin("child_id", "id", CondEq, Query("nested_options_level_2"));
	Query root = Query("nested_options_root").InnerJoin("child_id", "id", CondEq, std::move(level1));

	root.Explain().Debug(3).Strict(StrictModeNames);
	ASSERT_EQ(root.GetJoinQueries().size(), 1);
	ASSERT_EQ(root.GetJoinQueries()[0].GetJoinQueries().size(), 1);
	const Query& deepest = root.GetJoinQueries()[0].GetJoinQueries()[0];
	EXPECT_TRUE(deepest.NeedExplain());
	EXPECT_EQ(deepest.GetDebugLevel(), 3);
	EXPECT_EQ(deepest.GetStrictMode(), StrictModeNames);
}

TEST(NestedJoinRegression, JoinedItemsCountIsNotTruncatedAtUint16Boundary) {
	constexpr uint32_t kItemsCount = uint32_t{std::numeric_limits<uint16_t>::max()} + 2;
	const IdType rowId = IdType::FromNumber(1);
	LocalQueryResults joinedQr;
	for (uint32_t i = 0; i < kItemsCount; ++i) {
		joinedQr.AddItemRef(IdType::FromNumber(i), PayloadValue{});
	}

	joins::NamespaceResults results;
	results.SetJoinedFieldsCount(1);
	results.Insert(rowId, 1, 0, std::move(joinedQr));
	const joins::ItemIterator itemIt{&results, rowId};
	EXPECT_EQ(itemIt.At(0).ItemsCount(), static_cast<int>(kItemsCount));
}

// QueryFormatV1 has no recursive framing for joins. A client must either reject
// a nested query during serialization or produce bytes which preserve the tree.
TEST(NestedJoinRegression, QueryFormatV1DoesNotSilentlyFlattenNestedJoin) {
	Query level1 = Query("nested_v1_level_1").InnerJoin("child_id", "id", CondEq, Query("nested_v1_level_2"));
	Query root = Query("nested_v1_root").InnerJoin("child_id", "id", CondEq, std::move(level1));

	WrSerializer wrser;
	try {
		root.Serialize(wrser, Normal, QueryFormatV1);
	} catch (const Error&) {
		return;	 // Explicit rejection is a valid compatibility policy.
	}

	try {
		Serializer ser(wrser.Buf(), wrser.Len());
		const Query decoded = Query::Deserialize(ser, QueryFormatV1);
		EXPECT_EQ(decoded, root);
	} catch (const Error& err) {
		FAIL() << "V1 serialization succeeded, but its output cannot represent the nested query: " << err.what();
	}
}

TEST_F(ShardingApi, NestedJoinRequiresShardKeyAtEveryLevel) {
	InitShardingConfig cfg;
	cfg.nodesInCluster = 1;
	Init(std::move(cfg));
	const auto rx = getNode(0)->api.reindexer;

	Query deepest{default_namespace};  // Sharded namespace without its shard key.
	Query middle = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").InnerJoin(kFieldId, kFieldId, CondEq, std::move(deepest));
	Query root = Query(default_namespace).Where(kFieldLocation, CondEq, "key1").InnerJoin(kFieldId, kFieldId, CondEq, std::move(middle));

	client::QueryResults qr;
	const Error err = rx->Select(root, qr);
	ASSERT_FALSE(err.ok()) << "Nested sharded namespace without a shard key was accepted";
	EXPECT_STREQ(err.what(), "Join query must contain shard key");
}

TEST_F(RPCClientTestApi, CoroJsonJoinedNsIdCacheIncludesJoinedField) {
	StartDefaultRealServer();
	net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop] {
		using client::CoroQueryResults;
		using client::CoroReindexer;

		const std::string leftNs = "joined_nsid_cache_left";
		const std::string rightNs1 = "joined_nsid_cache_right_1";
		const std::string rightNs2 = "joined_nsid_cache_right_2";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		auto createNamespace = [&rx](const std::string& ns) {
			auto err = rx.OpenNamespace(ns);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.AddIndex(ns, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
			ASSERT_TRUE(err.ok()) << err.what();
		};
		createNamespace(leftNs);
		createNamespace(rightNs1);
		createNamespace(rightNs2);

		auto upsertJson = [&rx](const std::string& ns, std::string_view json) {
			auto item = rx.NewItem(ns);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			auto err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.Upsert(ns, item);
			ASSERT_TRUE(err.ok()) << err.what();
		};
		upsertJson(leftNs, R"json({"id":1})json");
		upsertJson(rightNs1, R"json({"id":1,"first_value":"one"})json");
		upsertJson(rightNs2, R"json({"id":1,"second_value":2})json");

		Query query =
			Query(leftNs).Join(InnerJoin, Query(rightNs1)).On("id", CondEq, "id").Join(InnerJoin, Query(rightNs2)).On("id", CondEq, "id");
		CoroQueryResults qr;
		err = rx.Select(query, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 1);

		auto it = qr.begin();
		ASSERT_NE(it, qr.end());
		WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		const auto expected =
			fmt::format(R"json({{"id":1,"joined_{}":[{{"id":1,"first_value":"one"}}],"joined_{}":[{{"id":1,"second_value":2}}]}})json",
						rightNs1, rightNs2);
		EXPECT_EQ(wrser.Slice(), expected);
		rx.Stop();
	}));

	loop.run();
}

std::set<int> JoinedLeafIds(QueryResults& qr) {
	std::set<int> ids;
	if (qr.Count() != 1) {
		ADD_FAILURE() << "Expected exactly one root item, got " << qr.Count();
		return ids;
	}

	auto rootIt = qr.begin();
	auto rootCtx = rootIt.GetJoinedContext();
	if (rootCtx.iterator.GetFieldsCount() != 1) {
		ADD_FAILURE() << "Expected exactly one joined field on root, got " << rootCtx.iterator.GetFieldsCount();
		return ids;
	}

	auto middleQr = rootCtx.iterator.At(0).ToQueryResults(rootCtx);
	for (const auto& middleIt : middleQr) {
		auto leafCtx = middleIt.GetJoinedContext();
		if (leafCtx.iterator.GetFieldsCount() != 1) {
			ADD_FAILURE() << "Expected exactly one nested joined field on middle, got " << leafCtx.iterator.GetFieldsCount();
			continue;
		}
		auto leafQr = leafCtx.iterator.At(0).ToQueryResults(leafCtx);
		for (auto leafIt : leafQr) {
			auto item = leafIt.GetItem(false);
			ids.emplace(item["id"].As<int>());
		}
	}
	return ids;
}

TEST_F(NestedJoinRegressionApi, JoinLongCachePreservesNestedJoinedPayload) {
	PrepareThreeLevelData();
	TurnOnJoinCache(std::string{kMiddleNs});
	AwaitIndexOptimization(kMiddleNs);

	auto runNestedSelect = [this] {
		Query middle = Query(kMiddleNs).InnerJoin("leaf_key", "id", CondEq, Query(kLeafNs));
		return rt.Select(Query(kRootNs).InnerJoin("join_key", "outer_key", CondEq, std::move(middle)));
	};

	const std::set<int> kExpectedMiddle{10, 11};
	const std::set<int> kExpectedLeaf{100, 200};

	// Warm-up lookups until the long-cache entry is eligible for Put.
	for (int i = 0; i < 2; ++i) {
		auto warm = runNestedSelect();
		ASSERT_EQ(JoinedMiddleIds(warm), kExpectedMiddle) << "warm-up select #" << (i + 1);
		ASSERT_EQ(JoinedLeafIds(warm), kExpectedLeaf) << "warm-up select #" << (i + 1);
	}

	// This select must take the joinResLong.haveData path (IdSet restore only).
	auto cached = runNestedSelect();
	ASSERT_EQ(JoinedMiddleIds(cached), kExpectedMiddle);
	EXPECT_EQ(JoinedLeafIds(cached), kExpectedLeaf) << "Nested leaf joined items must be materialized on join long-cache hit "
													   "(CacheVal only restores middle IdSet today)";
}

TEST_F(NestedJoinRegressionApi, NestedJoinSubqueryOffsetDoesNotAffectPerItemSelect) {
	PrepareThreeLevelData();

	const std::set<int> kExpectedMiddle{10, 11};

	{
		Query middle = Query(kMiddleNs).Offset(2);
		auto flat = rt.Select(Query(kRootNs).InnerJoin("join_key", "outer_key", CondEq, std::move(middle)));
		ASSERT_EQ(flat.Count(), 1);
		ASSERT_EQ(JoinedMiddleIds(flat), kExpectedMiddle) << "flat join must ignore subquery Offset in per-item Select";
	}

	Query middle = Query(kMiddleNs).Offset(2).InnerJoin("leaf_key", "id", CondEq, Query(kLeafNs));
	auto nested = rt.Select(Query(kRootNs).InnerJoin("join_key", "outer_key", CondEq, std::move(middle)));
	ASSERT_EQ(nested.Count(), 1);
	EXPECT_EQ(JoinedMiddleIds(nested), kExpectedMiddle) << "nested join must ignore subquery Offset in per-item Select "
														   "(Offset currently leaks from JoinedQuery copy into itemQuery; "
														   "PreSelect has Offset cleared, so the root row survives with empty joins)";
}

}  // namespace reindexer_tests
