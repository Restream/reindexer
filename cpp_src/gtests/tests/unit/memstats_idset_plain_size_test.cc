#include "core/idset/idset.h"
#include "core/system_ns_names.h"
#include "reindexer_api.h"
#include "tools/scope_guard.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests {

namespace {

struct [[nodiscard]] MemStatsSizes {
	int64_t indexesSize = 0;
	int64_t tagIdsetPlainSize = 0;
	bool foundTagIndex = false;
	std::string json;
};

MemStatsSizes ReadTagIndexMemStats(reindexer::Reindexer& rx, std::string_view ns, std::string_view tagField) {
	// Keep QueryResults alive until GetJSON() is copied — Item from getMemStat() would UAF under ASAN.
	reindexer::QueryResults qr;
	auto err = rx.Select(reindexer::Query(reindexer::kMemStatsNamespace).Where("name", CondEq, ns), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1);

	MemStatsSizes out;
	out.json = std::string(qr.begin().GetItem(false).GetJSON());
	gason::JsonParser parser;
	auto root = parser.Parse(std::string_view(out.json));
	out.indexesSize = root["total"]["indexes_size"].As<int64_t>();
	for (const auto& idx : root["indexes"]) {
		if (idx["name"].As<std::string_view>() == tagField) {
			out.foundTagIndex = true;
			out.tagIdsetPlainSize = idx["idset_plain_size"].As<int64_t>();
			break;
		}
	}
	return out;
}

void FillSharedTagNs(ReindexerApi& api, std::string_view ns, std::string_view idField, std::string_view tagField, size_t items,
					 int sharedTag) {
	for (size_t i = 0; i < items; ++i) {
		auto item = api.NewItem(ns);
		item[idField] = static_cast<int>(i);
		item[tagField] = sharedTag;
		api.Upsert(ns, item);
	}
}

}  // namespace

// Regression: IdSet::Add clear() frees plain heap under wlock; Commit under rlock rebuilds it;
// memStat must account for that delta so Delete cannot underflow idset_plain_size (#2384).
TEST_F(ReindexerApi, MemStatsIdsetPlainSizeAfterBtreeCommit) {
	using reindexer::IndexOpts;
	using reindexer::Query;
	using reindexer::kMaxPlainIdsetSize;

	constexpr std::string_view kNs = "memstats_idset_plain_ns";
	constexpr std::string_view kFieldId = "id";
	constexpr std::string_view kFieldTag = "tag";
	constexpr int kSharedTag = 42;
	constexpr size_t kItems = kMaxPlainIdsetSize + 1;

	rt.OpenNamespace(kNs, StorageOpts().Enabled(false));
	DefineNamespaceDataset(
		kNs, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTag, "hash", "int", IndexOpts(), 0}});

	FillSharedTagNs(*this, kNs, kFieldId, kFieldTag, kItems, kSharedTag);
	AwaitIndexOptimization(kNs);

	{
		const auto beforeDelete = ReadTagIndexMemStats(*rt.reindexer, kNs, kFieldTag);
		SCOPED_TRACE(beforeDelete.json);
		ASSERT_TRUE(beforeDelete.foundTagIndex);
		EXPECT_GE(beforeDelete.tagIdsetPlainSize, 0);
		EXPECT_GE(beforeDelete.indexesSize, 0);
		EXPECT_GE(beforeDelete.tagIdsetPlainSize, static_cast<int64_t>(kItems * sizeof(reindexer::IdType)));
	}

	ASSERT_GT(Delete(Query(kNs).Where(kFieldTag, CondEq, kSharedTag)), 0);

	{
		const auto afterDelete = ReadTagIndexMemStats(*rt.reindexer, kNs, kFieldTag);
		SCOPED_TRACE(afterDelete.json);
		ASSERT_TRUE(afterDelete.foundTagIndex);
		EXPECT_GE(afterDelete.tagIdsetPlainSize, 0);
		EXPECT_GE(afterDelete.indexesSize, 0);
	}
}

TEST_F(ReindexerApi, MemStatsIdsetPlainSizeAfterSetSortedIdxCount) {
	using reindexer::IndexOpts;
	using reindexer::Query;
	using reindexer::IndexDef;
	using reindexer::kMaxPlainIdsetSize;

	constexpr std::string_view kNs = "memstats_idset_setsorted_ns";
	constexpr std::string_view kFieldId = "id";
	constexpr std::string_view kFieldTag = "tag";
	constexpr std::string_view kFieldSort = "sort";
	constexpr int kSharedTag = 11;
	constexpr size_t kItems = kMaxPlainIdsetSize + 1;

	rt.OpenNamespace(kNs, StorageOpts().Enabled(false));
	DefineNamespaceDataset(
		kNs, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTag, "hash", "int", IndexOpts(), 0}});

	FillSharedTagNs(*this, kNs, kFieldId, kFieldTag, kItems, kSharedTag);
	AwaitIndexOptimization(kNs);

	// Triggers SetSortedIdxCount / OnSortedIndexCountChanged (capacity change without Upsert).
	rt.AddIndex(kNs, IndexDef{std::string(kFieldSort), {std::string(kFieldSort)}, "tree", "int", IndexOpts()});
	AwaitIndexOptimization(kNs);

	ASSERT_GT(Delete(Query(kNs).Where(kFieldTag, CondEq, kSharedTag)), 0);

	const auto afterDelete = ReadTagIndexMemStats(*rt.reindexer, kNs, kFieldTag);
	SCOPED_TRACE(afterDelete.json);
	ASSERT_TRUE(afterDelete.foundTagIndex);
	EXPECT_GE(afterDelete.tagIdsetPlainSize, 0);
	EXPECT_GE(afterDelete.indexesSize, 0);
}

// Logical clone (ns copy on TX) must preserve idsetPlainSizeBytes_ for copied idx_map
// (minus pkSortedIds_ contribution, which is not copied on Logical).
TEST_F(ReindexerApi, MemStatsIdsetPlainSizeAfterLogicalClone) {
	using reindexer::IndexOpts;
	using reindexer::Query;
	using reindexer::kConfigNamespace;
	using reindexer::kMaxPlainIdsetSize;

	constexpr std::string_view kNs = "memstats_idset_logical_clone_ns";
	constexpr std::string_view kFieldId = "id";
	constexpr std::string_view kFieldTag = "tag";
	constexpr int kSharedTag = 99;
	constexpr size_t kItems = kMaxPlainIdsetSize + 1;
	const int64_t kMinPlainFloor = static_cast<int64_t>(kItems * sizeof(reindexer::IdType));

	rt.OpenNamespace(kNs, StorageOpts().Enabled(false));
	DefineNamespaceDataset(
		kNs, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldTag, "hash", "int", IndexOpts(), 0}});

	FillSharedTagNs(*this, kNs, kFieldId, kFieldTag, kItems, kSharedTag);
	AwaitIndexOptimization(kNs);

	const auto before = ReadTagIndexMemStats(*rt.reindexer, kNs, kFieldTag);
	{
		SCOPED_TRACE(before.json);
		ASSERT_TRUE(before.foundTagIndex);
		ASSERT_GE(before.tagIdsetPlainSize, kMinPlainFloor);
	}

	ASSERT_GT(rt.Update(Query(kConfigNamespace).Set("namespaces[*].tx_size_to_always_copy", 1).Where("type", CondEq, "namespaces")), 0);
	auto cfgGuard = reindexer::MakeScopeGuard([this] {
		std::ignore =
			rt.Update(Query(kConfigNamespace).Set("namespaces[*].tx_size_to_always_copy", 100000).Where("type", CondEq, "namespaces"));
	});

	// Heuristic: ns copy is skipped until there was at least one select.
	std::ignore = rt.Select(Query(kNs).Limit(1));

	{
		auto tx = rt.NewTransaction(kNs);
		auto item = tx.NewItem();
		item[kFieldId] = static_cast<int>(kItems);
		item[kFieldTag] = kSharedTag + 1;
		auto err = tx.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
		std::ignore = rt.CommitTransaction(tx);
	}

	{
		const auto afterClone = ReadTagIndexMemStats(*rt.reindexer, kNs, kFieldTag);
		SCOPED_TRACE(afterClone.json);
		ASSERT_TRUE(afterClone.foundTagIndex);
		// Shared-tag idset is still in the copied map with large plain heap — counter must not reset to ~0.
		EXPECT_GE(afterClone.tagIdsetPlainSize, kMinPlainFloor) << "before.tagIdsetPlainSize=" << before.tagIdsetPlainSize;
	}

	ASSERT_GT(Delete(Query(kNs).Where(kFieldTag, CondEq, kSharedTag)), 0);

	{
		const auto afterDelete = ReadTagIndexMemStats(*rt.reindexer, kNs, kFieldTag);
		SCOPED_TRACE(afterDelete.json);
		ASSERT_TRUE(afterDelete.foundTagIndex);
		EXPECT_GE(afterDelete.tagIdsetPlainSize, 0);
		EXPECT_GE(afterDelete.indexesSize, 0);
	}
}

}  // namespace reindexer_tests
