#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <set>
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/index/index.h"
#include "core/namespace/itemsloader.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/namespace/stringsholder.h"
#include "core/system_ns_names.h"
#include "json_helpers.h"
#include "reindexertestapi.h"
#include "tools/fsops.h"
#include "vendor/gason/gason.h"

namespace reindexer_tests {

using reindexer::IndexDef;
using reindexer::IndexOpts;
using reindexer::Query;
using reindexer::QueryResults;

class [[nodiscard]] FtIndexingConditionTestBase : public ::testing::Test, public ReindexerTestApi<reindexer::Reindexer> {
protected:
	void ConnectIsolated(std::string_view suiteName) {
		storagePath_ =
			reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), std::string("reindex/FtIndexingCondition/") + std::string(suiteName));
		std::ignore = reindexer::fs::RmDirAll(storagePath_);
		reindexer = std::make_shared<reindexer::Reindexer>();
		Connect(std::string("builtin://") + storagePath_);
	}

	void Restart(std::string_view ns) {
		reindexer.reset();
		reindexer = std::make_shared<reindexer::Reindexer>();
		Connect(std::string("builtin://") + storagePath_);
		OpenNamespace(ns);
	}

	void TearDown() override {
		reindexer.reset();
		if (!storagePath_.empty()) {
			std::ignore = reindexer::fs::RmDirAll(storagePath_);
		}
	}

	void CheckFtMemStats(std::string_view indexName, int64_t itemsCount, int64_t vdocsCount) {
		SCOPED_TRACE(indexName);
		auto qr = Select(Query(reindexer::kMemStatsNamespace).Where("name", CondEq, nsName()));
		ASSERT_EQ(qr.Count(), 1);
		const std::string json(qr.begin().GetItem().GetJSON());
		gason::JsonParser parser;
		const auto root = parser.Parse(std::string_view(json));
		for (const auto& index : root["indexes"]) {
			if (index["name"].As<std::string_view>() == indexName) {
				const auto& stats = index["text_index_stats"];
				ASSERT_FALSE(stats["indexed_items_count"].isEmpty());
				EXPECT_EQ(stats["indexed_items_count"].As<int64_t>(), itemsCount);
				EXPECT_EQ(stats["total_vdocs"].As<int64_t>(), vdocsCount);
				return;
			}
		}
		FAIL() << "Index not found in #memstats: " << indexName;
	}

	virtual std::string_view nsName() const noexcept = 0;

private:
	std::string storagePath_;
};

class [[nodiscard]] FtIndexingConditionApi : public FtIndexingConditionTestBase {
public:
	static constexpr std::string_view kNs = "ft_indexing_condition_ns";
	std::string_view nsName() const noexcept override { return kNs; }

	void SetUp() override {
		ConnectIsolated(kNs);
		OpenNamespace(kNs);
		DefineNamespaceDataset(
			kNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"type", "hash", "string", IndexOpts(), 0},
				  IndexDeclaration{"score", "tree", "int", IndexOpts(), 0}});

		IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"};
		AddIndex(kNs, ftIdx);
	}

	void UpsertItem(int id, std::string_view type, std::string_view title, int score = 0) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);
		jb.Put("id", id);
		jb.Put("type", type);
		jb.Put("title", title);
		jb.Put("score", score);
		jb.End();
		auto item = NewItem(kNs);
		ASSERT_TRUE(item.FromJSON(ser.Slice()).ok());
		Upsert(kNs, item);
	}

	void CheckIndexDefinitions(const std::vector<IndexDef>& expected) {
		const auto namespaces = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
		ASSERT_EQ(namespaces.size(), 1);
		const auto& actual = namespaces[0].indexes;
		ASSERT_EQ(actual.size(), expected.size());
		for (const auto& expectedIndex : expected) {
			const auto found = std::find_if(actual.begin(), actual.end(),
											[&expectedIndex](const IndexDef& index) { return index.Name() == expectedIndex.Name(); });
			ASSERT_NE(found, actual.end()) << expectedIndex.Name();
			EXPECT_TRUE(found->Compare(expectedIndex).Equal()) << expectedIndex.Name();
		}
	}

	QueryResults SelectByTitle(std::string_view dsl) { return Select(Query(kNs).Where("title", CondEq, dsl)); }
};

TEST_F(FtIndexingConditionApi, IndexesOnlyMatchingDocuments) {
	UpsertItem(1, "series", "word alpha");
	UpsertItem(2, "episode", "word beta");
	UpsertItem(3, "series", "word gamma");

	auto qr = SelectByTitle("word");
	ASSERT_EQ(qr.Count(), 2);
	std::set<int> ids;
	for (auto it : qr) {
		const reindexer::Item item = it.GetItem();
		ids.insert(item["id"].As<int>());
	}
	EXPECT_EQ(ids, (std::set<int>{1, 3}));

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	EXPECT_EQ(SelectByTitle("word").Count(), 1);
	EXPECT_EQ(SelectByTitle("alpha").Count(), 0);
	EXPECT_EQ(SelectByTitle("gamma").Count(), 1);
}

TEST_F(FtIndexingConditionApi, SupportsWhereComparisonOperatorsAndBooleanExpressions) {
	IndexDef ftIdx{"description",
				   {"description"},
				   "text",
				   "string",
				   IndexOpts{},
				   0,
				   "id = 1 AND id <> 2 AND score > 0 AND score < 3 AND score >= 1 AND score <= 2 AND id IN (1, 2) AND score RANGE (1, 2)"};
	AddIndex(kNs, ftIdx);
	IndexDef orIdx{"or_text", {"or_text"}, "text", "string", IndexOpts{}, 0, "id = 1 OR id = 3"};
	AddIndex(kNs, orIdx);
	IndexDef notIdx{"not_text", {"not_text"}, "text", "string", IndexOpts{}, 0, "NOT id = 1"};
	AddIndex(kNs, notIdx);

	UpsertJSON(
		kNs,
		R"json({"id":1,"type":"series","score":1,"title":"operator title","description":"operator match","or_text":"boolean match","not_text":"negation excluded"})json");
	UpsertJSON(
		kNs,
		R"json({"id":2,"type":"series","score":2,"title":"operator title","description":"operator excluded","or_text":"boolean excluded","not_text":"negation match"})json");

	auto qr = Select(Query(kNs).Where("description", CondEq, "operator"));
	ASSERT_EQ(qr.Count(), 1);
	for (auto it : qr) {
		EXPECT_EQ(it.GetItem()["id"].As<int>(), 1);
	}
	EXPECT_EQ(Select(Query(kNs).Where("or_text", CondEq, "boolean")).Count(), 1);
	qr = Select(Query(kNs).Where("not_text", CondEq, "negation"));
	ASSERT_EQ(qr.Count(), 1);
	for (auto it : qr) {
		EXPECT_EQ(it.GetItem()["id"].As<int>(), 2);
	}

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	EXPECT_EQ(Select(Query(kNs).Where("description", CondEq, "operator")).Count(), 0);
	EXPECT_EQ(Select(Query(kNs).Where("or_text", CondEq, "boolean")).Count(), 0);
	EXPECT_EQ(SelectByTitle("operator").Count(), 1);
	EXPECT_EQ(Select(Query(kNs).Where("not_text", CondEq, "negation")).Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	EXPECT_EQ(Select(Query(kNs).Where("not_text", CondEq, "negation")).Count(), 0);
}

TEST_F(FtIndexingConditionApi, SupportsNotWithBooleanExpressions) {
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "NOT (type = 'episode' OR score < 2)"};
	UpdateIndex(kNs, ftIdx);

	UpsertItem(1, "series", "boolean token", 2);
	UpsertItem(2, "episode", "boolean token", 2);
	UpsertItem(3, "series", "boolean token", 1);
	UpsertItem(4, "series", "boolean token", 3);

	auto qr = SelectByTitle("boolean");
	ASSERT_EQ(qr.Count(), 2);
	std::set<int> ids;
	for (auto it : qr) {
		ids.insert(it.GetItem()["id"].As<int>());
	}
	EXPECT_EQ(ids, (std::set<int>{1, 4}));

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	qr = SelectByTitle("boolean");
	ASSERT_EQ(qr.Count(), 1);
	for (auto it : qr) {
		EXPECT_EQ(it.GetItem()["id"].As<int>(), 4);
	}
}

TEST_F(FtIndexingConditionApi, UpdateRemovesFromIndexWhenConditionFails) {
	UpsertItem(1, "series", "unique token one");
	UpsertItem(2, "series", "unique token two");

	EXPECT_EQ(SelectByTitle("unique").Count(), 2);

	UpsertItem(2, "episode", "unique token two");
	EXPECT_EQ(SelectByTitle("unique").Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	EXPECT_EQ(SelectByTitle("unique").Count(), 1);
}

TEST_F(FtIndexingConditionApi, UpdateAddsToIndexWhenConditionPasses) {
	UpsertItem(1, "episode", "appearing token");
	EXPECT_EQ(SelectByTitle("appearing").Count(), 0);

	UpsertItem(1, "series", "appearing token");
	EXPECT_EQ(SelectByTitle("appearing").Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	EXPECT_EQ(SelectByTitle("appearing").Count(), 0);
}

TEST_F(FtIndexingConditionApi, UpdateQueryRemovesFromIndexWhenConditionFails) {
	UpsertItem(1, "series", "updatequery token one");
	UpsertItem(2, "series", "updatequery token two");
	ASSERT_EQ(SelectByTitle("updatequery").Count(), 2);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("type", {"episode"})), 1);
	EXPECT_EQ(SelectByTitle("updatequery").Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	EXPECT_EQ(SelectByTitle("updatequery").Count(), 1);
}

TEST_F(FtIndexingConditionApi, UpdateQueryAddsToIndexWhenConditionPasses) {
	UpsertItem(1, "episode", "updatequery add token");
	ASSERT_EQ(SelectByTitle("updatequery").Count(), 0);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"series"})), 1);

	auto qr = Select(Query(kNs).Where("id", CondEq, 1));
	ASSERT_EQ(qr.Count(), 1);
	for (auto it : qr) {
		EXPECT_EQ(it.GetItem()["type"].As<std::string>(), "series");
		break;
	}

	EXPECT_EQ(SelectByTitle("updatequery").Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	EXPECT_EQ(SelectByTitle("updatequery").Count(), 0);
}

TEST_F(FtIndexingConditionApi, DeleteIndexedAndNonIndexedDocumentsSucceeds) {
	UpsertItem(1, "episode", "delete token");
	EXPECT_EQ(SelectByTitle("delete").Count(), 0);
	UpsertItem(2, "series", "delete token");
	EXPECT_EQ(SelectByTitle("delete").Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	EXPECT_EQ(SelectByTitle("delete").Count(), 1);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	EXPECT_EQ(SelectByTitle("delete").Count(), 0);
	EXPECT_EQ(Select(Query(kNs)).Count(), 0);
}

TEST_F(FtIndexingConditionApi, MemStatsCountsScalarAndCompositeItems) {
	IndexDef compositeIdx{"search", {"description", "notes"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, compositeIdx);
	const auto checkStats = [&](int64_t itemsCount, int64_t vdocsCount) {
		CheckFtMemStats("title", itemsCount, vdocsCount);
		CheckFtMemStats("search", itemsCount, vdocsCount);
	};
	const auto upsertItem = [&](int id, std::string_view type, std::string_view text) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);
		jb.Put("id", id);
		jb.Put("type", type);
		jb.Put("title", text);
		jb.Put("description", text);
		jb.End();
		UpsertJSON(kNs, ser.Slice());
	};
	checkStats(0, 0);

	upsertItem(1, "series", "shared token");
	upsertItem(2, "series", "shared token");
	upsertItem(3, "episode", "shared token");
	checkStats(2, 1);
	EXPECT_EQ(SelectByTitle("shared").Count(), 2);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "shared")).Count(), 2);
	checkStats(2, 1);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("type", {"episode"})), 1);
	checkStats(1, 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 3).Set("type", {"series"})), 1);
	checkStats(2, 1);
	upsertItem(1, "series", "changed token");
	checkStats(2, 2);

	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	checkStats(2, 2);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 3)), 1);
	checkStats(1, 1);
	Restart(kNs);
	checkStats(1, 1);

	upsertItem(4, "episode", "shared token");
	checkStats(1, 1);
	for (const auto condition : {"id = 4", ""}) {
		UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
		UpdateIndex(kNs, IndexDef{"search", {"description", "notes"}, IndexCompositeFastFT, IndexOpts{}, 0, condition});
		const int64_t count = condition[0] ? 1 : 2;
		checkStats(count, count);
	}

	TruncateNamespace(kNs);
	checkStats(0, 0);
	upsertItem(1, "episode", "reinserted token");
	checkStats(1, 1);
	ASSERT_EQ(Delete(Query(kNs)), 1);
	checkStats(0, 0);
	EXPECT_EQ(SelectByTitle("reinserted").Count(), 0);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "reinserted")).Count(), 0);
}

TEST_F(FtIndexingConditionApi, MemStatsCountsItemsWithTextValuesOnce) {
	IndexDef arrayIdx{"tags", {"tags"}, "text", "string", IndexOpts{}.Array(), 0, "type = 'series'"};
	AddIndex(kNs, arrayIdx);
	UpsertJSON(kNs, R"json({"id":1,"type":"series"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","title":null,"tags":[]})json");
	CheckFtMemStats("title", 2, 1);
	CheckFtMemStats("tags", 0, 0);
	UpsertJSON(kNs, R"json({"id":3,"type":"series","title":"","tags":["first","second"]})json");
	UpsertJSON(kNs, R"json({"id":4,"type":"series","title":"","tags":["first","second"]})json");
	UpsertJSON(kNs, R"json({"id":5,"type":"episode","title":"","tags":["first","second"]})json");
	CheckFtMemStats("title", 4, 1);
	CheckFtMemStats("tags", 2, 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "first")).Count(), 2);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 3)), 1);
	CheckFtMemStats("title", 3, 1);
	CheckFtMemStats("tags", 1, 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "first")).Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionOnNonFtIndex) {
	IndexDef hashIdx{"genre", {"genre"}, "hash", "string", IndexOpts{}, 0, "id > 0"};
	const auto err = reindexer->AddIndex(kNs, hashIdx);
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_EQ(
		err.whatStr(),
		"Cannot add index 'genre' in namespace 'ft_indexing_condition_ns': indexing_condition is supported only for fulltext indexes");
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithLimit) {
	IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "type = 'series' LIMIT 0"};
	const auto err = reindexer->AddIndex(kNs, ftIdx);
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_EQ(err.whatStr(), "indexing_condition must not contain LIMIT");
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithMerge) {
	IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "type = 'series' MERGE (SELECT * FROM other)"};
	const auto err = reindexer->AddIndex(kNs, ftIdx);
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_EQ(err.whatStr(), "indexing_condition must not contain MERGE");
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithJoin) {
	IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "type = 'series' AND INNER JOIN other ON _.id = other.id"};
	const auto err = reindexer->AddIndex(kNs, ftIdx);
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_EQ(err.whatStr(), "indexing_condition must not contain JOIN");
}

TEST_F(FtIndexingConditionApi, RejectsEmptyIndexingConditionInJson) {
	for (const auto value : {R"json("")json", R"json(" \t\r\n")json", R"json("\u000b")json", R"json("\f")json"}) {
		SCOPED_TRACE(value);
		const auto indexDef = IndexDef::FromJSON(
			std::string(
				R"json({"name":"notes","json_paths":["notes"],"index_type":"text","field_type":"string","indexing_condition":)json") +
			value + "}");
		ASSERT_FALSE(indexDef.has_value());
		EXPECT_EQ(indexDef.error().code(), errParams) << indexDef.error().what();
		EXPECT_EQ(indexDef.error().whatStr(), "indexing_condition must not be empty");
	}
}

TEST_F(FtIndexingConditionApi, RejectsNonStringIndexingConditionInJson) {
	for (const auto value : {"null", "[]", "{}", "true", "42"}) {
		SCOPED_TRACE(value);
		const auto indexDef = IndexDef::FromJSON(
			std::string(
				R"json({"name":"notes","json_paths":["notes"],"index_type":"text","field_type":"string","indexing_condition":)json") +
			value + "}");
		ASSERT_FALSE(indexDef.has_value());
		EXPECT_EQ(indexDef.error().code(), errParams) << indexDef.error().what();
		EXPECT_EQ(indexDef.error().whatStr(), "indexing_condition must be a string");
	}
}

TEST_F(FtIndexingConditionApi, JsonConditionCanBeSetAndRemovedByOmittingIt) {
	const auto conditionalDef = IndexDef::FromJSON(
		R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string","indexing_condition":"id = 1"})json");
	ASSERT_TRUE(conditionalDef.has_value());
	EXPECT_EQ(conditionalDef->IndexingCondition(), "id = 1");
	UpdateIndex(kNs, *conditionalDef);
	UpsertItem(1, "series", "json token");
	UpsertItem(2, "series", "json token");
	EXPECT_EQ(SelectByTitle("json").Count(), 1);

	const auto unconditionalDef =
		IndexDef::FromJSON(R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string"})json");
	ASSERT_TRUE(unconditionalDef.has_value());
	EXPECT_TRUE(unconditionalDef->IndexingCondition().empty());
	UpdateIndex(kNs, *unconditionalDef);
	EXPECT_EQ(SelectByTitle("json").Count(), 2);
}

TEST_F(FtIndexingConditionApi, AddIndexPreservesOmittedConditionButReplayChecksIt) {
	const IndexDef unconditional{"title", {"title"}, "text", "string", IndexOpts{}};
	const IndexDef conditional{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}});
	UpsertItem(1, "series", "preserved token");
	UpsertItem(2, "episode", "preserved token");
	const auto definitions = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(definitions.size(), 1);
	const auto stateBefore = GetReplicationState(kNs);

	AddIndex(kNs, unconditional);
	AddIndex(kNs, conditional);
	for (const auto& conflicting : {
			 IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'episode'"},
			 IndexDef{"title", {"description"}, "text", "string", IndexOpts{}},
			 IndexDef{"title", {"title"}, "text", "string", IndexOpts{}.Sparse()},
			 IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "type = 'series'"},
		 }) {
		SCOPED_TRACE(conflicting.Name());
		const auto err = reindexer->AddIndex(kNs, conflicting);
		EXPECT_EQ(err.code(), errConflict) << err.what();
		EXPECT_NE(err.whatStr().find("already exists with different settings"), std::string::npos) << err.what();
	}

	// Authoritative WAL records must still detect a condition mismatch.
	reindexer::WrSerializer json;
	unconditional.GetJSON(json);
	reindexer::PackedWALRecord record;
	record.Pack(reindexer::WALRecord{reindexer::WalIndexAdd, json.Slice()});
	reindexer::SnapshotChunk chunk;
	chunk.MarkWAL();
	chunk.records.emplace_back(reindexer::lsn_t{}, std::move(record));
	const auto err = reindexer->ApplySnapshotChunk(kNs, chunk);
	EXPECT_EQ(err.code(), errConflict) << err.what();
	EXPECT_NE(err.whatStr().find("already exists with different settings"), std::string::npos) << err.what();

	CheckIndexDefinitions(definitions[0].indexes);
	const auto stateAfter = GetReplicationState(kNs);
	EXPECT_EQ(stateAfter.lsn, stateBefore.lsn);
	EXPECT_EQ(stateAfter.checksum, stateBefore.checksum);
	EXPECT_EQ(stateAfter.dataCount, stateBefore.dataCount);
	EXPECT_EQ(Select(Query(kNs)).Count(), 2);
	const auto matching = SelectByTitle("preserved");
	ASSERT_EQ(matching.Count(), 1);
	EXPECT_EQ(matching.begin().GetItem()["id"].As<int>(), 1);

	UpdateIndex(kNs, unconditional);
	EXPECT_EQ(SelectByTitle("preserved").Count(), 2);
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithSubquery) {
	for (const auto condition :
		 {"id = (SELECT id FROM other WHERE id < 10)", "(SELECT max(id) FROM other) > 0", "(SELECT * FROM other LIMIT 0) IS NOT NULL"}) {
		SCOPED_TRACE(condition);
		IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, condition};
		const auto err = reindexer->AddIndex(kNs, ftIdx);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.whatStr(), "indexing_condition must not contain SUBQUERY");
	}
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithEqualPosition) {
	UpsertItem(1, "series", "position token");
	UpsertItem(2, "episode", "position token");
	const auto namespaces = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(namespaces.size(), 1);

	for (const auto condition : {"a = 1 AND b = 2 equal_position(a, b)", "equal_position(a, b) a = 1 AND b = 2",
								 "type = 'series' AND (a = 1 AND b = 2 equal_position(a, b))",
								 "type = 'series' OR (id = 1 AND (a = 1 AND b = 2 equal_position(a, b)))"}) {
		SCOPED_TRACE(condition);
		const IndexDef newIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, condition};
		const auto addErr = reindexer->AddIndex(kNs, newIdx);
		EXPECT_EQ(addErr.code(), errParams) << addErr.what();
		EXPECT_EQ(addErr.whatStr(), "indexing_condition must not contain EQUAL_POSITION");

		const IndexDef existingIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
		const auto updateErr = reindexer->UpdateIndex(kNs, existingIdx);
		EXPECT_EQ(updateErr.code(), errParams) << updateErr.what();
		EXPECT_EQ(updateErr.whatStr(), "indexing_condition must not contain EQUAL_POSITION");
		CheckIndexDefinitions(namespaces[0].indexes);
		const auto qr = SelectByTitle("position");
		ASSERT_EQ(qr.Count(), 1);
		EXPECT_EQ(qr.begin().GetItem()["id"].As<int>(), 1);
	}

	UpsertItem(2, "series", "position token");
	EXPECT_EQ(SelectByTitle("position").Count(), 2);
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionReferencingFloatVectorIndex) {
	AddIndex(kNs, IndexDef{"embedding",
						   {"embedding"},
						   "vec_bf",
						   "float_vector",
						   IndexOpts{}.SetFloatVector(IndexVectorBruteforce, reindexer::FloatVectorIndexOpts{}.SetDimension(4))});

	IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "embedding IS NOT NULL"};
	const auto addErr = reindexer->AddIndex(kNs, ftIdx);
	EXPECT_EQ(addErr.code(), errParams) << addErr.what();
	EXPECT_NE(addErr.whatStr().find("must not reference float vector index 'embedding'"), std::string::npos);

	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "id > 0"});
	const auto updateErr = reindexer->UpdateIndex(kNs, ftIdx);
	EXPECT_EQ(updateErr.code(), errParams) << updateErr.what();
	EXPECT_NE(updateErr.whatStr().find("must not reference float vector index 'embedding'"), std::string::npos);
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithFunctions) {
	const struct [[nodiscard]] TestCase {
		std::string_view condition;
		std::string_view error;
	} cases[] = {
		{.condition = "id = now()", .error = "Invalid indexing_condition: Function 'now' is not supported"},
		{.condition = "flat_array_len(arr) > 0", .error = "Invalid indexing_condition: Function 'flat_array_len' is not supported"},
		{.condition = "type = 'series' AND (id = now())", .error = "Invalid indexing_condition: Function 'now' is not supported"},
		{.condition = "type = 'series' AND (flat_array_len(arr) > 0)",
		 .error = "Invalid indexing_condition: Function 'flat_array_len' is not supported"},
		{.condition = "id = flat_array_len(arr)", .error = "Invalid indexing_condition: arithmetic expressions are not supported"},
		{.condition = "now() = id", .error = "Invalid indexing_condition: arithmetic expressions are not supported"},
	};
	UpsertItem(1, "series", "function token");
	UpsertItem(2, "episode", "function token");
	ASSERT_EQ(SelectByTitle("function").Count(), 1);

	for (const auto& test : cases) {
		SCOPED_TRACE(test.condition);
		IndexDef newIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, std::string(test.condition)};
		const auto addErr = reindexer->AddIndex(kNs, newIdx);
		EXPECT_EQ(addErr.code(), errParams) << addErr.what();
		EXPECT_EQ(addErr.whatStr(), test.error);

		IndexDef existingIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, std::string(test.condition)};
		const auto updateErr = reindexer->UpdateIndex(kNs, existingIdx);
		EXPECT_EQ(updateErr.code(), errParams) << updateErr.what();
		EXPECT_EQ(updateErr.whatStr(), test.error);
		EXPECT_EQ(SelectByTitle("function").Count(), 1);
	}

	UpsertItem(2, "series", "function token");
	EXPECT_EQ(SelectByTitle("function").Count(), 2);
	UpsertItem(1, "episode", "function token");
	EXPECT_EQ(SelectByTitle("function").Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsInvalidIndexingConditionPredicatesBeforeWritingItems) {
	const struct [[nodiscard]] TestCase {
		std::string_view condition;
		std::string_view error;
	} cases[] = {
		{.condition = "id = 'abc'", .error = "Can't convert 'abc' to number"},
		{.condition = "id IS NULL", .error = "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes"},
		{.condition = "id IS NOT NULL", .error = "The 'NOT NULL' condition is supported only by 'sparse' or 'array' indexes"},
		{.condition = "ST_DWithin(point, ST_GeomFromText('point(0 0)'), 100)",
		 .error = "indexing_condition must not contain ST_DWithin conditions"},
		{.condition = "KNN(embedding, [1.0, 2.1], k=10, ef=10)", .error = "indexing_condition must not contain KNN conditions"},
	};
	for (const auto& test : cases) {
		SCOPED_TRACE(test.condition);
		IndexDef newIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, std::string(test.condition)};
		const auto addErr = reindexer->AddIndex(kNs, newIdx);
		EXPECT_EQ(addErr.code(), errParams) << addErr.what();
		EXPECT_EQ(addErr.whatStr(), test.error);
		IndexDef updated{"title", {"title"}, "text", "string", IndexOpts{}, 0, std::string(test.condition)};
		const auto updateErr = reindexer->UpdateIndex(kNs, updated);
		EXPECT_EQ(updateErr.code(), errParams) << updateErr.what();
		EXPECT_EQ(updateErr.whatStr(), test.error);
	}
	UpsertItem(1, "series", "valid token");
	UpsertItem(2, "episode", "valid token");
	EXPECT_EQ(SelectByTitle("valid").Count(), 1);
}

TEST_F(FtIndexingConditionApi, NullConditionsDistinguishMissingNullAndNonNullValues) {
	AddIndex(kNs, IndexDef{"nullable", {"nullable"}, "hash", "int", IndexOpts{}.Sparse()});
	UpsertJSON(kNs, R"json({"id":1,"title":"null token"})json");
	UpsertJSON(kNs, R"json({"id":2,"nullable":null,"unindexed":null,"title":"null token"})json");
	UpsertJSON(kNs, R"json({"id":3,"nullable":0,"unindexed":0,"title":"null token"})json");
	UpsertJSON(kNs, R"json({"id":4,"nullable":1,"unindexed":1,"title":"null token"})json");
	for (const auto field : {"nullable", "unindexed"}) {
		for (const auto op : {" IS NULL", " IS NOT NULL"}) {
			const std::string condition = std::string(field) + op;
			SCOPED_TRACE(condition);
			IndexDef updated{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
			UpdateIndex(kNs, updated);
			const auto result = SelectByTitle("null");
			ASSERT_EQ(result.Count(), 2);
			std::set<int> ids;
			for (auto it : result) {
				ids.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(ids, (std::string_view(op) == " IS NULL" ? std::set<int>{1, 2} : std::set<int>{3, 4}));
		}
	}
}

TEST_F(FtIndexingConditionApi, NotNullConditionIncludesObjects) {
	IndexDef updated{"title", {"title"}, "text", "string", IndexOpts{}, 0, "metadata IS NOT NULL"};
	UpdateIndex(kNs, updated);
	UpsertJSON(kNs, R"json({"id":1,"title":"object token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":null,"title":"object token"})json");
	UpsertJSON(kNs, R"json({"id":3,"metadata":{},"title":"object token"})json");
	const auto result = SelectByTitle("object");
	ASSERT_EQ(result.Count(), 1);
	EXPECT_EQ(result.begin().GetItem()["id"].As<int>(), 3);
}

TEST_F(FtIndexingConditionApi, RejectsIndexingConditionWithArithmetic) {
	for (const auto condition : {"score + 1 > 0", "score > id + 1", "type = 'series' AND (score + 1 > id * 2)"}) {
		SCOPED_TRACE(condition);
		IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, condition};
		const auto err = reindexer->AddIndex(kNs, ftIdx);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_TRUE(err.whatStr().starts_with("Invalid indexing_condition:")) << err.what();
	}
}

TEST_F(FtIndexingConditionApi, ArithmeticUpdateReappliesIndexingCondition) {
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "score >= 1"};
	UpdateIndex(kNs, ftIdx);
	UpsertItem(1, "series", "arithmetic token", 0);
	EXPECT_EQ(SelectByTitle("arithmetic").Count(), 0);

	ASSERT_EQ(Update(Query::FromSQL("UPDATE ft_indexing_condition_ns SET score = score + 1 WHERE id = 1")), 1);
	EXPECT_EQ(SelectByTitle("arithmetic").Count(), 1);

	ASSERT_EQ(Update(Query::FromSQL("UPDATE ft_indexing_condition_ns SET score = score - 1 WHERE id = 1")), 1);
	EXPECT_EQ(SelectByTitle("arithmetic").Count(), 0);
}

TEST_F(FtIndexingConditionApi, IndexingConditionIgnoresStrictModeAndResolvesFutureFields) {
	for (const auto mode : {"names", "indexes", "none"}) {
		SCOPED_TRACE(mode);
		UpsertJSON(reindexer::kConfigNamespace,
				   std::string(R"json({"type":"namespaces","namespaces":[{"namespace":"ft_indexing_condition_ns","strict_mode":")json") +
					   mode + R"json("}]})json");

		const std::string condition = std::string("metadata.") + mode + " = 'yes'";
		IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
		UpdateIndex(kNs, ftIdx);

		const auto upsert = [&](int id, std::string_view value) {
			UpsertJSON(kNs, std::string(R"json({"id":)json") + std::to_string(id) + R"json(,"title":"future token","metadata":{")json" +
								mode + R"json(":")json" + std::string(value) + R"json("}})json");
		};
		UpsertItem(1, "series", "future token");
		EXPECT_EQ(SelectByTitle("future").Count(), 0);
		upsert(1, "yes");
		upsert(2, "no");
		EXPECT_EQ(SelectByTitle("future").Count(), 1);

		ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set(std::string("metadata.") + mode, {"yes"})), 1);
		EXPECT_EQ(SelectByTitle("future").Count(), 2);
		EXPECT_EQ(Delete(Query(kNs)), 2);
		EXPECT_EQ(SelectByTitle("future").Count(), 0);
		upsert(1, "no");
		EXPECT_EQ(SelectByTitle("future").Count(), 0);
		upsert(1, "yes");
		EXPECT_EQ(SelectByTitle("future").Count(), 1);
		EXPECT_EQ(Delete(Query(kNs)), 1);
	}
}

TEST_F(FtIndexingConditionApi, ReappliesConditionForNestedPathsAndIndexAliases) {
	for (const auto field : {"metadata.kind", "kind_index"}) {
		SCOPED_TRACE(field);
		if (std::string_view(field) == "kind_index") {
			UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}});
			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}}));
		}
		IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, std::string(field) + " = 'series'"};
		UpdateIndex(kNs, ftIdx);
		UpsertJSON(kNs, R"json({"id":1,"title":"nested token","metadata":{"kind":"episode"}})json");
		EXPECT_EQ(SelectByTitle("nested").Count(), 0);

		ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set(field, {"series"})), 1);
		EXPECT_EQ(SelectByTitle("nested").Count(), 1);
		ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).SetObject("metadata", R"json({"kind":"episode"})json")), 1);
		EXPECT_EQ(SelectByTitle("nested").Count(), 0);
		ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("metadata.kind", {"series"})), 1);
		EXPECT_EQ(SelectByTitle("nested").Count(), 1);

		Restart(kNs);
		ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).SetObject("metadata", R"json({"kind":"episode"})json")), 1);
		EXPECT_EQ(SelectByTitle("nested").Count(), 0);
		ASSERT_EQ(Delete(Query(kNs)), 1);
		EXPECT_EQ(SelectByTitle("nested").Count(), 0);
	}
}

TEST_F(FtIndexingConditionApi, ReappliesConditionForArrayElementPaths) {
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "entries.kind = 'series'"};
	UpdateIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"title":"array token","entries":[{"kind":"episode"},{"kind":"episode"}]})json");
	EXPECT_EQ(SelectByTitle("array").Count(), 0);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("entries[1].kind", {"series"})), 1);
	EXPECT_EQ(SelectByTitle("array").Count(), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).SetObject("entries[1]", R"json({"kind":"episode"})json")), 1);
	EXPECT_EQ(SelectByTitle("array").Count(), 0);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("entries[*].kind", {"series"})), 1);
	EXPECT_EQ(SelectByTitle("array").Count(), 1);
	ASSERT_EQ(Delete(Query(kNs)), 1);
	EXPECT_EQ(SelectByTitle("array").Count(), 0);
}

TEST_F(FtIndexingConditionApi, ReappliesConditionForAllIndexJsonPaths) {
	AddIndex(kNs, IndexDef{"kind_index", {"primary.kind", "secondary.kind"}, "hash", "string", IndexOpts{}.Array()});
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "kind_index = 'series'"};
	UpdateIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"title":"multipath token","primary":{"kind":"episode"},"secondary":{"kind":"episode"}})json");
	EXPECT_EQ(SelectByTitle("multipath").Count(), 0);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("secondary.kind", {"series"})), 1);
	EXPECT_EQ(SelectByTitle("multipath").Count(), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).SetObject("secondary", R"json({"kind":"episode"})json")), 1);
	EXPECT_EQ(SelectByTitle("multipath").Count(), 0);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("primary.kind", {"series"})), 1);
	EXPECT_EQ(SelectByTitle("multipath").Count(), 1);
	ASSERT_EQ(Delete(Query(kNs)), 1);
	EXPECT_EQ(SelectByTitle("multipath").Count(), 0);
}

TEST_F(FtIndexingConditionApi, KeepsScalarAndCompositeConditionsAfterTruncate) {
	IndexDef compositeIdx{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, compositeIdx);

	for (int i = 0; i < 2; ++i) {
		SCOPED_TRACE(i);
		UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"truncate token","description":"composite token"})json");
		UpsertJSON(kNs, R"json({"id":2,"type":"episode","title":"truncate token","description":"composite token"})json");
		EXPECT_EQ(SelectByTitle("truncate").Count(), 1);
		EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);
		TruncateNamespace(kNs);
		EXPECT_EQ(Select(Query(kNs)).Count(), 0);
		EXPECT_EQ(SelectByTitle("truncate").Count(), 0);
		EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 0);
	}

	Restart(kNs);
	UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"truncate token","description":"composite token"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"episode","title":"truncate token","description":"composite token"})json");
	EXPECT_EQ(SelectByTitle("truncate").Count(), 1);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);
}

TEST_F(FtIndexingConditionApi, SparseConditionTracksUpdatesAndRestart) {
	IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, "type = 'series'"};
	AddIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"type":"episode","notes":"original token"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","notes":"original token"})json");
	UpsertJSON(kNs, R"json({"id":3,"type":"series"})json");
	UpsertJSON(kNs, R"json({"id":4,"type":"series","notes":null})json");
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "token")).Count(), 1);
	CheckFtMemStats("notes", 1, 1);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("notes", {"changed token"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "changed")).Count(), 0);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"series"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "changed")).Count(), 1);
	UpsertJSON(kNs, R"json({"id":2,"type":"episode","notes":"original token"})json");
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "original")).Count(), 0);
	Restart(kNs);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "token")).Count(), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"episode"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "token")).Count(), 0);
	UpsertJSON(kNs, R"json({"id":2,"type":"series","notes":"original token"})json");
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "original")).Count(), 1);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "token")).Count(), 0);
	CheckFtMemStats("notes", 0, 0);
}

TEST_F(FtIndexingConditionApi, InvalidatesCachedTotalAfterSparseIndexRebuild) {
	UpsertJSON(kNs, R"json({"id":1,"type":"series","notes":"token","body":"other"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","notes":"token","body":"other"})json");
	UpsertJSON(kNs, R"json({"id":3,"type":"episode","notes":"token","body":"token"})json");

	const auto check = [this](const std::set<int>& expected) {
		const Query query = Query(kNs).Where("notes", CondEq, "token");
		const auto total = Select(Query{query}.ReqTotal()).TotalCount();
		EXPECT_EQ(total, expected.size());
		// Populate the count cache and perform another query that can reuse its stored total.
		for (uint32_t i = 0; i <= reindexer::kDefaultHitCountToCache; ++i) {
			SCOPED_TRACE(i);
			auto qr = Select(Query{query}.CachedTotal());
			EXPECT_EQ(qr.TotalCount(), total);
			EXPECT_EQ(qr.Count(), expected.size());
			std::set<int> ids;
			for (auto it : qr) {
				ids.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(ids, expected);
		}
	};
	for (const bool conditional : {false, true}) {
		SCOPED_TRACE(conditional ? "indexing_condition" : "json_paths");
		ASSERT_NO_FATAL_FAILURE(
			AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, conditional ? "type = 'series'" : ""}));
		ASSERT_NO_FATAL_FAILURE(check(conditional ? std::set<int>{1, 2} : std::set<int>{1, 2, 3}));

		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"notes",
														  {conditional ? "notes" : "body"},
														  "text",
														  "string",
														  IndexOpts{}.Sparse(),
														  0,
														  conditional ? "type = 'episode'" : ""}));
		ASSERT_NO_FATAL_FAILURE(check({3}));
		ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "notes"));
	}
}

TEST_F(FtIndexingConditionApi, SparseConditionSurvivesIndexLayoutChanges) {
	AddIndex(kNs, IndexDef{"enabled", {"enabled"}, "hash", "int", IndexOpts{}});
	IndexDef ftIdx{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, "enabled = 1"};
	AddIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"enabled":0,"notes":"shared token"})json");
	UpsertJSON(kNs, R"json({"id":2,"enabled":1,"notes":"shared token"})json");

	// Shift both the referenced payload field and the sparse index position.
	DropIndex(kNs, "score");
	AddIndex(kNs, IndexDef{"other", {"other"}, "hash", "string", IndexOpts{}.Sparse()});
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("enabled", {1})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 2);
	DropIndex(kNs, "other");
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("enabled", {0})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 1);

	for (const auto& opts : {IndexOpts{}, IndexOpts{}.Sparse(), IndexOpts{}.Sparse().Dense()}) {
		ftIdx.SetOpts(opts);
		UpdateIndex(kNs, ftIdx);
		EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 1);
		Restart(kNs);
		EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 1);
	}
	UpdateIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", ftIdx.Opts()});
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 2);
	UpdateIndex(kNs, ftIdx);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 1);
	ASSERT_EQ(Delete(Query(kNs)), 2);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "shared")).Count(), 0);
}

TEST_F(FtIndexingConditionApi, ExcludedArrayKeepsValuesAfterPartialUpdate) {
	IndexDef ftIdx{"tags", {"tags"}, "text", "string", IndexOpts{}.Array(), 0, "type = 'series'"};
	AddIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"type":"episode","tags":["first","second","third"]})json");
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("tags[*]", {"repeated"})), 1);
	{
		auto qr = Select(Query(kNs).Where("id", CondEq, 1));
		ASSERT_EQ(qr.Count(), 1);
		const auto item = qr.begin().GetItem();
		const reindexer::VariantArray tags = item["tags"];
		ASSERT_EQ(tags.size(), 3);
		// Upsert creates a separate owner for each occurrence of the non-owning string literal.
		// Check the payload references before StringsHolder can mask a dangling reference after a partial update.
		std::set<const reindexer::key_string_impl*> owners;
		for (const auto& tag : tags) {
			ASSERT_TRUE(owners.insert(static_cast<reindexer::p_string>(tag).getBaseKeyString()).second);
		}
	}
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("tags[2]", {"untouched"})), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("tags[0]", {"changed"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "changed")).Count(), 0);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"series"})), 1);
	const auto check = [this] {
		for (const auto word : {"changed", "repeated", "untouched"}) {
			SCOPED_TRACE(word);
			EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, word)).Count(), 1);
		}
	};
	check();
	Restart(kNs);
	check();
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "changed")).Count(), 0);
}

TEST_F(FtIndexingConditionApi, ArrayConditionUpdatesWithCompositePayloadReferences) {
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "-", "string", IndexOpts{}}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"description", {"description"}, "-", "string", IndexOpts{}}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"tags", {"tags"}, "text", "string", IndexOpts{}.Array(), 0, "type = 'series'"}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}}));
	ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, R"json({"id":1,"type":"series","tags":["alpha","beta"],
		"title":"titleword","description":"bodyword"})json"));
	ASSERT_EQ(Select(Query(kNs).Where("search", CondEq, "titleword")).Count(), 1);

	for (const bool updateArray : {false, true}) {
		SCOPED_TRACE(updateArray);
		for (const auto type : {"episode", "series"}) {
			SCOPED_TRACE(type);
			auto query = Query(kNs).Where("id", CondEq, 1).Set("type", {type});
			if (updateArray) {
				query.Set("tags", {"alpha", "beta"});
			}
			ASSERT_EQ(Update(query), 1);
			const auto items = Select(Query(kNs).Where("id", CondEq, 1));
			ASSERT_EQ(items.Count(), 1);
			const auto item = items.begin().GetItem();
			const reindexer::VariantArray tags = item["tags"];
			ASSERT_EQ(tags.size(), 2);
			EXPECT_EQ(tags[0].As<std::string>(), "alpha");
			EXPECT_EQ(tags[1].As<std::string>(), "beta");
			for (const auto word : {"alpha", "beta"}) {
				EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, word)).Count(), static_cast<std::string_view>(type) == "series" ? 1 : 0);
			}
			for (const auto word : {"titleword", "bodyword"}) {
				EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, word)).Count(), 1);
			}
		}
	}
	ASSERT_EQ(Delete(Query(kNs)), 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "alpha")).Count(), 0);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "titleword")).Count(), 0);
}

TEST(FtIndexingConditionStorage, PartialDeleteKeepsOtherDuplicateAlive) {
	for (const size_t removed : {0u, 1u}) {
		SCOPED_TRACE(removed);
		const IndexDef ftIdx{"tags", {"tags"}, "text", "string", IndexOpts{}.Array()};
		auto index =
			reindexer::Index::New(ftIdx, reindexer::PayloadType{}, reindexer::FieldsSet{}, reindexer::NamespaceCacheConfigData{}, 0);
		const auto rowId = reindexer::IdType::FromNumber(0);
		bool clearCache = false;
		reindexer::StringsHolder strHolder;
		reindexer::VariantArray indexed;
		index->Upsert(indexed, {reindexer::Variant{"same"}, reindexer::Variant{"same"}}, rowId, clearCache);
		ASSERT_EQ(indexed.size(), 2);
		ASSERT_NE(static_cast<reindexer::p_string>(indexed[0]).getBaseKeyString(),
				  static_cast<reindexer::p_string>(indexed[1]).getBaseKeyString());

		reindexer::VariantArray excluded;
		ASSERT_TRUE(index->Reconcile(excluded, indexed, rowId, reindexer::MatchesCondition_False, strHolder, clearCache));
		ASSERT_EQ(excluded.size(), 2);
		// Like payload slots, these references must not retain ownership of the strings.
		const reindexer::Variant remaining{static_cast<reindexer::p_string>(excluded[1 - removed]), reindexer::Variant::noHold};
		const reindexer::Variant deleted{static_cast<reindexer::p_string>(excluded[removed]), reindexer::Variant::noHold};
		indexed.clear();
		excluded.clear();
		strHolder.Clear();

		index->Delete(deleted, rowId, reindexer::MustExist_True, strHolder, clearCache);
		strHolder.Clear();
		// ASAN detects a dangling reference here if the owner of the other duplicate was removed.
		EXPECT_EQ(remaining.As<std::string>(), "same");
		index->Delete(remaining, rowId, reindexer::MustExist_True, strHolder, clearCache);
		strHolder.Clear();
	}
}

TEST_F(FtIndexingConditionApi, FailedConditionEvaluationLeavesUpsertStateUnchanged) {
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "x LIKE 'a%'"});
	UpsertJSON(kNs, R"json({"id":1,"x":"abc","title":"token"})json");
	ASSERT_EQ(SelectByTitle("token").Count(), 1);

	const auto getDataSize = [this] {
		const auto stats = Select(Query(reindexer::kMemStatsNamespace).Where("name", CondEq, kNs));
		EXPECT_EQ(stats.Count(), 1);
		if (stats.Count() != 1) {
			return uint64_t{0};
		}
		const std::string json(stats.begin().GetItem().GetJSON());
		gason::JsonParser parser;
		return parser.Parse(std::string_view(json))["total"]["data_size"].As<uint64_t>();
	};
	const auto original = Select(Query(kNs).Where("id", CondEq, 1));
	ASSERT_EQ(original.Count(), 1);
	const auto originalLsn = original.begin().GetItem().GetLSN();
	const auto stateBefore = GetReplicationState(kNs);
	const auto dataSizeBefore = getDataSize();
	for (const int id : {1, 2}) {
		SCOPED_TRACE(id);
		auto item = NewItem(kNs);
		const std::string json = std::string(R"json({"id":)json") + std::to_string(id) + R"json(,"x":123,"title":"token"})json";
		ASSERT_TRUE(item.FromJSON(json).ok());
		const auto err = reindexer->Upsert(kNs, item);
		ASSERT_EQ(err.code(), errLogic) << err.what();
		EXPECT_NE(err.whatStr().find("Condition LIKE must be applied to data of string type"), std::string::npos) << err.what();

		const auto stateAfter = GetReplicationState(kNs);
		EXPECT_EQ(stateAfter.lsn, stateBefore.lsn);
		EXPECT_EQ(stateAfter.checksum, stateBefore.checksum);
		EXPECT_EQ(stateAfter.dataCount, stateBefore.dataCount);
		EXPECT_EQ(getDataSize(), dataSizeBefore);
		const auto items = Select(Query(kNs));
		ASSERT_EQ(items.Count(), 1);
		auto stored = items.begin().GetItem();
		EXPECT_EQ(stored["id"].As<int>(), 1);
		EXPECT_EQ(stored["x"].As<std::string>(), "abc");
		EXPECT_EQ(stored["title"].As<std::string>(), "token");
		EXPECT_EQ(stored.GetLSN(), originalLsn);
		EXPECT_EQ(SelectByTitle("token").Count(), 1);
	}

	UpsertJSON(kNs, R"json({"id":1,"x":"abc","title":"changed"})json");
	UpsertJSON(kNs, R"json({"id":2,"x":"abc","title":"token"})json");
	EXPECT_EQ(SelectByTitle("changed").Count(), 1);
	EXPECT_EQ(SelectByTitle("token").Count(), 1);
	Restart(kNs);
	EXPECT_EQ(SelectByTitle("changed").Count(), 1);
	EXPECT_EQ(SelectByTitle("token").Count(), 1);
	ASSERT_EQ(Delete(Query(kNs)), 2);
	EXPECT_EQ(SelectByTitle("changed").Count(), 0);
	EXPECT_EQ(SelectByTitle("token").Count(), 0);
}

TEST_F(FtIndexingConditionApi, FailedUpdateRestoresConditionalIndexes) {
	IndexDef sparseIdx{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, "type = 'series'"};
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, sparseIdx));
	IndexDef compositeIdx{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, compositeIdx));
	for (const auto type : {"episode", "series"}) {
		SCOPED_TRACE(type);
		const std::string json = std::string(R"json({"id":1,"type":")json") + type +
								 R"json(","title":"original token","notes":"original token","description":"compositeoriginal token"})json";
		UpsertJSON(kNs, json);
		QueryResults result;
		const auto err = reindexer->Update(Query(kNs)
											   .Where("id", CondEq, 1)
											   .Set("title", {"changed token"})
											   .Set("notes", {"changed token"})
											   .Set("description", {"compositechanged token"})
											   .Set("type", {"series"})
											   .Set("id", reindexer::Variant{}),
										   result);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.whatStr(), "Index fields that are neither arrays nor sparse cannot be set to null");
		for (const auto index : {"title", "notes", "search"}) {
			SCOPED_TRACE(index);
			EXPECT_EQ(Select(Query(kNs).Where(index, CondEq, "changed")).Count(), 0);
			EXPECT_EQ(Select(Query(kNs).Where(index, CondEq, "original")).Count(), std::string_view(type) == "series" ? 1 : 0);
		}
		EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "compositechanged")).Count(), 0);
		EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "compositeoriginal")).Count(), std::string_view(type) == "series" ? 1 : 0);
		const auto items = Select(Query(kNs).Where("id", CondEq, 1));
		ASSERT_EQ(items.Count(), 1);
		const auto item = items.begin().GetItem();
		EXPECT_EQ(item["title"].As<std::string>(), "original token");
		EXPECT_EQ(item["notes"].As<std::string>(), "original token");
		EXPECT_EQ(item["description"].As<std::string>(), "compositeoriginal token");
		EXPECT_EQ(item["type"].As<std::string>(), type);
	}
	ASSERT_EQ(Delete(Query(kNs)), 1);
	for (const auto index : {"title", "notes", "search"}) {
		SCOPED_TRACE(index);
		EXPECT_EQ(Select(Query(kNs).Where(index, CondEq, "original")).Count(), 0);
	}
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "compositeoriginal")).Count(), 0);
}

TEST_F(FtIndexingConditionApi, KeepsConditionAndPayloadAfterIndexOptionsUpdate) {
	for (const auto& opts : {IndexOpts{}.Dense(), IndexOpts{}.NoIndexColumn(), IndexOpts{}}) {
		UpsertItem(1, "episode", "shared token");
		UpsertItem(2, "series", "shared token");
		UpsertItem(3, "series", "shared token");
		ASSERT_EQ(SelectByTitle("shared").Count(), 2);

		IndexDef ftIdx{"title", {"title"}, "text", "string", opts, 0, "type = 'series'"};
		UpdateIndex(kNs, ftIdx);

		const auto check = [this] {
			auto ftQr = SelectByTitle("shared");
			ASSERT_EQ(ftQr.Count(), 2);
			for (auto it : ftQr) {
				EXPECT_NE(it.GetItem()["id"].As<int>(), 1);
			}
			auto allQr = Select(Query(kNs));
			ASSERT_EQ(allQr.Count(), 3);
			for (auto it : allQr) {
				EXPECT_EQ(it.GetItem()["title"].As<std::string>(), "shared token");
			}
		};
		check();
		Restart(kNs);
		check();
		ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"series"})), 1);
		EXPECT_EQ(SelectByTitle("shared").Count(), 3);
		ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
		EXPECT_EQ(SelectByTitle("shared").Count(), 2);
		ASSERT_EQ(Delete(Query(kNs)), 2);
		EXPECT_EQ(SelectByTitle("shared").Count(), 0);
	}
}

TEST_F(FtIndexingConditionApi, KeepsCompositeConditionAfterIndexOptionsUpdate) {
	IndexDef ftIdx{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"type":"episode","title":"shared token","description":"composite token"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","title":"shared token","description":"composite token"})json");
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);

	IndexDef updated{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}.Dense(), 0, ftIdx.IndexingCondition()};
	UpdateIndex(kNs, updated);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);
	Restart(kNs);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"series"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 2);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsAddingForbiddenIndexReferencedByExistingCondition) {
	for (const auto& ftIdx :
		 {IndexDef{"conditional", {"body"}, "text", "string", IndexOpts{}, 0, "non_idx_field = 'string'"},
		  IndexDef{"conditional", {"body"}, "text", "string", IndexOpts{}.Sparse(), 0, "non_idx_field = 'string'"},
		  IndexDef{"conditional", {"title", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, "non_idx_field = 'string'"}}) {
		SCOPED_TRACE(ftIdx.Opts().IsSparse() ? "sparse" : "regular");
		SCOPED_TRACE(ftIdx.IndexTypeStr());
		AddIndex(kNs, ftIdx);
		const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
		ASSERT_EQ(before.size(), 1);

		const struct [[nodiscard]] TestCase {
			IndexDef index;
			std::string_view error;
		} cases[] = {
			{.index = IndexDef{"non_idx_field",
							   {"other"},
							   "vec_bf",
							   "float_vector",
							   IndexOpts{}.SetFloatVector(IndexVectorBruteforce, reindexer::FloatVectorIndexOpts{}.SetDimension(4))},
			 .error = "must not reference float vector index 'non_idx_field'"},
			{.index = IndexDef{"vector_alias",
							   {"non_idx_field"},
							   "vec_bf",
							   "float_vector",
							   IndexOpts{}.SetFloatVector(IndexVectorBruteforce, reindexer::FloatVectorIndexOpts{}.SetDimension(4))},
			 .error = "must not reference float vector index 'vector_alias'"},
			{.index = IndexDef{"non_idx_field", {"other"}, "rtree", "point", IndexOpts{}},
			 .error = "must not reference geospatial index 'non_idx_field'"},
			{.index = IndexDef{"non_idx_field", {"other"}, "text", "string", IndexOpts{}},
			 .error = "must not reference fulltext index 'non_idx_field'"},
		};
		for (int itemCount : {0, 2}) {
			SCOPED_TRACE(itemCount);
			if (itemCount) {
				UpsertJSON(kNs, R"json({"id":1,"non_idx_field":"string","title":"schema token","body":"schema token"})json");
				UpsertJSON(kNs, R"json({"id":2,"non_idx_field":"other","title":"schema token","body":"schema token"})json");
			}
			for (const auto& test : cases) {
				SCOPED_TRACE(test.index.IndexTypeStr());
				const auto err = reindexer->AddIndex(kNs, test.index);
				EXPECT_EQ(err.code(), errParams) << err.what();
				EXPECT_NE(err.whatStr().find(test.error), std::string::npos) << err.what();
				EXPECT_NE(err.whatStr().find("indexing_condition of fulltext index 'conditional'"), std::string::npos) << err.what();
				CheckIndexDefinitions(before[0].indexes);
				EXPECT_EQ(Select(Query(kNs).Where("conditional", CondEq, "schema")).Count(), itemCount / 2);
			}
		}
		Restart(kNs);
		CheckIndexDefinitions(before[0].indexes);
		EXPECT_EQ(Select(Query(kNs).Where("conditional", CondEq, "schema")).Count(), 1);
		EXPECT_EQ(Delete(Query(kNs)), 2);
		DropIndex(kNs, "conditional");
	}
}

TEST_F(FtIndexingConditionApi, RejectsAddingIndexChangingConditionFieldBinding) {
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "metadata.kind = 'series'"};
	UpdateIndex(kNs, ftIdx);
	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, ftIdx.IndexingCondition()});
	AddIndex(kNs, IndexDef{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, ftIdx.IndexingCondition()});
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"title":"binding token"})json");
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);

	for (const auto& idx : {IndexDef{"metadata.kind", {"other"}, "hash", "string", IndexOpts{}},
							IndexDef{"alias", {"metadata.kind"}, "hash", "string", IndexOpts{}.Sparse()},
							IndexDef{"alias", {"metadata"}, "hash", "string", IndexOpts{}}}) {
		const auto err = reindexer->AddIndex(kNs, idx);
		EXPECT_EQ(err.code(), errLogic) << err.what();
		EXPECT_NE(err.whatStr().find("the change may alter indexing_condition results"), std::string::npos) << err.what();
		CheckIndexDefinitions(before[0].indexes);
		EXPECT_EQ(SelectByTitle("binding").Count(), 1);
	}

	// JSON paths remain case-sensitive; an unrelated field must not block DDL.
	AddIndex(kNs, IndexDef{"unrelated", {"metadata.Kind"}, "hash", "string", IndexOpts{}.Sparse()});
	EXPECT_EQ(SelectByTitle("binding").Count(), 1);
	Restart(kNs);
	EXPECT_EQ(SelectByTitle("binding").Count(), 1);

	// No indexed documents remain to preserve. All FT kinds must use the new binding for subsequent inserts.
	EXPECT_EQ(Delete(Query(kNs)), 1);
	AddIndex(kNs, IndexDef{"metadata.kind", {"other"}, "hash", "string", IndexOpts{}.Sparse()});
	for (const auto& opts : {IndexOpts{}.Sparse(), IndexOpts{}}) {
		SCOPED_TRACE(opts.IsSparse() ? "sparse" : "regular");
		UpdateIndex(kNs, IndexDef{"metadata.kind", {"other"}, "hash", "string", opts});
		UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"other":"episode",
			"title":"binding token","notes":"binding token","description":"binding token"})json");
		UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode"},"other":"series",
			"title":"binding token","notes":"binding token","description":"binding token"})json");
		for (const auto index : {"title", "notes", "search"}) {
			SCOPED_TRACE(index);
			const auto items = Select(Query(kNs).Where(index, CondEq, "binding"));
			ASSERT_EQ(items.Count(), 1);
			EXPECT_EQ(items.begin().GetItem()["id"].As<int>(), 2);
		}
		EXPECT_EQ(Delete(Query(kNs)), 2);
	}
}

TEST_F(FtIndexingConditionApi, RejectsAddingIndexShadowingIndexedJsonPathInCondition) {
	AddIndex(kNs, IndexDef{"kind", {"metadata.kind"}, "hash", "string", IndexOpts{}});
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "metadata.kind = 'series'"};
	UpdateIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"title":"shadow token"})json");
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);

	const auto err = reindexer->AddIndex(kNs, IndexDef{"metadata.kind", {"other"}, "hash", "string", IndexOpts{}});
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_NE(err.whatStr().find("the change may alter indexing_condition results"), std::string::npos) << err.what();
	CheckIndexDefinitions(before[0].indexes);
	EXPECT_EQ(SelectByTitle("shadow").Count(), 1);
}

TEST_F(FtIndexingConditionApi, AllowsIndependentCompositeIndexChanges) {
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "score = 1"}));
	UpsertItem(1, "series", "composite token", 1);
	UpsertItem(2, "series", "composite token", 2);
	UpsertItem(3, "series", "composite token", 1);

	const auto checkIds = [this](const std::set<int>& expected) {
		const auto qr = SelectByTitle("composite");
		std::set<int> actual;
		for (auto it : qr) {
			actual.insert(it.GetItem()["id"].As<int>());
		}
		EXPECT_EQ(actual, expected);
	};
	checkIds({1, 3});

	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"score_comp", {"score"}, IndexCompositeHash, IndexOpts{}}));
	checkIds({1, 3});
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"score_comp", {"score", "id"}, IndexCompositeHash, IndexOpts{}}));
	checkIds({1, 3});
	ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "score_comp"));
	checkIds({1, 3});
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"score_comp", {"score", "id"}, IndexCompositeHash, IndexOpts{}}));

	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "score_comp = {1, 3}"}));
	checkIds({3});
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);

	const auto err = reindexer->UpdateIndex(kNs, IndexDef{"score_comp", {"id", "score"}, IndexCompositeHash, IndexOpts{}});
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_NE(err.whatStr().find("the change may alter indexing_condition results"), std::string::npos) << err.what();
	CheckIndexDefinitions(before[0].indexes);
	checkIds({3});

	const auto dropErr = reindexer->DropIndex(kNs, IndexDef{"score_comp"});
	EXPECT_EQ(dropErr.code(), errLogic) << dropErr.what();
	EXPECT_NE(dropErr.whatStr().find("referenced by indexing_condition of fulltext index 'title'"), std::string::npos) << dropErr.what();
	CheckIndexDefinitions(before[0].indexes);
	checkIds({3});
}

TEST_F(FtIndexingConditionApi, RejectsReferencedIndexSemanticChanges) {
	UpsertItem(1, "series", "semantic token");
	UpsertItem(2, "episode", "semantic token");
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);

	for (const auto& idx :
		 {IndexDef{"type", {"type"}, "hash", "string", IndexOpts{}.SetCollateMode(CollateASCII)},
		  IndexDef{"type", {"type"}, "hash", "string", IndexOpts{}.Sparse()}, IndexDef{"type", {"other"}, "hash", "string", IndexOpts{}}}) {
		const auto err = reindexer->UpdateIndex(kNs, idx);
		EXPECT_EQ(err.code(), errLogic) << err.what();
		EXPECT_EQ(err.whatStr(),
				  "Cannot update index 'type' in namespace 'ft_indexing_condition_ns': indexing_condition of fulltext "
				  "index 'title': the change may alter indexing_condition results; rebuilding dependent fulltext indexes "
				  "is not supported");
		CheckIndexDefinitions(before[0].indexes);
		EXPECT_EQ(SelectByTitle("semantic").Count(), 1);
	}
	Restart(kNs);
	CheckIndexDefinitions(before[0].indexes);
	EXPECT_EQ(SelectByTitle("semantic").Count(), 1);
	UpsertItem(2, "series", "semantic token");
	EXPECT_EQ(SelectByTitle("semantic").Count(), 2);

	EXPECT_EQ(Delete(Query(kNs)), 2);
	// The fast-update case must rebind collation too, including when the changed field belongs to a composite.
	AddIndex(kNs, IndexDef{"type_score", {"type", "score"}, "hash", "composite", IndexOpts{}});
	UpdateIndex(kNs, IndexDef{"type", {"type"}, "hash", "string", IndexOpts{}.SetCollateMode(CollateASCII)});
	UpsertItem(1, "SERIES", "semantic token");
	EXPECT_EQ(SelectByTitle("semantic").Count(), 1);
	Restart(kNs);
	EXPECT_EQ(SelectByTitle("semantic").Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsReferencedSparseToRegularChange) {
	AddIndex(kNs, IndexDef{"optional", {"optional"}, "hash", "string", IndexOpts{}.Sparse()});
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "optional IS NULL"};
	UpdateIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"title":"missing token"})json");
	UpsertJSON(kNs, R"json({"id":2,"optional":"present","title":"missing token"})json");
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);

	const auto err = reindexer->UpdateIndex(kNs, IndexDef{"optional", {"optional"}, "hash", "string", IndexOpts{}});
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_NE(err.whatStr().find("The 'is NULL' condition is supported only by 'sparse' or 'array' indexes"), std::string::npos)
		<< err.what();
	CheckIndexDefinitions(before[0].indexes);
	EXPECT_EQ(SelectByTitle("missing").Count(), 1);
	Restart(kNs);
	EXPECT_EQ(SelectByTitle("missing").Count(), 1);

	// Emptiness permits a different binding, but never an invalid condition. A failed prepare must leave it usable.
	EXPECT_EQ(Delete(Query(kNs)), 2);
	const auto emptyErr = reindexer->UpdateIndex(kNs, IndexDef{"optional", {"optional"}, "hash", "string", IndexOpts{}});
	EXPECT_EQ(emptyErr.code(), errParams) << emptyErr.what();
	EXPECT_NE(emptyErr.whatStr().find("The 'is NULL' condition is supported only by 'sparse' or 'array' indexes"), std::string::npos)
		<< emptyErr.what();
	CheckIndexDefinitions(before[0].indexes);
	UpdateIndex(kNs, IndexDef{"optional", {"optional"}, "hash", "string", IndexOpts{}.Sparse().SetCollateMode(CollateASCII)});
	UpsertJSON(kNs, R"json({"id":1,"title":"missing token"})json");
	EXPECT_EQ(SelectByTitle("missing").Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsReferencedIndexTypeChanges) {
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);
	for (const auto& idx :
		 {IndexDef{"type", {"type"}, "text", "string", IndexOpts{}}, IndexDef{"type", {"type"}, "rtree", "point", IndexOpts{}},
		  IndexDef{"type",
				   {"type"},
				   "vec_bf",
				   "float_vector",
				   IndexOpts{}.SetFloatVector(IndexVectorBruteforce, reindexer::FloatVectorIndexOpts{}.SetDimension(4))}}) {
		const auto err = reindexer->UpdateIndex(kNs, idx);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_NE(err.whatStr().find("indexing_condition must not reference"), std::string::npos) << err.what();
		CheckIndexDefinitions(before[0].indexes);
	}
	UpsertItem(1, "series", "preserved token");
	EXPECT_EQ(SelectByTitle("preserved").Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsFieldTypeChangeUsedOnEitherSideOfCondition) {
	UpsertItem(1, "series", "numeric token", 1);
	for (const auto condition : {"score = id", "id = score"}) {
		IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
		UpdateIndex(kNs, ftIdx);
		const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
		ASSERT_EQ(before.size(), 1);
		for (const auto& idx : {IndexDef{"score", {"score"}, "tree", "int64", IndexOpts{}},
								IndexDef{"score", {"score"}, "tree", "int", IndexOpts{}.Array()}}) {
			const auto err = reindexer->UpdateIndex(kNs, idx);
			if (idx.Opts().IsArray()) {
				EXPECT_EQ(err.code(), errParams) << err.what();
				EXPECT_NE(err.whatStr().find("Can't convert array index to not array and vice versa in non-empty namespace"),
						  std::string::npos)
					<< err.what();
			} else {
				EXPECT_EQ(err.code(), errLogic) << err.what();
				EXPECT_NE(err.whatStr().find("the change may alter indexing_condition results"), std::string::npos) << err.what();
			}
			CheckIndexDefinitions(before[0].indexes);
		}
	}
	EXPECT_EQ(SelectByTitle("numeric").Count(), 1);
}

TEST_F(FtIndexingConditionApi, RejectsDroppingReferencedIndex) {
	UpsertItem(1, "series", "drop token");
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);
	const auto err = reindexer->DropIndex(kNs, IndexDef{"type"});
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_EQ(err.whatStr(),
			  "Cannot drop index 'type': it is referenced by indexing_condition of fulltext index 'title' in namespace "
			  "'ft_indexing_condition_ns'");
	CheckIndexDefinitions(before[0].indexes);
	EXPECT_EQ(SelectByTitle("drop").Count(), 1);
}

TEST_F(FtIndexingConditionApi, DropsIndexWithItsOwnConditionButChecksOtherIndexes) {
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"meta.title"}, "text", "string", IndexOpts{}, 0, "NOT (meta IS NULL)"}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}, 0, "NOT (meta IS NULL)"}));
	ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, R"json({"id":1,"meta":{"title":"token"},"notes":"token"})json"));
	ASSERT_EQ(SelectByTitle("token").Count(), 1);

	const auto err = reindexer->DropIndex(kNs, IndexDef{"TITLE"});
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_EQ(err.whatStr(),
			  "Cannot drop index 'title': it is referenced by indexing_condition of fulltext index 'notes' in namespace "
			  "'ft_indexing_condition_ns'");
	EXPECT_EQ(SelectByTitle("token").Count(), 1);
	EXPECT_EQ(Select(Query(kNs).Where("notes", CondEq, "token")).Count(), 1);

	ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "notes"));
	ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "TITLE"));
	EXPECT_EQ(Select(Query(kNs).Where("meta.title", CondEq, "token")).Count(), 1);
}

TEST_F(FtIndexingConditionApi, IndexRemovalRespectsConditionJsonPathCase) {
	AddIndex(kNs, IndexDef{"unrelated", {"meta.kind"}, "hash", "string", IndexOpts{}.Sparse()});
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "Meta.kind = 'series' AND TyPe = 'series'"});
	UpsertJSON(kNs, R"json({"id":1,"type":"series","Meta":{"kind":"series"},"meta":{"kind":"episode"},"title":"drop token"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","Meta":{"kind":"episode"},"meta":{"kind":"series"},"title":"drop token"})json");
	ASSERT_EQ(SelectByTitle("drop").Count(), 1);

	DropIndex(kNs, "unrelated");
	const auto err = reindexer->DropIndex(kNs, IndexDef{"TYPE"});
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_EQ(err.whatStr(),
			  "Cannot drop index 'type': it is referenced by indexing_condition of fulltext index 'title' in namespace "
			  "'ft_indexing_condition_ns'");
	const auto qr = SelectByTitle("drop");
	ASSERT_EQ(qr.Count(), 1);
	EXPECT_EQ(qr.begin().GetItem()["id"].As<int>(), 1);
}

TEST_F(FtIndexingConditionApi, KeepsConditionAfterReferencedIndexUpdate) {
	UpsertItem(1, "series", "referenced update");
	UpsertItem(2, "episode", "referenced update");
	ASSERT_EQ(SelectByTitle("referenced").Count(), 1);

	UpdateIndex(kNs, IndexDef{"type", {"type"}, "tree", "string", IndexOpts{}});
	EXPECT_EQ(SelectByTitle("referenced").Count(), 1);
	Restart(kNs);
	EXPECT_EQ(SelectByTitle("referenced").Count(), 1);

	UpsertItem(2, "series", "referenced update");
	EXPECT_EQ(SelectByTitle("referenced").Count(), 2);
}

TEST_F(FtIndexingConditionApi, BindsConditionsAfterAddingNamespaceIndexes) {
	ASSERT_NO_FATAL_FAILURE(DropNamespace(kNs));
	const std::string condition = "kind_index = 'series' AND gate_index = 1";
	reindexer::NamespaceDef definition{kNs};
	definition.indexes = {IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()},
						  IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition},
						  IndexDef{"gate_index", {"metadata.gate"}, "hash", "int", IndexOpts{}},
						  IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}.Sparse()},
						  IndexDef{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, condition}};
	ASSERT_NO_FATAL_FAILURE(AddNamespace(definition));
	CheckIndexDefinitions(definition.indexes);
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series","gate":1},"kind_index":"episode","gate_index":0,
		"title":"bootstrap token","description":"bootstrap token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode","gate":1},"kind_index":"series","gate_index":1,
		"title":"bootstrap token","description":"bootstrap token"})json");
	UpsertJSON(kNs, R"json({"id":3,"metadata":{"kind":"series","gate":0},"kind_index":"series","gate_index":1,
		"title":"bootstrap token","description":"bootstrap token"})json");
	const auto check = [this] {
		for (const auto index : {"title", "search"}) {
			SCOPED_TRACE(index);
			const auto items = Select(Query(kNs).Where(index, CondEq, "bootstrap"));
			ASSERT_EQ(items.Count(), 1);
			EXPECT_EQ(items.begin().GetItem()["id"].As<int>(), 1);
		}
	};
	check();
	// Reopening the namespace must keep its conditions and documents intact.
	ASSERT_NO_FATAL_FAILURE(CloseNamespace(kNs));
	ASSERT_NO_FATAL_FAILURE(OpenNamespace(kNs));
	CheckIndexDefinitions(definition.indexes);
	check();
}

TEST_F(FtIndexingConditionApi, KeepsExistingNamespaceAfterFailedAddNamespace) {
	ASSERT_NO_FATAL_FAILURE(DropNamespace(kNs));
	reindexer::NamespaceDef definition{kNs};
	definition.indexes = {IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()},
						  IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "kind_index = 'series'"},
						  IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}}};
	ASSERT_NO_FATAL_FAILURE(AddNamespace(definition));
	ASSERT_NO_FATAL_FAILURE(CheckIndexDefinitions(definition.indexes));
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"title":"existing token"})json");
	ASSERT_EQ(SelectByTitle("existing").Count(), 1);

	const auto err = reindexer->AddNamespace(definition);
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_EQ(err.whatStr(), fmt::format("Namespace '{}' already exists", kNs));
	ASSERT_NO_FATAL_FAILURE(CheckIndexDefinitions(definition.indexes));
	EXPECT_EQ(SelectByTitle("existing").Count(), 1);
}

TEST_F(FtIndexingConditionApi, RestoresConditionsFromRawSnapshot) {
	DropIndex(kNs, "title");
	// Before kind_index is restored, its name resolves to this integer JSON-path.
	AddIndex(kNs, IndexDef{"raw_kind", {"kind_index"}, "hash", "int", IndexOpts{}});
	AddIndex(kNs, IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}.Sparse()});
	AddIndex(kNs, IndexDef{"gate_index", {"metadata.gate"}, "hash", "int", IndexOpts{}});
	const std::string condition = "kind_index = 'series' AND gate_index = 1";
	AddIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
	AddIndex(kNs, IndexDef{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, condition});
	constexpr auto kTimeout = std::chrono::seconds{15};
	auto replica = reindexer->WithLSN(reindexer::lsn_t{0, 0}).WithTimeout(kTimeout);
	const auto source = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
	ASSERT_EQ(source.size(), 1);
	for (const int itemCount : {0, 503}) {
		SCOPED_TRACE(itemCount);
		if (itemCount != 0) {
			UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series","gate":1},"kind_index":1,"gate_index":0,
				"title":"snapshot token","description":"snapshot token"})json");
			UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode","gate":1},"kind_index":2,"gate_index":1,
				"title":"snapshot token","description":"snapshot token"})json");
			UpsertJSON(kNs, R"json({"id":3,"metadata":{"kind":"series","gate":0},"kind_index":3,"gate_index":1,
				"title":"snapshot token","description":"snapshot token"})json");
			// Keep later RAW chunks non-empty without adding matches to the conditional indexes.
			for (int id = 4; id <= itemCount; ++id) {
				UpsertItem(id, "episode", "snapshot token");
			}
		}
		reindexer::Snapshot snapshot;
		auto err = reindexer->GetSnapshot(kNs, reindexer::SnapshotOpts{}, snapshot);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(snapshot.HasRawData());
		if (itemCount != 0) {
			ASSERT_GT(snapshot.RawDataSize(), 1);
		}
		std::string targetNs;
		err = replica.CreateTemporaryNamespace(kNs, targetNs, StorageOpts{}, snapshot.NsVersion());
		ASSERT_TRUE(err.ok()) << err.what();
		for (auto& it : snapshot) {
			const auto chunk = it.Chunk();
			err = replica.ApplySnapshotChunk(targetNs, chunk);
			ASSERT_TRUE(err.ok()) << err.what();
			if (!chunk.IsWAL()) {
				const auto partial = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(targetNs));
				ASSERT_EQ(partial.size(), 1);
				ASSERT_EQ(partial[0].indexes.size(), source[0].indexes.size());
				for (size_t i = 0; i < source[0].indexes.size(); ++i) {
					EXPECT_TRUE(partial[0].indexes[i].Compare(source[0].indexes[i]).Equal()) << source[0].indexes[i].Name();
				}
			}
		}
		const auto restored = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(targetNs));
		ASSERT_EQ(restored.size(), 1);
		ASSERT_EQ(restored[0].indexes.size(), source[0].indexes.size());
		for (size_t i = 0; i < source[0].indexes.size(); ++i) {
			EXPECT_TRUE(restored[0].indexes[i].Compare(source[0].indexes[i]).Equal()) << source[0].indexes[i].Name();
		}
		EXPECT_EQ(Select(Query(targetNs)).Count(), itemCount);
		for (const auto index : {"title", "search"}) {
			SCOPED_TRACE(index);
			const auto items = Select(Query(targetNs).Where(index, CondEq, "snapshot"));
			ASSERT_EQ(items.Count(), itemCount == 0 ? 0 : 1);
			if (itemCount != 0) {
				EXPECT_EQ(items.begin().GetItem()["id"].As<int>(), 1);
			}
		}
		if (itemCount == 0) {
			continue;
		}

		// Both ordinary and transaction WAL chunks must work after RAW has populated the namespace.
		UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"series","gate":1},"title":"wal token","description":"wal token"})json");
		auto tx = NewTransaction(kNs);
		auto item = tx.NewItem();
		err = item.FromJSON(R"json({"id":1,"metadata":{"kind":"episode","gate":1},"title":"wal token","description":"wal token"})json");
		ASSERT_TRUE(err.ok()) << err.what();
		err = tx.Upsert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
		std::ignore = CommitTransaction(tx);

		reindexer::Snapshot wal;
		err = reindexer->GetSnapshot(kNs, reindexer::SnapshotOpts{reindexer::ExtendedLsn{snapshot.NsVersion(), snapshot.LastLSN()}}, wal);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_FALSE(wal.HasRawData());
		bool sawRegularItem = false;
		bool sawTransaction = false;
		for (auto& it : wal) {
			const auto chunk = it.Chunk();
			ASSERT_TRUE(chunk.IsWAL());
			ASSERT_FALSE(chunk.IsShallow());
			sawTransaction |= chunk.IsTx();
			for (const auto& record : chunk.Records()) {
				sawRegularItem |= !chunk.IsTx() && record.Unpack().type == reindexer::WalRawItem;
			}
			err = replica.ApplySnapshotChunk(targetNs, chunk);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		EXPECT_TRUE(sawRegularItem);
		EXPECT_TRUE(sawTransaction);
		EXPECT_EQ(Select(Query(targetNs)).Count(), itemCount);
		for (const auto index : {"title", "search"}) {
			SCOPED_TRACE(index);
			const auto items = Select(Query(targetNs).Where(index, CondEq, "wal"));
			ASSERT_EQ(items.Count(), 1);
			EXPECT_EQ(items.begin().GetItem()["id"].As<int>(), 2);
			EXPECT_EQ(Select(Query(targetNs).Where(index, CondEq, "snapshot")).Count(), 0);
		}
	}
}

TEST_F(FtIndexingConditionApi, RejectsInvalidRawSnapshotConditionBeforeLoadingDocuments) {
	constexpr auto kTimeout = std::chrono::seconds{15};
	auto replica = reindexer->WithLSN(reindexer::lsn_t{0, 0}).WithTimeout(kTimeout);
	for (const int itemCount : {0, 1}) {
		SCOPED_TRACE(itemCount);
		std::string targetNs;
		auto err = replica.CreateTemporaryNamespace(kNs, targetNs, StorageOpts{}, reindexer::lsn_t{0, 0});
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::SnapshotChunk chunk;
		chunk.MarkLast();
		for (const auto& index : {IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()},
								  IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "kind_index IS NULL"},
								  IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}}}) {
			reindexer::WrSerializer ser;
			index.GetJSON(ser);
			reindexer::PackedWALRecord record;
			record.Pack(reindexer::WALRecord{reindexer::WalIndexAdd, ser.Slice()});
			chunk.records.emplace_back(reindexer::lsn_t{}, std::move(record));
		}
		if (itemCount != 0) {
			auto item = NewItem(targetNs);
			err = item.FromJSON(R"json({"id":1,"metadata":{"kind":"series"},"title":"blocked token"})json");
			ASSERT_TRUE(err.ok()) << err.what();
			enum { kWithTagsMatcher = true };
			reindexer::PackedWALRecord record;
			record.Pack(reindexer::WALRecord{reindexer::WalRawItem, reindexer::IdType::Zero(), item.GetCJSON(kWithTagsMatcher)});
			chunk.records.emplace_back(reindexer::lsn_t{0, 0}, std::move(record));
		}
		// Invalid conditions must fail both a schema-only chunk and a chunk containing documents.
		err = replica.ApplySnapshotChunk(targetNs, chunk);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_NE(err.whatStr().find("Cannot apply RAW snapshot indexing_condition of index 'title'"), std::string::npos) << err.what();
		EXPECT_NE(err.whatStr().find("The 'is NULL' condition is supported only by 'sparse' or 'array' indexes"), std::string::npos)
			<< err.what();
		EXPECT_EQ(Select(Query(targetNs)).Count(), 0);
	}
}

TEST_F(FtIndexingConditionApi, RestoresConditionsAfterLoadingReferencedIndexAliases) {
	DropIndex(kNs, "title");
	// Restore raw_kind before title, while the sparse kind_index follows title in storage.
	AddIndex(kNs, IndexDef{"raw_kind", {"kind_index"}, "hash", "int", IndexOpts{}});
	AddIndex(kNs, IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}.Sparse()});
	AddIndex(kNs, IndexDef{"gate_index", {"metadata.gate"}, "hash", "int", IndexOpts{}});
	AddIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}});
	const std::string condition = "kind_index = 'series' AND gate_index = 1";
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, condition});
	AddIndex(kNs, IndexDef{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, condition});
	const auto definitions = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(definitions.size(), 1);

	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series","gate":1},"kind_index":1,"gate_index":0,
		"title":"restored token","notes":"restored token","description":"restored token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode","gate":1},"kind_index":2,"gate_index":1,
		"title":"restored token","notes":"restored token","description":"restored token"})json");
	UpsertJSON(kNs, R"json({"id":3,"metadata":{"kind":"series","gate":0},"kind_index":3,"gate_index":1,
		"title":"restored token","notes":"restored token","description":"restored token"})json");
	// Exceed the loader's buffer with different verdicts for reused slots.
	constexpr int kAdditionalItems = 4000;
	static_assert(kAdditionalItems > reindexer::ItemsLoader::kBufferSize);
	std::set<int> expectedBufferIds;
	for (int id = 4; id < 4 + kAdditionalItems; ++id) {
		const bool isSeries = id % 7 == 0;
		const int gate = id % 2;
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);
		jb.Put("id", id);
		{
			auto metadata = jb.Object("metadata");
			metadata.Put("kind", isSeries ? "series" : "episode");
			metadata.Put("gate", gate);
		}
		jb.Put("kind_index", id);
		jb.Put("gate_index", 1 - gate);
		for (const auto field : {"title", "notes", "description"}) {
			jb.Put(field, "buffer token");
		}
		jb.End();
		UpsertJSON(kNs, ser.Slice());
		if (isSeries && gate == 1) {
			expectedBufferIds.insert(id);
		}
	}
	const auto check = [this, &expectedBufferIds](size_t count) {
		for (const auto index : {"title", "notes", "search"}) {
			SCOPED_TRACE(index);
			EXPECT_EQ(Select(Query(kNs).Where(index, CondEq, "restored")).Count(), count);
			const auto items = Select(Query(kNs).Where(index, CondEq, "buffer"));
			EXPECT_EQ(items.Count(), expectedBufferIds.size());
			std::set<int> actualIds;
			for (auto it : items) {
				actualIds.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(actualIds, expectedBufferIds);
		}
	};
	check(1);
	Restart(kNs);
	CheckIndexDefinitions(definitions[0].indexes);
	check(1);
	for (const auto index : {"title", "notes", "search"}) {
		const auto items = Select(Query(kNs).Where(index, CondEq, "restored"));
		ASSERT_EQ(items.Count(), 1);
		EXPECT_EQ(items.begin().GetItem()["id"].As<int>(), 1);
	}

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("metadata.kind", {"series"})), 1);
	check(2);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 3).Set("gate_index", {1})), 1);
	check(3);
	Restart(kNs);
	CheckIndexDefinitions(definitions[0].indexes);
	check(3);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	check(2);
}

class [[nodiscard]] FtIndexingConditionAddIndexApi : public FtIndexingConditionTestBase {
public:
	static constexpr std::string_view kNs = "ft_indexing_condition_add_ns";
	std::string_view nsName() const noexcept override { return kNs; }

	void SetUp() override {
		ConnectIsolated(kNs);
		OpenNamespace(kNs);
		DefineNamespaceDataset(
			kNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"type", "hash", "string", IndexOpts(), 0}});
	}

	void UpsertItem(int id, std::string_view type, std::string_view title) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);
		jb.Put("id", id);
		jb.Put("type", type);
		jb.Put("title", title);
		jb.End();
		auto item = NewItem(kNs);
		ASSERT_TRUE(item.FromJSON(ser.Slice()).ok());
		Upsert(kNs, item);
	}

	QueryResults SelectByTitle(std::string_view dsl) { return Select(Query(kNs).Where("title", CondEq, dsl)); }
};

TEST_F(FtIndexingConditionAddIndexApi, UpdateIndexesStoredTitleAfterConditionalAdd) {
	UpsertItem(1, "episode", "late add token");

	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, ftIdx);

	EXPECT_EQ(SelectByTitle("late").Count(), 0);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("type", {"series"})), 1);
	EXPECT_EQ(SelectByTitle("late").Count(), 1);
}

TEST_F(FtIndexingConditionAddIndexApi, AddsRegularScalarIndexWithMissingAndNullValues) {
	UpsertJSON(kNs, R"json({"id":1,"type":"series"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","title":null})json");
	UpsertJSON(kNs, R"json({"id":3,"type":"series","title":"regular scalar token"})json");

	AddIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}});

	EXPECT_EQ(SelectByTitle("regular").Count(), 1);
	EXPECT_EQ(Select(Query(kNs)).Count(), 3);
}

TEST_F(FtIndexingConditionAddIndexApi, AddsRegularArrayIndexWithNullValueInObjects) {
	UpsertJSON(kNs, R"json({"id":1,"type":"series","objects":[{"title":null},{"title":"regular array token"}]})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","objects":[{"other":"value"}]})json");

	AddIndex(kNs, IndexDef{"object_titles", {"objects.title"}, "text", "string", IndexOpts{}.Array()});

	EXPECT_EQ(Select(Query(kNs).Where("object_titles", CondEq, "regular")).Count(), 1);
	EXPECT_EQ(Select(Query(kNs)).Count(), 2);
}

TEST_F(FtIndexingConditionAddIndexApi, CompositeIndexIndexesOnlyMatchingDocuments) {
	UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"composite title","description":"first description"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"episode","title":"composite title","description":"second description"})json");

	IndexDef ftIdx{"title_description", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, ftIdx);

	EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "composite")).Count(), 1);
	EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "description")).Count(), 1);
}

TEST_F(FtIndexingConditionAddIndexApi, PreselectBeforeFtSkipsRowsAbsentFromIndex) {
	UpsertItem(1, "series", "preselect token");
	UpsertItem(2, "episode", "preselect token");
	UpsertItem(3, "episode", "preselect token");

	const auto ftOpts = IndexOpts{}.SetConfig(IndexFastFT, R"({"enable_preselect_before_ft":true})");
	AddIndex(kNs, IndexDef{"title", {"title"}, "text", "string", ftOpts, 0, "type = 'series'"});

	EXPECT_EQ(Select(Query(kNs).Where("type", CondEq, "episode").Where("title", CondEq, "preselect")).Count(), 0);
	EXPECT_EQ(Select(Query(kNs).Where("id", CondGe, 1).Where("title", CondEq, "preselect")).Count(), 1);
}

TEST_F(FtIndexingConditionAddIndexApi, FutureFieldConditionSurvivesRestartAndDdl) {
	IndexDef scalarIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "metadata.kind = 'series'"};
	AddIndex(kNs, scalarIdx);
	IndexDef compositeIdx{"search", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, "metadata.kind = 'series'"};
	AddIndex(kNs, compositeIdx);

	Restart(kNs);
	DropIndex(kNs, "type");

	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"title":"future token","description":"composite token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode"},"title":"future token","description":"composite token"})json");
	EXPECT_EQ(SelectByTitle("future").Count(), 1);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 1);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("metadata.kind", {"series"})), 1);
	EXPECT_EQ(SelectByTitle("future").Count(), 2);
	EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "composite")).Count(), 2);
}

TEST_F(FtIndexingConditionAddIndexApi, CompositeUpsertChangesConditionAndTextTogether) {
	UpsertJSON(kNs, R"json({"id":1,"type":"episode","title":"excluded","description":"original"})json");
	UpsertJSON(kNs, R"json({"id":2,"type":"series","title":"shared","description":"matching"})json");

	IndexDef ftIdx{"title_description", {"title", "description"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, ftIdx);

	for (int i = 0; i < 3; ++i) {
		SCOPED_TRACE(i);
		EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "excluded")).Count(), 0);
		EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "shared")).Count(), 1);

		UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"shared","description":"matching"})json");
		EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "shared")).Count(), 2);

		UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"unique","description":"changed"})json");
		EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "shared")).Count(), 1);
		EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "unique")).Count(), 1);

		UpsertJSON(kNs, R"json({"id":1,"type":"episode","title":"excluded","description":"original"})json");
		EXPECT_EQ(Select(Query(kNs).Where("title_description", CondEq, "unique")).Count(), 0);
	}
}

class [[nodiscard]] FtIndexingConditionDropFieldApi : public FtIndexingConditionTestBase {
public:
	static constexpr std::string_view kNs = "ft_indexing_condition_drop_ns";
	std::string_view nsName() const noexcept override { return kNs; }

	void SetUp() override {
		ConnectIsolated(kNs);
		OpenNamespace(kNs);
		DefineNamespaceDataset(
			kNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"filler", "hash", "string", IndexOpts(), 0},
				  IndexDeclaration{"type", "hash", "string", IndexOpts(), 0}});

		IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"};
		AddIndex(kNs, ftIdx);
	}

	void UpsertItem(int id, std::string_view filler, std::string_view type, std::string_view title) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder jb(ser);
		jb.Put("id", id);
		jb.Put("filler", filler);
		jb.Put("type", type);
		jb.Put("title", title);
		jb.End();
		auto item = NewItem(kNs);
		ASSERT_TRUE(item.FromJSON(ser.Slice()).ok());
		Upsert(kNs, item);
	}

	QueryResults SelectByTitle(std::string_view dsl) { return Select(Query(kNs).Where("title", CondEq, dsl)); }

	void ExpectDocTitleInJson(int id, std::string_view titleFragment) {
		auto qr = Select(Query(kNs).Where("id", CondEq, id));
		ASSERT_EQ(qr.Count(), 1);
		for (auto it : qr) {
			const std::string json{it.GetItem().GetJSON()};
			EXPECT_NE(json.find(std::string(titleFragment)), std::string::npos) << "id=" << id << " json=" << json;
			return;
		}
	}
};

TEST_F(FtIndexingConditionDropFieldApi, KeepsCompositePredicateAfterDroppingPrecedingField) {
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}};
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, ftIdx));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"score", {"score"}, "hash", "int", IndexOpts{}}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"type_score", {"type", "score"}, IndexCompositeHash, IndexOpts{}}));
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type_score = {'series', 7}"}));

	UpsertJSON(kNs, R"json({"id":1,"filler":"x","type":"series","score":7,"title":"shared token"})json");
	UpsertJSON(kNs, R"json({"id":2,"filler":"x","type":"episode","score":7,"title":"shared token"})json");
	UpsertJSON(kNs, R"json({"id":3,"filler":"x","type":"series","score":8,"title":"shared token"})json");

	const auto checkIds = [this](const std::set<int>& expected) {
		const auto qr = SelectByTitle("shared");
		std::set<int> actual;
		for (auto it : qr) {
			actual.insert(it.GetItem()["id"].As<int>());
		}
		EXPECT_EQ(actual, expected);
	};
	checkIds({1});

	ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "filler"));
	checkIds({1});
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("type", {"series"})), 1);
	checkIds({1, 2});
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 3).Set("score", {7})), 1);
	checkIds({1, 2, 3});
	UpsertJSON(kNs, R"json({"id":1,"type":"episode","score":7,"title":"shared token"})json");
	checkIds({2, 3});
}

TEST_F(FtIndexingConditionDropFieldApi, KeepsConditionalIndexingAfterDroppingPrecedingField) {
	UpsertItem(1, "x", "series", "dropfield token one");
	EXPECT_EQ(SelectByTitle("dropfield").Count(), 1);

	UpsertItem(2, "x", "episode", "dropfield token two");
	ASSERT_EQ(SelectByTitle("dropfield").Count(), 1);
	ExpectDocTitleInJson(2, "dropfield token two");

	DropIndex(kNs, "filler");

	EXPECT_EQ(SelectByTitle("dropfield").Count(), 1);
	ExpectDocTitleInJson(2, "dropfield token two");

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 2).Set("type", {"series"})), 1);
	ExpectDocTitleInJson(2, "dropfield token two");
	EXPECT_EQ(SelectByTitle("dropfield").Count(), 2);
}

}  // namespace reindexer_tests
