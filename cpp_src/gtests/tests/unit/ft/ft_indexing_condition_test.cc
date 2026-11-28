#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <set>
#include <utility>
#include <vector>
#include "core/cjson/jsonbuilder.h"
#include "core/index/indexing_condition.h"
#include "core/namespace/itemsloader.h"
#include "core/namespace/snapshot/snapshot.h"
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
	int64_t FtWordsCount(std::string_view indexName) {
		const auto result = Select(Query(reindexer::kMemStatsNamespace).Where("name", CondEq, nsName()));
		EXPECT_EQ(result.Count(), 1);
		if (result.Count() != 1) {
			return -1;
		}
		const std::string json(result.begin().GetItem().GetJSON());
		gason::JsonParser parser;
		const auto root = parser.Parse(static_cast<std::string_view>(json));
		for (const auto& index : root["indexes"]) {
			if (index["name"].As<std::string_view>() == indexName) {
				return index["text_index_stats"]["words_count"].As<int64_t>();
			}
		}
		ADD_FAILURE() << "Index not found in #memstats: " << indexName;
		return -1;
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

TEST_F(FtIndexingConditionApi, RejectsCompositeIndexesOverConditionalFields) {
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"description", {"description"}, "-", "string", IndexOpts{}}));
	const auto checkFailure = [this](const reindexer::Error& err, const std::vector<IndexDef>& expected) {
		ASSERT_EQ(err.code(), errLogic) << err.what();
		EXPECT_EQ(err.whatStr(),
				  fmt::format("Composite index 'search' cannot include index 'title' with indexing_condition in namespace '{}'", kNs));
		CheckIndexDefinitions(expected);
	};
	for (const int itemsCount : {0, 2}) {
		SCOPED_TRACE(itemsCount);
		if (itemsCount != 0) {
			UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"token","description":"description"})json");
			UpsertJSON(kNs, R"json({"id":2,"type":"episode","title":"token","description":"description"})json");
		}
		for (const auto indexType : {"hash", "tree", "text"}) {
			SCOPED_TRACE(indexType);
			const IndexDef invalid{"search", {"title", "description"}, indexType, "composite", IndexOpts{}};
			const auto beforeAdd = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
			ASSERT_EQ(beforeAdd.size(), 1);
			ASSERT_NO_FATAL_FAILURE(checkFailure(reindexer->AddIndex(kNs, invalid), beforeAdd[0].indexes));

			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"search", {"type", "description"}, indexType, "composite", IndexOpts{}}));
			const auto beforeUpdate = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
			ASSERT_EQ(beforeUpdate.size(), 1);
			ASSERT_NO_FATAL_FAILURE(checkFailure(reindexer->UpdateIndex(kNs, invalid), beforeUpdate[0].indexes));
			EXPECT_EQ(SelectByTitle("token").Count(), itemsCount / 2);
			EXPECT_EQ(Select(Query(kNs)).Count(), itemsCount);
			ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "search"));
		}
	}
}

TEST_F(FtIndexingConditionApi, RejectsConditionalIndexesUsedByComposites) {
	const IndexDef titleDef{"title", {"title"}, "text", "string", IndexOpts{}};
	const IndexDef conditionalTitle{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"};
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, titleDef));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"description", {"description"}, "-", "string", IndexOpts{}}));
	const auto checkFailure = [this](const reindexer::Error& err, std::string_view component, const std::vector<IndexDef>& expected) {
		ASSERT_EQ(err.code(), errLogic) << err.what();
		EXPECT_EQ(err.whatStr(), fmt::format("Composite index 'search' cannot include index '{}' with indexing_condition in namespace '{}'",
											 component, kNs));
		CheckIndexDefinitions(expected);
	};
	const auto setConditionAndCheck = [this, &conditionalTitle](int itemsCount) {
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, conditionalTitle));
		const auto namespaces = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
		ASSERT_EQ(namespaces.size(), 1);
		const auto& indexes = namespaces[0].indexes;
		const auto title = std::find_if(indexes.begin(), indexes.end(), [](const IndexDef& index) { return index.Name() == "title"; });
		ASSERT_NE(title, indexes.end());
		EXPECT_EQ(title->IndexingCondition(), conditionalTitle.IndexingCondition());
		const auto result = SelectByTitle("token");
		ASSERT_EQ(result.Count(), itemsCount / 2);
		if (itemsCount != 0) {
			EXPECT_EQ(result.begin().GetItem()["id"].As<int>(), 1);
		}
		EXPECT_EQ(Select(Query(kNs)).Count(), itemsCount);
	};
	for (const int itemsCount : {0, 2}) {
		SCOPED_TRACE(itemsCount);
		if (itemsCount != 0) {
			UpsertJSON(kNs, R"json({"id":1,"type":"series","title":"token","description":"description",
				"metadata":{"title":"alias"},"extra":{"title":"late"}})json");
			UpsertJSON(kNs, R"json({"id":2,"type":"episode","title":"token","description":"description",
				"metadata":{"title":"alias"},"extra":{"title":"late"}})json");
		}
		for (const auto indexType : {"hash", "tree", "text"}) {
			SCOPED_TRACE(indexType);
			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"search", {"title", "description"}, indexType, "composite", IndexOpts{}}));
			const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
			ASSERT_EQ(before.size(), 1);
			const auto err = reindexer->UpdateIndex(kNs, conditionalTitle);
			ASSERT_NO_FATAL_FAILURE(checkFailure(err, "title", before[0].indexes));
			EXPECT_EQ(SelectByTitle("token").Count(), itemsCount);
			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"search", {"type", "description"}, indexType, "composite", IndexOpts{}}));
			ASSERT_NO_FATAL_FAILURE(setConditionAndCheck(itemsCount));
			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, titleDef));
			EXPECT_EQ(SelectByTitle("token").Count(), itemsCount);

			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"search", {"title", "description"}, indexType, "composite", IndexOpts{}}));
			ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "search"));
			ASSERT_NO_FATAL_FAILURE(setConditionAndCheck(itemsCount));
			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, titleDef));
			EXPECT_EQ(SelectByTitle("token").Count(), itemsCount);
		}

		// A composite referring to a JSON-path must respect the owning index's condition too.
		ASSERT_NO_FATAL_FAILURE(
			AddIndex(kNs, IndexDef{"alias_title", {"metadata.title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"}));
		const auto beforeAlias = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
		ASSERT_EQ(beforeAlias.size(), 1);
		for (const auto path : {"metadata.title", "metadata"}) {
			SCOPED_TRACE(path);
			const auto err = reindexer->AddIndex(kNs, IndexDef{"search", {path, "description"}, IndexCompositeFastFT, IndexOpts{}});
			ASSERT_NO_FATAL_FAILURE(checkFailure(err, "alias_title", beforeAlias[0].indexes));
		}
		ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"search", {"Metadata", "description"}, IndexCompositeFastFT, IndexOpts{}}));
		ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "search"));
		ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "alias_title"));

		// Adding an index may bind a previously non-indexed component of an existing composite.
		for (const auto path : {"extra.title", "extra"}) {
			SCOPED_TRACE(path);
			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"search", {path, "description"}, IndexCompositeFastFT, IndexOpts{}}));
			const auto beforeLate = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
			ASSERT_EQ(beforeLate.size(), 1);
			const auto err =
				reindexer->AddIndex(kNs, IndexDef{"late_title", {"extra.title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"});
			ASSERT_NO_FATAL_FAILURE(checkFailure(err, "late_title", beforeLate[0].indexes));
			EXPECT_EQ(Select(Query(kNs).Where("search", CondEq, "late")).Count(), itemsCount);
			EXPECT_EQ(SelectByTitle("token").Count(), itemsCount);
			EXPECT_EQ(Select(Query(kNs)).Count(), itemsCount);
			ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "search"));
		}
	}

	// A failed sparse addition must not modify the shared cache subtree for "extra".
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"anchor", {"extra.id"}, "hash", "int", IndexOpts{}}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"search", {"extra.title", "description"}, IndexCompositeFastFT, IndexOpts{}}));
	const auto beforeSparse = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
	ASSERT_EQ(beforeSparse.size(), 1);
	const auto err =
		reindexer->AddIndex(kNs, IndexDef{"late_title", {"extra.title"}, "text", "string", IndexOpts{}.Sparse(), 0, "type = 'series'"});
	ASSERT_NO_FATAL_FAILURE(checkFailure(err, "late_title", beforeSparse[0].indexes));
	EXPECT_EQ(Select(Query(kNs).Where("extra.title", CondEq, "late")).Count(), 2);

	ASSERT_NO_FATAL_FAILURE(
		UpsertJSON(kNs, R"json({"id":3,"type":"series","title":"new token","extra":{"id":30,"title":"survivingtoken"}})json"));
	for (const auto& query : {Query(kNs).Where("extra.title", CondEq, "survivingtoken"), Query(kNs).Where("anchor", CondEq, 30),
							  Query(kNs).Where("search", CondEq, "survivingtoken")}) {
		SCOPED_TRACE(query.GetSQL());
		const auto result = Select(query);
		ASSERT_EQ(result.Count(), 1);
		const auto item = result.begin().GetItem();
		EXPECT_EQ(item["id"].As<int>(), 3);
		EXPECT_EQ(item["anchor"].As<int>(), 30);
	}
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

TEST_F(FtIndexingConditionApi, ConditionChangesPreserveStoredTextAndUpdateIndexingLazily) {
	constexpr int kItemsCount = 20;
	const IndexOpts opts = IndexOpts{}.SetConfig(IndexFastFT, R"({"enable_preselect_before_ft":true})");
	const IndexDef indexes[] = {
		IndexDef{"title", {"title"}, "text", "string", opts, 0, "score <= 10"},
		IndexDef{"tags", {"tags"}, "text", "string", IndexOpts{opts}.Array(), 0, "score <= 10"},
		IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, opts, 0, "score <= 10"},
	};
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, indexes[0]));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, indexes[1]));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, indexes[2]));
	for (int id = 1; id <= kItemsCount; ++id) {
		const std::string text = fmt::format("common token{}", id == 11 ? 1 : id);
		ASSERT_NO_FATAL_FAILURE(UpsertJSON(
			kNs, fmt::format(R"json({{"id":{},"score":{},"title":"{}","tags":["{}","{}"],"description":"{}","body":"bodytoken"}})json", id,
							 id, text, text, text, text)));
	}
	const auto storedRefs = [this, kItemsCount] {
		std::vector<std::vector<const reindexer::key_string_impl*>> refs(kItemsCount + 1);
		const auto items = Select(Query(kNs));
		EXPECT_EQ(items.Count(), kItemsCount);
		for (auto it : items) {
			const auto item = it.GetItem();
			const int id = item["id"].As<int>();
			EXPECT_GE(id, 1);
			EXPECT_LE(id, kItemsCount);
			if (id < 1 || id > kItemsCount) {
				continue;
			}
			const std::string text = fmt::format("common token{}", id == 11 ? 1 : id);
			for (const std::string_view field : {"title", "tags"}) {
				const reindexer::VariantArray values = item[field];
				EXPECT_EQ(values.size(), field == "title" ? 1 : 2);
				for (const auto& value : values) {
					EXPECT_EQ(value.As<std::string>(), text);
					refs[id].push_back(static_cast<reindexer::p_string>(value).getBaseKeyString());
				}
			}
		}
		return refs;
	};
	const auto originalRefs = storedRefs();
	for (const auto& index : indexes) {
		// Consecutive condition updates must preserve the first lazy build.
		for (const auto condition : {"score <= 9", "score <= 10"}) {
			ASSERT_NO_FATAL_FAILURE(
				UpdateIndex(kNs, IndexDef{index.Name(), index.JsonPaths(), index.IndexType(), index.Opts(), 0, condition}));
			EXPECT_EQ(FtWordsCount(index.Name()), 0);
		}
		ASSERT_EQ(Select(Query(kNs).Where(index.Name(), CondEq, "common")).Count(), 10);
		ASSERT_GT(FtWordsCount(index.Name()), 0);
	}

	enum class [[nodiscard]] Build { Deferred, Now };
	const struct [[nodiscard]] Step {
		std::string_view condition;
		int firstId;
		int lastId;
		Build build = Build::Now;
	} steps[] = {
		{.condition = "score >= 2 AND score <= 11", .firstId = 2, .lastId = 11},
		{.condition = "score <= 10", .firstId = 1, .lastId = 10},
		{.condition = "score <= 9", .firstId = 1, .lastId = 9},
		{.condition = "score <= 7", .firstId = 1, .lastId = 7},
		{.condition = "score <= 4", .firstId = 1, .lastId = 4},
		{.condition = "score >= 2 AND score <= 4", .firstId = 2, .lastId = 4},
		// Preserve pending activations across another DDL update before selecting.
		{.condition = "score <= 8", .firstId = 1, .lastId = 8, .build = Build::Deferred},
		{.condition = "score <= 9", .firstId = 1, .lastId = 9},
		{.condition = "score <= 10", .firstId = 1, .lastId = 10, .build = Build::Deferred},
		{.condition = "score >= 2 AND score <= 10", .firstId = 2, .lastId = 10},
		{.condition = "score <= 0", .firstId = 1, .lastId = 0},
		{.condition = "", .firstId = 1, .lastId = kItemsCount},
	};
	for (const auto& step : steps) {
		SCOPED_TRACE(step.condition);
		std::set<int> expected;
		std::set<int> expectedVdocs;
		for (int id = step.firstId; id <= step.lastId; ++id) {
			expected.insert(id);
			expectedVdocs.insert(id == 11 ? 1 : id);
		}
		for (const auto& index : indexes) {
			SCOPED_TRACE(index.Name());
			const auto beforeWords = FtWordsCount(index.Name());
			ASSERT_GE(beforeWords, 0);
			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{index.Name(), index.JsonPaths(), index.IndexType(), index.Opts(), 0,
															  static_cast<std::string>(step.condition)}));
			EXPECT_EQ(FtWordsCount(index.Name()), beforeWords);
			if (step.build == Build::Deferred) {
				continue;
			}
			const auto result = Select(Query(kNs).Where(index.Name(), CondEq, "common"));
			ASSERT_EQ(result.Count(), expected.size());
			std::set<int> actual;
			for (auto it : result) {
				actual.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(actual, expected);
			ASSERT_NO_FATAL_FAILURE(CheckFtMemStats(index.Name(), expected.size(), expectedVdocs.size()));
			EXPECT_EQ(Select(Query(kNs).Where("id", CondEq, 11).Where(index.Name(), CondEq, "common")).Count(), expected.count(11));
		}
		EXPECT_EQ(storedRefs(), originalRefs);
		if (step.condition == "score <= 4") {
			std::vector<int64_t> beforeWords;
			for (const auto& index : indexes) {
				beforeWords.push_back(FtWordsCount(index.Name()));
			}
			for (const int score : {20, 1}) {
				ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("score", {score})), 1);
				for (size_t i = 0; i < std::size(indexes); ++i) {
					EXPECT_EQ(Select(Query(kNs).Where(indexes[i].Name(), CondEq, "common")).Count(), score == 1 ? 4 : 3);
					EXPECT_EQ(FtWordsCount(indexes[i].Name()), beforeWords[i]);
				}
				EXPECT_EQ(storedRefs(), originalRefs);
			}
		}
	}
	Restart(kNs);
	std::ignore = storedRefs();
	for (const auto& index : indexes) {
		EXPECT_EQ(Select(Query(kNs).Where(index.Name(), CondEq, "common")).Count(), kItemsCount);
	}
	// Removed rows leave free slots in the storage map traversed by condition updates.
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondGt, 10)), 10);
	// Reopening normalizes configs; use the stored definitions to change only the condition.
	const auto namespaces = EnumNamespaces(reindexer::EnumNamespacesOpts{}.WithFilter(kNs));
	ASSERT_EQ(namespaces.size(), 1);
	const auto& restoredIndexes = namespaces[0].indexes;
	for (const auto& original : indexes) {
		const auto found = std::find_if(restoredIndexes.begin(), restoredIndexes.end(),
										[&original](const IndexDef& index) { return index.Name() == original.Name(); });
		ASSERT_NE(found, restoredIndexes.end());
		const auto& index = *found;
		const auto beforeWords = FtWordsCount(index.Name());
		ASSERT_GT(beforeWords, 0);
		ASSERT_NO_FATAL_FAILURE(
			UpdateIndex(kNs, IndexDef{index.Name(), index.JsonPaths(), index.IndexType(), index.Opts(), 0, "score <= 8"}));
		EXPECT_EQ(FtWordsCount(index.Name()), beforeWords);
		const auto result = Select(Query(kNs).Where(index.Name(), CondEq, "common"));
		ASSERT_EQ(result.Count(), 8);
		std::set<int> actual;
		for (auto it : result) {
			actual.insert(it.GetItem()["id"].As<int>());
		}
		EXPECT_EQ(actual, (std::set<int>{1, 2, 3, 4, 5, 6, 7, 8}));
	}
}

TEST_F(FtIndexingConditionApi, SharedCompositeRowsSurviveConditionRepartition) {
	constexpr int kItemsCount = 128;
	const IndexDef index{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, "score < 3"};
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, index));
	for (int id = 1; id <= kItemsCount; ++id) {
		ASSERT_NO_FATAL_FAILURE(UpsertJSON(
			kNs, fmt::format(R"({{"id":{},"score":{},"marker":{},"description":"shared token","body":"bodytoken"}})", id, id, id)));
	}
	const auto checkIds = [this](std::string_view word, const std::set<int>& expected) {
		const auto result = Select(Query(kNs).Where("search", CondEq, word));
		ASSERT_EQ(result.Count(), expected.size());
		std::set<int> actual;
		for (auto it : result) {
			actual.insert(it.GetItem()["id"].As<int>());
		}
		EXPECT_EQ(actual, expected);
	};
	ASSERT_NO_FATAL_FAILURE(checkIds("shared", {1, 2}));
	const auto wordsCount = FtWordsCount("search");
	ASSERT_GT(wordsCount, 0);

	const struct [[nodiscard]] Step {
		std::string_view condition;
		int firstId;
		int lastId;
	} steps[] = {
		{.condition = "score >= 127", .firstId = 127, .lastId = kItemsCount},
		{.condition = "score > 0", .firstId = 1, .lastId = kItemsCount},
		{.condition = "score < 3", .firstId = 1, .lastId = 2},
	};
	for (const auto& step : steps) {
		SCOPED_TRACE(step.condition);
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(
			kNs,
			IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, static_cast<std::string>(step.condition)}));
		EXPECT_EQ(FtWordsCount("search"), wordsCount);
		std::set<int> expected;
		for (int id = step.firstId; id <= step.lastId; ++id) {
			expected.insert(id);
		}
		ASSERT_NO_FATAL_FAILURE(CheckFtMemStats("search", expected.size(), 1));
		ASSERT_NO_FATAL_FAILURE(checkIds("shared", expected));
	}

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("score", {200})), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 100).Set("score", {1})), 1);
	ASSERT_NO_FATAL_FAILURE(checkIds("shared", {2, 100}));
	ASSERT_NO_FATAL_FAILURE(CheckFtMemStats("search", 2, 1));
	// Refresh the payload of a row moved across the boundary, then detach that row from the shared VDoc.
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 100).Set("marker", {1000})), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 100).Set("description", {"changed token"})), 1);
	ASSERT_NO_FATAL_FAILURE(checkIds("shared", {2}));
	ASSERT_NO_FATAL_FAILURE(checkIds("changed", {100}));
	ASSERT_NO_FATAL_FAILURE(CheckFtMemStats("search", 2, 2));
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 1)), 1);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 128).Set("score", {1})), 1);
	ASSERT_NO_FATAL_FAILURE(checkIds("shared", {128}));
	ASSERT_NO_FATAL_FAILURE(checkIds("changed", {100}));
	ASSERT_EQ(Delete(Query(kNs).Not().Where("id", CondEq, 100)), kItemsCount - 3);
	ASSERT_NO_FATAL_FAILURE(checkIds("shared", {}));
	ASSERT_NO_FATAL_FAILURE(checkIds("changed", {100}));
	Restart(kNs);
	ASSERT_NO_FATAL_FAILURE(checkIds("shared", {}));
	ASSERT_NO_FATAL_FAILURE(checkIds("changed", {100}));
	const auto remaining = Select(Query(kNs));
	ASSERT_EQ(remaining.Count(), 1);
	EXPECT_EQ(remaining.begin().GetItem()["marker"].As<int>(), 1000);
	ASSERT_EQ(Delete(Query(kNs)), 1);
	ASSERT_NO_FATAL_FAILURE(CheckFtMemStats("search", 0, 0));
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

TEST_F(FtIndexingConditionApi, RejectsInvalidIndexingConditionTypesInJson) {
	for (const auto value : {"[]", "{}", "true", "42"}) {
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

TEST_F(FtIndexingConditionApi, JsonConditionCanBeSetAndRemovedByOmittingItOrUsingNull) {
	const auto conditionalDef = IndexDef::FromJSON(
		R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string","indexing_condition":"id = 1"})json");
	ASSERT_TRUE(conditionalDef.has_value());
	EXPECT_EQ(conditionalDef->IndexingCondition(), "id = 1");
	UpsertItem(1, "series", "json token");
	UpsertItem(2, "series", "json token");

	for (const auto json : {
			 R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string"})json",
			 R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string","indexing_condition":null})json",
		 }) {
		SCOPED_TRACE(json);
		UpdateIndex(kNs, *conditionalDef);
		EXPECT_EQ(SelectByTitle("json").Count(), 1);
		const auto unconditionalDef = IndexDef::FromJSON(json);
		ASSERT_TRUE(unconditionalDef.has_value());
		EXPECT_TRUE(unconditionalDef->IndexingCondition().empty());
		UpdateIndex(kNs, *unconditionalDef);
		EXPECT_EQ(SelectByTitle("json").Count(), 2);
	}
}

TEST_F(FtIndexingConditionApi, AddIndexPreservesOmittedOrNullConditionButReplayChecksIt) {
	const IndexDef unconditional{"title", {"title"}, "text", "string", IndexOpts{}};
	const IndexDef conditional{"title", {"title"}, "text", "string", IndexOpts{}, 0, "type = 'series'"};
	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}});
	UpsertItem(1, "series", "preserved token");
	UpsertItem(2, "episode", "preserved token");
	const auto definitions = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(definitions.size(), 1);
	const auto stateBefore = GetReplicationState(kNs);

	AddIndex(kNs, unconditional);
	for (const auto json : {
			 R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string"})json",
			 R"json({"name":"title","json_paths":["title"],"index_type":"text","field_type":"string","indexing_condition":null})json",
		 }) {
		SCOPED_TRACE(json);
		const auto indexDef = IndexDef::FromJSON(json);
		ASSERT_TRUE(indexDef.has_value());
		EXPECT_TRUE(indexDef->IndexingCondition().empty());
		AddIndex(kNs, *indexDef);
	}
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
	AddIndex(kNs, IndexDef{"score_id", {"score", "id"}, IndexCompositeHash, IndexOpts{}});
	const struct [[nodiscard]] TestCase {
		std::string_view condition;
		std::string_view error;
	} cases[] = {
		{.condition = "id = 'abc'", .error = "Can't convert 'abc' to number"},
		{.condition = "id IS NULL", .error = "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes"},
		{.condition = "score_id = NULL", .error = "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes"},
		{.condition = "id IS NOT NULL", .error = "The 'NOT NULL' condition is supported only by 'sparse' or 'array' indexes"},
		{.condition = "unindexed > NULL",
		 .error = "Invalid indexing_condition: Conditions CondGe|CondGt|CondLt|CondLe can't have null argument"},
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

TEST_F(FtIndexingConditionApi, NullConditionsMatchWhereForObjectsAndArrays) {
	for (const auto& index : {IndexDef{"regular_values", {"regular_values"}, "hash", "int", IndexOpts{}.Array()},
							  IndexDef{"sparse_values", {"sparse_values"}, "hash", "int", IndexOpts{}.Array().Sparse()},
							  IndexDef{"sparse_scalar", {"scalar"}, "hash", "int", IndexOpts{}.Sparse()},
							  IndexDef{"multi_values", {"left_values", "right_values"}, "hash", "int", IndexOpts{}.Array()},
							  IndexDef{"nested_values", {"nested.values"}, "hash", "int", IndexOpts{}.Array().Sparse()},
							  IndexDef{"nested_regular", {"nested.regular"}, "hash", "int", IndexOpts{}.Array()},
							  IndexDef{"nested_scalar", {"nested.scalar"}, "hash", "int", IndexOpts{}.Sparse()},
							  IndexDef{"nested_multi", {"left.values", "right.values"}, "hash", "int", IndexOpts{}.Array()}}) {
		ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, index));
	}
	const auto upsert = [this](int id, std::string_view value, std::string_view rightValue = {}) {
		reindexer::WrSerializer ser;
		reindexer::JsonBuilder json{ser};
		json.Put("id", id);
		json.Put("title", "token");
		if (!value.empty()) {
			json.Raw("x", value);
			{
				auto nested = json.Object("nested");
				nested.Raw("x", value);
				if (value.front() != '{') {
					nested.Raw("values", value);
					nested.Raw("regular", value);
					if (value.front() != '[') {
						nested.Raw("scalar", value);
					}
				}
			}
			// Objects are valid only in the non-indexed fields.
			if (value.front() != '{') {
				for (const auto field : {"regular_values", "sparse_values", "left_values"}) {
					json.Raw(field, value);
				}
				if (value.front() != '[') {
					json.Raw("scalar", value);
				}
				auto left = json.Object("left");
				left.Raw("values", value);
			}
		}
		if (!rightValue.empty()) {
			json.Raw("right_values", rightValue);
			auto right = json.Object("right");
			right.Raw("values", rightValue);
		}
		json.End();
		UpsertJSON(kNs, ser.Slice());
	};
	int id = 0;
	for (const auto value : {"", "null", "[]", "{}", R"({"value":1})", "0", "[null]", "[null,1]", "[0]"}) {
		ASSERT_NO_FATAL_FAILURE(upsert(++id, value));
	}
	for (const auto& [left, right] : {std::pair{"null", "1"},
									  {"null", "null"},
									  {"1", "null"},
									  {"[]", "[]"},
									  {"[1]", "[]"},
									  {"[null]", "[null]"},
									  {"[1,null]", "[1,null]"},
									  {"0", "0"},
									  {"[0]", "[0]"}}) {
		ASSERT_NO_FATAL_FAILURE(upsert(++id, left, right));
	}
	ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, R"({"id":19,"title":"token","nested":{},"left":{},"right":{}})"));
	ASSERT_NO_FATAL_FAILURE(upsert(20, R"({"f":null})"));
	ASSERT_NO_FATAL_FAILURE(upsert(21, R"({"f":null,"a":0})"));

	// IS NULL expectations: http://docs-itv.pages.itv.restr.im/reindexer-docs/select/simple_condition/
	// In particular, [null, 1] may match both predicates; an object may match neither.
	const struct [[nodiscard]] TestCase {
		std::vector<std::string_view> fields;
		std::set<int> nullIds;
		std::set<int> nonNullIds;
	} cases[] = {
		{.fields = {"x", "nested.x"},
		 .nullIds = {1, 2, 3, 7, 8, 10, 11, 13, 15, 16, 19},
		 .nonNullIds = {5, 6, 8, 9, 12, 14, 16, 17, 18, 21}},
		{.fields = {"regular_values", "nested_regular"},
		 .nullIds = {1, 2, 3, 4, 5, 10, 11, 13, 19, 20, 21},
		 .nonNullIds = {6, 7, 8, 9, 12, 14, 15, 16, 17, 18}},
		{.fields = {"sparse_values", "nested_values"},
		 .nullIds = {1, 2, 3, 4, 5, 7, 8, 10, 11, 13, 15, 16, 19, 20, 21},
		 .nonNullIds = {6, 8, 9, 12, 14, 16, 17, 18}},
		{.fields = {"sparse_scalar", "nested_scalar"},
		 .nullIds = {1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21},
		 .nonNullIds = {6, 12, 17}},
		{.fields = {"multi_values", "nested_multi"},
		 .nullIds = {1, 2, 3, 4, 5, 11, 13, 19, 20, 21},
		 .nonNullIds = {6, 7, 8, 9, 10, 12, 14, 15, 16, 17, 18}},
	};
	for (const auto& test : cases) {
		for (const auto field : test.fields) {
			for (const std::string_view op : {" IS NULL", " IS NOT NULL", " = NULL"}) {
				const auto condition = fmt::format("{}{}", field, op);
				SCOPED_TRACE(condition);
				const auto& expected = op == " IS NOT NULL" ? test.nonNullIds : test.nullIds;
				ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition}));
				const auto check = [this, &condition](const std::set<int>& expectedIds) {
					std::set<int> whereIds;
					for (auto it : Select(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)))) {
						whereIds.insert(it.GetItem()["id"].As<int>());
					}
					EXPECT_EQ(whereIds, expectedIds);
					std::set<int> actual;
					for (auto it : SelectByTitle("token")) {
						actual.insert(it.GetItem()["id"].As<int>());
					}
					EXPECT_EQ(actual, expectedIds);
				};
				check(expected);
				ASSERT_NO_FATAL_FAILURE(upsert(6, "{}"));
				auto afterUpdate = expected;
				std::ignore = afterUpdate.erase(6);
				if (expected.contains(4)) {
					afterUpdate.insert(6);
				}
				check(afterUpdate);
				ASSERT_NO_FATAL_FAILURE(upsert(6, "0"));
				check(expected);
			}
		}
	}
}

TEST_F(FtIndexingConditionApi, MixedTypesConditionsMatchWhere) {
	for (const auto json : {R"({"id":1,"x":"abc","y":"abc","title":"token"})", R"({"id":2,"x":2,"y":"2","title":"token"})",
							R"({"id":3,"x":2,"y":2.0,"title":"token"})", R"({"id":4,"x":"10","y":2,"title":"token"})",
							R"({"id":5,"x":true,"y":1,"title":"token"})", R"({"id":6,"x":"2.31","y":2.31,"title":"token"})"}) {
		UpsertJSON(kNs, json);
	}
	for (const auto condition : {"x = 2", "x < 2", "x <= 2", "x > 2", "x >= 2", "x IN (2,3)", "x RANGE (1,3)", "x LIKE 'a%'", "x = y",
								 "x < y", "x <= y", "x > y", "x >= y"}) {
		SCOPED_TRACE(condition);
		UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
		std::set<int> expected;
		for (auto it : Select(Query::FromSQL(std::string("SELECT * FROM ") + std::string(kNs) + " WHERE " + condition))) {
			expected.insert(it.GetItem()["id"].As<int>());
		}
		std::set<int> actual;
		for (auto it : SelectByTitle("token")) {
			actual.insert(it.GetItem()["id"].As<int>());
		}
		EXPECT_EQ(actual, expected);
	}
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "x = 2"});
	const auto count = SelectByTitle("token").Count();
	UpsertJSON(kNs, R"({"id":1,"x":2,"y":"abc","title":"token"})");
	EXPECT_EQ(SelectByTitle("token").Count(), count + 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("x", {"abc"})), 1);
	EXPECT_EQ(SelectByTitle("token").Count(), count);
}

TEST_F(FtIndexingConditionApi, IndexedMixedTypesMatchWhereOnRebuildAndInsert) {
	const struct [[nodiscard]] TestValue {
		std::string_view fieldType;
		std::string_view json;
		std::string_view sql;
	} values[] = {
		{.fieldType = "int", .json = "1", .sql = "1"},
		{.fieldType = "double", .json = "1.05", .sql = "1.05"},
		{.fieldType = "bool", .json = "true", .sql = "true"},
		{.fieldType = "string", .json = R"("1")", .sql = "'1'"},
		{.fieldType = "string", .json = R"("1.05")", .sql = "'1.05'"},
		{.fieldType = "string", .json = R"("true")", .sql = "'true'"},
		{.fieldType = "string", .json = R"("abc")", .sql = "'abc'"},
	};
	for (const auto& opts : {IndexOpts{}, IndexOpts{}.Sparse()}) {
		const auto prefix = opts.IsSparse() ? "sparse" : "regular";
		for (const auto type : {"int", "double", "bool", "string"}) {
			const auto field = fmt::format("{}_y_{}", prefix, type);
			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{field, {field}, "", type, opts}));
		}
		for (size_t i = 0; i < std::size(values); ++i) {
			const auto& left = values[i];
			const auto x = fmt::format("{}_x_{}", prefix, i);
			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{x, {x}, "", std::string(left.fieldType), opts}));
			for (const auto& right : values) {
				const auto y = fmt::format("{}_y_{}", prefix, right.fieldType);
				const auto json = fmt::format(R"({{"id":1,"title":"token","{}":{},"{}":{}}})", x, left.json, y, right.json);
				SCOPED_TRACE(json);
				for (const std::string_view operand : {right.sql, std::string_view(y)}) {
					for (const std::string_view op : {"=", "<", "<=", ">", ">=", "IN", "ALLSET", "RANGE", "LIKE"}) {
						if (operand == y && (op == "IN" || op == "ALLSET" || op == "RANGE" || op == "LIKE")) {
							continue;
						}
						if (op == "LIKE" && right.fieldType != "string") {
							continue;
						}
						const auto rhs = op == "RANGE"					  ? fmt::format("({0},{0})", operand)
										 : (op == "IN" || op == "ALLSET") ? fmt::format("({})", operand)
																		  : std::string(operand);
						const auto condition = fmt::format("{} {} {}", x, op, rhs);
						SCOPED_TRACE(condition);
						ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
						ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}}));
						ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, json));
						QueryResults expected;
						const auto whereErr = SelectErr(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)), expected);
						const IndexDef conditional{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
						const auto rebuildErr = reindexer->UpdateIndex(kNs, conditional);
						ASSERT_EQ(rebuildErr.code(), whereErr.code()) << rebuildErr.what() << "; WHERE: " << whereErr.what();
						if (whereErr.ok()) {
							EXPECT_EQ(SelectByTitle("token").Count(), expected.Count());
						}

						ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
						const auto prepareErr = reindexer->UpdateIndex(kNs, conditional);
						if (!prepareErr.ok()) {
							EXPECT_EQ(prepareErr.code(), whereErr.code()) << prepareErr.what() << "; WHERE: " << whereErr.what();
							continue;
						}
						auto item = NewItem(kNs);
						const auto parseErr = item.FromJSON(json);
						ASSERT_TRUE(parseErr.ok()) << parseErr.what();
						const auto insertErr = reindexer->Upsert(kNs, item);
						ASSERT_EQ(insertErr.code(), whereErr.code()) << insertErr.what() << "; WHERE: " << whereErr.what();
						if (whereErr.ok()) {
							EXPECT_EQ(SelectByTitle("token").Count(), expected.Count());
						} else {
							EXPECT_EQ(Select(Query(kNs)).Count(), 0);
						}
					}
				}
			}
		}
	}
}

TEST_F(FtIndexingConditionApi, SingleComponentCompositeMatchesScalarFields) {
	AddIndex(kNs, IndexDef{"single_score", {"score"}, IndexCompositeHash, IndexOpts{}});
	AddIndex(kNs, IndexDef{"other", {"other"}, "hash", "int64", IndexOpts{}});
	for (const auto json : {R"({"id":1,"score":1,"other":1,"type":"series","title":"token"})",
							R"({"id":2,"score":1,"other":2,"type":"series","title":"token"})",
							R"({"id":3,"score":4,"other":3,"type":"series","title":"token"})"}) {
		ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, json));
	}
	for (const auto condition : {"single_score = id", "id = single_score", "single_score < id", "id < single_score", "single_score = other",
								 "other = single_score"}) {
		SCOPED_TRACE(condition);
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition}));
		const auto check = [this, condition] {
			std::set<int> expected;
			for (auto it : Select(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)))) {
				expected.insert(it.GetItem()["id"].As<int>());
			}
			ASSERT_FALSE(expected.empty());
			std::set<int> actual;
			for (auto it : SelectByTitle("token")) {
				actual.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(actual, expected);
		};
		ASSERT_NO_FATAL_FAILURE(check());
		ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, R"({"id":4,"score":4,"other":4,"type":"series","title":"token"})"));
		ASSERT_NO_FATAL_FAILURE(check());
	}
	for (const auto condition : {"single_score = type", "type = single_score"}) {
		SCOPED_TRACE(condition);
		QueryResults result;
		const auto whereErr = SelectErr(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)), result);
		ASSERT_EQ(whereErr.code(), errQueryExec) << whereErr.what();
		const auto conditionErr = reindexer->UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
		EXPECT_EQ(conditionErr.code(), whereErr.code()) << conditionErr.what();
	}
}

TEST_F(FtIndexingConditionApi, BoundaryConditionsMatchWhereOnRebuildAndInsert) {
	AddIndex(kNs, IndexDef{"indexed_y", {"indexed_y"}, "hash", "int", IndexOpts{}.Array()});
	constexpr std::string_view kLikeError = "For condition LIKE fields should be of string type";
	constexpr std::string_view kRangeError = "For condition range second field should be an array of 2 values";
	const struct [[nodiscard]] TestCase {
		std::string_view condition;
		std::string_view fields;
		size_t count;
		std::string_view error = {};
	} cases[] = {
		{.condition = "x LIKE y", .fields = R"("x":"series","y":[])", .count = 0},
		{.condition = "x LIKE y", .fields = R"("x":"series")", .count = 0},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":["a%","ser%"])", .count = 1},
		{.condition = "x LIKE y", .fields = R"("x":["none","series"],"y":["a%","ser%"])", .count = 1},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":["a%","b%"])", .count = 0},
		{.condition = "x LIKE y", .fields = R"("x":[],"y":123)", .count = 0},
		{.condition = "x LIKE y", .fields = R"("x":123,"y":[])", .count = 0},
		{.condition = "x LIKE y", .fields = R"("x":123,"y":"a%")", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":123)", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":[123,"s%"])", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":["s%",123])", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":[123,"series"],"y":"s%")", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":["series",123],"y":"s%")", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":null,"y":"s%")", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":[null,"s%"])", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":"series","y":["s%",null])", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":[null,"series"],"y":"s%")", .count = 0, .error = kLikeError},
		{.condition = "x LIKE y", .fields = R"("x":["series",null],"y":"s%")", .count = 0, .error = kLikeError},
		{.condition = "x LIKE 's%'", .fields = R"("x":[123,"series"])", .count = 1},
		{.condition = "x LIKE 's%'", .fields = R"("x":["series",123])", .count = 1},
		{.condition = "x RANGE y", .fields = R"("x":[0,1],"y":[1,2])", .count = 1},
		{.condition = "x RANGE y", .fields = R"("x":[],"y":[1,2])", .count = 0},
		{.condition = "x RANGE y", .fields = R"("x":[0,3],"y":[1,2])", .count = 0},
		{.condition = "x RANGE y", .fields = R"("x":2.0,"y":[1,2])", .count = 1},
		{.condition = "x RANGE y", .fields = R"("x":"1","y":[1,2])", .count = 0},
		{.condition = "x RANGE y", .fields = R"("x":[null,1],"y":[1,2])", .count = 1},
		{.condition = "x RANGE y", .fields = R"("x":1,"y":[1,2,0])", .count = 1},
		{.condition = "x RANGE y", .fields = R"("x":1,"y":[])", .count = 0, .error = kRangeError},
		{.condition = "x RANGE y", .fields = R"("x":1,"y":[1])", .count = 0, .error = kRangeError},
		{.condition = "x RANGE y", .fields = R"("x":1,"y":[null,2])", .count = 0, .error = kRangeError},
		{.condition = "x RANGE y", .fields = R"("x":1,"y":[1,null])", .count = 0, .error = kRangeError},
		{.condition = "x RANGE y", .fields = R"("x":[],"y":[])", .count = 0, .error = kRangeError},
		{.condition = "x RANGE (1,2)", .fields = R"("x":[0,1])", .count = 1},
		{.condition = "x RANGE (1,2)", .fields = R"("x":[])", .count = 0},
		{.condition = "x RANGE (1,2)", .fields = R"("x":[0,3])", .count = 0},
		{.condition = "x ALLSET ()", .fields = R"("x":[1],"y":[])", .count = 0},
		{.condition = "id ALLSET ()", .fields = R"("x":[1],"y":[])", .count = 0},
		{.condition = "x ALLSET ('2','02')", .fields = R"("x":2)", .count = 1},
		{.condition = "x ALLSET ('2','02')", .fields = R"("x":[2,2])", .count = 1},
		{.condition = "x ALLSET ('2','02')", .fields = R"("x":[2,3])", .count = 1},
		{.condition = "x ALLSET ('2','02')", .fields = R"("x":["2","02"])", .count = 1},
		{.condition = "x ALLSET ('2','02')", .fields = R"("x":[2,"02"])", .count = 1},
		{.condition = "x ALLSET ('02','2')", .fields = R"("x":[2,"02"])", .count = 1},
		{.condition = "x ALLSET ('2','2')", .fields = R"("x":2)", .count = 1},
		{.condition = "id ALLSET ('1','01')", .fields = R"("x":2)", .count = 1},
		{.condition = "x ALLSET ('2','02','002')", .fields = R"("x":2)", .count = 1},
		{.condition = "x ALLSET ('2','02','002')", .fields = R"("x":"2")", .count = 0},
		{.condition = "x ALLSET ('02','2','2.0','002')", .fields = R"("x":2)", .count = 1},
		{.condition = "x ALLSET ('02','2','2.0','002')", .fields = R"("x":"2")", .count = 0},
		{.condition = "x ALLSET ('02','2','2.0','002')", .fields = R"("x":["2",2])", .count = 1},
		{.condition = "x ALLSET (2,2,2,2)", .fields = R"("x":"2.0")", .count = 1},
		{.condition = "x ALLSET (2,2,2,2)", .fields = R"("x":"2.5")", .count = 0},
		{.condition = "x ALLSET ('2',2.13,'+002',-1)", .fields = R"("x":["-1",2])", .count = 0},
		{.condition = "x ALLSET (true,1)", .fields = R"("x":"true")", .count = 0},
		{.condition = "x ALLSET (1,true)", .fields = R"("x":"true")", .count = 0},
		{.condition = "x ALLSET (true,1,true,1)", .fields = R"("x":"true")", .count = 0},
		{.condition = "x ALLSET (1,true,1,true)", .fields = R"("x":"true")", .count = 0},
		{.condition = "x ALLSET (true,1,'true','01')", .fields = R"("x":true)", .count = 1},
		{.condition = "x ALLSET ('true','1','01','001')", .fields = R"("x":[1,true])", .count = 1},
		{.condition = "x ALLSET ('true','1','01','001')", .fields = R"("x":[1])", .count = 0},
		{.condition = "x ALLSET y", .fields = R"("x":[1],"y":[])", .count = 0},
		{.condition = "x ALLSET y", .fields = R"("x":[],"y":[])", .count = 0},
		{.condition = "NOT x ALLSET y", .fields = R"("x":[1],"y":[])", .count = 1},
		{.condition = "NOT x ALLSET y", .fields = R"("x":[],"y":[])", .count = 1},
		{.condition = "id ALLSET indexed_y", .fields = R"("indexed_y":[])", .count = 0},
		{.condition = "id ALLSET indexed_y", .fields = R"("indexed_y":[1])", .count = 1},
		{.condition = "x ALLSET indexed_y", .fields = R"("x":[1],"indexed_y":[])", .count = 0},
		{.condition = "indexed_y ALLSET y", .fields = R"("indexed_y":[1],"y":[])", .count = 0},
		{.condition = "indexed_y ALLSET indexed_y", .fields = R"("indexed_y":[])", .count = 0},
		{.condition = "x ALLSET y", .fields = R"("x":[],"y":[null])", .count = 1},
		{.condition = "x ALLSET y", .fields = R"("x":[1],"y":null)", .count = 1},
		{.condition = "NOT x ALLSET y", .fields = R"("x":[1],"y":[null])", .count = 0},
		{.condition = "x ALLSET y", .fields = R"("x":[1],"y":[null])", .count = 1},
		{.condition = "x ALLSET y", .fields = R"("x":[],"y":[null,1])", .count = 0},
		{.condition = "x ALLSET y", .fields = R"("x":[1],"y":[null,1])", .count = 1},
		{.condition = "x IN (1,2,NULL)", .fields = R"("x":null)", .count = 1},
		{.condition = "x IN (1,2,NULL)", .fields = R"("x":2)", .count = 1},
		{.condition = "x IN (1,2,NULL)", .fields = R"("x":3)", .count = 0},
		{.condition = "x IN (1,2,NULL)", .fields = R"("x":[null,3])", .count = 1},
		{.condition = "x IN (1,2,NULL)", .fields = R"("x":[])", .count = 1},
		{.condition = "x ALLSET (1,2,NULL)", .fields = R"("x":[null,1,2])", .count = 1},
		{.condition = "x ALLSET (1,2,NULL)", .fields = R"("x":[1,2])", .count = 0},
		{.condition = "x ALLSET (1,2,NULL)", .fields = R"("x":[null,1])", .count = 0},
		{.condition = "x ALLSET (1,2,NULL)", .fields = R"("x":null)", .count = 0},
		{.condition = "x ALLSET (1,2,NULL)", .fields = R"("x":[])", .count = 0},
		{.condition = "x ALLSET (1,2,3,4,NULL)", .fields = R"("x":[null,1,2,3,4])", .count = 1},
		{.condition = "x ALLSET (1,2,3,4,NULL)", .fields = R"("x":[1,2,3,4])", .count = 0},
		{.condition = "indexed_y ALLSET (1,2,NULL)", .fields = R"("indexed_y":[null,1,2])", .count = 0},
		{.condition = "indexed_y ALLSET (1,2,NULL)", .fields = R"("indexed_y":[])", .count = 0},
	};
	for (const auto& test : cases) {
		SCOPED_TRACE(test.condition);
		SCOPED_TRACE(test.fields);
		ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}}));
		const auto json = fmt::format(R"({{"id":1,"title":"token",{}}})", test.fields);
		ASSERT_NO_FATAL_FAILURE(UpsertJSON(kNs, json));
		QueryResults expected;
		const auto whereErr =
			SelectErr(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, test.condition)).Strict(StrictModeNone), expected);
		const IndexDef conditional{"title", {"title"}, "text", "string", IndexOpts{}, 0, std::string(test.condition)};
		const auto rebuildErr = reindexer->UpdateIndex(kNs, conditional);
		if (test.error.empty()) {
			ASSERT_TRUE(whereErr.ok()) << whereErr.what();
			ASSERT_TRUE(rebuildErr.ok()) << rebuildErr.what();
			EXPECT_EQ(expected.Count(), test.count);
			EXPECT_EQ(SelectByTitle("token").Count(), expected.Count());
		} else {
			EXPECT_EQ(whereErr.code(), errQueryExec) << whereErr.what();
			EXPECT_NE(whereErr.whatStr().find(test.error), std::string::npos) << whereErr.what();
			EXPECT_EQ(rebuildErr.code(), whereErr.code()) << rebuildErr.what();
			EXPECT_NE(rebuildErr.whatStr().find(test.error), std::string::npos) << rebuildErr.what();
		}

		ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, conditional));
		auto item = NewItem(kNs);
		const auto parseErr = item.FromJSON(json);
		ASSERT_TRUE(parseErr.ok()) << parseErr.what();
		const auto insertErr = reindexer->Upsert(kNs, item);
		EXPECT_EQ(insertErr.code(), whereErr.code()) << insertErr.what();
		if (test.error.empty()) {
			EXPECT_EQ(SelectByTitle("token").Count(), test.count);
		} else {
			EXPECT_NE(insertErr.whatStr().find(test.error), std::string::npos) << insertErr.what();
			EXPECT_EQ(Select(Query(kNs)).Count(), 0);
		}
	}
}

TEST_F(FtIndexingConditionApi, PreparedIndexedAllSetMatchesWhereOnRebuildAndInsert) {
	const struct [[nodiscard]] TestCase {
		std::string_view fieldType;
		std::string_view indexType = {};
		IndexOpts opts{IndexOpts{}.Array()};
		std::string_view right;
		std::string_view first;
		std::string_view second;
	} cases[] = {
		{.fieldType = "int", .right = "('2','02','3')", .first = "2", .second = "3"},
		{.fieldType = "int64", .right = "('2147483648',2147483649)", .first = "2147483648", .second = "2147483649"},
		{.fieldType = "double", .right = "('1.5',2.5)", .first = "1.5", .second = "2.5"},
		{.fieldType = "bool", .right = "(true,false)", .first = "true", .second = "false"},
		{.fieldType = "uuid",
		 .right = "('00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000002')",
		 .first = R"("00000000-0000-4000-8000-000000000001")",
		 .second = R"("00000000-0000-4000-8000-000000000002")"},
		{.fieldType = "string", .right = "('2','02')", .first = R"("2")", .second = R"("02")"},
		{.fieldType = "string",
		 .opts = IndexOpts{}.Array().SetCollateMode(CollateASCII),
		 .right = "('alpha','ALPHA','beta')",
		 .first = R"("ALPHA")",
		 .second = R"("BETA")"},
		{.fieldType = "string",
		 .opts = IndexOpts{}.Array().SetCollateMode(CollateUTF8),
		 .right = "('ёж','ЁЖ','кот')",
		 .first = R"("ЁЖ")",
		 .second = R"("КОТ")"},
		{.fieldType = "string",
		 .opts = IndexOpts{}.Array().SetCollateMode(CollateNumeric),
		 .right = "('2','3')",
		 .first = R"("2")",
		 .second = R"("3")"},
		{.fieldType = "string",
		 .indexType = "tree",
		 .opts = IndexOpts{}.Array().SetCollateMode(CollateNumeric),
		 .right = "('0','00','1')",
		 .first = R"("")",
		 .second = R"("1")"},
		{.fieldType = "string",
		 .indexType = "tree",
		 .opts = IndexOpts{}.Array().Sparse().SetCollateMode(CollateNumeric),
		 .right = "('0','00','1')",
		 .first = R"("")",
		 .second = R"("1")"},
		{.fieldType = "string",
		 .indexType = "hash",
		 .opts = IndexOpts{}.Array().SetCollateMode(CollateNumeric),
		 .right = "('0','00','1')",
		 .first = R"("")",
		 .second = R"("1")"},
		{.fieldType = "string",
		 .indexType = "hash",
		 .opts = IndexOpts{}.Array().Sparse().SetCollateMode(CollateNumeric),
		 .right = "('0','00','1')",
		 .first = R"("")",
		 .second = R"("1")"},
		{.fieldType = "string",
		 .opts = IndexOpts{"a-z"}.Array(),
		 .right = "('alpha','beta')",
		 .first = R"("alpha")",
		 .second = R"("beta")"},
		{.fieldType = "int", .opts = IndexOpts{}.Array().Sparse(), .right = "(2,3)", .first = "2", .second = "3"},
	};
	for (size_t i = 0; i < std::size(cases); ++i) {
		const auto& test = cases[i];
		const auto field = fmt::format("values_{}", i);
		const auto condition = fmt::format("{} ALLSET {}", field, test.right);
		SCOPED_TRACE(condition);
		ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}}));
		ASSERT_NO_FATAL_FAILURE(
			AddIndex(kNs, IndexDef{field, {field}, std::string(test.indexType), std::string(test.fieldType), test.opts}));
		const auto insert = [this](const TestCase& test, std::string_view field) {
			const std::string arrays[] = {fmt::format("[{},{}]", test.first, test.second), fmt::format("[{}]", test.first),
										  fmt::format("[{}]", test.second), "[]", "null"};
			for (size_t id = 0; id < std::size(arrays); ++id) {
				UpsertJSON(kNs, fmt::format(R"({{"id":{},"title":"token","{}":{}}})", id + 1, field, arrays[id]));
			}
			UpsertJSON(kNs, R"({"id":6,"title":"token"})");
		};
		const auto check = [this, &condition] {
			std::set<int> expected;
			for (auto it : Select(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)))) {
				expected.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(expected, (std::set<int>{1}));
			std::set<int> actual;
			for (auto it : SelectByTitle("token")) {
				actual.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(actual, expected);
		};
		ASSERT_NO_FATAL_FAILURE(insert(test, field));
		const IndexDef conditional{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
		ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, conditional));
		ASSERT_NO_FATAL_FAILURE(check());
		ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
		ASSERT_NO_FATAL_FAILURE(insert(test, field));
		ASSERT_NO_FATAL_FAILURE(check());
	}
}

TEST_F(FtIndexingConditionApi, IndexedAllSetMatchTrackingBoundary) {
	const struct [[nodiscard]] TestCase {
		std::string_view fieldType;
		IndexOpts opts;
	} cases[] = {
		{.fieldType = "int", .opts = IndexOpts{}.Array()},
		{.fieldType = "int", .opts = IndexOpts{}.Array().Sparse()},
		{.fieldType = "string", .opts = IndexOpts{}.Array().SetCollateMode(CollateASCII)},
		{.fieldType = "string", .opts = IndexOpts{}.Array().Sparse().SetCollateMode(CollateASCII)},
	};
	for (size_t caseId = 0; caseId < std::size(cases); ++caseId) {
		const auto& test = cases[caseId];
		for (const int count : {100, 101}) {
			const auto field = fmt::format("tracked_values_{}_{}", caseId, count);
			SCOPED_TRACE(field);
			ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}}));
			ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{field, {field}, "", std::string(test.fieldType), test.opts}));
			const auto sqlValue = [&test](int value) {
				return test.fieldType == "string" ? fmt::format("'value{}'", value) : std::to_string(value);
			};
			const auto jsonValue = [&test](int value) {
				return test.fieldType == "string" ? fmt::format(R"("VALUE{}")", value) : std::to_string(value);
			};
			std::string right;
			// Duplicate prefixes must not leave gaps in the matched requirement IDs.
			for (int i = 0; i < 100; ++i) {
				right += sqlValue(0) + ',';
			}
			std::string complete = "[";
			std::string missing = "[";
			for (int i = 0; i < count; ++i) {
				if (i != 0) {
					right += ',';
					complete += ',';
					missing += ',';
				}
				right += sqlValue(i);
				complete += jsonValue(i);
				missing += jsonValue(i == count - 1 ? 0 : i);
			}
			complete += ']';
			missing += ']';
			const std::string docs[] = {complete, missing, '[' + jsonValue(count - 1) + ']', complete, "[]"};
			const auto insert = [this, &docs](std::string_view field) {
				for (size_t i = 0; i < std::size(docs); ++i) {
					UpsertJSON(kNs, fmt::format(R"({{"id":{},"title":"token","{}":{}}})", i + 1, field, docs[i]));
				}
			};
			const auto condition = fmt::format("{} ALLSET ({})", field, right);
			const auto check = [this, &condition](std::string_view field) {
				std::set<int> expected;
				for (auto it : Select(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)))) {
					expected.insert(it.GetItem()["id"].As<int>());
				}
				EXPECT_EQ(expected, (std::set<int>{1, 4}));
				const auto distinct = Select(Query::FromSQL(fmt::format("SELECT * FROM {} WHERE {}", kNs, condition)).Distinct(field));
				ASSERT_EQ(distinct.Count(), 1);
				EXPECT_EQ(distinct.begin().GetItem()["id"].As<int>(), 1);
				std::set<int> actual;
				for (auto it : SelectByTitle("token")) {
					actual.insert(it.GetItem()["id"].As<int>());
				}
				EXPECT_EQ(actual, expected);
			};
			ASSERT_NO_FATAL_FAILURE(insert(field));
			ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition}));
			ASSERT_NO_FATAL_FAILURE(check(field));
			ASSERT_NO_FATAL_FAILURE(TruncateNamespace(kNs));
			ASSERT_NO_FATAL_FAILURE(insert(field));
			ASSERT_NO_FATAL_FAILURE(check(field));
		}
	}
}

TEST(FtIndexingCondition, CompositeNormalizationPreservesNullValues) {
	using namespace reindexer;
	PayloadType pt{"composite_null"};
	pt.Add({KeyValueType::Int{}, "score", {"score"}, IsArray_False});
	pt.Add({KeyValueType::Int{}, "id", {"id"}, IsArray_False});
	QueryEntry entry{"score_id", CondEq, VariantArray{}};
	entry.FieldData().SetIndexData(0, "score_id", FieldsSet{0, 1}, KeyValueType::Composite{}, KeyValueType::Composite{},
								   {KeyValueType::Int{}, KeyValueType::Int{}});
	// Set values directly: the QueryEntry constructor rewrites an equality to NULL as IS NULL.
	entry.SetCondAndValues(CondEq, VariantArray{Variant{}});
	ASSERT_NO_THROW(IndexingCondition::VerifyQueryEntry(entry, nullptr, pt));
	ASSERT_EQ(entry.Values().size(), 1);
	EXPECT_EQ(entry.Condition(), CondEq);
	EXPECT_TRUE(entry.Values()[0].IsNullValue());

	entry.SetCondAndValues(CondSet, VariantArray{Variant{}, Variant{VariantArray{Variant{1.9}, Variant{3}}}, Variant{}});
	ASSERT_NO_THROW(IndexingCondition::VerifyQueryEntry(entry, nullptr, pt));
	ASSERT_EQ(entry.Values().size(), 3);
	EXPECT_EQ(entry.Condition(), CondSet);
	EXPECT_TRUE(entry.Values()[0].IsNullValue());
	EXPECT_TRUE(entry.Values()[2].IsNullValue());
	ASSERT_TRUE(entry.Values()[1].Type().Is<KeyValueType::Composite>());
	const auto key = entry.Values()[1].As<PayloadValue>();
	const auto parts = ConstPayload{pt, key}.GetComposite(entry.Fields(), entry.CompositeFieldsTypes()).As<VariantArray>();
	ASSERT_EQ(parts.size(), 2);
	EXPECT_TRUE(parts[0].Type().Is<KeyValueType::Int>());
	EXPECT_EQ(parts[0].As<int>(), 1);
	EXPECT_EQ(parts[1].As<int>(), 3);
}

TEST_F(FtIndexingConditionApi, CompositeConstantConversionsMatchWhere) {
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}});
	AddIndex(kNs, IndexDef{"score_id", {"score", "id"}, IndexCompositeHash, IndexOpts{}});
	AddIndex(kNs, IndexDef{"type_score", {"type", "score"}, IndexCompositeHash, IndexOpts{}});
	UpsertItem(1, "2", "token", 1);
	UpsertItem(2, "2", "token", 2);
	UpsertItem(3, "3", "token", 1);
	for (const auto condition :
		 {"score_id = {1.9,1.9}", "type_score = {2,1.9}", "score_id ALLSET ({1.9,1.9},{1,1})", "type_score ALLSET ({2,1.9},{'2',1})"}) {
		SCOPED_TRACE(condition);
		UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
		const auto expected = Select(Query::FromSQL(std::string("SELECT * FROM ") + std::string(kNs) + " WHERE " + condition));
		ASSERT_EQ(expected.Count(), 1);
		EXPECT_EQ(expected.begin().GetItem()["id"].As<int>(), 1);
		const auto actual = SelectByTitle("token");
		ASSERT_EQ(actual.Count(), expected.Count());
		EXPECT_EQ(actual.begin().GetItem()["id"].As<int>(), 1);
	}
	Restart(kNs);
	const auto restored = SelectByTitle("token");
	ASSERT_EQ(restored.Count(), 1);
	EXPECT_EQ(restored.begin().GetItem()["id"].As<int>(), 1);
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
	IndexDef compositeIdx{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
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

TEST_F(FtIndexingConditionApi, UpdateOneElementOfSharedTextArray) {
	AddIndex(kNs, IndexDef{"tags", {"tags"}, "text", "string", IndexOpts{}.Array()});
	UpsertJSON(kNs, R"({"id":1,"tags":["orchid","orchid","birch"]})");
	UpsertJSON(kNs, R"({"id":2,"tags":["orchid","orchid","birch"]})");
	ASSERT_EQ(Select(Query(kNs).Where("tags", CondEq, "orchid")).Count(), 2);

	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("tags[0]", {"cedar"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "cedar")).Count(), 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "orchid")).Count(), 2);
	ASSERT_EQ(Delete(Query(kNs).Where("id", CondEq, 2)), 1);
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("tags[1]", {"cedar"})), 1);
	EXPECT_EQ(Select(Query(kNs).Where("tags", CondEq, "orchid")).Count(), 0);
	const auto result = Select(Query(kNs).Where("tags", CondEq, "birch"));
	ASSERT_EQ(result.Count(), 1);
	const reindexer::VariantArray tags = result.begin().GetItem()["tags"];
	ASSERT_EQ(tags.size(), 3);
	EXPECT_EQ(tags[0].As<std::string>(), "cedar");
	EXPECT_EQ(tags[1].As<std::string>(), "cedar");
	EXPECT_EQ(tags[2].As<std::string>(), "birch");
}

TEST_F(FtIndexingConditionApi, FailedConditionEvaluationLeavesUpsertStateUnchanged) {
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "x LIKE pattern"});
	UpsertJSON(kNs, R"json({"id":1,"x":"abc","pattern":"a%","title":"token"})json");
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
		const std::string json =
			std::string(R"json({"id":)json") + std::to_string(id) + R"json(,"x":123,"pattern":"a%","title":"token"})json";
		ASSERT_TRUE(item.FromJSON(json).ok());
		const auto err = reindexer->Upsert(kNs, item);
		ASSERT_EQ(err.code(), errQueryExec) << err.what();
		EXPECT_NE(err.whatStr().find("For condition LIKE fields should be of string type"), std::string::npos) << err.what();

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

	UpsertJSON(kNs, R"json({"id":1,"x":"abc","pattern":"a%","title":"changed"})json");
	UpsertJSON(kNs, R"json({"id":2,"x":"abc","pattern":"a%","title":"token"})json");
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
	IndexDef compositeIdx{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, "type = 'series'"};
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, compositeIdx));
	for (const auto type : {"episode", "series"}) {
		SCOPED_TRACE(type);
		const std::string json =
			std::string(R"json({"id":1,"type":")json") + type +
			R"json(","title":"original token","notes":"original token","description":"original compositeoriginal token"})json";
		UpsertJSON(kNs, json);
		QueryResults result;
		const auto err = reindexer->Update(Query(kNs)
											   .Where("id", CondEq, 1)
											   .Set("title", {"changed token"})
											   .Set("notes", {"changed token"})
											   .Set("description", {"changed compositechanged token"})
											   .Set("type", {"series"})
											   .Set("id", reindexer::Variant{}),
										   result);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.whatStr(), "Index fields that are neither arrays nor sparse cannot be set to null");
		for (const std::string_view index : {"title", "notes", "search"}) {
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
		EXPECT_EQ(item["description"].As<std::string>(), "original compositeoriginal token");
		EXPECT_EQ(item["type"].As<std::string>(), type);
	}
	ASSERT_EQ(Delete(Query(kNs)), 1);
	for (const std::string_view index : {"title", "notes", "search"}) {
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
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}});
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
		  IndexDef{"conditional", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, "non_idx_field = 'string'"}}) {
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

TEST_F(FtIndexingConditionApi, RebuildsConditionsAfterAddingIndexChangingFieldBinding) {
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "metadata.kind = 'series'"};
	UpdateIndex(kNs, ftIdx);
	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, ftIdx.IndexingCondition()});
	AddIndex(kNs, IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, ftIdx.IndexingCondition()});
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"other":"episode",
		"title":"binding token","notes":"binding token","description":"binding token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"SERIES"},"other":"series",
		"title":"binding token","notes":"binding token","description":"binding token"})json");
	const auto checkIds = [this](const std::set<int>& expected) {
		for (const std::string_view index : {"title", "notes", "search"}) {
			SCOPED_TRACE(index);
			const auto result = Select(Query(kNs).Where(index, CondEq, "binding"));
			ASSERT_EQ(result.Count(), expected.size());
			std::set<int> actual;
			for (auto it : result) {
				actual.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(actual, expected);
		}
	};
	checkIds({1});

	// JSON paths remain case-sensitive; an unrelated field must not block DDL.
	AddIndex(kNs, IndexDef{"unrelated", {"metadata.Kind"}, "hash", "string", IndexOpts{}.Sparse()});
	checkIds({1});
	AddIndex(kNs, IndexDef{"alias", {"metadata.kind"}, "hash", "string", IndexOpts{}.Sparse().SetCollateMode(CollateASCII)});
	checkIds({1, 2});
	UpdateIndex(kNs, IndexDef{"alias", {"metadata.kind"}, "hash", "string", IndexOpts{}.SetCollateMode(CollateASCII)});
	checkIds({1, 2});
	AddIndex(kNs, IndexDef{"metadata.kind", {"other"}, "hash", "string", IndexOpts{}.Sparse()});
	checkIds({2});
	for (const auto& opts : {IndexOpts{}.Sparse(), IndexOpts{}}) {
		SCOPED_TRACE(opts.IsSparse() ? "sparse" : "regular");
		UpdateIndex(kNs, IndexDef{"metadata.kind", {"other"}, "hash", "string", opts});
		checkIds({2});
	}
	Restart(kNs);
	checkIds({2});
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("other", {"series"})), 1);
	checkIds({1, 2});
}

TEST_F(FtIndexingConditionApi, RebuildsConditionAfterAddingIndexShadowingIndexedJsonPath) {
	AddIndex(kNs, IndexDef{"kind", {"metadata.kind"}, "hash", "string", IndexOpts{}});
	IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, "metadata.kind = 'series'"};
	UpdateIndex(kNs, ftIdx);
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series"},"other":"episode","title":"shadow token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode"},"other":"series","title":"shadow token"})json");
	ASSERT_EQ(SelectByTitle("shadow").Count(), 1);
	AddIndex(kNs, IndexDef{"metadata.kind", {"other"}, "hash", "string", IndexOpts{}});
	const auto result = SelectByTitle("shadow");
	ASSERT_EQ(result.Count(), 1);
	EXPECT_EQ(result.begin().GetItem()["id"].As<int>(), 2);
	const auto err = reindexer->DropIndex(kNs, IndexDef{"metadata.kind"});
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_EQ(err.whatStr(), fmt::format("Cannot drop index 'metadata.kind': it is referenced by indexing_condition of fulltext index "
										 "'title' in namespace '{}'",
										 kNs));
	ASSERT_NO_FATAL_FAILURE(DropIndex(kNs, "kind"));
	const auto afterDrop = SelectByTitle("shadow");
	ASSERT_EQ(afterDrop.Count(), 1);
	EXPECT_EQ(afterDrop.begin().GetItem()["id"].As<int>(), 2);
}

TEST_F(FtIndexingConditionApi, AllowsIndependentCompositeIndexChanges) {
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "score = 1"}));
	UpsertItem(1, "series", "composite token", 1);
	UpsertItem(2, "series", "composite token", 2);
	UpsertItem(3, "series", "composite token", 1);

	const auto checkIds = [this](const std::set<int>& expected) {
		const auto qr = SelectByTitle("composite");
		ASSERT_EQ(qr.Count(), expected.size());
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
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "score_comp = {1, 3}"}));
	checkIds({});
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"score_comp", {"score", "id"}, IndexCompositeHash, IndexOpts{}}));
	checkIds({3});
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"score_comp", {"id", "score"}, IndexCompositeHash, IndexOpts{}}));
	checkIds({});
	UpsertItem(1, "series", "composite token", 3);
	checkIds({1});
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);

	const auto dropErr = reindexer->DropIndex(kNs, IndexDef{"score_comp"});
	EXPECT_EQ(dropErr.code(), errLogic) << dropErr.what();
	EXPECT_NE(dropErr.whatStr().find("referenced by indexing_condition of fulltext index 'title'"), std::string::npos) << dropErr.what();
	CheckIndexDefinitions(before[0].indexes);
	checkIds({1});
}

TEST_F(FtIndexingConditionApi, RebuildsConditionalIndexesAfterReferencedIndexSemanticChanges) {
	const std::string condition = "type = 'series' OR type = 'ёж'";
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition});
	AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, condition});
	AddIndex(kNs, IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, condition});
	AddIndex(kNs, IndexDef{"type_score", {"type", "score"}, IndexCompositeHash, IndexOpts{}});
	for (int id = 1; id <= 20; ++id) {
		const std::string_view type = id == 1 ? "series" : id == 2 ? "SERIES" : id <= 5 ? "ЁЖ" : id == 6 ? "ёж" : "episode";
		const std::string text = fmt::format("semantic token{}", id);
		UpsertJSON(kNs, fmt::format(R"json({{"id":{},"type":"{}","other":"{}","title":"{}","notes":"{}","description":"{}"}})json", id,
									type, id == 20 ? "series" : "episode", text, text, text));
	}
	const auto checkIds = [this](const std::set<int>& expected) {
		for (const std::string_view index : {"title", "notes", "search"}) {
			SCOPED_TRACE(index);
			const auto result = Select(Query(kNs).Where(index, CondEq, "semantic"));
			ASSERT_EQ(result.Count(), expected.size());
			std::set<int> actual;
			for (auto it : result) {
				actual.insert(it.GetItem()["id"].As<int>());
			}
			EXPECT_EQ(actual, expected);
		}
	};
	checkIds({1, 6});
	for (const auto mode : {CollateASCII, CollateUTF8}) {
		SCOPED_TRACE(static_cast<int>(mode));
		std::vector<int64_t> beforeWords;
		for (const std::string_view index : {"title", "notes", "search"}) {
			beforeWords.push_back(FtWordsCount(index));
			ASSERT_GT(beforeWords.back(), 0);
		}
		UpdateIndex(kNs, IndexDef{"type", {"type"}, "hash", "string", IndexOpts{}.SetCollateMode(mode)});
		size_t pos = 0;
		for (const std::string_view index : {"title", "notes", "search"}) {
			EXPECT_EQ(FtWordsCount(index), beforeWords[pos]);
			++pos;
		}
		checkIds(mode == CollateASCII ? std::set<int>{1, 2, 6} : std::set<int>{1, 2, 3, 4, 5, 6});
	}
	DropIndex(kNs, "type_score");
	for (const auto& opts : {IndexOpts{}.Sparse(), IndexOpts{}}) {
		UpdateIndex(kNs, IndexDef{"type", {"type"}, "hash", "string", opts});
		checkIds({1, 6});
		if (opts.IsSparse()) {
			UpdateIndex(kNs, IndexDef{"type", {"type"}, "hash", "string", IndexOpts{opts}.SetCollateMode(CollateASCII)});
			checkIds({1, 2, 6});
			Restart(kNs);
			checkIds({1, 2, 6});
		}
	}
	UpdateIndex(kNs, IndexDef{"type", {"other"}, "hash", "string", IndexOpts{}});
	checkIds({20});
	Restart(kNs);
	checkIds({20});
	ASSERT_EQ(Update(Query(kNs).Where("id", CondEq, 1).Set("other", {"series"})), 1);
	checkIds({1, 20});
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

TEST_F(FtIndexingConditionApi, AllowsCompatibleFieldTypeChangeUsedOnEitherSideOfCondition) {
	UpsertItem(1, "series", "numeric token", 1);
	UpsertItem(2, "series", "numeric token", 1);
	for (const auto condition : {"score = id", "id = score"}) {
		IndexDef ftIdx{"title", {"title"}, "text", "string", IndexOpts{}, 0, condition};
		UpdateIndex(kNs, ftIdx);
		for (const auto fieldType : {"int64", "int"}) {
			UpdateIndex(kNs, IndexDef{"score", {"score"}, "tree", fieldType, IndexOpts{}});
			const auto result = SelectByTitle("numeric");
			ASSERT_EQ(result.Count(), 1);
			EXPECT_EQ(result.begin().GetItem()["id"].As<int>(), 1);
		}
		const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
		ASSERT_EQ(before.size(), 1);
		const auto err = reindexer->UpdateIndex(kNs, IndexDef{"score", {"score"}, "tree", "int", IndexOpts{}.Array()});
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_NE(err.whatStr().find("Can't convert array index to not array and vice versa in non-empty namespace"), std::string::npos)
			<< err.what();
		CheckIndexDefinitions(before[0].indexes);
	}
	EXPECT_EQ(SelectByTitle("numeric").Count(), 1);
}

TEST_F(FtIndexingConditionApi, FailedReferencedIndexConversionPreservesConditionAndItems) {
	UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "numeric = '7'"});
	UpsertJSON(kNs, R"json({"id":1,"numeric":"7","title":"preserved token"})json");
	UpsertJSON(kNs, R"json({"id":2,"numeric":"bad","title":"preserved token"})json");
	ASSERT_EQ(SelectByTitle("preserved").Count(), 1);
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);
	const auto stateBefore = GetReplicationState(kNs);
	const auto storedJson = [this] {
		std::set<std::string> result;
		for (auto it : Select(Query(kNs))) {
			result.emplace(it.GetItem().GetJSON());
		}
		return result;
	};
	const auto jsonBefore = storedJson();
	const auto err = reindexer->AddIndex(kNs, IndexDef{"numeric", {"numeric"}, "hash", "int", IndexOpts{}});
	ASSERT_FALSE(err.ok());
	EXPECT_NE(err.whatStr().find("bad"), std::string::npos) << err.what();
	CheckIndexDefinitions(before[0].indexes);
	EXPECT_EQ(storedJson(), jsonBefore);
	const auto stateAfter = GetReplicationState(kNs);
	EXPECT_EQ(stateAfter.lsn, stateBefore.lsn);
	EXPECT_EQ(stateAfter.checksum, stateBefore.checksum);
	EXPECT_EQ(stateAfter.dataCount, stateBefore.dataCount);
	EXPECT_EQ(SelectByTitle("preserved").Count(), 1);
	Restart(kNs);
	CheckIndexDefinitions(before[0].indexes);
	EXPECT_EQ(storedJson(), jsonBefore);
	EXPECT_EQ(SelectByTitle("preserved").Count(), 1);
}

TEST_F(FtIndexingConditionApi, FailedConditionRebindingPreservesPreparedIndexes) {
	ASSERT_NO_FATAL_FAILURE(UpdateIndex(kNs, IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "x = '01'"}));
	ASSERT_NO_FATAL_FAILURE(AddIndex(kNs, IndexDef{"notes", {"notes"}, "text", "string", IndexOpts{}.Sparse(), 0, "x LIKE '1'"}));
	UpsertJSON(kNs, R"json({"id":1,"x":"1","title":"preserved first","notes":"preserved first"})json");
	UpsertJSON(kNs, R"json({"id":2,"x":"01","title":"preserved second","notes":"preserved second"})json");
	const auto checkIds = [this] {
		const auto title = SelectByTitle("preserved");
		ASSERT_EQ(title.Count(), 1);
		EXPECT_EQ(title.begin().GetItem()["id"].As<int>(), 2);
		const auto notes = Select(Query(kNs).Where("notes", CondEq, "preserved"));
		ASSERT_EQ(notes.Count(), 1);
		EXPECT_EQ(notes.begin().GetItem()["id"].As<int>(), 1);
	};
	ASSERT_NO_FATAL_FAILURE(checkIds());
	const auto titleWords = FtWordsCount("title");
	const auto notesWords = FtWordsCount("notes");
	ASSERT_GT(titleWords, 0);
	ASSERT_GT(notesWords, 0);
	const auto before = EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNs));
	ASSERT_EQ(before.size(), 1);
	const auto stateBefore = GetReplicationState(kNs);
	const auto storedJson = [this] {
		const auto rows = Select(Query(kNs));
		EXPECT_EQ(rows.Count(), 2);
		std::set<std::string> result;
		for (auto it : rows) {
			result.emplace(it.GetItem().GetJSON());
		}
		return result;
	};
	const auto jsonBefore = storedJson();
	// Rebinding prepares title with both rows, then fails to bind the LIKE condition for notes to the integer index on x.
	const auto err = reindexer->AddIndex(kNs, IndexDef{"x", {"x"}, "hash", "int", IndexOpts{}});
	ASSERT_EQ(err.code(), errQueryExec) << err.what();
	EXPECT_EQ(err.whatStr(), "Condition CondLike with type int");
	ASSERT_NO_FATAL_FAILURE(CheckIndexDefinitions(before[0].indexes));
	EXPECT_EQ(storedJson(), jsonBefore);
	const auto stateAfter = GetReplicationState(kNs);
	EXPECT_EQ(stateAfter.lsn, stateBefore.lsn);
	EXPECT_EQ(stateAfter.checksum, stateBefore.checksum);
	EXPECT_EQ(stateAfter.dataCount, stateBefore.dataCount);
	EXPECT_EQ(FtWordsCount("title"), titleWords);
	EXPECT_EQ(FtWordsCount("notes"), notesWords);
	ASSERT_NO_FATAL_FAILURE(checkIds());
	Restart(kNs);
	ASSERT_NO_FATAL_FAILURE(CheckIndexDefinitions(before[0].indexes));
	EXPECT_EQ(storedJson(), jsonBefore);
	ASSERT_NO_FATAL_FAILURE(checkIds());
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
						  IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, condition}};
	ASSERT_NO_FATAL_FAILURE(AddNamespace(definition));
	CheckIndexDefinitions(definition.indexes);
	UpsertJSON(kNs, R"json({"id":1,"metadata":{"kind":"series","gate":1},"kind_index":"episode","gate_index":0,
		"title":"bootstrap token","description":"bootstrap token"})json");
	UpsertJSON(kNs, R"json({"id":2,"metadata":{"kind":"episode","gate":1},"kind_index":"series","gate_index":1,
		"title":"bootstrap token","description":"bootstrap token"})json");
	UpsertJSON(kNs, R"json({"id":3,"metadata":{"kind":"series","gate":0},"kind_index":"series","gate_index":1,
		"title":"bootstrap token","description":"bootstrap token"})json");
	const auto check = [this] {
		for (const std::string_view index : {"title", "search"}) {
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

TEST_F(FtIndexingConditionApi, RejectsInvalidConditionWhenAddingNamespace) {
	ASSERT_NO_FATAL_FAILURE(DropNamespace(kNs));
	reindexer::NamespaceDef definition{kNs};
	definition.indexes = {IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()},
						  IndexDef{"title", {"title"}, "text", "string", IndexOpts{}, 0, "kind_index IS NULL"},
						  IndexDef{"kind_index", {"metadata.kind"}, "hash", "string", IndexOpts{}}};
	const auto err = reindexer->AddNamespace(definition);
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_EQ(err.whatStr(), fmt::format("Cannot add index 'kind_index' in namespace '{}': indexing_condition of fulltext index 'title': "
										 "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes",
										 kNs));
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
	AddIndex(kNs, IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, condition});
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
		for (const std::string_view index : {"title", "search"}) {
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
		for (const std::string_view index : {"title", "search"}) {
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
	AddIndex(kNs, IndexDef{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, condition});
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
		for (const std::string_view index : {"title", "notes", "search"}) {
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
	IndexDef compositeIdx{"search", {"description", "body"}, IndexCompositeFastFT, IndexOpts{}, 0, "metadata.kind = 'series'"};
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
