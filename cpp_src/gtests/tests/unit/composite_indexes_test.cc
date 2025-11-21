#include "composite_indexes_api.h"
#include "gmock/gmock.h"
#include "vendor/fmt/ranges.h"
#include "yaml-cpp/yaml.h"

using QueryResults = ReindexerApi::QueryResults;
using Item = ReindexerApi::Item;
using Reindexer = ReindexerApi::Reindexer;

TEST_F(CompositeIndexesApi, CompositeIndexesAddTest) {
	addCompositeIndex({kFieldNameBookid, kFieldNameBookid2}, CompositeIndexHash, IndexOpts().PK());
	fillNamespace(0, 100);
	addCompositeIndex({kFieldNameTitle, kFieldNamePages}, CompositeIndexHash, IndexOpts());
	fillNamespace(101, 200);
	addCompositeIndex({kFieldNameTitle, kFieldNamePrice}, CompositeIndexBTree, IndexOpts());
	fillNamespace(201, 300);
}

TEST_F(CompositeIndexesApi, AddIndexWithExistingCompositeIndex) {
	static constexpr const char* namespaceName = "test_ns_add_index";
	static const std::string kFieldNameComposite = std::string{kFieldNameName} + compositePlus + kFieldNameTitle;
	static const std::vector<std::string> suffixes = {"Vol1", "Vol2", "Vol3"};
	static constexpr int size = 10;
	std::vector<std::string> names;
	names.reserve(size);
	for (int i = 0; i < size; ++i) {
		names.push_back(kFieldNameName + suffixes[i % suffixes.size()]);
	}

	rt.OpenNamespace(namespaceName);
	DefineNamespaceDataset(namespaceName, {IndexDeclaration{kFieldNameBookid, "hash", "int", IndexOpts().PK(), 0},
										   IndexDeclaration{kFieldNameBookid2, "hash", "int", IndexOpts(), 0},
										   IndexDeclaration{kFieldNamePages, "hash", "int", IndexOpts(), 0},
										   IndexDeclaration{kFieldNamePrice, "hash", "int", IndexOpts(), 0},
										   IndexDeclaration{kFieldNameComposite.c_str(), "text", "composite", IndexOpts(), 0}});
	for (int i = 0; i < size; ++i) {
		Item item = NewItem(namespaceName);
		item[this->kFieldNameBookid] = i;
		item[this->kFieldNameBookid2] = 777;
		item[this->kFieldNamePages] = 1010;
		item[this->kFieldNamePrice] = 1200;
		item[this->kFieldNameName] = names[i];
		item[this->kFieldNameTitle] = kFieldNameTitle;
		Upsert(namespaceName, item);
	}
	rt.AddIndex(namespaceName, {kFieldNameName, {kFieldNameName}, "text", "string", IndexOpts()});
}

static void selectAll(reindexer::Reindexer* reindexer, const std::string& ns) {
	QueryResults qr;
	Error err = reindexer->Select(Query(ns, 0, 1000, ModeAccurateTotal), qr);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qr) {
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

TEST_F(CompositeIndexesApi, DropTest2) {
	const std::string test_ns = "weird_namespace";
	rt.OpenNamespace(test_ns, StorageOpts().Enabled(false));
	rt.AddIndex(test_ns, {"id", "hash", "int", IndexOpts().PK().Dense()});

	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(test_ns);
		EXPECT_FALSE(!item);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i + 1;

		rt.Upsert(test_ns, item);
	}

	selectAll(rt.reindexer.get(), test_ns);

	rt.DropIndex(test_ns, "id");

	selectAll(rt.reindexer.get(), test_ns);
}

TEST_F(CompositeIndexesApi, CompositeIndexesDropTest) {
	addCompositeIndex({kFieldNameBookid, kFieldNameBookid2}, CompositeIndexHash, IndexOpts().PK());
	fillNamespace(0, 100);
	addCompositeIndex({kFieldNameTitle, kFieldNamePages}, CompositeIndexHash, IndexOpts());
	fillNamespace(101, 200);
	addCompositeIndex({kFieldNameTitle, kFieldNamePrice}, CompositeIndexBTree, IndexOpts());
	fillNamespace(201, 300);

	dropIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePrice}));
	fillNamespace(401, 500);
	dropIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePages}));
	fillNamespace(601, 700);
}

TEST_F(CompositeIndexesApi, CompositeIndexesSelectTest) {
	int priceValue = 77777, pagesValue = 88888;
	constexpr std::string_view titleValue = "test book1 title";
	constexpr std::string_view nameValue = "test book1 name";

	addCompositeIndex({kFieldNameBookid, kFieldNameBookid2}, CompositeIndexHash, IndexOpts().PK());
	fillNamespace(0, 100);

	std::string compositeIndexName(getCompositeIndexName({kFieldNamePrice, kFieldNamePages}));
	addCompositeIndex({kFieldNamePrice, kFieldNamePages}, CompositeIndexHash, IndexOpts());

	addOneRow(300, 3000, std::string(titleValue), pagesValue, priceValue, std::string(nameValue));
	fillNamespace(101, 200);

	auto qr = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondEq, {{Variant(priceValue), Variant(pagesValue)}}));
	ASSERT_EQ(qr.Count(), 1);

	Item pricePageRow = qr.begin().GetItem(false);
	Variant selectedPrice = pricePageRow[kFieldNamePrice];
	Variant selectedPages = pricePageRow[kFieldNamePages];
	EXPECT_EQ(static_cast<int>(selectedPrice), priceValue);
	EXPECT_EQ(static_cast<int>(selectedPages), pagesValue);

	Item titleNameRow = qr.begin().GetItem(false);
	Variant selectedTitle = titleNameRow[kFieldNameTitle];
	Variant selectedName = titleNameRow[kFieldNameName];
	EXPECT_EQ(static_cast<reindexer::key_string>(selectedTitle), titleValue);
	EXPECT_EQ(static_cast<reindexer::key_string>(selectedName), nameValue);

	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName, CondLt, {{Variant(priceValue), Variant(pagesValue)}}));
	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName, CondLe, {{Variant(priceValue), Variant(pagesValue)}}));
	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName, CondGt, {{Variant(priceValue), Variant(pagesValue)}}));
	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName, CondGe, {{Variant(priceValue), Variant(pagesValue)}}));

	fillNamespace(301, 400);

	std::ignore = execAndCompareQuery(
		Query(default_namespace)
			.WhereComposite(compositeIndexName, CondRange, {{Variant(1), Variant(1)}, {Variant(priceValue), Variant(pagesValue)}}));

	std::vector<VariantArray> intKeys;
	intKeys.reserve(10);
	for (int i = 0; i < 10; ++i) {
		intKeys.emplace_back(VariantArray{Variant(i), Variant(i * 5)});
	}
	std::ignore = execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName, CondSet, intKeys));

	dropIndex(compositeIndexName);
	fillNamespace(401, 500);

	std::string compositeIndexName2(getCompositeIndexName({kFieldNameTitle, kFieldNameName}));
	addCompositeIndex({kFieldNameTitle, kFieldNameName}, CompositeIndexBTree, IndexOpts());

	fillNamespace(701, 900);

	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName2, CondEq, {{Variant(titleValue), Variant(nameValue)}}));
	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName2, CondGe, {{Variant(titleValue), Variant(nameValue)}}));
	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName2, CondLt, {{Variant(titleValue), Variant(nameValue)}}));
	std::ignore = execAndCompareQuery(
		Query(default_namespace).WhereComposite(compositeIndexName2, CondLe, {{Variant(titleValue), Variant(nameValue)}}));

	fillNamespace(1201, 2000);

	constexpr size_t kStringKeysCnt = 1010;
	std::vector<VariantArray> stringKeys;
	stringKeys.reserve(kStringKeysCnt);
	for (size_t i = 0; i < kStringKeysCnt; ++i) {
		stringKeys.emplace_back(VariantArray{Variant(RandString()), Variant(RandString())});
	}
	std::ignore = execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName2, CondSet, stringKeys));
	std::ignore = execAndCompareQuery(Query(default_namespace)
										  .Where(kFieldNameName, CondEq, nameValue)
										  .WhereComposite(compositeIndexName2, CondEq, {{Variant(titleValue), Variant(nameValue)}}));

	dropIndex(compositeIndexName2);
	fillNamespace(201, 300);

	std::ignore = execAndCompareQuery(Query(default_namespace));
}

TEST_F(CompositeIndexesApi, SelectsBySubIndexes) {
	// Check if selects work for composite index parts
	struct [[nodiscard]] Case {
		std::string name;
		const std::vector<IndexDeclaration> idxs;
	};

	const std::vector<Case> caseSet = {Case{"hash+store",
											{IndexDeclaration{CompositeIndexesApi::kFieldNameBookid, "hash", "int", IndexOpts().PK(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePrice, "hash", "int", IndexOpts(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePages, "-", "int", IndexOpts(), 0}}},
									   Case{"store+hash",
											{IndexDeclaration{CompositeIndexesApi::kFieldNameBookid, "hash", "int", IndexOpts().PK(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePrice, "-", "int", IndexOpts(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePages, "hash", "int", IndexOpts(), 0}}},
									   Case{"store+store",
											{IndexDeclaration{CompositeIndexesApi::kFieldNameBookid, "hash", "int", IndexOpts().PK(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePrice, "-", "int", IndexOpts(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePages, "-", "int", IndexOpts(), 0}}},
									   Case{"hash+tree",
											{IndexDeclaration{CompositeIndexesApi::kFieldNameBookid, "hash", "int", IndexOpts().PK(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePrice, "hash", "int", IndexOpts(), 0},
											 IndexDeclaration{CompositeIndexesApi::kFieldNamePages, "tree", "int", IndexOpts(), 0}}}};

	for (const auto& c : caseSet) {
		SCOPED_TRACE(c.name);

		rt.DropNamespace(default_namespace);
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, c.idxs);
		addCompositeIndex({kFieldNamePrice, kFieldNamePages}, CompositeIndexHash, IndexOpts());

		int priceValue = 77777, pagesValue = 88888, bookid = 300;
		const char* titleValue = "test book1 title";
		const char* nameValue = "test book1 name";
		for (int i = -5; i < 10; ++i) {
			addOneRow(bookid + i, 3000 + i, titleValue + std::to_string(i), pagesValue, priceValue + i, nameValue + std::to_string(i));
		}
		auto qr = execAndCompareQuery(
			Query(default_namespace).Explain().Where(kFieldNamePrice, CondEq, {priceValue}).Where(kFieldNameBookid, CondEq, {bookid}));
		ASSERT_EQ(qr.Count(), 1) << c.name;
		auto item = qr.begin().GetItem();
		EXPECT_EQ(item[kFieldNameBookid].As<int>(), bookid) << c.name;
		EXPECT_EQ(item[kFieldNamePrice].As<int>(), priceValue) << c.name;
		EXPECT_EQ(item[kFieldNamePages].As<int>(), pagesValue) << c.name;
	}
}

TEST_F(CompositeIndexesApi, CompositeOverCompositeTest) {
	constexpr auto kExpectedErrorPattern = "Cannot create composite index '{}' over the other composite '{}'";
	constexpr size_t stepSize = 10;
	size_t from = 0, to = stepSize;
	auto addData = [this, &from, &to] {
		fillNamespace(from, to);
		from += stepSize;
		to += stepSize;
	};
	auto checkError = [this, &addData, &kExpectedErrorPattern](std::initializer_list<std::string> compositeFields,
															   const std::string& singleField, CompositeIndexType type) {
		auto compositeName = getCompositeIndexName(std::move(compositeFields));
		auto err = tryAddCompositeIndex({compositeName, singleField}, type, IndexOpts());
		EXPECT_EQ(err.code(), errParams) << compositeName;
		EXPECT_EQ(err.what(), fmt::format(kExpectedErrorPattern, getCompositeIndexName({compositeName, singleField}), compositeName))
			<< compositeName;
		addData();

		err = tryAddCompositeIndex({singleField, compositeName}, type, IndexOpts());
		EXPECT_EQ(err.code(), errParams) << compositeName;
		EXPECT_EQ(err.what(), fmt::format(kExpectedErrorPattern, getCompositeIndexName({singleField, compositeName}), compositeName))
			<< compositeName;
		addData();
	};

	addCompositeIndex({kFieldNameBookid, kFieldNameBookid2}, CompositeIndexHash, IndexOpts().PK());
	addData();

	const std::string kNewIdxName = "new_idx";
	rt.AddIndex(default_namespace, {kNewIdxName, {kNewIdxName}, "-", "int", IndexOpts()});
	addData();

	addCompositeIndex({kFieldNameTitle, kNewIdxName}, CompositeIndexHash, IndexOpts());
	addData();

	checkError({kFieldNameBookid, kFieldNameBookid2}, kNewIdxName, CompositeIndexBTree);
	checkError({kFieldNameTitle, kNewIdxName}, kFieldNamePrice, CompositeIndexBTree);
	checkError({kFieldNameBookid, kFieldNameBookid2}, kFieldNamePrice, CompositeIndexHash);

	const auto kComposite1 = getCompositeIndexName({kFieldNameBookid, kFieldNameBookid2});
	const auto kComposite2 = getCompositeIndexName({kFieldNameTitle, kNewIdxName});
	auto err = tryAddCompositeIndex({kComposite1, kComposite2}, CompositeIndexHash, IndexOpts());
	EXPECT_EQ(err.code(), errParams);
	EXPECT_EQ(err.what(), fmt::format(kExpectedErrorPattern, getCompositeIndexName({kComposite1, kComposite2}), kComposite1));
	addData();
}

TEST_F(CompositeIndexesApi, FastUpdateIndex) {
	const std::vector<std::string> kIndexTypes{"-", "hash", "tree"};
	const std::vector<std::string> kIndexNames{"IntIndex", "Int64Index", "DoubleIndex", "StringIndex"};
	const std::vector<std::string> kFieldTypes{"int", "int64", "double", "string"};

	auto indexDef = [](const std::string& idxName, const std::string& fieldType, const std::string& type) {
		return reindexer::IndexDef{idxName, {idxName}, type, fieldType, IndexOpts()};
	};

	rt.AddIndex(default_namespace, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts().PK()});

	for (size_t i = 0; i < kIndexNames.size(); ++i) {
		rt.AddIndex(default_namespace, indexDef(kIndexNames[i], kFieldTypes[i], kIndexTypes[2]));
	}

	auto compParts = {kIndexNames[0], kIndexNames[1], kIndexNames[2], kIndexNames[3]};

	addCompositeIndex(compParts, CompositeIndexHash, IndexOpts());

	for (int i = 0; i < 100; ++i) {
		Item item = NewItem(default_namespace);
		item["id"] = i;
		item[kIndexNames[0]] = i % 10 == 0 ? 0 : rand();
		item[kIndexNames[1]] = i % 10 == 0 ? 1 : rand();
		item[kIndexNames[2]] = i % 10 == 0 ? 2.0 : (rand() / 100.0);
		item[kIndexNames[3]] = i % 10 == 0 ? "string" : RandString();
		Upsert(default_namespace, item);
	};

	auto query = Query(default_namespace)
					 .Explain()
					 .WhereComposite(getCompositeIndexName(compParts), CondEq, {{Variant{0}, Variant{1}, Variant{2.0}, Variant{"string"}}});

	auto qrCheck = rt.Select(query);
	auto checkItems = rt.GetSerializedQrItems(qrCheck);
	auto checkCount = qrCheck.Count();
	for (size_t i = 0; i < kIndexNames.size(); ++i) {
		for (size_t j = 0; j < kIndexTypes.size(); ++j) {
			if (kFieldTypes[i] == "double" && kIndexTypes[j] == "hash") {
				continue;
			}
			rt.UpdateIndex(default_namespace, indexDef(kIndexNames[i], kFieldTypes[i], kIndexTypes[j]));
			auto qr = rt.Select(query);

			ASSERT_EQ(rt.GetSerializedQrItems(qr), checkItems);
			ASSERT_EQ(qr.Count(), checkCount);

			YAML::Node root = YAML::Load(qr.GetExplainResults());
			auto selectors = root["selectors"];
			ASSERT_TRUE(selectors.IsSequence()) << qr.GetExplainResults();
			ASSERT_EQ(selectors.size(), 1) << qr.GetExplainResults();
			ASSERT_EQ(selectors[0]["field"].as<std::string>(), getCompositeIndexName(compParts)) << qr.GetExplainResults();
		}
	}

	for (size_t i = 0; i < kFieldTypes.size(); ++i) {
		for (size_t j = 0; j < kFieldTypes.size(); ++j) {
			if (i == j) {
				continue;
			}
			auto err = rt.reindexer->UpdateIndex(default_namespace, indexDef(kIndexNames[i], kFieldTypes[j], "tree"));
			ASSERT_FALSE(err.ok()) << err.what();
			auto err1Text = fmt::format("Cannot remove index '{}': it's a part of a composite index '.*'", kIndexNames[i]);
			auto err2Text = fmt::format("Cannot convert key from type {} to {}", kFieldTypes[i], kFieldTypes[j]);
			ASSERT_THAT(err.what(), testing::MatchesRegex(fmt::format("({}|{})", err1Text, err2Text)));
		}
	}
}

TEST_F(CompositeIndexesApi, CompositePartsOrdering) {
	// Check that jsonpaths of the composite index preserve ordering after update
	constexpr std::string_view kNsName = "CompositePartsOrderingNS";
	const std::string kField1 = "str_field1", kField2 = "str_field2";
	const reindexer::JsonPaths jsonPaths = {kField2, kField1};

	rt.OpenNamespace(kNsName);
	rt.AddIndex(kNsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts().PK()});
	rt.AddIndex(kNsName, reindexer::IndexDef{kField1, {kField1}, "-", "string", IndexOpts()});
	rt.AddIndex(kNsName, reindexer::IndexDef{"text_composite", reindexer::JsonPaths({kField2, kField1}), "text", "composite", IndexOpts()});
	{
		// Check jsonpaths after insertion
		const auto nsDefs = rt.EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNsName));
		ASSERT_EQ(nsDefs.size(), 1);
		ASSERT_EQ(nsDefs[0].indexes.size(), 3);
		const auto& resPaths = nsDefs[0].indexes[2].JsonPaths();
		EXPECT_TRUE(std::ranges::equal(resPaths, jsonPaths))
			<< fmt::format("Jsonpath in namespace: [{}]; Expected: [{}]", fmt::join(resPaths, ", "), fmt::join(jsonPaths, ", "));
	}
	{
		// Add new index and check jsonpaths again
		rt.AddIndex(kNsName, reindexer::IndexDef{kField2, {kField2}, "-", "string", IndexOpts()});
		const auto nsDefs = rt.EnumNamespaces(reindexer::EnumNamespacesOpts().WithFilter(kNsName));
		ASSERT_EQ(nsDefs.size(), 1);
		ASSERT_EQ(nsDefs[0].indexes.size(), 4);
		const auto& resPaths = nsDefs[0].indexes[3].JsonPaths();
		EXPECT_TRUE(std::ranges::equal(resPaths, jsonPaths))
			<< fmt::format("Jsonpath in namespace: [{}]; Expected: [{}]", fmt::join(resPaths, ", "), fmt::join(jsonPaths, ", "));
	}
	{
		// Unable to drop one of the composite subindexes.
		// If we will support subindex drop someday, we have to also preserver paths ordering here
		const auto& idx = (rand() % 2 == 0) ? kField2 : kField1;
		auto err = rt.reindexer->DropIndex(kNsName, reindexer::IndexDef{idx});
		EXPECT_EQ(err.code(), errLogic);
		EXPECT_EQ(err.whatStr(), fmt::format("Cannot remove index '{}': it's a part of a composite index 'text_composite'", idx));
	}
}
