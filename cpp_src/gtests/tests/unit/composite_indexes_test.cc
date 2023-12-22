#include "composite_indexes_api.h"

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

	Error err = rt.reindexer->OpenNamespace(namespaceName);
	ASSERT_TRUE(err.ok()) << err.what();
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
		err = Commit(namespaceName);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	err = rt.reindexer->AddIndex(namespaceName, {kFieldNameName, {kFieldNameName}, "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
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
	auto err = rt.reindexer->OpenNamespace(test_ns, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(test_ns, {"id", "hash", "int", IndexOpts().PK().Dense()});
	EXPECT_TRUE(err.ok()) << err.what();

	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(test_ns);
		EXPECT_FALSE(!item);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i + 1;

		err = rt.reindexer->Upsert(test_ns, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->Commit(test_ns);
	EXPECT_TRUE(err.ok()) << err.what();

	selectAll(rt.reindexer.get(), test_ns);

	reindexer::IndexDef idef("id");
	err = rt.reindexer->DropIndex(test_ns, idef);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(test_ns);
	EXPECT_TRUE(err.ok()) << err.what();

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
	const char* titleValue = "test book1 title";
	const char* nameValue = "test book1 name";

	addCompositeIndex({kFieldNameBookid, kFieldNameBookid2}, CompositeIndexHash, IndexOpts().PK());
	fillNamespace(0, 100);

	std::string compositeIndexName(getCompositeIndexName({kFieldNamePrice, kFieldNamePages}));
	addCompositeIndex({kFieldNamePrice, kFieldNamePages}, CompositeIndexHash, IndexOpts());

	addOneRow(300, 3000, titleValue, pagesValue, priceValue, nameValue);
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
	EXPECT_EQ(static_cast<reindexer::key_string>(selectedTitle)->compare(std::string(titleValue)), 0);
	EXPECT_EQ(static_cast<reindexer::key_string>(selectedName)->compare(std::string(nameValue)), 0);

	execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName, CondLt, {{Variant(priceValue), Variant(pagesValue)}}));
	execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName, CondLe, {{Variant(priceValue), Variant(pagesValue)}}));
	execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName, CondGt, {{Variant(priceValue), Variant(pagesValue)}}));
	execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName, CondGe, {{Variant(priceValue), Variant(pagesValue)}}));

	fillNamespace(301, 400);

	execAndCompareQuery(
		Query(default_namespace)
			.WhereComposite(compositeIndexName, CondRange, {{Variant(1), Variant(1)}, {Variant(priceValue), Variant(pagesValue)}}));

	std::vector<VariantArray> intKeys;
	intKeys.reserve(10);
	for (int i = 0; i < 10; ++i) {
		intKeys.emplace_back(VariantArray{Variant(i), Variant(i * 5)});
	}
	execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondSet, intKeys));

	dropIndex(compositeIndexName);
	fillNamespace(401, 500);

	std::string compositeIndexName2(getCompositeIndexName({kFieldNameTitle, kFieldNameName}));
	addCompositeIndex({kFieldNameTitle, kFieldNameName}, CompositeIndexBTree, IndexOpts());

	fillNamespace(701, 900);

	execAndCompareQuery(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondEq, {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));
	execAndCompareQuery(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondGe, {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));
	execAndCompareQuery(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondLt, {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));
	execAndCompareQuery(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondLe, {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));

	fillNamespace(1201, 2000);

	std::vector<VariantArray> stringKeys;
	for (size_t i = 0; i < 1010; ++i) {
		stringKeys.emplace_back(VariantArray{Variant(RandString()), Variant(RandString())});
	}
	execAndCompareQuery(Query(default_namespace).WhereComposite(compositeIndexName2.c_str(), CondSet, stringKeys));
	execAndCompareQuery(
		Query(default_namespace)
			.Where(kFieldNameName, CondEq, nameValue)
			.WhereComposite(compositeIndexName2.c_str(), CondEq, {{Variant(std::string(titleValue)), Variant(std::string(nameValue))}}));

	dropIndex(compositeIndexName2);
	fillNamespace(201, 300);

	execAndCompareQuery(Query(default_namespace));
}

TEST_F(CompositeIndexesApi, SelectsBySubIndexes) {
	// Check if selects work for composite index parts
	struct Case {
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
		auto err = rt.reindexer->DropNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << c.name;
		err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << c.name;
		DefineNamespaceDataset(default_namespace, c.idxs);
		std::string compositeIndexName(getCompositeIndexName({kFieldNamePrice, kFieldNamePages}));
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
	constexpr char kExpectedErrorPattern[] = "Cannot create composite index '%s' over the other composite '%s'";
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
		EXPECT_EQ(err.what(), fmt::sprintf(kExpectedErrorPattern, getCompositeIndexName({compositeName, singleField}), compositeName))
			<< compositeName;
		addData();

		err = tryAddCompositeIndex({singleField, compositeName}, type, IndexOpts());
		EXPECT_EQ(err.code(), errParams) << compositeName;
		EXPECT_EQ(err.what(), fmt::sprintf(kExpectedErrorPattern, getCompositeIndexName({singleField, compositeName}), compositeName))
			<< compositeName;
		addData();
	};

	addCompositeIndex({kFieldNameBookid, kFieldNameBookid2}, CompositeIndexHash, IndexOpts().PK());
	addData();

	const std::string kNewIdxName = "new_idx";
	auto err = rt.reindexer->AddIndex(default_namespace, {kNewIdxName, {kNewIdxName}, "-", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
	addData();

	addCompositeIndex({kFieldNameTitle, kNewIdxName}, CompositeIndexHash, IndexOpts());
	addData();

	checkError({kFieldNameBookid, kFieldNameBookid2}, kNewIdxName, CompositeIndexBTree);
	checkError({kFieldNameTitle, kNewIdxName}, kFieldNamePrice, CompositeIndexBTree);
	checkError({kFieldNameBookid, kFieldNameBookid2}, kFieldNamePrice, CompositeIndexHash);

	const auto kComposite1 = getCompositeIndexName({kFieldNameBookid, kFieldNameBookid2});
	const auto kComposite2 = getCompositeIndexName({kFieldNameTitle, kNewIdxName});
	err = tryAddCompositeIndex({kComposite1, kComposite2}, CompositeIndexHash, IndexOpts());
	EXPECT_EQ(err.code(), errParams);
	EXPECT_EQ(err.what(), fmt::sprintf(kExpectedErrorPattern, getCompositeIndexName({kComposite1, kComposite2}), kComposite1));
	addData();
}
