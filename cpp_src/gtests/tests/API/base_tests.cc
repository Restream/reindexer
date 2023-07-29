#include <fstream>
#include <vector>
#include "reindexer_api.h"
#include "tools/errors.h"

#include "core/item.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/queryresults/joinresults.h"
#include "core/reindexer.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

#include <deque>
#include <thread>
#include "debug/backtrace.h"

#include "core/keyvalue/p_string.h"
#include "gason/gason.h"
#include "server/loggerwrapper.h"
#include "tools/serializer.h"

static const std::string kBaseTestsStoragePath = "/tmp/reindex/base_tests";

TEST_F(ReindexerApi, AddNamespace) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_EQ(true, err.ok()) << err.what();

	const auto item = getMemStat(*rt.reindexer, default_namespace);
	ASSERT_EQ(item["storage_ok"].As<bool>(), false);
	ASSERT_EQ(item["storage_enabled"].As<bool>(), false);
	ASSERT_EQ(item["storage_status"].As<std::string>(), "DISABLED");
}

TEST_F(ReindexerApi, AddNamespace_CaseInsensitive) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string upperNS(default_namespace);
	std::transform(default_namespace.begin(), default_namespace.end(), upperNS.begin(), [](int c) { return std::toupper(c); });

	err = rt.reindexer->AddNamespace(reindexer::NamespaceDef(upperNS));
	ASSERT_FALSE(err.ok()) << "Somehow namespace '" << upperNS << "' was added. But namespace '" << default_namespace << "' already exists";
}

TEST_F(ReindexerApi, AddExistingNamespace) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddNamespace(reindexer::NamespaceDef(default_namespace, StorageOpts().Enabled(false)));
	ASSERT_FALSE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, RenameNamespace) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok());
	for (int i = 0; i < 10; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;
		item["column1"] = i + 100;
		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	const std::string renameNamespace("rename_namespace");
	const std::string existingNamespace("existing_namespace");

	err = rt.reindexer->OpenNamespace(existingNamespace);
	ASSERT_TRUE(err.ok()) << err.what();

	auto testInList = [&](const std::string& testNamespaceName, bool inList) {
		std::vector<reindexer::NamespaceDef> namespacesList;
		err = rt.reindexer->EnumNamespaces(namespacesList, reindexer::EnumNamespacesOpts());
		ASSERT_TRUE(err.ok()) << err.what();
		auto r = std::find_if(namespacesList.begin(), namespacesList.end(),
							  [testNamespaceName](const reindexer::NamespaceDef& d) { return d.name == testNamespaceName; });
		if (inList) {
			ASSERT_FALSE(r == namespacesList.end()) << testNamespaceName << " not exist";
		} else {
			ASSERT_TRUE(r == namespacesList.end()) << testNamespaceName << " exist";
		}
	};

	auto getRowsInJSON = [&](const std::string& namespaceName, std::vector<std::string>& resStrings) {
		QueryResults result;
		rt.reindexer->Select(Query(namespaceName), result);
		resStrings.clear();
		for (auto it = result.begin(); it != result.end(); ++it) {
			reindexer::WrSerializer sr;
			it.GetJSON(sr, false);
			std::string_view sv = sr.Slice();
			resStrings.emplace_back(sv.data(), sv.size());
		}
	};

	std::vector<std::string> resStrings;
	std::vector<std::string> resStringsBeforeTest;
	getRowsInJSON(default_namespace, resStringsBeforeTest);

	// ok
	err = rt.reindexer->RenameNamespace(default_namespace, renameNamespace);
	ASSERT_TRUE(err.ok()) << err.what();
	testInList(renameNamespace, true);
	testInList(default_namespace, false);
	getRowsInJSON(renameNamespace, resStrings);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";

	// rename to equal name
	err = rt.reindexer->RenameNamespace(renameNamespace, renameNamespace);
	ASSERT_TRUE(err.ok()) << err.what();
	testInList(renameNamespace, true);
	getRowsInJSON(renameNamespace, resStrings);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";

	// rename to empty namespace
	err = rt.reindexer->RenameNamespace(renameNamespace, "");
	ASSERT_FALSE(err.ok()) << err.what();
	testInList(renameNamespace, true);
	getRowsInJSON(renameNamespace, resStrings);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";

	// rename to system namespace
	err = rt.reindexer->RenameNamespace(renameNamespace, "#rename_namespace");
	ASSERT_FALSE(err.ok()) << err.what();
	testInList(renameNamespace, true);
	getRowsInJSON(renameNamespace, resStrings);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";

	// rename to existing namespace
	err = rt.reindexer->RenameNamespace(renameNamespace, existingNamespace);
	ASSERT_TRUE(err.ok()) << err.what();
	testInList(renameNamespace, false);
	testInList(existingNamespace, true);
	getRowsInJSON(existingNamespace, resStrings);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";
}

TEST_F(ReindexerApi, AddIndex) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, DistinctDiffType) {
	reindexer::p_string stringVal("abc");
	std::hash<reindexer::p_string> hashStr;
	size_t vString = hashStr(stringVal);
	std::hash<size_t> hashInt;
	auto vInt = hashInt(vString);
	ASSERT_EQ(vString, vInt) << "hash not equals";

	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	{
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = 1;
		item["column1"] = int64_t(vInt);
		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	{
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = 2;
		item["column1"] = stringVal;
		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	{
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = 3;
		item["column1"] = stringVal;
		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	QueryResults result;
	err = rt.reindexer->Select("select column1, distinct(column1) from test_namespace;", result);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(result.Count(), 2);
	std::set<std::string> BaseVals = {"{\"column1\":" + std::to_string(int64_t(vInt)) + "}", "{\"column1\":\"abc\"}"};
	std::set<std::string> Vals;
	for (auto& r : result) {
		reindexer::WrSerializer ser;
		auto err = r.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		Vals.insert(ser.c_str());
	}
	ASSERT_TRUE(bool(BaseVals == Vals));
}

TEST_F(ReindexerApi, DistinctCompositeIndex) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"v1", "-", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"v2", "-", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indexDeclr;
	indexDeclr.name_ = "v1+v2";
	indexDeclr.indexType_ = "hash";
	indexDeclr.fieldType_ = "composite";
	indexDeclr.opts_ = IndexOpts();
	indexDeclr.jsonPaths_ = reindexer::JsonPaths({"v1", "v2"});
	err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
	EXPECT_TRUE(err.ok()) << err.what();

	{
		Item item = NewItem(default_namespace);
		item["id"] = 1;
		item["v1"] = 2;
		item["v2"] = 3;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		Item item = NewItem(default_namespace);
		item["id"] = 2;
		item["v1"] = 2;
		item["v2"] = 3;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	Query q;
	q._namespace = default_namespace;
	q.Distinct("v1+v2");
	{
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
	}

	{
		Item item = NewItem(default_namespace);
		item["id"] = 3;
		item["v1"] = 3;
		item["v2"] = 2;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 2);
	}
	{
		Item item = NewItem(default_namespace);
		item["id"] = 4;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	{
		Item item = NewItem(default_namespace);
		item["id"] = 5;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 3);
	}
	{
		Item item = NewItem(default_namespace);
		item["id"] = 6;
		item["v1"] = 3;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		Item item = NewItem(default_namespace);
		item["id"] = 7;
		item["v1"] = 3;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 4);
	}
	{
		Item item = NewItem(default_namespace);
		item["id"] = 8;
		item["v1"] = 4;
		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	{
		QueryResults qr;
		err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 5);
	}
}

TEST_F(ReindexerApi, CompositeIndexCreationError) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"x", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	constexpr char kExpectedErrMsgField[] =
		"Composite indexes over non-indexed field ('%s') are not supported yet (except for full-text indexes). Create at least column "
		"index('-') over each field inside the composite index";
	{
		// Attempt to create composite over 2 non-index fields
		reindexer::IndexDef indexDeclr{"v1+v2", reindexer::JsonPaths({"v1", "v2"}), "hash", "composite", IndexOpts()};
		err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), fmt::sprintf(kExpectedErrMsgField, "v1"));
	}
	{
		// Attempt to create composite over 1 index and 1 non-index fields
		reindexer::IndexDef indexDeclr{"id+v2", reindexer::JsonPaths({"id", "v2"}), "hash", "composite", IndexOpts()};
		err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), fmt::sprintf(kExpectedErrMsgField, "v2"));
	}
	{
		// Attempt to create composite over 1 index and 1 non-index fields
		reindexer::IndexDef indexDeclr{"v2+id", reindexer::JsonPaths({"v2", "id"}), "hash", "composite", IndexOpts()};
		err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), fmt::sprintf(kExpectedErrMsgField, "v2"));
	}
	{
		// Attempt to create sparse composite index
		reindexer::IndexDef indexDeclr{"id+x", reindexer::JsonPaths({"id", "x"}), "hash", "composite", IndexOpts().Sparse()};
		err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.what(), "Composite index cannot be sparse. Use non-sparse composite instead");
	}
}

TEST_F(ReindexerApi, AddIndex_CaseInsensitive) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string idxName = "IdEnTiFiCaToR";
	err = rt.reindexer->AddIndex(default_namespace, {idxName, "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok());

	// check adding index named in lower case
	idxName = "identificator";
	err = rt.reindexer->AddIndex(default_namespace, {idxName, "hash", "int64", IndexOpts().PK()});
	ASSERT_FALSE(err.ok()) << "Somehow index 'identificator' was added. But index 'IdEnTiFiCaToR' already exists";

	// check adding index named in upper case
	idxName = "IDENTIFICATOR";
	err = rt.reindexer->AddIndex(default_namespace, {idxName, "hash", "int64", IndexOpts().PK()});
	ASSERT_FALSE(err.ok()) << "Somehow index 'IDENTIFICATOR' was added. But index 'IdEnTiFiCaToR' already exists";

	// check case insensitive field access
	Item item = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	ASSERT_NO_THROW(item[idxName] = 1234);
}

TEST_F(ReindexerApi, AddExistingIndex) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, AddUnacceptablePKIndex) {
	const std::string kIdxName = "id";
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	// Try to add an array as a PK
	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "hash", "int", IndexOpts().PK().Array()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	// Try to add a store indexes of few types as a PKs
	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "-", "int", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "-", "bool", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "-", "int64", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "-", "double", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "-", "string", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "text", "string", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "fuzzytext", "string", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	// Add valid index with the same name
	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, UpdateToUnacceptablePKIndex) {
	const std::string kIdxName = "id";
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	// Try to update to an array as a PK
	err = rt.reindexer->UpdateIndex(default_namespace, {kIdxName, "tree", "int", IndexOpts().PK().Array()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	// Try to update to a store indexes of few types as a PKs
	const std::vector<std::string> kTypes = {"int", "bool", "int64", "double", "string"};
	for (auto& type : kTypes) {
		err = rt.reindexer->UpdateIndex(default_namespace, {kIdxName, "-", type, IndexOpts().PK()});
		ASSERT_EQ(err.code(), errParams) << err.what();
	}

	// Update to a valid index with the same name
	err = rt.reindexer->UpdateIndex(default_namespace, {kIdxName, "tree", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, IndexNameValidation) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();
	// Index names with cirillic characters are not allowed
	err = rt.reindexer->AddIndex(default_namespace, {"индекс", "hash", "int", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"idд", "hash", "int", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errParams) << err.what();
	// Index names with special characters are not allowed
	const std::string_view kForbiddenChars = "?#№/@!$%^*)+";
	for (auto c : kForbiddenChars) {
		auto idxName = std::string("id");
		idxName += c;
		err = rt.reindexer->AddIndex(default_namespace, {idxName, "hash", "int", IndexOpts().PK()});
		ASSERT_EQ(err.code(), errParams) << err.what() << "; IdxName: " << idxName;
	}
}

TEST_F(ReindexerApi, AddExistingIndexWithDiffType) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int64", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errConflict) << err.what();
}

TEST_F(ReindexerApi, CloseNamespace) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->CloseNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_FALSE(err.ok()) << "Namespace '" << default_namespace << "' open. But must be closed";
}

TEST_F(ReindexerApi, DropStorage) {
	auto rx = std::make_unique<Reindexer>();
	auto err = rx->Connect("builtin://" + kBaseTestsStoragePath);
	ASSERT_TRUE(err.ok()) << err.what();
	auto storagePath = reindexer::fs::JoinPath(kBaseTestsStoragePath, default_namespace);
	err = rx->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(reindexer::fs::Stat(storagePath) == reindexer::fs::StatDir);

	err = rx->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	const auto item = getMemStat(*rx, default_namespace);
	ASSERT_EQ(item["storage_ok"].As<bool>(), true);
	ASSERT_EQ(item["storage_enabled"].As<bool>(), true);
	ASSERT_EQ(item["storage_status"].As<std::string>(), "OK");

	err = rx->DropNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(reindexer::fs::Stat(storagePath) == reindexer::fs::StatError);
}

TEST_F(ReindexerApi, DeleteNonExistingNamespace) {
	auto err = rt.reindexer->CloseNamespace(default_namespace);
	ASSERT_FALSE(err.ok()) << "Error: unexpected result of delete non-existing namespace.";
}

TEST_F(ReindexerApi, NewItem) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled());

	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
	Item item(rt.reindexer->NewItem(default_namespace));
	ASSERT_TRUE(!!item);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
}

TEST_F(ReindexerApi, GetItemFromQueryResults) {
	constexpr size_t kItemsCount = 10;
	initializeDefaultNs();
	std::vector<std::pair<int, std::string>> data;
	while (data.size() < kItemsCount) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		auto err = rt.reindexer->Insert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	reindexer::QueryResults qr;
	auto err = rt.reindexer->Select(Query(default_namespace).Sort("id", false), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), kItemsCount);
	// items in QueryResults are valid after the ns is destroyed
	err = rt.reindexer->TruncateNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->DropNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	size_t i = 0;
	for (auto it = qr.begin(), end = qr.end(); it != end; ++it, ++i) {
		ASSERT_LT(i, data.size());
		auto item = it.GetItem();
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		EXPECT_EQ(item["id"].As<int>(), data[i].first);
		EXPECT_EQ(item["value"].As<std::string>(), data[i].second);
	}

	qr.Clear();
	data.clear();
	initializeDefaultNs();
	{
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		err = rt.reindexer->Insert(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		err = rt.reindexer->Upsert(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.back().first, RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		err = rt.reindexer->Upsert(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		err = rt.reindexer->Insert(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.back().first, RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		err = rt.reindexer->Update(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.back());
		item["id"] = data.back().first;
		item["value"] = RandString();
		err = rt.reindexer->Delete(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		item["id"] = static_cast<int>(data.size());
		item["value"] = RandString();
		err = rt.reindexer->Update(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.reindexer->NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		item["id"] = static_cast<int>(data.size());
		item["value"] = RandString();
		err = rt.reindexer->Delete(default_namespace, item, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), data.size());
	}
	err = rt.reindexer->TruncateNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->DropNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	ASSERT_EQ(qr.Count(), 6);
	ASSERT_EQ(qr.Count(), data.size());
	i = 0;
	for (auto it = qr.begin(), end = qr.end(); it != end; ++it, ++i) {
		ASSERT_LT(i, data.size());
		auto item = it.GetItem();
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		EXPECT_EQ(item["id"].As<int>(), data[i].first);
		EXPECT_EQ(item["value"].As<std::string>(), data[i].second);
	}
}

TEST_F(ReindexerApi, NewItem_CaseInsensitiveCheck) {
	int idVal = 1000;
	std::string valueVal = "value";

	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled());

	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	auto item = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	ASSERT_NO_THROW(item["ID"] = 1000);
	ASSERT_NO_THROW(item["VaLuE"] = "value");
	ASSERT_NO_THROW(ASSERT_EQ(item["id"].As<int>(), idVal));
	ASSERT_NO_THROW(ASSERT_EQ(item["value"].As<std::string>(), valueVal));
}

TEST_F(ReindexerApi, Insert) {
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	Item item(rt.reindexer->NewItem(default_namespace));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(R"_({"id":1234, "value" : "value"})_");
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Insert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	// check item consist and check case insensitive access to field by name
	Item selItem = qr.begin().GetItem(false);
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));
}

TEST_F(ReindexerApi, WithTimeoutInterface) {
	using std::chrono::milliseconds;

	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	Item item(rt.reindexer->NewItem(default_namespace));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(R"_({"id":1234, "value" : "value"})_");
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->WithTimeout(milliseconds(1000)).Insert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->WithTimeout(milliseconds(100)).Commit(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	err = rt.reindexer->WithTimeout(milliseconds(1000)).Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	// check item consist and check case insensitive access to field by name
	Item selItem = qr.begin().GetItem(false);
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));

	qr.Clear();
	err = rt.reindexer->WithTimeout(milliseconds(1000)).Delete(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <CollateMode collateMode>
struct CollateComparer {
	bool operator()(const std::string& lhs, const std::string& rhs) const {
		return reindexer::collateCompare<collateMode>(lhs, rhs, reindexer::SortingPrioritiesTable()) < 0;
	}
};

TEST_F(ReindexerApi, SortByMultipleColumns) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"column1", "tree", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"column2", "tree", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"column3", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	const std::vector<std::string> possibleValues = {
		"apple",	 "arrangment", "agreement", "banana",	"bull",	 "beech", "crocodile", "crucifix", "coat",	   "day",
		"dog",		 "deer",	   "easter",	"ear",		"eager", "fair",  "fool",	   "foot",	   "genes",	   "genres",
		"greatness", "hockey",	   "homeless",	"homocide", "key",	 "kit",	  "knockdown", "motion",   "monument", "movement"};

	int sameOldValue = 0;
	int stringValuedIdx = 0;
	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;
		item["column1"] = sameOldValue;
		item["column2"] = possibleValues[stringValuedIdx];
		item["column3"] = rand() % 30;

		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();

		if (i % 5 == 0) sameOldValue += 5;
		if (i % 3 == 0) ++stringValuedIdx;
		stringValuedIdx %= possibleValues.size();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	const size_t offset = 23;
	const size_t limit = 61;

	QueryResults qr;
	Query query{Query(default_namespace, offset, limit).Sort("column1", true).Sort("column2", false).Sort("column3", false)};
	err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() == limit) << qr.Count();

	PrintQueryResults(default_namespace, qr);

	std::vector<Variant> lastValues(query.sortingEntries_.size());
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem(false);

		std::vector<int> cmpRes(query.sortingEntries_.size());
		std::fill(cmpRes.begin(), cmpRes.end(), -1);

		for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
			const reindexer::SortingEntry& sortingEntry(query.sortingEntries_[j]);
			Variant sortedValue = item[sortingEntry.expression];
			if (!lastValues[j].Type().Is<reindexer::KeyValueType::Null>()) {
				cmpRes[j] = lastValues[j].Compare(sortedValue);
				bool needToVerify = true;
				if (j != 0) {
					for (int k = j - 1; k >= 0; --k)
						if (cmpRes[k] != 0) {
							needToVerify = false;
							break;
						}
				}
				needToVerify = (j == 0) || needToVerify;
				if (needToVerify) {
					bool sortOrderSatisfied =
						(sortingEntry.desc && cmpRes[j] >= 0) || (!sortingEntry.desc && cmpRes[j] <= 0) || (cmpRes[j] == 0);
					EXPECT_TRUE(sortOrderSatisfied)
						<< "\nSort order is incorrect for column: " << sortingEntry.expression << "; rowID: " << item[1].As<int>();
				}
			}
		}
	}

	// Check sql parser work correctness
	QueryResults qrSql;
	std::string sqlQuery = ("select * from test_namespace order by column2 asc, column3 desc");
	err = rt.reindexer->Select(sqlQuery, qrSql);
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, SortByMultipleColumnsWithLimits) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"f1", "tree", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"f2", "tree", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	const std::vector<std::string> srcStrValues = {
		"A", "A", "B", "B", "B", "C", "C",
	};
	const std::vector<int> srcIntValues = {1, 2, 4, 3, 5, 7, 6};

	for (size_t i = 0; i < srcIntValues.size(); ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = static_cast<int>(i);
		item["f1"] = srcStrValues[i];
		item["f2"] = srcIntValues[i];

		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	const size_t offset = 4;
	const size_t limit = 3;

	QueryResults qr;
	Query query{Query(default_namespace, offset, limit).Sort("f1", false).Sort("f2", false)};
	err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() == limit) << qr.Count();

	const std::vector<int> properRes = {5, 6, 7};
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem(false);
		Variant kr = item["f2"];
		EXPECT_TRUE(static_cast<int>(kr) == properRes[i]);
	}
}

TEST_F(ReindexerApi, SortByUnorderedIndexes) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueInt", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueString", "hash", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueStringASCII", "hash", "string", IndexOpts().SetCollateMode(CollateASCII)});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueStringNumeric", "hash", "string", IndexOpts().SetCollateMode(CollateNumeric)});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueStringUTF8", "hash", "string", IndexOpts().SetCollateMode(CollateUTF8)});
	ASSERT_TRUE(err.ok()) << err.what();

	std::deque<int> allIntValues;
	std::set<std::string> allStrValues;
	std::set<std::string, CollateComparer<CollateASCII>> allStrValuesASCII;
	std::set<std::string, CollateComparer<CollateNumeric>> allStrValuesNumeric;
	std::set<std::string, CollateComparer<CollateUTF8>> allStrValuesUTF8;

	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;
		item["valueInt"] = i;
		allIntValues.push_front(i);

		std::string strCollateNone = RandString().c_str();
		allStrValues.insert(strCollateNone);
		item["valueString"] = strCollateNone;

		std::string strASCII(strCollateNone + "ASCII");
		allStrValuesASCII.insert(strASCII);
		item["valueStringASCII"] = strASCII;

		std::string strNumeric(std::to_string(i + 1));
		allStrValuesNumeric.insert(strNumeric);
		item["valueStringNumeric"] = strNumeric;

		allStrValuesUTF8.insert(strCollateNone);
		item["valueStringUTF8"] = strCollateNone;

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	bool descending = true;
	const unsigned offset = 5;
	const unsigned limit = 30;

	QueryResults sortByIntQr;
	Query sortByIntQuery{Query(default_namespace, offset, limit).Sort("valueInt", descending)};
	err = rt.reindexer->Select(sortByIntQuery, sortByIntQr);
	EXPECT_TRUE(err.ok()) << err.what();

	std::deque<int> selectedIntValues;
	for (auto it : sortByIntQr) {
		Item item(it.GetItem(false));
		int value = item["valueInt"].Get<int>();
		selectedIntValues.push_back(value);
	}

	EXPECT_TRUE(std::equal(allIntValues.begin() + offset, allIntValues.begin() + offset + limit, selectedIntValues.begin()));

	QueryResults sortByStrQr, sortByASCIIStrQr, sortByNumericStrQr, sortByUTF8StrQr;
	Query sortByStrQuery{Query(default_namespace).Sort("valueString", !descending)};				// -V547
	Query sortByASSCIIStrQuery{Query(default_namespace).Sort("valueStringASCII", !descending)};		// -V547
	Query sortByNumericStrQuery{Query(default_namespace).Sort("valueStringNumeric", !descending)};	// -V547
	Query sortByUTF8StrQuery{Query(default_namespace).Sort("valueStringUTF8", !descending)};		// -V547

	err = rt.reindexer->Select(sortByStrQuery, sortByStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Select(sortByASSCIIStrQuery, sortByASCIIStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Select(sortByNumericStrQuery, sortByNumericStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Select(sortByUTF8StrQuery, sortByUTF8StrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	auto collectQrStringFieldValues = [](const QueryResults& qr, const char* fieldName, std::vector<std::string>& selectedStrValues) {
		selectedStrValues.clear();
		for (auto it : qr) {
			Item item(it.GetItem(false));
			selectedStrValues.push_back(item[fieldName].As<std::string>());
		}
	};

	std::vector<std::string> selectedStrValues;
	{
		auto itSortedStr(allStrValues.begin());
		collectQrStringFieldValues(sortByStrQr, "valueString", selectedStrValues);
		for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
			EXPECT_EQ(*it, *itSortedStr++);
		}
	}

	{
		auto itSortedStr = allStrValuesASCII.begin();
		collectQrStringFieldValues(sortByASCIIStrQr, "valueStringASCII", selectedStrValues);
		for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
			EXPECT_EQ(*it, *itSortedStr++);
		}
	}

	auto itSortedNumericStr = allStrValuesNumeric.cbegin();
	collectQrStringFieldValues(sortByNumericStrQr, "valueStringNumeric", selectedStrValues);
	for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
		EXPECT_EQ(*it, *itSortedNumericStr++);
	}

	{
		auto itSortedStr = allStrValuesUTF8.cbegin();
		collectQrStringFieldValues(sortByUTF8StrQr, "valueStringUTF8", selectedStrValues);
		for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
			EXPECT_EQ(*it, *itSortedStr++);
		}
	}
}

TEST_F(ReindexerApi, SortByUnorderedIndexWithJoins) {
	const std::string secondNamespace = "test_namespace_2";
	std::vector<int> secondNamespacePKs;

	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"fk", "hash", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	{
		err = rt.reindexer->OpenNamespace(secondNamespace, StorageOpts().Enabled(false));
		ASSERT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->AddIndex(secondNamespace, {"pk", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		for (int i = 0; i < 50; ++i) {
			Item item(rt.reindexer->NewItem(secondNamespace));
			ASSERT_TRUE(!!item);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			secondNamespacePKs.push_back(i);
			item["pk"] = i;

			err = rt.reindexer->Upsert(secondNamespace, item);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		err = rt.reindexer->Commit(secondNamespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;

		int fk = secondNamespacePKs[rand() % (secondNamespacePKs.size() - 1)];
		item["fk"] = fk;

		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	bool descending = true;
	const unsigned offset = 10;
	const unsigned limit = 40;

	Query querySecondNamespace = Query(secondNamespace);
	Query joinQuery{Query(default_namespace, offset, limit).Sort("id", descending)};
	joinQuery.InnerJoin("fk", "pk", CondEq, std::move(querySecondNamespace));

	QueryResults queryResult;
	err = rt.reindexer->Select(joinQuery, queryResult);
	EXPECT_TRUE(err.ok()) << err.what();

	for (auto it : queryResult) {
		auto itemIt = it.GetJoined();
		EXPECT_TRUE(itemIt.getJoinedItemsCount() > 0);
	}
}

static void TestDSLParseCorrectness(const std::string& testDsl) {
	Query query;
	Error err = query.FromJSON(testDsl);
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, DslFieldsTest) {
	TestDSLParseCorrectness(R"xxx({
		"namespace": "test_ns"
		"filters": [
			{
				"op": "AND",
				"join_query": {
					"type": "inner",
					"namespace": "test1",
					"filters": [
						{
							"Op": "OR",
							"Field": "id",
							"Cond": "EMPTY"
						}
					],
					"sort": {
						"field": "test1",
						"desc": true
					},
					"limit": 3,
					"offset": 0,
					"on": [
						{
							"left_field": "joined",
							"right_field": "joined",
							"cond": "lt",
							"op": "OR"
						},
						{
							"left_field": "joined2",
							"right_field": "joined2",
							"cond": "gt",
							"op": "AND"
						}
					]
				}
			},
		   {
			"op": "OR",
			"join_query": {
				"type": "left",
				"namespace": "test2",
				"filters": [
					{
						"filters": [
							{
								"Op": "And",
								"Filters": [
									{
										"Op": "Not",
										"Field": "id2",
										"Cond": "SET",
										"Value": [
											81204872,
											101326571,
											101326882
										]
									},
									{
										"Op": "Or",
										"Field": "id2",
										"Cond": "SET",
										"Value": [
											81204872,
											101326571,
											101326882
										]
									},
									{
										"Op": "And",
										"filters": [
											{
												"Op": "Not",
												"Field": "id2",
												"Cond": "SET",
												"Value": [
													81204872,
													101326571,
													101326882
												]
											},
											{
												"Op": "Or",
												"Field": "id2",
												"Cond": "SET",
												"Value": [
													81204872,
													101326571,
													101326882
												]
											}
										]
									}
								]
							},
							{
								"Op": "Not",
								"Field": "id2",
								"Cond": "SET",
								"Value": [
									81204872,
									101326571,
									101326882
								]
							}
						]
					}
				],
				"sort": {
					"field": "test2",
					"desc": true
				},
				"limit": 4,
				"offset": 5,
				"on": [
					{
						"left_field": "joined1",
						"right_field": "joined1",
						"cond": "le",
						"op": "AND"
					},
					{
						"left_field": "joined2",
						"right_field": "joined2",
						"cond": "ge",
						"op": "OR"
					}
				]
				}
			}

		]
	})xxx");

	TestDSLParseCorrectness(R"xxx({
										"namespace": "test_ns",
										"merge_queries": [{
											"namespace": "services",
											"offset": 0,
											"limit": 3,
											"sort": {
												"field": "",
												"desc": true
											},
											"filters": [{
												"Op": "or",
												"Field": "id",
												"Cond": "SET",
												"Value": [81204872, 101326571, 101326882]
											}]
										},
										{
											"namespace": "services",
											"offset": 1,
											"limit": 5,
											"sort": {
												"field": "field1",
												"desc": false
											},
											"filters": [{
												"Op": "not",
												"Field": "id",
												"Cond": "ge",
												"Value": 81204872
											}]
										}
									]
								})xxx");

	TestDSLParseCorrectness(R"xxx({"namespace": "test1","select_filter": ["f1", "f2", "f3", "f4", "f5"]})xxx");
	TestDSLParseCorrectness(R"xxx({"namespace": "test1","select_functions": ["f1()", "f2()", "f3()", "f4()", "f5()"]})xxx");
	TestDSLParseCorrectness(R"xxx({"namespace": "test1","req_total":"cached"})xxx");
	TestDSLParseCorrectness(R"xxx({"namespace": "test1","req_total":"enabled"})xxx");
	TestDSLParseCorrectness(R"xxx({"namespace": "test1","req_total":"disabled"})xxx");
	TestDSLParseCorrectness(
		R"xxx({"namespace": "test1","aggregations":[{"fields":["field1"], "type":"sum"}, {"fields":["field2"], "type":"avg"}]})xxx");
	TestDSLParseCorrectness(R"xxx({"namespace": "test1", "strict_mode":"indexes"})xxx");
}

TEST_F(ReindexerApi, DistinctQueriesEncodingTest) {
	const std::string sql = "select distinct(country), distinct(city) from clients;";

	Query q1;
	q1.FromSQL(sql);
	EXPECT_EQ(q1.entries.Size(), 0);
	ASSERT_EQ(q1.aggregations_.size(), 2);
	EXPECT_EQ(q1.aggregations_[0].Type(), AggDistinct);
	ASSERT_EQ(q1.aggregations_[0].Fields().size(), 1);
	EXPECT_EQ(q1.aggregations_[0].Fields()[0], "country");
	EXPECT_EQ(q1.aggregations_[1].Type(), AggDistinct);
	ASSERT_EQ(q1.aggregations_[1].Fields().size(), 1);
	EXPECT_EQ(q1.aggregations_[1].Fields()[0], "city");

	std::string dsl = q1.GetJSON();
	Query q2;
	q2.FromJSON(dsl);
	EXPECT_TRUE(q1 == q2);

	Query q3{Query(default_namespace).Distinct("name").Distinct("city").Where("id", CondGt, static_cast<int64_t>(10))};
	std::string sql2 = q3.GetSQL();

	Query q4;
	q4.FromSQL(sql2);
	EXPECT_TRUE(q3 == q4);
	EXPECT_TRUE(sql2 == q4.GetSQL());
}

TEST_F(ReindexerApi, ContextCancelingTest) {
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	Item item(rt.reindexer->NewItem(default_namespace));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();

	err = item.FromJSON(R"_({"id":1234, "value" : "value"})_");
	ASSERT_TRUE(err.ok()) << err.what();

	// Canceled insert
	CanceledRdxContext canceledCtx;
	err = rt.reindexer->WithContext(&canceledCtx).Insert(default_namespace, item);
	ASSERT_TRUE(err.code() == errCanceled);

	err = rt.reindexer->Commit(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	// Canceled delete
	std::vector<reindexer::NamespaceDef> namespaces;
	err = rt.reindexer->WithContext(&canceledCtx).EnumNamespaces(namespaces, reindexer::EnumNamespacesOpts());
	ASSERT_TRUE(err.code() == errCanceled);

	// Canceled select
	QueryResults qr;
	err = rt.reindexer->WithContext(&canceledCtx).Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.code() == errCanceled);
	std::string sqlQuery = ("select * from test_namespace");
	err = rt.reindexer->WithContext(&canceledCtx).Select(sqlQuery, qr);
	ASSERT_TRUE(err.code() == errCanceled);

	DummyRdxContext dummyCtx;
	err = rt.reindexer->WithContext(&dummyCtx).Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 0);
	qr.Clear();

	FakeRdxContext fakeCtx;
	err = rt.reindexer->WithContext(&fakeCtx).Insert(default_namespace, item);
	EXPECT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->WithContext(&fakeCtx).Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	qr.Clear();

	// Canceled upsert
	item["value"] = "value1";
	err = rt.reindexer->WithContext(&canceledCtx).Upsert(default_namespace, item);
	ASSERT_TRUE(err.code() == errCanceled);
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	Item selItem = qr.begin().GetItem(false);
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));
	qr.Clear();

	// Canceled update
	err = rt.reindexer->WithContext(&canceledCtx).Update(default_namespace, item);
	ASSERT_TRUE(err.code() == errCanceled);
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	selItem = qr.begin().GetItem(false);
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));
	qr.Clear();

	// Canceled delete
	err = rt.reindexer->WithContext(&canceledCtx).Delete(default_namespace, item);
	ASSERT_TRUE(err.code() == errCanceled);
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	qr.Clear();

	err = rt.reindexer->WithContext(&canceledCtx).Delete(Query(default_namespace), qr);
	ASSERT_TRUE(err.code() == errCanceled);
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	qr.Clear();

	err = rt.reindexer->WithContext(&fakeCtx).Delete(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 0);
}

TEST_F(ReindexerApi, JoinConditionsSqlParserTest) {
	Query q1, q2;
	const std::string sql1 = "SELECT * FROM ns WHERE a > 0 AND  INNER JOIN (SELECT * FROM ns2 WHERE b > 10 AND c = 1) ON ns2.id = ns.fk_id";
	q1.FromSQL(sql1);
	ASSERT_EQ(q1.GetSQL(), sql1);

	const std::string sql2 =
		"SELECT * FROM ns WHERE a > 0 AND  INNER JOIN (SELECT * FROM ns2 WHERE b > 10 AND c = 1 LIMIT 0) ON ns2.id = ns.fk_id";
	q2.FromSQL(sql2);
	ASSERT_EQ(q2.GetSQL(), sql2);
}

TEST_F(ReindexerApi, UpdateWithBoolParserTest) {
	Query query;
	const std::string sql = "UPDATE ns SET flag1 = true,flag2 = false WHERE id > 100";
	query.FromSQL(sql);
	ASSERT_EQ(query.UpdateFields().size(), 2);
	EXPECT_EQ(query.UpdateFields().front().Column(), "flag1");
	EXPECT_EQ(query.UpdateFields().front().Mode(), FieldModeSet);
	ASSERT_EQ(query.UpdateFields().front().Values().size(), 1);
	EXPECT_TRUE(query.UpdateFields().front().Values().front().Type().Is<reindexer::KeyValueType::Bool>());
	EXPECT_TRUE(query.UpdateFields().front().Values().front().As<bool>());
	EXPECT_EQ(query.UpdateFields().back().Column(), "flag2");
	EXPECT_EQ(query.UpdateFields().back().Mode(), FieldModeSet);
	ASSERT_EQ(query.UpdateFields().back().Values().size(), 1);
	EXPECT_TRUE(query.UpdateFields().back().Values().front().Type().Is<reindexer::KeyValueType::Bool>());
	EXPECT_FALSE(query.UpdateFields().back().Values().front().As<bool>());
	EXPECT_EQ(query.GetSQL(), sql) << query.GetSQL();
}

TEST_F(ReindexerApi, EqualPositionsSqlParserTest) {
	const std::string sql =
		"SELECT * FROM ns WHERE (f1 = 1 AND f2 = 2 OR f3 = 3 equal_position(f1, f2) equal_position(f1, f3)) OR (f4 = 4 AND f5 > 5 "
		"equal_position(f4, f5))";

	Query query;
	query.FromSQL(sql);
	EXPECT_EQ(query.GetSQL(), sql);
	EXPECT_TRUE(query.entries.equalPositions.empty());
	ASSERT_EQ(query.entries.Size(), 7);

	ASSERT_TRUE(query.entries.IsSubTree(0));
	const auto& ep1 = query.entries.Get<reindexer::QueryEntriesBracket>(0).equalPositions;
	ASSERT_EQ(ep1.size(), 2);
	ASSERT_EQ(ep1[0].size(), 2);
	EXPECT_EQ(ep1[0][0], "f1");
	EXPECT_EQ(ep1[0][1], "f2");
	ASSERT_EQ(ep1[1].size(), 2);
	EXPECT_EQ(ep1[1][0], "f1");
	EXPECT_EQ(ep1[1][1], "f3");

	ASSERT_TRUE(query.entries.IsSubTree(4));
	const auto& ep2 = query.entries.Get<reindexer::QueryEntriesBracket>(4).equalPositions;
	ASSERT_EQ(ep2.size(), 1);
	ASSERT_EQ(ep2[0].size(), 2);
	EXPECT_EQ(ep2[0][0], "f4");
	EXPECT_EQ(ep2[0][1], "f5");
}

TEST_F(ReindexerApi, SchemaSuggestions) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	// clang-format off
	const std::string jsonschema = R"xxx(
	{
	  "required": [
		"Countries",
		"Nest_fake",
		"nested"
	  ],
	  "properties": {
		"Countries": {
		  "items": {
			"type": "string"
		  },
		  "type": "array"
		},
		"Nest_fake": {
		  "type": "number"
		},
		"nested": {
		  "required": [
			"Name",
			"Naame",
			"Age"
		  ],
		  "properties": {
			"Name": {
			  "type": "string"
			},
			"Naame": {
			  "type": "string"
			},
			"Age": {
			  "type": "integer"
			}
		  },
		  "additionalProperties": false,
		  "type": "object"
		}
	  },
	  "additionalProperties": false,
	  "type": "object"
	})xxx";
	// clang-format on

	err = rt.reindexer->SetSchema(default_namespace, jsonschema);
	ASSERT_TRUE(err.ok()) << err.what();

	auto validateSuggestions = [](const std::vector<std::string>& actual, const std::unordered_set<std::string>& expected) {
		ASSERT_EQ(actual.size(), expected.size());
		for (auto& sugg : actual) {
			EXPECT_TRUE(expected.find(sugg) != expected.end()) << "Unexpected suggestion: " << sugg;
		}
	};

	{
		std::unordered_set<std::string> expected = {"Nest_fake", "nested"};
		std::vector<std::string> suggestions;
		std::string_view query = "select * from test_namespace where ne";
		err = rt.reindexer->GetSqlSuggestions(query, query.size() - 1, suggestions);
		ASSERT_TRUE(err.ok()) << err.what();
		validateSuggestions(suggestions, expected);
	}

	{
		std::unordered_set<std::string> expected;
		std::vector<std::string> suggestions;
		std::string_view query = "select * from test_namespace where nested";
		err = rt.reindexer->GetSqlSuggestions(query, query.size() - 1, suggestions);
		ASSERT_TRUE(err.ok()) << err.what();
		validateSuggestions(suggestions, expected);
	}

	{
		std::unordered_set<std::string> expected = {".Name", ".Naame", ".Age"};
		std::vector<std::string> suggestions;
		std::string_view query = "select * from test_namespace where nested.";
		err = rt.reindexer->GetSqlSuggestions(query, query.size() - 1, suggestions);
		ASSERT_TRUE(err.ok()) << err.what();
		validateSuggestions(suggestions, expected);
	}

	{
		std::unordered_set<std::string> expected = {".Name", ".Naame"};
		std::vector<std::string> suggestions;
		std::string_view query = "select * from test_namespace where nested.Na";
		err = rt.reindexer->GetSqlSuggestions(query, query.size() - 1, suggestions);
		ASSERT_TRUE(err.ok()) << err.what();
		validateSuggestions(suggestions, expected);
	}
}

TEST_F(ReindexerApi, LoggerWriteInterruptTest) {
	struct Logger {
		Logger() {
			spdlog::drop_all();
			spdlog::set_async_mode(16384, spdlog::async_overflow_policy::discard_log_msg, nullptr, std::chrono::seconds(2));
			spdlog::set_level(spdlog::level::trace);
			spdlog::set_pattern("[%L%d/%m %T.%e %t] %v");

			std::remove(logFile.c_str());
			sinkPtr = std::make_shared<spdlog::sinks::fast_file_sink>(logFile);
			spdlog::create("log", sinkPtr);
			logger = reindexer_server::LoggerWrapper("log");
		}
		~Logger() {
			spdlog::drop_all();
			std::remove(logFile.c_str());
		}
		const std::string logFile = "/tmp/logtest.out";
		reindexer_server::LoggerWrapper logger;
		std::shared_ptr<spdlog::sinks::fast_file_sink> sinkPtr;
	} instance;

	reindexer::logInstallWriter([&](int level, char* buf) {
		if (level <= LogTrace) {
			instance.logger.trace(buf);
		}
	});
	auto writeThread = std::thread([]() {
		for (size_t i = 0; i < 10000; ++i) {
			reindexer::logPrintf(LogTrace, "Detailed and amazing description of this error: [%d]!", i);
		}
	});
	auto reopenThread = std::thread([&instance]() {
		for (size_t i = 0; i < 1000; ++i) {
			instance.sinkPtr->reopen();
			reindexer::logPrintf(LogTrace, "REOPENED [%d]", i);
			std::this_thread::sleep_for(std::chrono::milliseconds(3));
		}
	});
	writeThread.join();
	reopenThread.join();
	reindexer::logPrintf(LogTrace, "FINISHED\n");
	reindexer::logInstallWriter(nullptr);
}

TEST_F(ReindexerApi, IntToStringIndexUpdate) {
	const std::string kFieldId = "id";
	const std::string kFieldNumeric = "numeric";

	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kFieldId, "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {kFieldNumeric, "tree", "int", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item[kFieldId] = i;
		item[kFieldNumeric] = i * 2;

		err = rt.reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->UpdateIndex(default_namespace, {kFieldNumeric, "tree", "string", IndexOpts()});
	EXPECT_FALSE(err.ok());
	EXPECT_TRUE(err.what() == "Cannot convert key from type int to string") << err.what();

	QueryResults qr;
	err = rt.reindexer->Select(Query(default_namespace), qr);
	EXPECT_TRUE(err.ok()) << err.what();

	for (auto it : qr) {
		Item item = it.GetItem(false);
		Variant v = item[kFieldNumeric];
		EXPECT_TRUE(v.Type().Is<reindexer::KeyValueType::Int>()) << v.Type().Name();
	}
}

TEST_F(ReindexerApi, SelectFilterWithAggregationConstraints) {
	Query q;

	std::string sql = "select id, distinct(year) from test_namespace";
	EXPECT_NO_THROW(q.FromSQL(sql));
	Error status = Query().FromJSON(q.GetJSON());
	EXPECT_TRUE(status.ok()) << status.what();

	q = Query();
	q.selectFilter_.emplace_back("id");
	EXPECT_NO_THROW(q.Aggregate(AggDistinct, {"year"}, {}));

	sql = "select id, max(year) from test_namespace";
	EXPECT_THROW(q.FromSQL(sql), Error);
	q = Query(default_namespace);
	q.selectFilter_.emplace_back("id");
	q.aggregations_.emplace_back(reindexer::AggregateEntry{AggMax, {"year"}});
	status = Query().FromJSON(q.GetJSON());
	EXPECT_FALSE(status.ok());
	EXPECT_TRUE(status.what() == std::string(reindexer::kAggregationWithSelectFieldsMsgError));
	EXPECT_THROW(q.Aggregate(AggMax, {"price"}, {}), Error);

	sql = "select facet(year), id, name from test_namespace";
	EXPECT_THROW(q.FromSQL(sql), Error);
	q = Query(default_namespace);
	q.selectFilter_.emplace_back("id");
	q.selectFilter_.emplace_back("name");
	EXPECT_THROW(q.Aggregate(AggFacet, {"year"}, {}), Error);
	q = Query(default_namespace);
	EXPECT_NO_THROW(q.Aggregate(AggFacet, {"year"}, {}));
	q.selectFilter_.emplace_back("id");
	q.selectFilter_.emplace_back("name");
	status = Query().FromJSON(q.GetJSON());
	EXPECT_FALSE(status.ok());
	EXPECT_TRUE(status.what() == std::string(reindexer::kAggregationWithSelectFieldsMsgError));

	EXPECT_THROW(Query().FromSQL("select max(id), * from test_namespace"), Error);
	EXPECT_THROW(Query().FromSQL("select *, max(id) from test_namespace"), Error);
	EXPECT_NO_THROW(Query().FromSQL("select *, count(*) from test_namespace"));
	EXPECT_NO_THROW(Query().FromSQL("select count(*), * from test_namespace"));
}
