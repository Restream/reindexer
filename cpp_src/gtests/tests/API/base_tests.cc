#include <vector>
#include "gtest/gtest.h"
#include "reindexer_api.h"

#include "cluster/config.h"
#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/defnsconfigs.h"
#include "core/keyvalue/variant.h"
#include "core/queryresults/joinresults.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

#include <deque>
#include <thread>

#include "core/keyvalue/p_string.h"
#include "core/system_ns_names.h"
#include "server/loggerwrapper.h"
#include "spdlog/async.h"
#include "spdlog/sinks/reopen_file_sink.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

TEST(ReindexerTest, DeleteTemporaryNamespaceOnConnect) {
	const auto kStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/base_tests/DeleteTemporaryNamespaceOnConnect");
	const std::string kBuiltin = "builtin://" + kStoragePath;

	std::string temporaryNamespacePath;
	{
		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();

		// Create temporary namespace
		std::string temporaryNamespaceOnFSName;
		err = rt.CreateTemporaryNamespace("tmp_ns", temporaryNamespaceOnFSName, StorageOpts().Enabled());
		ASSERT_TRUE(err.ok()) << err.what();

		// Check temporary namespace on filesystem
		temporaryNamespacePath = reindexer::fs::JoinPath(kStoragePath, temporaryNamespaceOnFSName);
		ASSERT_TRUE(reindexer::fs::Stat(temporaryNamespacePath) == reindexer::fs::StatDir);
	}

	// On second connect we already have tmp namespace, and Connect should delete it.
	{
		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(reindexer::fs::Stat(temporaryNamespacePath) == reindexer::fs::StatError);
	}
}

TEST_F(ReindexerApi, CheckTagsMatcherVersionInQR) {
	rt.OpenNamespace(default_namespace);

	const auto memstatQR = rt.Select(Query(reindexer::kMemStatsNamespace).Where("name", CondEq, default_namespace));
	ASSERT_EQ(memstatQR.Count(), 1);
	auto memstatItem = memstatQR.begin().GetItem(false);
	gason::JsonParser parser;
	auto jsonMemStat = parser.Parse(memstatItem.GetJSON());

	const auto qr = rt.Select(reindexer::Query{default_namespace}.Limit(0));
	const auto& tm = qr.GetTagsMatcher(0);

	EXPECT_EQ(tm.size(), jsonMemStat["tags_matcher"]["tags_count"].As<unsigned>());
	EXPECT_EQ(tm.version(), jsonMemStat["tags_matcher"]["version"].As<int>());
	EXPECT_EQ(tm.stateToken(), jsonMemStat["tags_matcher"]["state_token"].As<uint32_t>());
}

TEST_F(ReindexerApi, AddNamespace) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));

	const auto item = getMemStat(*rt.reindexer, default_namespace);
	ASSERT_EQ(item["storage_ok"].As<bool>(), false);
	ASSERT_EQ(item["storage_enabled"].As<bool>(), false);
	ASSERT_EQ(item["storage_status"].As<std::string>(), "DISABLED");
}

TEST_F(ReindexerApi, AddNamespace_CaseInsensitive) {
	rt.OpenNamespace(default_namespace);

	std::string upperNS(default_namespace);
	std::transform(default_namespace.begin(), default_namespace.end(), upperNS.begin(), [](int c) { return std::toupper(c); });

	auto err = rt.reindexer->AddNamespace(reindexer::NamespaceDef(upperNS));
	ASSERT_FALSE(err.ok()) << "Somehow namespace '" << upperNS << "' was added. But namespace '" << default_namespace << "' already exists";
}

TEST_F(ReindexerApi, AddExistingNamespace) {
	rt.OpenNamespace(default_namespace);

	auto err = rt.reindexer->AddNamespace(reindexer::NamespaceDef(default_namespace, StorageOpts().Enabled(false)));
	ASSERT_FALSE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, RenameNamespace) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	for (int i = 0; i < 10; ++i) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = i;
		item["column1"] = i + 100;
		rt.Upsert(default_namespace, item);
	}

	const std::string renameNamespace("rename_namespace");
	const std::string existingNamespace("existing_namespace");

	rt.OpenNamespace(existingNamespace);

	auto testInList = [&](std::string_view testNamespaceName, bool inList) {
		auto namespacesList = rt.EnumNamespaces(reindexer::EnumNamespacesOpts());
		auto r = std::find_if(namespacesList.begin(), namespacesList.end(),
							  [testNamespaceName](const reindexer::NamespaceDef& d) { return d.name == testNamespaceName; });
		if (inList) {
			ASSERT_FALSE(r == namespacesList.end()) << testNamespaceName << " not exist";
		} else {
			ASSERT_TRUE(r == namespacesList.end()) << testNamespaceName << " exist";
		}
	};

	auto getRowsInJSON = [&](std::string_view namespaceName, std::vector<std::string>& resStrings, reindexer::TagsMatcher& tm) {
		auto result = rt.Select(Query(namespaceName));
		resStrings.clear();
		tm = result.GetTagsMatcher(0);
		for (auto it = result.begin(); it != result.end(); ++it) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			reindexer::WrSerializer sr;
			auto err = it.GetJSON(sr, false);
			ASSERT_TRUE(err.ok()) << err.what();
			std::string_view sv = sr.Slice();
			resStrings.emplace_back(sv.data(), sv.size());
		}
	};

	std::vector<std::string> resStrings, resStringsBeforeTest;
	reindexer::TagsMatcher tm, tmBeforeTest;  // Expecting the same tagsmatchers before and after rename
	getRowsInJSON(default_namespace, resStringsBeforeTest, tmBeforeTest);

	// ok
	rt.RenameNamespace(default_namespace, renameNamespace);
	testInList(renameNamespace, true);
	testInList(default_namespace, false);
	getRowsInJSON(renameNamespace, resStrings, tm);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";
	ASSERT_EQ(tm.stateToken(), tmBeforeTest.stateToken());
	ASSERT_EQ(tm.version(), tmBeforeTest.version());

	// rename to equal name
	rt.RenameNamespace(renameNamespace, renameNamespace);
	testInList(renameNamespace, true);
	getRowsInJSON(renameNamespace, resStrings, tm);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";
	ASSERT_EQ(tm.stateToken(), tmBeforeTest.stateToken());
	ASSERT_EQ(tm.version(), tmBeforeTest.version());

	// rename to empty namespace
	auto err = rt.reindexer->RenameNamespace(renameNamespace, "");
	ASSERT_FALSE(err.ok()) << err.what();
	testInList(renameNamespace, true);
	getRowsInJSON(renameNamespace, resStrings, tm);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";
	ASSERT_EQ(tm.stateToken(), tmBeforeTest.stateToken());
	ASSERT_EQ(tm.version(), tmBeforeTest.version());

	// rename to system namespace
	err = rt.reindexer->RenameNamespace(renameNamespace, "#rename_namespace");
	ASSERT_FALSE(err.ok()) << err.what();
	testInList(renameNamespace, true);
	getRowsInJSON(renameNamespace, resStrings, tm);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";
	ASSERT_EQ(tm.stateToken(), tmBeforeTest.stateToken());
	ASSERT_EQ(tm.version(), tmBeforeTest.version());

	// rename to existing namespace
	rt.RenameNamespace(renameNamespace, existingNamespace);
	testInList(renameNamespace, false);
	testInList(existingNamespace, true);
	getRowsInJSON(existingNamespace, resStrings, tm);
	ASSERT_TRUE(resStrings == resStringsBeforeTest) << "Data in namespace changed";
	ASSERT_EQ(tm.stateToken(), tmBeforeTest.stateToken());
	ASSERT_EQ(tm.version(), tmBeforeTest.version());
}

TEST_F(ReindexerApi, ConcurrentRenaming) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	for (int i = 0; i < 1000; ++i) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = i;
		item["column1"] = i + 100;
		rt.Upsert(default_namespace, item);
	}

	const std::string renamedNamespace("renamed_namespace");
	std::atomic_bool done{false};
	std::atomic_uint32_t succeed = 0;

	auto executeAndValidate = [this, &succeed](std::string_view ns, std::string_view otherNs, const Query& q, size_t expectedCnt) {
		QueryResults qr;
		auto err = rt.reindexer->Select(q, qr);
		if (err.ok()) {
			ASSERT_EQ(qr.Count(), expectedCnt);
			++succeed;
		} else if (err.code() == errNotFound) {
			ASSERT_EQ(err.whatStr(), fmt::format("Namespace '{}' does not exist", ns));
		} else if (err.code() == errConflict) {
			ASSERT_EQ(err.whatStr(), fmt::format("Unable to get namespace: '{}'/'{}' - conflicting rename is in progress", ns, otherNs));
		} else {
			ASSERT_TRUE(false) << err.what();
		}
	};

	auto selectionTh1 = [&](std::string_view ns, std::string_view otherNs) {
		while (!done) {
			executeAndValidate(ns, otherNs, Query(ns).Sort("hash()", false).Limit(10), 10);
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	};
	auto selectionTh2 = [&](std::string_view ns, std::string_view otherNs) {
		while (!done) {
			executeAndValidate(ns, otherNs, Query(ns).InnerJoin("id", "id", CondEq, Query(ns)).Sort("hash()", false).Limit(10), 10);
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	};
	auto renameTh = [&, this]() {
		unsigned counter = 0;
		while (!done) {
			if (counter++ % 2 == 0) {
				rt.RenameNamespace(default_namespace, renamedNamespace);
			} else {
				rt.RenameNamespace(renamedNamespace, default_namespace);
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	};
	std::vector<std::thread> threads;
	threads.reserve(5);
	threads.emplace_back(selectionTh1, default_namespace, renamedNamespace);
	threads.emplace_back(selectionTh1, renamedNamespace, default_namespace);
	threads.emplace_back(selectionTh2, default_namespace, renamedNamespace);
	threads.emplace_back(selectionTh2, renamedNamespace, default_namespace);
	threads.emplace_back(renameTh);
	std::this_thread::sleep_for(std::chrono::seconds(5));
	done = true;
	for (auto& th : threads) {
		th.join();
	}
	ASSERT_GT(succeed, 0);
}

TEST_F(ReindexerApi, AddIndex) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
}

TEST_F(ReindexerApi, DistinctDiffType) {
	reindexer::p_string stringVal("abc");
	std::hash<reindexer::p_string> hashStr;
	size_t vString = hashStr(stringVal);
	std::hash<size_t> hashInt;
	auto vInt = hashInt(vString);
	ASSERT_EQ(vString, vInt) << "hash not equals";

	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	{
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = 1;
		item["column1"] = int64_t(vInt);
		rt.Upsert(default_namespace, item);
	}
	{
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = 2;
		item["column1"] = stringVal;
		rt.Upsert(default_namespace, item);
	}
	{
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = 3;
		item["column1"] = stringVal;
		rt.Upsert(default_namespace, item);
	}

	auto result = rt.ExecSQL("select column1, distinct(column1) from test_namespace;");
	ASSERT_EQ(result.Count(), 2);
	std::set<std::string> BaseVals = {"{\"column1\":" + std::to_string(int64_t(vInt)) + "}", "{\"column1\":\"abc\"}"};
	std::set<std::string> Vals;
	for (auto& r : result) {
		reindexer::WrSerializer ser;
		auto err = r.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		Vals.insert(ser.c_str());
	}
	ASSERT_EQ(BaseVals, Vals);
}

TEST_F(ReindexerApi, DistinctCompositeIndex) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"v1", "-", "int", IndexOpts()});
	rt.AddIndex(default_namespace, {"v2", "-", "int", IndexOpts()});

	reindexer::IndexDef indexDeclr{"v1+v2", reindexer::JsonPaths({"v1", "v2"}), "hash", "composite", IndexOpts()};
	rt.AddIndex(default_namespace, indexDeclr);

	{
		Item item(NewItem(default_namespace));
		item["id"] = 1;
		item["v1"] = 2;
		item["v2"] = 3;
		rt.Upsert(default_namespace, item);
	}
	{
		Item item(NewItem(default_namespace));
		item["id"] = 2;
		item["v1"] = 2;
		item["v2"] = 3;
		rt.Upsert(default_namespace, item);
	}

	Query q{default_namespace};
	q.Distinct("v1+v2");
	{
		auto qr = rt.Select(q);
		EXPECT_EQ(qr.Count(), 1);
	}

	{
		Item item(NewItem(default_namespace));
		item["id"] = 3;
		item["v1"] = 3;
		item["v2"] = 2;
		rt.Upsert(default_namespace, item);
	}
	{
		auto qr = rt.Select(q);
		EXPECT_EQ(qr.Count(), 2);
	}
	{
		Item item(NewItem(default_namespace));
		item["id"] = 4;
		rt.Upsert(default_namespace, item);
	}

	{
		Item item(NewItem(default_namespace));
		item["id"] = 5;
		rt.Upsert(default_namespace, item);
	}
	{
		auto qr = rt.Select(q);
		EXPECT_EQ(qr.Count(), 3);
	}
	{
		Item item(NewItem(default_namespace));
		item["id"] = 6;
		item["v1"] = 3;
		rt.Upsert(default_namespace, item);
	}
	{
		Item item(NewItem(default_namespace));
		item["id"] = 7;
		item["v1"] = 3;
		rt.Upsert(default_namespace, item);
	}
	{
		auto qr = rt.Select(q);
		EXPECT_EQ(qr.Count(), 4);
	}
	{
		Item item(NewItem(default_namespace));
		item["id"] = 8;
		item["v1"] = 4;
		rt.Upsert(default_namespace, item);
	}
	{
		auto qr = rt.Select(q);
		EXPECT_EQ(qr.Count(), 5);
	}
}

TEST_F(ReindexerApi, DistinctMultiColumn) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	struct [[nodiscard]] row {
		std::vector<reindexer::Variant> vals;
		row() = default;
		row(std::span<const reindexer::Variant> d) : vals(d.begin(), d.end()) {}
		bool operator<(const row& other) const {
			assertrx(other.vals.size() == vals.size());
			for (unsigned int k = 0; k < vals.size(); k++) {
				if (!vals[k].Type().IsSame(other.vals[k].Type())) {
					return vals[k].Type().ToTagType() < other.vals[k].Type().ToTagType();
				}
				if (vals[k].Type().Is<reindexer::KeyValueType::Null>()) {
					continue;
				}
				if (vals[k] != other.vals[k]) {
					return vals[k] < other.vals[k];
				}
			}
			return false;
		}
	};
	std::set<row> data;

	auto insertItem = [&data, this](int id, const std::vector<std::vector<reindexer::Variant>>& rows, bool asVal = true) {
		if (rows.empty()) {
			return;
		}
		Item item(NewItem(default_namespace));
		std::vector<std::vector<reindexer::Variant>> arrays;
		arrays.resize(rows[0].size());
		std::vector<bool> mask;
		mask.resize(arrays.size(), true);
		unsigned maxArraySize = 0;
		for (const auto& r : rows) {
			bool isAdd = false;
			for (unsigned int i = 0; i < r.size(); i++) {
				if (r[i].Type().Is<reindexer::KeyValueType::Null>()) {
					mask[i] = false;
				}
				if (mask[i]) {
					arrays[i].emplace_back(r[i]);
					isAdd = true;
				}
			}
			if (isAdd) {
				maxArraySize++;
			}
		}

		reindexer::WrSerializer ser;
		reindexer::JsonBuilder builder(ser);
		builder.Put("id", id);
		if (maxArraySize) {
			for (unsigned int i = 0; i < arrays.size(); i++) {
				if (arrays[i].size() > 0) {
					if (asVal && arrays[i].size() == 1) {
						builder.Put("v" + std::to_string(i), arrays[i][0]);
					} else {
						auto a = builder.Array("v" + std::to_string(i));
						for (const auto& p : arrays[i]) {
							a.Put(reindexer::TagName::Empty(), p);
						}
					}
				}
			}
		}
		builder.End();
		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		rt.Upsert(default_namespace, item);
		if (maxArraySize > 0) {
			for (unsigned int i = 0; i < arrays.size(); i++) {
				if (arrays[i].size() > 0) {
					if (asVal && arrays[i].size() == 1) {
						builder.Put("v" + std::to_string(i), arrays[i][0]);
					} else {
						auto a = builder.Array("v" + std::to_string(i));
						for (const auto& p : arrays[i]) {
							a.Put(reindexer::TagName::Empty(), p);
						}
					}
				}
			}
			for (unsigned k = 0; k < maxArraySize; ++k) {
				row r1;
				for (unsigned i = 0; i < arrays.size(); ++i) {
					if (arrays[i].size() == 1 && asVal) {
						r1.vals.emplace_back(arrays[i][0]);
					} else {
						if (k < arrays[i].size()) {
							r1.vals.emplace_back(arrays[i][k]);
						} else {
							r1.vals.emplace_back(Variant{});
						}
					}
				}
				data.insert(r1);
			}
		}
	};

	using namespace std::string_view_literals;
	std::initializer_list<std::string_view> distinctFields{"v0"sv, "v1"sv, "v2"sv};

	auto check = [this, &distinctFields, &data](int count, int distinctCount) {
		Query q{default_namespace};

		q.Distinct("v0"sv, "v1"sv, "v2"sv);
		auto qr = rt.Select(q);
		ASSERT_EQ(qr.Count(), count);
		const std::vector<reindexer::AggregationResult>& agr = qr.GetAggregationResults();
		ASSERT_EQ(agr.size(), 1);
		const reindexer::AggregationResult& agr0 = agr[0];
		ASSERT_EQ(agr0.GetType(), AggDistinct);
		{
			const auto& f = agr0.GetFields();
			reindexer::h_vector<std::string, 1> fieldsToCompare{distinctFields.begin(), distinctFields.end()};
			ASSERT_EQ(f, fieldsToCompare);
			ASSERT_EQ(agr0.GetDistinctRowCount(), distinctCount);
			std::set<row> q;
			for (unsigned int m = 0; m < agr0.GetDistinctRowCount(); m++) {
				const auto& r = agr0.GetDistinctRow(m);
				q.insert(row{r});
			}
			std::vector<row> diff;
			std::set_difference(data.begin(), data.end(), q.begin(), q.end(), std::back_inserter(diff));
			ASSERT_TRUE(diff.empty());
		}
	};

	{
		insertItem(0, {std::vector{Variant{int64_t(1)}, Variant{2.12}, Variant{"string"}}});
		check(1, 1);
	}
	{
		insertItem(1, {std::vector{Variant{int64_t(3)}, Variant{3.12}, Variant{"string"}}});
		check(2, 2);
	}
	{
		insertItem(2, {std::vector{Variant{int64_t(4)}, Variant{3.12}, Variant{"string"}}});
		check(3, 3);
	}
	{
		insertItem(3, {std::vector{Variant{int64_t(4)}, Variant{3.12}, Variant{}}});
		check(4, 4);
	}
	{
		insertItem(4, {std::vector{Variant{}, Variant{3.12}, Variant{}}});
		check(5, 5);
	}
	{
		insertItem(5, {std::vector{Variant{}, Variant{}, Variant{}}});
		check(5, 5);
	}
	{
		insertItem(6, {std::vector{Variant{"4"}, Variant{3.12}, Variant{}}});
		check(6, 6);
	}
	{
		insertItem(7, {std::vector{Variant{"45"}, Variant{4.12}, Variant{}}}, false);
		check(7, 7);
	}
	{
		insertItem(8,
				   {std::vector{Variant{"47"}, Variant{5.12}, Variant{"100"}}, std::vector{Variant{"48"}, Variant{5.14}, Variant{"101"}}},
				   false);
		check(8, 9);
	}
	{
		insertItem(9,
				   {std::vector{Variant{"47"}, Variant{5.12}, Variant{"102"}}, std::vector{Variant{"48"}, Variant{5.14}, Variant{"101"}}},
				   false);
		check(9, 10);
	}
	{
		insertItem(10,
				   {std::vector{Variant{"47"}, Variant{5.12}, Variant{"102"}}, std::vector{Variant{"48"}, Variant{5.14}, Variant{"101"}}},
				   false);
		check(9, 10);
	}
	{
		insertItem(11,
				   {std::vector{Variant{"50"}, Variant{6.12}, Variant{"202"}}, std::vector{Variant{"51"}, Variant{}, Variant{"201"}},
					std::vector{Variant{"52"}, Variant{}, Variant{}}},
				   false);
		check(10, 13);
	}
	{
		insertItem(12,
				   {std::vector{Variant{"55"}, Variant{6.15}, Variant{"202"}}, std::vector{Variant{"56"}, Variant{}, Variant{}},
					std::vector{Variant{"57"}, Variant{}, Variant{}}},
				   true);
		check(11, 16);
	}
	{
		insertItem(13,
				   {std::vector{Variant{"61"}, Variant{6.15}, Variant{}}, std::vector{Variant{"62"}, Variant{}, Variant{}},
					std::vector{Variant{"63"}, Variant{}, Variant{}}},
				   true);
		check(12, 19);
	}
}

TEST_F(ReindexerApi, CompositeIndexCreationError) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"x", "hash", "int", IndexOpts()});

	constexpr std::string_view kExpectedErrMsgField =
		"Composite indexes over non-indexed field ('{}') are not supported yet (except for full-text indexes). Create at least column "
		"index('-') over each field inside the composite index";
	{
		// Attempt to create composite over 2 non-index fields
		reindexer::IndexDef indexDeclr{"v1+v2", reindexer::JsonPaths({"v1", "v2"}), "hash", "composite", IndexOpts()};
		auto err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.whatStr(), fmt::format(kExpectedErrMsgField, "v1"));
	}
	{
		// Attempt to create composite over 1 index and 1 non-index fields
		reindexer::IndexDef indexDeclr{"id+v2", reindexer::JsonPaths({"id", "v2"}), "hash", "composite", IndexOpts()};
		auto err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.whatStr(), fmt::format(kExpectedErrMsgField, "v2"));
	}
	{
		// Attempt to create composite over 1 index and 1 non-index fields
		reindexer::IndexDef indexDeclr{"v2+id", reindexer::JsonPaths({"v2", "id"}), "hash", "composite", IndexOpts()};
		auto err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_EQ(err.whatStr(), fmt::format(kExpectedErrMsgField, "v2"));
	}
	{
		// Attempt to create sparse composite index
		reindexer::IndexDef indexDeclr{"id+x", reindexer::JsonPaths({"id", "x"}), "hash", "composite", IndexOpts().Sparse()};
		auto err = rt.reindexer->AddIndex(default_namespace, indexDeclr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_STREQ(err.what(), "Composite index cannot be sparse. Use non-sparse composite instead");
	}
}

TEST_F(ReindexerApi, AddIndex_CaseInsensitive) {
	rt.OpenNamespace(default_namespace);

	std::string idxName = "IdEnTiFiCaToR";
	rt.AddIndex(default_namespace, {idxName, "hash", "int", IndexOpts().PK()});

	// check adding index named in lower case
	idxName = "identificator";
	auto err = rt.reindexer->AddIndex(default_namespace, {idxName, "hash", "int64", IndexOpts().PK()});
	ASSERT_FALSE(err.ok()) << "Somehow index 'identificator' was added. But index 'IdEnTiFiCaToR' already exists";

	// check adding index named in upper case
	idxName = "IDENTIFICATOR";
	err = rt.reindexer->AddIndex(default_namespace, {idxName, "hash", "int64", IndexOpts().PK()});
	ASSERT_FALSE(err.ok()) << "Somehow index 'IDENTIFICATOR' was added. But index 'IdEnTiFiCaToR' already exists";

	// check case-insensitive field access
	Item item(rt.NewItem(default_namespace));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	ASSERT_NO_THROW(item[idxName] = 1234);
}

TEST_F(ReindexerApi, AddExistingIndex) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
}

TEST_F(ReindexerApi, AddUnacceptablePKIndex) {
	const std::string kIdxName = "id";
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));

	// Try to add an array as a PK
	auto err = rt.reindexer->AddIndex(default_namespace, {kIdxName, "hash", "int", IndexOpts().PK().Array()});
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
	rt.AddIndex(default_namespace, {kIdxName, "hash", "int", IndexOpts().PK()});
}

TEST_F(ReindexerApi, UpdateToUnacceptablePKIndex) {
	const std::string kIdxName = "id";
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {kIdxName, "hash", "int", IndexOpts().PK()});

	// Try to update to an array as a PK
	auto err = rt.reindexer->UpdateIndex(default_namespace, {kIdxName, "tree", "int", IndexOpts().PK().Array()});
	ASSERT_EQ(err.code(), errParams) << err.what();

	// Try to update to a store indexes of few types as a PKs
	const std::vector<std::string> kTypes = {"int", "bool", "int64", "double", "string"};
	for (auto& type : kTypes) {
		err = rt.reindexer->UpdateIndex(default_namespace, {kIdxName, "-", type, IndexOpts().PK()});
		ASSERT_EQ(err.code(), errParams) << err.what();
	}

	// Update to a valid index with the same name
	rt.UpdateIndex(default_namespace, {kIdxName, "tree", "int", IndexOpts().PK()});
}

TEST_F(ReindexerApi, IndexNameValidation) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	// Index names with cirillic characters are not allowed
	auto err = rt.reindexer->AddIndex(default_namespace, {"индекс", "hash", "int", IndexOpts().PK()});
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
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	auto err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int64", IndexOpts().PK()});
	ASSERT_EQ(err.code(), errConflict) << err.what();
}

TEST_F(ReindexerApi, CloseNamespace) {
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.CloseNamespace(default_namespace);

	QueryResults qr;
	auto err = rt.reindexer->Select(Query(default_namespace), qr);
	ASSERT_FALSE(err.ok()) << "Namespace '" << default_namespace << "' open. But must be closed";
}

TEST_F(ReindexerApi, DropStorage) {
	const std::string kBaseTestsStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/api_drop_storage/");
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
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled());
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	Item item(rt.NewItem(default_namespace));
	ASSERT_TRUE(!!item);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
}

TEST_F(ReindexerApi, GetItemFromQueryResults) {
	constexpr size_t kItemsCount = 10;
	initializeDefaultNs();
	std::vector<std::pair<int, std::string>> data;
	while (data.size() < kItemsCount) {
		Item item(rt.NewItem(default_namespace));
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		rt.Insert(default_namespace, item);
	}
	auto qr = rt.Select(Query(default_namespace).Sort("id", false));
	ASSERT_EQ(qr.Count(), kItemsCount);
	// items in QueryResults are valid after the ns is destroyed
	rt.TruncateNamespace(default_namespace);
	rt.DropNamespace(default_namespace);

	size_t i = 0;
	for (auto it = qr.begin(), end = qr.end(); it != end; ++it, ++i) {
		ASSERT_LT(i, data.size());
		Item item(it.GetItem());
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		EXPECT_EQ(item["id"].As<int>(), data[i].first);
		EXPECT_EQ(item["value"].As<std::string>(), data[i].second);
	}

	qr.Clear();
	data.clear();
	initializeDefaultNs();
	{
		Item item(rt.NewItem(default_namespace));
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		rt.Insert(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		rt.Upsert(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		data.emplace_back(data.back().first, RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		rt.Upsert(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.size(), RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		rt.Insert(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		data.emplace_back(data.back().first, RandString());
		item["id"] = data.back().first;
		item["value"] = data.back().second;
		rt.Update(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		data.emplace_back(data.back());
		item["id"] = data.back().first;
		item["value"] = RandString();
		rt.Delete(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		item["id"] = static_cast<int>(data.size());
		item["value"] = RandString();
		rt.Update(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());

		item = rt.NewItem(default_namespace);
		item["id"] = static_cast<int>(data.size());
		item["value"] = RandString();
		rt.Delete(default_namespace, item, qr);
		ASSERT_EQ(qr.Count(), data.size());
	}
	rt.TruncateNamespace(default_namespace);
	rt.DropNamespace(default_namespace);

	ASSERT_EQ(qr.Count(), 6);
	ASSERT_EQ(qr.Count(), data.size());
	i = 0;
	for (auto it = qr.begin(), end = qr.end(); it != end; ++it, ++i) {
		ASSERT_LT(i, data.size());
		Item item(it.GetItem());
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		EXPECT_EQ(item["id"].As<int>(), data[i].first);
		EXPECT_EQ(item["value"].As<std::string>(), data[i].second);
	}
}

TEST_F(ReindexerApi, NewItem_CaseInsensitiveCheck) {
	int idVal = 1000;
	std::string valueVal = "value";

	rt.OpenNamespace(default_namespace, StorageOpts().Enabled());
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});

	auto item = rt.NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	ASSERT_NO_THROW(item["ID"] = 1000);
	ASSERT_NO_THROW(item["VaLuE"] = "value");
	ASSERT_NO_THROW(ASSERT_EQ(item["id"].As<int>(), idVal));
	ASSERT_NO_THROW(ASSERT_EQ(item["value"].As<std::string>(), valueVal));
}

TEST_F(ReindexerApi, Insert) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});

	Item item(rt.NewItem(default_namespace));
	auto err = item.FromJSON(R"_({"id":1234, "value" : "value"})_");
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Insert(default_namespace, item);

	auto qr = rt.Select(Query(default_namespace));
	ASSERT_EQ(qr.Count(), 1);

	// check item consist and check case-insensitive access to field by name
	Item selItem(qr.begin().GetItem(false));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));
}

TEST_F(ReindexerApi, ItemJSONWithDouble) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	Item item(rt.NewItem(default_namespace));

	{
		const std::string kJSON = R"_({"id":1234,"double":0.0})_";
		auto err = item.FromJSON(kJSON);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(item.GetJSON(), kJSON);
	}

	{
		const std::string kJSON = R"_({"id":1234,"double":0.1})_";
		auto err = item.FromJSON(kJSON);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(item.GetJSON(), kJSON);
	}
}

TEST_F(ReindexerApi, WithTimeoutInterface) {
	using std::chrono::milliseconds;

	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
	Item item(rt.NewItem(default_namespace));

	auto err = item.FromJSON(R"_({"id":1234, "value" : "value"})_");
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->WithTimeout(milliseconds(1000)).Insert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	err = rt.reindexer->WithTimeout(milliseconds(1000)).Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	// check item consist and check case-insensitive access to field by name
	Item selItem(qr.begin().GetItem(false));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));

	qr.Clear();
	err = rt.reindexer->WithTimeout(milliseconds(1000)).Delete(Query(default_namespace), qr);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <CollateMode collateMode>
struct [[nodiscard]] CollateComparer {
	bool operator()(const std::string& lhs, const std::string& rhs) const {
		return reindexer::collateCompare<collateMode>(lhs, rhs, reindexer::SortingPrioritiesTable()) == reindexer::ComparationResult::Lt;
	}
};

TEST_F(ReindexerApi, SortByMultipleColumns) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"column1", "tree", "int", IndexOpts()});
	rt.AddIndex(default_namespace, {"column2", "tree", "string", IndexOpts()});
	rt.AddIndex(default_namespace, {"column3", "hash", "int", IndexOpts()});

	const std::vector<std::string> possibleValues = {
		"apple",	 "arrangment", "agreement", "banana",	"bull",	 "beech", "crocodile", "crucifix", "coat",	   "day",
		"dog",		 "deer",	   "easter",	"ear",		"eager", "fair",  "fool",	   "foot",	   "genes",	   "genres",
		"greatness", "hockey",	   "homeless",	"homocide", "key",	 "kit",	  "knockdown", "motion",   "monument", "movement"};

	int sameOldValue = 0;
	int stringValuedIdx = 0;
	for (int i = 0; i < 100; ++i) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = i;
		item["column1"] = sameOldValue;
		item["column2"] = possibleValues[stringValuedIdx];
		item["column3"] = rand() % 30;

		rt.Upsert(default_namespace, item);

		if (i % 5 == 0) {
			sameOldValue += 5;
		}
		if (i % 3 == 0) {
			++stringValuedIdx;
		}
		stringValuedIdx %= possibleValues.size();
	}

	const size_t offset = 23;
	const size_t limit = 61;

	Query query{Query(default_namespace, offset, limit).Sort("column1", true).Sort("column2", false).Sort("column3", false)};
	auto qr = rt.Select(query);
	EXPECT_EQ(qr.Count(), limit);

	PrintQueryResults(default_namespace, qr);

	std::vector<Variant> lastValues(query.GetSortingEntries().size());
	for (auto& it : qr) {
		Item item(it.GetItem(false));

		std::vector<reindexer::ComparationResult> cmpRes(query.GetSortingEntries().size());
		std::fill(cmpRes.begin(), cmpRes.end(), reindexer::ComparationResult::Lt);

		for (size_t j = 0; j < query.GetSortingEntries().size(); ++j) {
			const reindexer::SortingEntry& sortingEntry(query.GetSortingEntries()[j]);
			Variant sortedValue = item[sortingEntry.expression];
			if (!lastValues[j].Type().Is<reindexer::KeyValueType::Null>()) {
				cmpRes[j] = lastValues[j].Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(sortedValue);
				bool needToVerify = true;
				if (j != 0) {
					for (int k = j - 1; k >= 0; --k) {
						if (cmpRes[k] != reindexer::ComparationResult::Eq) {
							needToVerify = false;
							break;
						}
					}
				}
				needToVerify = (j == 0) || needToVerify;
				if (needToVerify) {
					bool sortOrderSatisfied = (sortingEntry.desc && (cmpRes[j] & reindexer::ComparationResult::Ge)) ||
											  (!sortingEntry.desc && (cmpRes[j] & reindexer::ComparationResult::Le)) ||
											  (cmpRes[j] == reindexer::ComparationResult::Eq);
					EXPECT_TRUE(sortOrderSatisfied)
						<< "\nSort order is incorrect for column: " << sortingEntry.expression << "; rowID: " << item[1].As<int>();
				}
			}
		}
	}

	// Check sql parser work correctness
	std::ignore = rt.ExecSQL("select * from test_namespace order by column2 asc, column3 desc");
}

TEST_F(ReindexerApi, SortByMultipleColumnsWithLimits) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"f1", "tree", "string", IndexOpts()});
	rt.AddIndex(default_namespace, {"f2", "tree", "int", IndexOpts()});

	const std::vector<std::string_view> srcStrValues = {
		"A", "A", "B", "B", "B", "C", "C",
	};
	const std::vector<int> srcIntValues = {1, 2, 4, 3, 5, 7, 6};

	for (size_t i = 0; i < srcIntValues.size(); ++i) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = static_cast<int>(i);
		item["f1"] = srcStrValues[i];
		item["f2"] = srcIntValues[i];

		rt.Upsert(default_namespace, item);
	}
	const size_t offset = 4;
	const size_t limit = 3;

	Query query{Query(default_namespace, offset, limit).Sort("f1", false).Sort("f2", false)};
	auto qr = rt.Select(query);
	EXPECT_EQ(qr.Count(), limit) << qr.Count();

	const std::vector<int> properRes = {5, 6, 7};
	size_t i = 0;
	for (auto& it : qr) {
		Item item(it.GetItem(false));
		Variant kr = item["f2"];
		EXPECT_EQ(static_cast<int>(kr), properRes[i]);
		++i;
	}
}

TEST_F(ReindexerApi, SortByHash) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	for (int i = 0; i < 1000; ++i) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = i;
		rt.Upsert(default_namespace, item);
	}

	auto getIds = [&](const std::string& sortExpr, std::vector<int>& ids) {
		Query q{default_namespace};
		q.Sort(sortExpr, true);
		auto qr = rt.Select(q);
		for (auto& it : qr) {
			Item item(it.GetItem());
			int id = item["id"].As<int>();
			ids.emplace_back(id);
		}
	};

	{
		const std::string expr("hash()");
		std::vector<int> ids1;
		getIds(expr, ids1);
		std::vector<int> ids2;
		getIds(expr, ids2);
		ASSERT_NE(ids1, ids2);
	}
	{
		int seed = std::rand() % 10000;
		const std::string expr("hash(" + std::to_string(seed) + ")");
		std::vector<int> ids1;
		getIds(expr, ids1);
		std::vector<int> ids2;
		getIds(expr, ids2);
		ASSERT_EQ(ids1, ids2);
	}
}

TEST_F(ReindexerApi, SortByUnorderedIndexes) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"valueInt", "hash", "int", IndexOpts()});
	rt.AddIndex(default_namespace, {"valueString", "hash", "string", IndexOpts()});
	rt.AddIndex(default_namespace, {"valueStringASCII", "hash", "string", IndexOpts().SetCollateMode(CollateASCII)});
	rt.AddIndex(default_namespace, {"valueStringNumeric", "hash", "string", IndexOpts().SetCollateMode(CollateNumeric)});
	rt.AddIndex(default_namespace, {"valueStringUTF8", "hash", "string", IndexOpts().SetCollateMode(CollateUTF8)});

	std::deque<int> allIntValues;
	std::multiset<std::string> allStrValues;
	std::multiset<std::string, CollateComparer<CollateASCII>> allStrValuesASCII;
	std::multiset<std::string, CollateComparer<CollateNumeric>> allStrValuesNumeric;
	std::multiset<std::string, CollateComparer<CollateUTF8>> allStrValuesUTF8;

	for (int i = 0; i < 100; ++i) {
		Item item(rt.NewItem(default_namespace));

		item["id"] = i;
		item["valueInt"] = i;
		allIntValues.push_front(i);

		std::string strCollateNone = RandString();
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

		rt.Upsert(default_namespace, item);
	}

	bool descending = true;
	const unsigned offset = 5;
	const unsigned limit = 30;

	auto sortByIntQr = rt.Select(Query(default_namespace, offset, limit).Sort("valueInt", descending));

	std::deque<int> selectedIntValues;
	for (auto& it : sortByIntQr) {
		Item item(it.GetItem(false));
		int value = item["valueInt"].Get<int>();
		selectedIntValues.push_back(value);
	}

	EXPECT_TRUE(std::equal(allIntValues.begin() + offset, allIntValues.begin() + offset + limit, selectedIntValues.begin()));

	auto validateOrdering = [](std::string_view fieldName, reindexer::QueryResults& qr, auto& expectedOrdering) {
		SCOPED_TRACE(fieldName);
		std::vector<std::string> selectedStrValues;
		for (auto& it : qr) {
			Item item(it.GetItem(false));
			selectedStrValues.emplace_back(item[fieldName].As<std::string>());
		}

		ASSERT_EQ(selectedStrValues.size(), expectedOrdering.size());
		auto itSorted(expectedOrdering.cbegin());
		for (size_t pos = 0; pos < selectedStrValues.size(); ++pos) {
			EXPECT_EQ(selectedStrValues[pos], *itSorted++) << "Pos: " << pos;
		}
	};

	auto sortByStrQr = rt.Select(Query(default_namespace).Sort("valueString", !descending));
	validateOrdering("valueString", sortByStrQr, allStrValues);

	auto sortByASCIIStrQr = rt.Select(Query(default_namespace).Sort("valueStringASCII", !descending));
	validateOrdering("valueStringASCII", sortByASCIIStrQr, allStrValuesASCII);

	auto sortByNumericStrQr = rt.Select(Query(default_namespace).Sort("valueStringNumeric", !descending));
	validateOrdering("valueStringNumeric", sortByNumericStrQr, allStrValuesNumeric);

	auto sortByUTF8StrQr = rt.Select(Query(default_namespace).Sort("valueStringUTF8", !descending));
	validateOrdering("valueStringUTF8", sortByUTF8StrQr, allStrValuesUTF8);
}

TEST_F(ReindexerApi, SortByUnorderedIndexWithJoins) {
	constexpr std::string_view secondNamespace = "test_namespace_2";
	std::vector<int> secondNamespacePKs;

	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"fk", "hash", "int", IndexOpts()});

	{
		rt.OpenNamespace(secondNamespace, StorageOpts().Enabled(false));
		rt.AddIndex(secondNamespace, {"pk", "hash", "int", IndexOpts().PK()});

		for (int i = 0; i < 50; ++i) {
			Item item(rt.NewItem(secondNamespace));
			ASSERT_TRUE(!!item);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();

			secondNamespacePKs.push_back(i);
			item["pk"] = i;

			rt.Upsert(secondNamespace, item);
		}
	}

	for (int i = 0; i < 100; ++i) {
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(!!item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;

		int fk = secondNamespacePKs[rand() % (secondNamespacePKs.size() - 1)];
		item["fk"] = fk;

		rt.Upsert(default_namespace, item);
	}

	bool descending = true;
	const unsigned offset = 10;
	const unsigned limit = 40;

	Query querySecondNamespace = Query(secondNamespace);
	Query joinQuery{Query(default_namespace, offset, limit).Sort("id", descending)};
	joinQuery.InnerJoin("fk", "pk", CondEq, std::move(querySecondNamespace));

	auto queryResult = rt.Select(joinQuery);
	for (auto it : queryResult) {
		auto itemIt = it.GetJoined();
		EXPECT_TRUE(itemIt.getJoinedItemsCount() > 0);
	}
}

static void TestDSLParseCorrectness(const std::string& testDsl) {
	try {
		Query query = query.FromJSON(testDsl);
	} catch (Error& err) {
		EXPECT_TRUE(err.ok()) << err.what();
	}
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
							"cond": "EMPTY"
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
			"op": "AND",
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
											"offset": 0,
											"limit": 3,
											"sort": {
												"field": "",
												"desc": true
											},
											"filters": [{
												"Op": "and",
												"Field": "vec",
												"Cond": "knn",
												"Value": [0.81204872, 0.101326571, 0.101326882],
												"Params": {
													"K": 1024,
													"Ef": 2048
												}
											}]
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
	constexpr std::string_view sql = "select distinct(country), distinct(city) from clients;";

	Query q1 = Query::FromSQL(sql);
	EXPECT_EQ(q1.Entries().Size(), 0);
	ASSERT_EQ(q1.aggregations_.size(), 2);
	EXPECT_EQ(q1.aggregations_[0].Type(), AggDistinct);
	ASSERT_EQ(q1.aggregations_[0].Fields().size(), 1);
	EXPECT_EQ(q1.aggregations_[0].Fields()[0], "country");
	EXPECT_EQ(q1.aggregations_[1].Type(), AggDistinct);
	ASSERT_EQ(q1.aggregations_[1].Fields().size(), 1);
	EXPECT_EQ(q1.aggregations_[1].Fields()[0], "city");

	std::string dsl = q1.GetJSON();
	Query q2;
	EXPECT_NO_THROW(q2 = Query::FromJSON(dsl));
	EXPECT_EQ(q1, q2) << "q1: " << q1.GetSQL() << "\nq2: " << q2.GetSQL();

	Query q3{Query(default_namespace).Distinct("name").Distinct("city").Where("id", CondGt, static_cast<int64_t>(10))};
	std::string sql2 = q3.GetSQL();

	Query q4;
	EXPECT_NO_THROW(q4 = Query::FromSQL(sql2));
	EXPECT_EQ(q3, q4) << "q3: " << q3.GetSQL() << "\nq4: " << q4.GetSQL();
	EXPECT_EQ(sql2, q4.GetSQL());
}

class [[nodiscard]] CanceledRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::Explicit; }
	bool IsCancelable() const noexcept override { return true; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
};

class [[nodiscard]] DummyRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return false; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
};

class [[nodiscard]] FakeRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return true; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
};

TEST_F(ReindexerApi, ContextCancelingTest) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});

	Item item(rt.NewItem(default_namespace));
	auto err = item.FromJSON(R"_({"id":1234, "value" : "value"})_");
	ASSERT_TRUE(err.ok()) << err.what();

	// Canceled insert
	CanceledRdxContext canceledCtx;
	err = rt.reindexer->WithContext(&canceledCtx).Insert(default_namespace, item);
	ASSERT_TRUE(err.code() == errCanceled);

	// Canceled delete
	std::vector<reindexer::NamespaceDef> namespaces;
	err = rt.reindexer->WithContext(&canceledCtx).EnumNamespaces(namespaces, reindexer::EnumNamespacesOpts());
	ASSERT_TRUE(err.code() == errCanceled);

	// Canceled select
	QueryResults qr;
	err = rt.reindexer->WithContext(&canceledCtx).Select(Query(default_namespace), qr);
	ASSERT_TRUE(err.code() == errCanceled);
	qr.Clear();

	std::string_view sqlQuery = "select * from test_namespace";
	err = rt.reindexer->WithContext(&canceledCtx).ExecSQL(sqlQuery, qr);
	ASSERT_TRUE(err.code() == errCanceled);
	qr.Clear();

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
	rt.Select(Query(default_namespace), qr);
	ASSERT_EQ(qr.Count(), 1);
	Item selItem(qr.begin().GetItem(false));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<std::string>(), "value"));
	qr.Clear();

	// Canceled update
	err = rt.reindexer->WithContext(&canceledCtx).Update(default_namespace, item);
	ASSERT_TRUE(err.code() == errCanceled);
	rt.Select(Query(default_namespace), qr);
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
	qr.Clear();
	rt.Select(Query(default_namespace), qr);
	ASSERT_EQ(qr.Count(), 1);
	qr.Clear();

	err = rt.reindexer->WithContext(&fakeCtx).Delete(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Select(Query(default_namespace), qr);
	ASSERT_EQ(qr.Count(), 0);
}

TEST_F(ReindexerApi, JoinConditionsSqlParserTest) {
	constexpr std::string_view sql1 =
		"SELECT * FROM ns WHERE a > 0 AND INNER JOIN (SELECT * FROM ns2 WHERE b > 10 AND c = 1) ON ns2.id = ns.fk_id";
	const auto q1 = Query::FromSQL(sql1);
	ASSERT_EQ(q1.GetSQL(), sql1);

	constexpr std::string_view sql2 =
		"SELECT * FROM ns WHERE a > 0 AND INNER JOIN (SELECT * FROM ns2 WHERE b > 10 AND c = 1 LIMIT 0) ON ns2.id = ns.fk_id";
	const auto q2 = Query::FromSQL(sql2);
	ASSERT_EQ(q2.GetSQL(), sql2);
}

TEST_F(ReindexerApi, UpdateWithBoolParserTest) {
	constexpr std::string_view sql = "UPDATE ns SET flag1 = true,flag2 = false WHERE id > 100";
	Query query = Query::FromSQL(sql);
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
	constexpr std::string_view sql =
		"SELECT * FROM ns WHERE (f1 = 1 AND f2 = 2 OR f3 = 3 equal_position(f1, f2) equal_position(f1, f3)) OR (f4 = 4 AND f5 > 5 "
		"equal_position(f4, f5))";

// GCC 12 False positive warning in the expression tree
#pragma GCC diagnostic ignored "-Warray-bounds"
#pragma GCC diagnostic push
	Query query = Query::FromSQL(sql);
#pragma GCC diagnostic pop
	EXPECT_EQ(query.GetSQL(), sql);
	EXPECT_TRUE(query.Entries().equalPositions.empty());
	ASSERT_EQ(query.Entries().Size(), 7);

	ASSERT_TRUE(query.Entries().IsSubTree(0));
	const auto& ep1 = query.Entries().Get<reindexer::QueryEntriesBracket>(0).equalPositions;
	ASSERT_EQ(ep1.size(), 2);
	ASSERT_EQ(ep1[0].size(), 2);
	EXPECT_EQ(ep1[0][0], "f1");
	EXPECT_EQ(ep1[0][1], "f2");
	ASSERT_EQ(ep1[1].size(), 2);
	EXPECT_EQ(ep1[1][0], "f1");
	EXPECT_EQ(ep1[1][1], "f3");

	ASSERT_TRUE(query.Entries().IsSubTree(4));
	const auto& ep2 = query.Entries().Get<reindexer::QueryEntriesBracket>(4).equalPositions;
	ASSERT_EQ(ep2.size(), 1);
	ASSERT_EQ(ep2[0].size(), 2);
	EXPECT_EQ(ep2[0][0], "f4");
	EXPECT_EQ(ep2[0][1], "f5");
}

TEST_F(ReindexerApi, SchemaSuggestions) {
	rt.OpenNamespace(default_namespace);
	rt.OpenNamespace("second_ns");

	// clang-format off
	constexpr std::string_view jsonschema = R"xxx(
	{
	  "required": [
		"Countries",
		"Nest_fake",
		"nested",
		"second_field"
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
		"second_field": {
		  "type": "number"
		},
	  },
	  "additionalProperties": false,
	  "type": "object"
	})xxx";
	// clang-format on

	// clang-format off
	constexpr std::string_view jsonschema2 = R"xxx(
	{
	  "required": [
		"id",
		"Field",
	  ],
	  "properties": {
		"id": {
		  "type": "number"
		},
		"Field": {
		  "type": "number"
		}
	  },
	  "additionalProperties": false,
	  "type": "object"
	})xxx";
	// clang-format on
	auto err = rt.reindexer->SetSchema(default_namespace, jsonschema);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->SetSchema("second_ns", jsonschema2);
	ASSERT_TRUE(err.ok()) << err.what();

	auto validateSuggestions = [this](std::string_view sql, const std::unordered_set<std::string_view>& expected, size_t position) {
		std::vector<std::string> suggestions;
		auto err = rt.reindexer->GetSqlSuggestions(sql, position, suggestions);
		ASSERT_TRUE(err.ok()) << err.what();
		for (auto& sugg : suggestions) {
			EXPECT_TRUE(expected.find(sugg) != expected.end()) << sql << '\n'
															   << std::string(position, ' ') << "^\nUnexpected suggestion: " << sugg;
		}
		for (auto& expSugg : expected) {
			EXPECT_TRUE(std::find(suggestions.begin(), suggestions.end(), expSugg) != suggestions.end())
				<< sql << '\n'
				<< std::string(position, ' ') << "^\nExpected but not found suggestion: " << expSugg;
		}
	};

	struct {
		std::string_view sql;
		std::unordered_set<std::string_view> expected;
		size_t position = sql.empty() ? 0 : sql.size() - 1;
	} testData[]{
		{"select * from test_namespace where ne", {"Nest_fake", "nested"}},
		{"select * from test_namespace where nested", {}},
		{"select * from test_namespace where nested.", {".Name", ".Naame", ".Age"}},
		{"select * from test_namespace where nested.Na", {".Name", ".Naame"}},

		{"", {"explain", "local", "select", "delete", "update", "truncate"}},
		{"s", {"select"}},
		{"select", {}},
		{"select ", {"*", "avg", "min", "max", "facet", "sum", "distinct", "rank()", "count", "count_cached", "vectors()"}},
		{"select *,", {}},
		{"select *, ", {"*", "avg", "min", "max", "facet", "sum", "distinct", "rank()", "count", "count_cached", "vectors()"}},
		{"select *, f", {"facet", "Field"}},
		{"select f", {"facet", "Field"}},
		{"select * ", {"from"}},
		{"select * f", {"from"}},
		{"select * from ",
		 {"test_namespace", "second_ns", "#memstats", "#activitystats", "#config", "#queriesperfstats", "#namespaces", "#perfstats",
		  "#clientsstats", "#replicationstats"}},
		{"select * from te", {"test_namespace"}},
		{"select * from test_namespace ",
		 {"where", ";", "equal_position", "inner", "join", "left", "limit", "merge", "offset", "or", "order"}},
		{"select * from test_namespace w", {"where"}},
		{"select * from test_namespace where ",
		 {"second_field", "ST_DWithin", "Countries", "nested", "Nest_fake", "inner", "join", "left", "not", "equal_position", "KNN"}},
		{"select * from test_namespace where s", {"second_field", "ST_DWithin"}},
		{"select * from second_ns where i", {"id", "inner"}},
		{"select * from test_namespace where (", {}},
		{"select * from test_namespace where (s", {"second_field", "ST_DWithin", "select"}},
		{"select * from test_namespace where (select m", {"max", "min"}},
		{"select * from test_namespace where (select i", {"id", "items_count", "ip"}},
		{"select * from test_namespace where (select second_field f", {"from"}},
		{"select * from test_namespace where (select id from s", {"second_ns"}},
		{"select * from test_namespace where (select Field from second_ns where ",
		 {"id", "ST_DWithin", "Field", "not", "equal_position", "KNN"}},
		{"select * from test_namespace where C", {"Countries"}},
		{"select * from test_namespace where Countries == (", {}},
		{"select * from test_namespace where Countries == (s", {"select"}},
		{"select * from test_namespace where Countries == (select m", {"max", "min"}},
		{"select * from test_namespace where Countries == (select i", {"id", "ip", "items_count"}},
		{"select * from test_namespace where Countries == (select second_field f", {"from"}},
		{"select * from test_namespace where Countries == (select second_field from ",
		 {"test_namespace", "second_ns", "#memstats", "#activitystats", "#config", "#queriesperfstats", "#namespaces", "#perfstats",
		  "#clientsstats", "#replicationstats"}},
		{"select * from test_namespace where Countries == (select second_field from s", {"second_ns"}},
		{"select * from test_namespace where i", {"inner"}},
		{"select * from test_namespace where inner j", {"join"}},
		{"select * from test_namespace where inner join s", {"second_ns"}},
		{"select * from test_namespace where inner join (s", {"select"}},
		{"select * from test_namespace where inner join (select m", {"min", "max"}},
		{"select * from test_namespace where inner join (select i", {"id", "ip", "items_count"}},
		{"select * from test_namespace where inner join (select second_field f", {"from"}},
		{"select * from test_namespace where inner join (select second_field from s", {"second_ns"}},
		{"SELECT * FROM ns WHERE id = ( ", {"null", "empty", "not", "select"}},
	};

	for (const auto& [sql, expected, position] : testData) {
		if (sql.empty() || sql.back() == ' ') {
			validateSuggestions(sql, expected, position);
		} else {
			for (const auto& td : testData) {
				if (reindexer::checkIfStartsWith(sql, td.sql)) {
					validateSuggestions(td.sql, expected, position);
				}
			}
		}
	}
}

TEST_F(ReindexerApi, LoggerWriteInterruptTest) {
	struct [[nodiscard]] Logger {
		Logger() {
			spdlog::drop_all();
			spdlog::init_thread_pool(16384, 1);
			spdlog::flush_every(std::chrono::seconds(2));
			spdlog::flush_on(spdlog::level::err);
			spdlog::set_level(spdlog::level::trace);
			spdlog::set_pattern("%^[%L%d/%m %T.%e %t] %v%$", spdlog::pattern_time_type::utc);

			std::remove(logFile.c_str());
			using LogFactoryT = spdlog::async_factory_impl<spdlog::async_overflow_policy::discard_new>;
			using spdlog::sinks::reopen_file_sink_st;
			auto lptr = LogFactoryT::create<reopen_file_sink_st>("log", logFile);
			assertrx(lptr->sinks().size() == 1);
			sinkPtr = std::dynamic_pointer_cast<reopen_file_sink_st>(lptr->sinks()[0]);
			assertrx(sinkPtr);
			logger = reindexer_server::LoggerWrapper("log");
		}
		~Logger() {
			spdlog::drop_all();
			spdlog::shutdown();
			std::remove(logFile.c_str());
		}
		const std::string logFile = "/tmp/reindex_logtest.out";
		reindexer_server::LoggerWrapper logger;
		std::shared_ptr<spdlog::sinks::reopen_file_sink_st> sinkPtr;
	} instance;

	reindexer::logInstallWriter(
		[&](int level, char* buf) {
			if (level <= LogTrace) {
				instance.logger.trace(buf);
			}
		},
		reindexer::LoggerPolicy::WithLocks, int(LogTrace));
	auto writeThread = std::thread([]() {
		for (size_t i = 0; i < 10000; ++i) {
			logFmt(LogTrace, "Detailed and amazing description of this error: [{}]!", i);
		}
	});
	auto reopenThread = std::thread([&instance]() {
		for (size_t i = 0; i < 1000; ++i) {
			instance.sinkPtr->reopen();
			logFmt(LogTrace, "REOPENED [{}]", i);
			std::this_thread::sleep_for(std::chrono::milliseconds(3));
		}
	});
	writeThread.join();
	reopenThread.join();
	logFmt(LogTrace, "FINISHED\n");
	reindexer::logInstallWriter(nullptr, reindexer::LoggerPolicy::WithLocks, int(LogTrace));
}

TEST_F(ReindexerApi, IntToStringIndexUpdate) {
	const std::string kFieldId = "id";
	const std::string kFieldNumeric = "numeric";

	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {kFieldId, "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {kFieldNumeric, "tree", "int", IndexOpts()});

	for (int i = 0; i < 100; ++i) {
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		item[kFieldId] = i;
		item[kFieldNumeric] = i * 2;

		rt.Upsert(default_namespace, item);
	}

	auto err = rt.reindexer->UpdateIndex(default_namespace, {kFieldNumeric, "tree", "string", IndexOpts()});
	EXPECT_FALSE(err.ok());
	EXPECT_STREQ(err.what(), "Cannot convert key from type int to string");

	QueryResults qr;
	err = rt.reindexer->Select(Query(default_namespace), qr);
	EXPECT_TRUE(err.ok()) << err.what();

	for (auto it : qr) {
		Item item(it.GetItem(false));
		Variant v = item[kFieldNumeric];
		EXPECT_TRUE(v.Type().Is<reindexer::KeyValueType::Int>()) << v.Type().Name();
	}
}

TEST_F(ReindexerApi, SelectFilterWithAggregationConstraints) {
	Query q;

	std::string sql = "select id, distinct(year) from test_namespace";
	EXPECT_NO_THROW(q = Query::FromSQL(sql));

	Query qJSON;
	EXPECT_NO_THROW(qJSON = Query::FromJSON(q.GetJSON()));

	q = Query().Select({"id"});
	EXPECT_NO_THROW(q.Aggregate(AggDistinct, {"year"}, {}));

	sql = "select id, max(year) from test_namespace";
	EXPECT_THROW(q = Query::FromSQL(sql), Error);
	q = Query(default_namespace).Select({"id"});
	q.aggregations_.emplace_back(reindexer::AggregateEntry{AggMax, {"year"}});
	try {
		std::ignore = Query::FromJSON(q.GetJSON());
	} catch (Error& err) {
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), reindexer::kAggregationWithSelectFieldsMsgError);
	}

	EXPECT_THROW(q.Aggregate(AggMax, {"price"}, {}), Error);

	sql = "select facet(year), id, name from test_namespace";
	EXPECT_THROW(q = Query::FromSQL(sql), Error);
	q = Query(default_namespace).Select({"id", "name"});
	EXPECT_THROW(q.Aggregate(AggFacet, {"year"}, {}), Error);
	try {
		std::ignore = Query::FromJSON(fmt::format(R"({{"namespace":"{}",
	"limit":-1,
	"offset":0,
	"req_total":"disabled",
	"explain":false,
	"type":"select",
	"select_with_rank":false,
	"select_filter":[
		"id",
		"name"
	],
	"select_functions":[],
	"sort":[],
	"filters":[],
	"merge_queries":[],
	"aggregations":[
		{{
			"type":"facet",
			"sort":[],
			"fields":["year"]
		}}
	]}})",
												  default_namespace));
	} catch (Error& err) {
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), reindexer::kAggregationWithSelectFieldsMsgError);
	}

	EXPECT_THROW((void)Query::FromSQL("select max(id), * from test_namespace"), Error);
	EXPECT_THROW((void)Query::FromSQL("select *, max(id) from test_namespace"), Error);
	EXPECT_NO_THROW((void)Query::FromSQL("select *, count(*) from test_namespace"));
	EXPECT_NO_THROW((void)Query::FromSQL("select count(*), * from test_namespace"));
}

TEST_F(ReindexerApi, InsertIncorrectItemWithJsonPathsDuplication) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	Item oldTagsItemCJSON(rt.NewItem(default_namespace));
	Item oldTagsItemJSON(rt.NewItem(default_namespace));

	rt.AddIndex(default_namespace, {"value", reindexer::JsonPaths{"value1"}, "hash", "string", IndexOpts()});

	{
		// Check item unmarshalled from json
		Item item(rt.NewItem(default_namespace));
		auto err = item.FromJSON(R"_({"id":0,"value1":"v","obj":{"id":11},"value1":"p"})_");
		EXPECT_EQ(err.code(), errLogic) << err.what();
	}
	{
		// Check item unmarshaled from cjson (with correct tags)
		Item item(rt.NewItem(default_namespace));
		constexpr char cjson[] = {0x06, 0x08, 0x00, 0x12, 0x01, 0x70, 0x12, 0x01, 0x70, 0x07};
		auto err = item.FromCJSON(std::string_view(cjson, sizeof(cjson)));
		ASSERT_EQ(err.code(), errLogic);
	}
	{
		// Check item unmarshaled from msgpack
		Item item(rt.NewItem(default_namespace));
		constexpr uint8_t msgpack[] = {0xDF, 0x00, 0x00, 0x00, 0x04, 0xA2, 0x69, 0x64, 0x00, 0xA6, 0x76, 0x61, 0x6C, 0x75,
									   0x65, 0x31, 0xA1, 0x76, 0xA3, 0x6F, 0x62, 0x6A, 0xDF, 0x00, 0x00, 0x00, 0x01, 0xA2,
									   0x69, 0x64, 0x0B, 0xA6, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x31, 0xA1, 0x70};
		size_t offset = 0;
		auto err = item.FromMsgPack(std::string_view(reinterpret_cast<const char*>(msgpack), sizeof(msgpack)), offset);
		EXPECT_EQ(err.code(), errLogic) << err.what();
	}
	{
		// Check item unmarshalled from msgpack (different encoding)
		Item item(rt.NewItem(default_namespace));
		constexpr uint8_t msgpack[] = {0x84, 0xA2, 0x69, 0x64, 0x00, 0xA6, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x31, 0xA1, 0x70, 0xA3, 0x6F,
									   0x62, 0x6A, 0x81, 0xA2, 0x69, 0x64, 0x0B, 0xA6, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x31, 0xA1, 0x70};
		size_t offset = 0;
		auto err = item.FromMsgPack(std::string_view(reinterpret_cast<const char*>(msgpack), sizeof(msgpack)), offset);
		EXPECT_EQ(err.code(), errLogic) << err.what();
	}

	{
		// Check item unmarshalled from cjson (with outdated tags)
		constexpr char cjson[] = {0x07, 0x13, 0x00, 0x00, 0x00, 0x06, 0x08, 0x00, 0x12, 0x01, 0x76, 0x1E, 0x08, 0x16, 0x07, 0x12, 0x01,
								  0x70, 0x07, 0x03, 0x02, 0x69, 0x64, 0x06, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x31, 0x03, 0x6F, 0x62, 0x6A};
		auto err = oldTagsItemCJSON.FromCJSON(std::string_view(cjson, sizeof(cjson)));
		EXPECT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Insert(default_namespace, oldTagsItemCJSON);
		ASSERT_EQ(err.code(), errLogic);

		auto qr = rt.Select(Query(default_namespace));
		ASSERT_EQ(qr.Count(), 0);
	}
	{
		// Check item unmarshaled from json with outdated tags
		auto err = oldTagsItemJSON.FromJSON(R"_({"id":0,"value1":"v","obj":{"id":11},"value1":"p"})_");
		EXPECT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->Insert(default_namespace, oldTagsItemJSON);
		ASSERT_EQ(err.code(), errLogic);

		auto qr = rt.Select(Query(default_namespace));
		ASSERT_EQ(qr.Count(), 0);
	}
}

TEST_F(ReindexerApi, UpdateDoublesItemByPKIndex) {
	rt.SetVerbose(true);

	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "tree", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"v1", "tree", "int", IndexOpts().Sparse()});
	rt.AddIndex(default_namespace, {"v2", "tree", "string", IndexOpts()});

	struct [[nodiscard]] ItemData {
		ItemData() = default;
		ItemData(unsigned int _id, unsigned int _v1, const std::string& _v2) : id(_id), v1(_v1), v2(_v2) {}
		unsigned int id = 0;
		unsigned int v1 = 0;
		std::string v2;
	};
	constexpr size_t kItemsCount = 4;
	std::vector<ItemData> data;
	for (unsigned i = 0; i < kItemsCount; i++) {
		Item item(rt.NewItem(default_namespace));
		data.emplace_back(ItemData{i, i + 100, RandString()});
		item["id"] = int(data.back().id);
		item["v1"] = int(data.back().v1);
		item["v2"] = data.back().v2;
		rt.Insert(default_namespace, item);
	}

	{
		reindexer::QueryResults qr;
		constexpr std::string_view sql = "UPDATE test_namespace SET v1=125, id = 3 WHERE id = 2";
		Query query = Query::FromSQL(sql);
		auto err = rt.reindexer->Update(query, qr);
		ASSERT_EQ(err.code(), errLogic);
		EXPECT_STREQ(err.what(), "Duplicate Primary Key {id: {3}} for rows [2, 3]!");
	}

	{
		auto qr = rt.Select(Query(default_namespace).Sort("id", false));
		ASSERT_EQ(qr.Count(), kItemsCount);

		unsigned int i = 0;
		for (auto it : qr) {
			Item item(it.GetItem());
			ASSERT_EQ(item["id"].As<int>(), data[i].id);
			ASSERT_EQ(item["v1"].As<int>(), data[i].v1);
			ASSERT_EQ(item["v2"].As<std::string>(), data[i].v2);
			i++;
		}
	}
}

TEST_F(ReindexerApi, IntFieldConvertToStringIndexTest) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	static int id = 0;
	enum class [[nodiscard]] Order { InsertThenAddIndex, AddIndexThenUpdate };

	auto testImpl = [this](Order order) {
		std::srand(std::time(0));
		int value = std::rand();
		auto indexName = fmt::format("data_{}", id);
		auto indexPaths = order == Order::AddIndexThenUpdate ? reindexer::JsonPaths{"n." + indexName} : reindexer::JsonPaths{indexName};
		auto insert = [this]<typename... Args>(fmt::format_string<Args...> tmplt, Args&&... args) {
			Item item(rt.NewItem(default_namespace));
			auto err = item.FromJSON(fmt::format(tmplt, std::forward<decltype(args)>(args)...));
			ASSERT_TRUE(err.ok()) << err.what();
			rt.Insert(default_namespace, item);
		};

		auto update = [&] {
			auto qr = rt.ExecSQL(fmt::format("UPDATE {} SET n = {{\"{}\":{}}} where id = {}", default_namespace, indexName, value, id));
			ASSERT_EQ(qr.Count(), 1);
		};

		auto addIndex = [&] { rt.AddIndex(default_namespace, {indexName, std::move(indexPaths), "hash", "string", IndexOpts()}); };

		auto checkResult = [&](const std::string& searchIndex, const std::string& searchValue) {
			auto qr = rt.Select(Query(default_namespace).Where(searchIndex, CondEq, searchValue));
			ASSERT_EQ(qr.Count(), 1);

			Item item(qr.begin().GetItem());
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			ASSERT_TRUE(Variant(item[indexName]).Type().Is<reindexer::KeyValueType::String>()) << Variant(item[indexName]).Type().Name();
			ASSERT_EQ(item[indexName].As<std::string>(), std::to_string(value));
		};

		switch (order) {
			case Order::InsertThenAddIndex: {
				insert("{{\"id\":{},\"{}\":{}}})", id, indexName, value);
				addIndex();
				break;
			}
			case Order::AddIndexThenUpdate: {
				addIndex();
				insert("{{\"id\":{}}}", id);
				update();
				break;
			}
		}
		checkResult("id", std::to_string(id));
		checkResult(indexName, std::to_string(value));
		id++;
	};

	testImpl(Order::InsertThenAddIndex);
	testImpl(Order::AddIndexThenUpdate);
}

TEST_F(ReindexerApi, MetaIndexTest) {
	const auto storagePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/meta_index_test/");
	std::ignore = reindexer::fs::RmDirAll(storagePath);

	auto rx = std::make_unique<Reindexer>();
	auto err = rx->Connect("builtin://" + storagePath);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rx->OpenNamespace(default_namespace, StorageOpts().Enabled().CreateIfMissing());
	ASSERT_TRUE(err.ok()) << err.what();

	std::string readMeta;
	std::vector<std::string> readKeys;
	const std::string emptyValue;
	const std::string unsettedKey = "unexpected#meta#key##name";
	const std::vector<std::pair<std::string, std::string>> meta = {{"key1", "data1"}, {"key2", "data2"}};

	// prepare state - clear meta in ns
	err = rx->EnumMeta(default_namespace, readKeys);
	ASSERT_TRUE(err.ok()) << err.what();
	for (const auto& key : readKeys) {
		err = rx->DeleteMeta(default_namespace, key);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// empty key to operations
	err = rx->GetMeta(default_namespace, emptyValue, readMeta);
	ASSERT_FALSE(err.ok()) << err.what();

	err = rx->PutMeta(default_namespace, emptyValue, emptyValue);
	ASSERT_FALSE(err.ok()) << err.what();

	err = rx->DeleteMeta(default_namespace, emptyValue);
	ASSERT_FALSE(err.ok()) << err.what();

	// before initialization - read\enum\delete on empty Meta
	readMeta = "DEFAULT";
	err = rx->GetMeta(default_namespace, meta.front().first, readMeta);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(readMeta, emptyValue);

	readKeys.clear();
	err = rx->EnumMeta(default_namespace, readKeys);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(readKeys, std::vector<std::string>{});

	err = rx->DeleteMeta(default_namespace, unsettedKey);
	ASSERT_TRUE(err.ok()) << err.what();

	// initialization - store meta (overwrite first item)
	for (const auto& item : meta) {
		err = rx->PutMeta(default_namespace, item.first, item.second);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// reload ns, check store\load meta from storage
	err = rx->CloseNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rx->OpenNamespace(default_namespace, StorageOpts().Enabled().CreateIfMissing());
	ASSERT_TRUE(err.ok()) << err.what();

	// check values - enumerate all data
	readKeys.clear();
	err = rx->EnumMeta(default_namespace, readKeys);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(readKeys.size(), meta.size());

	// get shard meta
	std::vector<reindexer::ShardedMeta> data;
	err = rx->GetMeta(default_namespace, "key1", data);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(data.size(), 1);
	ASSERT_EQ(data[0].data, "data1");
	ASSERT_EQ(data[0].shardId, ShardingKeyType::NotSetShard);

	for (const auto& key : readKeys) {
		err = rx->GetMeta(default_namespace, key, readMeta);
		ASSERT_TRUE(err.ok()) << err.what();
		auto it =
			std::find_if(meta.begin(), meta.end(), [&key](const std::pair<std::string, std::string>& elem) { return elem.first == key; });
		ASSERT_TRUE(it != meta.end());
		ASSERT_EQ(readMeta, it != meta.end() ? it->second : unsettedKey);
	}

	// deleting
	err = rx->DeleteMeta(default_namespace, unsettedKey);
	ASSERT_TRUE(err.ok()) << err.what();

	for (const auto& item : meta) {
		// first delete
		err = rx->DeleteMeta(default_namespace, item.first);
		ASSERT_TRUE(err.ok()) << err.what();

		// read just removed
		err = rx->GetMeta(default_namespace, item.first, readMeta);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(readMeta, emptyValue);

		// write back just removed
		err = rx->PutMeta(default_namespace, item.first, item.second);
		ASSERT_TRUE(err.ok()) << err.what();

		// real delete
		err = rx->DeleteMeta(default_namespace, item.first);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// enum again
	readKeys.push_back(unsettedKey);
	err = rx->EnumMeta(default_namespace, readKeys);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(readKeys, std::vector<std::string>{});
}

TEST_F(ReindexerApi, QueryResultsLSNTest) {
	using namespace std::string_view_literals;
	constexpr int kDataCount = 10;

	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	for (int i = 0; i < kDataCount; ++i) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = i;
		rt.Upsert(default_namespace, item);
	}

	// Select and save current LSNs
	auto qr = rt.Select(Query(default_namespace));
	ASSERT_EQ(qr.Count(), kDataCount);
	auto& local = qr.ToLocalQr();
	std::vector<reindexer::lsn_t> lsns;
	lsns.reserve(kDataCount);
	for (auto& it : local) {
		auto lsn = it.GetLSN();
		ASSERT_FALSE(lsn.isEmpty());
		lsns.emplace_back(lsn);
	}

	{
		// Modify data in different ways
		const auto updated = rt.Update(Query(default_namespace).Where("id", CondEq, 0).Set("data", {Variant{"modified_0"sv}}));
		ASSERT_EQ(updated, 1);
		const auto deleted = rt.Delete(Query(default_namespace).Where("id", CondEq, 1));
		ASSERT_EQ(deleted, 1);
		Item delItem(rt.NewItem(default_namespace));
		delItem["id"] = 2;
		rt.Delete(default_namespace, delItem);
		rt.UpsertJSON(default_namespace, R"j({"id":3, "data":"modified_3"})j");
	}

	{
		// Modify data via transaction
		auto tx = rt.NewTransaction(default_namespace);

		auto updQ = Query(default_namespace).Where("id", CondEq, 4).Set("data", {Variant{"modified_4"sv}});
		updQ.type_ = QueryUpdate;
		auto err = tx.Modify(std::move(updQ));
		ASSERT_TRUE(err.ok()) << err.what();

		auto delQ = Query(default_namespace).Where("id", CondEq, 5);
		delQ.type_ = QueryDelete;
		err = tx.Modify(std::move(delQ));
		ASSERT_TRUE(err.ok()) << err.what();

		auto delItem = tx.NewItem();
		delItem["id"] = 6;
		err = tx.Delete(std::move(delItem));
		ASSERT_TRUE(err.ok()) << err.what();
		auto updItem = tx.NewItem();
		updItem["id"] = 7;
		updItem["data"] = "modified_7";
		err = tx.Update(std::move(updItem));
		ASSERT_TRUE(err.ok()) << err.what();
		std::ignore = rt.CommitTransaction(tx);
	}

	// Truncate all remaining data
	rt.TruncateNamespace(default_namespace);

	// Check, that LSNs did not change for the existing Qr
	for (size_t i = 0; i < kDataCount; ++i) {
		auto lsn = (local.begin() + i).GetLSN();
		ASSERT_EQ(lsn, lsns[i]) << i;
	}
}

TEST_F(ReindexerApi, SelectNull) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"value", "tree", "string", IndexOpts()});
	rt.AddIndex(default_namespace, {"store", "-", "string", IndexOpts()});
	rt.AddIndex(default_namespace, {"store_num", "-", "string", IndexOpts().Sparse()});
	rt.UpsertJSON(default_namespace, R"_({"id":1234, "value" : "value", "store": "store", "store_num": 10, "not_indexed": null})_");

	for (unsigned i = 0; i < 3; ++i) {
		QueryResults qr;
		auto err = rt.reindexer->Select(Query(default_namespace).Not().Where("id", CondEq, Variant()), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_STREQ(err.what(), "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes");

		qr.Clear();
		err = rt.reindexer->Select(Query(default_namespace).Where("id", CondSet, VariantArray{Variant(1234), Variant()}), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_STREQ(err.what(), "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes");

		EXPECT_THROW(err = rt.reindexer->Select(Query(default_namespace).Where("value", CondLt, Variant()), qr), Error);

		qr.Clear();
		err = rt.reindexer->Select(Query(default_namespace).Where("store", CondEq, Variant()), qr);
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_STREQ(err.what(), "The 'is NULL' condition is supported only by 'sparse' or 'array' indexes");

		qr.Clear();
		rt.Select(Query(default_namespace).Where("store_num", CondSet, Variant()), qr);

		qr.Clear();
		rt.Select(Query(default_namespace).Where("not_indexed", CondSet, VariantArray{Variant(1234), Variant()}), qr);

		AwaitIndexOptimization(default_namespace);
	}
}

TEST_F(ReindexerApi, DefautlIndexDefJSON) {
	const reindexer::IndexDef empty("some_index");
	reindexer::WrSerializer ser;
	ASSERT_NO_THROW(empty.GetJSON(ser));
	const auto newDef = reindexer::IndexDef::FromJSON(ser.Slice());
	ASSERT_TRUE(newDef) << newDef.error().what();
	ASSERT_TRUE(empty.Compare(*newDef).Equal()) << ser.Slice();
}

TEST_F(ReindexerApi, NamespaceStorageRaceTest) {
	const std::string kBaseTestsStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/ns_storage_race_test/");
	std::ignore = reindexer::fs::RmDirAll(kBaseTestsStoragePath);
	auto rx = std::make_unique<Reindexer>();
	auto err = rx->Connect("builtin://" + kBaseTestsStoragePath);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string_view kNamespaces[] = {"ns1", "ns2", "ns3", "ns4"};
	const auto nsCnt = std::distance(std::begin(kNamespaces), std::end(kNamespaces));
	std::atomic<bool> terminate{false};

	auto openCloseFn = [&] {
		while (!terminate.load()) {
			auto targetNs = kNamespaces[rand() % nsCnt];
			auto err = rx->OpenNamespace(targetNs, StorageOpts().Enabled().CreateIfMissing().DropOnFileFormatError(rand() % 2));
			// Should never get errors on Open
			ASSERT_TRUE(err.ok()) << err.what();

			std::this_thread::sleep_for(std::chrono::milliseconds(5));
			targetNs = kNamespaces[rand() % nsCnt];
			if (rand() % 2) {
				err = rx->CloseNamespace(targetNs);
				ASSERT_TRUE(err.ok() || err.code() == errNotFound) << err.what();
			} else {
				err = rx->DropNamespace(targetNs);
			}
		}
	};
	auto addFn = [&] {
		while (!terminate.load()) {
			auto targetNs = kNamespaces[rand() % nsCnt];
			reindexer::NamespaceDef def(targetNs);
			def.storage = StorageOpts().Enabled().CreateIfMissing();
			def.AddIndex("id", "hash", "int", IndexOpts().PK());
			auto err = rx->AddNamespace(def);
			ASSERT_TRUE(err.ok() || err.code() == errParams || err.code() == errNamespaceOverwritten) << err.what();
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	};
	auto renameFn = [&] {
		while (!terminate.load()) {
			auto targetNs1 = kNamespaces[rand() % nsCnt];
			auto targetNs2 = kNamespaces[rand() % nsCnt];
			auto err = rx->RenameNamespace(targetNs1, std::string(targetNs2));
			ASSERT_TRUE(err.ok() || err.code() == errParams) << err.what();
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
	};

	std::vector<std::thread> threads;
	threads.emplace_back(openCloseFn);
	threads.emplace_back(openCloseFn);
	threads.emplace_back(openCloseFn);
	threads.emplace_back(addFn);
	threads.emplace_back(renameFn);
	threads.emplace_back(renameFn);

	std::this_thread::sleep_for(std::chrono::seconds(10));
	terminate.store(true);
	for (auto& th : threads) {
		th.join();
	}
}

TEST_F(ReindexerApi, AlignedPayload) {
	// Tests TSAN's warning with specific payload's alignment.
	// Check issue #2061 for details.
	const std::string kBaseTestsStoragePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/payload_race_test/");
	std::ignore = reindexer::fs::RmDirAll(kBaseTestsStoragePath);
	auto rx = std::make_unique<Reindexer>();
	auto err = rx->Connect("builtin://" + kBaseTestsStoragePath);
	ASSERT_TRUE(err.ok()) << err.what();

	const std::string_view kNsName = "ns";
	const std::string_view kJson =
		R"j({"idx1":"some string content","idx2":13448911603,"idx3":true,"uuid":"d9e8be28-f2e5-11ef-8143-0242ac110005","idx4":448911603})j";
	reindexer::NamespaceDef def(kNsName);
	def.storage = StorageOpts().Enabled().CreateIfMissing();
	def.AddIndex("idx1", "hash", "string");
	def.AddIndex("idx2", "hash", "int64");
	def.AddIndex("idx3", "-", "bool");
	def.AddIndex("uuid", "hash", "uuid", IndexOpts().PK());
	def.AddIndex("idx4", "hash", "int");
	err = rx->AddNamespace(def);
	ASSERT_TRUE(err.ok()) << err.what();

	auto item = rx->NewItem(kNsName);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(kJson);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx->Upsert(kNsName, item);
	ASSERT_TRUE(err.ok()) << err.what();

	rx.reset();

	rx = std::make_unique<Reindexer>();
	err = rx->Connect("builtin://" + kBaseTestsStoragePath);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	err = rx->Select(Query(kNsName), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);
	ASSERT_EQ(qr.begin().GetItem().GetJSON(), kJson);
}

static void DefaultConfigTest(std::string_view configType, ErrorCode expectedCode, std::string_view expectedJson) {
	auto cleanString = [](std::string_view input) {
		std::string result;
		result.reserve(input.size());
		for (char c : input) {
			if (!std::isspace(c)) {
				result.push_back(c);
			}
		}
		return result;
	};

	reindexer::WrSerializer ser;
	reindexer::JsonBuilder jb(ser);

	Error err = reindexer::GetDefaultConfigs(configType, jb);
	ASSERT_EQ(err.code(), expectedCode);
	if (err.ok()) {
		ASSERT_EQ(cleanString(std::string(ser.Slice())), cleanString(expectedJson));
	}
}

TEST_F(ReindexerApi, DefaultsConfigsTest) {
	DefaultConfigTest("profiling", ErrorCode::errOK, reindexer::kDefProfilingConfig);
	DefaultConfigTest("namespaces", ErrorCode::errOK, reindexer::kDefNamespacesConfig);
	DefaultConfigTest("replication", ErrorCode::errOK, reindexer::kDefReplicationConfig);
	DefaultConfigTest("async_replication", ErrorCode::errOK, reindexer::kDefAsyncReplicationConfig);
	DefaultConfigTest("embedders", ErrorCode::errOK, reindexer::kDefEmbeddersConfig);
	DefaultConfigTest("incorrect_type", ErrorCode::errNotFound, "");
}

TEST_F(ReindexerApi, UnableToCallApiWithoutConnect) {
	auto rx = std::make_unique<Reindexer>();

	auto err = rx->Status();
	EXPECT_EQ(err.code(), errNotValid);

	err = rx->OpenNamespace("ns");
	EXPECT_EQ(err.code(), errNotValid);

	err = rx->DropNamespace("ns");
	EXPECT_EQ(err.code(), errNotValid);

	err = rx->CloseNamespace("ns");
	EXPECT_EQ(err.code(), errNotValid);

	err = rx->PutMeta("ns", "key", "value");
	EXPECT_EQ(err.code(), errNotValid);

	auto item = rx->NewItem("ns");
	EXPECT_EQ(item.Status().code(), errNotValid);

	auto tx = rx->NewTransaction("ns");
	EXPECT_EQ(tx.Status().code(), errNotValid);
}
