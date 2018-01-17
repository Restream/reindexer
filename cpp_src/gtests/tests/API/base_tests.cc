#include <fstream>
#include <vector>
#include "reindexer_api.h"
#include "tools/errors.h"

#include "core/item.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/keyvalue.h"
#include "core/reindexer.h"
#include "tools/stringstools.h"

#include <deque>

using reindexer::Reindexer;

TEST_F(ReindexerApi, AddNamespace) {
	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	ASSERT_EQ(true, err.ok());
}

TEST_F(ReindexerApi, AddExistingNamespace) {
	CreateNamespace(default_namespace);

	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	ASSERT_FALSE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, AddIndex) {
	IndexOpts opts = {false, true, false};  // IsArray = false, IsPK = true

	CreateNamespace(default_namespace);

	auto err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, AddExistingIndex) {
	IndexOpts opts = {false, true, false};  // IsArray = false, IsPK = true

	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what() << err.what();
}

TEST_F(ReindexerApi, AddExistingIndexWithDiffType) {
	IndexOpts opts = {false, true, false};  // IsArray = false, IsPK = true

	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int64", opts});
	ASSERT_FALSE(err.ok());
}

TEST_F(ReindexerApi, CloneNamespace) {
	CreateNamespace(default_namespace);

	auto err = reindexer->CloneNamespace(default_namespace, default_namespace + "new");
	ASSERT_TRUE(err.ok());
}

TEST_F(ReindexerApi, CloneNonExistingNamespace) {
	auto err = reindexer->CloneNamespace(default_namespace, default_namespace + "new");
	ASSERT_FALSE(err.ok()) << "Error: unexpected result of clone non-existing namespace.";
}

TEST_F(ReindexerApi, CloneSameNamespaces) {
	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	err = reindexer->CloneNamespace(default_namespace, default_namespace);
	ASSERT_FALSE(err.ok()) << "Error: unexpected result of clone same namespaces.";
}

TEST_F(ReindexerApi, DeleteNamespace) {
	CreateNamespace(default_namespace);

	auto err = reindexer->CloseNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, DeleteNonExistingNamespace) {
	auto err = reindexer->CloseNamespace(default_namespace);
	ASSERT_FALSE(err.ok()) << "Error: unexpected result of delete non-existing namespace.";
}

TEST_F(ReindexerApi, NewItem) {
	IndexOpts opts = {false, true, false};  // IsArray = false, IsPK = true
	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));

	//	DefineNamespaceDataset(
	//				default_namespace,
	//				{
	//					{"id", IndexIntHash, &opts},
	//					{"value", IndexFullText, nullptr}
	//				});

	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->AddIndex(default_namespace, {"value", "", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
	auto item = reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item != nullptr);
	ASSERT_TRUE(item->Status().ok()) << item->Status().what();
}

TEST_F(ReindexerApi, Insert) {
	IndexOpts opts = {false, true, false};  // IsArray = false, IsPK = true
	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));

	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->AddIndex(default_namespace, {"value", "", "text", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();
	auto item = reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item != nullptr);
	ASSERT_TRUE(item->Status().ok()) << item->Status().what();

	// Set field 'id'
	reindexer::KeyRefs refs;
	reindexer::KeyRef id(1);
	refs.push_back(id);
	err = item->SetField("id", refs);
	ASSERT_TRUE(err.ok()) << err.what();

	// Set field 'value'
	refs.clear();
	reindexer::p_string p("value of item");
	reindexer::KeyRef value(p);
	// reindexer::KeyRef value(make_shared<string>("value of field"));
	refs.push_back(value);
	err = item->SetField("value", refs);
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->Insert(default_namespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, DISABLED_DslSetOrder) {
	IndexOpts opts = {false, true, false};  // IsArray = false, IsPK = true

	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));

	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->AddIndex(default_namespace, {"id", "", "tree", "int", opts});
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer->AddIndex(default_namespace, {"value", "", "hash", "string", IndexOpts()});
	ASSERT_TRUE(err.ok()) << err.what();

	//	DefineNamespaceDataset(default_namespace, {
	//		{"id", IndexIntBTree, &opts},
	//		{"value", IndexStrHash, &arrOpts}
	//	});

	{
		auto item = reindexer->NewItem(default_namespace);
		ASSERT_TRUE(item != nullptr);
		ASSERT_TRUE(item->Status().ok()) << item->Status().what();

		reindexer::KeyRefs ids;
		reindexer::KeyRef id(static_cast<int>(3));
		ids.push_back(id);
		err = item->SetField("id", ids);
		ASSERT_TRUE(err.ok()) << err.what();

		reindexer::KeyRefs values;

		auto str = reindexer::make_key_string("val3");

		reindexer::KeyRef val(str);
		values.push_back(val);
		err = item->SetField("value", values);
		ASSERT_TRUE(err.ok()) << err.what();

		err = reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();

		delete item;
	}

	{
		auto item = reindexer->NewItem(default_namespace);
		ASSERT_TRUE(item != nullptr);
		ASSERT_TRUE(item->Status().ok()) << item->Status().what();

		reindexer::KeyRefs ids;
		reindexer::KeyRef id(static_cast<int>(2));
		ids.push_back(id);
		err = item->SetField("id", ids);
		ASSERT_TRUE(err.ok()) << err.what();

		reindexer::KeyRefs values;

		auto str = reindexer::make_key_string("val2");

		reindexer::KeyRef val(str);
		values.push_back(val);

		err = item->SetField("value", values);
		ASSERT_TRUE(err.ok()) << err.what();

		std::cout << item->GetJSON().data() << std::endl;

		err = reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();

		delete item;
	}

	{
		auto item = reindexer->NewItem(default_namespace);
		ASSERT_TRUE(item != nullptr);
		ASSERT_TRUE(item->Status().ok()) << item->Status().what();

		reindexer::KeyRefs ids;
		reindexer::KeyRef id(static_cast<int>(1));
		ids.push_back(id);
		err = item->SetField("id", ids);
		ASSERT_TRUE(err.ok()) << err.what();

		reindexer::KeyRefs values;
		auto str = reindexer::make_key_string("val1");

		reindexer::KeyRef val(str);
		values.push_back(val);

		err = item->SetField("value", values);
		ASSERT_TRUE(err.ok()) << err.what();

		std::cout << item->GetJSON().data() << std::endl;

		err = reindexer->Upsert(default_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();

		delete item;
	}

	err = reindexer->Commit(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	std::ifstream t("/Users/viktor/Desktop/test_dsl_set_order.json");

	ASSERT_TRUE(t.is_open());

	std::string json1((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());

	reindexer::Query q;
	err = q.ParseJson(json1);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::QueryResults r;

	err = reindexer->Select(q, r);
	ASSERT_TRUE(err.ok()) << err.what();

	PrintQueryResults(default_namespace, r);
}

template <int collateMode>
struct CollateComparer {
	bool operator()(const string& lhs, const string& rhs) const {
		reindexer::Slice sl1(lhs.c_str(), lhs.length());
		reindexer::Slice sl2(rhs.c_str(), rhs.length());
		return collateCompare(sl1, sl2, collateMode) < 0;
	}
};

TEST_F(ReindexerApi, SortByUnorderedIndexes) {
	IndexOpts opts = {false, true, false};

	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"valueInt", "", "hash", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"valueString", "", "hash", "string", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace,
							  {"valueStringASCII", "", "hash", "string", IndexOpts(false, false, false, false, CollateASCII)});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace,
							  {"valueStringNumeric", "", "hash", "string", IndexOpts(false, false, false, false, CollateNumeric)});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace,
							  {"valueStringUTF8", "", "hash", "string", IndexOpts(false, false, false, false, CollateUTF8)});
	EXPECT_TRUE(err.ok()) << err.what();

	deque<int> allIntValues;
	std::set<string> allStrValues;
	std::set<string, CollateComparer<CollateASCII>> allStrValuesASCII;
	std::set<string, CollateComparer<CollateNumeric>> allStrValuesNumeric;
	std::set<string, CollateComparer<CollateUTF8>> allStrValuesUTF8;
	for (int i = 0; i < 100; ++i) {
		auto item = reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item != nullptr);
		EXPECT_TRUE(item->Status().ok()) << item->Status().what();

		AddData(default_namespace, "id", i, item);
		AddData(default_namespace, "valueInt", i, item);
		allIntValues.push_front(i);

		string strCollateNone = RandString().c_str();
		AddData(default_namespace, "valueString", strCollateNone, item);
		allStrValues.insert(strCollateNone);

		string strASCII(strCollateNone + "ASCII");
		AddData(default_namespace, "valueStringASCII", strASCII, item);
		allStrValuesASCII.insert(strASCII);

		string strNumeric(std::to_string(i + 1));
		AddData(default_namespace, "valueStringNumeric", strNumeric, item);
		allStrValuesNumeric.insert(strNumeric);

		AddData(default_namespace, "valueStringUTF8", strCollateNone, item);
		allStrValuesUTF8.insert(strCollateNone);

		err = reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		delete item;
	}

	err = reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	bool descending = true;
	const unsigned offset = 5;
	const unsigned limit = 30;

	QueryResults sortByIntQr;
	Query sortByIntQuery = Query(default_namespace, offset, limit).Sort("valueInt", descending);
	err = reindexer->Select(sortByIntQuery, sortByIntQr);
	EXPECT_TRUE(err.ok()) << err.what();

	deque<int> selectedIntValues;
	for (size_t i = 0; i < sortByIntQr.size(); ++i) {
		std::unique_ptr<reindexer::Item> item(sortByIntQr.GetItem(static_cast<int>(i)));
		auto ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());
		KeyRef value = ritem->GetField("valueInt");
		selectedIntValues.push_back(static_cast<int>(value));
	}

	EXPECT_TRUE(std::equal(allIntValues.begin() + offset, allIntValues.begin() + limit, selectedIntValues.begin()));

	QueryResults sortByStrQr, sortByASCIIStrQr, sortByNumericStrQr, sortByUTF8StrQr;
	Query sortByStrQuery = Query(default_namespace).Sort("valueString", !descending);
	Query sortByASSCIIStrQuery = Query(default_namespace).Sort("valueStringASCII", !descending);
	Query sortByNumericStrQuery = Query(default_namespace).Sort("valueStringNumeric", !descending);
	Query sortByUTF8StrQuery = Query(default_namespace).Sort("valueStringUTF8", !descending);

	err = reindexer->Select(sortByStrQuery, sortByStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->Select(sortByASSCIIStrQuery, sortByASCIIStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->Select(sortByNumericStrQuery, sortByNumericStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->Select(sortByUTF8StrQuery, sortByUTF8StrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	auto collectQrStringFieldValues = [](const QueryResults& qr, const char* fieldName, vector<string>& selectedStrValues) {
		selectedStrValues.clear();
		for (size_t i = 0; i < qr.size(); ++i) {
			std::unique_ptr<reindexer::Item> item(qr.GetItem((int)i));
			auto ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());
			KeyRef value = ritem->GetField(fieldName);
			selectedStrValues.push_back(*value.operator p_string().getCxxstr());
		}
	};

	vector<string> selectedStrValues;
	auto itSortedStr(allStrValues.begin());
	collectQrStringFieldValues(sortByStrQr, "valueString", selectedStrValues);
	for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
		EXPECT_EQ(*it, *itSortedStr++);
	}

	itSortedStr = allStrValuesASCII.begin();
	collectQrStringFieldValues(sortByASCIIStrQr, "valueStringASCII", selectedStrValues);
	for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
		EXPECT_EQ(*it, *itSortedStr++);
	}

	auto itSortedNumericStr = allStrValuesNumeric.cbegin();
	collectQrStringFieldValues(sortByNumericStrQr, "valueStringNumeric", selectedStrValues);
	for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
		EXPECT_EQ(*it, *itSortedNumericStr++);
	}

	itSortedStr = allStrValuesUTF8.cbegin();
	collectQrStringFieldValues(sortByUTF8StrQr, "valueStringUTF8", selectedStrValues);
	for (auto it = selectedStrValues.begin(); it != selectedStrValues.end(); ++it) {
		EXPECT_EQ(*it, *itSortedStr++);
	}
}

TEST_F(ReindexerApi, SortByUnorderedIndexWithJoins) {
	IndexOpts opts = {false, true, false};
	const string secondNamespace = "test_namespace_2";
	vector<int> secondNamespacePKs;

	auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(default_namespace, false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", opts});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"fk", "", "hash", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	{
		err = reindexer->OpenNamespace(reindexer::NamespaceDef(secondNamespace, false));
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->AddIndex(secondNamespace, {"pk", "", "hash", "int", opts});
		EXPECT_TRUE(err.ok()) << err.what();

		for (int i = 0; i < 50; ++i) {
			auto item = reindexer->NewItem(secondNamespace);
			EXPECT_TRUE(item != nullptr);
			EXPECT_TRUE(item->Status().ok()) << item->Status().what();

			reindexer::KeyRefs ids;
			reindexer::KeyRef id(static_cast<int>(i));
			ids.push_back(id);
			secondNamespacePKs.push_back(i);
			err = item->SetField("pk", ids);
			EXPECT_TRUE(err.ok()) << err.what();

			err = reindexer->Upsert(secondNamespace, item);
			EXPECT_TRUE(err.ok()) << err.what();

			delete item;
		}

		err = reindexer->Commit(secondNamespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	for (size_t i = 0; i < 100; ++i) {
		auto item = reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item != nullptr);
		EXPECT_TRUE(item->Status().ok()) << item->Status().what();

		reindexer::KeyRefs ids;
		reindexer::KeyRef id(static_cast<int>(i));
		ids.push_back(id);
		err = item->SetField("id", ids);
		EXPECT_TRUE(err.ok()) << err.what();

		int fk = secondNamespacePKs[rand() % (secondNamespacePKs.size() - 1)];
		reindexer::KeyRef value(static_cast<int>(fk));
		reindexer::KeyRefs values;
		values.push_back(value);

		err = item->SetField("fk", values);
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		delete item;
	}

	err = reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	bool descending = true;
	const unsigned offset = 10;
	const unsigned limit = 40;

	Query querySecondNamespace = Query(secondNamespace);
	Query sortQuery = Query(default_namespace, offset, limit).Sort("id", descending);
	Query joinQuery = sortQuery.InnerJoin("fk", "pk", CondEq, querySecondNamespace);

	QueryResults queryResult;
	err = reindexer->Select(joinQuery, queryResult);
	EXPECT_TRUE(err.ok()) << err.what();

	for (size_t i = 0; i < queryResult.size(); ++i) {
		const reindexer::ItemRef& itemRef = queryResult[i];
		auto itFind(queryResult.joined_.find(itemRef.id));
		EXPECT_TRUE(itFind != queryResult.joined_.end());
	}
}
