#include <fstream>
#include <vector>
#include "reindexer_api.h"
#include "tools/errors.h"

#include "core/item.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/reindexer.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

#include <deque>

using reindexer::Reindexer;

TEST_F(ReindexerApi, AddNamespace) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_EQ(true, err.ok()) << err.what();
}

TEST_F(ReindexerApi, AddNamespace_CaseInsensitive) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	string upperNS(default_namespace);
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

TEST_F(ReindexerApi, AddIndex) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, AddIndex_CaseInsensitive) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	string idxName = "IdEnTiFiCaToR";
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

TEST_F(ReindexerApi, AddExistingIndexWithDiffType) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int64", IndexOpts().PK()});
	ASSERT_FALSE(err.ok());
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
	ASSERT_TRUE(item);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
}

TEST_F(ReindexerApi, NewItem_CaseInsensitiveCheck) {
	int idVal = 1000;
	string valueVal = "value";

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
	ASSERT_NO_THROW(ASSERT_EQ(item["value"].As<string>(), valueVal));
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
	Item selItem = qr.begin().GetItem();
	ASSERT_NO_THROW(ASSERT_EQ(selItem["id"].As<int>(), 1234));
	ASSERT_NO_THROW(ASSERT_EQ(selItem["value"].As<string>(), "value"));
}

template <int collateMode>
struct CollateComparer {
	bool operator()(const string& lhs, const string& rhs) const {
		reindexer::string_view sl1(lhs.c_str(), lhs.length());
		reindexer::string_view sl2(rhs.c_str(), rhs.length());
		CollateOpts opts(collateMode);
		return collateCompare(sl1, sl2, opts) < 0;
	}
};

TEST_F(ReindexerApi, SortByMultipleColumns) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"column1", "tree", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"column2", "tree", "string", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"column3", "hash", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	const std::vector<string> possibleValues = {
		"apple",	 "arrangment", "agreement", "banana",   "bull",  "beech", "crocodile", "crucifix", "coat",	 "day",
		"dog",		 "deer",	   "easter",	"ear",		"eager", "fair",  "fool",	  "foot",	 "genes",	"genres",
		"greatness", "hockey",	 "homeless",  "homocide", "key",   "kit",   "knockdown", "motion",   "monument", "movement"};

	int sameOldValue = 0;
	int stringValuedIdx = 0;
	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		EXPECT_TRUE(item);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;
		item["column1"] = sameOldValue;
		item["column2"] = possibleValues[stringValuedIdx];
		item["column3"] = rand() % 30;

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		if (i % 5 == 0) sameOldValue += 5;
		if (i % 3 == 0) ++stringValuedIdx;
		stringValuedIdx %= possibleValues.size();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	const size_t offset = 23;
	const size_t limit = 61;

	QueryResults qr;
	Query query = Query(default_namespace, offset, limit).Sort("column1", true).Sort("column2", false).Sort("column3", false);
	err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() == limit);

	PrintQueryResults(default_namespace, qr);

	vector<Variant> lastValues(query.sortingEntries_.size());
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();

		std::vector<int> cmpRes(query.sortingEntries_.size());
		std::fill(cmpRes.begin(), cmpRes.end(), -1);

		for (size_t j = 0; j < query.sortingEntries_.size(); ++j) {
			const reindexer::SortingEntry& sortingEntry(query.sortingEntries_[j]);
			Variant sortedValue = item[sortingEntry.column];
			if (lastValues[j].Type() != KeyValueNull) {
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
						<< "\nSort order is incorrect for column: " << sortingEntry.column << "; rowID: " << item[1].As<int>();
				}
			}
		}
	}

	// Check sql parser work correctness
	QueryResults qrSql;
	string sqlQuery = ("select * from test_namespace order by column2 asc, column3 desc");
	err = rt.reindexer->Select(sqlQuery, qrSql);
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, SortByMultipleColumnsWithLimits) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"f1", "tree", "string", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"f2", "tree", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	const vector<string> srcStrValues = {
		"A", "A", "B", "B", "B", "C", "C",
	};
	const vector<int> srcIntValues = {1, 2, 4, 3, 5, 7, 6};

	for (size_t i = 0; i < srcIntValues.size(); ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		EXPECT_TRUE(item);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = static_cast<int>(i);
		item["f1"] = srcStrValues[i];
		item["f2"] = srcIntValues[i];

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	const size_t offset = 4;
	const size_t limit = 3;

	QueryResults qr;
	Query query = Query(default_namespace, offset, limit).Sort("f1", false).Sort("f2", false);
	err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() == limit);

	const std::vector<int> properRes = {5, 6, 7};
	for (size_t i = 0; i < qr.Count(); ++i) {
		Item item = qr[i].GetItem();
		Variant kr = item["f2"];
		EXPECT_TRUE(static_cast<int>(kr) == properRes[i]);
	}
}

TEST_F(ReindexerApi, SortByUnorderedIndexes) {
	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueInt", "hash", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueString", "hash", "string", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueStringASCII", "hash", "string", IndexOpts().SetCollateMode(CollateASCII)});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueStringNumeric", "hash", "string", IndexOpts().SetCollateMode(CollateNumeric)});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"valueStringUTF8", "hash", "string", IndexOpts().SetCollateMode(CollateUTF8)});
	EXPECT_TRUE(err.ok()) << err.what();

	deque<int> allIntValues;
	std::set<string> allStrValues;
	std::set<string, CollateComparer<CollateASCII>> allStrValuesASCII;
	std::set<string, CollateComparer<CollateNumeric>> allStrValuesNumeric;
	std::set<string, CollateComparer<CollateUTF8>> allStrValuesUTF8;

	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		EXPECT_TRUE(item);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;
		item["valueInt"] = i;
		allIntValues.push_front(i);

		string strCollateNone = RandString().c_str();
		allStrValues.insert(strCollateNone);
		item["valueString"] = strCollateNone;

		string strASCII(strCollateNone + "ASCII");
		allStrValuesASCII.insert(strASCII);
		item["valueStringASCII"] = strASCII;

		string strNumeric(std::to_string(i + 1));
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
	Query sortByIntQuery = Query(default_namespace, offset, limit).Sort("valueInt", descending);
	err = rt.reindexer->Select(sortByIntQuery, sortByIntQr);
	EXPECT_TRUE(err.ok()) << err.what();

	deque<int> selectedIntValues;
	for (auto it : sortByIntQr) {
		Item item(it.GetItem());
		int value = item["valueInt"].Get<int>();
		selectedIntValues.push_back(value);
	}

	EXPECT_TRUE(std::equal(allIntValues.begin() + offset, allIntValues.begin() + offset + limit, selectedIntValues.begin()));

	QueryResults sortByStrQr, sortByASCIIStrQr, sortByNumericStrQr, sortByUTF8StrQr;
	Query sortByStrQuery = Query(default_namespace).Sort("valueString", !descending);
	Query sortByASSCIIStrQuery = Query(default_namespace).Sort("valueStringASCII", !descending);
	Query sortByNumericStrQuery = Query(default_namespace).Sort("valueStringNumeric", !descending);
	Query sortByUTF8StrQuery = Query(default_namespace).Sort("valueStringUTF8", !descending);

	err = rt.reindexer->Select(sortByStrQuery, sortByStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Select(sortByASSCIIStrQuery, sortByASCIIStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Select(sortByNumericStrQuery, sortByNumericStrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Select(sortByUTF8StrQuery, sortByUTF8StrQr);
	EXPECT_TRUE(err.ok()) << err.what();

	auto collectQrStringFieldValues = [](const QueryResults& qr, const char* fieldName, vector<string>& selectedStrValues) {
		selectedStrValues.clear();
		for (auto it : qr) {
			Item item(it.GetItem());
			selectedStrValues.push_back(item[fieldName].As<string>());
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
	const string secondNamespace = "test_namespace_2";
	vector<int> secondNamespacePKs;

	auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"fk", "hash", "int", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	{
		err = rt.reindexer->OpenNamespace(secondNamespace, StorageOpts().Enabled(false));
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->AddIndex(secondNamespace, {"pk", "hash", "int", IndexOpts().PK()});
		EXPECT_TRUE(err.ok()) << err.what();

		for (int i = 0; i < 50; ++i) {
			Item item(rt.reindexer->NewItem(secondNamespace));
			EXPECT_TRUE(item);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			secondNamespacePKs.push_back(i);
			item["pk"] = i;

			err = rt.reindexer->Upsert(secondNamespace, item);
			EXPECT_TRUE(err.ok()) << err.what();
		}

		err = rt.reindexer->Commit(secondNamespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	for (int i = 0; i < 100; ++i) {
		Item item(rt.reindexer->NewItem(default_namespace));
		EXPECT_TRUE(item);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		item["id"] = i;

		int fk = secondNamespacePKs[rand() % (secondNamespacePKs.size() - 1)];
		item["fk"] = fk;

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	bool descending = true;
	const unsigned offset = 10;
	const unsigned limit = 40;

	Query querySecondNamespace = Query(secondNamespace);
	Query sortQuery = Query(default_namespace, offset, limit).Sort("id", descending);
	Query joinQuery = sortQuery.InnerJoin("fk", "pk", CondEq, querySecondNamespace);

	QueryResults queryResult;
	err = rt.reindexer->Select(joinQuery, queryResult);
	EXPECT_TRUE(err.ok()) << err.what();

	for (auto it : queryResult) {
		const reindexer::QRVector& jres = it.GetJoined();
		EXPECT_TRUE(!jres.empty());
	}
}

void TestDSLParseCorrectness(const string& testDsl) {
	Query query;
	Error err = query.ParseJson(testDsl);
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(ReindexerApi, DslFieldsTest) {
	TestDSLParseCorrectness(R"xxx({"join_queries": [{
                                    "type": "inner",
                                    "op": "AND",
                                    "namespace": "test1",
                                    "filters": [{
                                        "Op": "",
                                        "Field": "id",
                                        "Cond": "SET",
                                        "Value": [81204872, 101326571, 101326882]
                                    }],
                                    "sort": {
                                        "field": "test1",
                                        "desc": true
                                    },
                                    "limit": 3,
                                    "offset": 0,
                                    "on": [{
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
                                },
                                {
                                    "type": "left",
                                    "op": "OR",
                                    "namespace": "test2",
                                    "filters": [{
                                        "Op": "",
                                        "Field": "id2",
                                        "Cond": "SET",
                                        "Value": [81204872, 101326571, 101326882]
                                    }],
                                    "sort": {
                                        "field": "test2",
                                        "desc": true
                                    },
                                    "limit": 4,
                                    "offset": 5,
                                    "on": [{
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
                            ]
                        })xxx");

	TestDSLParseCorrectness(R"xxx({"merge_queries": [{
                                    "namespace": "services",
                                    "offset": 0,
                                    "limit": 3,
                                    "distinct": "",
                                    "sort": {
                                        "field": "",
                                        "desc": true
                                    },
                                    "filters": [{
                                        "Op": "",
                                        "Field": "id",
                                        "Cond": "SET",
                                        "Value": [81204872, 101326571, 101326882]
                                    }]
                                },
                                {
                                    "namespace": "services",
                                    "offset": 1,
                                    "limit": 5,
                                    "distinct": "",
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
	TestDSLParseCorrectness(R"xxx({"select_filter": ["f1", "f2", "f3", "f4", "f5"]})xxx");
	TestDSLParseCorrectness(R"xxx({"select_functions": ["f1()", "f2()", "f3()", "f4()", "f5()"]})xxx");
	TestDSLParseCorrectness(R"xxx({"req_total":"cached"})xxx");
	TestDSLParseCorrectness(R"xxx({"req_total":"enabled"})xxx");
	TestDSLParseCorrectness(R"xxx({"req_total":"disabled"})xxx");
	TestDSLParseCorrectness(R"xxx({"aggregations":[{"field":"field1", "type":"sum"}, {"field":"field2", "type":"avg"}]})xxx");
}
