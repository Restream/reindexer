#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <tuple>

#include <iostream>
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/query/query.h"
#include "core/query/querywhere.h"
#include "core/reindexer.h"
#include "gtests/tests/gtest_cout.h"
#include "tools/errors.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

using namespace std;
using reindexer::Error;
using reindexer::Item;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::Query;
using reindexer::QueryEntry;
using reindexer::QueryResults;
using reindexer::Reindexer;
using reindexer::make_key_string;
using reindexer::p_string;
using reindexer::QueryJoinEntry;

class ReindexerApi : public ::testing::Test {
protected:
	void SetUp() { reindexer.reset(new Reindexer); }

	void TearDown() {}

	typedef tuple<const char *, const char *, const char *, IndexOpts> IndexDeclaration;

public:
	ReindexerApi() { reindexer = make_shared<Reindexer>(); }

	void DefineNamespaceDataset(const string &ns,
								initializer_list<const tuple<const char *, const char *, const char *, const IndexOpts>> fields) {
		auto err = Error();
		for (auto field : fields) {
			string indexName = get<0>(field);
			string fieldType = get<1>(field);
			string indexType = get<2>(field);

			if (indexType != "composite") {
				err = reindexer->AddIndex(ns, {indexName, {indexName}, fieldType, indexType, get<3>(field)});
			} else {
				string realName = indexName;
				string idxContents = indexName;
				auto eqPos = indexName.find_first_of('=');
				if (eqPos != string::npos) {
					idxContents = indexName.substr(0, eqPos);
					realName = indexName.substr(eqPos + 1);
				}
				reindexer::JsonPaths jsonPaths;
				jsonPaths = reindexer::split(idxContents, "+", true, jsonPaths);

				err = reindexer->AddIndex(ns, {realName, jsonPaths, fieldType, indexType, get<3>(field)});
			}
			ASSERT_TRUE(err.ok()) << err.what();
		}
		err = reindexer->Commit(ns);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	Item NewItem(const std::string &ns) { return reindexer->NewItem(ns); }

	Error Commit(const std::string &ns) { return reindexer->Commit(ns); }
	void Upsert(const std::string &ns, Item &item) {
		assert(item);
		auto err = reindexer->Upsert(ns, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void PrintQueryResults(const std::string &ns, const QueryResults &res) {
		if (!verbose) return;
		{
			Item rdummy(reindexer->NewItem(ns));
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += rdummy[idx].Name();
			}
			TestCout() << outBuf << std::endl;
		}

		for (auto it : res) {
			Item ritem(it.GetItem());
			std::string outBuf = "";
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += ritem[idx].As<string>();
			}
			TestCout() << outBuf << std::endl;
		}
		TestCout() << std::endl;
	}

	std::string RandString() {
		string res;
		uint8_t len = rand() % 20 + 4;
		res.resize(len);
		for (int i = 0; i < len; ++i) {
			int f = rand() % letters.size();
			res[i] = letters[f];
		}
		return res;
	}
	std::string RuRandString() {
		string res;
		uint8_t len = rand() % 20 + 4;
		res.resize(len);
		for (int i = 0; i < len; ++i) {
			int f = rand() % ru_letters.size();
			res[i] = ru_letters[f];
		}
		return res;
	}
	vector<int> RandIntVector(size_t size, int start, int range) {
		vector<int> vec;
		vec.reserve(size);
		for (size_t i = 0; i < size; ++i) {
			vec.push_back(start + rand() % range);
		}
		return vec;
	}

public:
	const string default_namespace = "test_namespace";
	const string letters = "abcdefghijklmnopqrstuvwxyz";
	const string ru_letters = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя";

	shared_ptr<Reindexer> reindexer;
	bool verbose = false;
};
