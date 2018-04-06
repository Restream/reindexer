#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <tuple>

#include "core/keyvalue/key_string.h"
#include "core/keyvalue/keyvalue.h"
#include "core/query/query.h"
#include "core/query/querywhere.h"
#include "core/reindexer.h"
#include "gtests/tests/gtest_cout.h"
#include "iostream"
#include "tools/errors.h"
using namespace std;
using reindexer::Error;
using reindexer::Item;
using reindexer::KeyRef;
using reindexer::KeyRefs;
using reindexer::KeyValue;
using reindexer::KeyValues;
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
	void CreateNamespace(const std::string &ns) {
		auto err = reindexer->OpenNamespace(ns, StorageOpts().Enabled());
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DefineNamespaceDataset(const string &ns,
								initializer_list<const tuple<const char *, const char *, const char *, const IndexOpts>> fields) {
		auto err = Error();
		for (auto field : fields) {
			err = reindexer->AddIndex(ns, {get<0>(field), get<0>(field), get<1>(field), get<2>(field), get<3>(field)});
			ASSERT_TRUE(err.ok()) << err.what();
		}
		err = reindexer->Commit(ns);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	Item NewItem(const std::string &ns) { return reindexer->NewItem(ns); }

	void Commit(const std::string &ns) { reindexer->Commit(ns); }
	void Upsert(const std::string &ns, Item &item) {
		assert(item);
		auto err = reindexer->Upsert(ns, item);
		assertf(err.ok(), "%s", err.what().c_str());
	}

	void PrintQueryResults(const std::string &ns, const QueryResults &res) {
		{
			Item rdummy(reindexer->NewItem(ns));
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += rdummy[idx].Name();
			}
			TestCout() << outBuf << std::endl;
		}

		for (size_t i = 0; i < res.size(); ++i) {
			Item ritem(res.GetItem(i));
			std::string outBuf = "";
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				auto field = ritem[idx].Name();
				outBuf += "\t";
				outBuf += ritem[field].As<string>();
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
	shared_ptr<Reindexer> reindexer;
};
