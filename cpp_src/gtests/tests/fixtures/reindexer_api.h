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
	void FillQuery(const std::string &ns, const string &data, const std::string &index, OpType op, CondType cond, Query &qr) {
		QueryEntry entry;
		entry.index = index;
		entry.condition = cond;
		entry.op = op;
		entry.values.push_back(KeyValue(make_key_string(data)));
		qr._namespace = ns;
		qr.entries.push_back(entry);
	}

	template <class T>
	void FillQuery(const std::string &ns, T data, const std::string &index, OpType op, CondType cond, Query &qr) {
		QueryEntry entry;
		entry.index = index;
		entry.condition = cond;
		entry.op = op;
		entry.values.push_back(KeyValue(data));
		qr._namespace = ns;
		qr.entries.push_back(entry);
	}

	ReindexerApi() { reindexer = make_shared<Reindexer>(); }
	void CreateNamespace(const std::string &ns) {
		auto err = reindexer->OpenNamespace(reindexer::NamespaceDef(ns, false));
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void DefineNamespaceDataset(const string &ns,
								initializer_list<const tuple<const char *, const char *, const char *, const IndexOpts>> fields) {
		auto err = Error();
		for (auto field : fields) {
			err = reindexer->AddIndex(ns, {get<0>(field), "", get<1>(field), get<2>(field), get<3>(field)});
			ASSERT_TRUE(err.ok()) << err.what();
		}
		err = reindexer->Commit(ns);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	Item *AddData(const std::string &ns, const string &index, const string &data, Item *item = nullptr) {
		if (!item) {
			item = reindexer->NewItem(ns);
		}
		assert(item);
		assert(item->Status().ok());
		cache_.push_back(data);
		KeyRefs refs;
		refs.push_back(KeyRef{p_string(&cache_.back())});
		auto err = item->SetField(index, refs);
		assert(err.ok());
		return item;
	}
	void Commit(const std::string &ns) { reindexer->Commit(ns); }

	template <class T>
	Item *AddData(const std::string &ns, const string &index, const T data, Item *item = nullptr) {
		KeyRefs refs;
		refs.push_back(KeyRef{data});
		return AddData(ns, index, refs, item);
	}
	template <class T>
	Item *AddData(const std::string &ns, const string &index, const vector<T> data, Item *item = nullptr) {
		KeyRefs refs;
		for (const T &item : data) {
			refs.push_back(KeyRef{item});
		}
		return AddData(ns, index, refs, item);
	}
	Item *AddData(const std::string &ns, const string &index, const KeyRefs &refs, Item *item = nullptr) {
		if (!item) {
			item = reindexer->NewItem(ns);
		}
		assert(item);
		assert(item->Status().ok());
		auto err = item->SetField(index, refs);
		assert(err.ok());
		return item;
	}
	void Upsert(const std::string &ns, Item *item) {
		assert(item);
		auto err = reindexer->Upsert(ns, item);
		assert(err.ok());
	}

	void PrintQueryResults(const std::string &ns, const QueryResults &res) {
		{
			auto dummy = reindexer->NewItem(ns);
			auto rdummy = reinterpret_cast<reindexer::ItemImpl *>(dummy);
			std::string outBuf;
			for (auto idx = 1; idx < rdummy->NumFields(); idx++) {
				outBuf += "\t";
				outBuf += rdummy->Type().Field(idx).Name();
			}
			TestCout() << outBuf << std::endl;
			delete dummy;
			rdummy = nullptr;
		}

		for (size_t i = 0; i < res.size(); ++i) {
			std::unique_ptr<reindexer::Item> item(res.GetItem(i));
			auto ritem = reinterpret_cast<reindexer::ItemImpl *>(item.get());
			std::string outBuf = "";
			for (auto idx = 1; idx < ritem->NumFields(); idx++) {
				auto field = ritem->Type().Field(idx).Name();
				outBuf += "\t";
				outBuf += reindexer::KeyValue(ritem->GetField(field)).toString();
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
	list<std::string> cache_;
	const string default_namespace = "test_namespace";
	const string letters = "abcdefghijklmnopqrstuvwxyz";
	shared_ptr<Reindexer> reindexer;
};
