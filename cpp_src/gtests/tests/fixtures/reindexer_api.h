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
#include "vendor/utf8cpp/utf8.h"

using reindexer::Error;
using reindexer::Item;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::Query;
using reindexer::QueryEntry;
using reindexer::QueryResults;
using reindexer::Reindexer;

typedef std::tuple<const char *, const char *, const char *, IndexOpts, int64_t> IndexDeclaration;

template <typename DB>
class ReindexerTestApi {
public:
	using ItemType = typename DB::ItemT;

	ReindexerTestApi() : reindexer(std::shared_ptr<DB>(new DB)) {}
	void DefineNamespaceDataset(const string &ns, std::initializer_list<const IndexDeclaration> fields) {
		auto err = Error();
		for (auto field : fields) {
			string indexName = std::get<0>(field);
			string fieldType = std::get<1>(field);
			string indexType = std::get<2>(field);
			int64_t expireAfter = std::get<4>(field);

			if (indexType != "composite") {
				err = reindexer->AddIndex(ns, {indexName, {indexName}, fieldType, indexType, std::get<3>(field)});
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

				err = reindexer->AddIndex(ns, {realName, jsonPaths, fieldType, indexType, std::get<3>(field), expireAfter});
			}
			ASSERT_TRUE(err.ok()) << err.what();
		}
		err = reindexer->Commit(ns);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	ItemType NewItem(const std::string &ns) { return reindexer->NewItem(ns); }
	Error Commit(const std::string &ns) { return reindexer->Commit(ns); }
	void Upsert(const std::string &ns, ItemType &item) {
		assert(!!item);
		auto err = reindexer->Upsert(ns, item);

		ASSERT_TRUE(err.ok()) << err.what();
	}
	void Upsert(const std::string &ns, ItemType &item, std::function<void(const reindexer::Error &)> cmpl) {
		assert(!!item);
		auto err = reindexer->WithCompletion(cmpl).Upsert(ns, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	void PrintQueryResults(const std::string &ns, const QueryResults &res) {
		if (!verbose) return;
		{
			ItemType rdummy(reindexer->NewItem(ns));
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				auto sv = rdummy[idx].Name();
				outBuf.append(sv.begin(), sv.end());
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
	string PrintItem(Item &item) {
		std::string outBuf = "";
		for (auto idx = 1; idx < item.NumFields(); idx++) {
			outBuf += string(item[idx].Name()) + "=";
			outBuf += item[idx].As<string>() + " ";
		}
		return outBuf;
	}
	std::string RandString() {
		string res;
		uint8_t len = rand() % 4 + 4;
		res.resize(len);
		for (int i = 0; i < len; ++i) {
			int f = rand() % letters.size();
			res[i] = letters[f];
		}
		return res;
	}
	std::string RandLikePattern() {
		string res;
		const uint8_t len = rand() % 4 + 4;
		for (uint8_t i = 0; i < len;) {
			if (rand() % 3 == 0) {
				res += '%';
				const uint8_t skipLen = rand() % (len - i + 1);
				i += skipLen;
			} else {
				if (rand() % 3 == 0) {
					res += '_';
				} else {
					int f = rand() % letters.size();
					res += letters[f];
				}
				++i;
			}
		}
		return res;
	}
	std::string RuRandString() {
		string res;
		uint8_t len = rand() % 20 + 4;
		res.resize(len * 3);
		auto it = res.begin();
		for (int i = 0; i < len; ++i) {
			int f = rand() % ru_letters.size();
			it = utf8::append(ru_letters[f], it);
		}
		res.erase(it, res.end());
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
	std::shared_ptr<DB> reindexer;

private:
	const string letters = "abcdefghijklmnopqrstuvwxyz";
	const wstring ru_letters = L"абвгдеёжзийклмнопрстуфхцчшщъыьэюя";
	bool verbose = false;
};

class ReindexerApi : public ::testing::Test {
protected:
	void SetUp() {}

	void TearDown() {}

public:
	ReindexerApi() {}

	void DefineNamespaceDataset(const string &ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(ns, fields);
	}
	Item NewItem(const std::string &ns) { return rt.NewItem(ns); }

	Error Commit(const std::string &ns) { return rt.Commit(ns); }
	void Upsert(const std::string &ns, Item &item) { rt.Upsert(ns, item); }

	void PrintQueryResults(const std::string &ns, const QueryResults &res) { rt.PrintQueryResults(ns, res); }
	string PrintItem(Item &item) { return rt.PrintItem(item); }

	std::string RandString() { return rt.RandString(); }
	std::string RandLikePattern() { return rt.RandLikePattern(); }
	std::string RuRandString() { return rt.RuRandString(); }
	vector<int> RandIntVector(size_t size, int start, int range) { return rt.RandIntVector(size, start, range); }

public:
	const string default_namespace = "test_namespace";
	ReindexerTestApi<reindexer::Reindexer> rt;
};

class CanceledRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType checkCancel() const noexcept override { return reindexer::CancelType::Explicit; }
	bool isCancelable() const noexcept override { return true; }
};

class DummyRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType checkCancel() const noexcept override { return reindexer::CancelType::None; }
	bool isCancelable() const noexcept override { return false; }
};

class FakeRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType checkCancel() const noexcept override { return reindexer::CancelType::None; }
	bool isCancelable() const noexcept override { return true; }
};
