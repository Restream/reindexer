#pragma once

#include <gtest/gtest.h>
#include <memory>

#include "core/reindexer.h"
#include "estl/fast_hash_map.h"
#include "gtests/tests/gtest_cout.h"

using std::string;

using reindexer::Error;
using reindexer::fast_hash_map;
using reindexer::h_vector;
using reindexer::Item;
using reindexer::NamespaceDef;
using reindexer::Query;
using reindexer::QueryResults;
using reindexer::Reindexer;

class ExtractPK : public testing::Test {
public:
	struct Data {
		int id;
		int fk_id;
		const char* name;
		const char* color;
		int weight;
		int height;
	};

	typedef fast_hash_map<string, NamespaceDef> DefsCacheType;

	template <typename... Args>
	static string StringFormat(const string& format, Args... args) {
		size_t size = snprintf(nullptr, 0, format.c_str(), args...) + 1;  // extra symbol for '\n'
		std::unique_ptr<char[]> buf(new char[size]);
		snprintf(buf.get(), size, format.c_str(), args...);
		return string(buf.get(), buf.get() + size - 1);
	}

public:
	Error CreateNamespace(const NamespaceDef& nsDef) {
		Error err = db_->OpenNamespace(nsDef.name);
		if (!err.ok()) return err;

		for (const auto& index : nsDef.indexes) {
			err = db_->AddIndex(nsDef.name, index);
			if (!err.ok()) break;
		}

		return err;
	}

	Error UpsertAndCommit(const string& ns, Item& item) {
		Error err = db_->Upsert(ns, item);
		if (!err.ok()) return err;

		return db_->Commit(ns);
	}

	std::tuple<Error, Item, Data> NewItem(const string& ns, const string& jsonPattern, Data* d = nullptr) {
		typedef std::tuple<Error, Item, Data> ResultType;

		Item item = db_->NewItem(ns);
		if (!item.Status().ok()) return ResultType(item.Status(), std::move(item), Data{0, 0, nullptr, nullptr, 0, 0});

		Data data = (d == nullptr) ? randomItemData() : *d;
		std::string json = StringFormat(jsonPattern, data.id, data.name, data.color, data.weight, data.height, data.fk_id);

		return ResultType(item.FromJSON(json), std::move(item), data);
	}

	Item ItemFromData(const string& ns, const Data& data) {
		Item item = db_->NewItem(ns);
		item["id"] = data.id;
		item["fk_id"] = data.fk_id;
		item["name"] = data.name;
		item["color"] = data.color;
		item["weight"] = data.weight;
		item["height"] = data.height;

		return item;
	}

	std::tuple<Error, QueryResults> Select(const Query& query, bool print = false) {
		typedef std::tuple<Error, QueryResults> ResultType;

		QueryResults qres;
		Error err = db_->Select(query, qres);
		if (!err.ok()) return ResultType(err, QueryResults{});

		if (print) printQueryResults(query._namespace, qres);
		return ResultType(err, std::move(qres));
	}

protected:
	void SetUp() {
		colors_ = {"red", "green", "blue", "yellow", "purple", "orange"};
		names_ = {"bubble", "dog", "tomorrow", "car", "dinner", "dish"};
		db_.reset(new Reindexer);
	}

	Data randomItemData() {
		return Data{rand() % 10000,
					700000 + (rand() % 10000),
					names_.at(rand() % names_.size()),
					colors_.at(rand() % colors_.size()),
					rand() % 1000,
					rand() % 1000};
	}

	void printQueryResults(const string& ns, QueryResults& res) {
		{
			Item rdummy(db_->NewItem(ns));
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += string(rdummy[idx].Name());
			}
			TestCout() << outBuf << std::endl;
		}

		for (auto it : res) {
			Item ritem(it.GetItem(false));
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

protected:
	std::shared_ptr<Reindexer> db_;

	reindexer::h_vector<const char*> colors_;
	reindexer::h_vector<const char*> names_;
};
