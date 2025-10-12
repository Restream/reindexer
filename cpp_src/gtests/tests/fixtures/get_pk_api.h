#pragma once

#include <gtest/gtest.h>
#include <memory>

#include "core/reindexer.h"
#include "estl/fast_hash_map.h"
#include "fmt/printf.h"
#include "gtests/tests/gtest_cout.h"

using std::string;

using reindexer::Error;
using reindexer::fast_hash_map;
using reindexer::h_vector;
using reindexer::NamespaceDef;
using reindexer::Query;

class [[nodiscard]] ExtractPK : public testing::Test {
public:
	using QueryResults = reindexer::QueryResults;
	using Reindexer = reindexer::Reindexer;
	using Item = reindexer::Item;
	struct [[nodiscard]] Data {
		int id;
		int fk_id;
		std::string_view name;
		std::string_view color;
		int weight;
		int height;
	};

	typedef fast_hash_map<std::string, NamespaceDef> DefsCacheType;

public:
	Error CreateNamespace(const NamespaceDef& nsDef) {
		Error err = db_->OpenNamespace(nsDef.name);
		if (!err.ok()) {
			return err;
		}

		for (const auto& index : nsDef.indexes) {
			err = db_->AddIndex(nsDef.name, index);
			if (!err.ok()) {
				break;
			}
		}

		return err;
	}

	Error Upsert(const std::string& ns, Item& item) { return db_->Upsert(ns, item); }

	std::tuple<Error, Item, Data> NewItem(const std::string& ns, const std::string& jsonPattern, Data* d = nullptr) {
		typedef std::tuple<Error, Item, Data> ResultType;

		Item item = db_->NewItem(ns);
		if (!item.Status().ok()) {
			return ResultType(item.Status(), std::move(item), Data{0, 0, {}, {}, 0, 0});
		}

		Data data = (d == nullptr) ? randomItemData() : *d;
		std::string json = fmt::sprintf(jsonPattern, data.id, data.name, data.color, data.weight, data.height, data.fk_id);

		return ResultType(item.FromJSON(json), std::move(item), data);
	}

	Item ItemFromData(const std::string& ns, const Data& data) {
		Item item = db_->NewItem(ns);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();
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
		if (!err.ok()) {
			return ResultType(err, QueryResults{});
		}

		if (print) {
			printQueryResults(query.NsName(), qres);
		}
		return ResultType(err, std::move(qres));
	}

protected:
	void SetUp() {
		db_ = std::make_unique<Reindexer>();
		auto err = db_->Connect("builtin://");
		ASSERT_TRUE(err.ok()) << err.what();
	}

	Data randomItemData() {
		return Data{rand() % 10000,
					700000 + (rand() % 10000),
					names_.at(rand() % names_.size()),
					colors_.at(rand() % colors_.size()),
					rand() % 1000,
					rand() % 1000};
	}

	void printQueryResults(const std::string& ns, QueryResults& res) {
		{
			Item rdummy(db_->NewItem(ns));
			ASSERT_TRUE(rdummy.Status().ok()) << rdummy.Status().what();
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += std::string(rdummy[idx].Name());
			}
			TestCout() << outBuf << std::endl;
		}

		for (auto it : res) {
			Item ritem(it.GetItem(false));
			std::string outBuf = "";
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				auto field = ritem[idx].Name();
				outBuf += "\t";
				outBuf += ritem[field].As<std::string>();
			}
			TestCout() << outBuf << std::endl;
		}
		TestCout() << std::endl;
	}

protected:
	std::unique_ptr<Reindexer> db_;

	reindexer::h_vector<std::string_view> colors_ = {"red", "green", "blue", "yellow", "purple", "orange"};
	reindexer::h_vector<std::string_view> names_ = {"bubble", "dog", "tomorrow", "car", "dinner", "dish"};
};
