#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <tuple>

#include <iostream>
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/query/query.h"
#include "core/reindexer.h"
#include "reindexertestapi.h"
#include "servercontrol.h"
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

class ReindexerApi : public virtual ::testing::Test {
protected:
	void SetUp() override {
		auto err = rt.reindexer->Connect("builtin://");
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void TearDown() override {}

public:
	ReindexerApi() = default;

	void DefineNamespaceDataset(const std::string &ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(ns, fields);
	}
	void DefineNamespaceDataset(const std::string &ns, const std::vector<IndexDeclaration> &fields) {
		rt.DefineNamespaceDataset(ns, fields);
	}
	void DefineNamespaceDataset(Reindexer &rx, const std::string &ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(rx, ns, fields);
	}
	Item NewItem(std::string_view ns) { return rt.NewItem(ns); }

	[[nodiscard]] Error Commit(std::string_view ns) { return rt.Commit(ns); }
	void Upsert(std::string_view ns, Item &item) { rt.Upsert(ns, item); }

	void PrintQueryResults(const std::string &ns, const QueryResults &res) { rt.PrintQueryResults(ns, res); }

	std::string RandString() { return rt.RandString(); }
	std::string RandLikePattern() { return rt.RandLikePattern(); }
	std::string RuRandString() { return rt.RuRandString(); }
	std::vector<int> RandIntVector(size_t size, int start, int range) { return rt.RandIntVector(size, start, range); }
	void AwaitIndexOptimization(const std::string &nsName) {
		bool optimization_completed = false;
		unsigned waitForIndexOptimizationCompleteIterations = 0;
		while (!optimization_completed) {
			ASSERT_LT(waitForIndexOptimizationCompleteIterations++, 200) << "Too long index optimization";
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			reindexer::QueryResults qr;
			Error err = rt.reindexer->Select(Query("#memstats").Where("name", CondEq, nsName), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(1, qr.Count());
			optimization_completed = qr[0].GetItem(false)["optimization_completed"].Get<bool>();
		}
	}

public:
	const std::string default_namespace = "test_namespace";
	ReindexerTestApi<reindexer::Reindexer> rt;

protected:
	void initializeDefaultNs() {
		auto err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled());
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->AddIndex(default_namespace, {"value", "text", "string", IndexOpts()});
		ASSERT_TRUE(err.ok()) << err.what();
	}

	static Item getMemStat(Reindexer &rx, const std::string &ns) {
		QueryResults qr;
		auto err = rx.Select(Query("#memstats").Where("name", CondEq, ns), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
		return qr[0].GetItem(false);
	}
};

class CanceledRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::Explicit; }
	bool IsCancelable() const noexcept override { return true; }
};

class DummyRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return false; }
};

class FakeRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return true; }
};
