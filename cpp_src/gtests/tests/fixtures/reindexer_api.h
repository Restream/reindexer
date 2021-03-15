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
#include "gtests/tests/gtest_cout.h"
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
