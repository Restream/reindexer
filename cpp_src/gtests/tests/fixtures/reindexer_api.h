#pragma once

#include "core/reindexer.h"
#include "reindexertestapi.h"

using reindexer::Error;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::Query;
using reindexer::QueryEntry;
using reindexer::LocalQueryResults;

class ReindexerApi : public virtual ::testing::Test {
protected:
	void SetUp() override {
		auto err = rt.reindexer->Connect("builtin://");
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void TearDown() override {}

public:
	using Reindexer = reindexer::Reindexer;
	using QueryResults = reindexer::QueryResults;
	using Item = reindexer::Item;

	ReindexerApi() = default;

	void DefineNamespaceDataset(std::string_view ns, std::initializer_list<const IndexDeclaration> fields);
	void DefineNamespaceDataset(std::string_view ns, const std::vector<IndexDeclaration>& fields);
	void DefineNamespaceDataset(Reindexer& rx, const std::string& ns, std::initializer_list<const IndexDeclaration> fields);
	Item NewItem(std::string_view ns) { return rt.NewItem(ns); }

	void Upsert(std::string_view ns, Item& item) { rt.Upsert(ns, item); }
	size_t Update(const Query& q) { return rt.Update(q); }
	void Delete(std::string_view ns, Item& item) { rt.Delete(ns, item); }
	size_t Delete(const Query& q) { return rt.Delete(q); }

	void PrintQueryResults(const std::string& ns, const QueryResults& res) { rt.PrintQueryResults(ns, res); }

	std::string RandString() { return rt.RandString(); }
	std::string RandString(unsigned len) { return rt.RandString(len); }
	std::string RandLikePattern() { return rt.RandLikePattern(); }
	std::string RuRandString() { return rt.RuRandString(); }
	std::vector<int> RandIntVector(size_t size, int start, int range) { return rt.RandIntVector(size, start, range); }
	void AwaitIndexOptimization(const std::string& nsName);

public:
	const std::string default_namespace = "test_namespace";
	ReindexerTestApi<reindexer::Reindexer> rt;

protected:
	void initializeDefaultNs();
	static reindexer::Item getMemStat(Reindexer& rx, const std::string& ns);
};

class CanceledRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::Explicit; }
	bool IsCancelable() const noexcept override { return true; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
};

class DummyRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return false; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
};

class FakeRdxContext : public reindexer::IRdxCancelContext {
public:
	reindexer::CancelType GetCancelType() const noexcept override { return reindexer::CancelType::None; }
	bool IsCancelable() const noexcept override { return true; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }
};
