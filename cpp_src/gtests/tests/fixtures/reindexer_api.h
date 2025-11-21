#pragma once

#include "reindexertestapi.h"

using reindexer::Error;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::Query;
using reindexer::QueryEntry;
using reindexer::LocalQueryResults;

class [[nodiscard]] ReindexerApi : public virtual ::testing::Test {
public:
	using Reindexer = reindexer::Reindexer;
	using QueryResults = reindexer::QueryResults;
	using Item = reindexer::Item;

	ReindexerApi() = default;

	void DefineNamespaceDataset(std::string_view ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(ns, fields);
	}
	void DefineNamespaceDataset(std::string_view ns, const std::vector<IndexDeclaration>& fields) { rt.DefineNamespaceDataset(ns, fields); }
	void DefineNamespaceDataset(Reindexer& rx, const std::string& ns, std::initializer_list<const IndexDeclaration> fields) {
		rt.DefineNamespaceDataset(rx, ns, fields);
	}
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
	std::vector<std::string> RandStrVector(size_t size) { return rt.RandStrVector(size); }
	void AwaitIndexOptimization(std::string_view nsName) { rt.AwaitIndexOptimization(nsName); }

public:
	const std::string default_namespace = "test_namespace";
	ReindexerTestApi<reindexer::Reindexer> rt;

protected:
	void initializeDefaultNs();
	static reindexer::Item getMemStat(Reindexer& rx, std::string_view ns);
};
