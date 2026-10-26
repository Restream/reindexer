#pragma once

#include "core/query/expression/arithmetic_expression.h"
#include "reindexer_api.h"

namespace reindexer_tests {

class [[nodiscard]] ExpressionApi : public ReindexerApi {
protected:
	void SetUp() override {
		using reindexer::IndexOpts;

		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameName, "hash", "string", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameEnabled, "-", "bool", IndexOpts(), 0},
												   IndexDeclaration{kFieldNameRate, "tree", "double", IndexOpts(), 0},
												   IndexDeclaration{kFieldNamePackages, "hash", "int", IndexOpts().Array(), 0},
												   IndexDeclaration{kFieldNameSparseAge, "hash", "int", IndexOpts().Sparse(), 0}});
	}

	void InsertSampleItem(size_t packagesCount = 0, int id = 1) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = id;
		item[kFieldNameAge] = 10;
		item[kFieldNameYear] = 2010;
		item[kFieldNameName] = "name";
		item[kFieldNameEnabled] = true;
		item[kFieldNameRate] = 1.5;
		item[kFieldNamePackages] = RandIntVector(packagesCount, 0, 10);
		Upsert(default_namespace, item);
	}

	size_t SelectCount(const Query& q) {
		QueryResults qr;
		const auto err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what() << " sql=" << q.GetSQL();
		return qr.Count();
	}

	Error SelectError(const Query& q) {
		QueryResults qr;
		return rt.reindexer->Select(q, qr);
	}

	const char* kFieldNameId = "id";
	const char* kFieldNameAge = "age";
	const char* kFieldNameYear = "year";
	const char* kFieldNameName = "name";
	const char* kFieldNameEnabled = "enabled";
	const char* kFieldNameRate = "rate";
	const char* kFieldNamePackages = "packages";
	const char* kFieldNameSparseAge = "sparse_age";
};

}  // namespace reindexer_tests
