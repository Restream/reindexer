#pragma once

#include "queries_verifier.h"
#include "reindexer_api.h"

class DistinctApi : public ReindexerApi, public QueriesVerifier {
	void SetUp() override {
		setPkFields(default_namespace, {"id"});
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts{}.PK(), 0},
												   IndexDeclaration{"vi1", "hash", "int", IndexOpts{}, 0},
												   IndexDeclaration{"vi2", "hash", "string", IndexOpts{}, 0}});
	}
};
