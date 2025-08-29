#pragma once

#include "queries_verifier.h"
#include "reindexer_api.h"

class [[nodiscard]] DistinctApi : public ReindexerApi, public QueriesVerifier {
	void SetUp() override {
		setPkFields(default_namespace, {"id"});
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts{}.PK(), 0},
												   IndexDeclaration{"vi1", "hash", "int", IndexOpts{}, 0},
												   IndexDeclaration{"vi2", "hash", "string", IndexOpts{}, 0}});
	}
};
