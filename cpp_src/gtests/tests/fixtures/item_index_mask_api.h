#include <gtest/gtest.h>
#include "reindexer_api.h"

class [[nodiscard]] ItemIndexMaskApi : public ReindexerApi {
public:
	const char* kFieldId = "id";
	const char* kFieldOne = "f1";
	const char* kFieldTwo = "f2";

	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(default_namespace);
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldOne, "tree", "int", IndexOpts(), 0},
												   IndexDeclaration{kFieldTwo, "tree", "int", IndexOpts().Array(), 0}});
		// clang-format off
		constexpr std::string_view jsonschema = R"xxx(
		{
	  		"required": [
				"id",
				"f1",
				"f2"
	  		],
	  		"properties": {
				"id": {
		  			"type": "number"
				},
				"f1": {
		  			"type": "number"
				},
				"f2": {
		  			"type": "number"
				}
	  		},
	  		"additionalProperties": false,
	  		"type": "object"
		})xxx";
		// clang-format on
		rt.SetSchema(default_namespace, jsonschema);
	}
};