#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "estl/tokenizer.h"

namespace reindexer {

class BaseFunctionCtx;

struct SelectFuncStruct {
	enum Type { kSelectFuncNone = 0, kSelectFuncSnippet = 1, kSelectFuncHighlight = 2, kSelectFuncProc = 3, kSelectFuncSnippetN = 4 };

	Type type = kSelectFuncNone;
	bool isFunction = false;
	std::string field;
	std::string value;
	std::string funcName;
	std::vector<std::string> funcArgs;
	std::unordered_map<std::string, std::string> namedArgs;
	std::shared_ptr<BaseFunctionCtx> ctx;
	TagsPath tagsPath;
	int indexNo = -1;
	int fieldNo = 0;
};

class SelectFuncParser {
public:
	SelectFuncParser() {}
	~SelectFuncParser() {}

	SelectFuncStruct &Parse(const std::string &query);
	SelectFuncStruct &ParseFunction(tokenizer &parser, bool partOfExpression, token tok = token());
	static bool IsFunction(std::string_view val);
	static bool IsFunction(const VariantArray &val);

protected:
	struct Args {
		explicit Args(unsigned int p, std::unordered_set<std::string> &&n) : posArgsCount(p), namedArgs(std::move(n)) {}
		unsigned int posArgsCount;
		std::unordered_set<std::string> namedArgs;
	};

	void parsePositionalAndNamedArgs(tokenizer &parser, const Args &args);

	SelectFuncStruct selectFuncStruct_;
};

}  // namespace reindexer
