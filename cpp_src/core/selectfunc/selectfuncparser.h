#pragma once

#include <memory>
#include <string>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "estl/tokenizer.h"

namespace reindexer {

class BaseFunctionCtx;

struct SelectFuncStruct {
	enum Type { kSelectFuncNone = 0, kSelectFuncSnippet = 1, kSelectFuncHighlight = 2, kSelectFuncProc = 3 };

	Type type = kSelectFuncNone;
	bool isFunction;
	std::string field;
	std::string value;
	std::string funcName;
	std::vector<std::string> funcArgs;
	std::shared_ptr<BaseFunctionCtx> ctx;
	TagsPath tagsPath;
	int indexNo = -1;
	int fieldNo = 0;
	bool fromCjson = false;
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
	SelectFuncStruct selectFuncStruct_;
};

}  // namespace reindexer
