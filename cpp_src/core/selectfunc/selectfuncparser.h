#pragma once

#include <memory>
#include <string>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "estl/tokenizer.h"

namespace reindexer {

using std::string;
using std::vector;
using std::shared_ptr;
class BaseFunctionCtx;

struct SelectFuncStruct {
	enum Type { kSelectFuncNone = 0, kSelectFuncSnippet = 1, kSelectFuncHighlight = 2, kSelectFuncProc = 3 };

	Type type = kSelectFuncNone;
	bool isFunction;
	string field;
	string value;
	string funcName;
	vector<string> funcArgs;
	shared_ptr<BaseFunctionCtx> ctx;
	TagsPath tagsPath;
	int indexNo = -1;
	int fieldNo = 0;
	bool fromCjson = false;
};

class SelectFuncParser {
public:
	SelectFuncParser() {}
	~SelectFuncParser() {}

	SelectFuncStruct &Parse(string query);

protected:
	void parseFunction(tokenizer &parser);

	SelectFuncStruct selectFuncStruct_;
};

}  // namespace reindexer
