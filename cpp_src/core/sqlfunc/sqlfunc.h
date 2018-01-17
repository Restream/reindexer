#pragma once

#include <string>
#include <vector>
#include "estl/tokenizer.h"

namespace reindexer {

using std::vector;
using std::string;

struct SqlFuncStruct {
	bool isFunction;
	string field;
	string value;
	string funcName;
	vector<string> funcArgs;
};

class SqlFunc {
public:
	SqlFunc() {}
	~SqlFunc() {}

	const SqlFuncStruct &Parse(string query);

protected:
	void parseFunction(tokenizer &parser);

	SqlFuncStruct sqlFuncStruct_;
};

}  // namespace reindexer
