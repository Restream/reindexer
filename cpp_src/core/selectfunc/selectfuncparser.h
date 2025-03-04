#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "core/cjson/tagspath.h"
#include "ctx/basefunctionctx.h"
#include "estl/tokenizer.h"

namespace reindexer {

class BaseFunctionCtx;

struct SelectFuncStruct {
	SelectFuncVariant func;
	bool isFunction = false;
	std::string field;
	std::string value;
	std::string funcName;
	std::vector<std::string> funcArgs;
	std::unordered_map<std::string, std::string> namedArgs;
	BaseFunctionCtx::Ptr ctx;
	TagsPath tagsPath;
	int indexNo = IndexValueType::NotSet;
	int fieldNo = 0;
};

class SelectFuncParser {
public:
	SelectFuncStruct& Parse(const std::string& query);
	SelectFuncStruct& ParseFunction(tokenizer& parser, bool partOfExpression, token& tok);
	static bool IsFunction(std::string_view val) noexcept;
	static bool IsFunction(const VariantArray& val) noexcept;

protected:
	struct Args {
		explicit Args(unsigned int p, std::unordered_set<std::string>&& n) : posArgsCount(p), namedArgs(std::move(n)) {}
		unsigned int posArgsCount;
		std::unordered_set<std::string> namedArgs;
	};

	void parsePositionalAndNamedArgs(tokenizer& parser, const Args& args);

	SelectFuncStruct selectFuncStruct_;
};

}  // namespace reindexer
