#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "estl/tokenizer.h"
#include "functions/highlight.h"
#include "functions/snippet.h"

namespace reindexer {

class BaseFunctionCtx;

class FuncNone {
public:
	bool Process(ItemRef &, PayloadType &, const SelectFuncStruct &, std::vector<key_string> &) { return false; }
};

template <typename VariantType, typename T, std::size_t index = 0>
constexpr std::size_t variant_index() {
	static_assert(std::variant_size_v<VariantType> > index, "Type not found in variant");
	if constexpr (std::is_same_v<std::variant_alternative_t<index, VariantType>, T>) {
		return index;
	} else {
		return variant_index<VariantType, T, index + 1>();
	}
}

struct SelectFuncStruct {
	using FuncVariant = std::variant<FuncNone, Snippet, Highlight, SnippetN>;
	enum class SelectFuncType {
		None = variant_index<FuncVariant, FuncNone>(),
		Snippet = variant_index<FuncVariant, Snippet>(),
		Highlight = variant_index<FuncVariant, Highlight>(),
		SnippetN = variant_index<FuncVariant, SnippetN>()
	};

	FuncVariant func;
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
