#pragma once

#include <functional>
#include "core/schema.h"
#include "estl/fast_hash_map.h"
#include "sqlparser.h"
#include "tools/stringstools.h"

namespace reindexer {

struct NamespaceDef;
struct EnumNamespacesOpts;
class SQLSuggester : public SQLParser {
public:
	using EnumNamespacesF = std::function<std::vector<NamespaceDef>(EnumNamespacesOpts opts)>;
	using GetSchemaF = std::function<std::shared_ptr<const Schema>(string_view ns)>;

	using SQLParser::SQLParser;
	/// Gets suggestions for autocomplte
	/// @param q - query to parse.
	/// @param pos - pos of cursor in query.
	/// @param enumNamespaces - functor which enums namespaces to be checked for existing fields.
	std::vector<string> GetSuggestions(string_view q, size_t pos, EnumNamespacesF enumNamespaces, GetSchemaF getSchemaSuggestions);

protected:
	/// Finds suggestions for token
	/// @param ctx - suggestion context.
	void getSuggestionsForToken(SqlParsingCtx::SuggestionData &ctx);

	/// Checks whether suggestion is neede for a token
	void checkForTokenSuggestions(SqlParsingCtx::SuggestionData &data);

	/// Tries to find token value among accepted tokens.
	bool findInPossibleTokens(int type, const string &v);
	/// Tries to find token value among indexes.
	bool findInPossibleFields(const string &tok);
	/// Tries to find among possible namespaces.
	bool findInPossibleNamespaces(const string &tok);
	/// Gets names of indexes that start with 'token'.
	void getMatchingFieldsNames(const string &token, std::vector<string> &variants);
	void getMatchingNamespacesNames(const string &token, vector<string> &variants);
	EnumNamespacesF enumNamespaces_;
	GetSchemaF getSchema_;
};

}  // namespace reindexer
