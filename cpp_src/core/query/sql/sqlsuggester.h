#pragma once

#include <functional>
#include <unordered_set>
#include "core/schema.h"
#include "sqlparser.h"

namespace reindexer {

struct NamespaceDef;
struct EnumNamespacesOpts;
class SQLSuggester : public SQLParser {
public:
	using EnumNamespacesF = std::function<std::vector<NamespaceDef>(EnumNamespacesOpts opts)>;
	using GetSchemaF = std::function<std::shared_ptr<const Schema>(std::string_view ns)>;

	using SQLParser::SQLParser;
	/// Gets suggestions for autocomplte
	/// @param q - query to parse.
	/// @param pos - pos of cursor in query.
	/// @param enumNamespaces - functor which enums namespaces to be checked for existing fields.
	/// @param getSchemaSuggestions - functor which get pointer to namespace's schema
	[[nodiscard]] static std::vector<std::string> GetSuggestions(std::string_view q, size_t pos, EnumNamespacesF enumNamespaces,
																 GetSchemaF getSchemaSuggestions);

private:
	/// Finds suggestions for token
	/// @param ctx - suggestion context.
	void getSuggestionsForToken(SqlParsingCtx::SuggestionData &ctx);

	/// Checks whether suggestion is neede for a token
	void checkForTokenSuggestions(SqlParsingCtx::SuggestionData &data);

	/// Tries to find token value among accepted tokens.
	[[nodiscard]] bool findInPossibleTokens(int type, const std::string &v);
	/// Tries to find token value among indexes.
	[[nodiscard]] bool findInPossibleFields(const std::string &tok);
	/// Tries to find among possible namespaces.
	[[nodiscard]] bool findInPossibleNamespaces(const std::string &tok);
	/// Gets names of indexes that start with 'token'.
	void getMatchingFieldsNames(const std::string &token, std::unordered_set<std::string> &variants);
	void getMatchingNamespacesNames(const std::string &token, std::unordered_set<std::string> &variants);
	EnumNamespacesF enumNamespaces_;
	GetSchemaF getSchema_;
};

}  // namespace reindexer
