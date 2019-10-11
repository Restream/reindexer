#pragma once

#include "estl/fast_hash_map.h"
#include "sqlparser.h"
#include "tools/stringstools.h"

namespace reindexer {

struct NamespaceDef;

class SQLSuggester : public SQLParser {
public:
	using Namespaces = std::vector<NamespaceDef>;
	using SQLParser::SQLParser;
	/// Gets suggestions for autocomplte
	/// @param q - query to parse.
	/// @param pos - pos of cursor in query.
	/// @param namespaces - list of namespaces to be checked for existing fields.
	std::vector<string> GetSuggestions(const string_view &q, size_t pos, const Namespaces &namespaces);

protected:
	/// Finds suggestions for token
	/// @param ctx - suggestion context.
	/// @param nsName - name of active Namespace.
	/// @param namespaces - list of namespaces in db.
	void getSuggestionsForToken(SqlParsingCtx::SuggestionData &ctx, const string &nsName, const Namespaces &namespaces);

	/// Checks whether suggestion is neede for a token
	void checkForTokenSuggestions(SqlParsingCtx::SuggestionData &data, const SqlParsingCtx &ctx, const Namespaces &namespaces);

	/// Tries to find token value among accepted tokens.
	bool findInPossibleTokens(int type, const string &v);
	/// Tries to find token value among indexes.
	bool findInPossibleIndexes(const string &tok, const string &nsName, const Namespaces &namespaces);
	/// Tries to find among possible namespaces.
	bool findInPossibleNamespaces(const string &tok, const Namespaces &namespaces);
	/// Gets names of indexes that start with 'token'.
	void getMatchingIndexesNames(const Namespaces &namespaces, const string &nsName, const string &token, std::vector<string> &variants);
};

}  // namespace reindexer
