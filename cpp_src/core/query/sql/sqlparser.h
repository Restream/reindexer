#pragma once

#include <vector>
#include "core/keyvalue/variant.h"
#include "estl/tokenizer.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class Query;
class JoinedQuery;
struct SortingEntries;
class UpdateEntry;
using EqualPosition_t = h_vector<std::string, 2>;
class SQLParser {
public:
	explicit SQLParser(Query &q) noexcept : query_(q) {}

	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	/// @return always returns 0.
	int Parse(std::string_view q);

protected:
	/// Sql parser context
	struct SqlParsingCtx {
		struct SuggestionData {
			SuggestionData(std::string tok, int tokType) : token(std::move(tok)), tokenType(tokType) {}
			std::string token;
			int tokenType = 0;
			std::vector<std::string> variants;
		};
		void updateLinkedNs(const std::string &ns) {
			if (autocompleteMode && (!foundPossibleSuggestions || possibleSuggestionDetectedInThisClause)) {
				suggestionLinkedNs = ns;
			}
			possibleSuggestionDetectedInThisClause = false;
		}
		bool autocompleteMode = false;
		bool foundPossibleSuggestions = false;
		bool possibleSuggestionDetectedInThisClause = false;
		size_t suggestionsPos = 0;
		std::vector<int> tokens;
		std::vector<SuggestionData> suggestions;
		std::string suggestionLinkedNs;
	};

	/// Parses query.
	/// @param tok - tokenizer object instance.
	/// @return always returns zero.
	int Parse(tokenizer &tok);

	/// Peeks next sql token.
	/// @param parser - tokenizer object instance.
	/// @param tokenType - token type.
	/// @param toLower - transform to lower representation.
	/// @return sql token object.
	token peekSqlToken(tokenizer &parser, int tokenType, bool toLower = true);

	/// Is current token last in autocomplete mode?
	bool reachedAutocompleteToken(tokenizer &parser, const token &tok);

	/// Parses filter part of sql query.
	/// @param parser - tokenizer object instance.
	/// @return always returns zero.
	int selectParse(tokenizer &parser);

	/// Parses filter part of sql query and gets suggestions from nested SQLParser
	/// @param parser - nested parser object instance.
	/// @param tok - tokenizer object instance.
	/// @return always returns zero.
	int nestedSelectParse(SQLParser &parser, tokenizer &tok);

	/// Parses filter part of sql delete query.
	/// @param parser - tokenizer object instance.
	/// @return always returns zero.
	int deleteParse(tokenizer &parser);

	/// Parses filter part of sql update query.
	/// @param parser - tokenizer object instance.
	/// @return always returns zero.
	int updateParse(tokenizer &parser);

	/// Parses filter part of sql truncate query.
	/// @param parser - tokenizer object instance.
	/// @return always returns zero.
	int truncateParse(tokenizer &parser);

	/// Parse where entries
	int parseWhere(tokenizer &parser);

	/// Parse order by
	int parseOrderBy(tokenizer &parser, SortingEntries &, std::vector<Variant> &forcedSortOrder);

	/// Parse join entries
	void parseJoin(JoinType type, tokenizer &tok);

	/// Parse join entries
	void parseJoinEntries(tokenizer &parser, const std::string &mainNs, JoinedQuery &jquery);

	/// Parse equal_positions
	void parseEqualPositions(tokenizer &parser, std::vector<std::pair<size_t, EqualPosition_t>> &equalPositions, size_t openBracketsCount);

	Point parseGeomFromText(tokenizer &parser) const;
	void parseDWithin(tokenizer &parser, OpType nextOp);

	/// Parse update field entries
	UpdateEntry parseUpdateField(tokenizer &parser);

	/// Parse joined Ns name: [Namespace.field]
	std::string parseJoinedFieldName(tokenizer &parser, std::string &name);

	/// Parse merge entries
	void parseMerge(tokenizer &parser);

	static CondType getCondType(std::string_view cond);
	SqlParsingCtx ctx_;
	Query &query_;
};

}  // namespace reindexer
