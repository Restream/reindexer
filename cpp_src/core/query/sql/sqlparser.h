#pragma once

#include <unordered_set>
#include <vector>
#include "core/query/knn_search_params.h"
#include "estl/tokenizer.h"
#include "sqltokentype.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class Query;
class JoinedQuery;
struct SortingEntries;
class UpdateEntry;
using EqualPosition_t = h_vector<std::string, 2>;

class [[nodiscard]] SQLParser {
	class ParserContextsAppendGuard;
	enum class [[nodiscard]] Nested : bool { Yes = true, No = false };

public:
	/// Parses pure sql select query and initializes Query object data members as a result.
	/// @param q - sql query.
	/// @return parsed query
	static Query Parse(std::string_view sql);

protected:
	explicit SQLParser(Query& q) noexcept : query_(q) {}
	/// Sql parser context
	struct [[nodiscard]] SqlParsingCtx {
		struct [[nodiscard]] SuggestionData {
			SuggestionData(std::string tok, SqlTokenType tokType) : token(std::move(tok)), tokenType(tokType) {}
			std::string token;
			SqlTokenType tokenType = Start;
			std::unordered_set<std::string> variants;
		};
		void updateLinkedNs(const std::string& ns) {
			if (autocompleteMode && (!foundPossibleSuggestions || possibleSuggestionDetectedInThisClause)) {
				suggestionLinkedNs = ns;
			}
			possibleSuggestionDetectedInThisClause = false;
		}
		bool autocompleteMode = false;
		bool foundPossibleSuggestions = false;
		bool possibleSuggestionDetectedInThisClause = false;
		size_t suggestionsPos = 0;
		std::vector<SqlTokenType> tokens;
		std::vector<SuggestionData> suggestions;
		std::string suggestionLinkedNs;
	};

	/// Parses query.
	/// @param tok - tokenizer object instance.
	/// @return always returns zero.
	int Parse(tokenizer& tok);

	/// Peeks next sql token.
	/// @param parser - tokenizer object instance.
	/// @param tokenType - token type.
	/// @param toLower - transform to lower representation.
	/// @return sql token object.
	token peekSqlToken(tokenizer& parser, SqlTokenType tokenType, bool toLower = true);

	/// Is current token last in autocomplete mode?
	bool reachedAutocompleteToken(tokenizer& parser, const token& tok) const;

	/// Parses filter part of sql query.
	/// @param parser - tokenizer object instance.
	template <Nested>
	void selectParse(tokenizer& parser);

	/// Parses filter part of sql delete query.
	/// @param parser - tokenizer object instance.
	void deleteParse(tokenizer& parser);

	/// Parses filter part of sql update query.
	/// @param parser - tokenizer object instance.
	void updateParse(tokenizer& parser);

	/// Parses filter part of sql truncate query.
	/// @param parser - tokenizer object instance.
	void truncateParse(tokenizer& parser);

	/// Parse where entries
	template <Nested>
	void parseWhere(tokenizer& parser);
	template <typename T>
	void parseWhereCondition(tokenizer&, T&& firstArg, OpType);

	/// Parse order by
	template <typename Sortable>
	void parseOrderBy(tokenizer& parser, Sortable&);

	/// Parse join entries
	void parseJoin(JoinType type, tokenizer& tok);

	/// Parse join entries
	void parseJoinEntries(tokenizer& parser, const std::string& mainNs, JoinedQuery& jquery);

	/// Parse equal_positions
	void parseEqualPositions(tokenizer& parser);

	Point parseGeomFromText(tokenizer& parser) const;
	void parseDWithin(tokenizer& parser, OpType nextOp);
	void parseKnn(tokenizer& parser, OpType nextOp);
	KnnSearchParams parseKnnParams(tokenizer&);
	template <typename T>
	void parseSingleKnnParam(tokenizer&, std::optional<T>& param, std::string_view paramName);

	/// Parse update field entries
	UpdateEntry parseUpdateField(tokenizer& parser);

	/// Parse joined Ns name: [Namespace.field]
	std::string parseJoinedFieldName(tokenizer& parser, std::string& name);

	/// Parse merge entries
	void parseMerge(tokenizer& parser);

	void parseModifyConditions(tokenizer& parser);

	Query parseSubQuery(tokenizer& parser);

	void parseArray(tokenizer& parser, std::string_view tokText, UpdateEntry* updateField) const;
	void parseCommand(tokenizer& parser) const;

	static CondType getCondType(std::string_view cond);
	SqlParsingCtx ctx_;
	Query& query_;
};

}  // namespace reindexer
