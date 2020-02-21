
#include "sqlsuggester.h"
#include "core/namespacedef.h"
#include "sqltokentype.h"

#include <set>
#include <unordered_map>

namespace reindexer {

bool checkIfTokenStartsWith(const string_view &src, const string_view &pattern) {
	return checkIfStartsWith(src, pattern) && src.length() < pattern.length();
}

vector<string> SQLSuggester::GetSuggestions(const string_view &q, size_t pos, EnumNamespacesF enumNamespaces) {
	ctx_.suggestionsPos = pos;
	ctx_.autocompleteMode = true;
	enumNamespaces_ = enumNamespaces;

	try {
		Parse(q);
	} catch (const Error &) {
	}

	for (SqlParsingCtx::SuggestionData &item : ctx_.suggestions) {
		checkForTokenSuggestions(item);
	}

	if (ctx_.suggestions.size() > 0) return ctx_.suggestions.front().variants;
	return std::vector<string>();
}

std::unordered_map<int, std::set<string>> sqlTokenMatchings = {
	{Start, {"explain", "select", "delete", "update", "truncate"}},
	{StartAfterExplain, {"select", "delete", "update"}},
	{AggregationSqlToken, {"sum", "avg", "max", "min", "facet", "count", "distinct"}},
	{SelectConditionsStart, {"where", "limit", "offset", "order", "join", "left", "inner", "equal_position", "merge", "or", ";"}},
	{ConditionSqlToken, {">", ">=", "<", "<=", "<>", "in", "range", "is", "==", "="}},
	{WhereFieldValueSqlToken, {"null", "empty", "not"}},
	{WhereFieldNegateValueSqlToken, {"null", "empty"}},
	{OpSqlToken, {"and", "or"}},
	{WhereOpSqlToken, {"and", "or", "order"}},
	{SortDirectionSqlToken, {"asc", "desc"}},
	{LeftSqlToken, {"join"}},
	{InnerSqlToken, {"join"}},
	{SelectSqlToken, {"select"}},
	{OnSqlToken, {"on"}},
	{BySqlToken, {"by"}},
	{NotSqlToken, {"not"}},
	{FieldSqlToken, {"field"}},
	{FromSqlToken, {"from"}},
	{SetSqlToken, {"set"}},
	{WhereSqlToken, {"where"}},
	{AllFieldsToken, {"*"}},
	{DeleteConditionsStart, {"where", "limit", "offset", "order"}},
	{UpdateOptions, {"set", "drop"}},
};

void getMatchingTokens(int tokenType, const string &token, vector<string> &variants) {
	const std::set<string> &suggestions = sqlTokenMatchings[tokenType];
	for (auto it = suggestions.begin(); it != suggestions.end(); ++it) {
		if (isBlank(token) || checkIfStartsWith(token, *it)) variants.push_back(*it);
	}
}

void SQLSuggester::getMatchingNamespacesNames(const string &token, vector<string> &variants) {
	auto namespaces = enumNamespaces_(EnumNamespacesOpts().OnlyNames());
	for (auto &ns : namespaces) {
		if (isBlank(token) || checkIfStartsWith(token, ns.name)) variants.push_back(ns.name);
	}
}

void SQLSuggester::getMatchingIndexesNames(const string &token, vector<string> &variants) {
	auto namespaces = enumNamespaces_(EnumNamespacesOpts().WithFilter(ctx_.suggestionLinkedNs));

	if (namespaces.empty()) return;
	for (auto &idx : namespaces[0].indexes) {
		if (idx.name_ == "#pk" || idx.name_ == "-tuple") continue;
		if (isBlank(token) || checkIfStartsWith(token, idx.name_)) variants.push_back(idx.name_);
	}
}

void SQLSuggester::getSuggestionsForToken(SqlParsingCtx::SuggestionData &ctx) {
	switch (ctx.tokenType) {
		case Start:
		case StartAfterExplain:
		case FromSqlToken:
		case SelectConditionsStart:
		case DeleteConditionsStart:
		case ConditionSqlToken:
		case WhereFieldValueSqlToken:
		case WhereFieldNegateValueSqlToken:
		case WhereOpSqlToken:
		case OpSqlToken:
		case LeftSqlToken:
		case InnerSqlToken:
		case SelectSqlToken:
		case OnSqlToken:
		case BySqlToken:
		case SetSqlToken:
		case WhereSqlToken:
		case UpdateOptions:
			getMatchingTokens(ctx.tokenType, ctx.token, ctx.variants);
			break;
		case SingleSelectFieldSqlToken:
			getMatchingTokens(AllFieldsToken, ctx.token, ctx.variants);
			getMatchingTokens(AggregationSqlToken, ctx.token, ctx.variants);
			getMatchingIndexesNames(ctx.token, ctx.variants);
			break;
		case SelectFieldsListSqlToken:
			getMatchingTokens(FromSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(AggregationSqlToken, ctx.token, ctx.variants);
			getMatchingIndexesNames(ctx.token, ctx.variants);
			break;
		case NamespaceSqlToken:
			getMatchingNamespacesNames(ctx.token, ctx.variants);
			break;
		case AndSqlToken:
		case WhereFieldSqlToken:
			getMatchingTokens(NotSqlToken, ctx.token, ctx.variants);
			getMatchingIndexesNames(ctx.token, ctx.variants);
			break;
		case FieldNameSqlToken:
			getMatchingIndexesNames(ctx.token, ctx.variants);
			break;
		case SortDirectionSqlToken:
			getMatchingTokens(SortDirectionSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(FieldSqlToken, ctx.token, ctx.variants);
			break;
		case JoinedFieldNameSqlToken:
			getMatchingNamespacesNames(ctx.token, ctx.variants);
			getMatchingIndexesNames(ctx.token, ctx.variants);
			break;
		default:
			break;
	};
}

bool SQLSuggester::findInPossibleTokens(int type, const string &v) {
	const std::set<string> &values = sqlTokenMatchings[type];
	return (values.find(v) != values.end());
}

bool SQLSuggester::findInPossibleIndexes(const string &tok) {
	auto namespaces = enumNamespaces_(EnumNamespacesOpts().WithFilter(ctx_.suggestionLinkedNs));

	if (namespaces.empty()) return false;
	return std::find_if(namespaces[0].indexes.begin(), namespaces[0].indexes.end(),
						[&](const IndexDef &lhs) { return lhs.name_ == tok; }) != namespaces[0].indexes.end();
}

bool SQLSuggester::findInPossibleNamespaces(const string &tok) {
	return !enumNamespaces_(EnumNamespacesOpts().WithFilter(tok).OnlyNames()).empty();
}

void SQLSuggester::checkForTokenSuggestions(SqlParsingCtx::SuggestionData &data) {
	switch (data.tokenType) {
		case Start:
		case StartAfterExplain:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case SingleSelectFieldSqlToken: {
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (data.token == "*") break;
			bool isIndex = false, isAggregationFunction = false;
			isIndex = findInPossibleIndexes(data.token);
			if (!isIndex) isAggregationFunction = findInPossibleTokens(AggregationSqlToken, data.token);
			if (!isIndex && !isAggregationFunction) {
				getSuggestionsForToken(data);
			}
		} break;
		case SelectFieldsListSqlToken: {
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}

			if ((data.token == ",") || (data.token == "(")) break;

			bool fromKeywordReached = false;
			if (ctx_.tokens.size() > 1) {
				int prevTokenType = ctx_.tokens.back();
				if ((prevTokenType == SingleSelectFieldSqlToken) || (prevTokenType == SelectFieldsListSqlToken)) {
					fromKeywordReached = checkIfStartsWith(data.token, "from");
					if (fromKeywordReached && data.token.length() < strlen("from")) {
						getSuggestionsForToken(data);
					}
				}
			}

			if (!fromKeywordReached && !findInPossibleIndexes(data.token)) {
				getSuggestionsForToken(data);
			}
		} break;
		case FromSqlToken:
			if (isBlank(data.token) || !iequals(data.token, "from")) {
				getSuggestionsForToken(data);
			}
			break;
		case NamespaceSqlToken:
			if (isBlank(data.token) || !findInPossibleNamespaces(data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case SelectConditionsStart:
		case DeleteConditionsStart:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case WhereFieldSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (iequals(data.token, "not")) break;
			if (!findInPossibleIndexes(data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case ConditionSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case WhereFieldValueSqlToken:
			if (isBlank(data.token)) break;
			if (checkIfTokenStartsWith(data.token, "null")) {
				getSuggestionsForToken(data);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "empty")) {
				getSuggestionsForToken(data);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "not")) {
				getSuggestionsForToken(data);
			}
			break;
		case WhereFieldNegateValueSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case WhereOpSqlToken:
		case OpSqlToken:
			if (isBlank(data.token)) {
				switch (ctx_.tokens.back()) {
					case WhereSqlToken:
						data.tokenType = FieldNameSqlToken;
						break;
					case OnSqlToken:
						data.tokenType = NamespaceSqlToken;
						break;
					default:
						break;
				}
				getSuggestionsForToken(data);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "and")) {
				getSuggestionsForToken(data);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "or")) {
				getSuggestionsForToken(data);
				break;
			}
			if ((data.tokenType == WhereOpSqlToken) && (ctx_.tokens.size() > 1)) {
				int prevTokenType = ctx_.tokens.back();
				if ((prevTokenType != WhereSqlToken) && checkIfTokenStartsWith(data.token, "order")) {
					getSuggestionsForToken(data);
					break;
				}
			}
			break;
		case AndSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (findInPossibleIndexes(data.token)) break;
			if (checkIfTokenStartsWith(data.token, "not")) {
				getSuggestionsForToken(data);
			}
			getSuggestionsForToken(data);
			break;
		case FieldNameSqlToken:
			if (isBlank(data.token) || !findInPossibleIndexes(data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case SortDirectionSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (data.token == "(") break;
			if (checkIfTokenStartsWith(data.token, "field")) {
				getSuggestionsForToken(data);
				break;
			}
			if (!findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			break;
		case JoinedFieldNameSqlToken:
			if (isBlank(data.token) || !findInPossibleIndexes(data.token) || !findInPossibleNamespaces(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			break;
		case LeftSqlToken:
		case InnerSqlToken:
		case SelectSqlToken:
		case OnSqlToken:
		case BySqlToken:
		case SetSqlToken:
		case WhereSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			break;
		default:
			break;
	}
}

}  // namespace reindexer
