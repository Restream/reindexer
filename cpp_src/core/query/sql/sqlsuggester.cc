
#include "sqlsuggester.h"
#include "core/namespacedef.h"
#include "sqltokentype.h"

#include <set>
#include <unordered_map>

namespace reindexer {

bool checkIfTokenStartsWith(const string_view &src, const string_view &pattern) {
	return checkIfStartsWith(src, pattern) && src.length() < pattern.length();
}

vector<string> SQLSuggester::GetSuggestions(const string_view &q, size_t pos, const Namespaces &namespaces) {
	ctx_.suggestionsPos = pos;
	ctx_.autocompleteMode = true;

	try {
		Parse(q);
	} catch (const Error &) {
	}

	for (SqlParsingCtx::SuggestionData &item : ctx_.suggestions) {
		checkForTokenSuggestions(item, ctx_, namespaces);
	}

	if (ctx_.suggestions.size() > 0) return ctx_.suggestions.front().variants;
	return std::vector<string>();
}

std::unordered_map<int, std::set<string>> sqlTokenMatchings = {
	{Start, {"explain", "select", "delete", "update", "truncate"}},
	{StartAfterExplain, {"select", "delete", "update"}},
	{AggregationSqlToken, {"sum", "avg", "max", "min", "facet", "count", "distinct"}},
	{SelectConditionsStart, {"where", "limit", "offset", "order", "join", "left", "inner", "merge", "or", ";"}},
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
};

void getMatchingTokens(int tokenType, const string &token, vector<string> &variants) {
	const std::set<string> &suggestions = sqlTokenMatchings[tokenType];
	for (auto it = suggestions.begin(); it != suggestions.end(); ++it) {
		if (isBlank(token) || checkIfStartsWith(token, *it)) variants.push_back(*it);
	}
}

void getMatchingNamespacesNames(const SQLSuggester::Namespaces &namespaces, const string &token, vector<string> &variants) {
	for (auto &ns : namespaces) {
		if (isBlank(token) || checkIfStartsWith(token, ns.name)) variants.push_back(ns.name);
	}
}

void SQLSuggester::getMatchingIndexesNames(const Namespaces &namespaces, const string &nsName, const string &token,
										   vector<string> &variants) {
	auto itNs = std::find_if(namespaces.begin(), namespaces.end(), [&](const NamespaceDef &lhs) { return lhs.name == nsName; });
	if (itNs == namespaces.end()) return;
	for (auto &idx : itNs->indexes) {
		if (idx.name_ == "#pk" || idx.name_ == "-tuple") continue;
		if (isBlank(token) || checkIfStartsWith(token, idx.name_)) variants.push_back(idx.name_);
	}
}

void SQLSuggester::getSuggestionsForToken(SqlParsingCtx::SuggestionData &ctx, const string &nsName, const Namespaces &namespaces) {
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
			getMatchingTokens(ctx.tokenType, ctx.token, ctx.variants);
			break;
		case SingleSelectFieldSqlToken:
			getMatchingTokens(AllFieldsToken, ctx.token, ctx.variants);
			getMatchingTokens(AggregationSqlToken, ctx.token, ctx.variants);
			getMatchingIndexesNames(namespaces, nsName, ctx.token, ctx.variants);
			break;
		case SelectFieldsListSqlToken:
			getMatchingTokens(FromSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(AggregationSqlToken, ctx.token, ctx.variants);
			getMatchingIndexesNames(namespaces, nsName, ctx.token, ctx.variants);
			break;
		case NamespaceSqlToken:
			getMatchingNamespacesNames(namespaces, ctx.token, ctx.variants);
			break;
		case AndSqlToken:
		case WhereFieldSqlToken:
			getMatchingTokens(NotSqlToken, ctx.token, ctx.variants);
			getMatchingIndexesNames(namespaces, nsName, ctx.token, ctx.variants);
			break;
		case FieldNameSqlToken:
			getMatchingIndexesNames(namespaces, nsName, ctx.token, ctx.variants);
			break;
		case SortDirectionSqlToken:
			getMatchingTokens(SortDirectionSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(FieldSqlToken, ctx.token, ctx.variants);
			break;
		case JoinedFieldNameSqlToken:
			getMatchingNamespacesNames(namespaces, ctx.token, ctx.variants);
			getMatchingIndexesNames(namespaces, nsName, ctx.token, ctx.variants);
			break;
		default:
			break;
	};
}

bool SQLSuggester::findInPossibleTokens(int type, const string &v) {
	const std::set<string> &values = sqlTokenMatchings[type];
	return (values.find(v) != values.end());
}

bool SQLSuggester::findInPossibleIndexes(const string &tok, const string &nsName, const Namespaces &namespaces) {
	auto itNs = std::find_if(namespaces.begin(), namespaces.end(), [&](const NamespaceDef &lhs) { return lhs.name == nsName; });
	if (itNs == namespaces.end()) return false;
	return std::find_if(itNs->indexes.begin(), itNs->indexes.end(), [&](const IndexDef &lhs) { return lhs.name_ == tok; }) !=
		   itNs->indexes.end();
}

bool SQLSuggester::findInPossibleNamespaces(const string &tok, const Namespaces &namespaces) {
	return std::find_if(namespaces.begin(), namespaces.end(), [&](const NamespaceDef &lhs) { return lhs.name == tok; }) != namespaces.end();
}

void SQLSuggester::checkForTokenSuggestions(SqlParsingCtx::SuggestionData &data, const SqlParsingCtx &ctx, const Namespaces &namespaces) {
	switch (data.tokenType) {
		case Start:
		case StartAfterExplain:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case SingleSelectFieldSqlToken: {
			if (isBlank(data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (data.token == "*") break;
			bool isIndex = false, isAggregationFunction = false;
			isIndex = findInPossibleIndexes(data.token, ctx.suggestionLinkedNs, namespaces);
			if (!isIndex) isAggregationFunction = findInPossibleTokens(AggregationSqlToken, data.token);
			if (!isIndex && !isAggregationFunction) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
		} break;
		case SelectFieldsListSqlToken: {
			if (isBlank(data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}

			if ((data.token == ",") || (data.token == "(")) break;

			bool fromKeywordReached = false;
			if (ctx.tokens.size() > 1) {
				int prevTokenType = ctx.tokens.back();
				if ((prevTokenType == SingleSelectFieldSqlToken) || (prevTokenType == SelectFieldsListSqlToken)) {
					fromKeywordReached = checkIfStartsWith(data.token, "from");
					if (fromKeywordReached && data.token.length() < strlen("from")) {
						getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
					}
				}
			}

			if (!fromKeywordReached && !findInPossibleIndexes(data.token, ctx.suggestionLinkedNs, namespaces)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
		} break;
		case FromSqlToken:
			if (isBlank(data.token) || !iequals(data.token, "from")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case NamespaceSqlToken:
			if (isBlank(data.token) || !findInPossibleNamespaces(data.token, namespaces)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case SelectConditionsStart:
		case DeleteConditionsStart:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case WhereFieldSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (iequals(data.token, "not")) break;
			if (!findInPossibleIndexes(data.token, ctx.suggestionLinkedNs, namespaces)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case ConditionSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case WhereFieldValueSqlToken:
			if (isBlank(data.token)) break;
			if (checkIfTokenStartsWith(data.token, "null")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "empty")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "not")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case WhereFieldNegateValueSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case WhereOpSqlToken:
		case OpSqlToken:
			if (isBlank(data.token)) {
				switch (ctx.tokens.back()) {
					case WhereSqlToken:
						data.tokenType = FieldNameSqlToken;
						break;
					case OnSqlToken:
						data.tokenType = NamespaceSqlToken;
						break;
					default:
						break;
				}
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "and")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (checkIfTokenStartsWith(data.token, "or")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if ((data.tokenType == WhereOpSqlToken) && (ctx.tokens.size() > 1)) {
				int prevTokenType = ctx.tokens.back();
				if ((prevTokenType != WhereSqlToken) && checkIfTokenStartsWith(data.token, "order")) {
					getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
					break;
				}
			}
			break;
		case AndSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (findInPossibleIndexes(data.token, ctx.suggestionLinkedNs, namespaces)) break;
			if (checkIfTokenStartsWith(data.token, "not")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			break;
		case FieldNameSqlToken:
			if (isBlank(data.token) || !findInPossibleIndexes(data.token, ctx.suggestionLinkedNs, namespaces)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
			}
			break;
		case SortDirectionSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (data.token == "(") break;
			if (checkIfTokenStartsWith(data.token, "field")) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			if (!findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			break;
		case JoinedFieldNameSqlToken:
			if (isBlank(data.token) || !findInPossibleIndexes(data.token, ctx.suggestionLinkedNs, namespaces) ||
				!findInPossibleNamespaces(data.token, namespaces)) {
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
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
				getSuggestionsForToken(data, ctx.suggestionLinkedNs, namespaces);
				break;
			}
			break;
		default:
			break;
	}
}

}  // namespace reindexer
