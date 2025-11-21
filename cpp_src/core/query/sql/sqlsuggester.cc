
#include "sqlsuggester.h"
#include <unordered_map>
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/system_ns_names.h"
#include "sqltokentype.h"

namespace reindexer {

static bool checkIfTokenStartsWith(std::string_view src, std::string_view pattern) {
	return checkIfStartsWith(src, pattern) && src.length() < pattern.length();
}

std::vector<std::string> SQLSuggester::GetSuggestions(std::string_view q, size_t pos, EnumNamespacesF enumNamespaces,
													  GetSchemaF getSchema) {
	Query query;
	SQLSuggester suggester{query};
	suggester.ctx_.suggestionsPos = pos;
	suggester.ctx_.autocompleteMode = true;
	suggester.enumNamespaces_ = std::move(enumNamespaces);
	suggester.getSchema_ = std::move(getSchema);

	try {
		Tokenizer tokens{q};
		std::ignore = suggester.Parse(tokens);
		// NOLINTBEGIN(bugprone-empty-catch)
	} catch (const Error&) {
	}
	// NOLINTEND(bugprone-empty-catch)

	for (SqlParsingCtx::SuggestionData& item : suggester.ctx_.suggestions) {
		suggester.checkForTokenSuggestions(item);
	}

	for (auto& it : suggester.ctx_.suggestions) {
		if (!it.variants.empty()) {
			return {it.variants.begin(), it.variants.end()};
		}
	}
	return {};
}

static const std::unordered_map<SqlTokenType, std::unordered_set<std::string>> sqlTokenMatchings = {
	{Start, {"explain", "select", "delete", "update", "truncate", "local"}},
	{StartAfterLocal, {"explain", "select"}},
	{StartAfterExplain, {"select", "delete", "update", "local"}},
	{StartAfterLocalExplain, {"select"}},
	{AggregationSqlToken, {"sum", "avg", "max", "min", "facet", "count", "distinct", "rank()", "count_cached", "vectors()"}},
	{SelectConditionsStart, {"where", "limit", "offset", "order", "join", "left", "inner", "equal_position", "merge", "or", ";"}},
	{NestedSelectConditionsStart, {"where", "limit", "offset", "order", "equal_position"}},
	{ConditionSqlToken, {">", ">=", "<", "<=", "<>", "in", "allset", "range", "is", "==", "="}},
	{WhereFieldValueSqlToken, {"null", "empty", "not"}},
	{WhereFieldNegateValueSqlToken, {"null", "empty"}},
	{OpSqlToken, {"and", "or"}},
	{WhereOpSqlToken, {"and", "or", "order", "equal_position"}},
	{SortDirectionSqlToken, {"asc", "desc"}},
	{JoinTypesSqlToken, {"join", "left", "inner"}},
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
	{ModifyConditionsStart, {"where", "limit", "offset", "order"}},
	{UpdateOptionsSqlToken, {"set", "drop"}},
	{EqualPositionSqlToken, {"equal_position"}},
	{WhereFunction, {"ST_DWithin", "KNN"}},
	{ST_GeomFromTextSqlToken, {"ST_GeomFromText"}},
	{KnnParamsToken,
	 {std::string{KnnSearchParams::kKName}, std::string{KnnSearchParams::kEfName}, std::string{KnnSearchParams::kNProbeName}}}};

static void getMatchingTokens(SqlTokenType tokenType, const std::string& token, std::unordered_set<std::string>& variants) {
	const auto suggestionsIt = sqlTokenMatchings.find(tokenType);
	if (suggestionsIt == sqlTokenMatchings.end()) {
		return;
	}
	const auto& suggestions = suggestionsIt->second;
	for (auto it = suggestions.begin(); it != suggestions.end(); ++it) {
		if (isBlank(token) || checkIfStartsWith(token, *it)) {
			variants.insert(*it);
		}
	}
}

void SQLSuggester::getMatchingNamespacesNames(const std::string& token, std::unordered_set<std::string>& variants) {
	auto namespaces = enumNamespaces_(EnumNamespacesOpts().OnlyNames());
	for (auto& ns : namespaces) {
		if (isBlank(token) || checkIfStartsWith(token, ns.name)) {
			variants.insert(ns.name);
		}
	}
}

void SQLSuggester::getMatchingFieldsNames(const std::string& token, std::unordered_set<std::string>& variants) {
	auto namespaces = enumNamespaces_(EnumNamespacesOpts().WithFilter(ctx_.suggestionLinkedNs));

	if (namespaces.empty() || (namespaces.size() > 1 && isBlank(token))) {
		return;
	}
	auto dotPos = token.find('.');
	for (const auto& ns : namespaces) {
		if (ns.name == kReplicationStatsNamespace) {
			// Do not suggest fields from #replicationstats - they are rarely be usefull
			continue;
		}
		for (auto& idx : ns.indexes) {
			if (idx.Name() == "#pk" || idx.Name() == "-tuple") {
				continue;
			}
			if (isBlank(token) || (dotPos != std::string::npos ? checkIfStartsWith<CaseSensitive::Yes>(token, idx.Name())
															   : checkIfStartsWith<CaseSensitive::No>(token, idx.Name()))) {
				if (dotPos == std::string::npos) {
					variants.insert(idx.Name());
				} else {
					variants.insert(idx.Name().substr(dotPos));
				}
			}
		}
	}

	if (getSchema_) {
		for (const auto& ns : namespaces) {
			if (ns.name == kReplicationStatsNamespace) {
				// Do not suggest fields from #replicationstats - they are rarely be usefull
				continue;
			}
			auto schema = getSchema_(ns.name);
			if (schema) {
				auto fieldsSuggestions = schema->GetSuggestions(token);
				for (auto& suggestion : fieldsSuggestions) {
					variants.insert(std::move(suggestion));
				}
			}
		}
	}
}

void SQLSuggester::getSuggestionsForToken(SqlParsingCtx::SuggestionData& ctx) {
	switch (ctx.tokenType) {
		case Start:
		case StartAfterExplain:
		case StartAfterLocal:
		case StartAfterLocalExplain:
		case FromSqlToken:
		case SelectConditionsStart:
		case NestedSelectConditionsStart:
		case ModifyConditionsStart:
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
		case UpdateOptionsSqlToken:
		case WhereFunction:
		case KnnParamsToken:
			getMatchingTokens(ctx.tokenType, ctx.token, ctx.variants);
			break;
		case SingleSelectFieldSqlToken:
			getMatchingTokens(AllFieldsToken, ctx.token, ctx.variants);
			getMatchingTokens(AggregationSqlToken, ctx.token, ctx.variants);
			getMatchingFieldsNames(ctx.token, ctx.variants);
			break;
		case NamespaceSqlToken:
			getMatchingNamespacesNames(ctx.token, ctx.variants);
			break;
		case WhereFieldOrSubquerySqlToken:
			getMatchingTokens(SelectSqlToken, ctx.token, ctx.variants);
			[[fallthrough]];
		case AndSqlToken:
		case WhereFieldSqlToken:
			getMatchingTokens(JoinTypesSqlToken, ctx.token, ctx.variants);
			[[fallthrough]];
		case NestedAndSqlToken:
		case NestedWhereFieldSqlToken:
			getMatchingTokens(NotSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(WhereFunction, ctx.token, ctx.variants);
			getMatchingFieldsNames(ctx.token, ctx.variants);
			getMatchingTokens(EqualPositionSqlToken, ctx.token, ctx.variants);
			break;
		case GeomFieldSqlToken:
			getMatchingTokens(ST_GeomFromTextSqlToken, ctx.token, ctx.variants);
			getMatchingFieldsNames(ctx.token, ctx.variants);
			break;
		case FieldNameSqlToken:
			getMatchingFieldsNames(ctx.token, ctx.variants);
			break;
		case SortDirectionSqlToken:
			getMatchingTokens(SortDirectionSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(FieldSqlToken, ctx.token, ctx.variants);
			break;
		case JoinedFieldNameSqlToken:
			getMatchingNamespacesNames(ctx.token, ctx.variants);
			getMatchingFieldsNames(ctx.token, ctx.variants);
			break;
		case WhereFieldValueOrSubquerySqlToken:
			getMatchingTokens(SelectSqlToken, ctx.token, ctx.variants);
			getMatchingTokens(WhereFieldValueSqlToken, ctx.token, ctx.variants);
			break;
		case DeleteSqlToken:
		case AggregationSqlToken:
		case NullSqlToken:
		case EmptySqlToken:
		case NotSqlToken:
		case OrSqlToken:
		case AllFieldsToken:
		case FieldSqlToken:
		case JoinSqlToken:
		case MergeSqlToken:
		case EqualPositionSqlToken:
		case JoinTypesSqlToken:
		case ST_GeomFromTextSqlToken:
		default:
			break;
	}
}

bool SQLSuggester::findInPossibleTokens(SqlTokenType type, const std::string& v) {
	const auto it = sqlTokenMatchings.find(type);
	return it == sqlTokenMatchings.end() || (it->second.find(v) != it->second.end());
}

bool SQLSuggester::findInPossibleFields(const std::string& tok) {
	auto namespaces = enumNamespaces_(EnumNamespacesOpts().WithFilter(ctx_.suggestionLinkedNs));

	if (namespaces.empty()) {
		return false;
	}
	if (std::find_if(namespaces[0].indexes.begin(), namespaces[0].indexes.end(), [&](const IndexDef& lhs) { return lhs.Name() == tok; }) !=
		namespaces[0].indexes.end()) {
		return true;
	}
	if (getSchema_) {
		auto schema = getSchema_(namespaces[0].name);
		return schema && schema->HasPath(tok);
	}
	return false;
}

bool SQLSuggester::findInPossibleNamespaces(const std::string& tok) {
	return !enumNamespaces_(EnumNamespacesOpts().WithFilter(tok).OnlyNames()).empty();
}

void SQLSuggester::checkForTokenSuggestions(SqlParsingCtx::SuggestionData& data) {
	switch (data.tokenType) {
		case Start:
		case StartAfterExplain:
		case StartAfterLocal:
		case StartAfterLocalExplain:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case SingleSelectFieldSqlToken: {
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (data.token == "*") {
				break;
			}
			bool isIndex = false, isAggregationFunction = false;
			isIndex = findInPossibleFields(data.token);
			if (!isIndex) {
				isAggregationFunction = findInPossibleTokens(AggregationSqlToken, data.token);
			}
			if (!isIndex && !isAggregationFunction) {
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
		case NestedSelectConditionsStart:
		case ModifyConditionsStart:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case GeomFieldSqlToken:
		case WhereFieldSqlToken:
		case NestedWhereFieldSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (iequals(data.token, "not")) {
				break;
			}
			if (!findInPossibleFields(data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case ConditionSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case WhereFieldValueSqlToken:
			if (isBlank(data.token)) {
				break;
			}
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
					case Start:
					case StartAfterLocal:
					case StartAfterLocalExplain:
					case SelectSqlToken:
					case DeleteSqlToken:
					case StartAfterExplain:
					case SingleSelectFieldSqlToken:
					case AggregationSqlToken:
					case FromSqlToken:
					case NamespaceSqlToken:
					case SelectConditionsStart:
					case NestedSelectConditionsStart:
					case WhereFieldSqlToken:
					case NestedWhereFieldSqlToken:
					case ConditionSqlToken:
					case OpSqlToken:
					case WhereOpSqlToken:
					case FieldNameSqlToken:
					case WhereFieldValueSqlToken:
					case WhereFieldNegateValueSqlToken:
					case NullSqlToken:
					case EmptySqlToken:
					case NotSqlToken:
					case AndSqlToken:
					case NestedAndSqlToken:
					case OrSqlToken:
					case BySqlToken:
					case AllFieldsToken:
					case SortDirectionSqlToken:
					case FieldSqlToken:
					case LeftSqlToken:
					case InnerSqlToken:
					case JoinSqlToken:
					case MergeSqlToken:
					case JoinedFieldNameSqlToken:
					case ModifyConditionsStart:
					case SetSqlToken:
					case UpdateOptionsSqlToken:
					case EqualPositionSqlToken:
					case JoinTypesSqlToken:
					case WhereFunction:
					case ST_GeomFromTextSqlToken:
					case GeomFieldSqlToken:
					case WhereFieldOrSubquerySqlToken:
					case WhereFieldValueOrSubquerySqlToken:
					case KnnParamsToken:
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
				if ((prevTokenType != WhereSqlToken) &&
					(checkIfTokenStartsWith(data.token, "order") || checkIfTokenStartsWith(data.token, "equal_position"))) {
					getSuggestionsForToken(data);
					break;
				}
			}
			break;
		case AndSqlToken:
		case NestedAndSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (findInPossibleFields(data.token)) {
				break;
			}
			getSuggestionsForToken(data);
			break;
		case FieldNameSqlToken:
			if (isBlank(data.token) || !findInPossibleFields(data.token)) {
				getSuggestionsForToken(data);
			}
			break;
		case SortDirectionSqlToken:
			if (isBlank(data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			if (data.token == "(") {
				break;
			}
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
			if (isBlank(data.token) || !findInPossibleFields(data.token) || !findInPossibleNamespaces(data.token)) {
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
		case UpdateOptionsSqlToken:
			if (isBlank(data.token) || !findInPossibleTokens(data.tokenType, data.token)) {
				getSuggestionsForToken(data);
				break;
			}
			break;
		case DeleteSqlToken:
		case AggregationSqlToken:
		case NullSqlToken:
		case EmptySqlToken:
		case NotSqlToken:
		case OrSqlToken:
		case AllFieldsToken:
		case FieldSqlToken:
		case JoinSqlToken:
		case MergeSqlToken:
		case EqualPositionSqlToken:
		case JoinTypesSqlToken:
		case WhereFunction:
		case ST_GeomFromTextSqlToken:
		case WhereFieldOrSubquerySqlToken:
		case WhereFieldValueOrSubquerySqlToken:
		case KnnParamsToken:
		default:
			getSuggestionsForToken(data);
			break;
	}
}

}  // namespace reindexer
