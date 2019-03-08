
#include "core/query/query.h"
#include "core/namespace.h"
#include "core/query/aggregationresult.h"
#include "core/query/dslencoder.h"
#include "core/query/dslparsetools.h"
#include "core/type_consts.h"
#include "gason/gason.h"
#include "sqltokentype.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "tools/serializer.h"

#include <set>
#include <unordered_map>

using std::set;
using std::unordered_map;

namespace reindexer {

Query::Query(const string &__namespace, unsigned _start, unsigned _count, CalcTotalMode _calcTotal)
	: _namespace(__namespace), calcTotal(_calcTotal), start(_start), count(_count) {}

bool Query::operator==(const Query &obj) const {
	if (!QueryWhere::operator==(obj)) return false;

	if (nextOp_ != obj.nextOp_) return false;
	if (_namespace != obj._namespace) return false;
	if (sortingEntries_ != obj.sortingEntries_) return false;
	if (calcTotal != obj.calcTotal) return false;
	if (start != obj.start) return false;
	if (count != obj.count) return false;
	if (debugLevel != obj.debugLevel) return false;
	if (joinType != obj.joinType) return false;
	if (forcedSortOrder != obj.forcedSortOrder) return false;

	if (selectFilter_ != obj.selectFilter_) return false;
	if (selectFunctions_ != obj.selectFunctions_) return false;
	if (joinQueries_ != obj.joinQueries_) return false;
	if (mergeQueries_ != obj.mergeQueries_) return false;
	if (updateFields_ != obj.updateFields_) return false;

	return true;
}

int Query::FromSQL(const string_view &q) {
	tokenizer parser(q);
	SqlParsingCtx ctx;
	return Parse(parser, ctx);
}

Error Query::ParseJson(const string &dsl) {
	try {
		parseJson(dsl);
	} catch (const Error &e) {
		return e;
	}
	return Error();
}

void Query::parseJson(const string &dsl) {
	JsonAllocator allocator;
	JsonValue root;
	char *endptr = nullptr;
	char *src = const_cast<char *>(dsl.data());

	auto error = jsonParse(src, &endptr, &root, allocator);
	if (error != JSON_OK) {
		throw Error(errParseJson, "Could not parse JSON-query: %s at %d", jsonStrError(error), int(endptr - src));
	}
	dsl::parse(root, *this);
}

void Query::deserialize(Serializer &ser) {
	while (!ser.Eof()) {
		QueryEntry qe;
		QueryJoinEntry qje;

		int qtype = ser.GetVarUint();
		switch (qtype) {
			case QueryCondition: {
				qe.index = ser.GetVString().ToString();
				qe.op = OpType(ser.GetVarUint());
				qe.condition = CondType(ser.GetVarUint());
				int count = ser.GetVarUint();
				qe.values.reserve(count);
				while (count--) qe.values.push_back(ser.GetVariant().EnsureHold());
				entries.push_back(std::move(qe));
				break;
			}
			case QueryAggregation:
				aggregations_.push_back({ser.GetVString().ToString(), AggType(ser.GetVarUint())});
				break;
			case QueryDistinct:
				qe.index = ser.GetVString().ToString();
				if (!qe.index.empty()) {
					qe.distinct = true;
					qe.condition = CondAny;
					entries.push_back(std::move(qe));
				}
				break;
			case QuerySortIndex: {
				SortingEntry sortingEntry;
				sortingEntry.column = ser.GetVString().ToString();
				sortingEntry.desc = bool(ser.GetVarUint());
				if (sortingEntry.column.length()) {
					sortingEntries_.push_back(std::move(sortingEntry));
				}
				int count = ser.GetVarUint();
				forcedSortOrder.reserve(count);
				while (count--) forcedSortOrder.push_back(ser.GetVariant().EnsureHold());
				break;
			}
			case QueryJoinOn:
				qje.op_ = OpType(ser.GetVarUint());
				qje.condition_ = CondType(ser.GetVarUint());
				qje.index_ = ser.GetVString().ToString();
				qje.joinIndex_ = ser.GetVString().ToString();
				joinEntries_.push_back(std::move(qje));
				break;
			case QueryDebugLevel:
				debugLevel = ser.GetVarUint();
				break;
			case QueryLimit:
				count = ser.GetVarUint();
				break;
			case QueryOffset:
				start = ser.GetVarUint();
				break;
			case QueryReqTotal:
				calcTotal = CalcTotalMode(ser.GetVarUint());
				break;
			case QuerySelectFilter:
				selectFilter_.push_back(ser.GetVString().ToString());
				break;
			case QueryEqualPosition: {
				vector<string> ep(ser.GetVarUint());
				for (size_t i = 0; i < ep.size(); ++i) ep[i] = ser.GetVString().ToString();
				equalPositions_.push_back(determineEqualPositionIndexes(ep));
				break;
			}
			case QueryExplain:
				explain_ = true;
				break;
			case QuerySelectFunction:
				selectFunctions_.push_back(ser.GetVString().ToString());
				break;
			case QueryUpdateField: {
				updateFields_.push_back({ser.GetVString().ToString(), {}});
				auto &field = updateFields_.back();
				int numValues = ser.GetVarUint();
				while (numValues--) {
					ser.GetVarUint();  // type: function/value
					field.second.push_back(ser.GetVariant());
				}
				break;
			}
			case QueryEnd:
				return;
		}
	}
}

bool checkIfTokenStartsWith(const string_view &src, const string_view &pattern) {
	return checkIfStartsWith(src, pattern) && src.length() < pattern.length();
}

vector<string> Query::GetSuggestions(const string_view &q, size_t pos, const Namespaces &namespaces) {
	SqlParsingCtx ctx;
	ctx.suggestionsPos = pos;
	ctx.autocompleteMode = true;

	try {
		tokenizer parser(q);
		Parse(parser, ctx);
	} catch (const Error &) {
	}

	for (SqlParsingCtx::SuggestionData &item : ctx.suggestions) {
		checkForTokenSuggestions(item, ctx, namespaces);
	}

	if (ctx.suggestions.size() > 0) return ctx.suggestions.front().variants;
	return std::vector<string>();
}

unordered_map<int, set<string>> sqlTokenMatchings = {
	{Start, {"explain", "select", "delete", "update"}},
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
	const set<string> &suggestions = sqlTokenMatchings[tokenType];
	for (auto it = suggestions.begin(); it != suggestions.end(); ++it) {
		if (isBlank(token) || checkIfStartsWith(token, *it)) variants.push_back(*it);
	}
}

void getMatchingNamespacesNames(const Namespaces &namespaces, const string &token, vector<string> &variants) {
	for (auto it = namespaces.begin(); it != namespaces.end(); ++it) {
		if (isBlank(token) || checkIfStartsWith(token, it->first)) variants.push_back(it->first);
	}
}

void Query::getMatchingIndexesNames(const Namespaces &namespaces, const string &nsName, const string &token, vector<string> &variants) {
	auto itNs = namespaces.find(nsName);
	if (itNs == namespaces.end()) return;
	Namespace::Ptr ns = itNs->second;
	for (auto it = ns->indexesNames_.begin(); it != ns->indexesNames_.end(); ++it) {
		if (it->first == "#pk" || it->first == "-tuple") continue;
		if (isBlank(token) || checkIfStartsWith(token, it->first)) variants.push_back(it->first);
	}
}

void Query::getSuggestionsForToken(SqlParsingCtx::SuggestionData &ctx, const string &nsName, const Namespaces &namespaces) {
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

bool Query::findInPossibleTokens(int type, const string &v) {
	const set<string> &values = sqlTokenMatchings[type];
	return (values.find(v) != values.end());
}

bool Query::findInPossibleIndexes(const string &tok, const string &nsName, const Namespaces &namespaces) {
	auto itNs = namespaces.find(nsName);
	if (itNs == namespaces.end()) return false;
	Namespace::Ptr ns = itNs->second;
	return ns->indexesNames_.find(tok) != ns->indexesNames_.end();
}

bool Query::findInPossibleNamespaces(const string &tok, const Namespaces &namespaces) { return namespaces.find(tok) != namespaces.end(); }

bool Query::reachedAutocompleteToken(tokenizer &parser, const token &tok, SqlParsingCtx &ctx) const {
	if (!ctx.foundPossibleSuggestions) {
		size_t pos = parser.pos() + tok.text().length();
		if ((parser.pos() == 0) && (tok.text().length() > 0)) --pos;
		if ((pos == 0) && (parser.length() == 0)) return true;
		return (pos >= ctx.suggestionsPos);
	}
	return false;
}

token Query::peekSqlToken(tokenizer &parser, SqlParsingCtx &ctx, int tokenType, bool toLower) {
	token tok = parser.peek_token(toLower);
	bool eof = ((parser.pos() + tok.text().length()) == parser.length());
	if (ctx.autocompleteMode && ctx.suggestions.empty() && reachedAutocompleteToken(parser, tok, ctx)) {
		int tokenLength = 0;
		if (eof) {
			tokenLength = tok.text().length();
		} else {
			tokenLength = ctx.suggestionsPos - parser.pos();
			if (parser.pos() == 0) ++tokenLength;
			if (ctx.suggestionsPos == parser.pos()) tokenLength = 0;
		}
		if (tokenLength < 0) tokenLength = 0;
		ctx.suggestions.emplace_back(string(tok.text_.data(), tokenLength), tokenType);
		ctx.foundPossibleSuggestions = true;
		ctx.possibleSuggestionDetectedInThisClause = true;
	}
	if (!ctx.foundPossibleSuggestions) ctx.tokens.push_back(tokenType);
	if (eof && ctx.autocompleteMode) throw Error(errLogic, "Query eof is reached!");
	return tok;
}

void Query::checkForTokenSuggestions(SqlParsingCtx::SuggestionData &data, const SqlParsingCtx &ctx, const Namespaces &namespaces) {
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

int Query::Parse(tokenizer &parser, SqlParsingCtx &ctx) {
	token tok = peekSqlToken(parser, ctx, Start);
	if (tok.text() == "explain"_sv) {
		explain_ = true;
		parser.next_token();
		tok = peekSqlToken(parser, ctx, StartAfterExplain);
	}

	if (tok.text() == "select"_sv) {
		type_ = QuerySelect;
		parser.next_token();
		selectParse(parser, ctx);
	} else if (tok.text() == "delete"_sv) {
		type_ = QueryDelete;
		tok = parser.next_token();
		deleteParse(parser, ctx);
	} else if (tok.text() == "update"_sv) {
		type_ = QueryUpdate;
		updateParse(parser, ctx);
	} else {
		throw Error(errParams, "Syntax error at or near '%s', %s", tok.text(), parser.where());
	}

	tok = parser.next_token();
	if (tok.text() == ";") {
		tok = parser.next_token();
	}
	parser.skip_space();
	if (tok.text() != "" || !parser.end()) throw Error(errParseSQL, "Unexpected '%s' in query, %s", tok.text(), parser.where());

	return 0;
}

int Query::selectParse(tokenizer &parser, SqlParsingCtx &ctx) {
	// Get filter
	token tok;
	bool wasSelectFilter = false;
	while (!parser.end()) {
		auto nameWithCase = peekSqlToken(parser, ctx, SingleSelectFieldSqlToken, false);
		auto name = parser.next_token();
		tok = peekSqlToken(parser, ctx, SelectFieldsListSqlToken);
		if (tok.text() == "("_sv) {
			parser.next_token();
			tok = peekSqlToken(parser, ctx, SingleSelectFieldSqlToken);
			AggType agg = AggregationResult::strToAggType(name.text());
			if (agg != AggUnknown) {
				aggregations_.push_back({tok.text().ToString(), agg});
			} else if (name.text() == "count"_sv) {
				calcTotal = ModeAccurateTotal;
				if (!wasSelectFilter) count = 0;
			} else if (name.text() == "distinct"_sv) {
				Distinct(tok.text().ToString());
				if (!wasSelectFilter) selectFilter_.push_back(tok.text().ToString());
			} else {
				throw Error(errParams, "Unknown function name SQL - %s, %s", name.text(), parser.where());
			}
			tok = parser.next_token();
			tok = parser.peek_token();
			if (tok.text() != ")"_sv) {
				throw Error(errParams, "Expected ')', but found %s, %s", tok.text(), parser.where());
			}
			tok = parser.next_token();
			tok = peekSqlToken(parser, ctx, SelectFieldsListSqlToken);

		} else if (name.text() != "*"_sv) {
			selectFilter_.push_back(nameWithCase.text().ToString());
			count = INT_MAX;
			wasSelectFilter = true;
		} else if (name.text() == "*"_sv) {
			count = INT_MAX;
			wasSelectFilter = true;
			selectFilter_.clear();
		}
		if (tok.text() != ","_sv) break;
		tok = parser.next_token();
	}

	peekSqlToken(parser, ctx, FromSqlToken);
	if (parser.next_token().text() != "from"_sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text(), parser.where());

	peekSqlToken(parser, ctx, NamespaceSqlToken);
	_namespace = parser.next_token().text().ToString();
	ctx.updateLinkedNs(_namespace);

	while (!parser.end()) {
		tok = peekSqlToken(parser, ctx, SelectConditionsStart);
		if (tok.text() == "where"_sv) {
			parser.next_token();
			parseWhere(parser, ctx);
		} else if (tok.text() == "limit"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			count = stoi(tok.text());
		} else if (tok.text() == "offset"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			start = stoi(tok.text());
		} else if (tok.text() == "order"_sv) {
			parser.next_token();
			parseOrderBy(parser, ctx);
			ctx.updateLinkedNs(_namespace);
		} else if (tok.text() == "join"_sv) {
			parser.next_token();
			parseJoin(JoinType::LeftJoin, parser, ctx);
		} else if (tok.text() == "left"_sv) {
			parser.next_token();
			peekSqlToken(parser, ctx, LeftSqlToken);
			if (parser.next_token().text() != "join"_sv) {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
			}
			parseJoin(JoinType::LeftJoin, parser, ctx);
		} else if (tok.text() == "inner"_sv) {
			parser.next_token();
			peekSqlToken(parser, ctx, InnerSqlToken);
			if (parser.next_token().text() != "join") {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
			}
			auto jtype = nextOp_ == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
			nextOp_ = OpAnd;
			parseJoin(jtype, parser, ctx);
		} else if (tok.text() == "merge"_sv) {
			parser.next_token();
			parseMerge(parser, ctx);
		} else if (tok.text() == "or"_sv) {
			parser.next_token();
			nextOp_ = OpOr;
		} else {
			break;
		}
	}
	return 0;
}

int Query::parseOrderBy(tokenizer &parser, SqlParsingCtx &ctx) {
	// Just skip token (BY)
	peekSqlToken(parser, ctx, BySqlToken);
	parser.next_token();
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, ctx, FieldNameSqlToken);
		auto tok = parser.next_token(false);
		if (tok.type != TokenName && tok.type != TokenString)
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		SortingEntry sortingEntry;
		sortingEntry.column = tok.text().ToString();
		tok = peekSqlToken(parser, ctx, SortDirectionSqlToken);
		if (tok.text() == "("_sv && nameWithCase.text() == "field"_sv) {
			parser.next_token();
			tok = peekSqlToken(parser, ctx, FieldNameSqlToken, false);
			if (tok.type != TokenName && tok.type != TokenString)
				throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
			sortingEntry.column = tok.text().ToString();
			tok = parser.next_token(false);
			for (;;) {
				tok = parser.next_token();
				if (tok.text() == ")"_sv) break;
				if (tok.text() != ","_sv)
					throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
				tok = peekSqlToken(parser, ctx, FieldNameSqlToken);
				if (tok.type != TokenNumber && tok.type != TokenString)
					throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", tok.text(), parser.where());
				forcedSortOrder.push_back(Variant(tok.text().ToString()));
				parser.next_token();
			}
			tok = parser.peek_token();
		}

		if (tok.text() == "asc"_sv || tok.text() == "desc"_sv) {
			sortingEntry.desc = bool(tok.text() == "desc"_sv);
			parser.next_token();
		}
		sortingEntries_.push_back(std::move(sortingEntry));

		auto nextToken = parser.peek_token();
		if (nextToken.text() != ","_sv) break;
		parser.next_token();
	}
	return 0;
}

int Query::deleteParse(tokenizer &parser, SqlParsingCtx &ctx) {
	// Get filter
	token tok;

	peekSqlToken(parser, ctx, FromSqlToken);
	if (parser.next_token().text() != "from"_sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text(), parser.where());

	peekSqlToken(parser, ctx, NamespaceSqlToken);
	_namespace = parser.next_token().text().ToString();
	ctx.updateLinkedNs(_namespace);

	while (!parser.end()) {
		tok = peekSqlToken(parser, ctx, DeleteConditionsStart);
		if (tok.text() == "where"_sv) {
			parser.next_token();
			parseWhere(parser, ctx);
		} else if (tok.text() == "limit"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			count = stoi(tok.text());
		} else if (tok.text() == "offset"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			start = stoi(tok.text());
		} else if (tok.text() == "order"_sv) {
			parser.next_token();
			parseOrderBy(parser, ctx);
			ctx.updateLinkedNs(_namespace);
		} else
			break;
	}
	return 0;
}

static Variant token2kv(const token &tok, tokenizer &parser) {
	if (tok.text() == "true"_sv) return Variant(true);
	if (tok.text() == "false"_sv) return Variant(false);

	if (tok.type != TokenNumber && tok.type != TokenString)
		throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", tok.text(), parser.where());

	auto text = tok.text();
	if (tok.type == TokenNumber) {
		bool digit = text.length() < 21 && text.length() > 0;
		bool flt = false;

		unsigned i = 0;
		if (digit && (text[i] == '+' || text[i] == '-')) {
			i++;
		}
		if (digit && i < text.length() && text[i] == '0') digit = false;

		for (; i < text.length() && digit; i++) {
			if (text[i] == '.')
				flt = true;
			else if (!isdigit(text[i]))
				digit = false;
		}
		if (digit && text.length()) {
			char *p = 0;
			if (!flt)
				return Variant(int64_t(stoll(text)));
			else
				return Variant(double(strtod(text.data(), &p)));
		}
	}
	return Variant(make_key_string(text.data(), text.length()));
}

int Query::updateParse(tokenizer &parser, SqlParsingCtx &ctx) {
	parser.next_token();

	token tok = peekSqlToken(parser, ctx, NamespaceSqlToken);
	_namespace = tok.text().ToString();
	ctx.updateLinkedNs(_namespace);
	parser.next_token();

	tok = peekSqlToken(parser, ctx, SetSqlToken);
	if (tok.text() != "set"_sv) throw Error(errParams, "Expected 'SET', but found '%s' in query, %s", tok.text(), parser.where());
	parser.next_token();

	while (!parser.end()) {
		pair<string, VariantArray> field;

		tok = peekSqlToken(parser, ctx, FieldNameSqlToken);
		if (tok.type != TokenName && tok.type != TokenString)
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		field.first = tok.text().ToString();
		parser.next_token();

		tok = parser.next_token();
		if (tok.text() != "="_sv) throw Error(errParams, "Expected '=' but found '%s' in query, '%s'", tok.text(), parser.where());

		tok = parser.next_token();

		if (tok.text() == "("_sv) {
			for (;;) {
				tok = parser.next_token();
				field.second.push_back(token2kv(tok, parser));
				tok = parser.next_token();
				if (tok.text() == ")"_sv) break;
				if (tok.text() != ","_sv)
					throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
			}
		} else {
			field.second.push_back(token2kv(tok, parser));
		}
		updateFields_.push_back(std::move(field));

		tok = parser.peek_token();
		if (tok.text() != ","_sv) break;
		parser.next_token();
	}

	tok = peekSqlToken(parser, ctx, WhereSqlToken);
	if (tok.text() != "where"_sv) throw Error(errParams, "Expected 'WHERE', but found '%s' in query, %s", tok.text(), parser.where());
	parser.next_token();
	parseWhere(parser, ctx);

	return 0;
}

int Query::parseWhere(tokenizer &parser, SqlParsingCtx &ctx) {
	token tok;
	OpType nextOp = OpAnd;

	tok = peekSqlToken(parser, ctx, WhereFieldSqlToken, false);

	if (iequals(tok.text(), "not"_sv)) {
		nextOp = OpNot;
		parser.next_token();
	}

	while (!parser.end()) {
		QueryEntry entry;
		entry.op = nextOp;
		// Just skip token.
		tok = parser.next_token(false);

		if (tok.text() == "("_sv) {
			throw Error(errParseSQL, "Found '(' - nested queries are not supported, %s", parser.where());

		} else if (tok.type == TokenName || tok.type == TokenString) {
			// Index name
			entry.index = tok.text().ToString();

			// Operator
			tok = peekSqlToken(parser, ctx, ConditionSqlToken);
			if (tok.text() == "<>"_sv) {
				entry.condition = CondEq;
				if (entry.op == OpAnd)
					entry.op = OpNot;
				else if (entry.op == OpNot)
					entry.op = OpAnd;
				else {
					throw Error(errParseSQL, "<> condition with OR is not supported, %s", parser.where());
				}
			} else {
				entry.condition = getCondType(tok.text());
			}
			tok = parser.next_token();

			// Value
			if (ctx.autocompleteMode) tok = peekSqlToken(parser, ctx, WhereFieldValueSqlToken, false);
			tok = parser.next_token();
			if (iequals(tok.text(), "null"_sv) || iequals(tok.text(), "empty"_sv)) {
				entry.condition = CondEmpty;
			} else if (iequals(tok.text(), "not"_sv)) {
				tok = peekSqlToken(parser, ctx, WhereFieldNegateValueSqlToken, false);
				if (iequals(tok.text(), "null"_sv) || iequals(tok.text(), "empty"_sv)) {
					entry.condition = CondAny;
				} else {
					throw Error(errParseSQL, "Expected NULL, but found '%s' in query, %s", tok.text(), parser.where());
				}
				tok = parser.next_token(false);
			}

			else if (tok.text() == "("_sv) {
				for (;;) {
					tok = parser.next_token();
					entry.values.push_back(token2kv(tok, parser));
					tok = parser.next_token();
					if (tok.text() == ")"_sv) break;
					if (tok.text() != ","_sv)
						throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
				}
			} else {
				entry.values.push_back(token2kv(tok, parser));
			}
		}
		// Push back parsed entry
		entries.push_back(entry);

		tok = peekSqlToken(parser, ctx, WhereOpSqlToken, false);

		if (iequals(tok.text(), "and"_sv)) {
			nextOp = OpAnd;
			parser.next_token();
			tok = peekSqlToken(parser, ctx, AndSqlToken, false);
			if (iequals(tok.text(), "not"_sv)) {
				parser.next_token();
				nextOp = OpNot;
			} else
				continue;
		} else if (iequals(tok.text(), "or"_sv)) {
			parser.next_token();
			peekSqlToken(parser, ctx, FieldNameSqlToken);
			nextOp = OpOr;
		} else
			break;
	}
	return 0;
}

void Query::parseJoin(JoinType type, tokenizer &parser, SqlParsingCtx &ctx) {
	Query jquery;
	auto tok = parser.next_token();
	if (tok.text() == "("_sv) {
		peekSqlToken(parser, ctx, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"_sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text(), parser.where());
		}
		jquery.selectParse(parser, ctx);
		tok = parser.next_token();
		if (tok.text() != ")"_sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
		}
	} else {
		jquery._namespace = tok.text().ToString();
		ctx.updateLinkedNs(jquery._namespace);
	}
	jquery.joinType = type;
	jquery.parseJoinEntries(parser, _namespace, ctx);

	joinQueries_.push_back(std::move(jquery));
}

void Query::parseMerge(tokenizer &parser, SqlParsingCtx &ctx) {
	Query mquery;
	auto tok = parser.next_token();
	if (tok.text() == "("_sv) {
		peekSqlToken(parser, ctx, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"_sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text(), parser.where());
		}
		SqlParsingCtx ctx;
		mquery.selectParse(parser, ctx);
		tok = parser.next_token();
		if (tok.text() != ")"_sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
		}
	}
	mquery.joinType = JoinType::Merge;

	mergeQueries_.push_back(std::move(mquery));
}

string Query::parseJoinedFieldName(tokenizer &parser, string &name, SqlParsingCtx &ctx) {
	auto tok = peekSqlToken(parser, ctx, JoinedFieldNameSqlToken);
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();

	if (parser.peek_token().text() != "."_sv) {
		return tok.text().ToString();
	}
	parser.next_token();
	name = tok.text().ToString();

	tok = peekSqlToken(parser, ctx, FieldNameSqlToken);
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();
	ctx.updateLinkedNs(name);
	return tok.text().ToString();
}

void Query::parseJoinEntries(tokenizer &parser, const string &mainNs, SqlParsingCtx &ctx) {
	QueryJoinEntry je;
	auto tok = peekSqlToken(parser, ctx, OnSqlToken);
	if (tok.text() != "on"_sv) {
		throw Error(errParseSQL, "Expected 'ON', but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();

	tok = parser.peek_token();

	bool braces = tok.text() == "("_sv;
	if (braces) parser.next_token();

	while (!parser.end()) {
		auto tok = peekSqlToken(parser, ctx, OpSqlToken);
		if (tok.text() == "or"_sv) {
			nextOp_ = OpOr;
			parser.next_token();
			tok = parser.peek_token();
		} else if (tok.text() == "and"_sv) {
			nextOp_ = OpAnd;
			parser.next_token();
			tok = parser.peek_token();
		}

		if (braces && tok.text() == ")"_sv) {
			parser.next_token();
			return;
		}

		string ns1 = mainNs, ns2 = _namespace;
		string idx1 = parseJoinedFieldName(parser, ns1, ctx);
		je.condition_ = getCondType(parser.next_token().text());
		string idx2 = parseJoinedFieldName(parser, ns2, ctx);

		if (ns1 == mainNs && ns2 == _namespace) {
			je.index_ = idx1;
			je.joinIndex_ = idx2;
		} else if (ns2 == mainNs && ns1 == _namespace) {
			je.index_ = idx2;
			je.joinIndex_ = idx1;
		} else {
			throw Error(errParseSQL, "Unexpected tables with ON statement: ('%s' and '%s') but expected ('%s' and '%s'), %s", ns1, ns2,
						mainNs, _namespace, parser.where());
		}

		je.op_ = nextOp_;
		nextOp_ = OpAnd;
		joinEntries_.push_back(std::move(je));
		if (!braces) {
			return;
		}
	}
}

void Query::Serialize(WrSerializer &ser, uint8_t mode) const {
	ser.PutVString(_namespace);
	for (auto &qe : entries) {
		qe.distinct ? ser.PutVarUint(QueryDistinct) : ser.PutVarUint(QueryCondition);
		ser.PutVString(qe.index);
		if (qe.distinct) continue;
		ser.PutVarUint(qe.op);
		ser.PutVarUint(qe.condition);
		ser.PutVarUint(qe.values.size());
		for (auto &kv : qe.values) ser.PutVariant(kv);
	}

	for (auto &agg : aggregations_) {
		ser.PutVarUint(QueryAggregation);
		ser.PutVString(agg.index_);
		ser.PutVarUint(agg.type_);
	}

	for (const SortingEntry &sortginEntry : sortingEntries_) {
		ser.PutVarUint(QuerySortIndex);
		ser.PutVString(sortginEntry.column);
		ser.PutVarUint(sortginEntry.desc);
		int cnt = forcedSortOrder.size();
		ser.PutVarUint(cnt);
		for (auto &kv : forcedSortOrder) ser.PutVariant(kv);
	}

	for (auto &qje : joinEntries_) {
		ser.PutVarUint(QueryJoinOn);
		ser.PutVarUint(qje.op_);
		ser.PutVarUint(qje.condition_);
		ser.PutVString(qje.index_);
		ser.PutVString(qje.joinIndex_);
	}

	for (const EqualPosition &ep : equalPositions_) {
		ser.PutVarUint(QueryEqualPosition);
		ser.PutVarUint(ep.size());
		for (int pos : ep) ser.PutVString(entries[pos].index);
	}

	ser.PutVarUint(QueryDebugLevel);
	ser.PutVarUint(debugLevel);

	if (!(mode & SkipLimitOffset)) {
		if (count != UINT_MAX) {
			ser.PutVarUint(QueryLimit);
			ser.PutVarUint(count);
		}
		if (start) {
			ser.PutVarUint(QueryOffset);
			ser.PutVarUint(start);
		}
	}

	if (calcTotal) {
		ser.PutVarUint(QueryReqTotal);
		ser.PutVarUint(calcTotal);
	}

	for (auto &sf : selectFilter_) {
		ser.PutVarUint(QuerySelectFilter);
		ser.PutVString(sf);
	}

	if (explain_) {
		ser.PutVarUint(QueryExplain);
	}

	for (auto &field : updateFields_) {
		ser.PutVarUint(QueryUpdateField);
		ser.PutVString(field.first);
		ser.PutVarUint(field.second.size());
		for (auto &val : field.second) {
			// function/value flag
			ser.PutVarUint(0);
			ser.PutVariant(val);
		}
	}

	ser.PutVarUint(QueryEnd);  // finita la commedia... of root query

	if (!(mode & SkipJoinQueries)) {
		for (auto &jq : joinQueries_) {
			ser.PutVarUint(static_cast<int>(jq.joinType));
			jq.Serialize(ser);
		}
	}

	if (!(mode & SkipMergeQueries)) {
		for (auto &mq : mergeQueries_) {
			ser.PutVarUint(static_cast<int>(mq.joinType));
			mq.Serialize(ser, mode);
		}
	}
}

void Query::Deserialize(Serializer &ser) {
	_namespace = ser.GetVString().ToString();
	deserialize(ser);

	bool nested = false;
	while (!ser.Eof()) {
		auto joinType = JoinType(ser.GetVarUint());
		Query q1(ser.GetVString().ToString());
		q1.joinType = joinType;
		q1.deserialize(ser);
		q1.debugLevel = debugLevel;
		if (joinType == JoinType::Merge) {
			mergeQueries_.emplace_back(std::move(q1));
			nested = true;
		} else if (nested) {
			mergeQueries_.back().joinQueries_.emplace_back(std::move(q1));
		} else {
			joinQueries_.emplace_back(std::move(q1));
		}
	}
}

string Query::GetJSON() const { return dsl::toDsl(*this); }

const char *Query::JoinTypeName(JoinType type) {
	switch (type) {
		case JoinType::InnerJoin:
			return "INNER JOIN";
		case JoinType::OrInnerJoin:
			return "OR INNER JOIN";
		case JoinType::LeftJoin:
			return "LEFT JOIN";
		case JoinType::Merge:
			return "MERGE";
		default:
			return "<unknown>";
	}
}

extern const char *condNames[];

void Query::dumpJoined(WrSerializer &ser, bool stripArgs) const {
	for (auto &je : joinQueries_) {
		ser << ' ' << JoinTypeName(je.joinType);

		if (je.entries.empty() && je.count == INT_MAX && je.sortingEntries_.empty()) {
			ser << ' ' << je._namespace << " ON ";
		} else {
			ser << " (";
			je.GetSQL(ser, stripArgs);
			ser << ") ON ";
		}
		if (je.joinEntries_.size() != 1) ser << "(";
		for (auto &e : je.joinEntries_) {
			if (&e != &*je.joinEntries_.begin()) {
				ser << ((e.op_ == OpOr) ? " OR " : " AND ");
			}
			ser << je._namespace << '.' << e.joinIndex_ << ' ' << condNames[e.condition_] << ' ' << _namespace << '.' << e.index_;
		}
		if (je.joinEntries_.size() != 1) ser << ')';
	}
}

void Query::dumpMerged(WrSerializer &ser, bool stripArgs) const {
	for (auto &me : mergeQueries_) {
		ser << ' ' << JoinTypeName(me.joinType) << "( ";
		me.GetSQL(ser, stripArgs);
		ser << ')';
	}
}

void Query::dumpOrderBy(WrSerializer &ser, bool stripArgs) const {
	if (sortingEntries_.empty()) return;

	ser << " ORDER BY ";
	for (size_t i = 0; i < sortingEntries_.size(); ++i) {
		const SortingEntry &sortingEntry(sortingEntries_[i]);
		if (forcedSortOrder.empty()) {
			ser << sortingEntry.column;
		} else {
			ser << "FIELD(" << sortingEntry.column;
			if (stripArgs) {
				ser << '?';
			} else {
				for (auto &v : forcedSortOrder) {
					ser << ", '" << v.As<string>() << "'";
				}
			}
			ser << ")";
		}
		ser << (sortingEntry.desc ? " DESC" : "");
		if (i != sortingEntries_.size() - 1) ser << ", ";
	}
}

WrSerializer &Query::GetSQL(WrSerializer &ser, bool stripArgs) const {
	switch (type_) {
		case QuerySelect:
			ser << "SELECT ";
			if (aggregations_.size()) {
				for (auto &a : aggregations_) {
					if (&a != &*aggregations_.begin()) ser << ',';
					ser << AggregationResult::aggTypeToStr(a.type_) << "(" << a.index_ << ')';
				}
			} else if (selectFilter_.size()) {
				for (auto &f : selectFilter_) {
					if (&f != &*selectFilter_.begin()) ser << ',';
					ser << f;
				}
			} else
				ser << '*';
			if (calcTotal) ser << ", COUNT(*)";
			ser << " FROM " << _namespace;
			break;
		case QueryDelete:
			ser << "DELETE FROM " << _namespace;
			break;
		case QueryUpdate:
			ser << "UPDATE " << _namespace << " SET ";
			for (auto &field : updateFields_) {
				if (&field != &*updateFields_.begin()) ser << ',';
				ser << "'" << field.first << "' = ";
				if (field.second.size() > 1) ser << '(';
				for (auto &v : field.second) {
					if (&v != &*field.second.begin()) ser << ',';
					if (v.Type() == KeyValueString)
						ser << '\'' << v.As<string>() << '\'';
					else
						ser << v.As<string>();
				}
				ser << ((field.second.size() > 1) ? ")" : "");
			}
			break;
		default:
			throw Error(errParams, "Not implemented");
	}

	dumpWhere(ser, stripArgs);
	dumpJoined(ser, stripArgs);
	dumpMerged(ser, stripArgs);
	dumpOrderBy(ser, stripArgs);

	if (start != 0) ser << " OFFSET " << start;
	if (count != UINT_MAX) ser << " LIMIT " << count;
	return ser;
}

}  // namespace reindexer
