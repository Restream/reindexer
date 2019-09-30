
#include "core/query/query.h"
#include "core/keyvalue/key_string.h"
#include "core/namespace.h"
#include "core/query/aggregationresult.h"
#include "core/query/dslencoder.h"
#include "core/query/dslparsetools.h"
#include "core/rdxcontext.h"
#include "core/type_consts.h"
#include "expressionevaluator.h"
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
	try {
		gason::JsonParser parser;

		auto root = parser.Parse(giftStr(dsl));
		dsl::parse(root.value, *this);
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "Query: %s", ex.what());
	}
}

void Query::deserialize(Serializer &ser, bool &hasJoinConditions) {
	std::unordered_set<int> innerJoinEntries;
	while (!ser.Eof()) {
		QueryEntry qe;
		QueryJoinEntry qje;

		int qtype = ser.GetVarUint();
		switch (qtype) {
			case QueryCondition: {
				qe.index = string(ser.GetVString());
				OpType op = OpType(ser.GetVarUint());
				qe.condition = CondType(ser.GetVarUint());
				int cnt = ser.GetVarUint();
				qe.values.reserve(cnt);
				while (cnt--) qe.values.push_back(ser.GetVariant().EnsureHold());
				entries.Append(op, std::move(qe));
				break;
			}
			case QueryJoinCondition: {
				uint64_t type = ser.GetVarUint();
				assert(type != JoinType::LeftJoin);
				QueryEntry joinEntry(ser.GetVarUint());
				hasJoinConditions = true;
				entries.Append((type == JoinType::OrInnerJoin) ? OpOr : OpAnd, std::move(joinEntry));
				break;
			}
			case QueryAggregation: {
				AggregateEntry ae;
				ae.type_ = static_cast<AggType>(ser.GetVarUint());
				size_t fieldsCount = ser.GetVarUint();
				ae.fields_.reserve(fieldsCount);
				while (fieldsCount--) ae.fields_.push_back(string(ser.GetVString()));
				auto pos = ser.Pos();
				bool aggEnd = false;
				while (!ser.Eof() && !aggEnd) {
					int atype = ser.GetVarUint();
					switch (atype) {
						case QueryAggregationSort: {
							auto fieldName = ser.GetVString();
							ae.sortingEntries_.push_back({string(fieldName), ser.GetVarUint() != 0});
							break;
						}
						case QueryAggregationLimit:
							ae.limit_ = ser.GetVarUint();
							break;
						case QueryAggregationOffset:
							ae.offset_ = ser.GetVarUint();
							break;
						default:
							ser.SetPos(pos);
							aggEnd = true;
					}
					pos = ser.Pos();
				}
				aggregations_.push_back(std::move(ae));
				break;
			}
			case QueryDistinct:
				qe.index = string(ser.GetVString());
				if (!qe.index.empty()) {
					qe.distinct = true;
					qe.condition = CondAny;
					entries.Append(OpAnd, std::move(qe));
				}
				break;
			case QuerySortIndex: {
				SortingEntry sortingEntry;
				sortingEntry.column = string(ser.GetVString());
				sortingEntry.desc = bool(ser.GetVarUint());
				if (sortingEntry.column.length()) {
					sortingEntries_.push_back(std::move(sortingEntry));
				}
				int cnt = ser.GetVarUint();
				forcedSortOrder.reserve(cnt);
				while (cnt--) forcedSortOrder.push_back(ser.GetVariant().EnsureHold());
				break;
			}
			case QueryJoinOn:
				qje.op_ = OpType(ser.GetVarUint());
				qje.condition_ = CondType(ser.GetVarUint());
				qje.index_ = string(ser.GetVString());
				qje.joinIndex_ = string(ser.GetVString());
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
				selectFilter_.push_back(string(ser.GetVString()));
				break;
			case QueryEqualPosition: {
				const unsigned strt = ser.GetVarUint();
				vector<string> ep(ser.GetVarUint());
				for (size_t i = 0; i < ep.size(); ++i) ep[i] = string(ser.GetVString());
				equalPositions_.insert({strt, entries.DetermineEqualPositionIndexes(strt, ep)});
				break;
			}
			case QueryExplain:
				explain_ = true;
				break;
			case QuerySelectFunction:
				selectFunctions_.push_back(string(ser.GetVString()));
				break;
			case QueryUpdateField: {
				updateFields_.push_back({string(ser.GetVString()), {}});
				auto &field = updateFields_.back();
				int numValues = ser.GetVarUint();
				while (numValues--) {
					field.isExpression = ser.GetVarUint();
					field.values.push_back(ser.GetVariant());
				}
				break;
			}
			case QueryOpenBracket: {
				OpType op = OpType(ser.GetVarUint());
				entries.OpenBracket(op);
				break;
			}
			case QueryCloseBracket:
				entries.CloseBracket();
				break;
			case QueryEnd:
				return;
			default:
				throw Error(errParseBin, "Unknown type %d while parsing binary buffer", qtype);
		}
	}
	return;
}

bool checkIfTokenStartsWith(const string_view &src, const string_view &pattern) {
	return checkIfStartsWith(src, pattern) && src.length() < pattern.length();
}

vector<string> Query::GetSuggestions(const string_view &q, size_t pos, const Namespaces &namespaces) {
	SqlParsingCtx sqlCtx;
	sqlCtx.suggestionsPos = pos;
	sqlCtx.autocompleteMode = true;

	try {
		tokenizer parser(q);
		Parse(parser, sqlCtx);
	} catch (const Error &) {
	}

	for (SqlParsingCtx::SuggestionData &item : sqlCtx.suggestions) {
		checkForTokenSuggestions(item, sqlCtx, namespaces);
	}

	if (sqlCtx.suggestions.size() > 0) return sqlCtx.suggestions.front().variants;
	return std::vector<string>();
}

unordered_map<int, set<string>> sqlTokenMatchings = {
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

bool Query::reachedAutocompleteToken(tokenizer &parser, const token &tok, SqlParsingCtx &ctx) {
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
	} else if (tok.text() == "truncate"_sv) {
		type_ = QueryTruncate;
		truncateParse(parser, ctx);
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
				AggregateEntry entry{agg, {string(tok.text())}, UINT_MAX, 0};
				tok = parser.next_token();
				for (tok = parser.peek_token(); tok.text() == ","_sv; tok = parser.peek_token()) {
					parser.next_token();
					tok = peekSqlToken(parser, ctx, SingleSelectFieldSqlToken);
					entry.fields_.push_back(string(tok.text()));
					tok = parser.next_token();
				}
				for (tok = parser.peek_token(); tok.text() != ")"_sv; tok = parser.peek_token()) {
					if (tok.text() == "order"_sv) {
						parser.next_token();
						VariantArray orders;
						parseOrderBy(parser, entry.sortingEntries_, orders, ctx);
						if (!orders.empty()) {
							throw Error(errParseSQL, "Forced sort order is not available in aggregation sort");
						}
					} else if (tok.text() == "limit"_sv) {
						parser.next_token();
						tok = parser.next_token();
						if (tok.type != TokenNumber)
							throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
						entry.limit_ = stoi(tok.text());
					} else if (tok.text() == "offset"_sv) {
						parser.next_token();
						tok = parser.next_token();
						if (tok.type != TokenNumber)
							throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
						entry.offset_ = stoi(tok.text());
					} else {
						break;
					}
				}
				aggregations_.push_back(std::move(entry));
			} else {
				if (name.text() == "count"_sv) {
					calcTotal = ModeAccurateTotal;
					if (!wasSelectFilter) count = 0;
				} else if (name.text() == "distinct"_sv) {
					Distinct(string(tok.text()));
					if (!wasSelectFilter) selectFilter_.push_back(string(tok.text()));
				} else {
					throw Error(errParams, "Unknown function name SQL - %s, %s", name.text(), parser.where());
				}
				tok = parser.next_token();
			}
			tok = parser.peek_token();
			if (tok.text() != ")"_sv) {
				throw Error(errParams, "Expected ')', but found %s, %s", tok.text(), parser.where());
			}
			parser.next_token();
			tok = peekSqlToken(parser, ctx, SelectFieldsListSqlToken);

		} else if (name.text() != "*"_sv) {
			selectFilter_.push_back(string(nameWithCase.text()));
			count = UINT_MAX;
			wasSelectFilter = true;
		} else if (name.text() == "*"_sv) {
			count = UINT_MAX;
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
	_namespace = string(parser.next_token().text());
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
			parseOrderBy(parser, sortingEntries_, forcedSortOrder, ctx);
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
			auto jtype = (nextOp_ == OpOr) ? JoinType::OrInnerJoin : JoinType::InnerJoin;
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

int Query::parseOrderBy(tokenizer &parser, SortingEntries &sortingEntries, VariantArray &forcedSortOrder, SqlParsingCtx &ctx) {
	// Just skip token (BY)
	peekSqlToken(parser, ctx, BySqlToken);
	parser.next_token();
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, ctx, FieldNameSqlToken);
		auto tok = parser.next_token(false);
		if (tok.type != TokenName && tok.type != TokenString)
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		SortingEntry sortingEntry;
		sortingEntry.column = string(tok.text());
		tok = peekSqlToken(parser, ctx, SortDirectionSqlToken);
		if (tok.text() == "("_sv && nameWithCase.text() == "field"_sv) {
			parser.next_token();
			tok = peekSqlToken(parser, ctx, FieldNameSqlToken, false);
			if (tok.type != TokenName && tok.type != TokenString)
				throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
			sortingEntry.column = string(tok.text());
			tok = parser.next_token(false);
			for (;;) {
				tok = parser.next_token();
				if (tok.text() == ")"_sv) break;
				if (tok.text() != ","_sv)
					throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
				tok = peekSqlToken(parser, ctx, FieldNameSqlToken);
				if (tok.type != TokenNumber && tok.type != TokenString)
					throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", tok.text(), parser.where());
				forcedSortOrder.push_back(Variant(string(tok.text())));
				parser.next_token();
			}
			tok = parser.peek_token();
		}

		if (tok.text() == "asc"_sv || tok.text() == "desc"_sv) {
			sortingEntry.desc = bool(tok.text() == "desc"_sv);
			parser.next_token();
		}
		sortingEntries.push_back(std::move(sortingEntry));

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
	_namespace = string(parser.next_token().text());
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
			parseOrderBy(parser, sortingEntries_, forcedSortOrder, ctx);
			ctx.updateLinkedNs(_namespace);
		} else
			break;
	}
	return 0;
}

static KeyValueType detectValueType(const token &currTok) {
	const string_view &val = currTok.text();
	if (currTok.type == TokenNumber) {
		unsigned i = 0;
		bool flt = false;
		bool digit = val.length() < 21 && val.length() > 0;
		if (val[i] == '+' || val[i] == '-') i++;
		for (; i < val.length() && digit; i++) {
			if (val[i] == '.') {
				flt = true;
			} else if (!isdigit(val[i])) {
				digit = false;
			}
		}
		if (digit && val.length() > 0) {
			return flt ? KeyValueDouble : KeyValueInt64;
		}
	}
	return KeyValueString;
}

static Variant token2kv(const token &currTok, tokenizer &parser) {
	if (currTok.text() == "true"_sv) return Variant(true);
	if (currTok.text() == "false"_sv) return Variant(false);

	if (currTok.type != TokenNumber && currTok.type != TokenString)
		throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", currTok.text(), parser.where());

	string_view value = currTok.text();
	switch (detectValueType(currTok)) {
		case KeyValueInt64:
			return Variant(int64_t(stoll(value)));
		case KeyValueDouble: {
			char *p = 0;
			return Variant(double(strtod(value.data(), &p)));
		}
		case KeyValueString:
			return Variant(make_key_string(value.data(), value.length()));
		default:
			std::abort();
	}
}

static void addUpdateValue(const token &currTok, tokenizer &parser, UpdateEntry &updateField) {
	if (currTok.type == TokenString) {
		updateField.values.push_back(token2kv(currTok, parser));
	} else {
		int count = 0;
		string expression(currTok.text());
		auto eof = [](tokenizer &parser) -> bool {
			if (parser.end()) return true;
			token nextTok = parser.peek_token();
			return ((nextTok.text() == "where"_sv) || (nextTok.text() == "]"_sv) || (nextTok.text() == ","_sv));
		};
		while (!eof(parser)) {
			++count;
			expression += string(parser.next_token(false).text());
		}
		updateField.values.push_back(count ? Variant(expression) : token2kv(currTok, parser));
		updateField.isExpression = count != 0;
	}
}

UpdateEntry Query::parseUpdateField(tokenizer &parser, SqlParsingCtx &ctx) {
	UpdateEntry updateField;
	token tok = peekSqlToken(parser, ctx, FieldNameSqlToken, false);
	if (tok.type != TokenName && tok.type != TokenString)
		throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
	updateField.column = string(tok.text());
	parser.next_token();

	tok = parser.next_token();
	if (tok.text() != "="_sv) throw Error(errParams, "Expected '=' but found '%s' in query, '%s'", tok.text(), parser.where());

	tok = parser.next_token(false);
	if (tok.text() == "["_sv) {
		for (;;) {
			tok = parser.next_token(false);
			addUpdateValue(tok, parser, updateField);
			tok = parser.next_token(false);
			if (tok.text() == "]"_sv) break;
			if (tok.text() != ","_sv)
				throw Error(errParseSQL, "Expected ']' or ',', but found '%s' in query, %s", tok.text(), parser.where());
		}
	} else {
		addUpdateValue(tok, parser, updateField);
	}
	return updateField;
}

int Query::updateParse(tokenizer &parser, SqlParsingCtx &ctx) {
	parser.next_token();

	token tok = peekSqlToken(parser, ctx, NamespaceSqlToken);
	_namespace = string(tok.text());
	ctx.updateLinkedNs(_namespace);
	parser.next_token();

	tok = peekSqlToken(parser, ctx, SetSqlToken);
	if (tok.text() != "set"_sv) throw Error(errParams, "Expected 'SET', but found '%s' in query, %s", tok.text(), parser.where());
	parser.next_token();

	while (!parser.end()) {
		UpdateEntry updateField = parseUpdateField(parser, ctx);
		updateFields_.push_back(std::move(updateField));

		tok = parser.peek_token();
		if (tok.text() != ","_sv) break;
		parser.next_token();
	}
	if (parser.end()) return 0;

	tok = peekSqlToken(parser, ctx, WhereSqlToken);
	if (tok.text() != "where"_sv) throw Error(errParams, "Expected 'WHERE', but found '%s' in query, %s", tok.text(), parser.where());
	parser.next_token();
	parseWhere(parser, ctx);

	return 0;
}

int Query::truncateParse(tokenizer &parser, SqlParsingCtx &ctx) {
	parser.next_token();
	token tok = peekSqlToken(parser, ctx, NamespaceSqlToken);
	_namespace = string(tok.text());
	ctx.updateLinkedNs(_namespace);
	parser.next_token();
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

	int openBracketCount = 0;
	while (!parser.end()) {
		tok = parser.next_token(false);
		if (tok.text() == "("_sv) {
			entries.OpenBracket(nextOp);
			++openBracketCount;
			tok = peekSqlToken(parser, ctx, WhereFieldSqlToken, false);
			if (iequals(tok.text(), "not"_sv)) {
				nextOp = OpNot;
				parser.next_token();
			} else {
				nextOp = OpAnd;
			}
			continue;
		}
		if (tok.type == TokenName || tok.type == TokenString) {
			if (iequals(tok.text(), "join"_sv)) {
				parseJoin(JoinType::LeftJoin, parser, ctx);
			} else if (iequals(tok.text(), "left"_sv)) {
				peekSqlToken(parser, ctx, LeftSqlToken);
				if (parser.next_token().text() != "join"_sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
				}
				parseJoin(JoinType::LeftJoin, parser, ctx);
			} else if (iequals(tok.text(), "inner"_sv)) {
				peekSqlToken(parser, ctx, InnerSqlToken);
				if (parser.next_token().text() != "join") {
					throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
				}
				auto jtype = nextOp == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
				nextOp_ = OpAnd;
				parseJoin(jtype, parser, ctx);
			} else {
				QueryEntry entry;
				// Index name
				entry.index = string(tok.text());

				// Operator
				tok = peekSqlToken(parser, ctx, ConditionSqlToken);
				if (tok.text() == "<>"_sv) {
					entry.condition = CondEq;
					if (nextOp == OpAnd)
						nextOp = OpNot;
					else if (nextOp == OpNot)
						nextOp = OpAnd;
					else {
						throw Error(errParseSQL, "<> condition with OR is not supported, %s", parser.where());
					}
				} else {
					entry.condition = getCondType(tok.text());
				}
				parser.next_token();

				// Value
				if (ctx.autocompleteMode) peekSqlToken(parser, ctx, WhereFieldValueSqlToken, false);
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
				} else if (tok.text() == "("_sv) {
					for (;;) {
						tok = parser.next_token();
						if (tok.text() == ")"_sv && tok.type == TokenSymbol) break;
						entry.values.push_back(token2kv(tok, parser));
						tok = parser.next_token();
						if (tok.text() == ")"_sv) break;
						if (tok.text() != ","_sv)
							throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
					}
				} else {
					entry.values.push_back(token2kv(tok, parser));
				}
				entries.Append(nextOp, std::move(entry));
				nextOp = OpAnd;
			}
		}

		tok = parser.peek_token();
		while (openBracketCount > 0 && tok.text() == ")"_sv) {
			entries.CloseBracket();
			--openBracketCount;
			parser.next_token();
			tok = parser.peek_token();
		}

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
		} else if (!iequals(tok.text(), "join"_sv) && !iequals(tok.text(), "inner"_sv) && !iequals(tok.text(), "left"_sv)) {
			break;
		}
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
		jquery._namespace = string(tok.text());
		ctx.updateLinkedNs(jquery._namespace);
	}
	jquery.joinType = type;
	jquery.parseJoinEntries(parser, _namespace, ctx);

	if (type != JoinType::LeftJoin) {
		entries.Append((type == JoinType::InnerJoin) ? OpAnd : OpOr, QueryEntry(joinQueries_.size()));
	}

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
		SqlParsingCtx mctx;
		mquery.selectParse(parser, mctx);
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
		return string(tok.text());
	}
	parser.next_token();
	name = string(tok.text());

	tok = peekSqlToken(parser, ctx, FieldNameSqlToken);
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();
	ctx.updateLinkedNs(name);
	return string(tok.text());
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
	entries.Serialize(ser);

	for (auto &agg : aggregations_) {
		ser.PutVarUint(QueryAggregation);
		ser.PutVarUint(agg.type_);
		ser.PutVarUint(agg.fields_.size());
		for (const auto &field : agg.fields_) {
			ser.PutVString(field);
		}
		for (const auto &se : agg.sortingEntries_) {
			ser.PutVarUint(QueryAggregationSort);
			ser.PutVString(se.column);
			ser.PutVarUint(se.desc);
		}
		if (agg.limit_ != UINT_MAX) {
			ser.PutVarUint(QueryAggregationLimit);
			ser.PutVarUint(agg.limit_);
		}
		if (agg.offset_ != 0) {
			ser.PutVarUint(QueryAggregationOffset);
			ser.PutVarUint(agg.offset_);
		}
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

	for (const std::pair<unsigned, EqualPosition> &equalPoses : equalPositions_) {
		ser.PutVarUint(QueryEqualPosition);
		ser.PutVarUint(equalPoses.first);
		ser.PutVarUint(equalPoses.second.size());
		for (unsigned ep : equalPoses.second) ser.PutVString(entries[ep].index);
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

	if (calcTotal != ModeNoTotal) {
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

	for (const UpdateEntry &field : updateFields_) {
		ser.PutVarUint(QueryUpdateField);
		ser.PutVString(field.column);
		ser.PutVarUint(field.values.size());
		for (const Variant &val : field.values) {
			ser.PutVarUint(field.isExpression);
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
	_namespace = string(ser.GetVString());
	bool hasJoinConditions = false;
	deserialize(ser, hasJoinConditions);

	bool nested = false;
	while (!ser.Eof()) {
		auto joinType = JoinType(ser.GetVarUint());
		Query q1(string(ser.GetVString()));
		q1.joinType = joinType;
		q1.deserialize(ser, hasJoinConditions);
		q1.debugLevel = debugLevel;
		if (joinType == JoinType::Merge) {
			mergeQueries_.emplace_back(std::move(q1));
			nested = true;
		} else {
			Query &q = nested ? mergeQueries_.back() : *this;
			if (joinType != JoinType::LeftJoin && !hasJoinConditions) {
				int joinIdx = joinQueries_.size();
				entries.Append((joinType == JoinType::OrInnerJoin) ? OpOr : OpAnd, QueryEntry(joinIdx));
			}
			q.joinQueries_.emplace_back(std::move(q1));
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

void Query::DumpSingleJoinQuery(size_t idx, WrSerializer &ser, bool stripArgs) const {
	assert(idx < joinQueries_.size());
	const Query &jq = joinQueries_[idx];
	ser << ' ' << JoinTypeName(jq.joinType);
	if (jq.entries.Empty() && jq.count == UINT_MAX && jq.sortingEntries_.empty()) {
		ser << ' ' << jq._namespace << " ON ";
	} else {
		ser << " (";
		jq.GetSQL(ser, stripArgs);
		ser << ") ON ";
	}
	if (jq.joinEntries_.size() != 1) ser << "(";
	for (auto &e : jq.joinEntries_) {
		if (&e != &*jq.joinEntries_.begin()) {
			ser << ((e.op_ == OpOr) ? " OR " : " AND ");
		}
		ser << jq._namespace << '.' << e.joinIndex_ << ' ' << condNames[e.condition_] << ' ' << _namespace << '.' << e.index_;
	}
	if (jq.joinEntries_.size() != 1) ser << ')';
}

void Query::dumpJoined(WrSerializer &ser, bool stripArgs) const {
	for (size_t i = 0; i < joinQueries_.size(); ++i) {
		if (joinQueries_[i].joinType == JoinType::LeftJoin) {
			DumpSingleJoinQuery(i, ser, stripArgs);
		}
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
				for (const auto &a : aggregations_) {
					if (&a != &*aggregations_.begin()) ser << ',';
					ser << AggregationResult::aggTypeToStr(a.type_) << "(";
					for (const auto &f : a.fields_) {
						if (&f != &*a.fields_.begin()) ser << ',';
						ser << f;
					}
					for (const auto &se : a.sortingEntries_) {
						ser << " ORDER BY " << se.column << (se.desc ? " DESC" : " ASC");
					}

					if (a.offset_ != 0 && !stripArgs) ser << " OFFSET " << a.offset_;
					if (a.limit_ != UINT_MAX && !stripArgs) ser << " LIMIT " << a.limit_;
					ser << ')';
				}
			} else if (selectFilter_.size()) {
				for (auto &f : selectFilter_) {
					if (&f != &*selectFilter_.begin()) ser << ',';
					ser << f;
				}
			} else
				ser << '*';
			if (calcTotal != ModeNoTotal) ser << ", COUNT(*)";
			ser << " FROM " << _namespace;
			break;
		case QueryDelete:
			ser << "DELETE FROM " << _namespace;
			break;
		case QueryUpdate:
			ser << "UPDATE " << _namespace << " SET ";
			for (const UpdateEntry &field : updateFields_) {
				if (&field != &*updateFields_.begin()) ser << ',';
				if (field.column.find('.') == string::npos)
					ser << field.column << " = ";
				else
					ser << "'" << field.column << "' = ";

				if (field.values.size() != 1) ser << '[';
				for (const Variant &v : field.values) {
					if (&v != &*field.values.begin()) ser << ',';
					if (v.Type() == KeyValueString && !field.isExpression) {
						ser << '\'' << v.As<string>() << '\'';
					} else {
						ser << v.As<string>();
					}
				}
				ser << ((field.values.size() != 1) ? "]" : "");
			}
			break;
		case QueryTruncate:
			ser << "TRUNCATE " << _namespace;
			break;
		default:
			throw Error(errParams, "Not implemented");
	}

	entries.WriteSQLWhere(*this, ser, stripArgs);
	dumpJoined(ser, stripArgs);
	dumpMerged(ser, stripArgs);
	dumpOrderBy(ser, stripArgs);

	if (start != 0 && !stripArgs) ser << " OFFSET " << start;
	if (count != UINT_MAX && !stripArgs) ser << " LIMIT " << count;
	return ser;
}

}  // namespace reindexer
