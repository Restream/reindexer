#include "sqlparser.h"
#include <charconv>
#include "core/keyvalue/geometry.h"
#include "core/query/query.h"
#include "core/queryresults/aggregationresult.h"
#include "core/type_consts_helpers.h"
#include "estl/gift_str.h"
#include "sqltokentype.h"
#include "tools/stringstools.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/gason/gason.h"

namespace reindexer {

using namespace std::string_view_literals;

Query SQLParser::Parse(std::string_view q) {
	tokenizer parser(q);
	Query query;
	SQLParser{query}.Parse(parser);
	return query;
}

bool SQLParser::reachedAutocompleteToken(tokenizer& parser, const token& tok) const {
	size_t pos = parser.getPos() + tok.text().length();
	return pos > ctx_.suggestionsPos;
}

token SQLParser::peekSqlToken(tokenizer& parser, SqlTokenType tokenType, bool toLower) {
	token tok = parser.peek_token(toLower ? tokenizer::flags::to_lower : tokenizer::flags::no_flags);
	const bool eof = ((parser.getPos() + tok.text().length()) == parser.length());
	if (ctx_.autocompleteMode && reachedAutocompleteToken(parser, tok)) {
		size_t tokenLen = 0;
		if (ctx_.suggestionsPos >= parser.getPos()) {
			tokenLen = ctx_.suggestionsPos - parser.getPos() + 1;
		}
		if (!ctx_.foundPossibleSuggestions || tokenLen) {
			// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
			ctx_.suggestions.emplace_back(std::string(tok.text().data(), std::min(tok.text().size(), tokenLen)), tokenType);
			ctx_.foundPossibleSuggestions = true;
			ctx_.possibleSuggestionDetectedInThisClause = true;
		}
	}
	if (!ctx_.foundPossibleSuggestions) {
		ctx_.tokens.push_back(tokenType);
	}
	if (eof && ctx_.autocompleteMode) {
		throw Error(errLogic, "SQLParser eof is reached!");
	}
	return tok;
}

int SQLParser::Parse(tokenizer& parser) {
	parser.skip_space();
	if (parser.length() == 0) {
		ctx_.suggestions.emplace_back(std::string(), Start);
		return 0;
	}
	token tok = peekSqlToken(parser, Start);
	if (tok.text() == "explain"sv) {
		query_.Explain();
		parser.next_token();
		tok = peekSqlToken(parser, StartAfterExplain);
		if (tok.text() == "local"sv) {
			query_.Local();
			parser.next_token();
			tok = peekSqlToken(parser, StartAfterLocalExplain);
		}
	} else if (tok.text() == "local"sv) {
		query_.Local();
		parser.next_token();
		tok = peekSqlToken(parser, StartAfterLocal);
		if (tok.text() == "explain"sv) {
			query_.Explain();
			parser.next_token();
			tok = peekSqlToken(parser, StartAfterLocalExplain);
		}
	}

	if (tok.text() == "select"sv) {
		query_.type_ = QuerySelect;
		parser.next_token();
		selectParse<Nested::No>(parser);
	} else if (query_.IsLocal()) {
		throw Error(errParams, "Syntax error at or near '{}', {}; only SELECT query could be LOCAL", tok.text(), parser.where());
	} else if (tok.text() == "delete"sv) {
		query_.type_ = QueryDelete;
		tok = parser.next_token();
		deleteParse(parser);
	} else if (tok.text() == "update"sv) {
		query_.type_ = QueryUpdate;
		updateParse(parser);
	} else if (tok.text() == "truncate"sv) {
		query_.type_ = QueryTruncate;
		truncateParse(parser);
	} else {
		throw Error(errParams, "Syntax error at or near '{}', {}", tok.text(), parser.where());
	}

	tok = parser.next_token();
	if (tok.text() == ";"sv) {
		tok = parser.next_token();
	}
	parser.skip_space();
	if (!tok.text().empty() || !parser.end()) {
		throw Error(errParseSQL, "Unexpected '{}' in query, {}", tok.text(), parser.where());
	}

	return 0;
}

template <SQLParser::Nested nested>
int SQLParser::selectParse(tokenizer& parser) {
	// Get filter
	token tok;
	bool wasSelectFilter = false;
	while (true) {
		auto nameWithCase = peekSqlToken(parser, SingleSelectFieldSqlToken, false);
		auto name = parser.next_token();
		tok = peekSqlToken(parser, FromSqlToken);
		if (tok.text() == "("sv) {
			parser.next_token();
			tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
			if (name.text() == "count"sv) {
				query_.CalcTotal(ModeAccurateTotal);
				if (!wasSelectFilter) {
					query_.Limit(0);
				}
				tok = parser.next_token();
				if (tok.text() != "*"sv) {
					throw Error(errParseSQL, "Expected '*', but found '{}' in query, {}", tok.text(), parser.where());
				}
			} else if (name.text() == "count_cached"sv) {
				query_.CalcTotal(ModeCachedTotal);
				if (!wasSelectFilter) {
					query_.Limit(0);
				}
				tok = parser.next_token();
				if (tok.text() != "*"sv) {
					throw Error(errParseSQL, "Expected '*', but found '{}' in query, {}", tok.text(), parser.where());
				}
			} else if (name.text() == "rank"sv) {
				query_.WithRank();
			} else if (name.text() == "vectors"sv) {
				if (!query_.CanAddSelectFilter()) {
					throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
				}
				query_.Limit(QueryEntry::kDefaultLimit);
				wasSelectFilter = true;
				query_.Select(FieldsNamesFilter::kAllVectorFieldsName);
			} else {
				AggType agg = AggregationResult::StrToAggType(name.text());
				if (agg != AggUnknown) {
					if (!query_.CanAddAggregation(agg) || (wasSelectFilter && agg != AggDistinct)) {
						throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
					}
					if (tok.type != TokenName) {
						throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.text(), parser.where());
					}
					h_vector<std::string, 1> fields{{std::string(tok.text())}};
					tok = parser.next_token();
					for (tok = parser.peek_token(); tok.type == TokenSymbol && tok.text() == ","sv; tok = parser.peek_token()) {
						parser.next_token();
						tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
						if (tok.type != TokenName) {
							throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.text(), parser.where());
						}
						fields.emplace_back(tok.text());
						tok = parser.next_token();
					}
					AggregateEntry entry{agg, std::move(fields)};
					for (tok = parser.peek_token(); tok.text() != ")"sv; tok = parser.peek_token()) {
						if (tok.text() == "order"sv) {
							parser.next_token();
							std::vector<Variant> orders;
							SortingEntries sortingEntries;
							parseOrderBy(parser, entry);
							if (!orders.empty()) {
							}
							for (auto s : sortingEntries) {
								entry.AddSortingEntry(std::move(s));
							}
						} else if (tok.text() == "limit"sv) {
							parser.next_token();
							tok = parser.next_token();
							if (tok.type != TokenNumber) {
								throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.text(), parser.where());
							}
							entry.SetLimit(stoi(tok.text()));
						} else if (tok.text() == "offset"sv) {
							parser.next_token();
							tok = parser.next_token();
							if (tok.type != TokenNumber) {
								throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.text(), parser.where());
							}
							entry.SetOffset(stoi(tok.text()));
						} else {
							break;
						}
					}
					query_.aggregations_.emplace_back(std::move(entry));
				} else {
					throw Error(errParams, "Unknown function name SQL - '{}', {}", name.text(), parser.where());
				}
			}
			tok = parser.peek_token();
			if (tok.text() != ")"sv) {
				throw Error(errParams, "Expected ')', but found '{}', {}", tok.text(), parser.where());
			}
			parser.next_token();
			tok = peekSqlToken(parser, FromSqlToken);

		} else if (name.text() != "*"sv) {
			if (!query_.CanAddSelectFilter()) {
				throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
			}
			query_.Select(nameWithCase.text());
			query_.Limit(QueryEntry::kDefaultLimit);
			wasSelectFilter = true;
		} else if (name.text() == "*"sv) {
			if (!query_.CanAddSelectFilter()) {
				throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
			}
			query_.Limit(QueryEntry::kDefaultLimit);
			wasSelectFilter = true;
			query_.Select("*");
		}
		if (tok.text() != ","sv) {
			break;
		}
		tok = parser.next_token();
	}

	peekSqlToken(parser, FromSqlToken);
	if (parser.next_token().text() != "from"sv) {
		throw Error(errParams, "Expected 'FROM', but found '{}' in query, {}", tok.text(), parser.where());
	}

	peekSqlToken(parser, NamespaceSqlToken);
	query_.SetNsName(parser.next_token().text());
	ctx_.updateLinkedNs(query_.NsName());

	do {
		tok = peekSqlToken(parser, nested == Nested::Yes ? NestedSelectConditionsStart : SelectConditionsStart);
		if (tok.text() == "where"sv) {
			parser.next_token();
			parseWhere<nested>(parser);
		} else if (tok.text() == "limit"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.text(), parser.where());
			}
			query_.Limit(stoi(tok.text()));
		} else if (tok.text() == "offset"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.text(), parser.where());
			}
			query_.Offset(stoi(tok.text()));
		} else if (tok.text() == "order"sv) {
			parser.next_token();
			parseOrderBy(parser, query_);
			ctx_.updateLinkedNs(query_.NsName());
		} else if constexpr (nested == Nested::No) {
			if (tok.text() == "join"sv) {
				parser.next_token();
				parseJoin(JoinType::LeftJoin, parser);
			} else if (tok.text() == "left"sv) {
				parser.next_token();
				peekSqlToken(parser, LeftSqlToken);
				if (parser.next_token().text() != "join"sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.text(), parser.where());
				}
				parseJoin(JoinType::LeftJoin, parser);
			} else if (tok.text() == "inner"sv) {
				parser.next_token();
				peekSqlToken(parser, InnerSqlToken);
				if (parser.next_token().text() != "join"sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.text(), parser.where());
				}
				auto jtype = (query_.NextOp() == OpOr) ? JoinType::OrInnerJoin : JoinType::InnerJoin;
				query_.And();
				parseJoin(jtype, parser);
			} else if (tok.text() == "merge"sv) {
				parser.next_token();
				parseMerge(parser);
			} else if (tok.text() == "or"sv) {
				parser.next_token();
				query_.Or();
			} else {
				break;
			}
		} else {
			break;
		}
	} while (!parser.end());
	return 0;
}

template <typename T>
static void moveAppend(T& dst, T& src) {
	if (dst.empty()) {
		dst = std::move(src);
	} else {
		dst.reserve(dst.size() + src.size());
		std::move(std::begin(src), std::end(src), std::back_inserter(dst));
		src.clear();
	}
}

class SQLParser::ParserContextsAppendGuard {
public:
	ParserContextsAppendGuard(SqlParsingCtx& mainCtx, SqlParsingCtx& nestedCtx) noexcept : mainCtx_{mainCtx}, nestedCtx_{nestedCtx} {}
	~ParserContextsAppendGuard() {
		moveAppend(mainCtx_.suggestions, nestedCtx_.suggestions);
		if (!mainCtx_.foundPossibleSuggestions && nestedCtx_.foundPossibleSuggestions) {
			mainCtx_.suggestionLinkedNs = std::move(nestedCtx_.suggestionLinkedNs);
		}
	}

private:
	SqlParsingCtx& mainCtx_;
	SqlParsingCtx& nestedCtx_;
};

Variant token2kv(const token& tok, tokenizer& parser, CompositeAllowed allowComposite, FieldAllowed allowField, NullAllowed allowNull) {
	if (tok.text() == "{"sv) {
		// Composite value parsing
		if (!allowComposite) {
			throw Error(errParseSQL, "Unexpected '{{' in query, {}", parser.where());
		}
		VariantArray compositeValues;
		for (;;) {
			auto nextTok = parser.next_token();
			compositeValues.push_back(token2kv(nextTok, parser, CompositeAllowed_False, FieldAllowed_False, allowNull));
			nextTok = parser.next_token();
			if (nextTok.text() == "}"sv) {
				return Variant(compositeValues);  // end process
			}
			if (nextTok.text() != ","sv) {
				throw Error(errParseSQL, "Expected ',', but found '{}' in query, {}", nextTok.text(), parser.where());
			}
		}
	}

	std::string_view value = tok.text();
	if (tok.type == TokenName) {
		if (iequals(value, "true"sv)) {
			return Variant{true};
		}
		if (iequals(value, "false"sv)) {
			return Variant{false};
		}
		if (iequals(value, "null"sv)) {
			if (allowNull) {
				return Variant{};
			}
		} else if (allowField) {
			return Variant();
		}
	}

	if (tok.type != TokenNumber && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected parameter, but found '{}' in query, {}", tok.text(), parser.where());
	}

	return getVariantFromToken(tok);
}

template <typename Sortable>
int SQLParser::parseOrderBy(tokenizer& parser, Sortable& sortable) {
	// Just skip token (BY)
	peekSqlToken(parser, BySqlToken);
	parser.next_token();
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken);
		auto tok = parser.next_token(tokenizer::flags::in_order_by);
		if (tok.type != TokenName && tok.type != TokenString) {
			throw Error(errParseSQL, "Expected name, but found '{}' in query, {}", tok.text(), parser.where());
		}
		std::vector<Variant> forcedSortOrder;
		std::string sortExpression(tok.text());
		bool desc = false;
		if (sortExpression.empty()) {
			throw Error(errParseSQL, "Order by expression should not be empty, {}", parser.where());
		}
		tok = peekSqlToken(parser, SortDirectionSqlToken);
		if (tok.text() == "("sv && nameWithCase.text() == "field"sv) {
			parser.next_token();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.type != TokenName) {
				throw Error(errParseSQL, "Expected name, but found '{}' in query, {}", tok.text(), parser.where());
			}
			sortExpression = tok.text();
			tok = parser.next_token(tokenizer::flags::no_flags);
			for (;;) {
				tok = parser.next_token();
				if (tok.text() == ")"sv) {
					break;
				}
				if (tok.text() != ","sv) {
					throw Error(errParseSQL, "Expected ')' or ',', but found '{}' in query, {}", tok.text(), parser.where());
				}
				tok = parser.next_token();
				forcedSortOrder.push_back(token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_False));
			}
			tok = parser.peek_token();
		}

		if (tok.text() == "asc"sv || tok.text() == "desc"sv) {
			desc = tok.text() == "desc"sv;
			parser.next_token();
		}
		if constexpr (std::is_same_v<Sortable, AggregateEntry>) {
			if (!forcedSortOrder.empty()) {
				throw Error(errParseSQL, "Forced sort order is not available in aggregation sort");
			}
			sortable.Sort(std::move(sortExpression), desc);
		} else {
			sortable.Sort(std::move(sortExpression), desc, std::move(forcedSortOrder));
		}

		auto nextToken = parser.peek_token();
		if (nextToken.text() != ","sv) {
			break;
		}
		parser.next_token();
	}
	return 0;
}

int SQLParser::deleteParse(tokenizer& parser) {
	// Get filter
	token tok;

	peekSqlToken(parser, FromSqlToken);
	if (parser.next_token().text() != "from"sv) {
		throw Error(errParams, "Expected 'FROM', but found '{}' in query, {}", tok.text(), parser.where());
	}

	peekSqlToken(parser, NamespaceSqlToken);
	query_.SetNsName(parser.next_token().text());
	ctx_.updateLinkedNs(query_.NsName());

	parseModifyConditions(parser);

	return 0;
}

static void addUpdateValue(const token& tok, tokenizer& parser, UpdateEntry& updateField) {
	if (tok.type == TokenString) {
		updateField.Values().push_back(token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True));
	} else {
		if (tok.text() == "null"sv) {
			updateField.Values().push_back(Variant());
		} else if (tok.text() == "{"sv) {
			try {
				size_t jsonPos = parser.getPrevPos();
				std::string json(parser.begin() + jsonPos, parser.length() - jsonPos);
				size_t jsonLength = 0;
				gason::JsonParser jsonParser;
				jsonParser.Parse(giftStr(json), &jsonLength);
				updateField.Values().emplace_back(Variant(std::string(parser.begin() + jsonPos, jsonLength)));
				updateField.SetMode(FieldModeSetJson);
				parser.setPos(jsonPos + jsonLength);
			} catch (const gason::Exception& e) {
				throw Error(errParseSQL, "{}, in query {}", e.what(), parser.where());
			}
		} else {
			auto eof = [](tokenizer& parser, bool& inArray) -> bool {
				if (parser.end()) {
					return true;
				}
				token nextTok = parser.peek_token();
				bool result = (nextTok.text() == "where"sv) || (nextTok.text() == "order"sv) || (nextTok.text() == "limit"sv) ||
							  (nextTok.text() == "offset"sv) || (!inArray && nextTok.text() == "]"sv) ||
							  (!inArray && nextTok.text() == ","sv);
				if (nextTok.text() == "["sv && !inArray) {
					inArray = true;
				}
				if (nextTok.text() == "]"sv && inArray) {
					inArray = false;
				}
				return result;
			};
			int count = 0;
			std::string expression(tok.text());
			bool inArray = false;
			while (!eof(parser, inArray)) {
				auto nextTok = parser.next_token(tokenizer::flags::treat_sign_as_token);
				expression += nextTok.type == TokenString ? '\'' + std::string{nextTok.text()} + '\'' : std::string{nextTok.text()};
				++count;
			}
			if (count > 0) {
				updateField.Values().push_back(Variant(expression));
				updateField.SetIsExpression(true);
			} else {
				try {
					Variant val = token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True);
					updateField.Values().push_back(val);
				} catch (const Error&) {
					updateField.Values().push_back(Variant(expression));
					updateField.SetIsExpression(true);
				}
			}
		}
	}
}

void SQLParser::parseArray(tokenizer& parser, std::string_view tokText, UpdateEntry* updateField) const {
	if (tokText != "["sv) {
		throw Error(errParams, "Expected '[' after field parameter, not {}", tokText);
	}

	for (;;) {
		auto nextTok = parser.next_token(tokenizer::flags::no_flags);
		if (nextTok.text() == "]"sv) {
			if (updateField && updateField->Values().empty()) {
				break;
			}
			throw Error(errParseSQL, "Expected field value, but found ']' in query, {}", parser.where());
		}
		if (updateField) {
			addUpdateValue(nextTok, parser, *updateField);
		}
		nextTok = parser.next_token(tokenizer::flags::no_flags);
		if (nextTok.text() == "]"sv) {
			break;
		}
		if (nextTok.text() != ","sv) {
			throw Error(errParseSQL, "Expected ']' or ',', but found '{}' in query, {}", nextTok.text(), parser.where());
		}
	}

	if (updateField && (updateField->Mode() == FieldModeSetJson)) {
		for (const auto& it : updateField->Values()) {
			if ((!it.Type().Is<KeyValueType::String>()) || std::string_view(it).front() != '{') {
				throw Error(errLogic, "Unexpected variant type in Array: {}. Expecting KeyValueType::String with JSON-content",
							it.Type().Name());
			}
		}
	}
}

void SQLParser::parseCommand(tokenizer& parser) const {
	// parse input, for example: array_remove(array_field, [24, 3, 81]) || [11,22]

	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParams, "Expected '(' after command name, not {}", tok.text());
	}

	tok = parser.next_token();
	if (tok.type != TokenName) {
		throw Error(errLogic, "Expected field name, but found {} in query, {}", tok.text(), parser.where());
	}

	tok = parser.next_token();
	if (tok.text() != ","sv) {
		throw Error(errParams, "Expected ',' after field parameter, not {}", tok.text());
	}

	// parse item or list of elements or field to be deleted
	tok = parser.next_token();
	// try parse as scalar value
	if ((tok.type == TokenNumber) || (tok.type == TokenString) || (tok.type == TokenName)) {
		token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_True, NullAllowed_False);  // ignore result
	} else {
		parseArray(parser, tok.text(), nullptr);
	}

	tok = parser.next_token();
	if (tok.text() != ")"sv) {
		throw Error(errParams, "Expected ')' after command name and params, not {}", tok.text());
	}

	// parse of possible concatenation
	tok = parser.peek_token(tokenizer::flags::no_flags);
	while (tok.text() == "|"sv) {
		parser.next_token();
		tok = parser.next_token();
		if (tok.text() != "|"sv) {
			throw Error(errLogic, "Expected '|', not '{}'", tok.text());
		}
		tok = parser.next_token();
		if (tok.type == TokenSymbol) {
			parseArray(parser, tok.text(), nullptr);
		} else if (tok.type != TokenName) {
			throw Error(errParseSQL, "Expected field name, but found {} in query, {}", tok.text(), parser.where());
		} else if (tok.text() == "array_remove"sv || tok.text() == "array_remove_once"sv) {
			parseCommand(parser);
		}
		tok = parser.peek_token();
	}
}

UpdateEntry SQLParser::parseUpdateField(tokenizer& parser) {
	token tok = peekSqlToken(parser, FieldNameSqlToken, false);
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected field name but found '{}' in query {}", tok.text(), parser.where());
	}
	UpdateEntry updateField{tok.text(), {}};

	parser.next_token();
	tok = parser.next_token();
	if (tok.text() != "="sv) {
		throw Error(errParams, "Expected '=' but found '{}' in query, '{}'", tok.text(), parser.where());
	}

	size_t startPos = parser.getPos();
	bool withArrayExpressions = false;

	tok = parser.next_token(tokenizer::flags::no_flags);
	if (tok.text() == "["sv) {
		updateField.Values().MarkArray();
		parseArray(parser, tok.text(), &updateField);
		updateField.SetIsExpression(false);
	} else if (tok.text() == "array_remove"sv || tok.text() == "array_remove_once"sv) {
		parseCommand(parser);

		updateField.SetIsExpression(true);
		updateField.Values() = {Variant(std::string(parser.begin() + startPos, parser.getPos() - startPos))};
	} else {
		addUpdateValue(tok, parser, updateField);
	}

	tok = parser.peek_token(tokenizer::flags::no_flags);
	while (tok.text() == "|"sv) {
		parser.next_token();
		tok = parser.next_token();
		if (tok.text() != "|"sv) {
			throw Error(errLogic, "Expected '|', not '{}'", tok.text());
		}
		tok = parser.next_token();
		if (tok.type != TokenName) {
			throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.text(), parser.where());
		} else if (tok.text() == "array_remove"sv || tok.text() == "array_remove_once"sv) {
			parseCommand(parser);
		}
		tok = parser.peek_token();
		withArrayExpressions = true;
	}

	if (withArrayExpressions) {
		updateField.SetIsExpression(true);
		updateField.Values() = {Variant(std::string(parser.begin() + startPos, parser.getPos() - startPos))};
	}

	return updateField;
}

int SQLParser::updateParse(tokenizer& parser) {
	parser.next_token();

	token tok = peekSqlToken(parser, NamespaceSqlToken);
	query_.SetNsName(tok.text());
	ctx_.updateLinkedNs(query_.NsName());
	parser.next_token();

	tok = peekSqlToken(parser, UpdateOptionsSqlToken);
	if (tok.text() == "set"sv) {
		parser.next_token();
		while (!parser.end()) {
			query_.UpdateField(parseUpdateField(parser));
			tok = parser.peek_token();
			if (tok.text() != ","sv) {
				break;
			}
			parser.next_token();
		}
	} else if (tok.text() == "drop"sv) {
		while (!parser.end()) {
			parser.next_token();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.type != TokenName) {
				throw Error(errParseSQL, "Expected field name but found '{}' in query {}", tok.text(), parser.where());
			}
			query_.Drop(std::string(tok.text()));
			parser.next_token();
			tok = parser.peek_token();
			if (tok.text() != ","sv) {
				break;
			}
		}
	} else {
		throw Error(errParseSQL, "Expected 'SET' or 'DROP' but found '{}' in query {}", tok.text(), parser.where());
	}

	parseModifyConditions(parser);

	return 0;
}

void SQLParser::parseModifyConditions(tokenizer& parser) {
	while (!parser.end()) {
		auto tok = peekSqlToken(parser, ModifyConditionsStart);
		if (tok.text() == "where"sv) {
			parser.next_token();
			parseWhere<Nested::No>(parser);
		} else if (tok.text() == "limit"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.text(), parser.where());
			}
			query_.Limit(stoi(tok.text()));
		} else if (tok.text() == "offset"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.text(), parser.where());
			}
			query_.Offset(stoi(tok.text()));
		} else if (tok.text() == "order"sv) {
			parser.next_token();
			parseOrderBy(parser, query_);
			ctx_.updateLinkedNs(query_.NsName());
		} else {
			break;
		}
	}
}

int SQLParser::truncateParse(tokenizer& parser) {
	parser.next_token();
	token tok = peekSqlToken(parser, NamespaceSqlToken);
	query_.SetNsName(tok.text());
	ctx_.updateLinkedNs(query_.NsName());
	parser.next_token();
	return 0;
}

static bool isCondition(std::string_view text) noexcept {
	return text == "="sv || text == "=="sv || text == "<>"sv || iequals(text, "is"sv) || text == ">"sv || text == ">="sv || text == "<"sv ||
		   text == "<="sv || iequals(text, "in"sv) || iequals(text, "range"sv) || iequals(text, "like"sv) || iequals(text, "allset"sv);
}

Query SQLParser::parseSubQuery(tokenizer& parser) {
	Query subquery;
	SQLParser subparser(subquery);
	const ParserContextsAppendGuard guard{ctx_, subparser.ctx_};
	if (ctx_.autocompleteMode) {
		subparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		subparser.ctx_.autocompleteMode = true;
		subparser.ctx_.foundPossibleSuggestions = ctx_.foundPossibleSuggestions;
		subparser.ctx_.possibleSuggestionDetectedInThisClause = ctx_.possibleSuggestionDetectedInThisClause;
	}
	// skip select
	auto tok = parser.next_token();
	subparser.selectParse<Nested::Yes>(parser);
	tok = parser.next_token();
	if (tok.text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found {}, {}", tok.text(), parser.where());
	}
	return subquery;
}

template <typename T>
void SQLParser::parseWhereCondition(tokenizer& parser, T&& firstArg, OpType op) {
	// Operator
	CondType condition;
	auto tok = peekSqlToken(parser, ConditionSqlToken);
	if (tok.text() == "<>"sv) {
		condition = CondEq;
		if (op == OpAnd) {
			op = OpNot;
		} else if (op == OpNot) {
			op = OpAnd;
		} else {
			throw Error(errParseSQL, "<> condition with OR is not supported, {}", parser.where());
		}
	} else {
		condition = getCondType(tok.text());
	}
	parser.next_token();

	// Value
	if (ctx_.autocompleteMode) {
		peekSqlToken(parser, WhereFieldValueSqlToken, false);
	}
	tok = parser.next_token();
	if (iequals(tok.text(), "null"sv) || iequals(tok.text(), "empty"sv)) {
		query_.NextOp(op).Where(std::forward<T>(firstArg), CondEmpty, VariantArray{});
	} else if (iequals(tok.text(), "not"sv)) {
		tok = peekSqlToken(parser, WhereFieldNegateValueSqlToken, false);
		if (!iequals(tok.text(), "null"sv) && !iequals(tok.text(), "empty"sv)) {
			throw Error(errParseSQL, "Expected NULL, but found '{}' in query, {}", tok.text(), parser.where());
		}
		query_.NextOp(op).Where(std::forward<T>(firstArg), CondAny, VariantArray{});
		tok = parser.next_token(tokenizer::flags::no_flags);
	} else if (tok.text() == "("sv) {
		if constexpr (!std::is_same_v<T, Query>) {
			if (iequals(peekSqlToken(parser, WhereFieldValueOrSubquerySqlToken, false).text(), "select"sv) &&
				!isCondition(parser.peek_second_token().text())) {
				query_.NextOp(op).Where(std::forward<T>(firstArg), condition, parseSubQuery(parser));
				return;
			}
		}
		VariantArray values;
		for (;;) {
			tok = parser.next_token();
			if (tok.text() == ")"sv && tok.type == TokenSymbol) {
				break;
			}
			values.push_back(token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_True));
			tok = parser.next_token();
			if (tok.text() == ")"sv) {
				break;
			}
			if (tok.text() != ","sv) {
				throw Error(errParseSQL, "Expected ')' or ',', but found '{}' in query, {}", tok.text(), parser.where());
			}
		}
		query_.NextOp(op).Where(std::forward<T>(firstArg), condition, std::move(values));
	} else if (tok.type != TokenName || iequals(tok.text(), "true"sv) || iequals(tok.text(), "false"sv)) {
		query_.NextOp(op).Where(std::forward<T>(firstArg), condition,
								{token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_True)});
	} else {
		if constexpr (std::is_same_v<T, Query>) {
			throw Error(errParseSQL, "Field cannot be after subquery. (text = '{}'  location = {})", tok.text(), parser.where());
		} else {
			// Second field
			query_.NextOp(op).WhereBetweenFields(std::forward<T>(firstArg), condition, std::string{tok.text()});
		}
	}
}

template <SQLParser::Nested nested>
int SQLParser::parseWhere(tokenizer& parser) {
	token tok;
	OpType nextOp = OpAnd;

	tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldSqlToken, false);

	if (iequals(tok.text(), "not"sv)) {
		nextOp = OpNot;
		parser.next_token();
	}
	int openBracketsCount = 0;
	bool expectSecondLogicalOperand = false;
	auto throwIfExpectSecondLogicalOperand = [&tok, &parser, &expectSecondLogicalOperand]() {
		if (expectSecondLogicalOperand) {
			throw Error(errParseSQL, "Expected second logical operand, but found '{}' in query '{}'", tok.text(), parser.where());
		}
	};
	while (!parser.end()) {
		tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldSqlToken, false);
		parser.next_token(tokenizer::flags::no_flags);
		while (tok.type == TokenName && iequals(tok.text(), "equal_position"sv)) {
			parseEqualPositions(parser);
			tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldSqlToken, false);
			parser.next_token(tokenizer::flags::no_flags);
		}
		expectSecondLogicalOperand = false;
		if (tok.text() == "("sv) {
			tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldOrSubquerySqlToken, false);
			// isCondition(parser.peek_second_token().text() to distinguish the token type operator 'select' or field with name 'select'
			if (nested == Nested::No && iequals(tok.text(), "select"sv) && !isCondition(parser.peek_second_token().text())) {
				parseWhereCondition(parser, parseSubQuery(parser), nextOp);
				nextOp = OpAnd;
			} else {
				query_.NextOp(nextOp);
				query_.OpenBracket();
				++openBracketsCount;
				if (iequals(tok.text(), "not"sv)) {
					nextOp = OpNot;
					parser.next_token();
				} else {
					nextOp = OpAnd;
				}
				continue;
			}
		} else if (tok.type == TokenName) {
			const auto nextToken = parser.peek_token();
			if (iequals(tok.text(), "st_dwithin"sv) && nextToken.text() == "("sv) {
				parseDWithin(parser, nextOp);
				nextOp = OpAnd;
			} else if (iequals(tok.text(), "knn"sv) && nextToken.text() == "("sv) {
				parseKnn(parser, nextOp);
				nextOp = OpAnd;
			} else if constexpr (nested == Nested::No) {
				if (iequals(tok.text(), "join"sv)) {
					parseJoin(JoinType::LeftJoin, parser);

				} else if (iequals(tok.text(), "left"sv)) {
					peekSqlToken(parser, LeftSqlToken);
					if (parser.next_token().text() != "join"sv) {
						throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.text(), parser.where());
					}
					parseJoin(JoinType::LeftJoin, parser);
				} else if (iequals(tok.text(), "inner"sv)) {
					peekSqlToken(parser, InnerSqlToken);
					if (parser.next_token().text() != "join"sv) {
						throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.text(), parser.where());
					}
					auto jtype = nextOp == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
					query_.And();
					parseJoin(jtype, parser);
				} else {
					parseWhereCondition(parser, std::string{tok.text()}, nextOp);
					nextOp = OpAnd;
				}
			} else {
				parseWhereCondition(parser, std::string{tok.text()}, nextOp);
				nextOp = OpAnd;
			}
		} else if (tok.type == TokenNumber || tok.type == TokenString) {
			throw Error(errParseSQL, "{} is invalid at this location. (text = '{}'  location = {})",
						tok.type == TokenNumber ? "Number" : "String", tok.text(), parser.where());
		} else {
			expectSecondLogicalOperand = true;
		}

		tok = parser.peek_token();
		while (tok.text() == "equal_position"sv) {
			parser.next_token();
			parseEqualPositions(parser);
			tok = parser.peek_token();
		}

		while (openBracketsCount > 0 && tok.text() == ")"sv) {
			throwIfExpectSecondLogicalOperand();
			query_.CloseBracket();
			--openBracketsCount;
			parser.next_token();
			tok = parser.peek_token();
			while (tok.text() == "equal_position"sv) {
				parser.next_token();
				parseEqualPositions(parser);
				tok = parser.peek_token();
			}
		}

		tok = peekSqlToken(parser, WhereOpSqlToken, false);
		if (iequals(tok.text(), "and"sv)) {
			throwIfExpectSecondLogicalOperand();
			nextOp = OpAnd;
			expectSecondLogicalOperand = true;
			parser.next_token();
			tok = peekSqlToken(parser, nested == Nested::Yes ? NestedAndSqlToken : AndSqlToken, false);
			if (iequals(tok.text(), "not"sv)) {
				parser.next_token();
				nextOp = OpNot;
			} else {
				continue;
			}
		} else if (iequals(tok.text(), "or"sv)) {
			throwIfExpectSecondLogicalOperand();
			parser.next_token();
			peekSqlToken(parser, FieldNameSqlToken);
			nextOp = OpOr;
			expectSecondLogicalOperand = true;
		} else if (!iequals(tok.text(), "join"sv) && !iequals(tok.text(), "inner"sv) && !iequals(tok.text(), "left"sv)) {
			break;
		}
	}
	throwIfExpectSecondLogicalOperand();

	if (query_.Entries().Empty()) {
		throw Error(errParseSQL, "Expected condition after 'WHERE'");
	}
	return 0;
}

void SQLParser::parseEqualPositions(tokenizer& parser) {
	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.text(), parser.where());
	}
	EqualPosition_t fieldNames;
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken);
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.type != TokenName) {
			throw Error(errParseSQL, "Expected name, but found '{}' in query, {}", tok.text(), parser.where());
		}
		fieldNames.emplace_back(nameWithCase.text());
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.text() == ")"sv) {
			break;
		}
		if (tok.text() != ","sv) {
			throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.text(), parser.where());
		}
	}
	query_.EqualPositions(std::move(fieldNames));
}

Point SQLParser::parseGeomFromText(tokenizer& parser) const {
	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.text(), parser.where());
	}
	tok = parser.next_token();
	if (tok.type != TokenString) {
		throw Error(errParseSQL, "Expected text, but found '{}', {}", tok.text(), parser.where());
	}
	std::string_view tokenText = tok.text();
	std::string_view str = skipSpace(tokenText);
	if (!checkIfStartsWith("point"sv, str)) {
		throw Error(errParseSQL, "Expected geometry object, but found '{}', {}", tok.text(), parser.where());
	}
	str = skipSpace(str.substr(5));
	if (str.empty() || str[0] != '(') {
		throw Error(errParseSQL, "Expected '(' after '{}', but found '{}' in '{}', {}", tokenText.substr(0, tokenText.size() - str.size()),
					str, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(1));
	size_t pos;
	double x, y;
	try {
		x = std::stod(std::string(str), &pos);
	} catch (...) {
		throw Error(errParseSQL, "Expected first number argument after '{}', but found '{}' in '{}', {}",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.text(), parser.where());
	}
	if (pos >= str.size()) {
		throw Error(errParseSQL, "Expected space after '{}', but found nothing in '{}', {}", tokenText, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(pos));
	try {
		y = std::stod(std::string(str), &pos);
	} catch (...) {
		throw Error(errParseSQL, "Expected second number argument after '{}', but found '{}' in '{}', {}",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.text(), parser.where());
	}
	if (pos >= str.size()) {
		throw Error(errParseSQL, "Expected ')' after '{}', but found nothing in '{}', {}", tokenText, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(pos));
	if (str.empty() || str[0] != ')') {
		throw Error(errParseSQL, "Expected ')' after '{}', but found '{}' in '{}', {}", tokenText.substr(0, tokenText.size() - str.size()),
					str, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(1));
	if (!str.empty()) {
		throw Error(errParseSQL, "Expected nothing after '{}', but found '{}' in '{}', {}",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.text(), parser.where());
	}

	tok = parser.next_token();
	if (tok.text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.text(), parser.where());
	}
	return Point{x, y};
}
template <typename T>
static auto parseNumber(const auto& begin, const auto& end, T& value) {
	if constexpr (std::is_floating_point_v<T>) {
		using namespace double_conversion;
		static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
		int countOfCharsParsedAsDouble;
		value = converter.StringToDouble(begin, end - begin, &countOfCharsParsedAsDouble);
	} else if constexpr (std::is_integral_v<T>) {
		return std::from_chars(begin, end, value).ec;
	}
	return std::errc{};
}

template <typename T>
void SQLParser::parseSingleKnnParam(tokenizer& parser, std::optional<T>& param, std::string_view paramName) {
	if (param.has_value()) {
		throw Error(errParseSQL, "Dublicate KNN parameter '{}': {}", paramName, parser.where());
	}
	auto tok = parser.next_token();
	if (tok.text() != "="sv) {
		throw Error(errParseSQL, "Expected '=', but found {}, {}", tok.text(), parser.where());
	}
	tok = parser.next_token();
	if (tok.type != TokenNumber) {
		throw Error(errParseSQL, "Expected number greater than 0, but found {}, {}", tok.text(), parser.where());
	}
	T paramValue;
	const auto res = parseNumber(tok.text().data(), tok.text().data() + tok.text().size(), paramValue);
	if (res != std::errc{}) {
		throw Error(errParseSQL, "Expected float or int greater than 0, but found {}, {}", tok.text(), parser.where());
	}

	param = paramValue;
}

KnnSearchParams SQLParser::parseKnnParams(tokenizer& parser) {
	std::optional<size_t> k, ef, nprobe;
	std::optional<float> radius;
	do {
		peekSqlToken(parser, KnnParamsToken);
		auto tok = parser.next_token();
		if (tok.text() == KnnSearchParams::kKName) {
			parseSingleKnnParam(parser, k, KnnSearchParams::kKName);
		} else if (tok.text() == KnnSearchParams::kRadiusName) {
			parseSingleKnnParam(parser, radius, KnnSearchParams::kRadiusName);
		} else if (tok.text() == KnnSearchParams::kEfName) {
			if (nprobe) {
				throw Error{errParseSQL, "Wrong SQL format: KNN query cannot contain both of '{}' and '{}'", KnnSearchParams::kEfName,
							KnnSearchParams::kNProbeName};
			}
			parseSingleKnnParam(parser, ef, KnnSearchParams::kEfName);
		} else if (tok.text() == KnnSearchParams::kNProbeName) {
			if (ef) {
				throw Error{errParseSQL, "Wrong SQL format: KNN query cannot contain both of '{}' and '{}'", KnnSearchParams::kEfName,
							KnnSearchParams::kNProbeName};
			}
			parseSingleKnnParam(parser, nprobe, KnnSearchParams::kNProbeName);
		} else {
			throw Error(errParseSQL, "Expected KNN parameter, but found '{}', {}", tok.text(), parser.where());
		}
		tok = parser.next_token();
		if (tok.text() == ")"sv) {
			break;
		} else if (tok.text() != ","sv) {
			throw Error(errParseSQL, "Expected ',' or ')', but found '{}', {}", tok.text(), parser.where());
		}
	} while (true);
	if (!k && !radius) {
		throw Error(errParseSQL, "The presence of one of the '{}' or '{}' parameters in KNN query is mandatory; {}",
					KnnSearchParams::kKName, KnnSearchParams::kRadiusName, parser.where());
	}
	if (ef.has_value()) {
		return HnswSearchParams().K(k).Radius(radius).Ef(*ef);
	} else if (nprobe.has_value()) {
		return IvfSearchParams().K(k).Radius(radius).NProbe(*nprobe);
	} else {
		return KnnSearchParamsBase().K(k).Radius(radius);
	}
}

void SQLParser::parseKnn(tokenizer& parser, OpType nextOp) {
	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.text(), parser.where());
	}
	peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.next_token();
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected field name, but found '{}', {}", tok.text(), parser.where());
	}
	std::string field(tok.text());
	tok = parser.next_token();
	if (tok.text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.text(), parser.where());
	}
	tok = parser.next_token();
	if (tok.text() != "["sv) {
		if (tok.type == TokenString) {
			std::string value(tok.text());
			tok = parser.next_token();
			if (tok.text() != ","sv) {
				throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.text(), parser.where());
			}
			query_.NextOp(nextOp).WhereKNN(std::move(field), std::move(value), parseKnnParams(parser));
			return;	 // NOTE: stop processing
		}

		throw Error(errParseSQL, "Expected '[' or ''', but found '{}', {}", tok.text(), parser.where());
	}
	std::vector<float> vec;
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_LEADING_SPACES |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   NAN, NAN, nullptr, nullptr};
	do {
		tok = parser.next_token();
		if (tok.type != TokenNumber) {
			throw Error(errParseSQL, "Expected number, but found '{}', {}", tok.text(), parser.where());
		}
		int countOfCharsParsedAsDouble;
		vec.push_back(0);
		try {
			vec.back() = converter.StringToDouble(tok.text().data(), tok.text().size(), &countOfCharsParsedAsDouble);
		} catch (...) {
			throw Error(errParseSQL, "Expected number, but found '{}', {}", tok.text(), parser.where());
		}
		tok = parser.next_token();
		if (tok.text() == "]"sv) {
			break;
		} else if (tok.text() != ","sv) {
			throw Error(errParseSQL, "Expected ',' or ']', but found '{}', {}", tok.text(), parser.where());
		}
	} while (true);
	const ConstFloatVectorView vecView{std::span<float>(vec)};

	tok = parser.next_token();
	if (tok.text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.text(), parser.where());
	}

	query_.NextOp(nextOp).WhereKNN(std::move(field), vecView, parseKnnParams(parser));
}

void SQLParser::parseDWithin(tokenizer& parser, OpType nextOp) {
	Point point;
	std::string field;

	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.text(), parser.where());
	}

	peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.next_token();
	if (iequals(tok.text(), "st_geomfromtext"sv)) {
		point = parseGeomFromText(parser);
	} else {
		field = std::string(tok.text());
	}

	tok = parser.next_token();
	if (tok.text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.text(), parser.where());
	}

	peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.next_token();
	if (iequals(tok.text(), "st_geomfromtext"sv)) {
		if (field.empty()) {
			throw Error(errParseSQL, "Expected field name, but found '{}', {}", tok.text(), parser.where());
		}
		point = parseGeomFromText(parser);
	} else {
		if (!field.empty()) {
			throw Error(errParseSQL, "Expected geometry object, but found '{}', {}", tok.text(), parser.where());
		}
		field = std::string(tok.text());
	}

	tok = parser.next_token();
	if (tok.text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.text(), parser.where());
	}

	tok = parser.next_token();
	const auto distance = token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_False);
	distance.Type().EvaluateOneOf(
		[](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float>) noexcept {},
		[&](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Composite,
				  KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector>) {
			throw Error(errParseSQL, "Expected number, but found '{}', {}", tok.text(), parser.where());
		});

	tok = parser.next_token();
	if (tok.text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.text(), parser.where());
	}
	query_.NextOp(nextOp).DWithin(field, point, distance.As<double>());
}

void SQLParser::parseJoin(JoinType type, tokenizer& parser) {
	JoinedQuery jquery;
	SQLParser jparser(jquery);
	const ParserContextsAppendGuard guard{ctx_, jparser.ctx_};
	if (ctx_.autocompleteMode) {
		jparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		jparser.ctx_.autocompleteMode = true;
		jparser.ctx_.foundPossibleSuggestions = ctx_.foundPossibleSuggestions;
		jparser.ctx_.possibleSuggestionDetectedInThisClause = ctx_.possibleSuggestionDetectedInThisClause;
	}
	peekSqlToken(parser, NamespaceSqlToken);
	auto tok = parser.next_token();
	if (tok.text() == "("sv) {
		peekSqlToken(parser, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found '{}', {}", tok.text(), parser.where());
		}

		jparser.selectParse<Nested::Yes>(parser);

		tok = parser.next_token();
		if (tok.text() != ")"sv) {
			throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.text(), parser.where());
		}
	} else {
		jquery.SetNsName(tok.text());
		ctx_.updateLinkedNs(jquery.NsName());
	}
	jquery.joinType = type;
	jparser.parseJoinEntries(parser, query_.NsName(), jquery);

	query_.Join(std::move(jquery));
}

void SQLParser::parseMerge(tokenizer& parser) {
	JoinedQuery mquery;
	SQLParser mparser(mquery);
	const ParserContextsAppendGuard guard{ctx_, mparser.ctx_};
	if (ctx_.autocompleteMode) {
		mparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		mparser.ctx_.autocompleteMode = true;
		mparser.ctx_.foundPossibleSuggestions = ctx_.foundPossibleSuggestions;
		mparser.ctx_.possibleSuggestionDetectedInThisClause = ctx_.possibleSuggestionDetectedInThisClause;
	}
	auto tok = parser.next_token();
	if (tok.text() == "("sv) {
		peekSqlToken(parser, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found '{}', {}", tok.text(), parser.where());
		}

		mparser.selectParse<Nested::No>(parser);

		tok = parser.next_token();
		if (tok.text() != ")"sv) {
			throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.text(), parser.where());
		}
	}
	mquery.joinType = JoinType::Merge;

	query_.Merge(std::move(mquery));
}

std::string SQLParser::parseJoinedFieldName(tokenizer& parser, std::string& name) {
	auto tok = peekSqlToken(parser, JoinedFieldNameSqlToken);
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected name, but found '{}', {}", tok.text(), parser.where());
	}

	auto dotPos = tok.text().find('.');
	if (dotPos == std::string_view::npos) {
		return std::string(tok.text());
	}
	name = std::string(tok.text().substr(0, dotPos));

	tok = peekSqlToken(parser, FieldNameSqlToken);
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected name, but found '{}', {}", tok.text(), parser.where());
	}
	parser.next_token();
	ctx_.updateLinkedNs(name);
	return std::string(tok.text().substr(dotPos + 1));
}

void SQLParser::parseJoinEntries(tokenizer& parser, const std::string& mainNs, JoinedQuery& jquery) {
	auto tok = peekSqlToken(parser, OnSqlToken);
	if (tok.text() != "on"sv) {
		throw Error(errParseSQL, "Expected 'ON', but found '{}', {}", tok.text(), parser.where());
	}
	parser.next_token();

	tok = parser.peek_token();

	bool braces = tok.text() == "("sv;
	if (braces) {
		parser.next_token();
	}

	while (!parser.end()) {
		tok = peekSqlToken(parser, OpSqlToken);
		if (tok.text() == "or"sv) {
			jquery.Or();
			parser.next_token();
			tok = parser.peek_token();
		} else if (tok.text() == "and"sv) {
			jquery.And();
			parser.next_token();
			tok = parser.peek_token();
		}

		if (braces && tok.text() == ")"sv) {
			parser.next_token();
			return;
		}

		std::string ns1 = mainNs, ns2 = jquery.NsName();
		std::string fld1 = parseJoinedFieldName(parser, ns1);
		CondType condition = getCondType(parser.next_token().text());
		std::string fld2 = parseJoinedFieldName(parser, ns2);
		bool reverseNamespacesOrder{false};

		if (ns1 != mainNs || ns2 != jquery.NsName()) {
			if (ns2 == mainNs && ns1 == jquery.NsName()) {
				std::swap(fld1, fld2);
				condition = InvertJoinCondition(condition);
				reverseNamespacesOrder = true;
			} else {
				throw Error(errParseSQL, "Unexpected tables with ON statement: ('{}' and '{}') but expected ('{}' and '{}'), {}", ns1, ns2,
							mainNs, jquery.NsName(), parser.where());
			}
		}

		jquery.joinEntries_.emplace_back(jquery.NextOp(), condition, std::move(fld1), std::move(fld2), reverseNamespacesOrder);
		jquery.And();
		if (!braces) {
			return;
		}
	}
}
CondType SQLParser::getCondType(std::string_view cond) {
	if (cond == "="sv || cond == "=="sv || iequals(cond, "is"sv)) {
		return CondEq;
	} else if (cond == ">"sv) {
		return CondGt;
	} else if (cond == ">="sv) {
		return CondGe;
	} else if (cond == "<"sv) {
		return CondLt;
	} else if (cond == "<="sv) {
		return CondLe;
	} else if (iequals(cond, "in"sv)) {
		return CondSet;
	} else if (iequals(cond, "range"sv)) {
		return CondRange;
	} else if (iequals(cond, "like"sv)) {
		return CondLike;
	} else if (iequals(cond, "allset"sv)) {
		return CondAllSet;
	}
	throw Error(errParseSQL, "Expected condition operator, but found '{}' in query", cond);
}

}  // namespace reindexer
