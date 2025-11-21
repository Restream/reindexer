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
	Tokenizer parser(q);
	Query query;
	std::ignore = SQLParser{query}.Parse(parser);
	return query;
}

bool SQLParser::reachedAutocompleteToken(Tokenizer& parser, const Token& tok) const {
	const size_t pos = parser.GetPos() + tok.Text().length();
	return pos > ctx_.suggestionsPos;
}

Token SQLParser::peekSqlToken(Tokenizer& parser, SqlTokenType tokenType, bool toLower) {
	auto tok = parser.PeekToken(toLower ? Tokenizer::Flags::ToLower : Tokenizer::Flags::NoFlags);
	const bool eof = ((parser.GetPos() + tok.Text().length()) == parser.Length());
	if (ctx_.autocompleteMode && reachedAutocompleteToken(parser, tok)) {
		size_t tokenLen = 0;
		if (ctx_.suggestionsPos >= parser.GetPos()) {
			tokenLen = ctx_.suggestionsPos - parser.GetPos() + 1;
		}
		if (!ctx_.foundPossibleSuggestions || tokenLen) {
			// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
			ctx_.suggestions.emplace_back(std::string(tok.Text().data(), std::min(tok.Text().size(), tokenLen)), tokenType);
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

int SQLParser::Parse(Tokenizer& parser) {
	parser.SkipSpace();
	if (parser.Length() == 0) {
		ctx_.suggestions.emplace_back(std::string(), Start);
		return 0;
	}
	auto tok = peekSqlToken(parser, Start);
	if (tok.Text() == "explain"sv) {
		query_.Explain();
		parser.SkipToken();
		tok = peekSqlToken(parser, StartAfterExplain);
		if (tok.Text() == "local"sv) {
			query_.Local();
			parser.SkipToken();
			tok = peekSqlToken(parser, StartAfterLocalExplain);
		}
	} else if (tok.Text() == "local"sv) {
		query_.Local();
		parser.SkipToken();
		tok = peekSqlToken(parser, StartAfterLocal);
		if (tok.Text() == "explain"sv) {
			query_.Explain();
			parser.SkipToken();
			tok = peekSqlToken(parser, StartAfterLocalExplain);
		}
	}

	if (tok.Text() == "select"sv) {
		query_.type_ = QuerySelect;
		parser.SkipToken();
		selectParse<Nested::No>(parser);
	} else if (query_.IsLocal()) {
		throw Error(errParams, "Syntax error at or near '{}', {}; only SELECT query could be LOCAL", tok.Text(), parser.Where(tok));
	} else if (tok.Text() == "delete"sv) {
		query_.type_ = QueryDelete;
		tok = parser.NextToken();
		deleteParse(parser);
	} else if (tok.Text() == "update"sv) {
		query_.type_ = QueryUpdate;
		updateParse(parser);
	} else if (tok.Text() == "truncate"sv) {
		query_.type_ = QueryTruncate;
		truncateParse(parser);
	} else {
		throw Error(errParams, "Syntax error at or near '{}', {}", tok.Text(), parser.Where(tok));
	}

	tok = parser.NextToken();
	if (tok.Text() == ";"sv) {
		tok = parser.NextToken();
	}
	parser.SkipSpace();
	if (!tok.Text().empty() || !parser.End()) {
		throw Error(errParseSQL, "Unexpected '{}' in query, {}", tok.Text(), parser.Where(tok));
	}

	return 0;
}

template <SQLParser::Nested nested>
void SQLParser::selectParse(Tokenizer& parser) {
	// Get filter
	Token tok;
	bool wasSelectFilter = false;
	while (true) {
		auto nameWithCase = peekSqlToken(parser, SingleSelectFieldSqlToken, false);
		auto name = parser.NextToken();
		tok = peekSqlToken(parser, FromSqlToken);
		if (tok.Text() == "("sv) {
			parser.SkipToken();
			tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
			if (name.Text() == "count"sv) {
				query_.CalcTotal(ModeAccurateTotal);
				if (!wasSelectFilter) {
					query_.Limit(0);
				}
				tok = parser.NextToken();
				if (tok.Text() != "*"sv) {
					throw Error(errParseSQL, "Expected '*', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
				}
			} else if (name.Text() == "count_cached"sv) {
				query_.CalcTotal(ModeCachedTotal);
				if (!wasSelectFilter) {
					query_.Limit(0);
				}
				tok = parser.NextToken();
				if (tok.Text() != "*"sv) {
					throw Error(errParseSQL, "Expected '*', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
				}
			} else if (name.Text() == "rank"sv) {
				query_.WithRank();
			} else if (name.Text() == "vectors"sv) {
				if (!query_.CanAddSelectFilter()) {
					throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
				}
				query_.Limit(QueryEntry::kDefaultLimit);
				wasSelectFilter = true;
				query_.Select(FieldsNamesFilter::kAllVectorFieldsName);
			} else {
				AggType agg = AggregationResult::StrToAggType(name.Text());
				if (agg != AggUnknown) {
					if (!query_.CanAddAggregation(agg) || (wasSelectFilter && agg != AggDistinct)) {
						throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
					}
					if (tok.Type() != TokenName) {
						throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
					}
					h_vector<std::string, 1> fields{{std::string(tok.Text())}};
					tok = parser.NextToken();
					for (tok = parser.PeekToken(); tok.Type() == TokenSymbol && tok.Text() == ","sv; tok = parser.PeekToken()) {
						parser.SkipToken();
						tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
						if (tok.Type() != TokenName) {
							throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
						}
						fields.emplace_back(tok.Text());
						tok = parser.NextToken();
					}
					AggregateEntry entry{agg, std::move(fields)};
					for (tok = parser.PeekToken(); tok.Text() != ")"sv; tok = parser.PeekToken()) {
						if (tok.Text() == "order"sv) {
							parser.SkipToken();
							SortingEntries sortingEntries;
							parseOrderBy(parser, entry);
							for (auto s : sortingEntries) {
								entry.AddSortingEntry(std::move(s));
							}
						} else if (tok.Text() == "limit"sv) {
							parser.SkipToken();
							tok = parser.NextToken();
							if (tok.Type() != TokenNumber) {
								throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
							}
							entry.SetLimit(stoi(tok.Text()));
						} else if (tok.Text() == "offset"sv) {
							parser.SkipToken();
							tok = parser.NextToken();
							if (tok.Type() != TokenNumber) {
								throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
							}
							entry.SetOffset(stoi(tok.Text()));
						} else {
							break;
						}
					}
					query_.aggregations_.emplace_back(std::move(entry));
				} else {
					throw Error(errParams, "Unknown function name SQL - '{}', {}", name.Text(), parser.Where(name));
				}
			}
			tok = parser.PeekToken();
			if (tok.Text() != ")"sv) {
				throw Error(errParams, "Expected ')', but found '{}', {}", tok.Text(), parser.Where(tok));
			}
			parser.SkipToken();
			tok = peekSqlToken(parser, FromSqlToken);
		} else if (name.Text() != "*"sv) {
			if (!query_.CanAddSelectFilter()) {
				throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
			}
			query_.Select(nameWithCase.Text());
			query_.Limit(QueryEntry::kDefaultLimit);
			wasSelectFilter = true;
		} else if (name.Text() == "*"sv) {
			if (!query_.CanAddSelectFilter()) {
				throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
			}
			query_.Limit(QueryEntry::kDefaultLimit);
			wasSelectFilter = true;
			query_.Select("*");
		}
		if (tok.Text() != ","sv) {
			break;
		}
		tok = parser.NextToken();
	}

	std::ignore = peekSqlToken(parser, FromSqlToken);
	tok = parser.NextToken();
	if (tok.Text() != "from"sv) {
		throw Error(errParams, "Expected 'FROM', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
	}

	auto nameWithCase = peekSqlToken(parser, NamespaceSqlToken, false);
	parser.SkipToken();
	query_.SetNsName(nameWithCase.Text());
	ctx_.updateLinkedNs(query_.NsName());

	do {
		tok = peekSqlToken(parser, nested == Nested::Yes ? NestedSelectConditionsStart : SelectConditionsStart);
		if (tok.Text() == "where"sv) {
			parser.SkipToken();
			parseWhere<nested>(parser);
		} else if (tok.Text() == "limit"sv) {
			parser.SkipToken();
			tok = parser.NextToken();
			if (tok.Type() != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
			}
			query_.Limit(stoi(tok.Text()));
		} else if (tok.Text() == "offset"sv) {
			parser.SkipToken();
			tok = parser.NextToken();
			if (tok.Type() != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
			}
			query_.Offset(stoi(tok.Text()));
		} else if (tok.Text() == "order"sv) {
			parser.SkipToken();
			parseOrderBy(parser, query_);
			ctx_.updateLinkedNs(query_.NsName());
		} else if constexpr (nested == Nested::No) {
			if (tok.Text() == "join"sv) {
				parser.SkipToken();
				parseJoin(JoinType::LeftJoin, parser);
			} else if (tok.Text() == "left"sv) {
				parser.SkipToken();
				std::ignore = peekSqlToken(parser, LeftSqlToken);
				tok = parser.NextToken();
				if (tok.Text() != "join"sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
				}
				parseJoin(JoinType::LeftJoin, parser);
			} else if (tok.Text() == "inner"sv) {
				parser.SkipToken();
				std::ignore = peekSqlToken(parser, InnerSqlToken);
				tok = parser.NextToken();
				if (tok.Text() != "join"sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
				}
				auto jtype = (query_.NextOp() == OpOr) ? JoinType::OrInnerJoin : JoinType::InnerJoin;
				query_.And();
				parseJoin(jtype, parser);
			} else if (tok.Text() == "merge"sv) {
				parser.SkipToken();
				parseMerge(parser);
			} else if (tok.Text() == "or"sv) {
				parser.SkipToken();
				query_.Or();
			} else {
				break;
			}
		} else {
			break;
		}
	} while (!parser.End());
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

class [[nodiscard]] SQLParser::ParserContextsAppendGuard {
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

Variant Token2kv(const Token& tok, Tokenizer& parser, CompositeAllowed allowComposite, FieldAllowed allowField, NullAllowed allowNull) {
	if (tok.Text() == "{"sv) {
		// Composite value parsing
		if (!allowComposite) {
			throw Error(errParseSQL, "Unexpected '{{' in query, {}", parser.Where(tok));
		}
		VariantArray compositeValues;
		for (;;) {
			auto nextTok = parser.NextToken();
			compositeValues.push_back(Token2kv(nextTok, parser, CompositeAllowed_False, FieldAllowed_False, allowNull));
			nextTok = parser.NextToken();
			if (nextTok.Text() == "}"sv) {
				return Variant(compositeValues);  // end process
			}
			if (nextTok.Text() != ","sv) {
				throw Error(errParseSQL, "Expected ',', but found '{}' in query, {}", nextTok.Text(), parser.Where(nextTok));
			}
		}
	}

	const std::string_view value = tok.Text();
	if (tok.Type() == TokenName) {
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
			return Variant{};
		}
	}

	if (tok.Type() != TokenNumber && tok.Type() != TokenString) {
		throw Error(errParseSQL, "Expected parameter, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
	}

	return GetVariantFromToken(tok);
}

template <typename Sortable>
void SQLParser::parseOrderBy(Tokenizer& parser, Sortable& sortable) {
	// Just skip token (BY)
	std::ignore = peekSqlToken(parser, BySqlToken);
	parser.SkipToken();
	Token tok, nameWithCase;
	for (;;) {
		nameWithCase = peekSqlToken(parser, FieldNameSqlToken, false);
		if (nameWithCase.Type() != TokenName && nameWithCase.Type() != TokenString) {
			throw Error(errParseSQL, "Expected name, but found '{}' in query, {}", nameWithCase.Text(), parser.Where(nameWithCase));
		}
		tok = parser.NextToken(Tokenizer::Flags::InOrderBy);
		std::string sortExpression(tok.Text());
		if (sortExpression.empty()) {
			throw Error(errParseSQL, "Order by expression should not be empty, {}", parser.Where(tok));
		}
		tok = peekSqlToken(parser, SortDirectionSqlToken);
		std::vector<Variant> forcedSortOrder;
		if (tok.Text() == "("sv && iequals(nameWithCase.Text(), "field"sv)) {
			parser.SkipToken();
			nameWithCase = peekSqlToken(parser, FieldNameSqlToken, false);
			if (nameWithCase.Type() != TokenName) {
				throw Error(errParseSQL, "Expected name, but found '{}' in query, {}", nameWithCase.Text(), parser.Where(nameWithCase));
			}
			sortExpression = nameWithCase.Text();
			tok = parser.NextToken(Tokenizer::Flags::NoFlags);
			for (;;) {
				tok = parser.NextToken();
				if (tok.Text() == ")"sv) {
					break;
				}
				if (tok.Text() != ","sv) {
					throw Error(errParseSQL, "Expected ')' or ',', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
				}
				tok = parser.NextToken();
				forcedSortOrder.push_back(Token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_False));
			}
			tok = parser.PeekToken();
		}

		bool desc = false;
		if (tok.Text() == "asc"sv || tok.Text() == "desc"sv) {
			desc = tok.Text() == "desc"sv;
			parser.SkipToken();
		}
		if constexpr (std::is_same_v<Sortable, AggregateEntry>) {
			if (!forcedSortOrder.empty()) {
				throw Error(errParseSQL, "Forced sort order is not available in aggregation sort");
			}
			sortable.Sort(std::move(sortExpression), desc);
		} else {
			sortable.Sort(std::move(sortExpression), desc, std::move(forcedSortOrder));
		}

		tok = parser.PeekToken();
		if (tok.Text() != ","sv) {
			break;
		}
		parser.SkipToken();
	}
}

void SQLParser::deleteParse(Tokenizer& parser) {
	// Get filter
	auto tok = peekSqlToken(parser, FromSqlToken);
	if (parser.NextToken().Text() != "from"sv) {
		throw Error(errParams, "Expected 'FROM', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
	}

	tok = peekSqlToken(parser, NamespaceSqlToken, false);
	parser.SkipToken();
	query_.SetNsName(tok.Text());
	ctx_.updateLinkedNs(query_.NsName());

	parseModifyConditions(parser);
}

static void addUpdateValue(const Token& tok, Tokenizer& parser, UpdateEntry& updateField) {
	if (tok.Type() == TokenString) {
		updateField.Values().push_back(Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True));
	} else {
		if (tok.Text() == "null"sv) {
			updateField.Values().push_back(Variant());
		} else if (tok.Text() == "{"sv) {
			try {
				const size_t jsonPos = parser.GetPrevPos();
				std::string json(parser.Begin() + jsonPos, parser.Length() - jsonPos);
				size_t jsonLength = 0;
				gason::JsonParser jsonParser;
				jsonParser.Parse(giftStr(json), &jsonLength);
				updateField.Values().emplace_back(Variant(std::string(parser.Begin() + jsonPos, jsonLength)));
				updateField.SetMode(FieldModeSetJson);
				parser.SetPos(jsonPos + jsonLength);
			} catch (const gason::Exception& e) {
				throw Error(errParseSQL, "{}, in query {}", e.what(), parser.Where(tok));
			}
		} else {
			auto eof = [](Tokenizer& parser, bool& inArray) -> bool {
				if (parser.End()) {
					return true;
				}
				auto nextTok = parser.PeekToken();
				bool result = (nextTok.Text() == "where"sv) || (nextTok.Text() == "order"sv) || (nextTok.Text() == "limit"sv) ||
							  (nextTok.Text() == "offset"sv) || (!inArray && nextTok.Text() == "]"sv) ||
							  (!inArray && nextTok.Text() == ","sv);
				if (nextTok.Text() == "["sv && !inArray) {
					inArray = true;
				}
				if (nextTok.Text() == "]"sv && inArray) {
					inArray = false;
				}
				return result;
			};
			int count = 0;
			std::string expression(tok.Text());
			bool inArray = false;
			while (!eof(parser, inArray)) {
				auto nextTok = parser.NextToken(Tokenizer::Flags::TreatSignAsToken);
				expression += (nextTok.Type() == TokenString) ? ('\'' + std::string{nextTok.Text()} + '\'') : std::string{nextTok.Text()};
				++count;
			}
			if (count > 0) {
				updateField.Values().emplace_back(std::move(expression));
				updateField.SetIsExpression(true);
			} else {
				try {
					Variant val = Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True);
					updateField.Values().emplace_back(std::move(val));
				} catch (const Error&) {
					updateField.Values().emplace_back(std::move(expression));
					updateField.SetIsExpression(true);
				}
			}
		}
	}
}

void SQLParser::parseArray(Tokenizer& parser, std::string_view tokText, UpdateEntry* updateField) const {
	if (tokText != "["sv) {
		throw Error(errParams, "Expected '[' after field parameter, not {}", tokText);
	}

	Token nextTok;
	for (;;) {
		nextTok = parser.NextToken(Tokenizer::Flags::NoFlags);
		if (nextTok.Text() == "]"sv) {
			if (updateField && updateField->Values().empty()) {
				break;
			}
			throw Error(errParseSQL, "Expected field value, but found ']' in query, {}", parser.Where(nextTok));
		}
		if (updateField) {
			addUpdateValue(nextTok, parser, *updateField);
		}
		nextTok = parser.NextToken(Tokenizer::Flags::NoFlags);
		if (nextTok.Text() == "]"sv) {
			break;
		}
		if (nextTok.Text() != ","sv) {
			throw Error(errParseSQL, "Expected ']' or ',', but found '{}' in query, {}", nextTok.Text(), parser.Where(nextTok));
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

void SQLParser::parseCommand(Tokenizer& parser) const {
	// parse input, for example: array_remove(array_field, [24, 3, 81]) || [11,22]

	auto tok = parser.NextToken();
	if (tok.Text() != "("sv) {
		throw Error(errParams, "Expected '(' after command name, not {}", tok.Text());
	}

	tok = parser.NextToken();
	if (tok.Type() != TokenName) {
		throw Error(errLogic, "Expected field name, but found {} in query, {}", tok.Text(), parser.Where(tok));
	}

	tok = parser.NextToken();
	if (tok.Text() != ","sv) {
		throw Error(errParams, "Expected ',' after field parameter, not {}", tok.Text());
	}

	// parse item or list of elements or field to be deleted
	tok = parser.NextToken();
	// try parse as scalar value
	if ((tok.Type() == TokenNumber) || (tok.Type() == TokenString) || (tok.Type() == TokenName)) {
		std::ignore = Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_True, NullAllowed_False);
	} else {
		parseArray(parser, tok.Text(), nullptr);
	}

	tok = parser.NextToken();
	if (tok.Text() != ")"sv) {
		throw Error(errParams, "Expected ')' after command name and params, not {}", tok.Text());
	}

	// parse of possible concatenation
	tok = parser.PeekToken(Tokenizer::Flags::NoFlags);
	while (tok.Text() == "|"sv) {
		parser.SkipToken();
		tok = parser.NextToken();
		if (tok.Text() != "|"sv) {
			throw Error(errLogic, "Expected '|', not '{}'", tok.Text());
		}
		tok = parser.NextToken();
		if (tok.Type() == TokenSymbol) {
			parseArray(parser, tok.Text(), nullptr);
		} else if (tok.Type() != TokenName) {
			throw Error(errParseSQL, "Expected field name, but found {} in query, {}", tok.Text(), parser.Where(tok));
		} else if (tok.Text() == "array_remove"sv || tok.Text() == "array_remove_once"sv) {
			parseCommand(parser);
		}
		tok = parser.PeekToken();
	}
}

UpdateEntry SQLParser::parseUpdateField(Tokenizer& parser) {
	auto tok = peekSqlToken(parser, FieldNameSqlToken, false);
	if (tok.Type() != TokenName) {
		throw Error(errParseSQL, "Expected field name but found '{}' in query {}", tok.Text(), parser.Where(tok));
	}
	UpdateEntry updateField{tok.Text(), {}};

	parser.SkipToken();
	tok = parser.NextToken();
	if (tok.Text() != "="sv) {
		throw Error(errParams, "Expected '=' but found '{}' in query, '{}'", tok.Text(), parser.Where(tok));
	}

	const size_t startPos = parser.GetPos();
	bool withArrayExpressions = false;

	tok = parser.NextToken(Tokenizer::Flags::NoFlags);
	if (tok.Text() == "["sv) {
		std::ignore = updateField.Values().MarkArray();
		parseArray(parser, tok.Text(), &updateField);
		updateField.SetIsExpression(false);
	} else if (tok.Text() == "array_remove"sv || tok.Text() == "array_remove_once"sv) {
		parseCommand(parser);

		updateField.SetIsExpression(true);
		updateField.Values() = {Variant(std::string(parser.Begin() + startPos, parser.GetPos() - startPos))};
	} else {
		addUpdateValue(tok, parser, updateField);
	}

	tok = parser.PeekToken(Tokenizer::Flags::NoFlags);
	while (tok.Text() == "|"sv) {
		parser.SkipToken();
		tok = parser.NextToken();
		if (tok.Text() != "|"sv) {
			throw Error(errLogic, "Expected '|', not '{}'", tok.Text());
		}
		tok = parser.NextToken();
		if (tok.Type() != TokenName) {
			throw Error(errParseSQL, "Expected field name, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
		} else if (tok.Text() == "array_remove"sv || tok.Text() == "array_remove_once"sv) {
			parseCommand(parser);
		}
		tok = parser.PeekToken();
		withArrayExpressions = true;
	}

	if (withArrayExpressions) {
		updateField.SetIsExpression(true);
		updateField.Values() = {Variant(std::string(parser.Begin() + startPos, parser.GetPos() - startPos))};
	}

	return updateField;
}

void SQLParser::updateParse(Tokenizer& parser) {
	parser.SkipToken();

	auto tok = peekSqlToken(parser, NamespaceSqlToken, false);
	query_.SetNsName(tok.Text());
	ctx_.updateLinkedNs(query_.NsName());
	parser.SkipToken();

	tok = peekSqlToken(parser, UpdateOptionsSqlToken);
	if (tok.Text() == "set"sv) {
		parser.SkipToken();
		while (!parser.End()) {
			query_.UpdateField(parseUpdateField(parser));
			tok = parser.PeekToken();
			if (tok.Text() != ","sv) {
				break;
			}
			parser.SkipToken();
		}
	} else if (tok.Text() == "drop"sv) {
		while (!parser.End()) {
			parser.SkipToken();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.Type() != TokenName) {
				throw Error(errParseSQL, "Expected field name but found '{}' in query {}", tok.Text(), parser.Where(tok));
			}
			query_.Drop(std::string(tok.Text()));
			parser.SkipToken();
			tok = parser.PeekToken();
			if (tok.Text() != ","sv) {
				break;
			}
		}
	} else {
		throw Error(errParseSQL, "Expected 'SET' or 'DROP' but found '{}' in query {}", tok.Text(), parser.Where(tok));
	}

	parseModifyConditions(parser);
}

void SQLParser::parseModifyConditions(Tokenizer& parser) {
	Token tok;
	while (!parser.End()) {
		tok = peekSqlToken(parser, ModifyConditionsStart);
		if (tok.Text() == "where"sv) {
			parser.SkipToken();
			parseWhere<Nested::No>(parser);
		} else if (tok.Text() == "limit"sv) {
			parser.SkipToken();
			tok = parser.NextToken();
			if (tok.Type() != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
			}
			query_.Limit(stoi(tok.Text()));
		} else if (tok.Text() == "offset"sv) {
			parser.SkipToken();
			tok = parser.NextToken();
			if (tok.Type() != TokenNumber) {
				throw Error(errParseSQL, "Expected number, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
			}
			query_.Offset(stoi(tok.Text()));
		} else if (tok.Text() == "order"sv) {
			parser.SkipToken();
			parseOrderBy(parser, query_);
			ctx_.updateLinkedNs(query_.NsName());
		} else {
			break;
		}
	}
}

void SQLParser::truncateParse(Tokenizer& parser) {
	parser.SkipToken();
	auto tok = peekSqlToken(parser, NamespaceSqlToken, false);
	query_.SetNsName(tok.Text());
	ctx_.updateLinkedNs(query_.NsName());
	parser.SkipToken();
}

RX_ALWAYS_INLINE static bool isCondition(std::string_view text) noexcept {
	return text == "="sv || text == "=="sv || text == "<>"sv || iequals(text, "is"sv) || text == ">"sv || text == ">="sv || text == "<"sv ||
		   text == "<="sv || iequals(text, "in"sv) || iequals(text, "range"sv) || iequals(text, "like"sv) || iequals(text, "allset"sv);
}

Query SQLParser::parseSubQuery(Tokenizer& parser) {
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
	auto tok = parser.NextToken();
	subparser.selectParse<Nested::Yes>(parser);
	tok = parser.NextToken();
	if (tok.Text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found {}, {}", tok.Text(), parser.Where(tok));
	}
	return subquery;
}

template <typename T>
void SQLParser::parseWhereCondition(Tokenizer& parser, T&& firstArg, OpType op) {
	// Operator
	CondType condition;
	auto tok = peekSqlToken(parser, ConditionSqlToken);
	if (tok.Text() == "<>"sv) {
		condition = CondEq;
		if (op == OpAnd) {
			op = OpNot;
		} else if (op == OpNot) {
			op = OpAnd;
		} else {
			throw Error(errParseSQL, "<> condition with OR is not supported, {}", parser.Where(tok));
		}
	} else {
		condition = getCondType(tok.Text());
	}
	parser.SkipToken();

	// Value
	if (ctx_.autocompleteMode) {
		std::ignore = peekSqlToken(parser, WhereFieldValueSqlToken, false);
	}
	tok = parser.NextToken();
	if (iequals(tok.Text(), "null"sv) || iequals(tok.Text(), "empty"sv)) {
		query_.NextOp(op).Where(std::forward<T>(firstArg), CondEmpty, VariantArray{});
	} else if (iequals(tok.Text(), "not"sv)) {
		tok = peekSqlToken(parser, WhereFieldNegateValueSqlToken, false);
		if (!iequals(tok.Text(), "null"sv) && !iequals(tok.Text(), "empty"sv)) {
			throw Error(errParseSQL, "Expected NULL, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
		}
		query_.NextOp(op).Where(std::forward<T>(firstArg), CondAny, VariantArray{});
		tok = parser.NextToken(Tokenizer::Flags::NoFlags);
	} else if (tok.Text() == "("sv) {
		if constexpr (!std::is_same_v<T, Query>) {
			if (iequals(peekSqlToken(parser, WhereFieldValueOrSubquerySqlToken, false).Text(), "select"sv) &&
				!isCondition(parser.PeekSecondToken().Text())) {
				query_.NextOp(op).Where(std::forward<T>(firstArg), condition, parseSubQuery(parser));
				return;
			}
		}
		VariantArray values;
		for (;;) {
			tok = parser.NextToken();
			if (tok.Text() == ")"sv && tok.Type() == TokenSymbol) {
				break;
			}
			values.push_back(Token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_True));
			tok = parser.NextToken();
			if (tok.Text() == ")"sv) {
				break;
			}
			if (tok.Text() != ","sv) {
				throw Error(errParseSQL, "Expected ')' or ',', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
			}
		}
		query_.NextOp(op).Where(std::forward<T>(firstArg), condition, std::move(values));
	} else if (tok.Type() != TokenName || iequals(tok.Text(), "true"sv) || iequals(tok.Text(), "false"sv)) {
		query_.NextOp(op).Where(std::forward<T>(firstArg), condition,
								{Token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_True)});
	} else {
		if constexpr (std::is_same_v<T, Query>) {
			throw Error(errParseSQL, "Field cannot be after subquery. (text = '{}'  location = {})", tok.Text(), parser.Where(tok));
		} else {
			// Second field
			query_.NextOp(op).WhereBetweenFields(std::forward<T>(firstArg), condition, std::string{tok.Text()});
		}
	}
}

template <SQLParser::Nested nested>
void SQLParser::parseWhere(Tokenizer& parser) {
	auto tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldSqlToken, false);

	OpType nextOp = OpAnd;
	if (iequals(tok.Text(), "not"sv)) {
		nextOp = OpNot;
		parser.SkipToken();
	}
	int openBracketsCount = 0;
	bool expectSecondLogicalOperand = false;
	auto throwIfExpectSecondLogicalOperand = [&tok, &parser, &expectSecondLogicalOperand]() {
		if (expectSecondLogicalOperand) {
			throw Error(errParseSQL, "Expected second logical operand, but found '{}' in query '{}'", tok.Text(), parser.Where(tok));
		}
	};
	while (!parser.End()) {
		tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldSqlToken, false);
		parser.SkipToken(Tokenizer::Flags::NoFlags);
		while (tok.Type() == TokenName && iequals(tok.Text(), "equal_position"sv)) {
			parseEqualPositions(parser);
			tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldSqlToken, false);
			parser.SkipToken(Tokenizer::Flags::NoFlags);
		}
		expectSecondLogicalOperand = false;
		if (tok.Text() == "("sv) {
			tok = peekSqlToken(parser, nested == Nested::Yes ? NestedWhereFieldSqlToken : WhereFieldOrSubquerySqlToken, false);
			// isCondition(parser.peek_second_token().text() to distinguish the token type operator 'select' or field with name 'select'
			if (nested == Nested::No && iequals(tok.Text(), "select"sv) && !isCondition(parser.PeekSecondToken().Text())) {
				parseWhereCondition(parser, parseSubQuery(parser), nextOp);
				nextOp = OpAnd;
			} else {
				if (nextOp != OpAnd && iequals(tok.Text(), "left"sv)) {
					throw Error(errParseSQL, "Left join with {} operation", OpTypeToStr(nextOp));
				}
				query_.NextOp(nextOp);
				query_.OpenBracket();
				++openBracketsCount;
				if (iequals(tok.Text(), "not"sv)) {
					nextOp = OpNot;
					parser.SkipToken();
				} else {
					nextOp = OpAnd;
				}
				continue;
			}
		} else if (tok.Type() == TokenName) {
			const auto nextToken = parser.PeekToken();
			if (iequals(tok.Text(), "st_dwithin"sv) && nextToken.Text() == "("sv) {
				parseDWithin(parser, nextOp);
				nextOp = OpAnd;
			} else if (iequals(tok.Text(), "knn"sv) && nextToken.Text() == "("sv) {
				parseKnn(parser, nextOp);
				nextOp = OpAnd;
			} else if constexpr (nested == Nested::No) {
				if (iequals(tok.Text(), "join"sv)) {
					parseJoin(JoinType::LeftJoin, parser);
				} else if (iequals(tok.Text(), "left"sv)) {
					if (nextOp != OpAnd) {
						throw Error(errParseSQL, "Left join with {} operation", OpTypeToStr(nextOp));
					}
					std::ignore = peekSqlToken(parser, LeftSqlToken);
					tok = parser.NextToken();
					if (tok.Text() != "join"sv) {
						throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
					}
					parseJoin(JoinType::LeftJoin, parser);
				} else if (iequals(tok.Text(), "inner"sv)) {
					std::ignore = peekSqlToken(parser, InnerSqlToken);
					tok = parser.NextToken();
					if (tok.Text() != "join"sv) {
						throw Error(errParseSQL, "Expected JOIN, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
					}
					auto jtype = nextOp == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
					query_.NextOp(nextOp);
					parseJoin(jtype, parser);
				} else {
					parseWhereCondition(parser, std::string{tok.Text()}, nextOp);
					nextOp = OpAnd;
				}
			} else {
				parseWhereCondition(parser, std::string{tok.Text()}, nextOp);
				nextOp = OpAnd;
			}
		} else if (tok.Type() == TokenNumber || tok.Type() == TokenString) {
			throw Error(errParseSQL, "{} is invalid at this location. (text = '{}'  location = {})",
						tok.Type() == TokenNumber ? "Number" : "String", tok.Text(), parser.Where(tok));
		} else {
			expectSecondLogicalOperand = true;
		}

		tok = parser.PeekToken();
		while (tok.Text() == "equal_position"sv) {
			parser.SkipToken();
			parseEqualPositions(parser);
			tok = parser.PeekToken();
		}

		while (openBracketsCount > 0 && tok.Text() == ")"sv) {
			throwIfExpectSecondLogicalOperand();
			query_.CloseBracket();
			--openBracketsCount;
			parser.SkipToken();
			tok = parser.PeekToken();
			while (tok.Text() == "equal_position"sv) {
				parser.SkipToken();
				parseEqualPositions(parser);
				tok = parser.PeekToken();
			}
		}

		tok = peekSqlToken(parser, WhereOpSqlToken, false);
		if (iequals(tok.Text(), "and"sv)) {
			throwIfExpectSecondLogicalOperand();
			nextOp = OpAnd;
			expectSecondLogicalOperand = true;
			parser.SkipToken();
			tok = peekSqlToken(parser, nested == Nested::Yes ? NestedAndSqlToken : AndSqlToken, false);
			if (iequals(tok.Text(), "not"sv)) {
				parser.SkipToken();
				nextOp = OpNot;
			} else {
				continue;
			}
		} else if (iequals(tok.Text(), "or"sv)) {
			throwIfExpectSecondLogicalOperand();
			parser.SkipToken();
			std::ignore = peekSqlToken(parser, FieldNameSqlToken);
			nextOp = OpOr;
			expectSecondLogicalOperand = true;
		} else if (!iequals(tok.Text(), "join"sv) && !iequals(tok.Text(), "inner"sv) && !iequals(tok.Text(), "left"sv)) {
			break;
		}
	}
	throwIfExpectSecondLogicalOperand();

	if (query_.Entries().Empty()) {
		throw Error(errParseSQL, "Expected condition after 'WHERE'");
	}
}

void SQLParser::parseEqualPositions(Tokenizer& parser) {
	auto tok = parser.NextToken();
	if (tok.Text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	EqualPosition_t fieldNames;
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken, false);
		tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		if (tok.Type() != TokenName) {
			throw Error(errParseSQL, "Expected name, but found '{}' in query, {}", tok.Text(), parser.Where(tok));
		}
		fieldNames.emplace_back(nameWithCase.Text());
		tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		if (tok.Text() == ")"sv) {
			break;
		}
		if (tok.Text() != ","sv) {
			throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.Text(), parser.Where(tok));
		}
	}
	query_.EqualPositions(std::move(fieldNames));
}

Point SQLParser::parseGeomFromText(Tokenizer& parser) const {
	auto tok = parser.NextToken();
	if (tok.Text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	tok = parser.NextToken();
	if (tok.Type() != TokenString) {
		throw Error(errParseSQL, "Expected text, but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	const std::string_view tokenText = tok.Text();
	std::string_view str = skipSpace(tokenText);
	if (!checkIfStartsWith("point"sv, str)) {
		throw Error(errParseSQL, "Expected geometry object, but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	str = skipSpace(str.substr(5));
	if (str.empty() || str[0] != '(') {
		throw Error(errParseSQL, "Expected '(' after '{}', but found '{}' in '{}', {}", tokenText.substr(0, tokenText.size() - str.size()),
					str, tok.Text(), parser.Where(tok));
	}
	str = skipSpace(str.substr(1));
	size_t pos = 0;
	double x = 0.0, y = 0.0;
	try {
		x = std::stod(std::string(str), &pos);
	} catch (...) {
		throw Error(errParseSQL, "Expected first number argument after '{}', but found '{}' in '{}', {}",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.Text(), parser.Where(tok));
	}
	if (pos >= str.size()) {
		throw Error(errParseSQL, "Expected space after '{}', but found nothing in '{}', {}", tokenText, tok.Text(), parser.Where(tok));
	}
	str = skipSpace(str.substr(pos));
	try {
		y = std::stod(std::string(str), &pos);
	} catch (...) {
		throw Error(errParseSQL, "Expected second number argument after '{}', but found '{}' in '{}', {}",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.Text(), parser.Where(tok));
	}
	if (pos >= str.size()) {
		throw Error(errParseSQL, "Expected ')' after '{}', but found nothing in '{}', {}", tokenText, tok.Text(), parser.Where(tok));
	}
	str = skipSpace(str.substr(pos));
	if (str.empty() || str[0] != ')') {
		throw Error(errParseSQL, "Expected ')' after '{}', but found '{}' in '{}', {}", tokenText.substr(0, tokenText.size() - str.size()),
					str, tok.Text(), parser.Where(tok));
	}
	str = skipSpace(str.substr(1));
	if (!str.empty()) {
		throw Error(errParseSQL, "Expected nothing after '{}', but found '{}' in '{}', {}",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.Text(), parser.Where(tok));
	}

	tok = parser.NextToken();
	if (tok.Text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	return Point{x, y};
}
template <typename T>
static auto parseNumber(const auto& begin, const auto& end, T& value) {
	if constexpr (std::is_floating_point_v<T>) {
		using namespace double_conversion;
		static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
		int countOfCharsParsedAsDoubleDummy = 0;
		value = converter.StringToDouble(begin, end - begin, &countOfCharsParsedAsDoubleDummy);
	} else if constexpr (std::is_integral_v<T>) {
		return std::from_chars(begin, end, value).ec;
	}
	return std::errc{};
}

template <typename T>
void SQLParser::parseSingleKnnParam(Tokenizer& parser, std::optional<T>& param, std::string_view paramName) {
	if (param.has_value()) {
		throw Error(errParseSQL, "Duplicate KNN parameter '{}': {}", paramName, parser.Where());
	}
	auto tok = parser.NextToken();
	if (tok.Text() != "="sv) {
		throw Error(errParseSQL, "Expected '=', but found {}, {}", tok.Text(), parser.Where(tok));
	}
	tok = parser.NextToken();
	if (tok.Type() != TokenNumber) {
		throw Error(errParseSQL, "Expected number greater than 0, but found {}, {}", tok.Text(), parser.Where(tok));
	}
	T paramValue;
	const auto res = parseNumber(tok.Text().data(), tok.Text().data() + tok.Text().size(), paramValue);
	if (res != std::errc{}) {
		throw Error(errParseSQL, "Expected float or int greater than 0, but found {}, {}", tok.Text(), parser.Where(tok));
	}

	param = paramValue;
}

KnnSearchParams SQLParser::parseKnnParams(Tokenizer& parser) {
	std::optional<size_t> k, ef, nprobe;
	std::optional<float> radius;
	Token tok;
	do {
		std::ignore = peekSqlToken(parser, KnnParamsToken);
		tok = parser.NextToken();
		if (tok.Text() == KnnSearchParams::kKName) {
			parseSingleKnnParam(parser, k, KnnSearchParams::kKName);
		} else if (tok.Text() == KnnSearchParams::kRadiusName) {
			parseSingleKnnParam(parser, radius, KnnSearchParams::kRadiusName);
		} else if (tok.Text() == KnnSearchParams::kEfName) {
			if (nprobe) {
				throw Error{errParseSQL, "Wrong SQL format: KNN query cannot contain both of '{}' and '{}'", KnnSearchParams::kEfName,
							KnnSearchParams::kNProbeName};
			}
			parseSingleKnnParam(parser, ef, KnnSearchParams::kEfName);
		} else if (tok.Text() == KnnSearchParams::kNProbeName) {
			if (ef) {
				throw Error{errParseSQL, "Wrong SQL format: KNN query cannot contain both of '{}' and '{}'", KnnSearchParams::kEfName,
							KnnSearchParams::kNProbeName};
			}
			parseSingleKnnParam(parser, nprobe, KnnSearchParams::kNProbeName);
		} else {
			throw Error(errParseSQL, "Expected KNN parameter, but found '{}', {}", tok.Text(), parser.Where(tok));
		}
		tok = parser.NextToken();
		if (tok.Text() == ")"sv) {
			break;
		} else if (tok.Text() != ","sv) {
			throw Error(errParseSQL, "Expected ',' or ')', but found '{}', {}", tok.Text(), parser.Where(tok));
		}
	} while (true);
	if (!k && !radius) {
		throw Error(errParseSQL, "The presence of one of the '{}' or '{}' parameters in KNN query is mandatory; {}",
					KnnSearchParams::kKName, KnnSearchParams::kRadiusName, parser.Where(tok));
	}
	if (ef.has_value()) {
		return HnswSearchParams().K(k).Radius(radius).Ef(*ef);
	} else if (nprobe.has_value()) {
		return IvfSearchParams().K(k).Radius(radius).NProbe(*nprobe);
	} else {
		return KnnSearchParamsBase().K(k).Radius(radius);
	}
}

void SQLParser::parseKnn(Tokenizer& parser, OpType nextOp) {
	auto tok = parser.NextToken();
	if (tok.Text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	std::ignore = peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.NextToken();
	if (tok.Type() != TokenName) {
		throw Error(errParseSQL, "Expected field name, but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	std::string field(tok.Text());
	tok = parser.NextToken();
	if (tok.Text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	tok = parser.NextToken();
	if (tok.Text() != "["sv) {
		if (tok.Type() == TokenString) {
			std::string value(tok.Text());
			tok = parser.NextToken();
			if (tok.Text() != ","sv) {
				throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.Text(), parser.Where(tok));
			}
			query_.NextOp(nextOp).WhereKNN(std::move(field), std::move(value), parseKnnParams(parser));
			return;	 // NOTE: stop processing
		}

		throw Error(errParseSQL, "Expected '[' or ''', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	std::vector<float> vec;
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_LEADING_SPACES |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   NAN, NAN, nullptr, nullptr};
	do {
		tok = parser.NextToken();
		if (tok.Type() != TokenNumber) {
			throw Error(errParseSQL, "Expected number, but found '{}', {}", tok.Text(), parser.Where(tok));
		}
		int countOfCharsParsedAsDoubleDummy = 0;
		vec.push_back(0);
		try {
			vec.back() = converter.StringToDouble(tok.Text().data(), tok.Text().size(), &countOfCharsParsedAsDoubleDummy);
		} catch (...) {
			throw Error(errParseSQL, "Expected number, but found '{}', {}", tok.Text(), parser.Where(tok));
		}
		tok = parser.NextToken();
		if (tok.Text() == "]"sv) {
			break;
		} else if (tok.Text() != ","sv) {
			throw Error(errParseSQL, "Expected ',' or ']', but found '{}', {}", tok.Text(), parser.Where(tok));
		}
	} while (true);
	const ConstFloatVectorView vecView{std::span<float>(vec)};

	tok = parser.NextToken();
	if (tok.Text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.Text(), parser.Where(tok));
	}

	query_.NextOp(nextOp).WhereKNN(std::move(field), vecView, parseKnnParams(parser));
}

void SQLParser::parseDWithin(Tokenizer& parser, OpType nextOp) {
	auto tok = parser.NextToken();
	if (tok.Text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found '{}', {}", tok.Text(), parser.Where(tok));
	}

	std::ignore = peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.NextToken();

	Point point;
	std::string field;
	if (iequals(tok.Text(), "st_geomfromtext"sv)) {
		point = parseGeomFromText(parser);
	} else {
		field = std::string(tok.Text());
	}

	tok = parser.NextToken();
	if (tok.Text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.Text(), parser.Where(tok));
	}

	std::ignore = peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.NextToken();
	if (iequals(tok.Text(), "st_geomfromtext"sv)) {
		if (field.empty()) {
			throw Error(errParseSQL, "Expected field name, but found '{}', {}", tok.Text(), parser.Where(tok));
		}
		point = parseGeomFromText(parser);
	} else {
		if (!field.empty()) {
			throw Error(errParseSQL, "Expected geometry object, but found '{}', {}", tok.Text(), parser.Where(tok));
		}
		field = std::string(tok.Text());
	}

	tok = parser.NextToken();
	if (tok.Text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found '{}', {}", tok.Text(), parser.Where(tok));
	}

	tok = parser.NextToken();
	const auto distance = Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_False);
	distance.Type().EvaluateOneOf(
		[](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) noexcept {},
		[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Composite,
							KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
			throw Error(errParseSQL, "Expected number, but found '{}', {}", tok.Text(), parser.Where(tok));
		});

	tok = parser.NextToken();
	if (tok.Text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	query_.NextOp(nextOp).DWithin(field, point, distance.As<double>());
}

void SQLParser::parseJoin(JoinType type, Tokenizer& parser) {
	JoinedQuery jquery;
	SQLParser jparser(jquery);
	const ParserContextsAppendGuard guard{ctx_, jparser.ctx_};
	if (ctx_.autocompleteMode) {
		jparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		jparser.ctx_.autocompleteMode = true;
		jparser.ctx_.foundPossibleSuggestions = ctx_.foundPossibleSuggestions;
		jparser.ctx_.possibleSuggestionDetectedInThisClause = ctx_.possibleSuggestionDetectedInThisClause;
	}
	auto nameWithCase = peekSqlToken(parser, NamespaceSqlToken, false);
	auto tok = parser.NextToken();
	if (tok.Text() == "("sv) {
		std::ignore = peekSqlToken(parser, SelectSqlToken);
		tok = parser.NextToken();
		if (tok.Text() != "select"sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found '{}', {}", tok.Text(), parser.Where(tok));
		}

		jparser.selectParse<Nested::Yes>(parser);

		tok = parser.NextToken();
		if (tok.Text() != ")"sv) {
			throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.Text(), parser.Where(tok));
		}
	} else {
		jquery.SetNsName(nameWithCase.Text());
		ctx_.updateLinkedNs(jquery.NsName());
	}
	jquery.joinType = type;
	jparser.parseJoinEntries(parser, query_.NsName(), jquery);

	query_.Join(std::move(jquery));
}

void SQLParser::parseMerge(Tokenizer& parser) {
	JoinedQuery mquery;
	SQLParser mparser(mquery);
	const ParserContextsAppendGuard guard{ctx_, mparser.ctx_};
	if (ctx_.autocompleteMode) {
		mparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		mparser.ctx_.autocompleteMode = true;
		mparser.ctx_.foundPossibleSuggestions = ctx_.foundPossibleSuggestions;
		mparser.ctx_.possibleSuggestionDetectedInThisClause = ctx_.possibleSuggestionDetectedInThisClause;
	}
	auto tok = parser.NextToken();
	if (tok.Text() == "("sv) {
		std::ignore = peekSqlToken(parser, SelectSqlToken);
		tok = parser.NextToken();
		if (tok.Text() != "select"sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found '{}', {}", tok.Text(), parser.Where(tok));
		}

		mparser.selectParse<Nested::No>(parser);

		tok = parser.NextToken();
		if (tok.Text() != ")"sv) {
			throw Error(errParseSQL, "Expected ')', but found '{}', {}", tok.Text(), parser.Where(tok));
		}
	}
	mquery.joinType = JoinType::Merge;

	query_.Merge(std::move(mquery));
}

std::string SQLParser::parseJoinedFieldName(Tokenizer& parser, std::string& name) {
	auto tok = peekSqlToken(parser, JoinedFieldNameSqlToken, false);
	if (tok.Type() != TokenName) {
		throw Error(errParseSQL, "Expected name, but found '{}', {}", tok.Text(), parser.Where(tok));
	}

	auto dotPos = tok.Text().find('.');
	if (dotPos == std::string_view::npos) {
		return std::string(tok.Text());
	}
	name = std::string(tok.Text().substr(0, dotPos));

	tok = peekSqlToken(parser, FieldNameSqlToken, false);
	if (tok.Type() != TokenName) {
		throw Error(errParseSQL, "Expected name, but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	parser.SkipToken();
	ctx_.updateLinkedNs(name);
	return std::string(tok.Text().substr(dotPos + 1));
}

void SQLParser::parseJoinEntries(Tokenizer& parser, const std::string& mainNs, JoinedQuery& jquery) {
	auto tok = peekSqlToken(parser, OnSqlToken);
	if (tok.Text() != "on"sv) {
		throw Error(errParseSQL, "Expected 'ON', but found '{}', {}", tok.Text(), parser.Where(tok));
	}
	parser.SkipToken();

	tok = parser.PeekToken();

	const bool brackets = tok.Text() == "("sv;
	if (brackets) {
		parser.SkipToken();
	}

	while (!parser.End()) {
		tok = peekSqlToken(parser, OpSqlToken);
		if (tok.Text() == "or"sv) {
			jquery.Or();
			parser.SkipToken();
			tok = parser.PeekToken();
		} else if (tok.Text() == "and"sv) {
			jquery.And();
			parser.SkipToken();
			tok = parser.PeekToken();
		}

		if (tok.Text() == "not"sv) {
			jquery.Not();
			parser.SkipToken();
			tok = parser.PeekToken();
		}

		if (brackets && tok.Text() == ")"sv) {
			parser.SkipToken();
			return;
		}

		std::string ns1 = mainNs, ns2 = jquery.NsName();
		std::string fld1 = parseJoinedFieldName(parser, ns1);
		CondType condition = getCondType(parser.NextToken().Text());
		std::string fld2 = parseJoinedFieldName(parser, ns2);
		bool reverseNamespacesOrder{false};

		if (ns1 != mainNs || ns2 != jquery.NsName()) {
			if (ns2 == mainNs && ns1 == jquery.NsName()) {
				std::swap(fld1, fld2);
				condition = InvertJoinCondition(condition);
				reverseNamespacesOrder = true;
			} else {
				throw Error(errParseSQL, "Unexpected tables with ON statement: ('{}' and '{}') but expected ('{}' and '{}'), {}", ns1, ns2,
							mainNs, jquery.NsName(), parser.Where());
			}
		}

		jquery.joinEntries_.emplace_back(jquery.NextOp(), condition, std::move(fld1), std::move(fld2), reverseNamespacesOrder);
		jquery.And();
		if (!brackets) {
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
