
#include "sqlparser.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/key_string.h"
#include "core/query/query.h"
#include "core/queryresults/aggregationresult.h"
#include "core/type_consts_helpers.h"
#include "sqltokentype.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/gason/gason.h"

namespace reindexer {

using namespace std::string_view_literals;

int SQLParser::Parse(std::string_view q) {
	tokenizer parser(q);
	return Parse(parser);
}

bool SQLParser::reachedAutocompleteToken(tokenizer &parser, const token &tok) {
	size_t pos = parser.getPos() + tok.text().length();
	return (pos > ctx_.suggestionsPos);
}

token SQLParser::peekSqlToken(tokenizer &parser, int tokenType, bool toLower) {
	token tok = parser.peek_token(toLower ? tokenizer::flags::to_lower : tokenizer::flags::no_flags);
	bool eof = ((parser.getPos() + tok.text().length()) == parser.length());
	if (ctx_.autocompleteMode && !tok.text().empty() && reachedAutocompleteToken(parser, tok)) {
		size_t tokenLen = 0;
		if (ctx_.suggestionsPos >= parser.getPos()) {
			tokenLen = ctx_.suggestionsPos - parser.getPos() + 1;
		}
		if (!ctx_.foundPossibleSuggestions || tokenLen) {
			ctx_.suggestions.emplace_back(std::string(tok.text().data(), tokenLen), tokenType);
			ctx_.foundPossibleSuggestions = true;
			ctx_.possibleSuggestionDetectedInThisClause = true;
		}
	}
	if (!ctx_.foundPossibleSuggestions) ctx_.tokens.push_back(tokenType);
	if (eof && ctx_.autocompleteMode) throw Error(errLogic, "SQLParser eof is reached!");
	return tok;
}

int SQLParser::Parse(tokenizer &parser) {
	parser.skip_space();
	token tok = peekSqlToken(parser, Start);
	if (tok.text() == "explain"sv) {
		query_.explain_ = true;
		parser.next_token();
		tok = peekSqlToken(parser, StartAfterExplain);
	}

	if (tok.text() == "select"sv) {
		query_.type_ = QuerySelect;
		parser.next_token();
		selectParse(parser);
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

int SQLParser::selectParse(tokenizer &parser) {
	// Get filter
	token tok;
	bool wasSelectFilter = false;
	while (!parser.end()) {
		auto nameWithCase = peekSqlToken(parser, SingleSelectFieldSqlToken, false);
		auto name = parser.next_token();
		tok = peekSqlToken(parser, SelectFieldsListSqlToken);
		if (tok.text() == "("sv) {
			parser.next_token();
			tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
			if (name.text() == "count"sv) {
				query_.calcTotal = ModeAccurateTotal;
				if (!wasSelectFilter) {
					query_.count = 0;
				}
				tok = parser.next_token();
				if (tok.text() != "*") throw Error(errParseSQL, "Expected '*', but found '%s' in query, %s", tok.text(), parser.where());
			} else if (name.text() == "count_cached"sv) {
				query_.calcTotal = ModeCachedTotal;
				if (!wasSelectFilter) {
					query_.count = 0;
				}
				tok = parser.next_token();
				if (tok.text() != "*"sv) throw Error(errParseSQL, "Expected '*', but found '%s' in query, %s", tok.text(), parser.where());
			} else if (name.text() == "rank"sv) {
				query_.WithRank();
			} else {
				AggType agg = AggregationResult::strToAggType(name.text());
				if (agg != AggUnknown) {
					if (!query_.CanAddAggregation(agg) || (wasSelectFilter && agg != AggDistinct)) {
						throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
					}
					h_vector<std::string, 1> fields{{std::string(tok.text())}};
					tok = parser.next_token();
					for (tok = parser.peek_token(); tok.text() == ","sv; tok = parser.peek_token()) {
						parser.next_token();
						tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
						fields.emplace_back(tok.text());
						tok = parser.next_token();
					}
					AggregateEntry entry{agg, std::move(fields)};
					for (tok = parser.peek_token(); tok.text() != ")"sv; tok = parser.peek_token()) {
						if (tok.text() == "order"sv) {
							parser.next_token();
							std::vector<Variant> orders;
							SortingEntries sortingEntries;
							parseOrderBy(parser, sortingEntries, orders);
							if (!orders.empty()) {
								throw Error(errParseSQL, "Forced sort order is not available in aggregation sort");
							}
							for (auto s : sortingEntries) {
								entry.AddSortingEntry(std::move(s));
							}
						} else if (tok.text() == "limit"sv) {
							parser.next_token();
							tok = parser.next_token();
							if (tok.type != TokenNumber) {
								throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
							}
							entry.SetLimit(stoi(tok.text()));
						} else if (tok.text() == "offset"sv) {
							parser.next_token();
							tok = parser.next_token();
							if (tok.type != TokenNumber) {
								throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
							}
							entry.SetOffset(stoi(tok.text()));
						} else {
							break;
						}
					}
					query_.aggregations_.emplace_back(std::move(entry));
				} else {
					throw Error(errParams, "Unknown function name SQL - %s, %s", name.text(), parser.where());
				}
			}
			tok = parser.peek_token();
			if (tok.text() != ")"sv) {
				throw Error(errParams, "Expected ')', but found %s, %s", tok.text(), parser.where());
			}
			parser.next_token();
			tok = peekSqlToken(parser, SelectFieldsListSqlToken);

		} else if (name.text() != "*"sv) {
			if (!query_.CanAddSelectFilter()) {
				throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
			}
			query_.selectFilter_.emplace_back(nameWithCase.text());
			query_.count = QueryEntry::kDefaultLimit;
			wasSelectFilter = true;
		} else if (name.text() == "*"sv) {
			if (!query_.CanAddSelectFilter()) {
				throw Error(errConflict, kAggregationWithSelectFieldsMsgError);
			}
			query_.count = QueryEntry::kDefaultLimit;
			wasSelectFilter = true;
			query_.selectFilter_.clear();
		}
		if (tok.text() != ","sv) break;
		tok = parser.next_token();
	}

	peekSqlToken(parser, FromSqlToken);
	if (parser.next_token().text() != "from"sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text(), parser.where());

	peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = std::string(parser.next_token().text());
	ctx_.updateLinkedNs(query_._namespace);

	while (!parser.end()) {
		tok = peekSqlToken(parser, SelectConditionsStart);
		if (tok.text() == "where"sv) {
			parser.next_token();
			parseWhere(parser);
		} else if (tok.text() == "limit"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.count = stoi(tok.text());
		} else if (tok.text() == "offset"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.start = stoi(tok.text());
		} else if (tok.text() == "order"sv) {
			parser.next_token();
			parseOrderBy(parser, query_.sortingEntries_, query_.forcedSortOrder_);
			ctx_.updateLinkedNs(query_._namespace);
		} else if (tok.text() == "join"sv) {
			parser.next_token();
			parseJoin(JoinType::LeftJoin, parser);
		} else if (tok.text() == "left"sv) {
			parser.next_token();
			peekSqlToken(parser, LeftSqlToken);
			if (parser.next_token().text() != "join"sv) {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
			}
			parseJoin(JoinType::LeftJoin, parser);
		} else if (tok.text() == "inner"sv) {
			parser.next_token();
			peekSqlToken(parser, InnerSqlToken);
			if (parser.next_token().text() != "join") {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
			}
			auto jtype = (query_.nextOp_ == OpOr) ? JoinType::OrInnerJoin : JoinType::InnerJoin;
			query_.nextOp_ = OpAnd;
			parseJoin(jtype, parser);
		} else if (tok.text() == "merge"sv) {
			parser.next_token();
			parseMerge(parser);
		} else if (tok.text() == "or"sv) {
			parser.next_token();
			query_.nextOp_ = OpOr;
		} else {
			break;
		}
	}
	return 0;
}

template <typename T>
static void MoveAppend(T &dst, T &src) {
	if (dst.empty()) {
		dst = std::move(src);
	} else {
		dst.reserve(dst.size() + src.size());
		std::move(std::begin(src), std::end(src), std::back_inserter(dst));
		src.clear();
	}
}

int SQLParser::nestedSelectParse(SQLParser &parser, tokenizer &tok) {
	try {
		int res = parser.selectParse(tok);
		MoveAppend(ctx_.suggestions, parser.ctx_.suggestions);
		return res;
	} catch (...) {
		MoveAppend(ctx_.suggestions, parser.ctx_.suggestions);
		throw;
	}
}

static KeyValueType detectValueType(const token &currTok) {
	const std::string_view val = currTok.text();
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
			if (flt) {
				return {KeyValueType::Double{}};
			} else {
				return {KeyValueType::Int64{}};
			}
		}
	}
	return {KeyValueType::String{}};
}

Variant token2kv(const token &currTok, tokenizer &parser, bool allowComposite) {
	if (currTok.text() == "{"sv) {
		// Composite value parsing
		if (!allowComposite) {
			throw Error(errParseSQL, "Unexpected '{' in query, %s", parser.where());
		}
		VariantArray compositeValues;
		for (;;) {
			auto tok = parser.next_token();
			compositeValues.push_back(token2kv(tok, parser, false));
			tok = parser.next_token();
			if (tok.text() == "}"sv) {
				return Variant(compositeValues);
			}
			if (tok.text() != ","sv) {
				throw Error(errParseSQL, "Expected ',', but found '%s' in query, %s", tok.text(), parser.where());
			}
		}
	}

	std::string_view value = currTok.text();
	if (currTok.type == TokenName) {
		if (iequals(value, "true"sv)) return Variant{true};
		if (iequals(value, "false"sv)) return Variant{false};
	}

	if (currTok.type != TokenNumber && currTok.type != TokenString) {
		throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", currTok.text(), parser.where());
	}

	return detectValueType(currTok).EvaluateOneOf(
		[&](KeyValueType::Int64) { return Variant(int64_t(stoll(value))); },
		[&](KeyValueType::Double) {
			try {
				using double_conversion::StringToDoubleConverter;
				static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
				int countOfCharsParsedAsDouble;
				return Variant(converter.StringToDouble(value.data(), value.size(), &countOfCharsParsedAsDouble));
			} catch (...) {
				throw Error(errParseSQL, "Unable to convert '%s' to double value", value);
			}
		},
		[&](KeyValueType::String) { return Variant(make_key_string(value.data(), value.length())); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Bool, KeyValueType::Int, KeyValueType::Composite,
				 KeyValueType::Null, KeyValueType::Uuid>) noexcept -> Variant {
			assertrx(0);
			abort();
		});
}

int SQLParser::parseOrderBy(tokenizer &parser, SortingEntries &sortingEntries, std::vector<Variant> &forcedSortOrder_) {
	// Just skip token (BY)
	peekSqlToken(parser, BySqlToken);
	parser.next_token();
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken);
		auto tok = parser.next_token(tokenizer::flags::in_order_by);
		if (tok.type != TokenName && tok.type != TokenString) {
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		}
		SortingEntry sortingEntry;
		sortingEntry.expression = std::string(tok.text());
		if (sortingEntry.expression.empty()) {
			throw Error(errParseSQL, "Order by expression should not be empty, %s", parser.where());
		}
		tok = peekSqlToken(parser, SortDirectionSqlToken);
		if (tok.text() == "("sv && nameWithCase.text() == "field"sv) {
			parser.next_token();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.type != TokenName) {
				throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
			}
			sortingEntry.expression = std::string(tok.text());
			tok = parser.next_token(tokenizer::flags::no_flags);
			for (;;) {
				tok = parser.next_token();
				if (tok.text() == ")"sv) break;
				if (tok.text() != ","sv) {
					throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
				}
				tok = parser.next_token();
				if (!sortingEntries.empty()) {
					throw Error(errParseSQL, "Forced sort order is allowed for the first sorting entry only, '%s', %s", tok.text(),
								parser.where());
				}
				forcedSortOrder_.push_back(token2kv(tok, parser, true));
			}
			tok = parser.peek_token();
		}

		if (tok.text() == "asc"sv || tok.text() == "desc"sv) {
			sortingEntry.desc = bool(tok.text() == "desc"sv);
			parser.next_token();
		}
		sortingEntries.push_back(std::move(sortingEntry));

		auto nextToken = parser.peek_token();
		if (nextToken.text() != ","sv) break;
		parser.next_token();
	}
	return 0;
}

int SQLParser::deleteParse(tokenizer &parser) {
	// Get filter
	token tok;

	peekSqlToken(parser, FromSqlToken);
	if (parser.next_token().text() != "from"sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text(), parser.where());

	peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = std::string(parser.next_token().text());
	ctx_.updateLinkedNs(query_._namespace);

	while (!parser.end()) {
		tok = peekSqlToken(parser, DeleteConditionsStart);
		if (tok.text() == "where"sv) {
			parser.next_token();
			parseWhere(parser);
		} else if (tok.text() == "limit"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.count = stoi(tok.text());
		} else if (tok.text() == "offset"sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.start = stoi(tok.text());
		} else if (tok.text() == "order"sv) {
			parser.next_token();
			parseOrderBy(parser, query_.sortingEntries_, query_.forcedSortOrder_);
			ctx_.updateLinkedNs(query_._namespace);
		} else
			break;
	}
	return 0;
}

static void addUpdateValue(const token &currTok, tokenizer &parser, UpdateEntry &updateField) {
	updateField.SetMode(FieldModeSet);
	if (currTok.type == TokenString) {
		updateField.Values().push_back(token2kv(currTok, parser, false));
	} else {
		if (currTok.text() == "null"sv) {
			updateField.Values().push_back(Variant());
		} else if (currTok.text() == "{"sv) {
			try {
				size_t jsonPos = parser.getPos() - 1;
				std::string json(parser.begin() + jsonPos, parser.length() - jsonPos);
				size_t jsonLength = 0;
				gason::JsonParser jsonParser;
				jsonParser.Parse(giftStr(json), &jsonLength);
				updateField.Values().emplace_back(Variant(std::string(parser.begin() + jsonPos, jsonLength)));
				updateField.SetMode(FieldModeSetJson);
				parser.setPos(jsonPos + jsonLength);
			} catch (const gason::Exception &e) {
				throw Error(errParseSQL, "%s, in query %s", e.what(), parser.where());
			}
		} else {
			auto eof = [](tokenizer &parser, bool &inArray) -> bool {
				if (parser.end()) return true;
				token nextTok = parser.peek_token();
				bool result =
					(nextTok.text() == "where"sv) || (!inArray && nextTok.text() == "]"sv) || (!inArray && nextTok.text() == ","sv);
				if (nextTok.text() == "["sv && !inArray) inArray = true;
				if (nextTok.text() == "]"sv && inArray) inArray = false;
				return result;
			};
			int count = 0;
			std::string expression(currTok.text());
			bool inArray = false;
			while (!eof(parser, inArray)) {
				auto tok = parser.next_token(tokenizer::flags::treat_sign_as_token);
				expression += tok.type == TokenString ? '\'' + std::string{tok.text()} + '\'' : std::string{tok.text()};
				++count;
			}
			if (count > 0) {
				updateField.Values().push_back(Variant(expression));
				updateField.SetIsExpression(true);
			} else {
				try {
					Variant val = token2kv(currTok, parser, false);
					updateField.Values().push_back(val);
				} catch (const Error &) {
					updateField.Values().push_back(Variant(expression));
					updateField.SetIsExpression(true);
				}
			}
		}
	}
}

UpdateEntry SQLParser::parseUpdateField(tokenizer &parser) {
	token tok = peekSqlToken(parser, FieldNameSqlToken, false);
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected field name but found '%s' in query %s", tok.text(), parser.where());
	}
	UpdateEntry updateField{{tok.text().data(), tok.text().length()}, {}};

	parser.next_token();
	tok = parser.next_token();
	if (tok.text() != "="sv) throw Error(errParams, "Expected '=' but found '%s' in query, '%s'", tok.text(), parser.where());

	size_t startPos = parser.getPos();
	bool withArrayExpressions = false;

	tok = parser.next_token(tokenizer::flags::no_flags);
	if (tok.text() == "["sv) {
		updateField.Values().MarkArray();
		for (;;) {
			tok = parser.next_token(tokenizer::flags::no_flags);
			if (tok.text() == "]") {
				if (updateField.Values().empty()) break;
				throw Error(errParseSQL, "Expected field value, but found ']' in query, %s", parser.where());
			}
			addUpdateValue(tok, parser, updateField);
			tok = parser.next_token(tokenizer::flags::no_flags);
			if (tok.text() == "]"sv) break;
			if (tok.text() != ","sv) {
				throw Error(errParseSQL, "Expected ']' or ',', but found '%s' in query, %s", tok.text(), parser.where());
			}
		}
	} else {
		addUpdateValue(tok, parser, updateField);
	}

	tok = parser.peek_token(false);
	while (tok.text() == "|"sv) {
		parser.next_token();
		tok = parser.next_token();
		if (tok.text() != "|"sv) throw Error(errLogic, "Expected '|', not %s", tok.text());
		tok = parser.next_token();
		if (tok.type != TokenName) {
			throw Error(errParseSQL, "Expected field name, but found %s in query, %s", tok.text(), parser.where());
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

int SQLParser::updateParse(tokenizer &parser) {
	parser.next_token();

	token tok = peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = std::string(tok.text());
	ctx_.updateLinkedNs(query_._namespace);
	parser.next_token();

	tok = peekSqlToken(parser, UpdateOptionsSqlToken);
	if (tok.text() == "set"sv) {
		parser.next_token();
		while (!parser.end()) {
			query_.updateFields_.emplace_back(parseUpdateField(parser));
			tok = parser.peek_token();
			if (tok.text() != ","sv) break;
			parser.next_token();
		}
	} else if (tok.text() == "drop"sv) {
		while (!parser.end()) {
			parser.next_token();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.type != TokenName) {
				throw Error(errParseSQL, "Expected field name but found '%s' in query %s", tok.text(), parser.where());
			}
			query_.Drop(std::string(tok.text()));
			parser.next_token();
			tok = parser.peek_token();
			if (tok.text() != ","sv) break;
		}
	} else {
		throw Error(errParseSQL, "Expected 'SET' or 'DROP' but found '%s' in query %s", tok.text(), parser.where());
	}

	tok = peekSqlToken(parser, WhereSqlToken);
	if (tok.text() == "where"sv) {
		parser.next_token();
		parseWhere(parser);
	}

	return 0;
}

int SQLParser::truncateParse(tokenizer &parser) {
	parser.next_token();
	token tok = peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = std::string(tok.text());
	ctx_.updateLinkedNs(query_._namespace);
	parser.next_token();
	return 0;
}

int SQLParser::parseWhere(tokenizer &parser) {
	token tok;
	OpType nextOp = OpAnd;

	tok = peekSqlToken(parser, WhereFieldSqlToken, false);

	if (iequals(tok.text(), "not"sv)) {
		nextOp = OpNot;
		parser.next_token();
	}
	std::vector<std::pair<size_t, EqualPosition_t>> equalPositions;
	size_t lastBracketPosition = 0;
	int openBracketsCount = 0;
	while (!parser.end()) {
		tok = peekSqlToken(parser, WhereFieldSqlToken, false);
		parser.next_token(false);
		if (tok.text() == "("sv) {
			query_.entries.OpenBracket(nextOp);
			++openBracketsCount;
			lastBracketPosition = query_.entries.Size();
			tok = peekSqlToken(parser, WhereFieldSqlToken, false);
			if (iequals(tok.text(), "not"sv)) {
				nextOp = OpNot;
				parser.next_token();
			} else {
				nextOp = OpAnd;
			}
			continue;
		}
		if (tok.type == TokenNumber) {
			throw Error(errParseSQL, "Number is invalid at this location. (text = '%s'  location = %s)", tok.text(), parser.where());
		}
		if (tok.type == TokenString) {
			throw Error(errParseSQL, "String is invalid at this location. (text = '%s'  location = %s)", tok.text(), parser.where());
		}

		if (tok.type == TokenName) {
			if (iequals(tok.text(), "join"sv)) {
				parseJoin(JoinType::LeftJoin, parser);
			} else if (iequals(tok.text(), "left"sv)) {
				peekSqlToken(parser, LeftSqlToken);
				if (parser.next_token().text() != "join"sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
				}
				parseJoin(JoinType::LeftJoin, parser);
			} else if (iequals(tok.text(), "inner"sv)) {
				peekSqlToken(parser, InnerSqlToken);
				if (parser.next_token().text() != "join") {
					throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
				}
				auto jtype = nextOp == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
				query_.nextOp_ = OpAnd;
				parseJoin(jtype, parser);
			} else if (iequals(tok.text(), "st_dwithin"sv)) {
				parseDWithin(parser, nextOp);
				nextOp = OpAnd;
			} else {
				// Index name
				const std::string index{tok.text()};

				// Operator
				CondType condition;
				tok = peekSqlToken(parser, ConditionSqlToken);
				if (tok.text() == "<>"sv) {
					condition = CondEq;
					if (nextOp == OpAnd)
						nextOp = OpNot;
					else if (nextOp == OpNot)
						nextOp = OpAnd;
					else {
						throw Error(errParseSQL, "<> condition with OR is not supported, %s", parser.where());
					}
				} else {
					condition = getCondType(tok.text());
				}
				parser.next_token();

				// Value
				if (ctx_.autocompleteMode) peekSqlToken(parser, WhereFieldValueSqlToken, false);
				tok = parser.next_token();
				if (iequals(tok.text(), "null"sv) || iequals(tok.text(), "empty"sv)) {
					query_.entries.Append(nextOp, QueryEntry{index, CondEmpty, {}});
				} else if (iequals(tok.text(), "not"sv)) {
					tok = peekSqlToken(parser, WhereFieldNegateValueSqlToken, false);
					if (!iequals(tok.text(), "null"sv) && !iequals(tok.text(), "empty"sv)) {
						throw Error(errParseSQL, "Expected NULL, but found '%s' in query, %s", tok.text(), parser.where());
					}
					query_.entries.Append(nextOp, QueryEntry{index, CondAny, {}});
					tok = parser.next_token(false);
				} else if (tok.text() == "("sv) {
					VariantArray values;
					for (;;) {
						tok = parser.next_token();
						if (tok.text() == ")"sv && tok.type == TokenSymbol) break;
						values.push_back(token2kv(tok, parser, true));
						tok = parser.next_token();
						if (tok.text() == ")"sv) break;
						if (tok.text() != ","sv)
							throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
					}
					query_.entries.Append(nextOp, QueryEntry{index, condition, std::move(values)});
				} else if (tok.type != TokenName || toLower(tok.text()) == "true" || toLower(tok.text()) == "false") {
					query_.entries.Append(nextOp, QueryEntry{index, condition, {token2kv(tok, parser, true)}});
					// Second field
				} else {
					query_.entries.Append(nextOp, BetweenFieldsQueryEntry{index, condition, std::string{tok.text()}});
				}
				nextOp = OpAnd;
			}
		}

		tok = parser.peek_token();
		while (tok.text() == "equal_position"sv) {
			parseEqualPositions(parser, equalPositions, lastBracketPosition);
			tok = parser.peek_token();
		}

		while (openBracketsCount > 0 && tok.text() == ")"sv) {
			query_.entries.CloseBracket();
			--openBracketsCount;
			parser.next_token();
			tok = parser.peek_token();
		}

		tok = peekSqlToken(parser, WhereOpSqlToken, false);

		if (iequals(tok.text(), "and"sv)) {
			nextOp = OpAnd;
			parser.next_token();
			tok = peekSqlToken(parser, AndSqlToken, false);
			if (iequals(tok.text(), "not"sv)) {
				parser.next_token();
				nextOp = OpNot;
			} else
				continue;
		} else if (iequals(tok.text(), "or"sv)) {
			parser.next_token();
			peekSqlToken(parser, FieldNameSqlToken);
			nextOp = OpOr;
		} else if (!iequals(tok.text(), "join"sv) && !iequals(tok.text(), "inner"sv) && !iequals(tok.text(), "left"sv)) {
			break;
		}
	}
	for (auto &&eqPos : equalPositions) {
		if (eqPos.first == 0) {
			query_.entries.equalPositions.emplace_back(std::move(eqPos.second));
		} else {
			query_.entries.Get<QueryEntriesBracket>(eqPos.first - 1).equalPositions.emplace_back(std::move(eqPos.second));
		}
	}

	if (query_.entries.Empty()) {
		throw Error(errParseSQL, "Expected condition after 'WHERE'");
	}

	return 0;
}

void SQLParser::parseEqualPositions(tokenizer &parser, std::vector<std::pair<size_t, EqualPosition_t>> &equalPositions,
									size_t lastBracketPosition) {
	parser.next_token();
	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found %s, %s", tok.text(), parser.where());
	}
	EqualPosition_t fields;
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken);
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.type != TokenName) {
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		}
		bool validField = false;
		for (auto it = query_.entries.begin_of_current_bracket(); it != query_.entries.end(); ++it) {
			if (it->HoldsOrReferTo<QueryEntry>() && nameWithCase.text() == it->Value<QueryEntry>().index) {
				validField = true;
				break;
			}
		}
		if (!validField) {
			throw Error(errParseSQL,
						"Only fields that present in 'Where' condition are allowed to use in equal_position(), but found '%s' in query, %s",
						nameWithCase.text(), parser.where());
		}
		fields.emplace_back(nameWithCase.text());
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.text() == ")"sv) break;
		if (tok.text() != ","sv) {
			throw Error(errParseSQL, "Expected ',', but found %s, %s", tok.text(), parser.where());
		}
	}
	if (fields.size() < 2) {
		throw Error(errLogic, "equal_position() is supposed to have at least 2 arguments. Arguments: [%s]",
					fields.size() ? fields[0] : "");  // -V547
	}
	equalPositions.emplace_back(lastBracketPosition, std::move(fields));
}

Point SQLParser::parseGeomFromText(tokenizer &parser) const {
	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found %s, %s", tok.text(), parser.where());
	}
	tok = parser.next_token();
	if (tok.type != TokenString) {
		throw Error(errParseSQL, "Expected text, but found %s, %s", tok.text(), parser.where());
	}
	std::string_view tokenText = tok.text();
	std::string_view str = skipSpace(tokenText);
	if (!checkIfStartsWith("point"sv, str)) {
		throw Error(errParseSQL, "Expected geometry object, but found %s, %s", tok.text(), parser.where());
	}
	str = skipSpace(str.substr(5));
	if (str.empty() || str[0] != '(') {
		throw Error(errParseSQL, "Expected '(' after '%s', but found '%s' in %s, %s", tokenText.substr(0, tokenText.size() - str.size()),
					str, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(1));
	size_t pos;
	double x, y;
	try {
		x = std::stod(std::string(str), &pos);
	} catch (...) {
		throw Error(errParseSQL, "Expected first number argument after '%s', but found '%s' in %s, %s",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.text(), parser.where());
	}
	if (pos >= str.size()) {
		throw Error(errParseSQL, "Expected space after '%s', but found nothing in %s, %s", tokenText, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(pos));
	try {
		y = std::stod(std::string(str), &pos);
	} catch (...) {
		throw Error(errParseSQL, "Expected second number argument after '%s', but found '%s' in %s, %s",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.text(), parser.where());
	}
	if (pos >= str.size()) {
		throw Error(errParseSQL, "Expected ')' after '%s', but found nothing in %s, %s", tokenText, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(pos));
	if (str.empty() || str[0] != ')') {
		throw Error(errParseSQL, "Expected ')' after '%s', but found '%s' in %s, %s", tokenText.substr(0, tokenText.size() - str.size()),
					str, tok.text(), parser.where());
	}
	str = skipSpace(str.substr(1));
	if (!str.empty()) {
		throw Error(errParseSQL, "Expected nothing after '%s', but found '%s' in %s, %s",
					tokenText.substr(0, tokenText.size() - str.size()), str, tok.text(), parser.where());
	}

	tok = parser.next_token();
	if (tok.text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
	}
	return Point{x, y};
}

void SQLParser::parseDWithin(tokenizer &parser, OpType nextOp) {
	Point point;
	std::string field;

	auto tok = parser.next_token();
	if (tok.text() != "("sv) {
		throw Error(errParseSQL, "Expected '(', but found %s, %s", tok.text(), parser.where());
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
		throw Error(errParseSQL, "Expected ',', but found %s, %s", tok.text(), parser.where());
	}

	peekSqlToken(parser, GeomFieldSqlToken);
	tok = parser.next_token();
	if (iequals(tok.text(), "st_geomfromtext"sv)) {
		if (field.empty()) {
			throw Error(errParseSQL, "Expected field name, but found %s, %s", tok.text(), parser.where());
		}
		point = parseGeomFromText(parser);
	} else {
		if (!field.empty()) {
			throw Error(errParseSQL, "Expected geometry object, but found %s, %s", tok.text(), parser.where());
		}
		field = std::string(tok.text());
	}

	tok = parser.next_token();
	if (tok.text() != ","sv) {
		throw Error(errParseSQL, "Expected ',', but found %s, %s", tok.text(), parser.where());
	}

	tok = parser.next_token();
	const auto distance = token2kv(tok, parser, false);
	distance.Type().EvaluateOneOf([](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) noexcept {},
								  [&](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple,
											KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Uuid>) {
									  throw Error(errParseSQL, "Expected number, but found %s, %s", tok.text(), parser.where());
								  });

	tok = parser.next_token();
	if (tok.text() != ")"sv) {
		throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
	}

	if (nextOp == OpOr) {
		query_.Or();
	} else if (nextOp == OpNot) {
		query_.Not();
	}
	query_.DWithin(field, point, distance.As<double>());
}

void SQLParser::parseJoin(JoinType type, tokenizer &parser) {
	JoinedQuery jquery;
	SQLParser jparser(jquery);
	if (ctx_.autocompleteMode) {
		jparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		jparser.ctx_.autocompleteMode = true;
	}
	auto tok = parser.next_token();
	if (tok.text() == "("sv) {
		peekSqlToken(parser, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text(), parser.where());
		}

		nestedSelectParse(jparser, parser);

		tok = parser.next_token();
		if (tok.text() != ")"sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
		}
	} else {
		jquery._namespace = std::string(tok.text());
		ctx_.updateLinkedNs(jquery._namespace);
	}
	jquery.joinType = type;
	jparser.parseJoinEntries(parser, query_._namespace, jquery);

	if (type != JoinType::LeftJoin) {
		query_.entries.Append((type == JoinType::InnerJoin) ? OpAnd : OpOr, JoinQueryEntry(query_.joinQueries_.size()));
	}

	query_.joinQueries_.emplace_back(std::move(jquery));
}

void SQLParser::parseMerge(tokenizer &parser) {
	JoinedQuery mquery;
	SQLParser mparser(mquery);
	if (ctx_.autocompleteMode) {
		mparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		mparser.ctx_.autocompleteMode = true;
	}
	auto tok = parser.next_token();
	if (tok.text() == "("sv) {
		peekSqlToken(parser, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text(), parser.where());
		}

		nestedSelectParse(mparser, parser);

		tok = parser.next_token();
		if (tok.text() != ")"sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
		}
	}
	mquery.joinType = JoinType::Merge;

	query_.mergeQueries_.emplace_back(std::move(mquery));
}

std::string SQLParser::parseJoinedFieldName(tokenizer &parser, std::string &name) {
	auto tok = peekSqlToken(parser, JoinedFieldNameSqlToken);
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}

	auto dotPos = tok.text().find('.');
	if (dotPos == std::string_view::npos) {
		return std::string(tok.text());
	}
	name = std::string(tok.text().substr(0, dotPos));

	tok = peekSqlToken(parser, FieldNameSqlToken);
	if (tok.type != TokenName) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();
	ctx_.updateLinkedNs(name);
	return std::string(tok.text().substr(dotPos + 1));
}

void SQLParser::parseJoinEntries(tokenizer &parser, const std::string &mainNs, JoinedQuery &jquery) {
	auto tok = peekSqlToken(parser, OnSqlToken);
	if (tok.text() != "on"sv) {
		throw Error(errParseSQL, "Expected 'ON', but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();

	tok = parser.peek_token();

	bool braces = tok.text() == "("sv;
	if (braces) parser.next_token();

	while (!parser.end()) {
		auto tok = peekSqlToken(parser, OpSqlToken);
		if (tok.text() == "or"sv) {
			jquery.nextOp_ = OpOr;
			parser.next_token();
			tok = parser.peek_token();
		} else if (tok.text() == "and"sv) {
			jquery.nextOp_ = OpAnd;
			parser.next_token();
			tok = parser.peek_token();
		}

		if (braces && tok.text() == ")"sv) {
			parser.next_token();
			return;
		}

		QueryJoinEntry je;
		std::string ns1 = mainNs, ns2 = jquery._namespace;
		std::string idx1 = parseJoinedFieldName(parser, ns1);
		je.condition_ = getCondType(parser.next_token().text());
		std::string idx2 = parseJoinedFieldName(parser, ns2);

		if (ns1 == mainNs && ns2 == jquery._namespace) {
			je.index_ = std::move(idx1);
			je.joinIndex_ = std::move(idx2);
		} else if (ns2 == mainNs && ns1 == jquery._namespace) {
			je.index_ = std::move(idx2);
			je.joinIndex_ = std::move(idx1);
			je.condition_ = InvertJoinCondition(je.condition_);
			je.reverseNamespacesOrder = true;
		} else {
			throw Error(errParseSQL, "Unexpected tables with ON statement: ('%s' and '%s') but expected ('%s' and '%s'), %s", ns1, ns2,
						mainNs, jquery._namespace, parser.where());
		}

		je.op_ = jquery.nextOp_;
		jquery.nextOp_ = OpAnd;
		jquery.joinEntries_.emplace_back(std::move(je));
		if (!braces) {
			return;
		}
	}
}
CondType SQLParser::getCondType(std::string_view cond) {
	if (cond == "="sv || cond == "=="sv || cond == "is"sv) {
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
	throw Error(errParseSQL, "Expected condition operator, but found '%s' in query", cond);
}

}  // namespace reindexer
