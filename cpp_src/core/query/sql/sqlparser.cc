
#include "sqlparser.h"
#include "core/keyvalue/key_string.h"
#include "core/query/query.h"
#include "core/queryresults/aggregationresult.h"
#include "sqltokentype.h"
#include "vendor/gason/gason.h"

namespace reindexer {

SQLParser::SQLParser(Query &query) : query_(query) {}

int SQLParser::Parse(const string_view &q) {
	tokenizer parser(q);
	return Parse(parser);
}

bool SQLParser::reachedAutocompleteToken(tokenizer &parser, const token &tok) {
	size_t pos = parser.getPos() + tok.text().length();
	return (pos > ctx_.suggestionsPos);
}

token SQLParser::peekSqlToken(tokenizer &parser, int tokenType, bool toLower) {
	token tok = parser.peek_token(toLower);
	bool eof = ((parser.getPos() + tok.text().length()) == parser.length());
	if (ctx_.autocompleteMode && !tok.text().empty() && reachedAutocompleteToken(parser, tok)) {
		size_t tokenLen = 0;
		if (ctx_.suggestionsPos >= parser.getPos()) {
			tokenLen = ctx_.suggestionsPos - parser.getPos() + 1;
		}
		if (!ctx_.foundPossibleSuggestions || tokenLen) {
			ctx_.suggestions.emplace_back(string(tok.text().data(), tokenLen), tokenType);
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
	if (tok.text() == "explain"_sv) {
		query_.explain_ = true;
		parser.next_token();
		tok = peekSqlToken(parser, StartAfterExplain);
	}

	if (tok.text() == "select"_sv) {
		query_.type_ = QuerySelect;
		parser.next_token();
		selectParse(parser);
	} else if (tok.text() == "delete"_sv) {
		query_.type_ = QueryDelete;
		tok = parser.next_token();
		deleteParse(parser);
	} else if (tok.text() == "update"_sv) {
		query_.type_ = QueryUpdate;
		updateParse(parser);
	} else if (tok.text() == "truncate"_sv) {
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
		if (tok.text() == "("_sv) {
			parser.next_token();
			tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
			AggType agg = AggregationResult::strToAggType(name.text());
			if (agg != AggUnknown) {
				AggregateEntry entry{agg, {string(tok.text())}, UINT_MAX, 0};
				tok = parser.next_token();
				for (tok = parser.peek_token(); tok.text() == ","_sv; tok = parser.peek_token()) {
					parser.next_token();
					tok = peekSqlToken(parser, SingleSelectFieldSqlToken);
					entry.fields_.push_back(string(tok.text()));
					tok = parser.next_token();
				}
				for (tok = parser.peek_token(); tok.text() != ")"_sv; tok = parser.peek_token()) {
					if (tok.text() == "order"_sv) {
						parser.next_token();
						h_vector<Variant, 0> orders;
						parseOrderBy(parser, entry.sortingEntries_, orders);
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
				query_.aggregations_.push_back(std::move(entry));
			} else {
				if (name.text() == "count"_sv) {
					query_.calcTotal = ModeAccurateTotal;
					if (!wasSelectFilter) query_.count = 0;
					tok = parser.next_token();
				} else if (name.text() == "count_cached"_sv) {
					query_.calcTotal = ModeCachedTotal;
					if (!wasSelectFilter) query_.count = 0;
					tok = parser.next_token();
				} else if (name.text() == "rank"_sv) {
					query_.WithRank();
				} else {
					throw Error(errParams, "Unknown function name SQL - %s, %s", name.text(), parser.where());
				}
			}
			tok = parser.peek_token();
			if (tok.text() != ")"_sv) {
				throw Error(errParams, "Expected ')', but found %s, %s", tok.text(), parser.where());
			}
			parser.next_token();
			tok = peekSqlToken(parser, SelectFieldsListSqlToken);

		} else if (name.text() != "*"_sv) {
			query_.selectFilter_.push_back(string(nameWithCase.text()));
			query_.count = UINT_MAX;
			wasSelectFilter = true;
		} else if (name.text() == "*"_sv) {
			query_.count = UINT_MAX;
			wasSelectFilter = true;
			query_.selectFilter_.clear();
		}
		if (tok.text() != ","_sv) break;
		tok = parser.next_token();
	}

	peekSqlToken(parser, FromSqlToken);
	if (parser.next_token().text() != "from"_sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text(), parser.where());

	peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = string(parser.next_token().text());
	ctx_.updateLinkedNs(query_._namespace);

	while (!parser.end()) {
		tok = peekSqlToken(parser, SelectConditionsStart);
		if (tok.text() == "where"_sv) {
			parser.next_token();
			parseWhere(parser);
		} else if (tok.text() == "limit"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.count = stoi(tok.text());
		} else if (tok.text() == "offset"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.start = stoi(tok.text());
		} else if (tok.text() == "order"_sv) {
			parser.next_token();
			parseOrderBy(parser, query_.sortingEntries_, query_.forcedSortOrder_);
			ctx_.updateLinkedNs(query_._namespace);
		} else if (tok.text() == "join"_sv) {
			parser.next_token();
			parseJoin(JoinType::LeftJoin, parser);
		} else if (tok.text() == "left"_sv) {
			parser.next_token();
			peekSqlToken(parser, LeftSqlToken);
			if (parser.next_token().text() != "join"_sv) {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
			}
			parseJoin(JoinType::LeftJoin, parser);
		} else if (tok.text() == "inner"_sv) {
			parser.next_token();
			peekSqlToken(parser, InnerSqlToken);
			if (parser.next_token().text() != "join") {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
			}
			auto jtype = (query_.nextOp_ == OpOr) ? JoinType::OrInnerJoin : JoinType::InnerJoin;
			query_.nextOp_ = OpAnd;
			parseJoin(jtype, parser);
		} else if (tok.text() == "merge"_sv) {
			parser.next_token();
			parseMerge(parser);
		} else if (tok.text() == "or"_sv) {
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

int SQLParser::parseOrderBy(tokenizer &parser, SortingEntries &sortingEntries, h_vector<Variant, 0> &forcedSortOrder_) {
	// Just skip token (BY)
	peekSqlToken(parser, BySqlToken);
	parser.next_token();
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken);
		auto tok = parser.next_token(false);
		if (tok.type != TokenName && tok.type != TokenString)
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		SortingEntry sortingEntry;
		sortingEntry.expression = string(tok.text());
		if (sortingEntry.expression.empty()) {
			throw Error(errParseSQL, "Order by expression should not be empty, %s", parser.where());
		}
		tok = peekSqlToken(parser, SortDirectionSqlToken);
		if (tok.text() == "("_sv && nameWithCase.text() == "field"_sv) {
			parser.next_token();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.type != TokenName && tok.type != TokenString)
				throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
			sortingEntry.expression = string(tok.text());
			tok = parser.next_token(false);
			for (;;) {
				tok = parser.next_token();
				if (tok.text() == ")"_sv) break;
				if (tok.text() != ","_sv)
					throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
				tok = peekSqlToken(parser, FieldNameSqlToken);
				if (tok.type != TokenNumber && tok.type != TokenString)
					throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", tok.text(), parser.where());
				forcedSortOrder_.push_back(Variant(string(tok.text())));
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

int SQLParser::deleteParse(tokenizer &parser) {
	// Get filter
	token tok;

	peekSqlToken(parser, FromSqlToken);
	if (parser.next_token().text() != "from"_sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text(), parser.where());

	peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = string(parser.next_token().text());
	ctx_.updateLinkedNs(query_._namespace);

	while (!parser.end()) {
		tok = peekSqlToken(parser, DeleteConditionsStart);
		if (tok.text() == "where"_sv) {
			parser.next_token();
			parseWhere(parser);
		} else if (tok.text() == "limit"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.count = stoi(tok.text());
		} else if (tok.text() == "offset"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text(), parser.where());
			query_.start = stoi(tok.text());
		} else if (tok.text() == "order"_sv) {
			parser.next_token();
			parseOrderBy(parser, query_.sortingEntries_, query_.forcedSortOrder_);
			ctx_.updateLinkedNs(query_._namespace);
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

static Variant token2kv(const token &currTok, tokenizer &parser, bool allowComposite) {
	if (currTok.text() == "true"_sv) return Variant(true);
	if (currTok.text() == "false"_sv) return Variant(false);

	if (currTok.text() == "{"_sv) {
		// Composite value parsing
		if (!allowComposite) {
			throw Error(errParseSQL, "Unexpected '{' in query, %s", parser.where());
		}
		VariantArray compositeValues;
		for (;;) {
			auto tok = parser.next_token();
			compositeValues.push_back(token2kv(tok, parser, false));
			tok = parser.next_token();
			if (tok.text() == "}"_sv) {
				return compositeValues;
			}
			if (tok.text() != ","_sv) {
				throw Error(errParseSQL, "Expected ',', but found '%s' in query, %s", tok.text(), parser.where());
			}
		}
	}

	string_view value = currTok.text();
	if ((currTok.type == TokenName) && (iequals(currTok.text(), "true"_sv) || iequals(currTok.text(), "false"_sv))) {
		return Variant(iequals(value, "true"_sv));
	}

	if (currTok.type != TokenNumber && currTok.type != TokenString) {
		throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", currTok.text(), parser.where());
	}

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
	updateField.mode = FieldModeSet;
	if (currTok.type == TokenString) {
		updateField.values.push_back(token2kv(currTok, parser, false));
	} else {
		if ((currTok.type == TokenName) && (currTok.text() == "null"_sv)) {
			updateField.values.push_back(Variant());
		} else if ((currTok.type == TokenSymbol) && (currTok.text() == "{"_sv)) {
			try {
				size_t jsonPos = parser.getPos() - 1;
				string json(parser.begin() + jsonPos, parser.length() - jsonPos);
				size_t jsonLength = 0;
				gason::JsonParser jsonParser;
				jsonParser.Parse(giftStr(json), &jsonLength);
				updateField.values.emplace_back(Variant(string(parser.begin() + jsonPos, jsonLength)));
				updateField.mode = FieldModeSetJson;
				parser.setPos(jsonPos + jsonLength);
			} catch (const gason::Exception &e) {
				throw Error(errParseSQL, "%s, in query %s", e.what(), parser.where());
			}
		} else {
			auto eof = [](tokenizer &parser) -> bool {
				if (parser.end()) return true;
				token nextTok = parser.peek_token();
				return ((nextTok.text() == "where"_sv) || (nextTok.text() == "]"_sv) || (nextTok.text() == ","_sv));
			};
			int count = 0;
			string expression(currTok.text());
			while (!eof(parser)) {
				expression += string(parser.next_token(false).text());
				++count;
			}
			if (count > 0) {
				updateField.values.push_back(Variant(expression));
				updateField.isExpression = true;
			} else {
				try {
					Variant val = token2kv(currTok, parser, false);
					updateField.values.push_back(val);
				} catch (const Error &) {
					updateField.values.push_back(Variant(expression));
					updateField.isExpression = true;
				}
			}
		}
	}
}

UpdateEntry SQLParser::parseUpdateField(tokenizer &parser) {
	UpdateEntry updateField;
	token tok = peekSqlToken(parser, FieldNameSqlToken, false);
	if (tok.type != TokenName && tok.type != TokenString)
		throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
	updateField.column = string(tok.text());
	parser.next_token();

	tok = parser.next_token();
	if (tok.text() != "="_sv) throw Error(errParams, "Expected '=' but found '%s' in query, '%s'", tok.text(), parser.where());

	tok = parser.next_token(false);
	if (tok.text() == "["_sv) {
		updateField.values.MarkArray();
		for (;;) {
			tok = parser.next_token(false);
			if (tok.text() == "]") {
				if (updateField.values.empty()) break;
				throw Error(errParseSQL, "Expected field value, but found ']' in query, %s", parser.where());
			}
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

int SQLParser::updateParse(tokenizer &parser) {
	parser.next_token();

	token tok = peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = string(tok.text());
	ctx_.updateLinkedNs(query_._namespace);
	parser.next_token();

	tok = peekSqlToken(parser, UpdateOptionsSqlToken);
	if (tok.text() == "set"_sv) {
		parser.next_token();
		while (!parser.end()) {
			query_.updateFields_.emplace_back(parseUpdateField(parser));

			tok = parser.peek_token();
			if (tok.text() != ","_sv) break;
			parser.next_token();
		}
	} else if (tok.text() == "drop"_sv) {
		while (!parser.end()) {
			parser.next_token();
			tok = peekSqlToken(parser, FieldNameSqlToken, false);
			if (tok.type != TokenName && tok.type != TokenString)
				throw Error(errParseSQL, "Expected field name, but found '%s' in query, %s", tok.text(), parser.where());
			query_.Drop(string(tok.text()));
			parser.next_token();
			tok = parser.peek_token();
			if (tok.text() != ","_sv) break;
		}
	} else {
		throw Error(errParseSQL, "Expected 'SET' or 'DROP' but found '%s' in query %s", tok.text(), parser.where());
	}

	tok = peekSqlToken(parser, WhereSqlToken);
	if (tok.text() == "where"_sv) {
		parser.next_token();
		parseWhere(parser);
	}

	return 0;
}

int SQLParser::truncateParse(tokenizer &parser) {
	parser.next_token();
	token tok = peekSqlToken(parser, NamespaceSqlToken);
	query_._namespace = string(tok.text());
	ctx_.updateLinkedNs(query_._namespace);
	parser.next_token();
	return 0;
}

int SQLParser::parseWhere(tokenizer &parser) {
	token tok;
	OpType nextOp = OpAnd;

	tok = peekSqlToken(parser, WhereFieldSqlToken, false);

	if (iequals(tok.text(), "not"_sv)) {
		nextOp = OpNot;
		parser.next_token();
	}

	int openBracketCount = 0;
	while (!parser.end()) {
		tok = peekSqlToken(parser, WhereFieldSqlToken, false);
		parser.next_token(false);
		if (tok.text() == "("_sv) {
			query_.entries.OpenBracket(nextOp);
			++openBracketCount;
			tok = peekSqlToken(parser, WhereFieldSqlToken, false);
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
				parseJoin(JoinType::LeftJoin, parser);
			} else if (iequals(tok.text(), "left"_sv)) {
				peekSqlToken(parser, LeftSqlToken);
				if (parser.next_token().text() != "join"_sv) {
					throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
				}
				parseJoin(JoinType::LeftJoin, parser);
			} else if (iequals(tok.text(), "inner"_sv)) {
				peekSqlToken(parser, InnerSqlToken);
				if (parser.next_token().text() != "join") {
					throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text(), parser.where());
				}
				auto jtype = nextOp == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
				query_.nextOp_ = OpAnd;
				parseJoin(jtype, parser);
			} else {
				QueryEntry entry;
				// Index name
				entry.index = string(tok.text());

				// Operator
				tok = peekSqlToken(parser, ConditionSqlToken);
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
				if (ctx_.autocompleteMode) peekSqlToken(parser, WhereFieldValueSqlToken, false);
				tok = parser.next_token();
				if (iequals(tok.text(), "null"_sv) || iequals(tok.text(), "empty"_sv)) {
					entry.condition = CondEmpty;
				} else if (iequals(tok.text(), "not"_sv)) {
					tok = peekSqlToken(parser, WhereFieldNegateValueSqlToken, false);
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
						entry.values.push_back(token2kv(tok, parser, true));
						tok = parser.next_token();
						if (tok.text() == ")"_sv) break;
						if (tok.text() != ","_sv)
							throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text(), parser.where());
					}
				} else {
					entry.values.push_back(token2kv(tok, parser, true));
				}
				query_.entries.Append(nextOp, std::move(entry));
				nextOp = OpAnd;
			}
		}

		tok = parser.peek_token();
		while (tok.text() == "equal_position"_sv) {
			parseEqualPositions(parser);
			tok = parser.peek_token();
		}

		while (openBracketCount > 0 && tok.text() == ")"_sv) {
			query_.entries.CloseBracket();
			--openBracketCount;
			parser.next_token();
			tok = parser.peek_token();
		}

		tok = peekSqlToken(parser, WhereOpSqlToken, false);

		if (iequals(tok.text(), "and"_sv)) {
			nextOp = OpAnd;
			parser.next_token();
			tok = peekSqlToken(parser, AndSqlToken, false);
			if (iequals(tok.text(), "not"_sv)) {
				parser.next_token();
				nextOp = OpNot;
			} else
				continue;
		} else if (iequals(tok.text(), "or"_sv)) {
			parser.next_token();
			peekSqlToken(parser, FieldNameSqlToken);
			nextOp = OpOr;
		} else if (!iequals(tok.text(), "join"_sv) && !iequals(tok.text(), "inner"_sv) && !iequals(tok.text(), "left"_sv)) {
			break;
		}
	}
	return 0;
}

void SQLParser::parseEqualPositions(tokenizer &parser) {
	parser.next_token();
	auto tok = parser.next_token();
	if (tok.text() != "("_sv) {
		throw Error(errParseSQL, "Expected '(', but found %s, %s", tok.text(), parser.where());
	}
	vector<string> fields;
	for (;;) {
		auto nameWithCase = peekSqlToken(parser, FieldNameSqlToken);
		tok = parser.next_token(false);
		if (tok.type != TokenName && tok.type != TokenString) {
			throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text(), parser.where());
		}
		bool validField = false;
		for (auto it = query_.entries.begin_of_current_bracket(); it != query_.entries.end(); ++it) {
			if (it->IsLeaf()) {
				if (nameWithCase.text() == it->Value().index) {
					validField = true;
					break;
				}
			}
		}
		if (!validField) {
			throw Error(errParseSQL,
						"Only fields that present in 'Where' condition are allowed to use in equal_position(), but found '%s' in query, %s",
						nameWithCase.text(), parser.where());
		}
		fields.emplace_back(nameWithCase.text());
		tok = parser.next_token(false);
		if (tok.text() == ")"_sv) break;
		if (tok.text() != ","_sv) {
			throw Error(errParseSQL, "Expected ',', but found %s, %s", tok.text(), parser.where());
		}
	}
	if (fields.size() < 2) {
		throw Error(errLogic, "equal_position() is supposed to have at least 2 arguments. Arguments: [%s]",
					fields.size() ? fields[0] : "");  // -V547
	}
	query_.equalPositions_.emplace(query_.entries.DetermineEqualPositionIndexes(fields));
}

void SQLParser::parseJoin(JoinType type, tokenizer &parser) {
	JoinedQuery jquery;
	SQLParser jparser(jquery);
	if (ctx_.autocompleteMode) {
		jparser.ctx_.suggestionsPos = ctx_.suggestionsPos;
		jparser.ctx_.autocompleteMode = true;
	}
	auto tok = parser.next_token();
	if (tok.text() == "("_sv) {
		peekSqlToken(parser, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"_sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text(), parser.where());
		}

		nestedSelectParse(jparser, parser);

		tok = parser.next_token();
		if (tok.text() != ")"_sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
		}
	} else {
		jquery._namespace = string(tok.text());
		ctx_.updateLinkedNs(jquery._namespace);
	}
	jquery.joinType = type;
	jparser.parseJoinEntries(parser, query_._namespace, jquery);

	if (type != JoinType::LeftJoin) {
		query_.entries.Append((type == JoinType::InnerJoin) ? OpAnd : OpOr, QueryEntry(query_.joinQueries_.size()));
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
	if (tok.text() == "("_sv) {
		peekSqlToken(parser, SelectSqlToken);
		tok = parser.next_token();
		if (tok.text() != "select"_sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text(), parser.where());
		}

		nestedSelectParse(mparser, parser);

		tok = parser.next_token();
		if (tok.text() != ")"_sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text(), parser.where());
		}
	}
	mquery.joinType = JoinType::Merge;

	query_.mergeQueries_.emplace_back(std::move(mquery));
}

string SQLParser::parseJoinedFieldName(tokenizer &parser, string &name) {
	auto tok = peekSqlToken(parser, JoinedFieldNameSqlToken);
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}

	auto dotPos = tok.text().find('.');
	if (dotPos == string_view::npos) {
		return string(tok.text());
	}
	name = string(tok.text().substr(0, dotPos));

	tok = peekSqlToken(parser, FieldNameSqlToken);
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();
	ctx_.updateLinkedNs(name);
	return string(tok.text().substr(dotPos + 1));
}

void SQLParser::parseJoinEntries(tokenizer &parser, const string &mainNs, JoinedQuery &jquery) {
	QueryJoinEntry je;
	auto tok = peekSqlToken(parser, OnSqlToken);
	if (tok.text() != "on"_sv) {
		throw Error(errParseSQL, "Expected 'ON', but found %s, %s", tok.text(), parser.where());
	}
	parser.next_token();

	tok = parser.peek_token();

	bool braces = tok.text() == "("_sv;
	if (braces) parser.next_token();

	while (!parser.end()) {
		auto tok = peekSqlToken(parser, OpSqlToken);
		if (tok.text() == "or"_sv) {
			jquery.nextOp_ = OpOr;
			parser.next_token();
			tok = parser.peek_token();
		} else if (tok.text() == "and"_sv) {
			jquery.nextOp_ = OpAnd;
			parser.next_token();
			tok = parser.peek_token();
		}

		if (braces && tok.text() == ")"_sv) {
			parser.next_token();
			return;
		}

		string ns1 = mainNs, ns2 = jquery._namespace;
		string idx1 = parseJoinedFieldName(parser, ns1);
		je.condition_ = getCondType(parser.next_token().text());
		string idx2 = parseJoinedFieldName(parser, ns2);

		if (ns1 == mainNs && ns2 == jquery._namespace) {
			je.index_ = idx1;
			je.joinIndex_ = idx2;
		} else if (ns2 == mainNs && ns1 == jquery._namespace) {
			je.index_ = idx2;
			je.joinIndex_ = idx1;
		} else {
			throw Error(errParseSQL, "Unexpected tables with ON statement: ('%s' and '%s') but expected ('%s' and '%s'), %s", ns1, ns2,
						mainNs, jquery._namespace, parser.where());
		}

		je.op_ = jquery.nextOp_;
		jquery.nextOp_ = OpAnd;
		jquery.joinEntries_.push_back(std::move(je));
		if (!braces) {
			return;
		}
	}
}
CondType SQLParser::getCondType(string_view cond) {
	if (cond == "="_sv || cond == "=="_sv || cond == "is"_sv) {
		return CondEq;
	} else if (cond == ">"_sv) {
		return CondGt;
	} else if (cond == ">="_sv) {
		return CondGe;
	} else if (cond == "<"_sv) {
		return CondLt;
	} else if (cond == "<="_sv) {
		return CondLe;
	} else if (iequals(cond, "in"_sv)) {
		return CondSet;
	} else if (iequals(cond, "range"_sv)) {
		return CondRange;
	} else if (iequals(cond, "like"_sv)) {
		return CondLike;
	}
	throw Error(errParseSQL, "Expected condition operator, but found '%s' in query", cond);
}
}  // namespace reindexer
