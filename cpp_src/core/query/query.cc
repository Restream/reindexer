
#include "core/query/query.h"
#include "core/query/dslencoder.h"
#include "core/query/dslparsetools.h"
#include "core/type_consts.h"
#include "estl/tokenizer.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/serializer.h"

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

	return true;
}

int Query::FromSQL(const string_view &q) {
	tokenizer parser(q);
	return Parse(parser);
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
			case QueryEnd:
				return;
		}
	}
}

int Query::Parse(tokenizer &parser) {
	token tok = parser.next_token();

	if (tok.text() == "explain"_sv) {
		explain_ = true;
		tok = parser.next_token();
	}

	if (tok.text() == "select"_sv) {
		selectParse(parser);
	} else {
		throw Error(errParams, "Syntax error at or near '%s', %s", tok.text().data(), parser.where().c_str());
	}
	tok = parser.next_token();
	if (tok.text() == ";") {
		tok = parser.next_token();
	}
	parser.skip_space();
	if (tok.text() != "" || !parser.end())
		throw Error(errParseSQL, "Unexpected '%s' in query, %s", tok.text().data(), parser.where().c_str());

	return 0;
}

int Query::selectParse(tokenizer &parser) {
	// Get filter
	token tok;
	while (!parser.end()) {
		auto nameWithCase = parser.peek_token(false);
		auto name = parser.next_token();
		tok = parser.peek_token();
		if (tok.text() == "("_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (name.text() == "avg"_sv) {
				aggregations_.push_back({tok.text().ToString(), AggAvg});
			} else if (name.text() == "facet"_sv) {
				aggregations_.push_back({tok.text().ToString(), AggFacet});
			} else if (name.text() == "sum"_sv) {
				aggregations_.push_back({tok.text().ToString(), AggSum});
			} else if (name.text() == "min"_sv) {
				aggregations_.push_back({tok.text().ToString(), AggMin});
			} else if (name.text() == "max"_sv) {
				aggregations_.push_back({tok.text().ToString(), AggMax});
			} else if (name.text() == "count"_sv) {
				calcTotal = ModeAccurateTotal;
				count = 0;
			} else {
				throw Error(errParams, "Unknown function name SQL - %s, %s", name.text().data(), parser.where().c_str());
			}
			tok = parser.next_token();
			if (tok.text() != ")"_sv) {
				throw Error(errParams, "Expected ')', but found %s, %s", tok.text().data(), parser.where().c_str());
			}
			tok = parser.peek_token();

		} else if (name.text() != "*"_sv) {
			selectFilter_.push_back(nameWithCase.text().ToString());
			count = INT_MAX;
		} else if (name.text() == "*"_sv) {
			count = INT_MAX;
		}
		if (tok.text() != ","_sv) break;
		tok = parser.next_token();
	}

	if (parser.next_token().text() != "from"_sv)
		throw Error(errParams, "Expected 'FROM', but found '%s' in query, %s", tok.text().data(), parser.where().c_str());

	_namespace = parser.next_token().text().ToString();
	parser.skip_space();

	while (!parser.end()) {
		tok = parser.peek_token();
		if (tok.text() == "where"_sv) {
			parser.next_token();
			ParseWhere(parser);
		} else if (tok.text() == "limit"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text().data(), parser.where().c_str());
			count = atoi(tok.text().data());
		} else if (tok.text() == "offset"_sv) {
			parser.next_token();
			tok = parser.next_token();
			if (tok.type != TokenNumber)
				throw Error(errParseSQL, "Expected number, but found '%s' in query, %s", tok.text().data(), parser.where().c_str());
			start = atoi(tok.text().data());
		} else if (tok.text() == "order"_sv) {
			parser.next_token();
			// Just skip token (BY)
			parser.next_token();
			for (;;) {
				auto nameWithCase = parser.peek_token();
				tok = parser.next_token(false);
				if (tok.type != TokenName && tok.type != TokenString)
					throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text().data(), parser.where().c_str());
				SortingEntry sortingEntry;
				sortingEntry.column = tok.text().ToString();
				tok = parser.peek_token();
				if (tok.text() == "("_sv && nameWithCase.text() == "field"_sv) {
					parser.next_token();
					tok = parser.next_token(false);
					if (tok.type != TokenName && tok.type != TokenString)
						throw Error(errParseSQL, "Expected name, but found '%s' in query, %s", tok.text().data(), parser.where().c_str());
					sortingEntry.column = tok.text().ToString();
					for (;;) {
						tok = parser.next_token();
						if (tok.text() == ")"_sv) break;
						if (tok.text() != ","_sv)
							throw Error(errParseSQL, "Expected ')' or ',', but found '%s' in query, %s", tok.text().data(),
										parser.where().c_str());
						tok = parser.next_token();
						if (tok.type != TokenNumber && tok.type != TokenString)
							throw Error(errParseSQL, "Expected parameter, but found '%s' in query, %s", tok.text().data(),
										parser.where().c_str());
						forcedSortOrder.push_back(Variant(tok.text().ToString()));
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
		} else if (tok.text() == "join"_sv) {
			parser.next_token();
			parseJoin(JoinType::LeftJoin, parser);
		} else if (tok.text() == "left"_sv) {
			parser.next_token();
			if (parser.next_token().text() != "join"_sv) {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text().data(), parser.where().c_str());
			}
			parseJoin(JoinType::LeftJoin, parser);
		} else if (tok.text() == "inner"_sv) {
			parser.next_token();
			if (parser.next_token().text() != "join") {
				throw Error(errParseSQL, "Expected JOIN, but found '%s' in query, %s", tok.text().data(), parser.where().c_str());
			}
			auto jtype = nextOp_ == OpOr ? JoinType::OrInnerJoin : JoinType::InnerJoin;
			nextOp_ = OpAnd;
			parseJoin(jtype, parser);
		} else if (tok.text() == "merge"_sv) {
			parser.next_token();
			parseMerge(parser);
		} else if (tok.text() == "or"_sv) {
			parser.next_token();
			nextOp_ = OpOr;
		} else {
			break;
		}
	}
	return 0;
}

void Query::parseJoin(JoinType type, tokenizer &parser) {
	Query jquery;
	auto tok = parser.next_token();
	if (tok.text() == "("_sv) {
		tok = parser.next_token();
		if (tok.text() != "select"_sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text().data(), parser.where().c_str());
		}
		jquery.selectParse(parser);
		tok = parser.next_token();
		if (tok.text() != ")"_sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text().data(), parser.where().c_str());
		}
	} else {
		jquery._namespace = tok.text().ToString();
	}
	jquery.joinType = type;
	jquery.parseJoinEntries(parser, _namespace);

	joinQueries_.push_back(std::move(jquery));
}

void Query::parseMerge(tokenizer &parser) {
	Query mquery;
	auto tok = parser.next_token();
	if (tok.text() == "("_sv) {
		tok = parser.next_token();
		if (tok.text() != "select"_sv) {
			throw Error(errParseSQL, "Expected 'SELECT', but found %s, %s", tok.text().data(), parser.where().c_str());
		}
		mquery.selectParse(parser);
		tok = parser.next_token();
		if (tok.text() != ")"_sv) {
			throw Error(errParseSQL, "Expected ')', but found %s, %s", tok.text().data(), parser.where().c_str());
		}
	}
	mquery.joinType = JoinType::Merge;

	mergeQueries_.push_back(std::move(mquery));
}

// parse [table.]field
// return field
string parseDotStr(tokenizer &parser, string &str1) {
	auto tok = parser.next_token();
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text().data(), parser.where().c_str());
	}
	if (parser.peek_token().text() != "."_sv) {
		return tok.text().ToString();
	}
	parser.next_token();
	str1 = tok.text().ToString();

	tok = parser.next_token();
	if (tok.type != TokenName && tok.type != TokenString) {
		throw Error(errParseSQL, "Expected name, but found %s, %s", tok.text().data(), parser.where().c_str());
	}
	return tok.text().ToString();
}

void Query::parseJoinEntries(tokenizer &parser, const string &mainNs) {
	parser.skip_space();
	QueryJoinEntry je;
	auto tok = parser.next_token();
	if (tok.text() != "on"_sv) {
		throw Error(errParseSQL, "Expected 'ON', but found %s, %s", tok.text().data(), parser.where().c_str());
	}

	tok = parser.peek_token();

	bool braces = tok.text() == "("_sv;
	if (braces) parser.next_token();

	while (!parser.end()) {
		auto tok = parser.peek_token();
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
		string idx1 = parseDotStr(parser, ns1);
		je.condition_ = getCondType(parser.next_token().text());
		string idx2 = parseDotStr(parser, ns2);

		if (ns1 == mainNs && ns2 == _namespace) {
			je.index_ = idx1;
			je.joinIndex_ = idx2;
		} else if (ns2 == mainNs && ns1 == _namespace) {
			je.index_ = idx2;
			je.joinIndex_ = idx1;
		} else {
			throw Error(errParseSQL, "Unexpected tables with ON statement: ('%s' and '%s') but expected ('%s' and '%s'), %s", ns1.c_str(),
						ns2.c_str(), mainNs.c_str(), _namespace.c_str(), parser.where().c_str());
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
	ser << "SELECT ";
	if (aggregations_.size()) {
		for (auto &a : aggregations_) {
			if (&a != &*aggregations_.begin()) ser << ',';
			switch (a.type_) {
				case AggAvg:
					ser << "AVG(";
					break;
				case AggSum:
					ser << "SUM(";
					break;
				case AggFacet:
					ser << "FACET(";
					break;
				case AggMin:
					ser << "MIN(";
					break;
				case AggMax:
					ser << "MAX(";
					break;
				default:
					ser << "<?> (";
					break;
			}
			ser << a.index_ << ')';
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
	dumpWhere(ser, stripArgs);
	dumpJoined(ser, stripArgs);
	dumpMerged(ser, stripArgs);
	dumpOrderBy(ser, stripArgs);

	if (start != 0) ser << " OFFSET " << start;
	if (count != UINT_MAX) ser << " LIMIT " << count;
	return ser;
}

}  // namespace reindexer
