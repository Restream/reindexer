#include <unordered_map>

#include "core/query/dslparsetools.h"
#include "core/query/query.h"
#include "core/type_consts.h"
#include "dslparsetools.h"
#include "estl/flat_str_map.h"
#include "estl/tokenizer.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/json2kv.h"
#include "tools/logger.h"

namespace reindexer {

Query::Query(const string &__namespace, unsigned _start, unsigned _count, CalcTotalMode _calcTotal)
	: _namespace(__namespace), calcTotal(_calcTotal), start(_start), count(_count) {}

int Query::Parse(const string &q) {
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
		throw Error(errParseJson, "Could not parse JSON-query: %s at %zd", jsonStrError(error), endptr - src);
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
				while (count--) qe.values.push_back(ser.GetValue());
				entries.push_back(qe);
				break;
			}
			case QueryAggregation:
				aggregations_.push_back({ser.GetVString().ToString(), AggType(ser.GetVarUint())});
				break;
			case QueryDistinct:
				qe.index = ser.GetVString().ToString();
				qe.distinct = true;
				qe.condition = CondAny;
				entries.push_back(qe);
				break;
			case QuerySortIndex: {
				sortBy = ser.GetVString().ToString();
				sortDirDesc = bool(ser.GetVarUint());
				int count = ser.GetVarUint();
				forcedSortOrder.reserve(count);
				while (count--) forcedSortOrder.push_back(ser.GetValue());
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
			case QueryEnd:
				return;
		}
	}
}

int Query::Parse(tokenizer &parser) {
	token tok = parser.next_token();

	if (tok.text == "describe") {
		return describeParse(parser);
	} else if (tok.text == "select") {
		return selectParse(parser);
	} else {
		throw Error(errParams, "Syntax error at or near '%s'", tok.text.c_str());
	}

	return 0;
}

int Query::selectParse(tokenizer &parser) {
	// Get filter
	token tok = parser.next_token(false);
	if (tok.text != "*") {
		while (!parser.end()) {
			selectFilter_.push_back(tok.text);
			tok = parser.peek_token();
			if (tok.text != ",") break;
			parser.next_token();
			tok = parser.next_token(false);
		}
	}

	if (parser.next_token().text != "from") throw Error(errParams, "Expected 'FROM', but found '%s' in query", tok.text.c_str());

	_namespace = parser.next_token().text;
	parser.skip_space();

	while (!parser.end()) {
		tok = parser.next_token();
		if (tok.text == "where") {
			ParseWhere(parser);
		} else if (tok.text == "limit") {
			tok = parser.next_token();
			if (tok.type != TokenNumber) return -1;
			count = stoi(tok.text);
		} else if (tok.text == "offset") {
			tok = parser.next_token();
			if (tok.type != TokenNumber) return -1;
			start = stoi(tok.text);
		} else if (tok.text == "order") {
			// Just skip token (BY)
			parser.next_token();
			tok = parser.next_token(false);
			if (tok.type != TokenName) throw Error(errParseSQL, "Expected name, but found '%s' in query", tok.text.c_str());
			sortBy = tok.text;
			tok = parser.peek_token();
			if (tok.text == "asc" || tok.text == "desc") {
				sortDirDesc = bool(tok.text == "desc");
				parser.next_token();
			}
		} else {
			throw Error(errParseSQL, "Unexpected '%s' in query", tok.text.c_str());
		}
	}

	return 0;
}

int Query::describeParse(tokenizer &parser) {
	// Get namespaces
	token tok = parser.next_token(false);
	parser.skip_space();

	if (tok.text != "*") {
		for (;;) {
			namespacesNames_.push_back(tok.text);
			tok = parser.peek_token();
			if (tok.text != ",") {
				token nextTok = parser.next_token(false);
				if (nextTok.text.length()) {
					throw Error(errParseSQL, "Unexpected '%s' in query", tok.text.c_str());
				}
				break;
			}

			parser.next_token();
			tok = parser.next_token(false);
			if (parser.end()) {
				namespacesNames_.push_back(tok.text);
				break;
			}
		}
	}
	describe = true;

	return 0;
}

string Query::DumpMerged() const {
	string ret;
	for (auto &me : mergeQueries_) {
		if (me.joinType == JoinType::Merge) {
			ret += "Merge ";
		} else {
			ret += "Wrong Merge Type";
		}

		ret += me.QueryWhere::toString();
	}

	return ret;
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
		for (auto &kv : qe.values) ser.PutValue(kv);
	}

	for (auto &agg : aggregations_) {
		ser.PutVarUint(QueryAggregation);
		ser.PutVString(agg.index_);
		ser.PutVarUint(agg.type_);
	}

	if (!sortBy.empty()) {
		ser.PutVarUint(QuerySortIndex);
		ser.PutVString(sortBy);
		ser.PutVarUint(sortDirDesc);
		int cnt = forcedSortOrder.size();
		ser.PutVarUint(cnt);
		for (auto &kv : forcedSortOrder) ser.PutValue(kv);
	}

	for (auto &qje : joinEntries_) {
		ser.PutVarUint(QueryJoinOn);
		ser.PutVarUint(qje.op_);
		ser.PutVarUint(qje.condition_);
		ser.PutVString(qje.index_);
		ser.PutVString(qje.joinIndex_);
	}

	ser.PutVarUint(QueryDebugLevel);
	ser.PutVarUint(debugLevel);

	if (!(mode & SkipLimitOffset)) {
		if (count) {
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
			mq.Serialize(ser);
		}
	}
}

void Query::Deserialize(Serializer &ser) {
	_namespace = ser.GetVString().ToString();
	deserialize(ser);

	while (!ser.Eof()) {
		auto joinType = JoinType(ser.GetVarUint());
		Query q1(ser.GetVString().ToString());
		q1.joinType = joinType;
		q1.deserialize(ser);
		q1.debugLevel = debugLevel;
		if (joinType == JoinType::Merge) {
			mergeQueries_.push_back(q1);
		} else {
			joinQueries_.push_back(q1);
		}
	}
}

string Query::DumpJoined() const {
	extern const char *condNames[];
	string ret;
	for (auto &je : joinQueries_) {
		switch (je.joinType) {
			case JoinType::InnerJoin:
				ret += "INNER JOIN ";
				break;
			case JoinType::OrInnerJoin:
				ret += "OR INNER JOIN ";
				break;
			case JoinType::LeftJoin:
				ret += "LEFT JOIN ";
				break;
			case JoinType::Merge:
				break;
		}
		ret += je._namespace + " ON ";
		for (auto &e : je.joinEntries_) {
			if (&e != &*je.joinEntries_.begin()) ret += "AND ";
			ret += je._namespace + "." + e.joinIndex_ + " " + condNames[e.condition_] + " " + _namespace + "." + e.index_ + " ";
		}
		ret += je.QueryWhere::toString();
	}

	return ret;
}

string Query::Dump() const {
	string lim, filt;
	if (start != 0) lim += "OFFSET " + std::to_string(start) + " ";
	if (count != UINT_MAX) lim += "LIMIT " + std::to_string(count);

	if (aggregations_.size()) {
		for (auto &a : aggregations_) {
			if (&a != &*aggregations_.begin()) filt += ",";
			switch (a.type_) {
				case AggAvg:
					filt += "AVG(";
					break;
				case AggSum:
					filt += "SUM(";
					break;
				default:
					filt += "<?> (";
					break;
			}
			filt += a.index_ + ")";
		}
	} else if (selectFilter_.size()) {
		for (auto &f : selectFilter_) {
			if (&f != &*selectFilter_.begin()) filt += ",";
			filt += f;
		}
	} else
		filt = "*";

	const int bufSize = 4096;
	char buf[bufSize];
	snprintf(buf, bufSize, "SELECT %s FROM %s %s%s%s%s%s%s%s", filt.c_str(), _namespace.c_str(), QueryWhere::toString().c_str(),
			 DumpJoined().c_str(), DumpMerged().c_str(), sortBy.length() ? (string("ORDER BY ") + sortBy).c_str() : "",
			 sortDirDesc ? " DESC " : "", lim.c_str(), calcTotal ? " REQTOTAL " : "");

	return string(buf);
}

}  // namespace reindexer
