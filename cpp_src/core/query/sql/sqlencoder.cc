#include "core/query/sql/sqlencoder.h"
#include "core/keyvalue/geometry.h"
#include "core/nsselecter/sortexpression.h"
#include "core/queryresults/aggregationresult.h"
#include "core/type_consts_helpers.h"
#include "tools/serializer.h"

namespace reindexer {

const char *SQLEncoder::JoinTypeName(JoinType type) {
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

static void indexToSql(const std::string &index, WrSerializer &ser) {
	if (index.find('+') == std::string::npos) {
		ser << index;
	} else {
		ser << '"' << index << '"';
	}
}

static WrSerializer &stringToSql(const std::string &str, WrSerializer &ser) {
	ser << '\'';
	for (auto c : str) {
		switch (c) {
			case '\'':
				ser << "\\'";
				break;
			case '\"':
				ser << "\\\"";
				break;
			case '\n':
				ser << "\\n";
				break;
			case '\r':
				ser << "\\r";
				break;
			case '\b':
				ser << "\\b";
				break;
			case '\t':
				ser << "\\t";
				break;
			case '\f':
				ser << "\\f";
				break;
			default:
				ser << c;
		}
	}
	ser << '\'';
	return ser;
}

SQLEncoder::SQLEncoder(const Query &q) : query_(q) {}

void SQLEncoder::DumpSingleJoinQuery(size_t idx, WrSerializer &ser, bool stripArgs) const {
	assertrx(idx < query_.joinQueries_.size());
	const auto &jq = query_.joinQueries_[idx];
	ser << ' ' << JoinTypeName(jq.joinType);
	if (jq.entries.Empty() && jq.count == QueryEntry::kDefaultLimit && jq.sortingEntries_.empty()) {
		ser << ' ' << jq._namespace << " ON ";
	} else {
		ser << " (";
		jq.GetSQL(ser, stripArgs);
		ser << ") ON ";
	}
	if (jq.joinEntries_.size() != 1) ser << "(";
	for (auto &e : jq.joinEntries_) {
		if (&e != &*jq.joinEntries_.begin()) {
			ser << ' ' << e.op_ << ' ';
		}
		if (e.reverseNamespacesOrder) {
			ser << jq._namespace << '.' << e.joinIndex_ << ' ' << InvertJoinCondition(e.condition_) << ' ' << query_._namespace << '.'
				<< e.index_;
		} else {
			ser << query_._namespace << '.' << e.index_ << ' ' << e.condition_ << ' ' << jq._namespace << '.' << e.joinIndex_;
		}
	}
	if (jq.joinEntries_.size() != 1) ser << ')';
}

void SQLEncoder::dumpJoined(WrSerializer &ser, bool stripArgs) const {
	for (size_t i = 0; i < query_.joinQueries_.size(); ++i) {
		if (query_.joinQueries_[i].joinType == JoinType::LeftJoin) {
			DumpSingleJoinQuery(i, ser, stripArgs);
		}
	}
}

void SQLEncoder::dumpMerged(WrSerializer &ser, bool stripArgs) const {
	for (auto &me : query_.mergeQueries_) {
		ser << ' ' << JoinTypeName(me.joinType) << "( ";
		me.GetSQL(ser, stripArgs);
		ser << ')';
	}
}

std::string escapeQuotes(std::string str) {
	for (size_t i = 0; i < str.size(); ++i) {
		if (str[i] == '\'' && (i == 0 || str[i - 1] != '\\')) str.insert(i++, 1, '\\');
	}
	return str;
}

void SQLEncoder::dumpOrderBy(WrSerializer &ser, bool stripArgs) const {
	if (query_.sortingEntries_.empty()) return;
	ser << " ORDER BY ";
	for (size_t i = 0; i < query_.sortingEntries_.size(); ++i) {
		const SortingEntry &sortingEntry(query_.sortingEntries_[i]);
		if (query_.forcedSortOrder_.empty()) {
			ser << '\'' << escapeQuotes(sortingEntry.expression) << '\'';
		} else {
			ser << "FIELD(" << sortingEntry.expression;
			if (stripArgs) {
				ser << '?';
			} else {
				for (auto &v : query_.forcedSortOrder_) {
					ser << ", ";
					v.Dump(ser);
				}
			}
			ser << ")";
		}
		ser << (sortingEntry.desc ? " DESC" : "");
		if (i != query_.sortingEntries_.size() - 1) ser << ", ";
	}
}

void SQLEncoder::dumpEqualPositions(WrSerializer &ser, const EqualPositions_t &equalPositions) const {
	for (const auto &ep : equalPositions) {
		assertrx(ep.size() > 1);
		ser << " equal_position(";
		for (size_t i = 0; i < ep.size(); ++i) {
			if (i != 0) ser << ", ";
			ser << ep[i];
		}
		ser << ")";
	}
}

WrSerializer &SQLEncoder::GetSQL(WrSerializer &ser, bool stripArgs) const {
	switch (query_.type_) {
		case QuerySelect: {
			ser << "SELECT ";
			bool needComma = false;
			if (query_.IsWithRank()) {
				ser << "RANK()";
				needComma = true;
			}
			for (const auto &a : query_.aggregations_) {
				if (needComma) {
					ser << ", ";
				} else {
					needComma = true;
				}
				ser << AggTypeToStr(a.Type()) << "(";
				for (const auto &f : a.Fields()) {
					if (&f != &*a.Fields().begin()) ser << ", ";
					ser << f;
				}
				for (const auto &se : a.Sorting()) {
					ser << " ORDER BY " << '\'' << escapeQuotes(se.expression) << '\'' << (se.desc ? " DESC" : " ASC");
				}

				if (a.Offset() != QueryEntry::kDefaultOffset && !stripArgs) ser << " OFFSET " << a.Offset();
				if (a.Limit() != QueryEntry::kDefaultLimit && !stripArgs) ser << " LIMIT " << a.Limit();
				ser << ')';
			}
			if (query_.aggregations_.empty() || (query_.aggregations_.size() == 1 && query_.aggregations_[0].Type() == AggDistinct)) {
				std::string distinctIndex;
				if (!query_.aggregations_.empty()) {
					assertrx(query_.aggregations_[0].Fields().size() == 1);
					distinctIndex = query_.aggregations_[0].Fields()[0];
				}
				if (query_.selectFilter_.empty()) {
					if (query_.count != 0 || query_.calcTotal == ModeNoTotal) {
						if (needComma) ser << ", ";
						ser << '*';
						if (query_.calcTotal != ModeNoTotal) {
							needComma = true;
						}
					}
				} else {
					for (const auto &filter : query_.selectFilter_) {
						if (filter == distinctIndex) continue;
						if (needComma) {
							ser << ", ";
						} else {
							needComma = true;
						}
						ser << filter;
					}
				}
			}
			if (query_.calcTotal != ModeNoTotal) {
				if (needComma) ser << ", ";
				if (query_.calcTotal == ModeAccurateTotal) ser << "COUNT(*)";
				if (query_.calcTotal == ModeCachedTotal) ser << "COUNT_CACHED(*)";
			}
			ser << " FROM " << query_._namespace;
		} break;
		case QueryDelete:
			ser << "DELETE FROM " << query_._namespace;
			break;
		case QueryUpdate: {
			if (query_.UpdateFields().empty()) break;
			ser << "UPDATE " << query_._namespace;
			FieldModifyMode mode = query_.UpdateFields().front().Mode();
			bool isUpdate = (mode == FieldModeSet || mode == FieldModeSetJson);
			if (isUpdate) {
				ser << " SET ";
			} else {
				ser << " DROP ";
			}
			for (const UpdateEntry &field : query_.UpdateFields()) {
				if (&field != &*query_.UpdateFields().begin()) ser << ',';
				ser << field.Column();
				if (isUpdate) {
					ser << " = ";
					bool isArray = (field.Values().IsArrayValue() || field.Values().size() > 1);
					if (isArray) ser << '[';
					for (const Variant &v : field.Values()) {
						if (&v != &*field.Values().begin()) ser << ',';
						if (v.Type().Is<KeyValueType::String>() && !field.IsExpression() && (mode != FieldModeSetJson)) {
							stringToSql(v.As<std::string>(), ser);
						} else {
							ser << v.As<std::string>();
						}
					}
					if (isArray) ser << "]";
				}
			}
			break;
		}
		case QueryTruncate:
			ser << "TRUNCATE " << query_._namespace;
			break;
		default:
			throw Error(errParams, "Not implemented");
	}

	dumpSQLWhere(ser, stripArgs);
	dumpJoined(ser, stripArgs);
	dumpMerged(ser, stripArgs);
	dumpOrderBy(ser, stripArgs);

	if (query_.start != QueryEntry::kDefaultOffset && !stripArgs) ser << " OFFSET " << query_.start;
	if (query_.count != QueryEntry::kDefaultLimit && !stripArgs) ser << " LIMIT " << query_.count;
	return ser;
}

static const char *opNames[] = {"-", "OR", "AND", "AND NOT"};

void SQLEncoder::dumpWhereEntries(QueryEntries::const_iterator from, QueryEntries::const_iterator to, WrSerializer &ser,
								  bool stripArgs) const {
	int encodedEntries = 0;
	for (auto it = from; it != to; ++it) {
		const OpType op = it->operation;
		if (encodedEntries) {
			ser << ' ';
		} else if (op == OpNot) {
			ser << "NOT ";
		}
		it->InvokeAppropriate<void>(
			Skip<AlwaysFalse>{},
			[&](const QueryEntriesBracket &bracket) {
				if (encodedEntries) {
					ser << opNames[op] << ' ';
				}
				ser << '(';
				dumpWhereEntries(it.cbegin(), it.cend(), ser, stripArgs);
				dumpEqualPositions(ser, bracket.equalPositions);
				ser << ')';
			},
			[&](const QueryEntry &entry) {
				if (encodedEntries) {
					ser << opNames[op] << ' ';
				}
				if (entry.condition == CondDWithin) {
					ser << "ST_DWithin(";
					indexToSql(entry.index, ser);
					if (stripArgs) {
						ser << ", ?, ?)";
					} else {
						assertrx(entry.values.size() == 2);
						Point point;
						double distance;
						if (entry.values[0].Type().Is<KeyValueType::Tuple>()) {
							point = static_cast<Point>(entry.values[0]);
							distance = entry.values[1].As<double>();
						} else {
							point = static_cast<Point>(entry.values[1]);
							distance = entry.values[0].As<double>();
						}
						ser << ", ST_GeomFromText('POINT(" << point.X() << ' ' << point.Y() << ")'), " << distance << ')';
					}
				} else {
					indexToSql(entry.index, ser);
					ser << ' ' << entry.condition << ' ';
					if (entry.condition == CondEmpty || entry.condition == CondAny) {
					} else if (stripArgs) {
						ser << '?';
					} else {
						if (entry.values.size() != 1) ser << '(';
						for (auto &v : entry.values) {
							if (&v != &entry.values[0]) ser << ',';
							if (v.Type().Is<KeyValueType::String>()) {
								stringToSql(v.As<std::string>(), ser);
							} else {
								ser << v.As<std::string>();
							}
						}
						if (entry.values.size() != 1) ser << ")";
					}
				}
			},
			[&](const JoinQueryEntry &jqe) {
				if (encodedEntries && query_.joinQueries_[jqe.joinIndex].joinType != JoinType::OrInnerJoin) {
					ser << opNames[op] << ' ';
				}
				SQLEncoder(query_).DumpSingleJoinQuery(jqe.joinIndex, ser, stripArgs);
			},
			[&ser](const BetweenFieldsQueryEntry &entry) {
				indexToSql(entry.firstIndex, ser);
				ser << ' ' << entry.Condition() << ' ';
				indexToSql(entry.secondIndex, ser);
			});
		++encodedEntries;
	}
}

void SQLEncoder::dumpSQLWhere(WrSerializer &ser, bool stripArgs) const {
	if (query_.entries.Empty()) return;
	ser << " WHERE ";
	dumpWhereEntries(query_.entries.cbegin(), query_.entries.cend(), ser, stripArgs);
	dumpEqualPositions(ser, query_.entries.equalPositions);
}

}  // namespace reindexer
