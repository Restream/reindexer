#include "core/query/sql/sqlencoder.h"

#include "core/keyvalue/geometry.h"
#include "core/keyvalue/p_string.h"
#include "core/queryresults/aggregationresult.h"
#include "core/type_consts_helpers.h"
#include "tools/serializer.h"

namespace reindexer {

static void indexToSql(const std::string &index, WrSerializer &ser) {
	if (index.find('+') == std::string::npos) {
		ser << index;
	} else {
		ser << '"' << index << '"';
	}
}

static WrSerializer &stringToSql(std::string_view str, WrSerializer &ser) {
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
void SQLEncoder::DumpSingleJoinQuery(size_t idx, WrSerializer &ser, bool stripArgs) const {
	assertrx(idx < query_.joinQueries_.size());
	const auto &jq = query_.joinQueries_[idx];
	ser << ' ' << jq.joinType;
	if (jq.entries.Empty() && !jq.HasLimit() && jq.sortingEntries_.empty()) {
		ser << ' ' << jq.NsName() << " ON ";
	} else {
		ser << " (";
		jq.GetSQL(ser, stripArgs);
		ser << ") ON ";
	}
	if (jq.joinEntries_.size() != 1) ser << "(";
	for (auto &e : jq.joinEntries_) {
		if (&e != &*jq.joinEntries_.begin()) {
			ser << ' ' << e.Operation() << ' ';
		}
		if (e.ReverseNamespacesOrder()) {
			ser << jq.NsName() << '.' << e.RightFieldName() << ' ' << InvertJoinCondition(e.Condition()) << ' ' << query_.NsName() << '.'
				<< e.LeftFieldName();
		} else {
			ser << query_.NsName() << '.' << e.LeftFieldName() << ' ' << e.Condition() << ' ' << jq.NsName() << '.' << e.RightFieldName();
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
		ser << ' ' << me.joinType << "( ";
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
	switch (realQueryType_) {
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
					if (query_.Limit() != 0 || query_.CalcTotal() == ModeNoTotal) {
						if (needComma) ser << ", ";
						ser << '*';
						if (query_.CalcTotal() != ModeNoTotal) {
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
			if (query_.CalcTotal() != ModeNoTotal) {
				if (needComma) ser << ", ";
				if (query_.CalcTotal() == ModeAccurateTotal) ser << "COUNT(*)";
				if (query_.CalcTotal() == ModeCachedTotal) ser << "COUNT_CACHED(*)";
			}
			ser << " FROM " << query_.NsName();
		} break;
		case QueryDelete:
			ser << "DELETE FROM " << query_.NsName();
			break;
		case QueryUpdate: {
			if (query_.UpdateFields().empty()) break;
			ser << "UPDATE " << query_.NsName();
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
						v.Type().EvaluateOneOf(overloaded{
							[&](KeyValueType::String) {
								if (!field.IsExpression() && mode != FieldModeSetJson) {
									stringToSql(v.As<p_string>(), ser);
								} else {
									ser << v.As<std::string>();
								}
							},
							[&](KeyValueType::Uuid) { ser << '\'' << v.As<std::string>() << '\''; },
							[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Null,
									  KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined>) {
								ser << v.As<std::string>();
							}});
					}
					if (isArray) ser << "]";
				}
			}
			break;
		}
		case QueryTruncate:
			ser << "TRUNCATE " << query_.NsName();
			break;
		default:
			throw Error(errParams, "Not implemented");
	}

	dumpSQLWhere(ser, stripArgs);
	dumpJoined(ser, stripArgs);
	dumpMerged(ser, stripArgs);
	dumpOrderBy(ser, stripArgs);

	if (query_.HasOffset() && !stripArgs) ser << " OFFSET " << query_.Offset();
	if (query_.HasLimit() && !stripArgs) ser << " LIMIT " << query_.Limit();
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
				if (entry.Condition() == CondDWithin) {
					ser << "ST_DWithin(";
					indexToSql(entry.FieldName(), ser);
					if (stripArgs) {
						ser << ", ?, ?)";
					} else {
						assertrx(entry.Values().size() == 2);
						Point point;
						double distance;
						if (entry.Values()[0].Type().Is<KeyValueType::Tuple>()) {
							point = static_cast<Point>(entry.Values()[0]);
							distance = entry.Values()[1].As<double>();
						} else {
							point = static_cast<Point>(entry.Values()[1]);
							distance = entry.Values()[0].As<double>();
						}
						ser << ", ST_GeomFromText('POINT(" << point.X() << ' ' << point.Y() << ")'), " << distance << ')';
					}
				} else {
					indexToSql(entry.FieldName(), ser);
					ser << ' ' << entry.Condition() << ' ';
					if (entry.Condition() == CondEmpty || entry.Condition() == CondAny) {
					} else if (stripArgs) {
						ser << '?';
					} else {
						if (entry.Values().size() != 1) ser << '(';
						for (auto &v : entry.Values()) {
							if (&v != &entry.Values()[0]) ser << ',';
							v.Type().EvaluateOneOf(overloaded{
								[&](KeyValueType::String) { stringToSql(v.As<p_string>(), ser); },
								[&](KeyValueType::Uuid) { ser << '\'' << v.As<std::string>() << '\''; },
								[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
										  KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined>) {
									ser << v.As<std::string>();
								}});
						}
						if (entry.Values().size() != 1) ser << ")";
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
				indexToSql(entry.LeftFieldName(), ser);
				ser << ' ' << entry.Condition() << ' ';
				indexToSql(entry.RightFieldName(), ser);
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
