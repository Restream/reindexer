#include "core/query/sql/sqlencoder.h"

#include "core/keyvalue/geometry.h"
#include "core/keyvalue/p_string.h"
#include "core/queryresults/aggregationresult.h"
#include "core/sorting/sortexpression.h"
#include "core/type_consts_helpers.h"
#include "tools/logger.h"
#include "tools/serializer.h"

enum class NeedQuote : bool { No = false, Yes = true };

template <NeedQuote needQuote>
static void indexToSql(std::string_view index, reindexer::WrSerializer &ser) {
	if (needQuote == NeedQuote::No || index.find('+') == std::string::npos) {
		ser << index;
	} else {
		ser << '"' << index << '"';
	}
}

static reindexer::WrSerializer &stringToSql(std::string_view str, reindexer::WrSerializer &ser) {
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

namespace reindexer {

void SQLEncoder::DumpSingleJoinQuery(size_t idx, WrSerializer &ser, bool stripArgs) const {
	assertrx(idx < query_.GetJoinQueries().size());
	const auto &jq = query_.GetJoinQueries()[idx];
	ser << jq.joinType;
	if (jq.Entries().Empty() && !jq.HasLimit() && jq.sortingEntries_.empty()) {
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
	for (size_t i = 0; i < query_.GetJoinQueries().size(); ++i) {
		if (query_.GetJoinQueries()[i].joinType == JoinType::LeftJoin) {
			ser << ' ';
			DumpSingleJoinQuery(i, ser, stripArgs);
		}
	}
}

void SQLEncoder::dumpMerged(WrSerializer &ser, bool stripArgs) const {
	for (auto &me : query_.GetMergeQueries()) {
		ser << ' ' << me.joinType << "( ";
		me.GetSQL(ser, stripArgs);
		ser << ')';
	}
}

static std::string escapeQuotes(std::string str) {
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
		if (query_.forcedSortOrder_.empty() || i != 0) {
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
			if (query_.IsLocal()) {
				ser << "LOCAL ";
			}
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
				if (query_.SelectFilters().empty()) {
					if (query_.Limit() != 0 || !query_.HasCalcTotal()) {
						if (needComma) ser << ", ";
						ser << '*';
						if (query_.HasCalcTotal()) {
							needComma = true;
						}
					}
				} else {
					for (const auto &filter : query_.SelectFilters()) {
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
			if (query_.HasCalcTotal()) {
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

template <NeedQuote needQuote>
static void dumpCondWithValues(WrSerializer &ser, std::string_view fieldName, CondType cond, const VariantArray &values, bool stripArgs) {
	switch (cond) {
		case CondDWithin:
			ser << "ST_DWithin(";
			indexToSql<needQuote>(fieldName, ser);
			if (stripArgs) {
				ser << ", ?, ?)";
			} else {
				assertrx(values.size() == 2);
				Point point;
				double distance;
				if (values[0].Type().Is<KeyValueType::Tuple>()) {
					point = static_cast<Point>(values[0]);
					distance = values[1].As<double>();
				} else {
					point = static_cast<Point>(values[1]);
					distance = values[0].As<double>();
				}
				ser << ", ST_GeomFromText('POINT(" << point.X() << ' ' << point.Y() << ")'), " << distance << ')';
			}
			break;
		case CondAny:
		case CondEmpty:
			indexToSql<needQuote>(fieldName, ser);
			ser << ' ' << cond;
			break;
		case CondGt:
		case CondGe:
		case CondLt:
		case CondLe:
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondRange:
		case CondLike:
			indexToSql<needQuote>(fieldName, ser);
			ser << ' ' << cond << ' ';
			if (stripArgs) {
				ser << '?';
			} else {
				if (values.size() != 1) ser << '(';
				for (auto &v : values) {
					if (&v != &values[0]) ser << ',';
					v.Type().EvaluateOneOf(overloaded{
						[&](KeyValueType::String) { stringToSql(v.As<p_string>(), ser); },
						[&](KeyValueType::Uuid) { ser << '\'' << v.As<std::string>() << '\''; },
						[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Null,
								  KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined>) { ser << v.As<std::string>(); }});
				}
				if (values.size() != 1) ser << ")";
			}
	}
}

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
			[&ser](const AlwaysTrue &) {
				logPrintf(LogTrace, "Not normalized query to dsl");
				ser << "true";
			},
			[&ser](const AlwaysFalse &) {
				logPrintf(LogTrace, "Not normalized query to dsl");
				ser << "false";
			},
			[&](const SubQueryEntry &sqe) {
				if (encodedEntries) {
					ser << opNames[op] << ' ';
				}
				dumpCondWithValues<NeedQuote::No>(ser, '(' + query_.GetSubQuery(sqe.QueryIndex()).GetSQL(stripArgs) + ')', sqe.Condition(),
												  sqe.Values(), stripArgs);
			},
			[&](const SubQueryFieldEntry &sqe) {
				if (encodedEntries) {
					ser << opNames[op] << ' ';
				}
				ser << sqe.FieldName() << ' ' << sqe.Condition() << " (";
				SQLEncoder{query_.GetSubQuery(sqe.QueryIndex())}.GetSQL(ser, stripArgs);
				ser << ')';
			},
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
				dumpCondWithValues<NeedQuote::Yes>(ser, entry.FieldName(), entry.Condition(), entry.Values(), stripArgs);
			},
			[&](const JoinQueryEntry &jqe) {
				if (encodedEntries && query_.GetJoinQueries()[jqe.joinIndex].joinType != JoinType::OrInnerJoin) {
					ser << opNames[op] << ' ';
				}
				SQLEncoder(query_).DumpSingleJoinQuery(jqe.joinIndex, ser, stripArgs);
			},
			[&](const BetweenFieldsQueryEntry &entry) {
				if (encodedEntries) {
					ser << opNames[op] << ' ';
				}
				indexToSql<NeedQuote::Yes>(entry.LeftFieldName(), ser);
				ser << ' ' << entry.Condition() << ' ';
				indexToSql<NeedQuote::Yes>(entry.RightFieldName(), ser);
			});
		++encodedEntries;
	}
}

void SQLEncoder::dumpSQLWhere(WrSerializer &ser, bool stripArgs) const {
	if (query_.Entries().Empty()) return;
	ser << " WHERE ";
	dumpWhereEntries(query_.Entries().cbegin(), query_.Entries().cend(), ser, stripArgs);
	dumpEqualPositions(ser, query_.Entries().equalPositions);
}

}  // namespace reindexer
