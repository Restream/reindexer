#include "core/query/sql/sqlencoder.h"
#include "core/nsselecter/sortexpression.h"
#include "core/queryresults/aggregationresult.h"
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

SQLEncoder::SQLEncoder(const Query &q) : query_(q) {}

extern const char *condNames[];

void SQLEncoder::DumpSingleJoinQuery(size_t idx, WrSerializer &ser, bool stripArgs) const {
	assert(idx < query_.joinQueries_.size());
	const auto &jq = query_.joinQueries_[idx];
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
		ser << jq._namespace << '.' << e.joinIndex_ << ' ' << condNames[e.condition_] << ' ' << query_._namespace << '.' << e.index_;
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

void SQLEncoder::dumpOrderBy(WrSerializer &ser, bool stripArgs) const {
	if (query_.sortingEntries_.empty()) return;
	ser << " ORDER BY ";
	for (size_t i = 0; i < query_.sortingEntries_.size(); ++i) {
		const SortingEntry &sortingEntry(query_.sortingEntries_[i]);
		if (query_.forcedSortOrder_.empty()) {
			ser << '\'' << sortingEntry.expression << '\'';
		} else {
			ser << "FIELD(" << sortingEntry.expression;
			if (stripArgs) {
				ser << '?';
			} else {
				for (auto &v : query_.forcedSortOrder_) {
					ser << ", '" << v.As<string>() << "'";
				}
			}
			ser << ")";
		}
		ser << (sortingEntry.desc ? " DESC" : "");
		if (i != query_.sortingEntries_.size() - 1) ser << ", ";
	}
}

void SQLEncoder::dumpEqualPositions(WrSerializer &ser, int parenthesisIndex) const {
	if (query_.equalPositions_.empty()) return;
	auto range = query_.equalPositions_.equal_range(parenthesisIndex);
	if (range.first == query_.equalPositions_.end()) return;
	for (auto it = range.first; it != range.second; ++it) {
		assert(it->second.size() > 0);
		ser << " equal_position(";
		for (size_t i = 0; i < it->second.size(); ++i) {
			if (i != 0) ser << ",";
			assert(query_.entries.IsValue(it->second[i]));
			ser << query_.entries[it->second[i]].index;
		}
		ser << ")";
	}
}

WrSerializer &SQLEncoder::GetSQL(WrSerializer &ser, bool stripArgs) const {
	switch (query_.type_) {
		case QuerySelect: {
			ser << "SELECT ";
			vector<int> distinctIndices;
			for (size_t i = 0; i != query_.entries.Size(); ++i) {
				if (query_.entries.IsValue(i) && query_.entries[i].distinct) {
					distinctIndices.emplace_back(i);
				}
			}
			for (size_t i = 0; i < distinctIndices.size(); ++i) {
				assert(size_t(distinctIndices[i]) < query_.entries.Size());
				if (i != 0) ser << ",";
				ser << "DISTINCT(" << query_.entries[distinctIndices[i]].index << ")";
			}
			if (query_.aggregations_.size()) {
				if (distinctIndices.size() > 0) ser << ",";
				for (const auto &a : query_.aggregations_) {
					if (&a != &*query_.aggregations_.begin()) ser << ',';
					ser << AggregationResult::aggTypeToStr(a.type_) << "(";
					for (const auto &f : a.fields_) {
						if (&f != &*a.fields_.begin()) ser << ',';
						ser << f;
					}
					for (const auto &se : a.sortingEntries_) {
						ser << " ORDER BY " << '\'' << se.expression << '\'' << (se.desc ? " DESC" : " ASC");
					}

					if (a.offset_ != 0 && !stripArgs) ser << " OFFSET " << a.offset_;
					if (a.limit_ != UINT_MAX && !stripArgs) ser << " LIMIT " << a.limit_;
					ser << ')';
				}
			} else if (query_.selectFilter_.size()) {
				for (size_t i = 0; i < query_.selectFilter_.size(); ++i) {
					bool isDistinct = false;
					for (size_t j = 0; j < distinctIndices.size(); ++j) {
						if (query_.selectFilter_[i] == query_.entries[j].index) {
							isDistinct = true;
							break;
						}
					}
					if (isDistinct) continue;
					if (i != 0 || distinctIndices.size() > 0) {
						ser << ',';
					}
					ser << query_.selectFilter_[i];
				}
			} else {
				if (distinctIndices.size() > 0) ser << ",";
				ser << '*';
			}
			if (query_.calcTotal == ModeAccurateTotal) ser << ", COUNT(*)";
			if (query_.calcTotal == ModeCachedTotal) ser << ", COUNT_CACHED(*)";
			ser << " FROM " << query_._namespace;
		} break;
		case QueryDelete:
			ser << "DELETE FROM " << query_._namespace;
			break;
		case QueryUpdate: {
			if (query_.updateFields_.empty()) break;
			ser << "UPDATE " << query_._namespace;
			bool isUpdate = (query_.updateFields_.front().mode == FieldModeSet);
			if (isUpdate) {
				ser << " SET ";
			} else {
				ser << " DROP ";
			}
			for (const UpdateEntry &field : query_.updateFields_) {
				if (&field != &*query_.updateFields_.begin()) ser << ',';
				if (field.column.find('.') == string::npos) {
					ser << field.column;
				} else {
					ser << "'" << field.column << "'";
				}
				if (isUpdate) {
					ser << " = ";
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

	if (query_.start != 0 && !stripArgs) ser << " OFFSET " << query_.start;
	if (query_.count != UINT_MAX && !stripArgs) ser << " LIMIT " << query_.count;
	return ser;
}

extern const char *condNames[];
const char *opNames[] = {"-", "OR", "AND", "AND NOT"};

void SQLEncoder::dumpWhereEntries(QueryEntries::const_iterator from, QueryEntries::const_iterator to, WrSerializer &ser,
								  bool stripArgs) const {
	int encodedEntries = 0;
	for (auto it = from; it != to; ++it) {
		bool isLeaf = it->IsLeaf();
		if (isLeaf && it->Value().distinct) continue;
		if (encodedEntries++) {
			bool orInnerJoin = false;
			if (it->IsLeaf() && (it->Value().joinIndex != QueryEntry::kNoJoins)) {
				if (query_.joinQueries_[it->Value().joinIndex].joinType == JoinType::OrInnerJoin) {
					orInnerJoin = true;
				}
			}
			ser << ' ';
			if (!orInnerJoin) ser << opNames[it->operation] << ' ';	 // -V547}
		} else if (it->operation == OpNot) {
			ser << "NOT ";
		}
		if (!isLeaf) {
			ser << '(';
			dumpWhereEntries(it.cbegin(), it.cend(), ser, stripArgs);
			ser << ')';
		} else {
			const QueryEntry &entry = it->Value();
			if (entry.joinIndex == QueryEntry::kNoJoins) {
				if (entry.index.find('.') == string::npos && entry.index.find('+') == string::npos)
					ser << entry.index << ' ';
				else
					ser << '\'' << entry.index << "\' ";

				ser << condNames[entry.condition] << ' ';
				if (entry.condition == CondEmpty || entry.condition == CondAny) {
				} else if (stripArgs) {
					ser << '?';
				} else {
					if (entry.values.size() != 1) ser << '(';
					for (auto &v : entry.values) {
						if (&v != &entry.values[0]) ser << ',';
						if (v.Type() == KeyValueString) {
							ser << '\'' << v.As<string>() << '\'';
						} else {
							ser << v.As<string>();
						}
					}
					ser << ((entry.values.size() != 1) ? ")" : "");
				}
			} else {
				SQLEncoder(query_).DumpSingleJoinQuery(entry.joinIndex, ser, stripArgs);
			}
		}
	}
	int parenthesisIndex = (from.PlainIterator() - query_.entries.begin().PlainIterator());
	dumpEqualPositions(ser, parenthesisIndex);
}

void SQLEncoder::dumpSQLWhere(WrSerializer &ser, bool stripArgs) const {
	if (query_.entries.Empty()) return;
	ser << " WHERE ";
	dumpWhereEntries(query_.entries.cbegin(), query_.entries.cend(), ser, stripArgs);
}

}  // namespace reindexer
