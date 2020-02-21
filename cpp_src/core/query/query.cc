
#include "core/query/query.h"
#include "core/query/dsl/dslencoder.h"
#include "core/query/dsl/dslparser.h"
#include "core/query/sql/sqlencoder.h"
#include "core/query/sql/sqlparser.h"
#include "tools/serializer.h"

namespace reindexer {

Query::Query(const string &__namespace, unsigned _start, unsigned _count, CalcTotalMode _calcTotal)
	: _namespace(__namespace), start(_start), count(_count), calcTotal(_calcTotal) {}

bool Query::operator==(const Query &obj) const {
	if (entries != obj.entries) return false;
	if (aggregations_ != obj.aggregations_) return false;

	if (_namespace != obj._namespace) return false;
	if (sortingEntries_ != obj.sortingEntries_) return false;
	if (calcTotal != obj.calcTotal) return false;
	if (start != obj.start) return false;
	if (count != obj.count) return false;
	if (debugLevel != obj.debugLevel) return false;
	if (forcedSortOrder_ != obj.forcedSortOrder_) return false;

	if (selectFilter_ != obj.selectFilter_) return false;
	if (selectFunctions_ != obj.selectFunctions_) return false;
	if (joinQueries_ != obj.joinQueries_) return false;
	if (mergeQueries_ != obj.mergeQueries_) return false;
	if (updateFields_ != obj.updateFields_) return false;

	return true;
}

bool JoinedQuery::operator==(const JoinedQuery &obj) const {
	if (joinEntries_ != obj.joinEntries_) return false;
	if (joinType != obj.joinType) return false;
	return Query::operator==(obj);
}
void Query::FromSQL(const string_view &q) { SQLParser(*this).Parse(q); }

Error Query::FromJSON(const string &dsl) { return dsl::Parse(dsl, *this); }

string Query::GetJSON() const { return dsl::toDsl(*this); }

WrSerializer &Query::GetSQL(WrSerializer &ser, bool stripArgs) const { return SQLEncoder(*this).GetSQL(ser, stripArgs); }

string Query::GetSQL(bool stripArgs) const {
	WrSerializer ser;
	return string(GetSQL(ser, stripArgs).Slice());
}

void Query::deserialize(Serializer &ser, bool &hasJoinConditions) {
	while (!ser.Eof()) {
		QueryEntry qe;
		QueryJoinEntry qje;

		int qtype = ser.GetVarUint();
		switch (qtype) {
			case QueryCondition: {
				qe.index = string(ser.GetVString());
				OpType op = OpType(ser.GetVarUint());
				qe.condition = CondType(ser.GetVarUint());
				int cnt = ser.GetVarUint();
				qe.values.reserve(cnt);
				while (cnt--) qe.values.push_back(ser.GetVariant().EnsureHold());
				entries.Append(op, std::move(qe));
				break;
			}
			case QueryJoinCondition: {
				uint64_t type = ser.GetVarUint();
				assert(type != JoinType::LeftJoin);
				QueryEntry joinEntry(ser.GetVarUint());
				hasJoinConditions = true;
				entries.Append((type == JoinType::OrInnerJoin) ? OpOr : OpAnd, std::move(joinEntry));
				break;
			}
			case QueryAggregation: {
				AggregateEntry ae;
				ae.type_ = static_cast<AggType>(ser.GetVarUint());
				size_t fieldsCount = ser.GetVarUint();
				ae.fields_.reserve(fieldsCount);
				while (fieldsCount--) ae.fields_.push_back(string(ser.GetVString()));
				auto pos = ser.Pos();
				bool aggEnd = false;
				while (!ser.Eof() && !aggEnd) {
					int atype = ser.GetVarUint();
					switch (atype) {
						case QueryAggregationSort: {
							auto fieldName = ser.GetVString();
							ae.sortingEntries_.push_back({string(fieldName), ser.GetVarUint() != 0});
							break;
						}
						case QueryAggregationLimit:
							ae.limit_ = ser.GetVarUint();
							break;
						case QueryAggregationOffset:
							ae.offset_ = ser.GetVarUint();
							break;
						default:
							ser.SetPos(pos);
							aggEnd = true;
					}
					pos = ser.Pos();
				}
				aggregations_.push_back(std::move(ae));
				break;
			}
			case QueryDistinct:
				qe.index = string(ser.GetVString());
				if (!qe.index.empty()) {
					qe.distinct = true;
					qe.condition = CondAny;
					entries.Append(OpAnd, std::move(qe));
				}
				break;
			case QuerySortIndex: {
				SortingEntry sortingEntry;
				sortingEntry.expression = string(ser.GetVString());
				sortingEntry.desc = bool(ser.GetVarUint());
				if (sortingEntry.expression.length()) {
					sortingEntries_.push_back(std::move(sortingEntry));
				}
				int cnt = ser.GetVarUint();
				forcedSortOrder_.reserve(cnt);
				while (cnt--) forcedSortOrder_.push_back(ser.GetVariant().EnsureHold());
				break;
			}
			case QueryJoinOn:
				qje.op_ = OpType(ser.GetVarUint());
				qje.condition_ = CondType(ser.GetVarUint());
				qje.index_ = string(ser.GetVString());
				qje.joinIndex_ = string(ser.GetVString());
				reinterpret_cast<JoinedQuery *>(this)->joinEntries_.push_back(std::move(qje));
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
				selectFilter_.push_back(string(ser.GetVString()));
				break;
			case QueryEqualPosition: {
				const unsigned strt = ser.GetVarUint();
				vector<string> ep(ser.GetVarUint());
				for (size_t i = 0; i < ep.size(); ++i) ep[i] = string(ser.GetVString());
				equalPositions_.emplace(strt, entries.DetermineEqualPositionIndexes(strt, ep));
				break;
			}
			case QueryExplain:
				explain_ = true;
				break;
			case QuerySelectFunction:
				selectFunctions_.push_back(string(ser.GetVString()));
				break;
			case QueryDropField: {
				updateFields_.push_back({string(ser.GetVString()), {}});
				auto &field = updateFields_.back();
				field.mode = FieldModeDrop;
				break;
			}
			case QueryUpdateField: {
				updateFields_.push_back({string(ser.GetVString()), {}});
				auto &field = updateFields_.back();
				field.mode = FieldModeSet;
				int numValues = ser.GetVarUint();
				while (numValues--) {
					field.isExpression = ser.GetVarUint();
					field.values.push_back(ser.GetVariant());
				}
				break;
			}
			case QueryOpenBracket: {
				OpType op = OpType(ser.GetVarUint());
				entries.OpenBracket(op);
				break;
			}
			case QueryCloseBracket:
				entries.CloseBracket();
				break;
			case QueryEnd:
				return;
			default:
				throw Error(errParseBin, "Unknown type %d while parsing binary buffer", qtype);
		}
	}
	return;
}

void Query::Serialize(WrSerializer &ser, uint8_t mode) const {
	ser.PutVString(_namespace);
	entries.Serialize(ser);

	for (auto &agg : aggregations_) {
		ser.PutVarUint(QueryAggregation);
		ser.PutVarUint(agg.type_);
		ser.PutVarUint(agg.fields_.size());
		for (const auto &field : agg.fields_) {
			ser.PutVString(field);
		}
		for (const auto &se : agg.sortingEntries_) {
			ser.PutVarUint(QueryAggregationSort);
			ser.PutVString(se.expression);
			ser.PutVarUint(se.desc);
		}
		if (agg.limit_ != UINT_MAX) {
			ser.PutVarUint(QueryAggregationLimit);
			ser.PutVarUint(agg.limit_);
		}
		if (agg.offset_ != 0) {
			ser.PutVarUint(QueryAggregationOffset);
			ser.PutVarUint(agg.offset_);
		}
	}

	for (const SortingEntry &sortginEntry : sortingEntries_) {
		ser.PutVarUint(QuerySortIndex);
		ser.PutVString(sortginEntry.expression);
		ser.PutVarUint(sortginEntry.desc);
		int cnt = forcedSortOrder_.size();
		ser.PutVarUint(cnt);
		for (auto &kv : forcedSortOrder_) ser.PutVariant(kv);
	}

	if (mode & WithJoinEntries) {
		for (auto &qje : reinterpret_cast<const JoinedQuery *>(this)->joinEntries_) {
			ser.PutVarUint(QueryJoinOn);
			ser.PutVarUint(qje.op_);
			ser.PutVarUint(qje.condition_);
			ser.PutVString(qje.index_);
			ser.PutVString(qje.joinIndex_);
		}
	}

	for (const std::pair<unsigned, EqualPosition> &equalPoses : equalPositions_) {
		ser.PutVarUint(QueryEqualPosition);
		ser.PutVarUint(equalPoses.first);
		ser.PutVarUint(equalPoses.second.size());
		for (unsigned ep : equalPoses.second) ser.PutVString(entries[ep].index);
	}

	ser.PutVarUint(QueryDebugLevel);
	ser.PutVarUint(debugLevel);

	if (!(mode & SkipLimitOffset)) {
		if (HasLimit()) {
			ser.PutVarUint(QueryLimit);
			ser.PutVarUint(count);
		}
		if (HasOffset()) {
			ser.PutVarUint(QueryOffset);
			ser.PutVarUint(start);
		}
	}

	if (calcTotal != ModeNoTotal) {
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

	for (const UpdateEntry &field : updateFields_) {
		if (field.mode == FieldModeSet) {
			ser.PutVarUint(QueryUpdateField);
			ser.PutVString(field.column);
			ser.PutVarUint(field.values.size());
			for (const Variant &val : field.values) {
				ser.PutVarUint(field.isExpression);
				ser.PutVariant(val);
			}
		} else if (field.mode == FieldModeDrop) {
			ser.PutVarUint(QueryDropField);
			ser.PutVString(field.column);
		} else {
			throw Error(errLogic, "Unsupported item modification mode = %d", field.mode);
		}
	}

	ser.PutVarUint(QueryEnd);  // finita la commedia... of root query

	if (!(mode & SkipJoinQueries)) {
		for (auto &jq : joinQueries_) {
			ser.PutVarUint(static_cast<int>(jq.joinType));
			jq.Serialize(ser, WithJoinEntries);
		}
	}

	if (!(mode & SkipMergeQueries)) {
		for (auto &mq : mergeQueries_) {
			ser.PutVarUint(static_cast<int>(mq.joinType));
			mq.Serialize(ser, mode | WithJoinEntries);
		}
	}
}

void Query::Deserialize(Serializer &ser) {
	_namespace = string(ser.GetVString());
	bool hasJoinConditions = false;
	deserialize(ser, hasJoinConditions);

	bool nested = false;
	while (!ser.Eof()) {
		auto joinType = JoinType(ser.GetVarUint());
		JoinedQuery q1(string(ser.GetVString()));
		q1.joinType = joinType;
		q1.deserialize(ser, hasJoinConditions);
		q1.debugLevel = debugLevel;
		if (joinType == JoinType::Merge) {
			mergeQueries_.emplace_back(std::move(q1));
			nested = true;
		} else {
			Query &q = nested ? mergeQueries_.back() : *this;
			if (joinType != JoinType::LeftJoin && !hasJoinConditions) {
				int joinIdx = joinQueries_.size();
				entries.Append((joinType == JoinType::OrInnerJoin) ? OpOr : OpAnd, QueryEntry(joinIdx));
			}
			q.joinQueries_.emplace_back(std::move(q1));
		}
	}
}

Query &Query::Join(JoinType joinType, const string &index, const string &joinIndex, CondType cond, OpType op, Query &qr) {
	QueryJoinEntry joinEntry;
	joinEntry.op_ = op;
	joinEntry.condition_ = cond;
	joinEntry.index_ = index;
	joinEntry.joinIndex_ = joinIndex;
	joinQueries_.emplace_back(JoinedQuery(qr));
	joinQueries_.back().joinType = joinType;
	joinQueries_.back().joinEntries_.push_back(joinEntry);
	if (joinType != JoinType::LeftJoin) {
		entries.Append((joinType == JoinType::InnerJoin) ? OpType::OpAnd : OpType::OpOr, QueryEntry(joinQueries_.size() - 1));
	}
	return *this;
}

void Query::WalkNested(bool withSelf, bool withMerged, std::function<void(const Query &q)> visitor) const {
	if (withSelf) visitor(*this);
	if (withMerged)
		for (auto &mq : mergeQueries_) visitor(mq);
	for (auto &jq : joinQueries_) visitor(jq);
	for (auto &mq : mergeQueries_)
		for (auto &jq : mq.joinQueries_) visitor(jq);
}

}  // namespace reindexer
