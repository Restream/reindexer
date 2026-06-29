#include "sqlencoder.h"

#include "core/keyvalue/geometry.h"
#include "core/keyvalue/p_string.h"
#include "core/nsselecter/joins/helpers.h"
#include "core/queryresults/aggregationresult.h"
#include "core/type_consts.h"
#include "core/type_consts_helpers.h"
#include "sql_formatters.h"
#include "tools/logger.h"
#include "tools/serilize/wrserializer.h"

enum class [[nodiscard]] NeedQuote : bool { No = false, Yes = true };
enum class [[nodiscard]] FunctionAsString : bool { No = false, Yes = true };
constexpr static std::string_view kJoinNames[] = {"LEFT JOIN ", "INNER JOIN ", "INNER JOIN ", "MERGE "};

template <NeedQuote needQuote>
static void indexToSql(std::string_view index, reindexer::WrSerializer& ser) {
	if (needQuote == NeedQuote::No || index.find('+') == std::string::npos) {
		ser << index;
	} else {
		ser << '"' << index << '"';
	}
}

static reindexer::WrSerializer& stringToSql(std::string_view str, reindexer::WrSerializer& ser) {
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

template <typename Formatter>
void SQLEncoder<Formatter>::DumpSingleJoinQuery(size_t idx, bool stripArgs) const {
	WrSerializer& ser = formatter_.Serializer();
	if (idx >= query_.JoinQueries().size()) [[unlikely]] {
		throw Error(errParams, "Error during parsing query join entries: idx({}) >= query_.GetJoinQueries().size()({})", idx,
					query_.JoinQueries().size());
	}

	const auto& jq = query_.JoinQueries()[idx];
	ser << kJoinNames[jq.GetJoinType()];
	if (jq.Entries().Empty() && !jq.HasLimit() && jq.GetSortingEntries().empty()) {
		ser << jq.NsName() << ' ';
	} else {
		{
			const auto parenthesisGuard = formatter_.OpenParenthesis();
			SQLEncoder(jq, formatter_).DumpSQL(stripArgs);
		}
		formatter_.Next();
	}
	ser << "ON ";
	{
		const auto onGuard = formatter_.ConditionallyOpenParenthesis(jq.JoinEntries().size() != 1);
		bool needEncloseOR = false;
		for (size_t i = 1; i < jq.JoinEntries().size(); ++i) {
			if (jq.JoinEntries()[i].Operation() != OpOr) {
				needEncloseOR = true;
				break;
			}
		}
		auto conditionsFormatter = formatter_.StartConditions(needEncloseOR);
		for (size_t i = 0; i < jq.JoinEntries().size(); ++i) {
			const auto& e = jq.JoinEntries()[i];
			const bool isNextOr = i + 1 < jq.JoinEntries().size() && jq.JoinEntries()[i + 1].Operation() == OpOr;
			conditionsFormatter.AddCondition(e.Operation(), isNextOr);
			if (e.ReverseNamespacesOrder()) {
				ser << jq.NsName() << '.' << e.RightFieldName() << ' ' << joins::InvertJoinCondition(e.Condition()) << ' '
					<< query_.NsName() << '.' << e.LeftFieldName();
			} else {
				ser << query_.NsName() << '.' << e.LeftFieldName() << ' ' << e.Condition() << ' ' << jq.NsName() << '.'
					<< e.RightFieldName();
			}
		}
	}
}

template <typename Formatter>
void SQLEncoder<Formatter>::dumpJoined(bool stripArgs) const {
	for (size_t i = 0; i < query_.JoinQueries().size(); ++i) {
		if (query_.JoinQueries()[i].GetJoinType() == JoinType::LeftJoin) {
			formatter_.Next();
			DumpSingleJoinQuery(i, stripArgs);
		}
	}
}

template <typename Formatter>
void SQLEncoder<Formatter>::dumpMerged(bool stripArgs) const {
	WrSerializer& ser = formatter_.Serializer();
	for (const auto& me : query_.MergeQueries()) {
		formatter_.Next();
		ser << kJoinNames[me.GetJoinType()];
		const auto parenthesisGuard = formatter_.OpenParenthesis();
		SQLEncoder(me, formatter_).DumpSQL(stripArgs);
	}
}

static std::string escapeQuotes(std::string str) {
	for (size_t i = 0; i < str.size(); ++i) {
		if (str[i] == '\'' && (i == 0 || str[i - 1] != '\\')) {
			str.insert(i++, 1, '\\');
		}
	}
	return str;
}

template <typename Formatter>
void SQLEncoder<Formatter>::dumpOrderBy(bool stripArgs) const {
	WrSerializer& ser = formatter_.Serializer();
	if (query_.GetSortingEntries().empty()) {
		return;
	}
	formatter_.Next();
	ser << "ORDER BY";
	const auto blockGuard = formatter_.StartBlock();
	for (size_t i = 0; i < query_.GetSortingEntries().size(); ++i) {
		const SortingEntry& sortingEntry(query_.GetSortingEntries()[i]);
		if (i != 0) {
			formatter_.Comma();
		}
		if (query_.ForcedSortOrder().empty() || i != 0) {
			ser << '\'' << escapeQuotes(sortingEntry.expression) << '\'';
		} else {
			ser << "FIELD";
			const auto parenthesisGuard = formatter_.OpenParenthesis();
			ser << sortingEntry.expression;
			if (stripArgs) {
				ser << '?';
			} else {
				for (const auto& v : query_.ForcedSortOrder()) {
					formatter_.Comma();
					v.Dump(ser);
				}
			}
		}
		if (sortingEntry.desc) {
			formatter_.Next();
			ser << "DESC";
		}
	}
}

template <typename Formatter>
void SQLEncoder<Formatter>::dumpEqualPositions(const EqualPositions_t& equalPositions) const {
	WrSerializer& ser = formatter_.Serializer();
	for (const auto& ep : equalPositions) {
		if (ep.size() <= 1) [[unlikely]] {
			throw Error(errParams, "Equal positions must contain more than 1 element, but {} was provided", ep.size());
		}
		formatter_.Next();
		ser << "equal_position";
		const auto parenthesisGuard = formatter_.OpenParenthesis();
		for (size_t i = 0; i < ep.size(); ++i) {
			if (i != 0) {
				formatter_.Comma();
			}
			ser << ep[i];
		}
	}
}

template <typename Formatter>
void SQLEncoder<Formatter>::printField(bool& needComma, std::string_view name) const {
	if (needComma) {
		formatter_.Comma();
	} else {
		needComma = true;
	}
	formatter_.Serializer() << name;
}

template <typename Formatter>
void SQLEncoder<Formatter>::DumpSQL(bool stripArgs) const {
	WrSerializer& ser = formatter_.Serializer();
	switch (realQueryType_) {
		case QuerySelect: {
			if (query_.IsLocal()) {
				ser << "LOCAL ";
			}
			ser << "SELECT";
			{
				const auto selectFiltersGuard = formatter_.StartBlock();
				bool needComma = false;
				if (query_.IsWithRank()) {
					ser << "RANK()";
					needComma = true;
				}
				for (const auto& a : query_.Aggregations()) {
					if (needComma) {
						formatter_.Comma();
					} else {
						needComma = true;
					}
					ser << AggTypeToStr(a.Type());
					{
						const auto aggregationsParenthesisGuard = formatter_.OpenParenthesis();
						for (const auto& f : a.Fields()) {
							if (&f != &*a.Fields().begin()) {
								formatter_.Comma();
							}
							ser << f;
						}
						for (const auto& se : a.Sorting()) {
							formatter_.Next();
							ser << "ORDER BY " << '\'' << escapeQuotes(se.expression) << '\'' << (se.desc ? " DESC" : " ASC");
						}

						if (a.Offset() != QueryEntry::kDefaultOffset && !stripArgs) {
							formatter_.Next();
							ser << "OFFSET " << a.Offset();
						}
						if (a.Limit() != QueryEntry::kDefaultLimit && !stripArgs) {
							formatter_.Next();
							ser << "LIMIT " << a.Limit();
						}
					}
				}
				if (query_.Aggregations().empty() ||
					(query_.Aggregations().size() == 1 && query_.Aggregations()[0].Type() == AggDistinct)) {
					if (query_.SelectFilters().Empty()) {
						if (query_.Limit() != 0 || !query_.HasCalcTotal()) {
							if (needComma) {
								formatter_.Comma();
							}
							ser << '*';
							if (query_.HasCalcTotal()) {
								needComma = true;
							}
						}
					} else {
						if (query_.SelectFilters().AllRegularFields()) {
							printField(needComma, FieldsNamesFilter::kAllRegularFieldsName);
						}
						for (const auto& field : query_.SelectFilters().Fields()) {
							printField(needComma, field);
						}
						if (query_.SelectFilters().AllVectorFields()) {
							printField(needComma, FieldsNamesFilter::kAllVectorFieldsName);
						}
					}
				}
				if (query_.HasCalcTotal()) {
					if (needComma) {
						formatter_.Comma();
					}
					if (query_.CalcTotal() == ModeAccurateTotal) {
						ser << "COUNT(*)";
					}
					if (query_.CalcTotal() == ModeCachedTotal) {
						ser << "COUNT_CACHED(*)";
					}
				}
			}
			ser << "FROM";
			{
				const auto nsGuard = formatter_.StartBlock();
				ser << query_.NsName();
			}
		} break;
		case QueryDelete:
			ser << "DELETE FROM";
			{
				const auto nsGuard = formatter_.StartBlock();
				ser << query_.NsName();
			}
			break;
		case QueryUpdate: {
			if (query_.UpdateFields().empty()) {
				break;
			}
			ser << "UPDATE";
			{
				const auto nsGuard = formatter_.StartBlock();
				ser << query_.NsName();
			}
			FieldModifyMode mode = query_.UpdateFields().front().Mode();
			bool isUpdate = (mode == FieldModeSet || mode == FieldModeSetJson);
			if (isUpdate) {
				formatter_.Next();
				ser << "SET";
			} else {
				formatter_.Next();
				ser << "DROP";
			}
			{
				const auto updateFieldsGuard = formatter_.StartBlock();
				for (const UpdateEntry& field : query_.UpdateFields()) {
					if (&field != &*query_.UpdateFields().begin()) {
						formatter_.Comma();
					}
					ser << field.Column();
					if (isUpdate) {
						ser << " = ";
						if (stripArgs) {
							ser << '?';
						} else {
							const bool isArray = (field.Values().IsArrayValue() || field.Values().size() > 1);
							const auto bracketGuard = formatter_.ConditionallyOpenBracket(isArray);
							for (const Variant& v : field.Values()) {
								if (&v != &*field.Values().begin()) {
									formatter_.Comma();
								}
								v.Type().EvaluateOneOf(overloaded{
									[&](KeyValueType::String) {
										if (!field.IsExpression() && mode != FieldModeSetJson) {
											stringToSql(v.As<p_string>(), ser);
										} else {
											ser << v.As<std::string>();
										}
									},
									[&](KeyValueType::Uuid) { ser << '\'' << v.As<std::string>() << '\''; },
									[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
														KeyValueType::Float, KeyValueType::Null, KeyValueType::Composite,
														KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::FloatVector> auto) {
										ser << v.As<std::string>();
									}});
							}
						}
					}
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

	dumpSQLWhere(stripArgs);
	dumpJoined(stripArgs);
	dumpMerged(stripArgs);
	dumpOrderBy(stripArgs);

	if (query_.HasOffset() && !stripArgs) {
		formatter_.Next();
		ser << "OFFSET " << query_.Offset();
	}
	if (query_.HasLimit() && !stripArgs) {
		formatter_.Next();
		ser << "LIMIT " << query_.Limit();
	}
}

template <NeedQuote needQuote, FunctionAsString functionAsString = FunctionAsString::No>
static void dumpCondWithValues(WrSerializer& ser, auto& formatter, std::string_view fieldName, CondType cond, const VariantArray& values,
							   bool stripArgs) {
	switch (cond) {
		case CondDWithin:
			ser << "ST_DWithin(";
			indexToSql<needQuote>(fieldName, ser);
			if (stripArgs) {
				ser << ", ?, ?)";
			} else {
				if (values.size() != 2) [[unlikely]] {
					throw Error(errParams, "Within-condition must contain exactly 2 elements, but {} was provided", values.size());
				}
				Point point;
				double distance;
				if (values[0].Type().Is<KeyValueType::Tuple>()) {
					point = values[0].As<Point>();
					distance = values[1].As<double>();
				} else {
					point = values[1].As<Point>();
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
				const auto parenthesisGuard = formatter.ConditionallyOpenParenthesis(values.size() != 1);
				for (auto& v : values) {
					if (&v != &values[0]) {
						formatter.Comma();
					}
					if (functionAsString == FunctionAsString::Yes) {
						ser << v.As<std::string>();
					} else {
						v.Type().EvaluateOneOf(overloaded{
							[&](KeyValueType::String) { stringToSql(v.As<p_string>(), ser); },
							[&](KeyValueType::Uuid) { ser << '\'' << v.As<std::string>() << '\''; },
							[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
												KeyValueType::Float, KeyValueType::Null, KeyValueType::Composite, KeyValueType::Tuple,
												KeyValueType::Undefined, KeyValueType::FloatVector> auto) { ser << v.As<std::string>(); }});
					}
				}
			}
			break;
		case CondKnn:
			throw Error(errParams, "Unexpected KNN-condition");
	}
}

template <typename Formatter>
void SQLEncoder<Formatter>::dumpWhereEntries(QueryEntries::const_iterator from, QueryEntries::const_iterator to, bool stripArgs) const {
	WrSerializer& ser = formatter_.Serializer();
	using namespace std::string_view_literals;
	bool needEncloseOR = false;
	for (auto it = from, next = from; it != to; it = next) {
		++next;
		if (next != to && next->operation != OpOr) {
			needEncloseOR = true;
			break;
		}
	}
	auto formatter = formatter_.StartConditions(needEncloseOR);
	for (auto it = from, next = from; it != to; it = next) {
		++next;
		const OpType op = it->operation;
		const bool isNextOr =
			(next != to &&
			 (next->operation == OpType::OpOr ||
			  next->Visit(
				  [&](const JoinQueryEntry& jqe) { return query_.JoinQueries()[jqe.joinIndex].GetJoinType() == JoinType::OrInnerJoin; },
				  [&](const auto&) { return false; })));
		formatter.AddCondition(op, isNextOr);
		it->Visit(
			[&](const AlwaysTrue&) {
				logFmt(LogTrace, "Not normalized query to dsl"sv);
				ser << "true"sv;
			},
			[&ser](const AlwaysFalse&) {
				logFmt(LogTrace, "Not normalized query to dsl"sv);
				ser << "false"sv;
			},
			[&](const SubQueryEntry& sqe) {
				{
					const auto parenthesisGuard = formatter_.OpenParenthesis();
					SQLEncoder{query_.SubQueries()[sqe.QueryIndex()], formatter_}.DumpSQL(stripArgs);
				}
				dumpCondWithValues<NeedQuote::No>(ser, formatter_, "", sqe.Condition(), sqe.Values(), stripArgs);
			},
			[&](const SubQueryFieldEntry& sqe) {
				ser << sqe.FieldName() << ' ' << sqe.Condition() << ' ';
				const auto parenthesisGuard = formatter_.OpenParenthesis();
				SQLEncoder{query_.SubQueries()[sqe.QueryIndex()], formatter_}.DumpSQL(stripArgs);
			},
			[&](const SubQueryFunctionEntry& sqe) {
				{
					const auto parenthesisGuard = formatter_.OpenParenthesis();
					SQLEncoder{query_.SubQueries()[sqe.QueryIndex()], formatter_}.DumpSQL(stripArgs);
				}
				ser << ' ' << sqe.Condition() << ' ' << sqe.Function().ToString();
			},
			[&](const QueryEntriesBracket& bracket) {
				const auto parenthesisGuard = formatter_.OpenParenthesis();
				dumpWhereEntries(it.cbegin(), it.cend(), stripArgs);
				dumpEqualPositions(bracket.equalPositions);
			},
			[&](const QueryEntry& entry) {
				dumpCondWithValues<NeedQuote::Yes>(ser, formatter_, entry.FieldName(), entry.Condition(), entry.Values(), stripArgs);
			},
			[&](const QueryFunctionEntry& entry) {
				std::visit(overloaded{[&](const functions::FlatArrayLen& f) {
										  dumpCondWithValues<NeedQuote::Yes>(ser, formatter_, f.ToString(), entry.Condition(),
																			 entry.Values(), stripArgs);
									  },
									  [&](const functions::Now& f) {
										  dumpCondWithValues<NeedQuote::Yes, FunctionAsString::Yes>(
											  ser, formatter_, entry.ComparisonField().FieldName(), entry.Condition(),
											  {Variant(f.ToString())}, stripArgs);
									  },
									  [&](const functions::Serial&) { assertrx_dbg(0); }},
						   entry.FunctionVariant());
			},
			[&](const MultiDistinctQueryEntry&) {},
			[&](const JoinQueryEntry& jqe) { SQLEncoder(query_, formatter_).DumpSingleJoinQuery(jqe.joinIndex, stripArgs); },
			[&](const BetweenFieldsQueryEntry& entry) {
				indexToSql<NeedQuote::Yes>(entry.LeftFieldName(), ser);
				ser << ' ' << entry.Condition() << ' ';
				indexToSql<NeedQuote::Yes>(entry.RightFieldName(), ser);
			},
			[&](const KnnQueryEntry& qe) {
				ser << "KNN"sv;
				const auto parenthesisGuard = formatter_.OpenParenthesis();
				indexToSql<NeedQuote::Yes>(qe.FieldName(), ser);
				formatter_.Comma();
				if (stripArgs) {
					ser << '?';
				} else {
					if (qe.Format() == KnnQueryEntry::DataFormatType::String) {
						ser << '\'';
						ser << qe.Data();
						ser << '\'';
					} else {
						const auto bracketGuard = formatter_.OpenBracket();
						const auto values{qe.Value().Span()};
						for (size_t i = 0; i < values.size(); ++i) {
							if (i != 0) {
								formatter_.Comma();
							}
							ser << values[i];  // TODO precision
						}
					}
				}
				formatter_.Comma();
				qe.Params().ToSql(ser);
			});
	}
}

template <typename Formatter>
void SQLEncoder<Formatter>::dumpSQLWhere(bool stripArgs) const {
	if (query_.Entries().Empty()) {
		return;
	}
	formatter_.Next();
	formatter_.Serializer() << "WHERE";
	{
		const auto whereGuard = formatter_.StartBlock();
		dumpWhereEntries(query_.Entries().cbegin(), query_.Entries().cend(), stripArgs);
		dumpEqualPositions(query_.Entries().equalPositions);
	}
}

template class SQLEncoder<SingleLineSqlFormatter>;
template class SQLEncoder<PrettySqlFormatter>;

}  // namespace reindexer
