#include "explaincalc.h"

#include <sstream>

#include "core/cjson/jsonbuilder.h"
#include "nsselecter.h"
#include "tools/logger.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace reindexer {

void ExplainCalc::LogDump(int logLevel) {
	using namespace std::string_view_literals;
	if (logLevel >= LogInfo && enabled_) {
		logFmt(LogInfo,
			   "Got {} items in {} µs [prepare {} µs, select {} µs, postprocess {} µs loop {} µs, general sort {} µs], sortindex {}",
			   count_, To_us(total_), To_us(prepare_), To_us(select_), To_us(postprocess_), To_us(loop_), To_us(sort_), sortIndex_);
	}

	if (logLevel >= LogTrace) {
		if (selectors_) {
			for (auto& it : *selectors_) {
				const auto op = it.operation;
				it.Visit([](const KnnRawSelectResult&) { throw_as_assert; }, Skip<JoinSelectIterator, SelectIteratorsBracket>{},
						 [this, op](const SelectIterator& s) {
							 logFmt(LogInfo, "{}: {} idsets, cost {}, matched {}, {}", s.name, s.size(), s.Cost(iters_),
									s.GetMatchedCount(op == OpNot), s.Dump());
						 },
						 [this, op](const concepts::OneOf<ComparatorsPackT> auto& c) {
							 logFmt(LogInfo, "{}: cost {}, matched {}, {}", c.Name(), c.Cost(iters_), c.GetMatchedCount(op == OpNot),
									c.Dump());
						 },
						 [](const AlwaysTrue&) { logPrint(LogInfo, "AlwaysTrue"sv); });
			}
		}

		if (jselectors_) {
			for (auto& js : *jselectors_) {
				if (js.Type() == JoinType::LeftJoin || js.Type() == JoinType::Merge) {
					logFmt(LogInfo, "{} {}: called {}", JoinTypeName(js.Type()), js.RightNsName(), js.Called());
				} else {
					// Using js.Matched(false), because there are no information about actual operation
					logFmt(LogInfo, "{} {}: called {}, matched {}", JoinTypeName(js.Type()), js.RightNsName(), js.Called(),
						   js.Matched(false));
				}
			}
		}
	}
}

constexpr static inline const char* joinTypeName(JoinType type) noexcept {
	switch (type) {
		case JoinType::InnerJoin:
			return "inner_join ";
		case JoinType::OrInnerJoin:
			return "or_inner_join ";
		case JoinType::LeftJoin:
			return "left_join ";
		case JoinType::Merge:
			return "merge ";
		default:
			return "<unknown>";
	}
}

constexpr static inline const char* opName(OpType op, bool first = true) {
	switch (op) {
		case OpAnd:
			return first ? "" : "and ";
		case OpOr:
			return "or ";
		case OpNot:
			return "not ";
		default:
			throw Error(errLogic, "Unexpected op type {}", int(op));
	}
}

constexpr std::string_view fieldKind(int fk) {
	using namespace std::string_view_literals;
	switch (fk) {
		case IndexValueType::SetByJsonPath:
			return "non-indexed"sv;
		case IndexValueType::NotSet:
			return ""sv;
		default:
			if (fk >= 0) {
				return "indexed"sv;
			} else {
				throw Error(errLogic, "Unexpected field type {}", int(fk));
			}
	}
}

RX_NO_INLINE static std::string buildPreselectDescription(const JoinPreResult& result) {
	assertrx_throw(result.properties);
	return std::visit(
		overloaded{
			[&](const IdSet&) -> std::string {
				const PreselectProperties& props = *result.properties;
				switch (result.storedValuesOptStatus) {
					case StoredValuesOptimizationStatus::DisabledByCompositeIndex:
						return fmt::format(
							"using preselected_rows, because joined query contains composite index condition in the ON-clause and "
							"joined query's expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreResult);
					case StoredValuesOptimizationStatus::DisabledByFullTextIndex:
						return fmt::format(
							"using preselected_rows, because joined query contains fulltext index condition in the ON-clause and joined "
							"query's expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreResult);
					case StoredValuesOptimizationStatus::DisabledByJoinedFieldSort:
						return fmt::format(
							"using preselected_rows, because sort by joined field was requested and joined query's "
							"expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreResult);
					case StoredValuesOptimizationStatus::DisabledByFloatVectorIndex:
						return fmt::format(
							"using preselected_rows, because joined query contains float vector index condition in the ON-clause and "
							"joined "
							"query's expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreResult);
					case StoredValuesOptimizationStatus::Enabled:
						return fmt::format(
							"using preselected_rows, because joined query's expected max iterations count of {} is less than "
							"max_iterations_idset_preresult limit of {} and larger then max copied values count of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreResult,
							JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization());
					default:
						throw_as_assert;
				}
			},
			[&](const SelectIteratorContainer&) -> std::string {
				const PreselectProperties& props = *result.properties;
				if (props.isLimitExceeded) {
					return fmt::format(
						"using no_preselect, because joined query's expected max iterations count of {} is larger than "
						"max_iterations_idset_preresult limit of {}",
						props.qresMaxIterations, props.maxIterationsIdSetPreResult);
				} else if (props.isUnorderedIndexSort) {
					return "using no_preselect, because there is a sorted query on an unordered index";
				}
				return "using no_preselect, because joined query expects a sort a btree index that is not yet committed "
					   "(optimization of indexes for the target namespace is not complete)";
			},
			[&](const JoinPreResult::Values&) {
				return fmt::format("using preselected_values, because the namespace's max iterations count is very small of {}",
								   result.properties->qresMaxIterations);
			}},
		result.payload);
}

static std::string addToJSON(JsonBuilder& builder, const JoinedSelector& js, OpType op = OpAnd) {
	using namespace std::string_view_literals;
	auto jsonSel = builder.Object();
	std::string name{joinTypeName(js.Type()) + js.RightNsName()};
	jsonSel.Put("field"sv, opName(op) + name);
	jsonSel.Put("matched"sv, js.Matched(op == OpNot));
	jsonSel.Put("selects_count"sv, js.Called());
	jsonSel.Put("join_select_total"sv, ExplainCalc::To_us(js.SelectTime()));
	switch (js.Type()) {
		case JoinType::InnerJoin:
		case JoinType::OrInnerJoin:
		case JoinType::LeftJoin:
			std::visit(overloaded{[&](const JoinPreResult::Values& values) {
									  jsonSel.Put("method"sv, "preselected_values"sv);
									  jsonSel.Put("keys"sv, values.Size());
								  },
								  [&](const IdSet& ids) {
									  jsonSel.Put("method"sv, "preselected_rows"sv);
									  jsonSel.Put("keys"sv, ids.size());
								  },
								  [&](const SelectIteratorContainer& iterators) {
									  jsonSel.Put("method"sv, "no_preselect"sv);
									  jsonSel.Put("keys"sv, iterators.Size());
								  }},
					   js.PreResult().payload);
			if (!js.PreResult().explainPreSelect.empty()) {
				jsonSel.Raw("explain_preselect"sv, js.PreResult().explainPreSelect);
			}
			if (js.PreResult().properties) {
				jsonSel.Put("description"sv, buildPreselectDescription(js.PreResult()));
			}
			if (!js.ExplainOneSelect().empty()) {
				jsonSel.Raw("explain_select"sv, js.ExplainOneSelect());
			}
			break;
		case JoinType::Merge:
			break;
	}
	return name;
}

static void addToJSON(JsonBuilder& builder, const ConditionInjection& injCond) {
	auto jsonSel = builder.Object();
	using namespace std::string_view_literals;
	using namespace std::string_literals;

	jsonSel.Put("condition"sv, injCond.initCond);
	jsonSel.Put("total_time_us"sv, ExplainCalc::To_us(injCond.totalTime_));
	jsonSel.Put("success"sv, injCond.succeed);
	if (!injCond.succeed) {
		if (injCond.reason.empty()) {
			if (injCond.orChainPart_) {
				jsonSel.Put("reason"sv, "Skipped as Or-chain part."sv);
			} else {
				jsonSel.Put("reason"sv, "Unknown"sv);
			}
		} else {
			std::string reason{injCond.reason};
			if (injCond.orChainPart_) {
				reason += " Or-chain part.";
			}
			jsonSel.Put("reason"sv, reason);
		}
	}

	if (!injCond.explain.empty()) {
		jsonSel.Raw("explain_select"sv, injCond.explain);
	}
	if (injCond.aggType != AggType::AggUnknown) {
		jsonSel.Put("agg_type"sv, AggTypeToStr(injCond.aggType));
	}
	jsonSel.Put("values_count"sv, injCond.valuesCount);
	jsonSel.Put("new_condition"sv, injCond.newCond);
}

static void addToJSON(JsonBuilder& builder, const JoinOnInjection& injCond) {
	auto jsonSel = builder.Object();
	using namespace std::string_view_literals;

	jsonSel.Put("namespace"sv, injCond.rightNsName);
	jsonSel.Put("on_condition"sv, injCond.joinCond);
	jsonSel.Put("type"sv, injCond.type == JoinOnInjection::ByValue ? "by_value"sv : "select"sv);
	jsonSel.Put("total_time_us"sv, ExplainCalc::To_us(injCond.totalTime_));
	jsonSel.Put("success"sv, injCond.succeed);
	if (!injCond.reason.empty()) {
		jsonSel.Put("reason"sv, injCond.reason);
	}
	jsonSel.Put("injected_condition"sv, injCond.injectedCond.Slice());
	if (!injCond.conditions.empty()) {
		auto jsonCondInjections = jsonSel.Array("conditions"sv);
		for (const auto& cond : injCond.conditions) {
			addToJSON(jsonCondInjections, cond);
		}
	}
}

std::string ExplainCalc::GetJSON() {
	using namespace std::string_view_literals;
	WrSerializer ser;
	{
		JsonBuilder json(ser);
		json.EmitTrailingForFloat(false);
		if (enabled_) {
			json.Put("total_us"sv, To_us(total_));
			json.Put("preselect_us"sv, To_us(preselect_));
			json.Put("prepare_us"sv, To_us(prepare_));
			json.Put("indexes_us"sv, To_us(select_));
			json.Put("postprocess_us"sv, To_us(postprocess_));
			json.Put("loop_us"sv, To_us(loop_));
			json.Put("general_sort_us"sv, To_us(sort_));
			if (!subqueries_.empty()) {
				auto subQueries = json.Array("subqueries");
				for (const auto& sq : subqueries_) {
					auto s = subQueries.Object();
					s.Put("namespace", sq.NsName());
					s.Raw("explain", sq.Explain());
					std::visit(overloaded{[&](size_t k) { s.Put("keys", k); }, [&](const std::string& f) { s.Put("field", f); }},
							   sq.FieldOrKeys());
				}
			}
		}
		json.Put("sort_index"sv, sortIndex_);
		json.Put("sort_by_uncommitted_index"sv, sortOptimization_);

		{
			auto jsonSelArr = json.Array("selectors"sv);

			if (selectors_) {
				selectors_->ExplainJSON(iters_, jsonSelArr, jselectors_);
			}

			if (jselectors_) {
				// adding explain for LeftJoin-s and Merge subqueries
				for (const JoinedSelector& js : *jselectors_) {
					if (js.Type() == JoinType::InnerJoin || js.Type() == JoinType::OrInnerJoin) {
						continue;
					}
					std::ignore = addToJSON(jsonSelArr, js);
				}
			}
		}

		if (onInjections_ && !onInjections_->empty()) {
			auto jsonOnInjections = json.Array("on_conditions_injections"sv);
			for (const JoinOnInjection& injCond : *onInjections_) {
				addToJSON(jsonOnInjections, injCond);
			}
		}
	}

	return std::string(ser.Slice());
}

template <typename T>
concept HasConditionStr = requires(T t) {
	{ t.ConditionStr() } -> std::convertible_to<std::string>;
};

std::string SelectIteratorContainer::explainJSON(const_iterator begin, const_iterator end, int iters, JsonBuilder& builder,
												 const JoinedSelectors* jselectors) {
	using namespace std::string_literals;
	using namespace std::string_view_literals;

	std::stringstream name;
	name << '(';
	for (const_iterator it = begin; it != end; ++it) {
		if (it != begin) {
			name << ' ';
		}
		it->Visit(
			[](const KnnRawSelectResult&) { throw_as_assert; },
			[&](const SelectIteratorsBracket&) {
				auto jsonSel = builder.Object();
				auto jsonSelArr = jsonSel.Array("selectors"sv);
				const std::string brName{explainJSON(it.cbegin(), it.cend(), iters, jsonSelArr, jselectors)};
				jsonSelArr.End();
				jsonSel.Put("field"sv, opName(it->operation) + brName);
				name << opName(it->operation, it == begin) << brName;
			},
			[&](const SelectIterator& siter) {
				auto jsonSel = builder.Object();
				const bool isScanIterator{std::string_view(siter.name) == "-scan"sv};
				if (!isScanIterator) {
					jsonSel.Put("keys"sv, siter.size());
					jsonSel.Put("cost"sv, std::round(siter.Cost(iters)));
				} else {
					jsonSel.Put("items"sv, siter.GetMaxIterations(iters));
				}
				jsonSel.Put("field"sv, opName(it->operation) + siter.name);
				if (siter.IndexNo() != IndexValueType::NotSet) {
					jsonSel.Put("field_type"sv, fieldKind(siter.IndexNo()));
				}
				jsonSel.Put("matched"sv, siter.GetMatchedCount(it->operation == OpNot));
				jsonSel.Put("method"sv, isScanIterator ? "scan"sv : "index"sv);
				jsonSel.Put("type"sv, siter.TypeName());
				name << opName(it->operation, it == begin) << siter.name;
			},
			[&](const JoinSelectIterator& jiter) {
				assertrx_throw(jselectors);
				assertrx_throw(jiter.joinIndex < jselectors->size());
				const std::string jName{addToJSON(builder, (*jselectors)[jiter.joinIndex], it->operation)};
				name << opName(it->operation, it == begin) << jName;
			},
			[&]<concepts::OneOf<FieldsComparator, EqualPositionComparator, GroupingEqualPositionComparator> T>(const T& c) {
				auto jsonSel = builder.Object();
				if constexpr (concepts::OneOf<T, EqualPositionComparator, GroupingEqualPositionComparator>) {
					jsonSel.Put("comparators"sv, c.FieldsCount());
				} else {
					jsonSel.Put("comparators"sv, 1);
				}
				jsonSel.Put("field"sv, opName(it->operation) + c.Name());
				jsonSel.Put("cost"sv, std::round(c.Cost(iters)));
				jsonSel.Put("method"sv, "scan"sv);
				jsonSel.Put("matched"sv, c.GetMatchedCount(it->operation == OpNot));
				jsonSel.Put("type"sv, std::is_same_v<FieldsComparator, decltype(c)> ? "TwoFieldsComparison"sv : "Comparator"sv);
				name << opName(it->operation, it == begin) << c.Name();
			},
			[&](const concepts::OneOf<
				ComparatorNotIndexed, ComparatorDistinctMulti, ComparatorDistinctMultiArray,
				Template<ComparatorDistinctMultiScalarBase, ComparatorDistinctMultiIndexedGetter, ComparatorDistinctMultiColumnGetter,
						 ComparatorDistinctMultiScalarGetter>,
				Template<ComparatorIndexed, bool, int, int64_t, double, key_string, PayloadValue, Point, Uuid, FloatVector>> auto& c) {
				auto jsonSel = builder.Object();
				jsonSel.Put("comparators"sv, 1);
				jsonSel.Put("field"sv, opName(it->operation) + std::string{c.Name()});
				jsonSel.Put("cost"sv, std::round(c.Cost(iters)));
				jsonSel.Put("method"sv, "scan"sv);
				jsonSel.Put("matched"sv, c.GetMatchedCount(it->operation == OpNot));
				jsonSel.Put("type"sv, "Comparator"sv);
				if constexpr (HasConditionStr<std::decay_t<decltype(c)>>) {
					jsonSel.Put("condition"sv, c.ConditionStr());
				} else {
					jsonSel.Put("condition"sv, "");
				}
				jsonSel.Put("field_type"sv, fieldKind(c.IsIndexed() ? 0 : IndexValueType::SetByJsonPath));
				name << opName(it->operation, it == begin) << c.Name();
			},
			[&](const AlwaysTrue&) {
				auto jsonSkipped = builder.Object();
				jsonSkipped.Put("type"sv, "Skipped"sv);
				jsonSkipped.Put("description"sv, (it->operation != OpNot ? opName(it->operation) : ""s) + "always "s +
													 (it->operation == OpNot ? "false" : "true"));
				name << opName(it->operation == OpNot ? OpAnd : it->operation, it == begin) << "Always"sv
					 << (it->operation == OpNot ? "False"sv : "True"sv);
			});
	}
	name << ')';
	return name.str();
}

int ExplainCalc::To_us(const ExplainCalc::Duration& d) noexcept { return duration_cast<microseconds>(d).count(); }

}  // namespace reindexer
