#include "explaincalc.h"

#include <sstream>

#include "core/cjson/jsonbuilder.h"
#include "nsselecter.h"
#include "tools/logger.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace reindexer {

void ExplainCalc::LogDump(int logLevel) {
	if (logLevel >= LogInfo && enabled_) {
		logPrintf(LogInfo,
				  "Got %d items in %d µs [prepare %d µs, select %d µs, postprocess %d µs loop %d µs, general sort %d µs], sortindex %s",
				  count_, To_us(total_), To_us(prepare_), To_us(select_), To_us(postprocess_), To_us(loop_), To_us(sort_), sortIndex_);
	}

	if (logLevel >= LogTrace) {
		if (selectors_) {
			selectors_->ExecuteAppropriateForEach(
				Skip<JoinSelectIterator, SelectIteratorsBracket>{},
				[this](const SelectIterator &s) {
					logPrintf(LogInfo, "%s: %d idsets, %d comparators, cost %g, matched %d, %s", s.name, s.size(), s.comparators_.size(),
							  s.Cost(iters_), s.GetMatchedCount(), s.Dump());
				},
				[this](const FieldsComparator &c) {
					logPrintf(LogInfo, "%s: cost %g, matched %d, %s", c.Name(), c.Cost(iters_), c.GetMatchedCount(), c.Dump());
				},
				[](const AlwaysFalse &) { logPrintf(LogInfo, "AlwaysFalse"); },
				[](const AlwaysTrue &) { logPrintf(LogInfo, "AlwaysTrue"); });
		}

		if (jselectors_) {
			for (auto &js : *jselectors_) {
				if (js.Type() == JoinType::LeftJoin || js.Type() == JoinType::Merge) {
					logPrintf(LogInfo, "%s %s: called %d", JoinTypeName(js.Type()), js.RightNsName(), js.Called());
				} else {
					logPrintf(LogInfo, "%s %s: called %d, matched %d", JoinTypeName(js.Type()), js.RightNsName(), js.Called(),
							  js.Matched());
				}
			}
		}
	}
}

constexpr static inline const char *joinTypeName(JoinType type) noexcept {
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

constexpr static inline const char *opName(OpType op, bool first = true) {
	switch (op) {
		case OpAnd:
			return first ? "" : "and ";
		case OpOr:
			return "or ";
		case OpNot:
			return "not ";
		default:
			throw Error(errLogic, "Unexpected op type %d", int(op));
	}
}

constexpr std::string_view fieldKind(IteratorFieldKind fk) {
	using namespace std::string_view_literals;
	switch (fk) {
		case IteratorFieldKind::NonIndexed:
			return "non-indexed"sv;
		case IteratorFieldKind::Indexed:
			return "indexed"sv;
		case IteratorFieldKind::None:
			return ""sv;
		default:
			throw Error(errLogic, "Unexpected field type %d", int(fk));
	}
}

static std::string addToJSON(JsonBuilder &builder, const JoinedSelector &js, OpType op = OpAnd) {
	using namespace std::string_view_literals;
	auto jsonSel = builder.Object();
	std::string name{joinTypeName(js.Type()) + js.RightNsName()};
	jsonSel.Put("field"sv, opName(op) + name);
	jsonSel.Put("matched"sv, js.Matched());
	jsonSel.Put("selects_count"sv, js.Called());
	jsonSel.Put("join_select_total"sv, ExplainCalc::To_us(js.PreResult()->selectTime));
	switch (js.Type()) {
		case JoinType::InnerJoin:
		case JoinType::OrInnerJoin:
		case JoinType::LeftJoin:
			assertrx(js.PreResult());
			switch (js.PreResult()->dataMode) {
				case JoinPreResult::ModeValues:
					jsonSel.Put("method"sv, "preselected_values"sv);
					jsonSel.Put("keys"sv, js.PreResult()->values.size());
					break;
				case JoinPreResult::ModeIdSet:
					jsonSel.Put("method"sv, "preselected_rows"sv);
					jsonSel.Put("keys"sv, js.PreResult()->ids.size());
					break;
				case JoinPreResult::ModeIterators:
					jsonSel.Put("method"sv, "no_preselect"sv);
					jsonSel.Put("keys"sv, js.PreResult()->iterators.Size());
					break;
				default:
					break;
			}
			if (!js.PreResult()->explainPreSelect.empty()) {
				jsonSel.Raw("explain_preselect"sv, js.PreResult()->explainPreSelect);
			}
			if (!js.PreResult()->explainOneSelect.empty()) {
				jsonSel.Raw("explain_select"sv, js.PreResult()->explainOneSelect);
			}
			break;
		case JoinType::Merge:
			break;
	}
	return name;
}

static void addToJSON(JsonBuilder &builder, const ConditionInjection &injCond) {
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

static std::string addToJSON(JsonBuilder &builder, const JoinOnInjection &injCond) {
	auto jsonSel = builder.Object();
	std::string name{injCond.rightNsName};
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
		for (const auto &cond : injCond.conditions) {
			addToJSON(jsonCondInjections, cond);
		}
	}

	return name;
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
				auto subQuries = json.Array("subqueries");
				for (const auto &sq : subqueries_) {
					auto s = subQuries.Object();
					s.Put("namespace", sq.NsName());
					s.Raw("explain", sq.Explain());
					std::visit(overloaded{[&](size_t k) { s.Put("keys", k); }, [&](const std::string &f) { s.Put("field", f); }},
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
				for (const JoinedSelector &js : *jselectors_) {
					if (js.Type() == JoinType::InnerJoin || js.Type() == JoinType::OrInnerJoin) continue;
					addToJSON(jsonSelArr, js);
				}
			}
		}

		if (onInjections_ && !onInjections_->empty()) {
			auto jsonOnInjections = json.Array("on_conditions_injections"sv);
			for (const JoinOnInjection &injCond : *onInjections_) {
				addToJSON(jsonOnInjections, injCond);
			}
		}
	}

	return std::string(ser.Slice());
}

std::string SelectIteratorContainer::explainJSON(const_iterator begin, const_iterator end, int iters, JsonBuilder &builder,
												 const JoinedSelectors *jselectors) {
	using namespace std::string_literals;
	using namespace std::string_view_literals;

	std::stringstream name;
	name << '(';
	for (const_iterator it = begin; it != end; ++it) {
		if (it != begin) name << ' ';
		it->InvokeAppropriate<void>(
			[&](const SelectIteratorsBracket &) {
				auto jsonSel = builder.Object();
				auto jsonSelArr = jsonSel.Array("selectors"sv);
				const std::string brName{explainJSON(it.cbegin(), it.cend(), iters, jsonSelArr, jselectors)};
				jsonSelArr.End();
				jsonSel.Put("field"sv, opName(it->operation) + brName);
				name << opName(it->operation, it == begin) << brName;
			},
			[&](const SelectIterator &siter) {
				auto jsonSel = builder.Object();
				const bool isScanIterator{std::string_view(siter.name) == "-scan"sv};
				if (!isScanIterator) {
					jsonSel.Put("keys"sv, siter.size());
					jsonSel.Put("comparators"sv, siter.comparators_.size());
					jsonSel.Put("cost"sv, siter.Cost(iters));
				} else {
					jsonSel.Put("items"sv, siter.GetMaxIterations(iters));
				}
				jsonSel.Put("field"sv, opName(it->operation) + siter.name);
				if (siter.fieldKind != IteratorFieldKind::None) {
					jsonSel.Put("field_type"sv, fieldKind(siter.fieldKind));
				}
				jsonSel.Put("matched"sv, siter.GetMatchedCount());
				jsonSel.Put("method"sv, isScanIterator || siter.comparators_.size() ? "scan"sv : "index"sv);
				jsonSel.Put("type"sv, siter.TypeName());
				name << opName(it->operation, it == begin) << siter.name;
			},
			[&](const JoinSelectIterator &jiter) {
				assertrx(jiter.joinIndex < jselectors->size());
				const std::string jName{addToJSON(builder, (*jselectors)[jiter.joinIndex], it->operation)};
				name << opName(it->operation, it == begin) << jName;
			},
			[&](const FieldsComparator &c) {
				auto jsonSel = builder.Object();
				jsonSel.Put("comparators"sv, 1);
				jsonSel.Put("field"sv, opName(it->operation) + c.Name());
				jsonSel.Put("cost"sv, c.Cost(iters));
				jsonSel.Put("method"sv, "scan"sv);
				jsonSel.Put("items"sv, iters);
				jsonSel.Put("matched"sv, c.GetMatchedCount());
				jsonSel.Put("type"sv, "TwoFieldsComparison"sv);
				name << opName(it->operation, it == begin) << c.Name();
			},
			[&](const AlwaysFalse &) {
				auto jsonSkiped = builder.Object();
				jsonSkiped.Put("type"sv, "Skipped"sv);
				jsonSkiped.Put("description"sv, "always "s + (it->operation == OpNot ? "true" : "false"));
				name << opName(it->operation == OpNot ? OpAnd : it->operation, it == begin) << "Always"sv
					 << (it->operation == OpNot ? "True"sv : "False"sv);
			},
			[&](const AlwaysTrue &) {
				auto jsonSkiped = builder.Object();
				jsonSkiped.Put("type"sv, "Skipped"sv);
				jsonSkiped.Put("description"sv, "always "s + (it->operation == OpNot ? "false" : "true"));
				name << opName(it->operation == OpNot ? OpAnd : it->operation, it == begin) << "Always"sv
					 << (it->operation == OpNot ? "False"sv : "True"sv);
			});
	}
	name << ')';
	return name.str();
}

int ExplainCalc::To_us(const ExplainCalc::Duration &d) noexcept { return duration_cast<microseconds>(d).count(); }

}  // namespace reindexer
