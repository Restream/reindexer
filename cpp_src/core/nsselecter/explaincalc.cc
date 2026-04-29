#include "explaincalc.h"

#include <sstream>

#include "core/cjson/jsonbuilder.h"
#include "core/nsselecter/joins/helpers.h"
#include "nsselecter.h"
#include "tools/logger.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace reindexer {

static int64_t To_us(const BasicExplainResults::Duration& d) noexcept { return duration_cast<microseconds>(d).count(); }

void BasicExplainResults::Append(const BasicExplainResults& other) noexcept {
	total += other.total;
	prepare += other.prepare;
	preselect += other.preselect;
	select += other.select;
	postprocess += other.postprocess;
	loop += other.loop;
	sort += other.sort;
}

void BasicExplainResults::GetJSON(JsonBuilder& json) const {
	using namespace std::string_view_literals;

	json.Put("total_us"sv, To_us(total));
	json.Put("preselect_us"sv, To_us(preselect));
	json.Put("prepare_us"sv, To_us(prepare));
	json.Put("indexes_us"sv, To_us(select));
	json.Put("postprocess_us"sv, To_us(postprocess));
	json.Put("loop_us"sv, To_us(loop));
	json.Put("general_sort_us"sv, To_us(sort));
}

void SingleQueryExplainCalc::LogDump(int logLevel) {
	using namespace std::string_view_literals;
	if (logLevel >= LogInfo && enabled_) {
		logFmt(LogInfo,
			   "Got {} items in {} µs [prepare {} µs, select {} µs, postprocess {} µs loop {} µs, general sort {} µs], sortindex {}",
			   count_, To_us(basics_.total), To_us(basics_.prepare), To_us(basics_.select), To_us(basics_.postprocess), To_us(basics_.loop),
			   To_us(basics_.sort), sortIndex_);
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

		if (jitemsprocessors_) {
			for (auto& js : *jitemsprocessors_) {
				if (js.Type() == JoinType::LeftJoin || js.Type() == JoinType::Merge) {
					logFmt(LogInfo, "{} {}: called {}", joins::JoinTypeName(js.Type()), js.RightNsName(), js.Called());
				} else {
					// Using js.Matched(false), because there are no information about actual operation
					logFmt(LogInfo, "{} {}: called {}, matched {}", joins::JoinTypeName(js.Type()), js.RightNsName(), js.Called(),
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

static std::string method(bool isScan, bool isCached) {
	std::string method{isScan ? "scan" : "index"};
	if (isCached) {
		method += "(cached)";
	}
	return method;
}

RX_NO_INLINE static std::string buildPreselectDescription(const joins::PreSelect& result) {
	assertrx_throw(result.properties);
	return std::visit(
		overloaded{
			[&](const IdSetPlain&) -> std::string {
				const joins::PreSelectProperties& props = *result.properties;
				switch (result.storedValuesOptStatus) {
					case StoredValuesOptimizationStatus::DisabledByCompositeIndex:
						return fmt::format(
							"using preselected_rows, because joined query contains composite index condition in the ON-clause and "
							"joined query's expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreSelect);
					case StoredValuesOptimizationStatus::DisabledByFullTextIndex:
						return fmt::format(
							"using preselected_rows, because joined query contains fulltext index condition in the ON-clause and joined "
							"query's expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreSelect);
					case StoredValuesOptimizationStatus::DisabledByJoinedFieldSort:
						return fmt::format(
							"using preselected_rows, because sort by joined field was requested and joined query's "
							"expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreSelect);
					case StoredValuesOptimizationStatus::DisabledByFloatVectorIndex:
						return fmt::format(
							"using preselected_rows, because joined query contains float vector index condition in the ON-clause and "
							"joined "
							"query's expected max iterations count of {} is less than max_iterations_idset_preresult limit of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreSelect);
					case StoredValuesOptimizationStatus::Enabled:
						return fmt::format(
							"using preselected_rows, because joined query's expected max iterations count of {} is less than "
							"max_iterations_idset_preresult limit of {} and larger then max copied values count of {}",
							props.qresMaxIterations, props.maxIterationsIdSetPreSelect,
							joins::PreSelect::MaxIterationsForValuesOptimization);
					default:
						throw_as_assert;
				}
			},
			[&](const SelectIteratorContainer&) -> std::string {
				const joins::PreSelectProperties& props = *result.properties;
				if (props.isLimitExceeded) {
					return fmt::format(
						"using no_preselect, because joined query's expected max iterations count of {} is larger than "
						"max_iterations_idset_preresult limit of {}",
						props.qresMaxIterations, props.maxIterationsIdSetPreSelect);
				} else if (props.isUnorderedIndexSort) {
					return "using no_preselect, because there is a sorted query on an unordered index";
				}
				return "using no_preselect, because joined query expects a sort a btree index that is not yet committed "
					   "(optimization of indexes for the target namespace is not complete)";
			},
			[&](const joins::PreSelect::Values&) {
				return fmt::format("using preselected_values, because the namespace's max iterations count is very small of {}",
								   result.properties->qresMaxIterations);
			}},
		result.payload);
}

static std::string addToJSON(JsonBuilder& builder, const joins::ItemsProcessor& js, OpType op = OpAnd) {
	using namespace std::string_view_literals;
	auto jsonSel{builder.Object()};
	std::string name{joinTypeName(js.Type()) + js.RightNsName()};
	jsonSel.Put("field"sv, opName(op) + name);
	jsonSel.Put("matched"sv, js.Matched(op == OpNot));
	jsonSel.Put("selects_count"sv, js.Called());
	jsonSel.Put("join_select_total"sv, To_us(js.SelectTime()));
	switch (js.Type()) {
		case JoinType::InnerJoin:
		case JoinType::OrInnerJoin:
		case JoinType::LeftJoin: {
			std::visit(overloaded{[&](const joins::PreSelect::Values& values) {
									  jsonSel.Put("method"sv, "preselected_values"sv);
									  jsonSel.Put("keys"sv, values.Size());
								  },
								  [&](const IdSetPlain& ids) {
									  jsonSel.Put("method"sv, "preselected_rows"sv);
									  jsonSel.Put("keys"sv, ids.Size());
								  },
								  [&](const SelectIteratorContainer& iterators) {
									  jsonSel.Put("method"sv, "no_preselect"sv);
									  jsonSel.Put("keys"sv, iterators.Size());
								  }},
					   js.PreSelectResults().payload);
			if (!js.PreSelectResults().explainPreSelect.empty()) {
				jsonSel.Raw("explain_preselect"sv, js.PreSelectResults().explainPreSelect);
			}
			if (js.PreSelectResults().properties) {
				jsonSel.Put("description"sv, buildPreselectDescription(js.PreSelectResults()));
			}
			if (!js.ExplainOneSelect().empty()) {
				jsonSel.Raw("explain_select"sv, js.ExplainOneSelect());
			}
			break;
		}
		case JoinType::Merge:
			break;
	}
	return name;
}

static void addToJSON(JsonBuilder& builder, const ConditionInsertion& insCond) {
	auto jsonSel = builder.Object();
	using namespace std::string_view_literals;
	using namespace std::string_literals;

	jsonSel.Put("condition"sv, insCond.initCond);
	jsonSel.Put("total_time_us"sv, To_us(insCond.totalTime_));
	jsonSel.Put("success"sv, insCond.succeed);
	if (!insCond.succeed) {
		if (insCond.reason.empty()) {
			if (insCond.orChainPart_) {
				jsonSel.Put("reason"sv, "Skipped as Or-chain part."sv);
			} else {
				jsonSel.Put("reason"sv, "Unknown"sv);
			}
		} else {
			std::string reason{insCond.reason};
			if (insCond.orChainPart_) {
				reason += " Or-chain part.";
			}
			jsonSel.Put("reason"sv, reason);
		}
	}

	if (!insCond.explain.empty()) {
		jsonSel.Raw("explain_select"sv, insCond.explain);
	}
	if (insCond.aggType != AggType::AggUnknown) {
		jsonSel.Put("agg_type"sv, AggTypeToStr(insCond.aggType));
	}
	jsonSel.Put("values_count"sv, insCond.valuesCount);
	jsonSel.Put("new_condition"sv, insCond.newCond);
}

static void addToJSON(JsonBuilder& builder, const JoinOnInsertion& insCond) {
	auto jsonSel = builder.Object();
	using namespace std::string_view_literals;

	jsonSel.Put("namespace"sv, insCond.rightNsName);
	jsonSel.Put("on_condition"sv, insCond.joinCond);
	jsonSel.Put("type"sv, insCond.type == JoinOnInsertion::ByValue ? "by_value"sv : "select"sv);
	jsonSel.Put("total_time_us"sv, To_us(insCond.totalTime_));
	jsonSel.Put("success"sv, insCond.succeed);
	if (!insCond.reason.empty()) {
		jsonSel.Put("reason"sv, insCond.reason);
	}
	jsonSel.Put("inserted_condition"sv, insCond.insertedCond.Slice());
	if (!insCond.conditions.empty()) {
		auto jsonCondInsertions = jsonSel.Array("conditions"sv);
		for (const auto& cond : insCond.conditions) {
			addToJSON(jsonCondInsertions, cond);
		}
	}
}

std::string SingleQueryExplainCalc::GetJSON() const {
	using namespace std::string_view_literals;
	WrSerializer ser;
	{
		JsonBuilder json(ser);
		json.EmitTrailingForFloat(false);
		if (enabled_) {
			basics_.GetJSON(json);
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
		json.Put("namespace"sv, nsName_);
		json.Put("sort_index"sv, sortIndex_);
		json.Put("sort_by_uncommitted_index"sv, sortOptimization_);

		{
			auto jsonSelArr = json.Array("selectors"sv);

			if (selectors_) {
				selectors_->ExplainJSON(iters_, jsonSelArr, jitemsprocessors_);
			}

			if (jitemsprocessors_) {
				// adding explain for LeftJoin-s and Merge subqueries
				for (const joins::ItemsProcessor& js : *jitemsprocessors_) {
					if (js.Type() == JoinType::InnerJoin || js.Type() == JoinType::OrInnerJoin) {
						continue;
					}
					std::ignore = addToJSON(jsonSelArr, js);
				}
			}
		}

		if (onInsertions_ && !onInsertions_->empty()) {
			auto jsonOnInsertions = json.Array("on_conditions_insertions"sv);
			for (const JoinOnInsertion& insCond : *onInsertions_) {
				addToJSON(jsonOnInsertions, insCond);
			}
		}
	}

	return std::string(ser.Slice());
}

SingleQueryExplainCalc::Duration SingleQueryExplainCalc::lap() noexcept {
	const auto now = Clock::now();
	Duration d = now - last_point_;
	last_point_ = now;
	return d;
}

void Explain::Append(const SingleQueryExplainCalc& explain) {
	mergedExplainJSONs_.emplace_back(explain.GetJSON());
	aggregated_.Append(explain.Basics());
}

std::string Explain::GetJSON() const {
	using namespace std::string_view_literals;

	if (mergedExplainJSONs_.size() == 1) {
		return mergedExplainJSONs_[0];
	}
	WrSerializer ser;
	{
		JsonBuilder json(ser);
		json.EmitTrailingForFloat(false);

		if (mergedExplainJSONs_.empty()) {
			aggregated_.GetJSON(json);
			json.Put("sort_index"sv, "-"sv);
		} else {
			aggregated_.GetJSON(json);
			json.Put("sort_index"sv, "-"sv);
			auto mergedArr = json.Array("merged");
			for (auto& explain : mergedExplainJSONs_) {
				mergedArr.Raw(explain);
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
												 const joins::ItemsProcessors* jitemsprocessors) {
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
				const std::string brName{explainJSON(it.cbegin(), it.cend(), iters, jsonSelArr, jitemsprocessors)};
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
				jsonSel.Put("method"sv, method(isScanIterator, siter.IsCached()));
				jsonSel.Put("type"sv, siter.TypeName());
				name << opName(it->operation, it == begin) << siter.name;
			},
			[&](const JoinSelectIterator& jiter) {
				assertrx_throw(jitemsprocessors);
				assertrx_throw(jiter.joinIndex < jitemsprocessors->size());
				const std::string jName{addToJSON(builder, (*jitemsprocessors)[jiter.joinIndex], it->operation)};
				name << opName(it->operation, it == begin) << jName;
			},
			[&]<concepts::OneOf<FieldsComparator, EqualPositionComparator, GroupingEqualPositionComparator, FunctionsComparator> T>(
				const T& c) {
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
			[&](const FunctionsComparator& c) {
				auto jsonSel = builder.Object();
				jsonSel.Put("comparators"sv, 1);
				jsonSel.Put("field"sv, opName(it->operation) + c.Name());
				jsonSel.Put("cost"sv, std::round(c.Cost(iters)));
				jsonSel.Put("method"sv, "scan"sv);
				jsonSel.Put("matched"sv, c.GetMatchedCount(it->operation == OpNot));
				jsonSel.Put("condition"sv, c.ConditionStr());
				jsonSel.Put("type"sv, "FunctionsComparator"sv);
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

}  // namespace reindexer
