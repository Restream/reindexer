#include "explaincalc.h"
#include <sstream>
#include "core/cbinding/reindexer_ctypes.h"
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/sql/sqlencoder.h"
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
				[](const AlwaysFalse &) { logPrintf(LogInfo, "AlwaysFalse"); });
		}

		if (jselectors_) {
			for (auto &js : *jselectors_) {
				if (js.Type() == JoinType::LeftJoin || js.Type() == JoinType::Merge) {
					logPrintf(LogInfo, "%s %s: called %d", SQLEncoder::JoinTypeName(js.Type()), js.RightNsName(), js.Called());
				} else {
					logPrintf(LogInfo, "%s %s: called %d, matched %d", SQLEncoder::JoinTypeName(js.Type()), js.RightNsName(), js.Called(),
							  js.Matched());
				}
			}
		}
	}
}

constexpr inline const char *joinTypeName(JoinType type) noexcept {
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

constexpr inline const char *opName(OpType op, bool first = true) {
	switch (op) {
		case OpAnd:
			return first ? "" : "and ";
		case OpOr:
			return "or ";
		case OpNot:
			return "not ";
		default:
			throw Error(errLogic, "Unexpected op type: %d", int(op));
	}
}

static std::string addToJSON(JsonBuilder &builder, const JoinedSelector &js, OpType op = OpAnd) {
	auto jsonSel = builder.Object();
	std::string name{joinTypeName(js.Type()) + js.RightNsName()};
	jsonSel.Put("field", opName(op) + name);
	jsonSel.Put("matched", js.Matched());
	jsonSel.Put("selects_count", js.Called());
	jsonSel.Put("join_select_total", ExplainCalc::To_us(js.PreResult()->selectTime));
	switch (js.Type()) {
		case JoinType::InnerJoin:
		case JoinType::OrInnerJoin:
		case JoinType::LeftJoin:
			assertrx(js.PreResult());
			switch (js.PreResult()->dataMode) {
				case JoinPreResult::ModeValues:
					jsonSel.Put("method", "preselected_values");
					jsonSel.Put("keys", js.PreResult()->values.size());
					break;
				case JoinPreResult::ModeIdSet:
					jsonSel.Put("method", "preselected_rows");
					jsonSel.Put("keys", js.PreResult()->ids.size());
					break;
				case JoinPreResult::ModeIterators:
					jsonSel.Put("method", "no_preselect");
					jsonSel.Put("keys", js.PreResult()->iterators.Size());
					break;
				default:
					break;
			}
			if (!js.PreResult()->explainPreSelect.empty()) {
				jsonSel.Raw("explain_preselect", js.PreResult()->explainPreSelect);
			}
			if (!js.PreResult()->explainOneSelect.empty()) {
				jsonSel.Raw("explain_select", js.PreResult()->explainOneSelect);
			}
			break;
		case JoinType::Merge:
			break;
	}
	return name;
}

std::string ExplainCalc::GetJSON() {
	WrSerializer ser;
	{
		JsonBuilder json(ser);
		if (enabled_) {
			json.Put("total_us", To_us(total_));
			json.Put("prepare_us", To_us(prepare_));
			json.Put("indexes_us", To_us(select_));
			json.Put("postprocess_us", To_us(postprocess_));
			json.Put("loop_us", To_us(loop_));
			json.Put("general_sort_us", To_us(sort_));
		}
		json.Put("sort_index", sortIndex_);
		json.Put("sort_by_uncommitted_index", sortOptimization_);

		auto jsonSelArr = json.Array("selectors");

		if (selectors_) {
			selectors_->ExplainJSON(iters_, jsonSelArr, jselectors_);
		}

		if (jselectors_) {
			for (JoinedSelector &js : *jselectors_) {
				if (js.Type() == JoinType::InnerJoin || js.Type() == JoinType::OrInnerJoin) continue;
				addToJSON(jsonSelArr, js);
			}
		}
	}

	return std::string(ser.Slice());
}

std::string SelectIteratorContainer::explainJSON(const_iterator begin, const_iterator end, int iters, JsonBuilder &builder,
												 const JoinedSelectors *jselectors) {
	using namespace std::string_literals;
	std::stringstream name;
	name << '(';
	for (const_iterator it = begin; it != end; ++it) {
		if (it != begin) name << ' ';
		it->InvokeAppropriate<void>(
			[&](const SelectIteratorsBracket &) {
				auto jsonSel = builder.Object();
				auto jsonSelArr = jsonSel.Array("selectors");
				const std::string brName{explainJSON(it.cbegin(), it.cend(), iters, jsonSelArr, jselectors)};
				jsonSelArr.End();
				jsonSel.Put("field", opName(it->operation) + brName);
				name << opName(it->operation, it == begin) << brName;
			},
			[&](const SelectIterator &siter) {
				auto jsonSel = builder.Object();
				const bool isScanIterator{std::string_view(siter.name) == "-scan"};
				if (!isScanIterator) {
					jsonSel.Put("keys", siter.size());
					jsonSel.Put("comparators", siter.comparators_.size());
					jsonSel.Put("cost", siter.Cost(iters));
				} else {
					jsonSel.Put("items", siter.GetMaxIterations(iters));
				}
				jsonSel.Put("field", opName(it->operation) + siter.name);
				jsonSel.Put("matched", siter.GetMatchedCount());
				jsonSel.Put("method", isScanIterator || siter.comparators_.size() ? "scan" : "index");
				jsonSel.Put("type", siter.TypeName());
				name << opName(it->operation, it == begin) << siter.name;
			},
			[&](const JoinSelectIterator &jiter) {
				assertrx(jiter.joinIndex < jselectors->size());
				const std::string jName{addToJSON(builder, (*jselectors)[jiter.joinIndex], it->operation)};
				name << opName(it->operation, it == begin) << jName;
			},
			[&](const FieldsComparator &c) {
				auto jsonSel = builder.Object();
				jsonSel.Put("comparators", 1);
				jsonSel.Put("field", opName(it->operation) + c.Name());
				jsonSel.Put("cost", c.Cost(iters));
				jsonSel.Put("method", "scan");
				jsonSel.Put("items", iters);
				jsonSel.Put("matched", c.GetMatchedCount());
				jsonSel.Put("type", "TwoFieldsComparison");
				name << opName(it->operation, it == begin) << c.Name();
			},
			[&](const AlwaysFalse &) {
				auto jsonSkiped = builder.Object();
				jsonSkiped.Put("type", "Skipped");
				jsonSkiped.Put("description", "always "s + (it->operation == OpNot ? "true" : "false"));
				name << opName(it->operation == OpNot ? OpAnd : it->operation, it == begin) << "Always"
					 << (it->operation == OpNot ? "True" : "False");
			});
	}
	name << ')';
	return name.str();
}

ExplainCalc::Duration ExplainCalc::lap() noexcept {
	auto now = Clock::now();
	Duration d = now - last_point_;
	last_point_ = now;
	return d;
}

int ExplainCalc::To_us(const ExplainCalc::Duration &d) noexcept { return duration_cast<microseconds>(d).count(); }

void reindexer::ExplainCalc::StartTiming() noexcept {
	if (enabled_) lap();
}

void reindexer::ExplainCalc::StopTiming() noexcept {
	if (enabled_) total_ = prepare_ + select_ + postprocess_ + loop_;
}

void reindexer::ExplainCalc::AddPrepareTime() noexcept {
	if (enabled_) prepare_ += lap();
}

void reindexer::ExplainCalc::AddSelectTime() noexcept {
	if (enabled_) select_ += lap();
}

void reindexer::ExplainCalc::AddPostprocessTime() noexcept {
	if (enabled_) postprocess_ += lap();
}

void reindexer::ExplainCalc::AddLoopTime() noexcept {
	if (enabled_) loop_ += lap();
}

void reindexer::ExplainCalc::StartSort() noexcept {
	if (enabled_) sort_start_point_ = Clock::now();
}

void reindexer::ExplainCalc::StopSort() noexcept {
	if (enabled_) sort_ = Clock::now() - sort_start_point_;
}

void reindexer::ExplainCalc::AddIterations(int iters) noexcept { iters_ += iters; }
void reindexer::ExplainCalc::PutSortIndex(std::string_view index) noexcept { sortIndex_ = index; }
void ExplainCalc::PutSelectors(SelectIteratorContainer *qres) noexcept { selectors_ = qres; }
void ExplainCalc::PutJoinedSelectors(JoinedSelectors *jselectors) noexcept { jselectors_ = jselectors; }

}  // namespace reindexer
