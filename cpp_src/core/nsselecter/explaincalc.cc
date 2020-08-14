#include "explaincalc.h"
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
	if (logLevel >= LogInfo) {
		logPrintf(LogInfo,
				  "Got %d items in %d µs [prepare %d µs, select %d µs, postprocess %d µs loop %d µs, general sort %d µs], sortindex %s",
				  count_, To_us(total_), To_us(prepare_), To_us(select_), To_us(postprocess_), To_us(loop_), To_us(sort_), sortIndex_);
	}

	if (logLevel >= LogTrace) {
		if (selectors_) {
			selectors_->ForEachIterator([this](const SelectIterator &s) {
				logPrintf(LogInfo, "%s: %d idsets, %d comparators, cost %g, matched %d, %s", s.name, s.size(), s.comparators_.size(),
						  s.Cost(iters_), s.GetMatchedCount(), s.Dump());
			});
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

static const char *joinTypeName(JoinType type) {
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

static void addToJSON(JsonBuilder &builder, const JoinedSelector &js) {
	auto jsonSel = builder.Object();
	jsonSel.Put("field", joinTypeName(js.Type()) + js.RightNsName());
	jsonSel.Put("matched", js.Matched());
	jsonSel.Put("selects_count", js.Called());
	jsonSel.Put("join_select_total", ExplainCalc::To_us(js.PreResult()->selectTime));
	switch (js.Type()) {
		case JoinType::InnerJoin:
		case JoinType::OrInnerJoin:
		case JoinType::LeftJoin:
			assert(js.PreResult());
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
		default:
			break;
	}
}

string ExplainCalc::GetJSON() {
	WrSerializer ser;
	{
		JsonBuilder json(ser);
		json.Put("total_us", To_us(total_));
		json.Put("prepare_us", To_us(prepare_));
		json.Put("indexes_us", To_us(select_));
		json.Put("postprocess_us", To_us(postprocess_));
		json.Put("loop_us", To_us(loop_));
		json.Put("general_sort_us", To_us(sort_));
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

	return string(ser.Slice());
}

void SelectIteratorContainer::explainJSON(const_iterator it, const_iterator end, int iters, JsonBuilder &builder,
										  const JoinedSelectors *jselectors) {
	for (; it != end; ++it) {
		if (it->IsLeaf()) {
			const SelectIterator &siter = it->Value();
			if (jselectors && !siter.joinIndexes.empty()) {
				const size_t jIdx = siter.joinIndexes[0];
				assert(jIdx < jselectors->size());
				if ((*jselectors)[jIdx].Type() == JoinType::InnerJoin) {
					addToJSON(builder, (*jselectors)[jIdx]);
				}
			}
			if (!siter.name.empty() || siter.joinIndexes.empty()) {
				auto jsonSel = builder.Object();
				bool isScanIterator = bool(siter.name == "-scan");
				if (!isScanIterator) {
					jsonSel.Put("field", (it->operation == OpNot ? "not " : "") + siter.name);
					jsonSel.Put("keys", siter.size());
					jsonSel.Put("comparators", siter.comparators_.size());
					jsonSel.Put("cost", siter.Cost(iters));
				} else {
					jsonSel.Put("items", siter.GetMaxIterations());
				}
				jsonSel.Put("matched", siter.GetMatchedCount());
				jsonSel.Put("method", isScanIterator || siter.comparators_.size() ? "scan" : "index");
				jsonSel.Put("type", siter.TypeName());
			}
			if (jselectors) {
				for (size_t i = 0; i < siter.joinIndexes.size(); ++i) {
					const size_t jIdx = siter.joinIndexes[i];
					assert(jIdx < jselectors->size());
					if (((*jselectors)[jIdx].Type() == JoinType::InnerJoin && i != 0) ||
						(*jselectors)[jIdx].Type() == JoinType::OrInnerJoin) {
						addToJSON(builder, (*jselectors)[jIdx]);
					}
				}
			}
		} else {
			auto jsonSel = builder.Object();
			auto jsonSelArr = jsonSel.Array("selectors");
			explainJSON(it.cbegin(), it.cend(), iters, jsonSelArr, jselectors);
		}
	}
}

ExplainCalc::Duration ExplainCalc::lap() {
	auto now = Clock::now();
	Duration d = now - last_point_;
	last_point_ = now;
	return d;
}

int ExplainCalc::To_us(const ExplainCalc::Duration &d) { return duration_cast<microseconds>(d).count(); }

void reindexer::ExplainCalc::StartTiming() {
	if (enabled_) lap();
}

void reindexer::ExplainCalc::StopTiming() {
	if (enabled_) total_ = prepare_ + select_ + postprocess_ + loop_;
}

void reindexer::ExplainCalc::AddPrepareTime() {
	if (enabled_) prepare_ += lap();
}

void reindexer::ExplainCalc::AddSelectTime() {
	if (enabled_) select_ += lap();
}

void reindexer::ExplainCalc::AddPostprocessTime() {
	if (enabled_) postprocess_ += lap();
}

void reindexer::ExplainCalc::AddLoopTime() {
	if (enabled_) loop_ += lap();
}

void reindexer::ExplainCalc::StartSort() {
	if (enabled_) sort_start_point_ = Clock::now();
}

void reindexer::ExplainCalc::StopSort() {
	if (enabled_) sort_ = Clock::now() - sort_start_point_;
}

void reindexer::ExplainCalc::AddIterations(int iters) { iters_ += iters; }
void reindexer::ExplainCalc::PutSortIndex(string_view index) { sortIndex_ = index; }
void ExplainCalc::PutSelectors(SelectIteratorContainer *qres) { selectors_ = qres; }
void ExplainCalc::PutJoinedSelectors(JoinedSelectors *jselectors) { jselectors_ = jselectors; }

}  // namespace reindexer
