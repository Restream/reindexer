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
		logPrintf(LogInfo, "Got %d items in %d µs [prepare %d µs, select %d µs, postprocess %d µs loop %d µs], sortindex %s", count_,
				  to_us(total_), to_us(prepare_), to_us(select_), to_us(postprocess_), to_us(loop_), sortIndex_);
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

string ExplainCalc::GetJSON() {
	WrSerializer ser;
	{
		JsonBuilder json(ser);
		json.Put("total_us", to_us(total_));
		json.Put("prepare_us", to_us(prepare_));
		json.Put("indexes_us", to_us(select_));
		json.Put("postprocess_us", to_us(postprocess_));
		json.Put("loop_us", to_us(loop_));
		json.Put("sort_index", sortIndex_);
		auto jsonSelArr = json.Array("selectors");

		if (selectors_) {
			selectors_->ExplainJSON(iters_, jsonSelArr);
		}

		if (jselectors_) {
			for (JoinedSelector &js : *jselectors_) {
				auto jsonSel = jsonSelArr.Object();
				jsonSel.Put("field", js.RightNsName());
				jsonSel.Put("method", JoinTypeName(js.Type()));
				jsonSel.Put("matched", js.Matched());
			}
		}
	}

	return string(ser.Slice());
}

void SelectIteratorContainer::explainJSON(const_iterator it, const_iterator end, int iters, JsonBuilder &builder) {
	for (; it != end; ++it) {
		auto jsonSel = builder.Object();
		if (it->IsLeaf()) {
			const SelectIterator &siter = it->Value();
			bool isScanIterator = bool(siter.name == "-scan");
			if (!isScanIterator) {
				if (siter.name.empty() && !siter.joinIndexes.empty()) continue;
				jsonSel.Put("field", siter.name);
				jsonSel.Put("keys", siter.size());
				jsonSel.Put("comparators", siter.comparators_.size());
				jsonSel.Put("cost", siter.Cost(iters));
			} else {
				jsonSel.Put("items", siter.GetMaxIterations());
			}
			jsonSel.Put("matched", siter.GetMatchedCount());
			jsonSel.Put("method", isScanIterator || siter.comparators_.size() ? "scan" : "index");
			jsonSel.Put("type", siter.TypeName());
		} else {
			auto jsonSelArr = jsonSel.Array("selectors");
			explainJSON(it.cbegin(), it.cend(), iters, jsonSelArr);
		}
	}
}

ExplainCalc::duration ExplainCalc::lap() {
	auto now = clock::now();
	duration d = now - last_point_;
	last_point_ = now;
	return d;
}

int ExplainCalc::to_us(const ExplainCalc::duration &d) { return duration_cast<microseconds>(d).count(); }

const char *ExplainCalc::JoinTypeName(JoinType type) {
	switch (type) {
		case JoinType::InnerJoin:
			return "inner_join";
		case JoinType::OrInnerJoin:
			return "or_inner_join";
		case JoinType::LeftJoin:
			return "left_join";
		case JoinType::Merge:
			return "merge";
		default:
			return "<unknown>";
	}
}

void reindexer::ExplainCalc::StartTiming() {
	if (enabled_) lap();
}

void reindexer::ExplainCalc::StopTiming() {
	if (enabled_) total_ = prepare_ + select_ + postprocess_ + loop_;
}

void reindexer::ExplainCalc::SetPrepareTime() {
	if (enabled_) prepare_ = lap();
}

void reindexer::ExplainCalc::SetSelectTime() {
	if (enabled_) select_ = lap();
}

void reindexer::ExplainCalc::SetPostprocessTime() {
	if (enabled_) postprocess_ = lap();
}

void reindexer::ExplainCalc::SetLoopTime() {
	if (enabled_) loop_ = lap();
}

void reindexer::ExplainCalc::SetIterations(int iters) { iters_ = iters; }
void reindexer::ExplainCalc::PutSortIndex(string_view index) { sortIndex_ = index; }
void ExplainCalc::PutSelectors(SelectIteratorContainer *qres) { selectors_ = qres; }
void ExplainCalc::PutJoinedSelectors(JoinedSelectors *jselectors) { jselectors_ = jselectors; }

}  // namespace reindexer
