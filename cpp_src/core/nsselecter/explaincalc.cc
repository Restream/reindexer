#include "explaincalc.h"
#include "core/cbinding/reindexer_ctypes.h"
#include "core/cjson/jsonbuilder.h"
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
			for (SelectIterator &s : *selectors_) {
				logPrintf(LogInfo, "%s: %d idsets, %d comparators, cost %g, matched %d, %s", s.name, s.size(), s.comparators_.size(),
						  s.Cost(iters_), s.GetMatchedCount(), s.Dump());
			}
		}

		if (jselectors_) {
			for (auto &js : *jselectors_) {
				if (js.type == JoinType::LeftJoin || js.type == JoinType::Merge) {
					logPrintf(LogInfo, "%s %s: called %d\n", Query::JoinTypeName(js.type), js.ns, js.called);
				} else {
					logPrintf(LogInfo, "%s %s: called %d, matched %d\n", Query::JoinTypeName(js.type), js.ns, js.called, js.matched);
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
			for (SelectIterator &s : *selectors_) {
				bool isScanIterator = bool(s.name == "-scan");
				auto jsonSel = jsonSelArr.Object();
				if (!isScanIterator) {
					jsonSel.Put("field", s.name);
					jsonSel.Put("keys", s.size());
					jsonSel.Put("comparators", s.comparators_.size());
					jsonSel.Put("cost", s.Cost(iters_));
				} else
					jsonSel.Put("items", s.GetMaxIterations());
				jsonSel.Put("matched", s.GetMatchedCount());
				jsonSel.Put("method", isScanIterator || s.comparators_.size() ? "scan" : "index");
			}
		}

		if (jselectors_) {
			for (JoinedSelector &js : *jselectors_) {
				auto jsonSel = jsonSelArr.Object();
				jsonSel.Put("field", js.ns);
				jsonSel.Put("method", JoinTypeName(js.type));
				jsonSel.Put("matched", js.matched);
			}
		}
	}

	return ser.Slice().ToString();
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
void ExplainCalc::PutSelectors(h_vector<SelectIterator> *qres) { selectors_ = qres; }
void ExplainCalc::PutJoinedSelectors(JoinedSelectors *jselectors) { jselectors_ = jselectors; }

}  // namespace reindexer
