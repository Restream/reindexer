#pragma once

#include <chrono>
#include <vector>

#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/string_view.h"

namespace reindexer {

class SelectIteratorContainer;
class JoinedSelector;
typedef std::vector<JoinedSelector> JoinedSelectors;

class ExplainCalc {
	typedef std::chrono::high_resolution_clock clock;
	typedef clock::duration duration;
	typedef clock::time_point time_point;

public:
	ExplainCalc(bool enable) : enabled_(enable) {}

	void StartTiming();
	void StopTiming();

	void SetPrepareTime();
	void SetSelectTime();
	void SetPostprocessTime();
	void SetLoopTime();
	void SetIterations(int iters);
	void StartSort();
	void StopSort();

	void PutCount(int cnt) { count_ = cnt; }
	void PutSortIndex(string_view index);
	void PutSelectors(SelectIteratorContainer *qres);
	void PutJoinedSelectors(JoinedSelectors *jselectors);
	void SetSortOptimization(bool enable) {sortOptimization_ = enable;}

	void LogDump(int logLevel);
	std::string GetJSON();

protected:
	duration lap();
	static int to_us(const duration &d);
	static const char *JoinTypeName(JoinType jtype);

protected:
	time_point last_point_, sort_start_point_;
	duration total_, prepare_, select_, postprocess_, loop_, sort_ = duration::zero();

	string_view sortIndex_;
	SelectIteratorContainer *selectors_ = nullptr;
	JoinedSelectors *jselectors_ = nullptr;
	bool sortOptimization_ = false;
	int iters_ = 0;
	int count_ = 0;
	bool enabled_;
};

}  // namespace reindexer
