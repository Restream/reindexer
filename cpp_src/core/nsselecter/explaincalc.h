#pragma once

#include <chrono>
#include <vector>

#include "estl/h_vector.h"
#include "estl/string_view.h"
#include "selectiterator.h"

using std::vector;

namespace reindexer {

class SelectIterator;
struct JoinedSelector;
typedef vector<JoinedSelector> JoinedSelectors;

class ExplainCalc {
	typedef std::chrono::high_resolution_clock clock;
	typedef clock::duration duration;
	typedef clock::time_point time_point;

public:
	ExplainCalc(bool enable) : enabled_(enable), started_(false) {}

	void StartTiming();
	void StopTiming();

	void SetPrepareTime();
	void SetSelectTime();
	void SetPostprocessTime();
	void SetLoopTime();
	void SetIterations(int iters);

	void PutCount(int cnt) { count_ = cnt; }
	void PutSortIndex(string_view index);
	void PutSelectors(h_vector<SelectIterator> *qres);
	void PutJoinedSelectors(JoinedSelectors *jselectors);

	void LogDump(int logLevel);
	string GetJSON();

protected:
	duration lap();
	static int to_us(const duration &d);
	static const char *JoinTypeName(JoinType jtype);

protected:
	time_point last_point_, pause_point_;
	duration total_, prepare_, select_, postprocess_, loop_;

	string_view sortIndex_;
	h_vector<SelectIterator> *selectors_ = nullptr;
	JoinedSelectors *jselectors_ = nullptr;
	int iters_ = 0;
	int count_;
	bool enabled_;
	bool started_;
};

}  // namespace reindexer
