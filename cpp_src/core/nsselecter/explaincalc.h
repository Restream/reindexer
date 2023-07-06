#pragma once

#include <chrono>
#include <string_view>
#include <vector>

#include "core/type_consts.h"
#include "estl/h_vector.h"

namespace reindexer {

class SelectIteratorContainer;
class JoinedSelector;
typedef std::vector<JoinedSelector> JoinedSelectors;

class ExplainCalc {
public:
	typedef std::chrono::high_resolution_clock Clock;
	typedef Clock::duration Duration;

private:
	typedef Clock::time_point time_point;

public:
	ExplainCalc(bool enable) noexcept : enabled_(enable) {}

	void StartTiming();
	void StopTiming();

	void AddPrepareTime();
	void AddSelectTime();
	void AddPostprocessTime();
	void AddLoopTime();
	void AddIterations(int iters);
	void StartSort();
	void StopSort();

	void PutCount(int cnt) noexcept { count_ = cnt; }
	void PutSortIndex(std::string_view index);
	void PutSelectors(SelectIteratorContainer *qres);
	void PutJoinedSelectors(JoinedSelectors *jselectors);
	void SetSortOptimization(bool enable) noexcept { sortOptimization_ = enable; }

	void LogDump(int logLevel);
	std::string GetJSON();
	Duration Total() const noexcept { return total_; }
	size_t Iterations() const noexcept { return iters_; }
	static int To_us(const Duration &d);
	bool IsEnabled() const noexcept { return enabled_; }

private:
	Duration lap();
	static const char *JoinTypeName(JoinType jtype);

	time_point last_point_, sort_start_point_;
	Duration total_, prepare_ = Duration::zero();
	Duration select_ = Duration::zero();
	Duration postprocess_ = Duration::zero();
	Duration loop_ = Duration::zero();
	Duration sort_ = Duration::zero();

	std::string_view sortIndex_;
	SelectIteratorContainer *selectors_ = nullptr;
	JoinedSelectors *jselectors_ = nullptr;
	int iters_ = 0;
	int count_ = 0;
	bool sortOptimization_ = false;
	const bool enabled_;
};

}  // namespace reindexer
