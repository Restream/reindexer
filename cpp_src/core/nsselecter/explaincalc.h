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
	ExplainCalc() = default;
	ExplainCalc(bool enable) noexcept : enabled_(enable) {}

	void StartTiming() noexcept;
	void StopTiming() noexcept;

	void AddPrepareTime() noexcept;
	void AddSelectTime() noexcept;
	void AddPostprocessTime() noexcept;
	void AddLoopTime() noexcept;
	void AddIterations(int iters) noexcept;
	void StartSort() noexcept;
	void StopSort() noexcept;

	void PutCount(int cnt) noexcept { count_ = cnt; }
	void PutSortIndex(std::string_view index) noexcept;
	void PutSelectors(SelectIteratorContainer *qres) noexcept;
	void PutJoinedSelectors(JoinedSelectors *jselectors) noexcept;
	void SetSortOptimization(bool enable) noexcept { sortOptimization_ = enable; }

	void LogDump(int logLevel);
	std::string GetJSON();

	Duration Total() const noexcept { return total_; }
	Duration Prepare() const noexcept { return prepare_; }
	Duration Indexes() const noexcept { return select_; }
	Duration Postprocess() const noexcept { return postprocess_; }
	Duration Loop() const noexcept { return loop_; }
	Duration Sort() const noexcept { return sort_; }

	size_t Iterations() const noexcept { return iters_; }
	static int To_us(const Duration &d) noexcept;
	bool IsEnabled() const noexcept { return enabled_; }

private:
	Duration lap() noexcept;
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
	bool enabled_ = false;
};

}  // namespace reindexer
