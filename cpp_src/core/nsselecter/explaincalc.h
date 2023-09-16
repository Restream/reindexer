#pragma once

#include <chrono>
#include <string_view>
#include <vector>

#include "core/type_consts.h"
#include "tools/serializer.h"

namespace reindexer {

class SelectIteratorContainer;
class JoinedSelector;
struct JoinOnInjection;
struct ConditionInjection;

typedef std::vector<JoinedSelector> JoinedSelectors;
typedef std::vector<JoinOnInjection> OnConditionInjections;

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
	void AddIterations(int iters) noexcept { iters_ += iters; }
	void StartSort() noexcept;
	void StopSort() noexcept;

	void PutCount(int cnt) noexcept { count_ = cnt; }
	void PutSortIndex(std::string_view index) noexcept { sortIndex_ = index; }
	void PutSelectors(const SelectIteratorContainer *qres) noexcept { selectors_ = qres; }
	void PutJoinedSelectors(const JoinedSelectors *jselectors) noexcept { jselectors_ = jselectors; }
	void SetPreselectTime(Duration preselectTime) noexcept { preselect_ = preselectTime; }
	void PutOnConditionInjections(const OnConditionInjections *onCondInjections) noexcept { onInjections_ = onCondInjections; }
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

	time_point last_point_, sort_start_point_;
	Duration total_, prepare_ = Duration::zero();
	Duration preselect_ = Duration::zero();
	Duration select_ = Duration::zero();
	Duration postprocess_ = Duration::zero();
	Duration loop_ = Duration::zero();
	Duration sort_ = Duration::zero();

	std::string_view sortIndex_;
	const SelectIteratorContainer *selectors_ = nullptr;
	const JoinedSelectors *jselectors_ = nullptr;
	const OnConditionInjections *onInjections_ = nullptr;  ///< Optional

	int iters_ = 0;
	int count_ = 0;
	bool sortOptimization_ = false;
	bool enabled_ = false;
};

/**
 * @brief Describes the process of a single JOIN-query ON-conditions injection into the Where clause of a main query
 */
struct JoinOnInjection {
	std::string_view rightNsName;  ///< joinable ns name
	std::string joinCond;		   ///< original ON-conditions clause. SQL-like string
	ExplainCalc::Duration totalTime_ =
		ExplainCalc::Duration::zero();			 ///< total amount of time spent on checking and substituting all conditions
	bool succeed = false;						 ///< result of injection attempt
	std::string_view reason;					 ///< optional{succeed==false}. Explains condition injection failure
	enum { ByValue, Select } type = ByValue;	 ///< byValue or Select
	WrSerializer injectedCond;					 ///< injected condition. SQL-like string
	std::vector<ConditionInjection> conditions;	 ///< individual conditions processing results
};

/**
 * @brief Describes an injection attempt of a single condition from the ON-clause of a JOIN-query
 */
struct ConditionInjection {
	std::string initCond;  ///< single condition from Join ON section. SQL-like string
	ExplainCalc::Duration totalTime_ =
		ExplainCalc::Duration::zero();		///< total time elapsed from injection attempt start till the end of substitution or rejection
	std::string explain;					///< optoinal{JoinOnInjection.type == Select}. Explain raw string from Select subquery.
	AggType aggType = AggType::AggUnknown;	///< aggregation type used in subquery
	bool succeed = false;					///< result of injection attempt
	std::string_view reason;				///< optional{succeed==false}. Explains condition injection failure
	bool orChainPart_ = false;				///< additional failure reason flag. Used in case if condition field was filled before  and
											///< also it does not fit because it is an OR chain part
	std::string newCond;					///< substituted condition in QueryEntry. SQL-like string
	size_t valuesCount = 0;					///< resulting size of query values set
};

}  // namespace reindexer
