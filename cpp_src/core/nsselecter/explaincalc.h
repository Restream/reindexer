#pragma once

#include <string_view>
#include <variant>
#include <vector>

#include "core/type_consts.h"
#include "tools/clock.h"
#include "tools/serializer.h"

namespace reindexer {

class SelectIteratorContainer;
class JoinedSelector;
struct JoinOnInjection;
struct ConditionInjection;

typedef std::vector<JoinedSelector> JoinedSelectors;
typedef std::vector<JoinOnInjection> OnConditionInjections;

class [[nodiscard]] SubQueryExplain {
public:
	SubQueryExplain(const std::string& ns, std::string&& exp) : explain_{std::move(exp)}, namespace_{ns} {}
	const std::string& NsName() const& noexcept { return namespace_; }
	const auto& FieldOrKeys() const& noexcept { return fieldOrKeys_; }
	const std::string& Explain() const& noexcept { return explain_; }
	void SetFieldOrKeys(std::variant<std::string, size_t>&& fok) noexcept { fieldOrKeys_ = std::move(fok); }

	auto NsName() const&& = delete;
	auto FieldOrKeys() const&& = delete;
	auto Explain() const&& = delete;

private:
	std::string explain_;
	std::string namespace_;
	std::variant<std::string, size_t> fieldOrKeys_{size_t(0)};
};

class [[nodiscard]] ExplainCalc {
public:
	typedef system_clock_w Clock;
	typedef Clock::duration Duration;

private:
	typedef Clock::time_point time_point;

public:
	ExplainCalc() = default;
	explicit ExplainCalc(bool enable) noexcept : enabled_(enable) {}

	void StartTiming() noexcept {
		if (enabled_) {
			lap();
		}
	}
	void StopTiming() noexcept {
		if (enabled_) {
			total_ = preselect_ + prepare_ + select_ + postprocess_ + loop_;
		}
	}
	void AddPrepareTime() noexcept {
		if (enabled_) {
			prepare_ += lap();
		}
	}
	void AddSelectTime() noexcept {
		if (enabled_) {
			select_ += lap();
		}
	}
	void AddPostprocessTime() noexcept {
		if (enabled_) {
			postprocess_ += lap();
		}
	}
	void AddLoopTime() noexcept {
		if (enabled_) {
			loop_ += lap();
		}
	}
	void AddIterations(int iters) noexcept { iters_ += iters; }
	void StartSort() noexcept {
		if (enabled_) {
			sort_start_point_ = Clock::now();
		}
	}
	void StopSort() noexcept {
		if (enabled_) {
			sort_ = Clock::now() - sort_start_point_;
		}
	}

	void PutCount(int cnt) noexcept { count_ = cnt; }
	void PutSortIndex(std::string_view index) noexcept { sortIndex_ = index; }
	void PutSelectors(const SelectIteratorContainer* qres) noexcept { selectors_ = qres; }
	void PutJoinedSelectors(const JoinedSelectors* jselectors) noexcept { jselectors_ = jselectors; }
	void SetPreselectTime(Duration preselectTime) noexcept { preselect_ = preselectTime; }
	void PutOnConditionInjections(const OnConditionInjections* onCondInjections) noexcept { onInjections_ = onCondInjections; }
	void SetSortOptimization(bool enable) noexcept { sortOptimization_ = enable; }
	void SetSubQueriesExplains(std::vector<SubQueryExplain>&& subQueriesExpl) noexcept { subqueries_ = std::move(subQueriesExpl); }

	void LogDump(int logLevel);
	std::string GetJSON();

	Duration Total() const noexcept { return total_; }
	Duration Prepare() const noexcept { return prepare_; }
	Duration Indexes() const noexcept { return select_; }
	Duration Postprocess() const noexcept { return postprocess_; }
	Duration Loop() const noexcept { return loop_; }
	Duration Sort() const noexcept { return sort_; }

	size_t Iterations() const noexcept { return iters_; }
	bool IsEnabled() const noexcept { return enabled_; }

	static int To_us(const Duration& d) noexcept;

private:
	Duration lap() noexcept {
		const auto now = Clock::now();
		Duration d = now - last_point_;
		last_point_ = now;
		return d;
	}

	time_point last_point_, sort_start_point_;
	Duration total_ = Duration::zero();
	Duration prepare_ = Duration::zero();
	Duration preselect_ = Duration::zero();
	Duration select_ = Duration::zero();
	Duration postprocess_ = Duration::zero();
	Duration loop_ = Duration::zero();
	Duration sort_ = Duration::zero();

	std::string_view sortIndex_;
	const SelectIteratorContainer* selectors_ = nullptr;
	const JoinedSelectors* jselectors_ = nullptr;
	const OnConditionInjections* onInjections_ = nullptr;  ///< Optional
	std::vector<SubQueryExplain> subqueries_;

	int iters_ = 0;
	int count_ = 0;
	bool sortOptimization_ = false;
	bool enabled_ = false;
};

/**
 * @brief Describes the process of a single JOIN-query ON-conditions injection into the Where clause of a main query
 */
struct [[nodiscard]] JoinOnInjection {
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
struct [[nodiscard]] ConditionInjection {
	std::string initCond;  ///< single condition from Join ON section. SQL-like string
	ExplainCalc::Duration totalTime_ =
		ExplainCalc::Duration::zero();		///< total time elapsed from injection attempt start till the end of substitution or rejection
	std::string explain;					///< optional{JoinOnInjection.type == Select}. Explain raw string from Select subquery.
	AggType aggType = AggType::AggUnknown;	///< aggregation type used in subquery
	bool succeed = false;					///< result of injection attempt
	std::string_view reason;				///< optional{succeed==false}. Explains condition injection failure
	bool orChainPart_ = false;				///< additional failure reason flag. Used in case if condition field was filled before  and
											///< also it does not fit because it is an OR chain part
	std::string newCond;					///< substituted condition in QueryEntry. SQL-like string
	size_t valuesCount = 0;					///< resulting size of query values set
};

}  // namespace reindexer
