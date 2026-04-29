#pragma once

#include <string_view>
#include <variant>
#include <vector>

#include "core/type_consts.h"
#include "tools/clock.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {

class SelectIteratorContainer;
struct JoinOnInsertion;
struct ConditionInsertion;

namespace joins {
class ItemsProcessor;
}

typedef std::vector<joins::ItemsProcessor> ItemsProcessors;
typedef std::vector<JoinOnInsertion> OnConditionInsertions;

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

class [[nodiscard]] BasicExplainResults {
public:
	typedef system_clock_w Clock;
	typedef Clock::duration Duration;

	void Append(const BasicExplainResults& other) noexcept;
	void GetJSON(JsonBuilder& json) const;

	Duration total = Duration::zero();
	Duration prepare = Duration::zero();
	Duration preselect = Duration::zero();
	Duration select = Duration::zero();
	Duration postprocess = Duration::zero();
	Duration loop = Duration::zero();
	Duration sort = Duration::zero();
};

class [[nodiscard]] SingleQueryExplainCalc {
public:
	using Duration = BasicExplainResults::Duration;
	using Clock = BasicExplainResults::Clock;

private:
	typedef Clock::time_point TimePoint;

public:
	explicit SingleQueryExplainCalc(std::string_view nsName, bool enable) noexcept : nsName_{nsName}, enabled_{enable} {}
	SingleQueryExplainCalc(const SingleQueryExplainCalc&) = delete;
	SingleQueryExplainCalc(SingleQueryExplainCalc&&) = delete;
	SingleQueryExplainCalc& operator=(const SingleQueryExplainCalc&) = delete;
	SingleQueryExplainCalc& operator=(SingleQueryExplainCalc&&) = delete;

	void StartTiming() noexcept {
		if (enabled_) [[unlikely]] {
			std::ignore = lap();
		}
	}
	void StopTiming() noexcept {
		if (enabled_) [[unlikely]] {
			basics_.total = basics_.preselect + basics_.prepare + basics_.select + basics_.postprocess + basics_.loop;
		}
	}
	void AddPrepareTime() noexcept {
		if (enabled_) [[unlikely]] {
			basics_.prepare += lap();
		}
	}
	void AddSelectTime() noexcept {
		if (enabled_) [[unlikely]] {
			basics_.select += lap();
		}
	}
	void AddPostprocessTime() noexcept {
		if (enabled_) [[unlikely]] {
			basics_.postprocess += lap();
		}
	}
	void AddLoopTime() noexcept {
		if (enabled_) [[unlikely]] {
			basics_.loop += lap();
		}
	}
	void AddIterations(int iters) noexcept { iters_ += iters; }
	void StartSort() noexcept {
		if (enabled_) [[unlikely]] {
			sort_start_point_ = Clock::now();
		}
	}
	void StopSort() noexcept {
		if (enabled_) [[unlikely]] {
			basics_.sort = Clock::now() - sort_start_point_;
		}
	}

	const BasicExplainResults& Basics() const& noexcept { return basics_; }
	const BasicExplainResults& Basics() const&& = delete;

	void PutCount(int cnt) noexcept { count_ = cnt; }
	void PutSortIndex(std::string_view index) noexcept { sortIndex_ = index; }
	void PutSelectors(const SelectIteratorContainer* qres) noexcept { selectors_ = qres; }
	void PutItemsProcessors(const ItemsProcessors* jitemsprocessors) noexcept { jitemsprocessors_ = jitemsprocessors; }
	void SetPreselectTime(Duration preselectTime) noexcept { basics_.preselect = preselectTime; }
	void PutOnConditionInsertions(const OnConditionInsertions* onCondInsertions) noexcept { onInsertions_ = onCondInsertions; }
	void SetSortOptimization(bool enable) noexcept { sortOptimization_ = enable; }
	void SetSubQueriesExplains(std::vector<SubQueryExplain>&& subQueriesExpl) noexcept { subqueries_ = std::move(subQueriesExpl); }

	void LogDump(int logLevel);
	std::string GetJSON() const;

	Duration Total() const noexcept { return basics_.total; }
	Duration Prepare() const noexcept { return basics_.prepare; }
	Duration Preselect() const noexcept { return basics_.preselect; }
	Duration Indexes() const noexcept { return basics_.select; }
	Duration Postprocess() const noexcept { return basics_.postprocess; }
	Duration Loop() const noexcept { return basics_.loop; }
	Duration Sort() const noexcept { return basics_.sort; }

	size_t Iterations() const noexcept { return iters_; }
	bool IsEnabled() const noexcept { return enabled_; }

private:
	Duration lap() noexcept;

	TimePoint last_point_, sort_start_point_;
	BasicExplainResults basics_;

	const std::string_view nsName_;
	std::string_view sortIndex_;
	const SelectIteratorContainer* selectors_ = nullptr;
	const ItemsProcessors* jitemsprocessors_ = nullptr;
	const OnConditionInsertions* onInsertions_ = nullptr;  ///< Optional
	std::vector<SubQueryExplain> subqueries_;

	int iters_ = 0;
	int count_ = 0;
	bool sortOptimization_ = false;
	bool enabled_ = false;
};

class [[nodiscard]] Explain {
public:
	using Duration = BasicExplainResults::Duration;
	using Clock = BasicExplainResults::Clock;

	Duration Total() const noexcept { return aggregated_.total; }
	Duration Prepare() const noexcept { return aggregated_.prepare; }
	Duration Preselect() const noexcept { return aggregated_.preselect; }
	Duration Indexes() const noexcept { return aggregated_.select; }
	Duration Postprocess() const noexcept { return aggregated_.postprocess; }
	Duration Loop() const noexcept { return aggregated_.loop; }
	Duration Sort() const noexcept { return aggregated_.sort; }
	void AddSortTime(Duration time) noexcept { aggregated_.sort += time; }

	void Append(const SingleQueryExplainCalc& explain);
	std::string GetJSON() const;

private:
	BasicExplainResults aggregated_;
	std::vector<std::string> mergedExplainJSONs_;
};

/**
 * @brief Describes the process of a single JOIN-query ON-conditions insertion into the Where clause of a main query
 */
struct [[nodiscard]] JoinOnInsertion {
	std::string_view rightNsName;							   ///< joinable ns name
	std::string joinCond;									   ///< original ON-conditions clause. SQL-like string
	Explain::Duration totalTime_ = Explain::Duration::zero();  ///< total amount of time spent on checking and substituting all conditions
	bool succeed = false;									   ///< result of insertion attempt
	std::string_view reason;								   ///< optional{succeed==false}. Explains condition insertion failure
	enum { ByValue, Select } type = ByValue;				   ///< byValue or Select
	WrSerializer insertedCond;								   ///< inserted condition. SQL-like string
	std::vector<ConditionInsertion> conditions;				   ///< individual conditions processing results
};

/**
 * @brief Describes an insertion attempt of a single condition from the ON-clause of a JOIN-query
 */
struct [[nodiscard]] ConditionInsertion {
	std::string initCond;  ///< single condition from Join ON section. SQL-like string
	Explain::Duration totalTime_ =
		Explain::Duration::zero();			///< total time elapsed from insertion attempt start till the end of substitution or rejection
	std::string explain;					///< optional{JoinOnInsertion.type == Select}. Explain raw string from Select subquery.
	AggType aggType = AggType::AggUnknown;	///< aggregation type used in subquery
	bool succeed = false;					///< result of insertion attempt
	std::string_view reason;				///< optional{succeed==false}. Explains condition insertion failure
	bool orChainPart_ = false;				///< additional failure reason flag. Used in case if condition field was filled before  and
											///< also it does not fit because it is an OR chain part
	std::string newCond;					///< substituted condition in QueryEntry. SQL-like string
	size_t valuesCount = 0;					///< resulting size of query values set
};

}  // namespace reindexer
