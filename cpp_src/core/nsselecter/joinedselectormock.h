#pragma once

#include <string_view>
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"

class JoinedNsNameMock {
public:
	JoinedNsNameMock(const char* ns) noexcept : ns_{ns} {}
	std::string_view RightNsName() const noexcept { return ns_; }

private:
	std::string_view ns_;
};

class JoinedSelectorMock {
public:
	JoinedSelectorMock(const reindexer::Query& q, std::string_view rIdx, std::string_view lIdx, CondType cond) noexcept
		: query_{q}, rightIndex_{rIdx}, leftIndex_{lIdx}, condition_{cond}, qr_{} {}
	const reindexer::Query& Query() const noexcept { return query_; }
	std::string_view RightNsName() const noexcept { return query_._namespace; }
	const std::string& RightIndexName() const noexcept { return rightIndex_; }
	const std::string& LeftIndexName() const noexcept { return leftIndex_; }
	CondType Condition() const noexcept { return condition_; }
	reindexer::QueryResults& QueryResults() noexcept { return qr_; }
	const reindexer::QueryResults& QueryResults() const noexcept { return qr_; }

private:
	const reindexer::Query& query_;
	std::string rightIndex_;
	std::string leftIndex_;
	CondType condition_;
	reindexer::QueryResults qr_;
};
