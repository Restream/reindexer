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
	JoinedSelectorMock(JoinType jt, reindexer::JoinedQuery q) : query_{std::move(q)}, qr_{}, joinType_{jt} {}
	const reindexer::JoinedQuery& JoinQuery() const noexcept { return query_; }
	const std::string& RightNsName() const noexcept { return query_._namespace; }
	reindexer::QueryResults& QueryResults() noexcept { return qr_; }
	const reindexer::QueryResults& QueryResults() const noexcept { return qr_; }
	JoinType Type() const noexcept { return joinType_; }

private:
	reindexer::JoinedQuery query_;
	reindexer::QueryResults qr_;
	JoinType joinType_;
};
