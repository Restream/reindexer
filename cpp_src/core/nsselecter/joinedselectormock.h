#pragma once

#include <string_view>
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"

class [[nodiscard]] JoinedNsNameMock {
public:
	JoinedNsNameMock(const char* ns) noexcept : ns_{ns} {}
	std::string_view RightNsName() const noexcept { return ns_; }

private:
	std::string_view ns_;
};

class [[nodiscard]] JoinedSelectorMock {
public:
	JoinedSelectorMock(JoinType jt, reindexer::JoinedQuery q, unsigned limit, unsigned offset)
		: query_{std::move(q)}, qr_{}, joinType_{jt}, limit_{limit}, offset_{offset} {}
	const reindexer::JoinedQuery& JoinQuery() const noexcept { return query_; }
	const std::string& RightNsName() const noexcept { return query_.NsName(); }
	reindexer::QueryResults& QueryResults() noexcept { return qr_; }
	const reindexer::QueryResults& QueryResults() const noexcept { return qr_; }
	JoinType Type() const noexcept { return joinType_; }
	unsigned Limit() const noexcept { return limit_; }
	unsigned Offset() const noexcept { return offset_; }

private:
	reindexer::JoinedQuery query_;
	reindexer::QueryResults qr_;
	JoinType joinType_;
	unsigned limit_;
	unsigned offset_;
};
