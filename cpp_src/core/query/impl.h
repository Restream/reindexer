#pragma once

#include "core/impl/impl.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"

namespace reindexer::impl {

template <>
class [[nodiscard]] Impl<const reindexer::Query&> {
public:
	explicit Impl(const reindexer::Query& q) noexcept : query_(q) {}

	const impl::Query& operator*() && {
		if (!query_.state_.ok()) {
			throw query_.state_;
		}
		return *query_.impl_;
	}

	const impl::Query* operator->() && {
		if (!query_.state_.ok()) {
			throw query_.state_;
		}
		return &*query_.impl_;
	}

	template <typename F>
		requires std::is_invocable_r_v<Error, F, const impl::Query&>
	Error TryExecute(F f) && noexcept(std::is_nothrow_invocable_v<F, const impl::Query&>) {
		if (query_.state_.ok()) {
			return f(*query_.impl_);
		} else {
			return query_.state_;
		}
	}

private:
	const reindexer::Query& query_;
};

template <>
class [[nodiscard]] Impl<reindexer::Query&&> {
public:
	explicit Impl(reindexer::Query&& q) noexcept : query_(q) {}

	impl::Query operator*() &&;
	Error&& State() && noexcept { return std::move(query_.state_); }

private:
	reindexer::Query& query_;
};

template <>
class [[nodiscard]] Impl<reindexer::Query&> {
public:
	explicit Impl(reindexer::Query& q) noexcept : query_(q) {}

	impl::Query& operator*() && {
		if (!query_.state_.ok()) {
			throw query_.state_;
		}
		return *query_.impl_;
	}

	const impl::Query* operator->() && {
		if (!query_.state_.ok()) {
			throw query_.state_;
		}
		return &*query_.impl_;
	}

	Error&& State() && noexcept { return std::move(query_.state_); }
	void operator=(impl::Query&&) &&;

private:
	reindexer::Query& query_;
};

}  // namespace reindexer::impl
