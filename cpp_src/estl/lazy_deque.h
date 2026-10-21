#pragma once

#include <deque>
#include <optional>
#include "tools/assertrx.h"

namespace reindexer {

// std::deque performs 2 allocations in default contructor. lazy_deque allows to delay this allocations until the moment, when container
// will actually be used.
// lazy_deque performs regular optional check and will be slower in some scenarious.
template <typename T, typename AllocT = std::allocator<T>>
class [[nodiscard]] lazy_deque {
public:
	using reference = typename std::deque<T, AllocT>::reference;
	using const_reference = typename std::deque<T, AllocT>::const_reference;
	using size_type = typename std::deque<T, AllocT>::size_type;
	using value_type = typename std::deque<T, AllocT>::value_type;

	lazy_deque() = default;
	lazy_deque(lazy_deque&&) = default;
	lazy_deque(const lazy_deque&) = default;
	lazy_deque& operator=(lazy_deque&&) = default;
	lazy_deque& operator=(const lazy_deque&) = default;

	reference operator[](size_type n) noexcept {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return (*container_)[n];
	}
	const_reference operator[](size_type n) const noexcept {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return (*container_)[n];
	}
	reference at(size_type n) {
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return container_.value().at(n);
	}
	const_reference at(size_type n) const { return container_.value().at(n); }
	size_type size() const noexcept { return container_.has_value() ? container_->size() : 0; }
	bool empty() const noexcept { return !size(); }

	void pop_back() {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		container_->pop_back();
	}
	void pop_front() {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		container_->pop_front();
	}

	void push_front(value_type&& x) { std::ignore = emplace_front(std::move(x)); }
	template <typename... Args>
	reference emplace_front(Args&&... args) {
		return safe_get().emplace_front(std::forward<Args>(args)...);
	}
	void push_back(value_type&& x) { std::ignore = emplace_back(std::move(x)); }
	template <typename... Args>
	reference emplace_back(Args&&... args) {
		return safe_get().emplace_back(std::forward<Args>(args)...);
	}

	reference front() noexcept {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return container_->front();
	}
	const_reference front() const noexcept {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return container_->front();
	}
	reference back() noexcept {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return container_->back();
	}
	const_reference back() const noexcept {
		assertrx_dbg(container_.has_value());
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access)
		return container_->back();
	}

private:
	std::deque<T, AllocT>& safe_get() { return container_.has_value() ? *container_ : container_.emplace(); }

	std::optional<std::deque<T, AllocT>> container_;
};

}  // namespace reindexer
