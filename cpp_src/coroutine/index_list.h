#pragma once

#include <type_traits>
#include <utility>
#include "routine_t.h"
#include "tools/assertrx.h"

namespace reindexer::coroutine {

/// Index-based intrusive containers used by the ordinator to thread its routine slots (free-list, deferred-resume stack,
/// call stack) without any auxiliary storage. Store 1-based routine_t ids and reach the link fields through a stateless Accessor over a
/// contiguous Storage, which keeps them valid across Storage reallocations.

template <typename Storage, typename Accessor>
class [[nodiscard]] index_stack {
	static_assert(std::is_same_v<decltype(std::declval<Accessor>()(std::declval<Storage&>(), routine_t{})), routine_t&>,
				  "index_stack accessor must return a mutable routine_t& (the threaded next-link)");

public:
	bool empty() const noexcept { return head_ == 0; }
	routine_t head() const noexcept { return head_; }

	void push(Storage& storage, routine_t id) noexcept {
		Accessor next;
		assertrx_dbg(id);
		assertrx_dbg(next(storage, id) == 0);
		next(storage, id) = head_;
		head_ = id;
	}

	routine_t pop(Storage& storage) noexcept {
		Accessor next;
		const routine_t id = head_;
		if (id) {
			head_ = next(storage, id);
			next(storage, id) = 0;
		}
		return id;
	}

	void reset() noexcept { head_ = 0; }

private:
	routine_t head_ = 0;
};

template <typename Storage, typename NextAccessor, typename PrevAccessor, typename LinkedAccessor>
class [[nodiscard]] index_list {
	static_assert(std::is_same_v<decltype(std::declval<NextAccessor>()(std::declval<Storage&>(), routine_t{})), routine_t&>,
				  "index_list NextAccessor must return a mutable routine_t&");
	static_assert(std::is_same_v<decltype(std::declval<PrevAccessor>()(std::declval<Storage&>(), routine_t{})), routine_t&>,
				  "index_list PrevAccessor must return a mutable routine_t&");
	static_assert(std::is_same_v<decltype(std::declval<LinkedAccessor>()(std::declval<Storage&>(), routine_t{})), bool&>,
				  "index_list LinkedAccessor must return a mutable bool&");

public:
	bool empty() const noexcept { return tail_ == 0; }

	void push_back(Storage& storage, routine_t id) noexcept {
		NextAccessor next;
		PrevAccessor prev;
		LinkedAccessor linked;
		assertrx_dbg(id);
		assertrx_dbg(!linked(storage, id));
		linked(storage, id) = true;
		prev(storage, id) = tail_;
		next(storage, id) = 0;
		if (tail_) {
			next(storage, tail_) = id;
		} else {
			head_ = id;
		}
		tail_ = id;
	}

	routine_t pop_back(Storage& storage) noexcept {
		const routine_t id = tail_;
		if (id) {
			erase(storage, id);
		}
		return id;
	}

	void erase(Storage& storage, routine_t id) noexcept {
		NextAccessor next;
		PrevAccessor prev;
		LinkedAccessor linked;
		if (!id || !linked(storage, id)) {
			return;
		}
		const routine_t prevID = prev(storage, id);
		const routine_t nextID = next(storage, id);
		if (prevID) {
			next(storage, prevID) = nextID;
		} else {
			head_ = nextID;
		}
		if (nextID) {
			prev(storage, nextID) = prevID;
		} else {
			tail_ = prevID;
		}
		next(storage, id) = 0;
		prev(storage, id) = 0;
		linked(storage, id) = false;
	}

	void clear(Storage& storage) noexcept {
		while (head_) {
			erase(storage, head_);
		}
	}

private:
	routine_t head_ = 0;
	routine_t tail_ = 0;
};

}  // namespace reindexer::coroutine
