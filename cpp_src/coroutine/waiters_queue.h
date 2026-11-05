#pragma once

#include <cstddef>
#include "routine_t.h"
#include "tools/assertrx.h"

namespace reindexer::coroutine {

/// Intrusive doubly-linked FIFO queue of suspended coroutines. Nodes (waiters_queue::hook) live on the stack frame of the coroutine that is
/// currently suspended: the frame stays alive while the coroutine is suspended, so the queue can reference the node without owning storage.
///
/// Unlinking responsibility is left to the consumer.
/// The single invariant the container relies on: a node must be unlinked before its stack frame is destroyed.
class [[nodiscard]] waiters_queue {
public:
	/// Intrusive node of waiters_queue. It must never be copied/moved (that would leave the queue pointing at a stale
	/// address) and must be unlinked before destruction.
	struct [[nodiscard]] hook {
		explicit hook(routine_t routine) noexcept : id(routine) {}
		hook(const hook&) = delete;
		hook& operator=(const hook&) = delete;
		hook(hook&&) = delete;
		hook& operator=(hook&&) = delete;
		~hook() { assertrx_dbg(!linked()); }

		bool linked() const noexcept { return prev || next || owner; }

		routine_t id = 0;
		hook* prev = nullptr;
		hook* next = nullptr;
		const void* owner = nullptr;
	};

	~waiters_queue() { assertrx_dbg(empty()); }

	bool empty() const noexcept { return head_ == nullptr; }
	size_t size() const noexcept { return size_; }
	routine_t front() const noexcept {
		assertrx_dbg(head_);
		return head_->id;
	}
	hook& front_hook() noexcept {
		assertrx_dbg(head_);
		return *head_;
	}

	void push_back(hook& node) noexcept {
		assertrx_dbg(!node.linked());
		node.owner = this;
		node.prev = tail_;
		node.next = nullptr;
		if (tail_) {
			tail_->next = &node;
		} else {
			head_ = &node;
		}
		tail_ = &node;
		++size_;
	}

	void erase(hook& node) noexcept {
		assertrx_dbg(node.owner == this);
		if (node.prev) {
			node.prev->next = node.next;
		} else {
			head_ = node.next;
		}
		if (node.next) {
			node.next->prev = node.prev;
		} else {
			tail_ = node.prev;
		}
		node.prev = nullptr;
		node.next = nullptr;
		node.owner = nullptr;
		assertrx_dbg(size_);
		--size_;
	}

private:
	hook* head_ = nullptr;
	hook* tail_ = nullptr;
	size_t size_ = 0;
};

}  // namespace reindexer::coroutine
