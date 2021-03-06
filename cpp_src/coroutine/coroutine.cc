#include "coroutine.h"
#include <cassert>
#include <chrono>
#include <cstdio>
#include <thread>

#ifdef REINDEX_WITH_TSAN_FIBERS
#include <sanitizer/tsan_interface.h>
#endif	// REINDEX_WITH_TSAN_FIBERS

namespace reindexer {
namespace coroutine {

static void static_entry(transfer_t from) { ordinator::instance().entry(from); }

ordinator &ordinator::instance() noexcept {
	static thread_local ordinator ord;
	return ord;
}

routine_t ordinator::create(std::function<void()> f, size_t stack_size) {
	// Create or reuse coroutine object
	if (indexes_.empty()) {
		routines_.emplace_back(std::move(f), stack_size);
		return routines_.size();
	}
	routine_t id = indexes_.back();
	indexes_.pop_back();
	routines_[id].reuse(std::move(f), stack_size);
	return id + 1;
}

int ordinator::resume(routine_t id) {
	if (id == current_) {
		return 0;
	}

	assert(id <= routines_.size());
	assert(id);	 // For now the main routine should not be resumed explicitly
	if (id > routines_.size()) return -1;

	transfer_t from;
	{
		routine &routine = routines_[id - 1];

		if (routine.is_finalized()) return -2;

		if (routine.is_empty()) {
			routine.create_ctx();
		}
		push_to_call_stack(current_);
		ctx_owner *owner = this;
		if (current_ > 0) {
			owner = &routines_[current_ - 1];
		}
		current_ = id;

#ifdef REINDEX_WITH_TSAN_FIBERS
		__tsan_switch_to_fiber(routine.tsan_fiber_, 0);
#endif	// REINDEX_WITH_TSAN_FIBERS

		from = jump_fcontext(routine.ctx_, owner);	// Switch context
													// It's unsafe to use routine reference after jump
	}
	if (from.data) {
		// If execution thread was returned from active coroutine
		reinterpret_cast<ctx_owner *>(from.data)->ctx_ = from.fctx;
	} else {
		// If execution thread was returned from finalized coroutine
		clear_finalized();
	}

	return 0;
}

void ordinator::suspend() {
	routine_t id = current_;
	transfer_t from;
	assert(id);	 // Suspend should not be called from main routine. Probably will be changed later
	{
		routine &routine = routines_[id - 1];
		assert(routine.validate_stack());

		from = jump_to_parent(&routine);
		// It's unsafe to use routine reference after jump
	}
	if (from.data) {
		// If execution thread was returned from active coroutine
		reinterpret_cast<ctx_owner *>(from.data)->ctx_ = from.fctx;
	} else {
		// If execution thread was returned from finalized coroutine
		clear_finalized();
	}
}

bool ordinator::set_loop_completion_callback(ordinator::cmpl_cb_t cb) noexcept {
	if (loop_completion_callback_) return false;
	loop_completion_callback_ = cb;
	return true;
}

int64_t ordinator::add_completion_callback(ordinator::cmpl_cb_t cb) {
	int64_t id = 0;
	uint8_t cnt = 0;
	for (;;) {
		id = std::chrono::steady_clock::now().time_since_epoch().count();
		auto found =
			std::find_if(completion_callbacks_.begin(), completion_callbacks_.end(), [id](cmpl_cb_data &data) { return data.id == id; });
		if (found == completion_callbacks_.end()) {
			break;
		} else if (++cnt == 3) {
			assert(false);
			break;
		} else {
			std::this_thread::yield();
		}
	}
	completion_callbacks_.emplace_back(cmpl_cb_data{std::move(cb), id});
	return id;
}

bool ordinator::remove_loop_completion_callback() noexcept {
	if (loop_completion_callback_) {
		loop_completion_callback_ = nullptr;
		return true;
	}
	return false;
}

int ordinator::remove_completion_callback(int64_t id) noexcept {
	auto old_sz = completion_callbacks_.size();
	completion_callbacks_.erase(
		std::remove_if(completion_callbacks_.begin(), completion_callbacks_.end(), [id](cmpl_cb_data &data) { return data.id == id; }),
		completion_callbacks_.end());
	auto diff = old_sz - completion_callbacks_.size();
	if (diff == 0) {
		return -1;
	}
	return 0;
}

size_t ordinator::shrink_storage() noexcept {
	size_t unused_cnt = 0;
	for (auto rIt = routines_.rbegin(); rIt != routines_.rend(); ++rIt) {
		if (rIt->is_empty() && rIt->is_finalized()) {
			++unused_cnt;
		} else {
			break;
		}
	}
	std::vector<routine> new_rt;
	routines_.resize(routines_.size() - unused_cnt);
	new_rt.reserve(routines_.size());
	std::move(routines_.begin(), routines_.end(), std::back_inserter(new_rt));
	std::swap(routines_, new_rt);
	std::vector<routine_t> new_idx;
	for (auto it = routines_.begin(); it != routines_.end(); ++it) {
		if (it->is_empty() && it->is_finalized()) {
			new_idx.emplace_back(std::distance(routines_.begin(), it));
		}
	}
	std::swap(indexes_, new_idx);
	return routines_.size();
}

void ordinator::entry(transfer_t from) {
	auto owner = reinterpret_cast<ctx_owner *>(from.data);
	assert(owner);
	owner->ctx_ = from.fctx;
	routine_t id = current_ - 1;
	if (routines_[id].func) {
		try {
			auto func = std::move(routines_[id].func);
			func();
		} catch (std::exception &e) {
			fprintf(stderr, "Unhandled exception in coroutine \"%u\": %s\n", id + 1, e.what());
		} catch (...) {
			fprintf(stderr, "Unhandled exception in coroutine \"%u\": some custom exception\n", id + 1);
		}
	}

	remove_from_call_stack(id + 1);
	routines_[id].finalize();
	indexes_.emplace_back(id);
	jump_to_parent(nullptr);
}

#ifdef REINDEX_WITH_TSAN_FIBERS
ordinator::routine::~routine() {
	if (tsan_fiber_) {
		__tsan_destroy_fiber(tsan_fiber_);
	}
}
#endif	// REINDEX_WITH_TSAN_FIBERS

void ordinator::routine::finalize() noexcept {
	assert(!is_finalized());
	finalized_ = true;
}

void ordinator::routine::clear() noexcept {
	assert(is_finalized());
#ifdef REINDEX_WITH_TSAN_FIBERS
	if (tsan_fiber_) {
		__tsan_destroy_fiber(tsan_fiber_);
		tsan_fiber_ = nullptr;
	}
#endif	// REINDEX_WITH_TSAN_FIBERS
	std::vector<char> v;
	v.swap(stack_);	 // Trying to deallocate
}

void ordinator::routine::create_ctx() {
	assert(is_empty());
	stack_.resize(stack_size_);
	ctx_ = make_fcontext(stack_.data() + stack_size_, stack_size_, static_entry);

#ifdef REINDEX_WITH_TSAN_FIBERS
	assert(!tsan_fiber_);
	tsan_fiber_ = __tsan_create_fiber(0);
#endif	// REINDEX_WITH_TSAN_FIBERS
}

bool ordinator::routine::validate_stack() const noexcept {
	auto stack_top = stack_.data() + stack_.size();
	char stack_bottom = 0;
	return size_t(stack_top - &stack_bottom) <= stack_size_;
}

ordinator::ordinator() : current_(0), loop_completion_callback_{nullptr} {
	routines_.reserve(16);
	rt_call_stack_.reserve(32);
	indexes_.reserve(8);
#ifdef REINDEX_WITH_TSAN_FIBERS
	tsan_fiber_ = __tsan_get_current_fiber();
#endif	// REINDEX_WITH_TSAN_FIBERS
}

void ordinator::push_to_call_stack(routine_t id) {
	if (id) {
		rt_call_stack_.emplace_back(id);
	} else {
		rt_call_stack_.clear();	 // Clears stack after switching to main routine
	}
}

routine_t ordinator::pop_from_call_stack() noexcept {
	if (rt_call_stack_.size()) {
		auto id = rt_call_stack_.back();
		rt_call_stack_.pop_back();
		return id;
	}
	return 0;
}

void ordinator::remove_from_call_stack(routine_t id) noexcept {
	rt_call_stack_.erase(std::remove(rt_call_stack_.begin(), rt_call_stack_.end(), id), rt_call_stack_.end());
}

void ordinator::clear_finalized() {
	assert(indexes_.size());
	auto idIndex = indexes_.back();
	auto &routine = routines_[idIndex];
	assert(routine.is_finalized());
	routine.clear();
	if (loop_completion_callback_) {
		// calling completion callback for dynamic_loop
		loop_completion_callback_(idIndex + 1);
	}
	if (completion_callbacks_.size() > 0) {
		// Copy callbacks to allow callbacks' vector modification
		auto tmp_cbs = completion_callbacks_;
		for (auto &cb_data : tmp_cbs) {
			cb_data.cb(idIndex + 1);
		}
	}
}

transfer_t ordinator::jump_to_parent(void *from_rt) noexcept {
	current_ = pop_from_call_stack();  // Next coroutine to switch should be taken from call stack
	ctx_owner *owner = this;
	if (current_ > 0) {
		owner = &routines_[current_ - 1];
	}

#ifdef REINDEX_WITH_TSAN_FIBERS
	__tsan_switch_to_fiber(owner->tsan_fiber_, 0);
#endif	// REINDEX_WITH_TSAN_FIBERS

	return jump_fcontext(owner->ctx_, from_rt);
}

}  // namespace coroutine
}  // namespace reindexer
