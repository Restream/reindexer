#include "coroutine.h"
#include <algorithm>
#include <cstdio>
#include <exception>
#include <iterator>
#include <thread>
#include <tuple>
#include "tools/assertrx.h"
#include "tools/clock.h"

namespace reindexer::coroutine {

static void static_entry() { ordinator::instance().entry(); }

ordinator& ordinator::instance() noexcept {
	static thread_local ordinator ord;
	return ord;
}

void ordinator::entry() {
	const routine_t index = current_ - 1;
	{
		routine& current_routine = routines_[index];
		if (current_routine.func) {
			try {
				auto func = std::move(current_routine.func);
				func();
			} catch (std::exception& e) {
				fprintf(stderr, "reindexer error: unhandled exception in coroutine \"%u\": %s\n", index + 1, e.what());
			} catch (...) {
				fprintf(stderr, "reindexer error: unhandled exception in coroutine \"%u\": some custom exception\n", index + 1);
			}
		}
	}

	// Per-fiber-exit drain point for deferred resumes. At this point func's frame is fully unwound, so std::uncaught_exceptions() == 0 and
	// the fiber is still current/alive -- a safe point to perform resumes deferred from the unwind path. flush_deferred_resumes() drains the
	// GLOBAL deferred list (not just this fiber's deferrals), so every fiber exit flushes all pending entries. This covers the dominant case
	// where an exception escapes func() and is caught above. It is NOT the only drain point: a coroutine that catches its own exceptions and
	// keeps running (e.g. in a loop) defers resumes without ever reaching here, and is drained by the loop-level flush in
	// dynamic_loop::run() instead. This call sits outside the try/catch above on purpose: both flush_deferred_resumes() and resume() are
	// noexcept, so any unexpected throw (e.g. bad_alloc while finalizing a nested coroutine) terminates deterministically here instead of
	// crossing the koishi fiber boundary.
	flush_deferred_resumes();

	// Re-fetch the routine: flush_deferred_resumes() may have resumed other coroutines that created new ones, growing
	// routines_ and invalidating any reference taken before the flush.
	routine& current_routine = routines_[index];
	remove_from_call_stack(index + 1);
	current_ = pop_from_call_stack();
	current_routine.finalize();
	finalized_indexes_.push(routines_, index + 1);
}

routine_t ordinator::create(std::function<void()> function, size_t stack_size) {
	if (finalized_indexes_.empty()) {
		routines_.emplace_back(std::move(function), koishi_create(), stack_size);
		return routines_.size();
	}

	const routine_t id = finalized_indexes_.pop(routines_);
	const routine_t index = id - 1;
	routines_[index].reuse(std::move(function), stack_size);
	return id;
}

void ordinator::routine::reuse(std::function<void()> function, size_t new_stack_size) noexcept {
	assertrx(is_finalized());
	assertrx(is_empty());
	assertrx(!links.deferred);
	assertrx(!links.free_next);
	assertrx(!links.call_stack_linked);
	func = std::move(function);
	finalized_ = false;
	stack_size_ = new_stack_size;
	links = {};
}

int ordinator::resume(routine_t id) noexcept {
	assertrx_dbg(std::uncaught_exceptions() == 0);

	if (id == current_) {
		return 0;
	}

	assertrx(id <= routines_.size());
	assertrx(id);  // For now the main routine should not be resumed explicitly

	if (id > routines_.size()) {
		return -1;
	}

	{
		routine& routine = routines_[id - 1];
		if (routine.is_finalized()) {
			return -2;
		}

		if (routine.links.call_stack_linked) [[unlikely]] {
			fprintf(stderr, "reindexer error: cyclic coroutine resume detected (id %u is an active ancestor)\n", id);
			std::terminate();
		}

		if (routine.is_empty()) {
			routine.create_fiber();
		}
		push_to_call_stack(current_);

		current_ = id;

		routine.resume();
	}

	routine& routine = routines_[id - 1];
	if (routine.is_dead()) {
		clear_finalized();
	}
	return 0;
}

void ordinator::suspend() noexcept {
	assertrx_dbg(std::uncaught_exceptions() == 0);
	assertrx(current_);	 // Suspend should not be called from main routine. Probably will be changed later
	current_ = pop_from_call_stack();
	koishi_yield(nullptr);

	if (koishi_state(koishi_active()) == KOISHI_DEAD) {
		clear_finalized();
	}
}

// NOLINTNEXTLINE(bugprone-exception-escape)
void ordinator::push_to_call_stack(routine_t id) noexcept {
	if (id) {
		rt_call_stack_.push_back(routines_, id);
		return;
	}
	rt_call_stack_.clear(routines_);  // Clears stack after switching to main routine
}

routine_t ordinator::pop_from_call_stack() noexcept { return rt_call_stack_.pop_back(routines_); }

void ordinator::remove_from_call_stack(routine_t id) noexcept { rt_call_stack_.erase(routines_, id); }

bool ordinator::set_loop_completion_callback(ordinator::cmpl_cb_t cb) noexcept {
	if (loop_completion_callback_) {
		return false;
	}
	loop_completion_callback_ = std::move(cb);
	return true;
}

int64_t ordinator::add_completion_callback(ordinator::cmpl_cb_t cb) {
	int64_t id = 0;
	uint8_t cnt = 0;
	for (;;) {
		id = steady_clock_w::now().time_since_epoch().count();
		auto found =
			std::find_if(completion_callbacks_.begin(), completion_callbacks_.end(), [id](cmpl_cb_data& data) { return data.id == id; });
		if (found == completion_callbacks_.end()) {
			break;
		} else if (++cnt == 3) {
			assertrx(false);
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
		std::remove_if(completion_callbacks_.begin(), completion_callbacks_.end(), [id](cmpl_cb_data& data) { return data.id == id; }),
		completion_callbacks_.end());
	auto diff = old_sz - completion_callbacks_.size();
	if (diff == 0) {
		return -1;
	}
	return 0;
}

void ordinator::abandon_unstarted(routine_t id) noexcept {
	assertrx(id > 0 && id <= routines_.size());
	routine& r = routines_[id - 1];
	assertrx_dbg(r.is_empty());
	assertrx_dbg(!r.is_finalized());
	r.func = {};
	r.finalize();
	finalized_indexes_.push(routines_, id);
}

void ordinator::defer_resume(routine_t id) noexcept {
	if (id == 0 || id == current_) {
		return;
	}
	assertrx(id <= routines_.size());
	if (id > routines_.size()) {
		return;
	}
	routine& r = routines_[id - 1];
	if (r.links.deferred) {
		return;	 // already enqueued, nothing to do
	}
	// Push onto the head of the intrusive list of the deferred resumes
	r.links.deferred = true;
	deferred_resumes_.push(routines_, id);
}

void ordinator::flush_deferred_resumes() noexcept {
	// Drain the intrusive list in LIFO order (head first). Re-entrancy safe: each routine is fully unlinked (pop() clears
	// its links.deferred_next and the deferred flag is cleared) BEFORE resume(), so a coroutine resumed here may re-enqueue
	// itself or others; those freshly pushed entries are picked up by the loop condition below within the same call.
	while (!deferred_resumes_.empty()) {
		const routine_t id = deferred_resumes_.pop(routines_);
		routines_[id - 1].links.deferred = false;
		std::ignore = resume(id);
	}
}

size_t ordinator::shrink_storage() noexcept {
	size_t unused_cnt = 0;
	for (auto rIt = routines_.rbegin(); rIt != routines_.rend(); ++rIt) {
		if (rIt->is_finalized()) {
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
	finalized_indexes_.reset();
	for (routine_t id = 1; id <= routines_.size(); ++id) {
		routines_[id - 1].links.free_next = 0;
		if (routines_[id - 1].is_finalized()) {
			finalized_indexes_.push(routines_, id);
		}
	}
	return routines_.size();
}

void ordinator::routine::create_fiber() {
	koishi_init(fiber_, stack_size_, reinterpret_cast<koishi_entrypoint_t>(static_entry));
	is_empty_ = false;
}

ordinator::routine::~routine() {
	if (fiber_) {
		clear();
		koishi_destroy(fiber_);
		fiber_ = nullptr;
	}
}

ordinator::routine::routine(ordinator::routine&& o) noexcept
	: func(std::move(o.func)),
	  links(o.links),
	  fiber_(o.fiber_),
	  stack_size_(o.stack_size_),
	  is_empty_(o.is_empty_),
	  finalized_(o.finalized_) {
	o.fiber_ = nullptr;
	o.links = {};
}

void ordinator::routine::finalize() noexcept {
	assertrx(!is_finalized());
	assertrx(!links.deferred);	// a routine must be unlinked from the deferred list (drained via flush) before it finalizes
	assertrx(!links.call_stack_linked);
	finalized_ = true;
}

void ordinator::routine::clear() noexcept {
	koishi_deinit(fiber_);
	is_empty_ = true;
}

void ordinator::clear_finalized() noexcept {
	assertrx(!finalized_indexes_.empty());
	const auto id = finalized_indexes_.head();
	const auto index = id - 1;

	auto& routine = routines_[index];
	assertrx(routine.is_finalized());
	routine.clear();

	// The completion callbacks and the temporary copy of completion_callbacks_ below are the only potentially throwing  operations.
	// Callbacks are required to be noexcept by contract and a std::bad_alloc while finalizing a coroutine is unrecoverable. We catch
	// explicitly (instead of relying on the implicit noexcept->terminate) so the intent is clear and clang-tidy does not flag the enclosing
	// noexcept function.
	try {
		if (loop_completion_callback_) {
			loop_completion_callback_(index + 1);
		}

		auto callbacks = completion_callbacks_;
		for (auto& callback : callbacks) {
			callback.cb(index + 1);
		}
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: unexpected exception during coroutine finalization callbacks: %s\n", e.what());
		std::terminate();  // crash is intended: completion callbacks must not throw
	}
}

ordinator::ordinator() : current_(0), loop_completion_callback_{nullptr} {
	routines_.reserve(16);
	koishi_active();
}

}  // namespace reindexer::coroutine
