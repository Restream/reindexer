#include "coroutine.h"
#include <algorithm>
#include <cstdio>
#include <iterator>
#include <thread>
#include "tools/clock.h"

namespace reindexer {
namespace coroutine {

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

	routine& current_routine = routines_[index];
	remove_from_call_stack(index + 1);
	current_ = pop_from_call_stack();
	current_routine.finalize();
	finalized_indexes_.emplace_back(index);
}

routine_t ordinator::create(std::function<void()> function, size_t stack_size) {
	if (finalized_indexes_.empty()) {
		routines_.emplace_back(std::move(function), koishi_create(), stack_size);
		return routines_.size();
	}

	const routine_t index = finalized_indexes_.back();
	finalized_indexes_.pop_back();
	routines_[index].reuse(std::move(function), stack_size);
	return index + 1;
}

void ordinator::routine::reuse(std::function<void()> function, size_t new_stack_size) noexcept {
	assertrx(is_finalized());
	assertrx(is_empty());
	func = std::move(function);
	finalized_ = false;
	stack_size_ = new_stack_size;
}

int ordinator::resume(routine_t id) {
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

void ordinator::suspend() {
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
		rt_call_stack_.emplace_back(id);
		return;
	}
	rt_call_stack_.clear();	 // Clears stack after switching to main routine
}

routine_t ordinator::pop_from_call_stack() noexcept {
	if (!rt_call_stack_.empty()) {
		auto id = rt_call_stack_.back();
		rt_call_stack_.pop_back();
		return id;
	}
	return 0;
}

void ordinator::remove_from_call_stack(routine_t id) noexcept {
	rt_call_stack_.erase(std::remove(rt_call_stack_.begin(), rt_call_stack_.end(), id), rt_call_stack_.end());
}

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
	std::vector<routine_t> new_idx;
	for (auto it = routines_.begin(); it != routines_.end(); ++it) {
		if (it->is_finalized()) {
			new_idx.emplace_back(std::distance(routines_.begin(), it));
		}
	}
	std::swap(finalized_indexes_, new_idx);
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
	: func(std::move(o.func)), fiber_(o.fiber_), stack_size_(o.stack_size_), is_empty_(o.is_empty_), finalized_(o.finalized_) {
	o.fiber_ = nullptr;
}

void ordinator::routine::finalize() noexcept {
	assertrx(!is_finalized());
	finalized_ = true;
}

void ordinator::routine::clear() noexcept {
	koishi_deinit(fiber_);
	is_empty_ = true;
}

void ordinator::clear_finalized() {
	assertrx(!finalized_indexes_.empty());
	auto index = finalized_indexes_.back();

	auto& routine = routines_[index];
	assertrx(routine.is_finalized());
	routine.clear();
	if (loop_completion_callback_) {
		loop_completion_callback_(index + 1);
	}

	auto callbacks = completion_callbacks_;
	for (auto& callback : callbacks) {
		callback.cb(index + 1);
	}
}

ordinator::ordinator() : current_(0), loop_completion_callback_{nullptr} {
	routines_.reserve(16);
	rt_call_stack_.reserve(32);
	finalized_indexes_.reserve(8);
	koishi_active();
}

}  // namespace coroutine
}  // namespace reindexer
