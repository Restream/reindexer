#include "coroutine.h"
#include <cassert>
#include <chrono>
#include <thread>

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

		if (routine.is_finialized()) return -2;

		if (routine.is_empty()) {
			routine.create_ctx();
		}
		push_to_call_stack(current_);
		ctx_owner *owner = this;
		if (current_ > 0) {
			owner = &routines_[current_ - 1];
		}
		current_ = id;
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
		if (rIt->is_empty() && rIt->is_finialized()) {
			++unused_cnt;
		} else {
			break;
		}
	}
	h_vector<routine, 16> new_rt;
	routines_.resize(routines_.size() - unused_cnt);
	new_rt.reserve(routines_.size());
	std::move(routines_.begin(), routines_.end(), std::back_inserter(new_rt));
	std::swap(routines_, new_rt);
	h_vector<routine_t> new_idx;
	for (auto it = routines_.begin(); it != routines_.end(); ++it) {
		if (it->is_empty() && it->is_finialized()) {
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

void ordinator::routine::finalize() noexcept {
	assert(!is_finialized());
	finalized_ = true;
}

void ordinator::routine::clear() noexcept {
	assert(is_finialized());
	std::vector<char> v;
	v.swap(stack_);	 // Trying to deallocate
}

void ordinator::routine::create_ctx() {
	assert(is_empty());
	stack_.resize(stack_size_);
	ctx_ = make_fcontext(stack_.data() + stack_size_, stack_size_, static_entry);
}

bool ordinator::routine::validate_stack() const noexcept {
	auto stack_top = stack_.data() + stack_.size();
	char stack_bottom = 0;
	return size_t(stack_top - &stack_bottom) <= stack_size_;
}

ordinator::ordinator() noexcept : current_(0) {}

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
	auto id = indexes_.back();
	auto &routine = routines_[id];
	assert(routine.is_finialized());
	routine.clear();
	// Copy callbacks to allow main callback vector modification
	auto tmp_cbs = completion_callbacks_;
	for (auto &cb_data : tmp_cbs) {
		cb_data.cb(id + 1);
	}
}

transfer_t ordinator::jump_to_parent(void *from_rt) noexcept {
	current_ = pop_from_call_stack();  // Next coroutine to switch should be taken from call stack
	ctx_owner *owner = this;
	if (current_ > 0) {
		owner = &routines_[current_ - 1];
	}
	return jump_fcontext(owner->ctx_, from_rt);
}

}  // namespace coroutine
}  // namespace reindexer
