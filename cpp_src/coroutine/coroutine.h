#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <vector>

#include "context/fcontext.hpp"
#include "estl/h_vector.h"

namespace reindexer {
namespace coroutine {

using routine_t = uint32_t;

constexpr size_t k_default_stack_limit = 128 * 1024;

/// @class Base class for coroutine and ordinator, which holds current execution context
struct ctx_owner {
	fcontext_t ctx_{nullptr};
};

/// @class Some kind of a coroutines scheduler. Exists as a singletone with thread_local storage duration.
class ordinator : public ctx_owner {
public:
	using cmpl_cb_t = std::function<void(routine_t)>;

	ordinator(const ordinator &) = delete;
	ordinator(ordinator &&) = delete;
	ordinator &operator=(const ordinator &) = delete;
	ordinator &operator=(ordinator &&) = delete;

	static ordinator &instance() noexcept;

	/// Create new coroutine in current thread
	/// @param f - Function, that will be executed in this coroutine
	/// @param stack_size - Coroutine's stack size
	/// @returns New routine's id
	routine_t create(std::function<void()> f, size_t stack_size);
	/// Resume coroutine with specified id. Returns error if coroutine doesn't exist.
	/// Does nothing if this couroutine is already running.
	/// Current coroutine becomes "parent" to the resumed one
	/// @param id - Coroutine's id
	/// @returns - 0 on success
	int resume(routine_t id);
	/// Switch from current coroutine to it's "parent" coroutine (the one, that is next on coroutines call stack)
	void suspend();
	/// Get current couroutine id
	/// @returns - current couroutine id
	routine_t current() const noexcept { return current_; }
	/// Add callback on coroutine complete
	/// @return Callback's ID, which may be used to remove this callback
	int64_t add_completion_callback(cmpl_cb_t);
	/// Remove callback on coroutine complete
	/// @param id Callback's ID
	/// @return 0 if callback was successfully removed
	int remove_completion_callback(int64_t id) noexcept;
	/// Shrink coroutines storage and free some occupied memory (remove unused coroutine objects)
	/// @return New storage size
	size_t shrink_storage() noexcept;
	/// Entry point for each new coroutine
	void entry(transfer_t from);

private:
	/// @class Holds coroutine's data such as stack and state
	class routine : public ctx_owner {
	public:
		routine() noexcept = default;
		routine(std::function<void()> f, size_t stack_size) noexcept : func(std::move(f)), stack_size_(stack_size) { assert(stack_size_); }
		routine(const routine &) = delete;
		routine(routine &&) = default;
		routine &operator=(routine &&) = default;

		/// Check if coroutine is already finished it's execution and ready to be cleared
		/// @returns true - if coroutine is finalized, false - if couroutine is still in progress
		bool is_finialized() const noexcept { return finalized_; }
		/// Mark routine as finilized
		void finalize() noexcept;
		/// Deallocate coroutines stack
		void clear() noexcept;
		/// Allocate coroutine's stack and create execution context on it
		/// Couroutine has to be empty to call this method
		void create_ctx();
		/// Check if coroutine has allocated stack
		/// @returns true - if stack is empty; false - if stack is allocated
		bool is_empty() const noexcept { return stack_.empty(); }
		/// Reuse finalized coroutine with new func
		/// @param _func - New coroutine's func
		void reuse(std::function<void()> _func, size_t stack_size = 0) noexcept {
			assert(is_finialized());
			assert(is_empty());
			func = std::move(_func);
			if (stack_size) {
				stack_size_ = stack_size;
			}
			finalized_ = false;
		}
		/// Check if current stack is still inside of allocated array
		/// @return true if stack size is fine
		bool validate_stack() const noexcept;

		std::function<void()> func;

	private:
		std::vector<char> stack_;
		size_t stack_size_ = k_default_stack_limit;
		bool finalized_ = false;
	};

	/// @struct Pair of completion callback and it's unique ID
	struct cmpl_cb_data {
		cmpl_cb_t cb;
		int64_t id;
	};

	/// Private constructor to create singletone object
	ordinator() noexcept;

	/// Add "parent" coroutine to coroutines call stack.
	/// If new "parent" has id == 0, than the stack will be cleared, because main routine is unable to call suspend anyway
	/// @param id - new "parent's" id
	void push_to_call_stack(routine_t id);
	/// Pop top coroutine's id from call stack.
	/// @returns "Parent's" coroutine id. If stack is empty returns 0
	routine_t pop_from_call_stack() noexcept;
	/// Remove specified coroutine from call stack. Method is used to clear coroutine from call stack after it's finalization
	/// @param id - coroutine id to remove
	void remove_from_call_stack(routine_t id) noexcept;
	/// Clear last finalized coroutine
	void clear_finalized();
	/// Switches context to "parent's" couroutine context taken from call stack
	/// @param from_rt - data which will be passed to "parent" coroutine. (usually it's a pointer to current coroutine)
	/// @return context data
	transfer_t jump_to_parent(void *from_rt) noexcept;

	routine_t current_;
	h_vector<routine, 16> routines_;
	/// List of routines_ indexes, which are occupied by empty routine objects
	h_vector<routine_t> indexes_;
	/// Stack, which contains sequence of coroutines switches. Ordiantor pushes new id in this stack on each resume()-call
	/// and pops top id on yeach suspend()-call.
	/// This stack allows to get coroutine's id to switch to after suspend or entry exit
	h_vector<routine_t, 32> rt_call_stack_;
	/// Vector with completion callbacks, which will be called on each coroutne's finalization
	h_vector<cmpl_cb_data, 2> completion_callbacks_;
};

/// Wrapper for corresponding ordinator's method
inline routine_t create(std::function<void()> f, size_t stack_size = k_default_stack_limit) {
	return ordinator::instance().create(std::move(f), stack_size);
}
/// Wrapper for corresponding ordinator's method
inline int resume(routine_t id) { return ordinator::instance().resume(id); }
/// Wrapper for corresponding ordinator's method
inline void suspend() { ordinator::instance().suspend(); }
/// Wrapper for corresponding ordinator's method
inline routine_t current() noexcept { return ordinator::instance().current(); }
/// Wrapper for corresponding ordinator's method
inline int64_t add_completion_callback(ordinator::cmpl_cb_t cb) { return ordinator::instance().add_completion_callback(std::move(cb)); }
/// Wrapper for corresponding ordinator's method
inline int remove_completion_callback(int64_t id) noexcept { return ordinator::instance().remove_completion_callback(id); }
/// Wrapper for corresponding ordinator's method
inline size_t shrink_storage() noexcept { return ordinator::instance().shrink_storage(); }

}  // namespace coroutine
}  // namespace reindexer
