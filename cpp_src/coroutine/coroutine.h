#pragma once

#include <assert.h>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <vector>

#include "context/fcontext.hpp"

#if defined(REINDEX_WITH_TSAN)
#if defined(__GNUC__) && !defined(__clang__) && !defined(__INTEL_COMPILER) && __GNUC__ >= 10
// Enable tsan fiber annotation for GCC >= 10.0
#define REINDEX_WITH_TSAN_FIBERS
#elif defined(__clang__) && defined(__clang_major__) && __clang_major__ >= 10
// Enable tsan fiber annotation for Clang >= 10.0
#define REINDEX_WITH_TSAN_FIBERS
#endif
#endif	// defined(REINDEX_WITH_TSAN)

namespace reindexer {
namespace coroutine {

using routine_t = uint32_t;

constexpr size_t k_default_stack_limit = 128 * 1024;

/// @class Base class for coroutine and ordinator, which holds current execution context
struct ctx_owner {
	fcontext_t ctx_{nullptr};

#ifdef REINDEX_WITH_TSAN_FIBERS
	void *tsan_fiber_{nullptr};
#endif	// REINDEX_WITH_TSAN_FIBERS

	ctx_owner() = default;
	virtual ~ctx_owner() = default;

#ifdef REINDEX_WITH_TSAN_FIBERS
	ctx_owner(ctx_owner &&o) : ctx_(o.ctx_), tsan_fiber_(o.tsan_fiber_) { o.tsan_fiber_ = nullptr; }
	ctx_owner &operator=(ctx_owner &&o) {
		if (this != &o) {
			ctx_ = o.ctx_;
			tsan_fiber_ = o.tsan_fiber_;
			o.tsan_fiber_ = nullptr;
		}
		return *this;
	}
#endif	// REINDEX_WITH_TSAN_FIBERS
};

/// @class Some kind of a coroutines scheduler. Exists as a singleton with thread_local storage duration.
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
	/// Add callback on coroutine completion for dynamic_loop object
	/// @return true, if callback was not not set before and was successfully set now
	bool set_loop_completion_callback(cmpl_cb_t) noexcept;
	/// Add callback that will be called after coroutine finalization
	/// @return callback unique ID that should to be used for its further removal
	int64_t add_completion_callback(cmpl_cb_t);
	/// Remove dynamic_loop callback on coroutine complete
	/// @return true if callback was set previously
	bool remove_loop_completion_callback() noexcept;
	/// Remove coroutine completion callback by id
	/// @param id - callback's ID
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
#ifdef REINDEX_WITH_TSAN_FIBERS
		~routine();
#endif	// REINDEX_WITH_TSAN_FIBERS
		routine(std::function<void()> f, size_t stack_size) noexcept : func(std::move(f)), stack_size_(stack_size) { assert(stack_size_); }
		routine(const routine &) = delete;
		routine(routine &&) = default;
		routine &operator=(routine &&) = default;

		/// Check if coroutine is already finished it's execution and ready to be cleared
		/// @returns true - if coroutine is finalized, false - if couroutine is still in progress
		bool is_finalized() const noexcept { return finalized_; }
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
			assert(is_finalized());
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
	ordinator();

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
	std::vector<routine> routines_;
	/// List of routines_ indexes, which are occupied by empty routine objects
	std::vector<routine_t> indexes_;
	/// Stack, which contains sequence of coroutines switches. Ordiantor pushes new id in this stack on each resume()-call
	/// and pops top id on each suspend()-call.
	/// This stack allows to get coroutine's id to switch to after suspend or entry exit
	std::vector<routine_t> rt_call_stack_;
	/// completion callback for dynamic_loop, which will be called after coroutine's finalization
	cmpl_cb_t loop_completion_callback_;
	/// completion callbacks, which will be called after coroutines finalization
	std::vector<cmpl_cb_data> completion_callbacks_;
};

/// Wrappers for ordinator's public methods
inline routine_t create(std::function<void()> f, size_t stack_size = k_default_stack_limit) {
	return ordinator::instance().create(std::move(f), stack_size);
}
inline int resume(routine_t id) { return ordinator::instance().resume(id); }
inline void suspend() { ordinator::instance().suspend(); }
inline routine_t current() noexcept { return ordinator::instance().current(); }
inline bool set_loop_completion_callback(ordinator::cmpl_cb_t cb) {
	return ordinator::instance().set_loop_completion_callback(std::move(cb));
}
inline int64_t add_completion_callback(ordinator::cmpl_cb_t cb) { return ordinator::instance().add_completion_callback(std::move(cb)); }
inline bool remove_loop_completion_callback() noexcept { return ordinator::instance().remove_loop_completion_callback(); }
inline int remove_completion_callback(int64_t id) { return ordinator::instance().remove_completion_callback(id); }
inline size_t shrink_storage() noexcept { return ordinator::instance().shrink_storage(); }

}  // namespace coroutine
}  // namespace reindexer
