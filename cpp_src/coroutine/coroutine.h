#pragma once

#include <cstdint>
#include <functional>
#include <utility>
#include <vector>
#include "tools/assertrx.h"
#include "vendor/koishi/include/koishi.h"

namespace reindexer {
namespace coroutine {

using routine_t = uint32_t;

constexpr size_t k_default_stack_limit = 128 * 1024;

/// @class Some kind of a coroutines scheduler. Exists as a singletone with thread_local storage duration.
class [[nodiscard]] ordinator {
public:
	using cmpl_cb_t = std::function<void(routine_t)>;

	ordinator(const ordinator&) = delete;
	ordinator(ordinator&&) = delete;
	ordinator& operator=(const ordinator&) = delete;
	ordinator& operator=(ordinator&&) = delete;

	static ordinator& instance() noexcept;

	/// Create new coroutine in current thread
	/// @param function - Function, that will be executed in this coroutine
	/// @param stack_size - Coroutine's stack size
	/// @returns New routine's id
	routine_t create(std::function<void()> function, size_t stack_size);
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
	/// @param cb - callback
	/// @return true, if callback was not not set before and was successfully set now
	bool set_loop_completion_callback(cmpl_cb_t cb) noexcept;
	/// Add callback that will be called after coroutine finalization
	/// @param cb - callback
	/// @return callback unique ID that should to be used for its further removal
	int64_t add_completion_callback(cmpl_cb_t cb);
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
	void entry();

private:
	/// @class Holds coroutine's data such as stack and state
	class [[nodiscard]] routine {
	public:
		routine() noexcept = default;
		~routine();
		routine(const routine&) = delete;
		routine(routine&& other) noexcept;
		routine(std::function<void()> _func, koishi_coroutine_t* fiber, size_t stack_size) noexcept
			: func(std::move(_func)), fiber_(fiber), stack_size_(stack_size), is_empty_(true) {
			assertrx(stack_size_);
			assertrx(fiber_);
		}
		routine& operator=(const routine&) = delete;
		routine& operator=(routine&&) = delete;

		/// Check if coroutine is already finished it's execution and ready to be cleared
		/// @returns true - if coroutine is finalized, false - if couroutine is still in progress
		bool is_finalized() const noexcept { return finalized_; }
		/// Mark routine as finilized
		void finalize() noexcept;
		/// Deallocate coroutines stack
		void clear() noexcept;
		/// Check if coroutine has allocated stack
		/// @returns true - if stack is empty; false - if stack is allocated
		bool is_empty() const noexcept { return is_empty_; }
		/// Allocate coroutine's stack and create execution context on it
		/// Couroutine has to be empty to call this method
		void create_fiber();
		/// Reuse finalized coroutine with new func
		/// @param _func - New coroutine's func
		/// @param new_stack_size - New coroutines stack size
		void reuse(std::function<void()> _func, size_t new_stack_size) noexcept;
		/// Resume coroutine
		void resume() { koishi_resume(fiber_, nullptr); }
		/// Check, that coroutine ended work
		bool is_dead() const { return koishi_state(fiber_) == KOISHI_DEAD; }

		std::function<void()> func;

	private:
		koishi_coroutine_t* fiber_ = nullptr;
		size_t stack_size_ = k_default_stack_limit;
		bool is_empty_ = true;
		bool finalized_ = false;
	};

	/// @struct Pair of completion callback and it's unique ID
	struct [[nodiscard]] cmpl_cb_data {
		cmpl_cb_t cb;
		int64_t id;
	};

	/// Private constructor to create singletone object
	ordinator();
	~ordinator() = default;

	/// Add "parent" coroutine to coroutines call stack.
	/// If new "parent" has id == 0, than the stack will be cleared, because main routine is unable to call suspend anyway
	/// @param id - new "parent's" id
	void push_to_call_stack(routine_t id) noexcept;
	/// Pop top coroutine's id from call stack.
	/// @returns "Parent's" coroutine id. If stack is empty returns 0
	routine_t pop_from_call_stack() noexcept;
	/// Remove specified coroutine from call stack. Method is used to clear coroutine from call stack after it's finalization
	/// @param id - coroutine id to remove
	void remove_from_call_stack(routine_t id) noexcept;
	/// Clear last finalized coroutine
	void clear_finalized();

	routine_t current_;
	std::vector<routine> routines_;
	/// Stack, which contains sequence of coroutines switches. Ordiantor pushes new id in this stack on each resume()-call
	/// and pops top id on each suspend()-call.
	/// This stack allows to get coroutine's id to switch to after suspend or entry exit
	std::vector<routine_t> rt_call_stack_;
	/// List of routines_ indexes, which are occupied by empty routine objects
	std::vector<routine_t> finalized_indexes_;
	/// completion callback for dynamic_loop, which will be called after coroutine's finalization
	cmpl_cb_t loop_completion_callback_;
	/// Vector with completion callbacks, which will be called on each coroutne's finalization
	std::vector<cmpl_cb_data> completion_callbacks_;
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
inline bool set_loop_completion_callback(ordinator::cmpl_cb_t cb) {
	return ordinator::instance().set_loop_completion_callback(std::move(cb));
}
/// Wrapper for corresponding ordinator's method
inline int64_t add_completion_callback(ordinator::cmpl_cb_t cb) { return ordinator::instance().add_completion_callback(std::move(cb)); }
/// Wrapper for corresponding ordinator's method
inline bool remove_loop_completion_callback() noexcept { return ordinator::instance().remove_loop_completion_callback(); }
inline int remove_completion_callback(int64_t id) noexcept { return ordinator::instance().remove_completion_callback(id); }
/// Wrapper for corresponding ordinator's method
inline size_t shrink_storage() noexcept { return ordinator::instance().shrink_storage(); }

}  // namespace coroutine
}  // namespace reindexer
