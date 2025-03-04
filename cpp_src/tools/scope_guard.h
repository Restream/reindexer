#pragma once

#include <cstddef>
#include <exception>
#include <utility>

namespace reindexer {

class StackUnwinding {
public:
	StackUnwinding() noexcept = default;
	StackUnwinding(const StackUnwinding&) = delete;
	StackUnwinding& operator=(const StackUnwinding&) = delete;

	bool operator()() { return (uncaughtExceptions_ != std::uncaught_exceptions()); }

private:
	int uncaughtExceptions_{std::uncaught_exceptions()};
};

template <typename F1, typename F2>
class ScopeGuard {
public:
	ScopeGuard(F1&& onConstruct, F2&& onDestruct) noexcept : onDestruct_(std::move(onDestruct)) { onConstruct(); }
	ScopeGuard(F2&& onDestruct) noexcept : onDestruct_(std::move(onDestruct)) {}
	~ScopeGuard() noexcept(false) {
		if (disabled_) {
			return;
		}
		if (stackUnwining_()) {
			try {
				onDestruct_();
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
				// Exception must be ignored during stack unwinding
			}
			// NOLINTEND(bugprone-empty-catch)
		} else {
			onDestruct_();
		}
	}
	void Disable() noexcept { disabled_ = true; }

private:
	F2 onDestruct_;
	StackUnwinding stackUnwining_;
	bool disabled_{false};
};

template <typename F1, typename F2>
ScopeGuard<F1, F2> MakeScopeGuard(F1&& onConstruct, F2&& onDestruct) noexcept {
	return ScopeGuard<F1, F2>(std::move(onConstruct), std::move(onDestruct));
}

template <typename F>
ScopeGuard<std::nullptr_t, F> MakeScopeGuard(F&& onDestruct) noexcept {
	return ScopeGuard<std::nullptr_t, F>(std::move(onDestruct));
}

}  // namespace reindexer
