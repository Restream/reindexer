#pragma once

#include <cstddef>
#include <exception>
#include <utility>

namespace reindexer {

class [[nodiscard]] StackUnwinding {
public:
	StackUnwinding() noexcept = default;
	StackUnwinding(const StackUnwinding&) = delete;
	StackUnwinding& operator=(const StackUnwinding&) = delete;

	bool operator()() const noexcept { return (uncaughtExceptions_ != std::uncaught_exceptions()); }

private:
	int uncaughtExceptions_{std::uncaught_exceptions()};
};

template <typename F1, typename F2>
class [[nodiscard]] ScopeGuard {
public:
	template <typename _F1, typename _F2>
	ScopeGuard(_F1&& onConstruct, _F2&& onDestruct) noexcept : onDestruct_(std::forward<_F2>(onDestruct)) {
		onConstruct();
	}
	template <typename _F2>
	ScopeGuard(_F2&& onDestruct) noexcept : onDestruct_(std::forward<_F2>(onDestruct)) {}
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
	return ScopeGuard<F1, F2>(std::forward<F1>(onConstruct), std::forward<F2>(onDestruct));
}

template <typename F>
ScopeGuard<std::nullptr_t, F> MakeScopeGuard(F&& onDestruct) noexcept {
	return ScopeGuard<std::nullptr_t, F>(std::forward<F>(onDestruct));
}

}  // namespace reindexer
