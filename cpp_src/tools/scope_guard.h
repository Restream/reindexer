#pragma once

namespace reindexer {

template <typename F1, typename F2>
class ScopeGuard {
public:
	ScopeGuard(F1&& onConstruct, F2&& onDestruct) noexcept : onDestruct_(std::move(onDestruct)) { onConstruct(); }
	~ScopeGuard() { onDestruct_(); }

private:
	F2 onDestruct_;
};

template <typename F1, typename F2>
ScopeGuard<F1, F2> MakeScopeGuard(F1&& onConstruct, F2&& onDestruct) noexcept {
	return ScopeGuard<F1, F2>(std::move(onConstruct), std::move(onDestruct));
}

}  // namespace reindexer
