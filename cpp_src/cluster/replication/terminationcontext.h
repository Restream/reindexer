#pragma once

#include <atomic>
#include "core/rdxcontext.h"

namespace reindexer::cluster {

class [[nodiscard]] TerminationContext final : public IRdxCancelContext {
public:
	explicit TerminationContext(const std::atomic<bool>& terminate) noexcept : terminate_(terminate) {}

	CancelType GetCancelType() const noexcept override {
		return terminate_.load(std::memory_order_relaxed) ? CancelType::Explicit : CancelType::None;
	}
	bool IsCancelable() const noexcept override { return true; }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }

private:
	const std::atomic<bool>& terminate_;
};

}  // namespace reindexer::cluster
