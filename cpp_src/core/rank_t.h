#pragma once

#include <compare>

namespace reindexer {

class [[nodiscard]] RankT {
public:
	explicit RankT(float v = 0.0f) noexcept : value_{v} {}
	[[nodiscard]] float Value() const noexcept { return value_; }
	[[nodiscard]] auto operator<=>(const RankT&) const noexcept = default;

private:
	float value_{0.0f};
};

}  // namespace reindexer
