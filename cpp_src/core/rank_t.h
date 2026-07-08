#pragma once

#include "tools/float_comparison.h"

namespace reindexer {

class [[nodiscard]] RankT {
public:
	explicit RankT(float v = 0.0f) noexcept : value_{v} {}
	float Value() const noexcept { return value_; }
	bool operator==(const RankT& o) const noexcept { return fp::ExactlyEqual(value_, o.value_); }
	auto operator<=>(const RankT& o) const noexcept = default;

private:
	float value_{0.0f};
};

}  // namespace reindexer
