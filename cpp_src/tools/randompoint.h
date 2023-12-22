#pragma once

#include "core/keyvalue/geometry.h"

namespace reindexer {

inline double randBinDouble(long long min, long long max) noexcept {
	assertrx(min < max);
	const long long divider = (1ull << (rand() % 10));
	min *= divider;
	max *= divider;
	return static_cast<double>((rand() % (max - min)) + min) / static_cast<double>(divider);
}

inline Point randPoint(long long range) noexcept { return Point{randBinDouble(-range, range), randBinDouble(-range, range)}; }

}  // namespace reindexer
