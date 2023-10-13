#include "random.h"

double randBinDouble(long long min, long long max) noexcept {
	assertrx(min < max);
	const long long divider = (1ull << (rand() % 10));
	min *= divider;
	max *= divider;
	return static_cast<double>((rand() % (max - min)) + min) / static_cast<double>(divider);
}

reindexer::Point randPoint(long long range) noexcept {
	return reindexer::Point{randBinDouble(-range, range), randBinDouble(-range, range)};
}
