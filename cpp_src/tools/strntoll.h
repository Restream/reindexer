#pragma once

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string_view>
#include "tools/assertrx.h"

namespace reindexer {

inline long long int strntoll(std::string_view str, const char** end, int base) noexcept {
	if (str.empty()) {
		return 0;
	}
	char buf[24];
	const char* beg = str.data();
	auto sz = str.size();
	for (; beg && sz && isspace(*beg); ++beg, --sz);
	assertrx_dbg(end);
	const bool isNegative = beg && *beg == '-';
	const bool hasSign = beg && (*beg == '-' || *beg == '+');
	unsigned nums = hasSign ? 1 : 0;
	for (auto it = beg + nums; sz && it; ++it, --sz) {
		if (!isdigit(*it)) {
			break;
		}
		++nums;
	}

	if (!nums || (nums == 1 && hasSign)) {
		*end = str.data();
		return 0;
	}
	if (nums >= sizeof(buf)) {
		*end = beg + nums;
		return isNegative ? std::numeric_limits<long long int>::min() : std::numeric_limits<long long int>::max();
	}

	// beg can not be null if nums != 0
	// NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)
	std::memcpy(buf, beg, nums);
	buf[nums] = '\0';
	const auto ret = std::strtoll(buf, const_cast<char**>(end), base);
	if (ret == std::numeric_limits<long long int>::min() || ret == std::numeric_limits<long long int>::max()) {
		return ret;
	}
	*end = beg + (*end - buf);
	return ret;
}

}  // namespace reindexer
