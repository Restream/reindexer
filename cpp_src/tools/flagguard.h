#pragma once

#include <assert.h>

namespace reindexer {

template <bool GuardValue>
class FlagGuard {
public:
	FlagGuard(bool& flag) : flag_(flag) {
		assert(flag_ != GuardValue);
		flag_ = GuardValue;
	}

	~FlagGuard() {
		assert(flag_ == GuardValue);
		flag_ = !GuardValue;
	}

private:
	bool& flag_;
};

using FlagGuardT = FlagGuard<true>;
using FlagGuardF = FlagGuard<false>;

}  // namespace reindexer
