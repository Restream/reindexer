#pragma once

#include "core/namespacedef.h"

namespace reindexer {

class [[nodiscard]] NsVersionCounter {
public:
	void SetServer(int server) noexcept { counter_.SetServer(server); }
	lsn_t GetNext() noexcept {
		if (!counter_.isEmpty()) {
			return ++counter_;
		}
		counter_.SetCounter(0);
		return counter_;
	}
	void UpdateCounter(int64_t counter) {
		if (counter > 0 && counter > counter_.Counter()) {
			counter_.SetCounter(counter);
		}
	}

private:
	lsn_t counter_;
};

}  // namespace reindexer
