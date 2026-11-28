#pragma once

#include <bitset>
#include <optional>
#include "estl/fast_hash_set.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] AllSetMatchState {
public:
	static constexpr size_t kMaxBitsetValues = 100;

	AllSetMatchState() = default;
	explicit AllSetMatchState(size_t count) { Reset(count); }

	void Reset(size_t count) {
		count_ = count;
		matchedCount_ = 0;
		if (count_ <= kMaxBitsetValues) {
			bits_.reset();
		} else if (hashed_) {
			hashed_->clear();
		} else {
			hashed_.emplace();
		}
	}

	// IDs must be dense in [0, count). Repeated matches keep the completed state.
	bool Mark(size_t id) {
		assertrx_dbg(id < count_);
		if (count_ <= kMaxBitsetValues) {
			if (!bits_[id]) {
				bits_[id] = true;
				++matchedCount_;
			}
		} else {
			if (!hashed_) [[unlikely]] {
				throw_assert(hashed_.has_value());
			}
			if (hashed_->insert(id).second) {
				++matchedCount_;
			}
		}
		return matchedCount_ == count_;
	}

private:
	std::bitset<kMaxBitsetValues> bits_;
	std::optional<fast_hash_set<size_t>> hashed_;
	size_t count_{0};
	size_t matchedCount_{0};
};

}  // namespace reindexer
