#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <limits>
#include <vector>
#include "tools/assertrx.h"

namespace reindexer {

// Fixed-capacity open addressing map from unsigned integer IDs to offsets in external storage.
// Each slot stores the ID next to its offset; lookups do not access the external storage.
// kEmpty is reserved for offsets; all ID values are usable.
template <std::unsigned_integral IdT, std::unsigned_integral OffsetT>
class [[nodiscard]] IdOffsetHash {
public:
	static constexpr OffsetT kEmpty = std::numeric_limits<OffsetT>::max();

	// IDs are in [0, idRangeSize); insert at most min(maxEntries, idRangeSize) distinct IDs.
	void Init(size_t maxEntries, size_t idRangeSize) {
		size_t cap = 1;
		const size_t maxUniqueIds = std::min(maxEntries, idRangeSize);
		const size_t threeMaxUniqueIds = maxUniqueIds * 3;
		while (cap <= threeMaxUniqueIds) {
			assertrx_dbg(cap <= (std::numeric_limits<size_t>::max() >> 1));
			cap <<= 1;
		}
		if (cap > 1 && idRangeSize > (cap / 2)) {
			cap >>= 1;
		}
		slots_.assign(cap, Slot{});
		mask_ = cap - 1;
	}

	// On a miss, save the ID but leave the offset empty until the caller assigns it.
	// The reference remains valid until Init(); assign it before another lookup can reuse the empty slot.
	OffsetT& FindSlot(IdT id) noexcept {
		size_t i = id & mask_;
		for (;;) {
			auto& slot = slots_[i];
			if (slot.offset == kEmpty) {
				slot.id = id;
				return slot.offset;
			}
			if (slot.id == id) {
				return slot.offset;
			}
			i = (i + 1) & mask_;
		}
	}

private:
	struct [[nodiscard]] Slot {
		IdT id = 0;
		OffsetT offset = kEmpty;
	};

	std::vector<Slot> slots_;
	size_t mask_ = 0;
};

}  // namespace reindexer
