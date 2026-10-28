#pragma once

#include <cstddef>
#include <type_traits>
#include "core/ft/idrelset.h"
#include "estl/lock.h"
#include "estl/spin_lock.h"

namespace reindexer {

struct [[nodiscard]] PostingsMemSize {
	size_t capacity = 0;
	size_t packed = 0;

	template <typename IdCont>
	static PostingsMemSize Measure(const IdCont& postings) noexcept {
		PostingsMemSize size{.capacity = postings.heap_size()};
		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			size.packed = postings.data_size();
		}
		return size;
	}
	PostingsMemSize& operator+=(PostingsMemSize other) noexcept {
		capacity += other.capacity;
		packed += other.packed;
		return *this;
	}
};

struct [[nodiscard]] PostingsMemDelta {
	PostingsMemSize removed;
	PostingsMemSize added;

	void Replace(PostingsMemSize before, PostingsMemSize after) noexcept {
		removed += before;
		added += after;
	}
	bool Empty() const noexcept { return removed.capacity == 0 && removed.packed == 0 && added.capacity == 0 && added.packed == 0; }
};

struct [[nodiscard]] TypoStatDelta {
	size_t groupsHeapAdded = 0;
	size_t keysAdded = 0;
};

// Tasks apply their accumulated changes once on exit. During cleanup the snapshot
// may lag behind published lists until task completion, but both postings counters
// always describe the same set of applied task deltas.
class [[nodiscard]] FtMemoryStatCache {
public:
	struct [[nodiscard]] Snapshot {
		PostingsMemSize postings;
		size_t typos = 0;
		size_t typosKeys = 0;
	};

	Snapshot Get() const noexcept RX_REQUIRES(!mtx_) {
		lock_guard lck(mtx_);
		return stats_;
	}
	void AddPostings(PostingsMemSize added) noexcept RX_REQUIRES(!mtx_) { ApplyPostings(PostingsMemDelta{.removed = {}, .added = added}); }
	void SetPostings(PostingsMemSize size) noexcept RX_REQUIRES(!mtx_) {
		lock_guard lck(mtx_);
		stats_.postings = size;
	}
	void ApplyPostings(const PostingsMemDelta& delta) noexcept RX_REQUIRES(!mtx_) {
		if (delta.Empty()) {
			return;
		}
		lock_guard lck(mtx_);
		stats_.postings.capacity = stats_.postings.capacity - delta.removed.capacity + delta.added.capacity;
		stats_.postings.packed = stats_.postings.packed - delta.removed.packed + delta.added.packed;
	}
	void UpdateTypos(size_t tableBefore, size_t tableAfter, const TypoStatDelta& delta) noexcept RX_REQUIRES(!mtx_) {
		lock_guard lck(mtx_);
		stats_.typos = stats_.typos - tableBefore + tableAfter + delta.groupsHeapAdded;
		stats_.typosKeys += delta.keysAdded;
	}
	void Clear() noexcept RX_REQUIRES(!mtx_) {
		lock_guard lck(mtx_);
		stats_ = {};
	}

private:
	mutable spinlock mtx_;
	Snapshot stats_ RX_GUARDED_BY(mtx_);
};

}  // namespace reindexer
