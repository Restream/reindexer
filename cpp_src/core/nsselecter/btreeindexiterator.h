#pragma once

#include <limits.h>
#include "btreeindexiteratorimpl.h"
#include "core/idset.h"
#include "core/index/indexiterator.h"

namespace reindexer {

template <class T>
class [[nodiscard]] BtreeIndexIterator final : public IndexIterator {
public:
	BtreeIndexIterator(const T& idxMap, base_idset_ptr&& empty_ids) noexcept
		: idxMap_(idxMap), first_(idxMap.begin()), last_(idxMap.end()), empty_ids_(std::move(empty_ids)) {}
	BtreeIndexIterator(const T& idxMap, const typename T::iterator& first, const typename T::iterator& last) noexcept
		: idxMap_(idxMap), first_(first), last_(last) {}
	~BtreeIndexIterator() override final = default;

	void Start(bool reverse) final override {
		if (reverse) {
			impl_ = std::make_shared<BtreeIndexReverseIteratorImpl<T>>(idxMap_, first_, last_, empty_ids_);
		} else {
			impl_ = std::make_shared<BtreeIndexForwardIteratorImpl<T>>(idxMap_, first_, last_, empty_ids_);
		}
		impl_->shiftToBegin();
		if (impl_->getSize() == 0) {
			return;
		}
		impl_->shiftIdsetToBegin();
	}

	bool Next() noexcept final override {
		assertrx_dbg(impl_);
		if (impl_->isOver()) {
			return impl_->finishIteration();
		}

		impl_->shiftIdsetToNext();
		if (impl_->isIdsetOver() && !impl_->shiftToNextIdset()) {
			return impl_->finishIteration();
		}

		impl_->updateCurrentValue();
		return true;
	}

	void ExcludeLastSet() noexcept override {
		assertrx_dbg(impl_);
		impl_->shiftToNextIdset();
	}

	IdType Value() const noexcept override final {
		assertrx_dbg(impl_);
		return impl_->getValue();
	}
	size_t GetMaxIterations(size_t limitIters) noexcept final {
		auto limit = std::min(kMaxBTreeIterations, limitIters);
		if (!cachedIters_.Valid(limit)) {
			auto [iters, fullyScanned] = BtreeIndexForwardIteratorImpl<T>(idxMap_, first_, last_, empty_ids_).getMaxIterations(limit);

			if (iters >= kMaxBTreeIterations && !fullyScanned) {
				cachedIters_ = CachedIters{std::numeric_limits<size_t>::max(), true};
			} else if (fullyScanned || iters > cachedIters_.value || cachedIters_.value == std::numeric_limits<size_t>::max()) {
				cachedIters_ = CachedIters{iters, fullyScanned};
			}
		}

		return std::min(cachedIters_.value, limitIters);
	}
	void SetMaxIterations(size_t iters) noexcept final { cachedIters_ = CachedIters{iters, true}; }

private:
	static constexpr size_t kMaxBTreeIterations = 200'000;

	std::shared_ptr<BtreeIndexIteratorImpl<T>> impl_;
	const T& idxMap_;
	const typename T::const_iterator first_;
	const typename T::const_iterator last_;

	base_idset_ptr empty_ids_ = nullptr;

	struct [[nodiscard]] CachedIters {
		bool Valid(size_t limitIters) const noexcept {
			return fullyScanned || (limitIters <= value && value != std::numeric_limits<size_t>::max());
		}

		size_t value = std::numeric_limits<size_t>::max();
		bool fullyScanned = false;
	} cachedIters_;
};

}  // namespace reindexer
