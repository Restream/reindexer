#pragma once

#include <limits.h>
#include "btreeindexiteratorimpl.h"
#include "core/idset.h"
#include "core/index/indexiterator.h"

namespace reindexer {

template <class T>
class BtreeIndexIterator final : public IndexIterator {
public:
	explicit BtreeIndexIterator(const T& idxMap) noexcept : idxMap_(idxMap), first_(idxMap.begin()), last_(idxMap.end()) {}
	BtreeIndexIterator(const T& idxMap, const typename T::iterator& first, const typename T::iterator& last) noexcept
		: idxMap_(idxMap), first_(first), last_(last) {}
	~BtreeIndexIterator() override final = default;

	void Start(bool reverse) final override {
		if (reverse) {
			impl_ = std::make_shared<BtreeIndexReverseIteratorImpl<T>>(idxMap_, first_, last_);
		} else {
			impl_ = std::make_shared<BtreeIndexForwardIteratorImpl<T>>(idxMap_, first_, last_);
		}
		if (impl_->getSize() == 0) return;
		impl_->shiftToBegin();
		impl_->shiftIdsetToBegin();
	}

	bool Next() noexcept final override {
		assertrx(impl_);
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
		assertrx(impl_);
		impl_->shiftToNextIdset();
	}

	IdType Value() const noexcept override final {
		assertrx(impl_);
		return impl_->getValue();
	}
	size_t GetMaxIterations(size_t limitIters) noexcept final {
		if (cachedIters_ != std::numeric_limits<size_t>::max()) return cachedIters_;
		return BtreeIndexForwardIteratorImpl<T>(idxMap_, first_, last_).getMaxIterations(limitIters);
	}
	void SetMaxIterations(size_t iters) noexcept final { cachedIters_ = iters; }

private:
	std::shared_ptr<BtreeIndexIteratorImpl<T>> impl_;
	const T& idxMap_;
	const typename T::const_iterator first_;
	const typename T::const_iterator last_;
	size_t cachedIters_ = std::numeric_limits<size_t>::max();
};

}  // namespace reindexer
