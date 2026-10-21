#pragma once

#include <stddef.h>
#include "core/idset/idset.h"

namespace reindexer {
namespace index {
namespace iterators {

template <class IndexIterator, class IdSetIterator, class IdSetIteratorRange>
class [[nodiscard]] BtreeIndexIteratorImplBase {
public:
	using IndexIteratorType = IndexIterator;

	BtreeIndexIteratorImplBase() = default;
	// NOLINTNEXTLINE(performance-unnecessary-value-param)
	BtreeIndexIteratorImplBase(IndexIterator begin, IndexIterator end) : indexItBegin_(begin), indexItEnd_(end), indexIt_(indexItBegin_) {}
	// NOLINTNEXTLINE(performance-unnecessary-value-param)
	BtreeIndexIteratorImplBase(IndexIterator begin, IndexIterator end, const IdSetIteratorRange& nullValuesRange, size_t nullValuesCount)
		: indexItBegin_(begin),
		  indexItEnd_(end),
		  indexIt_(indexItBegin_),
		  nullValuesRange_(nullValuesRange),
		  nullValuesIt_(nullValuesRange_.begin()),
		  nullValuesCount_(nullValuesCount) {}
	~BtreeIndexIteratorImplBase() = default;

	std::pair<size_t, bool> MaxIterations(size_t limit) const noexcept {
		size_t iterations = 0;
		auto it = indexItBegin_;
		for (; iterations < limit && it != indexItEnd_; ++it) {
			iterations += it->second.Unsorted().Size();
		}
		const bool fullyScanned = it == indexItEnd_;
		if (fullyScanned) {
			iterations += nullValuesCount_;
		}
		return {iterations, fullyScanned};
	}

protected:
	void skipEmptyIdSets() noexcept {
		while ((indexIt_ != indexItEnd_) && (indexIt_->second.Unsorted().Size() == 0)) {
			++indexIt_;
		}
	}

	IndexIterator indexItBegin_;
	IndexIterator indexItEnd_;
	IndexIterator indexIt_;
	IdSetIterator idsetIt_;
	IdSetIteratorRange idsetRange_;
	IdSetIteratorRange nullValuesRange_;
	IdSetIterator nullValuesIt_;
	size_t nullValuesCount_ = 0;
	bool nullValuesMode_ = false;
};

template <class IndexMap>
class [[nodiscard]] BtreeIndexForwardIteratorImpl
	: public BtreeIndexIteratorImplBase<typename IndexMap::const_iterator, typename IdSet::idset_iterator,
										typename IdSet::idset_iterator_range> {
	using Base =
		BtreeIndexIteratorImplBase<typename IndexMap::const_iterator, typename IdSet::idset_iterator, typename IdSet::idset_iterator_range>;

public:
	BtreeIndexForwardIteratorImpl() = default;
	BtreeIndexForwardIteratorImpl(Base::IndexIteratorType begin, Base::IndexIteratorType end) : Base(begin, end) {}
	BtreeIndexForwardIteratorImpl(Base::IndexIteratorType begin, Base::IndexIteratorType end, const IdSet& nullValues)
		: Base(begin, end, nullValues.idset_range(), nullValues.Size()) {}

	void Start() noexcept {
		if (this->nullValuesCount_ > 0) {
			this->nullValuesIt_ = this->nullValuesRange_.begin();
			this->nullValuesMode_ = true;
		} else {
			std::ignore = moveToIndexBegin();
		}
	}

	std::pair<bool, IdType> Next() noexcept {
		if (this->nullValuesMode_) {
			if (this->nullValuesIt_ != this->nullValuesRange_.end()) {
				return {true, *this->nullValuesIt_++};
			}
			this->nullValuesMode_ = false;
			if (!moveToIndexBegin()) {
				return {false, IdType::Min()};
			}
		} else if (this->indexIt_ == this->indexItEnd_) {
			return {false, IdType::Min()};
		} else if (this->idsetIt_ == this->idsetRange_.end()) {
			++this->indexIt_;
			if (!moveToValidIdset()) {
				return {false, IdType::Min()};
			}
		}
		return {true, *this->idsetIt_++};
	}

	void SkipKey() noexcept {
		if (this->nullValuesMode_) {
			std::ignore = moveToIndexBegin();
			this->nullValuesMode_ = false;
		} else if (this->indexIt_ != this->indexItEnd_) {
			++this->indexIt_;
			std::ignore = moveToValidIdset();
		}
	}

private:
	bool moveToIndexBegin() noexcept {
		this->indexIt_ = this->indexItBegin_;
		return moveToValidIdset();
	}

	bool moveToValidIdset() noexcept {
		this->skipEmptyIdSets();
		if (this->indexIt_ != this->indexItEnd_) {
			this->idsetRange_ = this->indexIt_->second.Unsorted().idset_range();
			this->idsetIt_ = this->idsetRange_.begin();
			return true;
		}
		return false;
	}
};

template <class IndexMap>
class [[nodiscard]] BtreeIndexReverseIteratorImpl
	: public BtreeIndexIteratorImplBase<typename IndexMap::const_reverse_iterator, typename IdSet::idset_reverse_iterator,
										typename IdSet::idset_reverse_iterator_range> {
	using Base = BtreeIndexIteratorImplBase<typename IndexMap::const_reverse_iterator, typename IdSet::idset_reverse_iterator,
											typename IdSet::idset_reverse_iterator_range>;

public:
	BtreeIndexReverseIteratorImpl() = default;
	BtreeIndexReverseIteratorImpl(typename IndexMap::const_iterator begin, typename IndexMap::const_iterator end)
		: Base(typename IndexMap::const_reverse_iterator(end), typename IndexMap::const_reverse_iterator(begin)) {}
	BtreeIndexReverseIteratorImpl(typename IndexMap::const_iterator begin, typename IndexMap::const_iterator end, const IdSet& nullValues)
		: Base(typename IndexMap::const_reverse_iterator(end), typename IndexMap::const_reverse_iterator(begin),
			   nullValues.idset_reverse_range(), nullValues.Size()) {}

	void Start() noexcept {
		std::ignore = moveToIndexRbegin();
		if (this->indexIt_ == this->indexItEnd_) {
			std::ignore = moveToNullValuesIdSet();
		}
	}

	std::pair<bool, IdType> Next() noexcept {
		if (this->nullValuesMode_) {
			if (this->nullValuesIt_ != this->nullValuesRange_.end()) {
				return {true, *this->nullValuesIt_++};
			}
			return {false, IdType::Max()};
		}
		if (this->indexIt_ != this->indexItEnd_) {
			if (this->idsetIt_ == this->idsetRange_.end()) {
				++this->indexIt_;
				if (moveToValidIdset()) {
					return {true, *this->idsetIt_++};
				}
			} else {
				return {true, *this->idsetIt_++};
			}
		}
		if (moveToNullValuesIdSet() && this->nullValuesIt_ != this->nullValuesRange_.end()) {
			return {true, *this->nullValuesIt_++};
		}
		return {false, IdType::Max()};
	}

	void SkipKey() noexcept {
		if (this->nullValuesMode_) {
			this->nullValuesIt_ = this->nullValuesRange_.end();
		} else if (this->indexIt_ != this->indexItEnd_) {
			++this->indexIt_;
			std::ignore = moveToValidIdset();
		}
	}

private:
	bool moveToIndexRbegin() noexcept {
		this->indexIt_ = this->indexItBegin_;
		return moveToValidIdset();
	}

	bool moveToValidIdset() noexcept {
		this->skipEmptyIdSets();
		if (this->indexIt_ != this->indexItEnd_) {
			this->idsetRange_ = this->indexIt_->second.Unsorted().idset_reverse_range();
			this->idsetIt_ = this->idsetRange_.begin();
			return true;
		}
		return false;
	}

	bool moveToNullValuesIdSet() noexcept {
		if (this->nullValuesCount_ > 0) {
			this->nullValuesIt_ = this->nullValuesRange_.begin();
			this->nullValuesMode_ = true;
			return true;
		}
		return false;
	}
};

}  // namespace iterators
}  // namespace index
}  // namespace reindexer
