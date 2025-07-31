#pragma once

#include <stddef.h>
#include <climits>
#include "core/idset.h"
#include "core/type_consts.h"

namespace reindexer {

template <class T>
class BtreeIndexIteratorImpl {
public:
	enum class IdsetType { Plain = 0, Btree };

	explicit BtreeIndexIteratorImpl(const T& idxMap) : idxMap_(idxMap) {}
	virtual ~BtreeIndexIteratorImpl() = default;

	virtual bool isOver() const noexcept = 0;
	virtual void shiftToBegin() noexcept = 0;
	virtual void next() noexcept = 0;

	bool shiftToNextIdset() noexcept {
		if (isOver()) {
			return false;
		}
		for (next(); !isOver() && getCurrentIdsetSize() == 0;) {
			next();
		}
		if (isOver()) {
			return false;
		}
		shiftIdsetToBegin();
		updateCurrentValue();
		return true;
	}

	void shiftIdsetToBegin() {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				shiftBtreeIdsetToBegin();
				break;
			case IdsetType::Plain:
				shiftPlainIdsetToBegin();
				break;
		}
	}

	void shiftIdsetToNext() {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				shiftBtreeIdsetToNext();
				break;
			case IdsetType::Plain:
				shiftPlainIdsetToNext();
				break;
		}
	}
	bool isIdsetOver() const noexcept {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				return isBtreeIdsetOver();
			case IdsetType::Plain:
				return isPlainIdsetOver();
			default:
				std::abort();
		}
	}
	void updateCurrentValue() noexcept {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				currVal_ = getBtreeIdsetCurrentValue();
				break;
			case IdsetType::Plain:
				currVal_ = getPlainIdsetCurrentValue();
				break;
			default:
				std::abort();
		}
	}
	bool finishIteration() noexcept {
		currVal_ = INT_MAX;
		return false;
	}

	template <typename TIdSet>
	void detectCurrentIdsetType(const TIdSet& idset) noexcept {
		if (std::is_same<TIdSet, IdSet>() && !idset.IsCommitted()) {
			currentIdsetType_ = IdsetType::Btree;
		} else {
			currentIdsetType_ = IdsetType::Plain;
		}
	}

	size_t getSize() const noexcept { return idxMap_.size(); }
	size_t getCurrentIdsetSize() const noexcept {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				return getBtreeIdsetSize();
			case IdsetType::Plain:
				return getPlainIdsetSize();
			default:
				std::abort();
		}
	}
	const IdType& getValue() const { return currVal_; }

protected:
	virtual void shiftPlainIdsetToBegin() = 0;
	virtual void shiftBtreeIdsetToBegin() = 0;
	virtual void shiftPlainIdsetToNext() = 0;
	virtual void shiftBtreeIdsetToNext() = 0;
	virtual bool isPlainIdsetOver() const = 0;
	virtual bool isBtreeIdsetOver() const = 0;
	virtual IdType getPlainIdsetCurrentValue() const = 0;
	virtual IdType getBtreeIdsetCurrentValue() const = 0;
	virtual size_t getPlainIdsetSize() const = 0;
	virtual size_t getBtreeIdsetSize() const = 0;

	const T& idxMap_;
	IdType currVal_ = INT_MIN;
	IdsetType currentIdsetType_;

	using ForwardIterator = typename T::const_iterator;
	using ReverseIterator = typename T::const_reverse_iterator;
};

template <class T>
class BtreeIndexForwardIteratorImpl : public BtreeIndexIteratorImpl<T> {
public:
	using Base = BtreeIndexIteratorImpl<T>;
	explicit BtreeIndexForwardIteratorImpl(const T& idxMap) : Base(idxMap) {
		this->idxMapItBegin_ = idxMap.begin();
		this->idxMapItEnd_ = idxMap.end();
		this->idxMapIt_ = this->idxMapItBegin_;
	}
	BtreeIndexForwardIteratorImpl(const T& idxMap, const typename Base::ForwardIterator& first, const typename Base::ForwardIterator& last)
		: Base(idxMap) {
		this->idxMapItBegin_ = first;
		this->idxMapItEnd_ = last;
		this->idxMapIt_ = this->idxMapItBegin_;
	}
	~BtreeIndexForwardIteratorImpl() override = default;

	void shiftToBegin() noexcept override {
		this->idxMapIt_ = this->idxMapItBegin_;
		if (this->getSize() > 0) {
			this->detectCurrentIdsetType(this->idxMapIt_->second.Unsorted());
			this->currVal_ = INT_MIN;
		}
	}

	void next() noexcept override {
		++this->idxMapIt_;
		if (!isOver()) {
			this->detectCurrentIdsetType(this->idxMapIt_->second.Unsorted());
		}
	}

	void shiftPlainIdsetToNext() noexcept override {
		const auto& idset = this->idxMapIt_->second.Unsorted();
		for (; it_ != idset.end() && *it_ <= this->currVal_; ++it_) {
		}
	}
	void shiftBtreeIdsetToNext() noexcept override {
		const IdSet& sortedIdset = static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted());
		for (; itset_ != sortedIdset.set_->end() && *itset_ <= this->currVal_; ++itset_) {
		}
	}

	bool isOver() const noexcept override { return this->idxMapIt_ == this->idxMapItEnd_; }
	void shiftPlainIdsetToBegin() noexcept override { it_ = this->idxMapIt_->second.Unsorted().begin(); }
	void shiftBtreeIdsetToBegin() noexcept override {
		itset_ = static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted()).set_->begin();
	}
	bool isPlainIdsetOver() const noexcept override { return it_ == this->idxMapIt_->second.Unsorted().end(); }
	bool isBtreeIdsetOver() const noexcept override {
		return itset_ == static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted()).set_->end();
	}
	IdType getPlainIdsetCurrentValue() const noexcept override { return *it_; }
	IdType getBtreeIdsetCurrentValue() const noexcept override { return *itset_; }
	size_t getPlainIdsetSize() const noexcept override { return this->idxMapIt_->second.Unsorted().size(); }
	size_t getBtreeIdsetSize() const noexcept override {
		return static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted()).set_->size();
	}
	std::pair<size_t, bool> getMaxIterations(size_t limitIters) noexcept {
		size_t cnt = 0;
		auto it = idxMapItBegin_;
		for (; cnt < limitIters && it != idxMapItEnd_; ++it) {
			this->detectCurrentIdsetType(it->second.Unsorted());
			switch (this->currentIdsetType_) {
				case Base::IdsetType::Btree:
					cnt += static_cast<const IdSet&>(it->second.Unsorted()).set_->size();
					break;
				case Base::IdsetType::Plain:
					cnt += it->second.Unsorted().size();
					break;
				default:
					std::abort();
			}
		}
		return {cnt, it == idxMapItEnd_};
	}

private:
#ifdef REINDEX_DEBUG_CONTAINERS
	IdSetPlain::const_iterator it_;
	base_idsetset::const_iterator itset_;

#else  // !REINDEX_DEBUG_CONTAINERS
	union {
		IdSetPlain::const_iterator it_;
		base_idsetset::const_iterator itset_;
	};

#endif	// !REINDEX_DEBUG_CONTAINERS

	typename Base::ForwardIterator idxMapItBegin_;
	typename Base::ForwardIterator idxMapItEnd_;
	typename Base::ForwardIterator idxMapIt_;
};

template <class T>
class BtreeIndexReverseIteratorImpl : public BtreeIndexIteratorImpl<T> {
public:
	using Base = BtreeIndexIteratorImpl<T>;
	explicit BtreeIndexReverseIteratorImpl(const T& idxMap) : Base(idxMap) {
		idxMapRitBegin_ = idxMap.rbegin();
		idxMapRitEnd_ = idxMap.rend();
		idxMapRit_ = idxMapRitBegin_;
		this->currVal_ = INT_MAX;
	}
	BtreeIndexReverseIteratorImpl(const T& idxMap, const typename Base::ForwardIterator& first, const typename Base::ForwardIterator& last)
		: Base(idxMap), idxMapRitBegin_(last), idxMapRitEnd_(first) {
		idxMapRit_ = idxMapRitBegin_;
	}

	~BtreeIndexReverseIteratorImpl() override = default;

	void shiftToBegin() noexcept override {
		this->idxMapRit_ = this->idxMapRitBegin_;
		if (this->getSize() > 0) {
			this->detectCurrentIdsetType(this->idxMapRit_->second.Unsorted());
			this->currVal_ = INT_MAX;
		}
	}

	void shiftPlainIdsetToNext() noexcept override {
		const auto& idset = this->idxMapRit_->second.Unsorted();
		for (; rit_ != idset.rend() && *rit_ >= this->currVal_; ++rit_) {
		}
	}

	void shiftBtreeIdsetToNext() noexcept override {
		const IdSet& sortedIdset = static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted());
		for (; ritset_ != sortedIdset.set_->rend() && *ritset_ >= this->currVal_; ++ritset_) {
		}
	}

	void next() noexcept override {
		++this->idxMapRit_;
		if (!isOver()) {
			this->detectCurrentIdsetType(this->idxMapRit_->second.Unsorted());
		}
	}

	bool isOver() const noexcept override { return idxMapRit_ == idxMapRitEnd_; }
	void shiftPlainIdsetToBegin() noexcept override { rit_ = this->idxMapRit_->second.Unsorted().rbegin(); }
	void shiftBtreeIdsetToBegin() noexcept override {
		ritset_ = static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted()).set_->rbegin();
	}
	bool isPlainIdsetOver() const noexcept override { return rit_ == this->idxMapRit_->second.Unsorted().rend(); }
	bool isBtreeIdsetOver() const noexcept override {
		return ritset_ == static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted()).set_->rend();
	}
	IdType getPlainIdsetCurrentValue() const noexcept override { return *rit_; }
	IdType getBtreeIdsetCurrentValue() const noexcept override { return *ritset_; }
	size_t getPlainIdsetSize() const noexcept override { return this->idxMapRit_->second.Unsorted().size(); }
	size_t getBtreeIdsetSize() const noexcept override {
		return static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted()).set_->size();
	}

private:
#ifdef REINDEX_DEBUG_CONTAINERS
	IdSetPlain::const_reverse_iterator rit_;
	base_idsetset::const_reverse_iterator ritset_;
#else
	union {
		IdSetPlain::const_reverse_iterator rit_;
		base_idsetset::const_reverse_iterator ritset_;
	};

#endif
	typename Base::ReverseIterator idxMapRitBegin_;
	typename Base::ReverseIterator idxMapRitEnd_;
	typename Base::ReverseIterator idxMapRit_;
};

}  // namespace reindexer
