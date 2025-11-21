#pragma once

#include <stddef.h>
#include <climits>
#include "core/idset.h"
#include "core/type_consts.h"

namespace reindexer {

template <class T>
class [[nodiscard]] BtreeIndexIteratorImpl {
public:
	enum class [[nodiscard]] IdsetType { Plain = 0, Btree, EmptyIds };

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
			case IdsetType::EmptyIds:
				shiftNullValuesIdsetToBegin();
				break;
			default:
				std::abort();
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
			case IdsetType::EmptyIds:
				shiftNullValuesIdsetToNext();
				break;
			default:
				std::abort();
		}
	}

	bool isIdsetOver() const noexcept {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				return isBtreeIdsetOver();
			case IdsetType::Plain:
				return isPlainIdsetOver();
			case IdsetType::EmptyIds:
				return isNullValuesIdsetOver();
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
			case IdsetType::EmptyIds:
				currVal_ = getNullValuesIdsetCurrentValue();
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

	size_t getCurrentIdsetSize() const noexcept {
		switch (currentIdsetType_) {
			case IdsetType::Btree:
				return getBtreeIdsetSize();
			case IdsetType::Plain:
				return getPlainIdsetSize();
			case IdsetType::EmptyIds:
				return getNullValuesIdsetSize();
			default:
				std::abort();
		}
	}

	size_t getSize() const noexcept { return idxMap_.size() + getNullValuesIdsetSize(); }

	const IdType& getValue() const { return currVal_; }

protected:
	virtual void shiftPlainIdsetToBegin() = 0;
	virtual void shiftBtreeIdsetToBegin() = 0;
	virtual void shiftNullValuesIdsetToBegin() = 0;
	virtual void shiftPlainIdsetToNext() = 0;
	virtual void shiftNullValuesIdsetToNext() = 0;
	virtual void shiftBtreeIdsetToNext() = 0;
	virtual bool isPlainIdsetOver() const = 0;
	virtual bool isBtreeIdsetOver() const = 0;
	virtual bool isNullValuesIdsetOver() const = 0;
	virtual IdType getPlainIdsetCurrentValue() const = 0;
	virtual IdType getBtreeIdsetCurrentValue() const = 0;
	virtual IdType getNullValuesIdsetCurrentValue() const = 0;
	virtual size_t getPlainIdsetSize() const = 0;
	virtual size_t getBtreeIdsetSize() const = 0;
	virtual size_t getNullValuesIdsetSize() const = 0;

	const T& idxMap_;
	IdType currVal_ = INT_MIN;
	IdsetType currentIdsetType_;

	using ForwardIterator = typename T::const_iterator;
	using ReverseIterator = typename T::const_reverse_iterator;
};

template <class T>
class [[nodiscard]] BtreeIndexForwardIteratorImpl : public BtreeIndexIteratorImpl<T> {
public:
	using Base = BtreeIndexIteratorImpl<T>;
	explicit BtreeIndexForwardIteratorImpl(const T& idxMap, const base_idset_ptr& emptyIds) : Base(idxMap) {
		this->idxMapItBegin_ = idxMap.begin();
		this->idxMapItEnd_ = idxMap.end();
		this->idxMapIt_ = this->idxMapItBegin_;
		if (emptyIds) {
			this->emptyIdsItBegin_ = emptyIds->begin();
			this->emptyIdsItEnd_ = emptyIds->end();
			this->emptyIdsIt_ = this->emptyIdsItBegin_;
		}
	}
	BtreeIndexForwardIteratorImpl(const T& idxMap, const typename Base::ForwardIterator& first, const typename Base::ForwardIterator& last,
								  const base_idset_ptr& emptyIds)
		: Base(idxMap) {
		this->idxMapItBegin_ = first;
		this->idxMapItEnd_ = last;
		this->idxMapIt_ = this->idxMapItBegin_;
		if (emptyIds) {
			this->emptyIdsItBegin_ = emptyIds->begin();
			this->emptyIdsItEnd_ = emptyIds->end();
			this->emptyIdsIt_ = this->emptyIdsItBegin_;
		}
	}
	~BtreeIndexForwardIteratorImpl() override = default;

	void shiftToBegin() noexcept override {
		if (getNullValuesIdsetSize() > 0) {
			this->currentIdsetType_ = Base::IdsetType::EmptyIds;
			this->emptyIdsIt_ = this->emptyIdsItBegin_;
		} else {
			this->idxMapIt_ = this->idxMapItBegin_;
			if (!this->idxMap_.empty()) {
				this->detectCurrentIdsetType(this->idxMapIt_->second.Unsorted());
			}
		}
		this->currVal_ = INT_MIN;
	}

	void next() noexcept override {
		if (this->currentIdsetType_ == Base::IdsetType::EmptyIds) {
			this->idxMapIt_ = this->idxMapItBegin_;
		} else {
			++this->idxMapIt_;
		}
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

	void shiftNullValuesIdsetToNext() noexcept override {
		for (; emptyIdsIt_ != this->emptyIdsItEnd_ && *emptyIdsIt_ <= this->currVal_; ++emptyIdsIt_) {
		}
	}

	bool isOver() const noexcept override {
		if (this->currentIdsetType_ == Base::IdsetType::EmptyIds) {
			return (emptyIdsIt_ == this->emptyIdsItEnd_ && this->idxMap_.empty());
		}
		return this->idxMapIt_ == this->idxMapItEnd_;
	}

	void shiftPlainIdsetToBegin() noexcept override { it_ = this->idxMapIt_->second.Unsorted().begin(); }
	void shiftBtreeIdsetToBegin() noexcept override {
		itset_ = static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted()).set_->begin();
	}
	void shiftNullValuesIdsetToBegin() noexcept override { emptyIdsIt_ = this->emptyIdsItBegin_; }
	bool isPlainIdsetOver() const noexcept override { return it_ == this->idxMapIt_->second.Unsorted().end(); }
	bool isBtreeIdsetOver() const noexcept override {
		return itset_ == static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted()).set_->end();
	}
	bool isNullValuesIdsetOver() const noexcept override { return emptyIdsIt_ == this->emptyIdsItEnd_; }
	IdType getPlainIdsetCurrentValue() const noexcept override { return *it_; }
	IdType getBtreeIdsetCurrentValue() const noexcept override { return *itset_; }
	IdType getNullValuesIdsetCurrentValue() const noexcept override { return *emptyIdsIt_; }
	size_t getPlainIdsetSize() const noexcept override { return this->idxMapIt_->second.Unsorted().size(); }
	size_t getBtreeIdsetSize() const noexcept override {
		return static_cast<const IdSet&>(this->idxMapIt_->second.Unsorted()).set_->size();
	}
	size_t getNullValuesIdsetSize() const noexcept override { return emptyIdsItEnd_ - emptyIdsItBegin_; }
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
				case Base::IdsetType::EmptyIds:
					break;
				default:
					std::abort();
			}
		}
		if ((emptyIdsItBegin_ != emptyIdsItEnd_) && (cnt + this->getNullValuesIdsetSize() < limitIters)) {
			cnt += this->getNullValuesIdsetSize();
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
	IdSetPlain::const_iterator emptyIdsIt_{};
	IdSetPlain::const_iterator emptyIdsItBegin_{};
	IdSetPlain::const_iterator emptyIdsItEnd_{};
};

template <class T>
class [[nodiscard]] BtreeIndexReverseIteratorImpl : public BtreeIndexIteratorImpl<T> {
public:
	using Base = BtreeIndexIteratorImpl<T>;
	explicit BtreeIndexReverseIteratorImpl(const T& idxMap, const base_idset_ptr& emptyIds) : Base(idxMap) {
		idxMapRitBegin_ = idxMap.rbegin();
		idxMapRitEnd_ = idxMap.rend();
		idxMapRit_ = idxMapRitBegin_;
		if (emptyIds) {
			emptyIdsRitBegin_ = emptyIds->rbegin();
			emptyIdsRitEnd_ = emptyIds->rend();
			emptyIdsRit_ = emptyIdsRitBegin_;
		}
		this->currVal_ = INT_MAX;
	}
	BtreeIndexReverseIteratorImpl(const T& idxMap, const typename Base::ForwardIterator& first, const typename Base::ForwardIterator& last,
								  const base_idset_ptr& emptyIds)
		: Base(idxMap), idxMapRitBegin_(last), idxMapRitEnd_(first) {
		idxMapRit_ = idxMapRitBegin_;
		if (emptyIds) {
			emptyIdsRitBegin_ = emptyIds->rbegin();
			emptyIdsRitEnd_ = emptyIds->rend();
			emptyIdsRit_ = emptyIdsRitBegin_;
		}
	}

	~BtreeIndexReverseIteratorImpl() override = default;

	void shiftToBegin() noexcept override {
		this->idxMapRit_ = this->idxMapRitBegin_;
		if (!this->idxMap_.empty()) {
			this->detectCurrentIdsetType(this->idxMapRit_->second.Unsorted());
		} else if (getNullValuesIdsetSize() > 0) {
			emptyIdsRit_ = this->emptyIdsRitBegin_;
			this->currentIdsetType_ = Base::IdsetType::EmptyIds;
		}
		this->currVal_ = INT_MAX;
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

	void shiftNullValuesIdsetToNext() noexcept override {
		for (; emptyIdsRit_ != this->emptyIdsRitEnd_ && *emptyIdsRit_ >= this->currVal_; ++emptyIdsRit_) {
		}
	}

	void next() noexcept override {
		++this->idxMapRit_;
		if (this->idxMapRit_ == this->idxMapRitEnd_) {
			if (getNullValuesIdsetSize() > 0) {
				if (emptyIdsRit_ == this->emptyIdsRitBegin_) {
					this->currentIdsetType_ = Base::IdsetType::EmptyIds;
				}
			}
		} else {
			this->detectCurrentIdsetType(this->idxMapRit_->second.Unsorted());
		}
	}

	bool isOver() const noexcept override {
		if (this->idxMapRit_ == this->idxMapRitEnd_) {
			if (getNullValuesIdsetSize() == 0) {
				return true;
			}
			return (this->currentIdsetType_ == Base::IdsetType::EmptyIds && emptyIdsRit_ == this->emptyIdsRitEnd_);
		}
		return false;
	}

	void shiftPlainIdsetToBegin() noexcept override { rit_ = this->idxMapRit_->second.Unsorted().rbegin(); }
	void shiftBtreeIdsetToBegin() noexcept override {
		ritset_ = static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted()).set_->rbegin();
	}
	void shiftNullValuesIdsetToBegin() noexcept override { emptyIdsRit_ = this->emptyIdsRitBegin_; }
	bool isPlainIdsetOver() const noexcept override { return rit_ == this->idxMapRit_->second.Unsorted().rend(); }
	bool isBtreeIdsetOver() const noexcept override {
		return ritset_ == static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted()).set_->rend();
	}
	bool isNullValuesIdsetOver() const noexcept override { return emptyIdsRit_ == this->emptyIdsRitEnd_; }
	IdType getPlainIdsetCurrentValue() const noexcept override { return *rit_; }
	IdType getBtreeIdsetCurrentValue() const noexcept override { return *ritset_; }
	IdType getNullValuesIdsetCurrentValue() const noexcept override { return *emptyIdsRit_; }
	size_t getPlainIdsetSize() const noexcept override { return this->idxMapRit_->second.Unsorted().size(); }
	size_t getBtreeIdsetSize() const noexcept override {
		return static_cast<const IdSet&>(this->idxMapRit_->second.Unsorted()).set_->size();
	}
	size_t getNullValuesIdsetSize() const noexcept override { return emptyIdsRitBegin_.base() - emptyIdsRitEnd_.base(); }

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
	IdSetPlain::const_reverse_iterator emptyIdsRitBegin_{};
	IdSetPlain::const_reverse_iterator emptyIdsRitEnd_{};
	IdSetPlain::const_reverse_iterator emptyIdsRit_{};
};

}  // namespace reindexer
