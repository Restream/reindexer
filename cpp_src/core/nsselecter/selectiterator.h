#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
#include "core/enums.h"
#include "core/id_type.h"
#include "core/selectkeyresult.h"
#include "estl/concepts.h"
#include "estl/heapify.h"
#include "tools/gallop_search.h"

namespace reindexer {

class StreamingKnnIndexIterator;

/// Allows to iterate over a result of selecting
/// data for one certain key.
class [[nodiscard]] SelectIterator : public SelectKeyResult {
public:
	enum class [[nodiscard]] Type : uint8_t {
		None,
		Forward,
		Reverse,
		SingleRange,
		SingleIdset,
		SingleIdSetWithDeferedSort,
		RevSingleRange,
		RevSingleIdset,
		RevSingleIdSetWithDeferedSort,
		Unsorted,
		UnbuiltSortOrdersIndex,
	};

	template <concepts::ConvertibleToString Str>
	SelectIterator(SelectKeyResult&& res, reindexer::IsDistinct distinct, Str&& n, int indexNo,
				   ForcedFirst forcedFirst = ForcedFirst_False) noexcept
		: SelectKeyResult(std::move(res)),
		  name(std::forward<Str>(n)),
		  type_(Type::None),
		  forcedFirst_(forcedFirst),
		  distinct_(distinct),
		  indexNo_(indexNo) {}

	/// Starts iteration process: prepares
	/// object for further work.
	/// @param reverse - direction of iteration.
	/// @param maxIterations - expected max iterations in select loop
	void Start(bool reverse, int maxIterations) {
		const bool explicitSort = applyDeferedSort(maxIterations);

		const auto begIt = begin();
		lastPos_ = 0;
		isReverse_ = reverse;
		heap_.clear();

		for (auto it = begIt, endIt = end(); it != endIt; ++it) {
			it->Start(isReverse_);
		}

		lastVal_ = (isReverse_) ? IdType::Max() : IdType::Min();
		if (isUnsorted_) {
			type_ = Type::Unsorted;
		} else if (size() == 1) {
			switch (begIt->collectionType_) {
				case SingleSelectKeyResult::Collection::NotSet:
					throw_assert(false);
				case SingleSelectKeyResult::Collection::SingleIterator:
					type_ = Type::UnbuiltSortOrdersIndex;
					break;
				case SingleSelectKeyResult::Collection::Range:
					type_ = isReverse_ ? Type::RevSingleRange : Type::SingleRange;
					break;
				case SingleSelectKeyResult::Collection::FlatIdSet:
				case SingleSelectKeyResult::Collection::TreeIdSet:
					if (isReverse_) {
						type_ = explicitSort ? Type::RevSingleIdSetWithDeferedSort : Type::RevSingleIdset;
					} else {
						type_ = explicitSort ? Type::SingleIdSetWithDeferedSort : Type::SingleIdset;
					}
					break;
			}
		} else {
			type_ = isReverse_ ? Type::Reverse : Type::Forward;
			buildHeap();
		}
		assertrx_dbg(GetType() != Type::None);
	}
	/// Signalizes if iteration is over.
	/// @return true if iteration is done.
	RX_ALWAYS_INLINE bool End() const noexcept {
		assertrx_dbg(GetType() != Type::None);
		return isReverse_ ? (lastVal_ == IdType::Min()) : (lastVal_ == IdType::Max());
	}
	/// Iterates to a next item of result. Increments 'matched' and 'total' counters.
	/// @param minHint - rowId value to start from.
	/// @return true if operation succeed.
	RX_ALWAYS_INLINE bool Next(IdType minHint) noexcept {
		assertrx_dbg(GetType() != Type::None);

		++totalCalls_;
		bool res = nextImpl(minHint);
		matchedCount_ += int(res);
		return res;
	}
	/// Checks if current iterator contains specified ID. Adjusts iterator if needed.
	/// Increments 'matched' and 'total' counters.
	/// @param targetId - rowId value to search.
	/// @return true if targetId was found.
	RX_ALWAYS_INLINE bool Compare(IdType targetId) noexcept {
		assertrx_dbg(GetType() != Type::None);

		++totalCalls_;
		const auto val = Val();
		if (isReverse_) {
			if (val < targetId) {
				return false;
			}
		} else if (val > targetId) {
			return false;
		}

		if (val == targetId || (nextImpl(targetId) && targetId == Val())) {
			++matchedCount_;
			return true;
		}
		return false;
	}

	/// Sets Unsorted iteration mode
	RX_ALWAYS_INLINE void SetUnsorted() noexcept { isUnsorted_ = true; }

	StreamingKnnIndexIterator* TryGetKnnStreamingIterator() const noexcept;

	/// Current rowId
	RX_ALWAYS_INLINE IdType Val() const noexcept { return lastVal_; }

	/// Current rowId index since the beginning
	/// of current SingleKeyValue object.
	/// Implemented for unsorted sets only
	int PosUnsorted() const {
		switch (type_) {
			case Type::Unsorted: {
				const auto& res = operator[](lastPos_);
				assertrx_throw(res.collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
				assertrx_dbg(res.direction_ == SingleSelectKeyResult::Direction::Forward);
				return res.flatIds_.u.fwd.it - res.flatIds_.u.fwd.begin - 1;
			}
			case Type::SingleIdset:
			case Type::SingleIdSetWithDeferedSort:
			case Type::Forward:
			case Type::Reverse:
			case Type::SingleRange:
			case Type::RevSingleRange:
			case Type::RevSingleIdset:
			case Type::RevSingleIdSetWithDeferedSort:
			case Type::UnbuiltSortOrdersIndex:
			case Type::None:
			default:
				// Not implemented
				throw_assert(false);
				return 0;
		}
	}

	/// @return number of matching items
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}

	/// Excludes last set of ids from each result
	/// to remove duplicated keys
	void ExcludeLastSet(IdType rowId) {
		assertrx_dbg(GetType() != Type::None);

		if (type_ == Type::UnbuiltSortOrdersIndex) {
			assertrx_dbg(begin()->collectionType_ == SingleSelectKeyResult::Collection::SingleIterator);
			auto& fwdIter = begin()->idxFwdIter_;
			if (fwdIter->Value() == rowId) {
				fwdIter->ExcludeLastSet();
			}
		} else if (!End() && lastPos_ != size() && lastVal_ == rowId) {
			auto& curRes = operator[](lastPos_);
			if (curRes.collectionType_ == SingleSelectKeyResult::Collection::TreeIdSet) {
				if (isReverse_) {
					curRes.treeIds_.u.rev.it = curRes.treeIds_.u.rev.end;
				} else {
					curRes.treeIds_.u.fwd.it = curRes.treeIds_.u.fwd.end;
				}
			} else {
				assertrx_throw(curRes.collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
				if (isReverse_) {
					curRes.flatIds_.u.rev.it = curRes.flatIds_.u.rev.end;
				} else {
					curRes.flatIds_.u.fwd.it = curRes.flatIds_.u.fwd.end;
				}
			}
			// Remove the last value from the heap; it must always be at the top
			if (!heap_.empty()) {
				assertrx(heap_.front().pos == lastPos_);
				if (isReverse_) {
					removeHeapRoot<RevHeapLess>(heap_);
				} else {
					removeHeapRoot<FwdHeapGreater>(heap_);
				}
			}
		}
	}

	/// Appends result to an existing set.
	/// @param other - results to add.
	void Append(SelectKeyResult&& other) {
		reserve(size() + other.size());
		for (auto& r : other) {
			emplace_back(std::move(r));
		}
		other.clear();
		type_ = Type::None;
	}
	/// Cost value used for sorting: object with a smaller
	/// cost goes before others.
	double Cost(int expectedIterations) const noexcept {
		assertrx_dbg(GetType() != Type::None);

		if (type_ == Type::UnbuiltSortOrdersIndex) {
			return -1;
		}
		if (forcedFirst_) {
			return -static_cast<double>(GetMaxIterations());
		}
		double result{0.0};
		const auto sz = size();
		if (distinct_) {
			result += sz;
		} else if (type_ != Type::SingleIdSetWithDeferedSort && type_ != Type::RevSingleIdSetWithDeferedSort && !deferedExplicitSort) {
			result += static_cast<double>(GetMaxIterations()) * sz;
		} else {
			result += static_cast<double>(CostWithDefferedSort(sz, GetMaxIterations(), expectedIterations));
		}
		return isNotOperation_ ? expectedIterations + result : result;
	}

	void SetNotOperationFlag(bool isNotOperation) noexcept { isNotOperation_ = isNotOperation; }

	Type GetType() const noexcept { return type_; }
	std::string_view TypeName() const noexcept;
	std::string Dump() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return distinct_; }
	int IndexNo() const noexcept { return indexNo_; }

	std::string name;

private:
	struct [[nodiscard]] HeapEntry {
		IdType value;
		unsigned pos;
	};
	using HeapT = h_vector<HeapEntry, 128>;

	struct [[nodiscard]] FwdHeapGreater {
		RX_ALWAYS_INLINE bool operator()(const HeapEntry& l, const HeapEntry& r) const noexcept {
			return (l.value == r.value) ? (l.pos > r.pos) : (l.value > r.value);
		}
	};

	struct [[nodiscard]] RevHeapLess {
		RX_ALWAYS_INLINE bool operator()(const HeapEntry& l, const HeapEntry& r) const noexcept {
			return (l.value == r.value) ? (l.pos > r.pos) : (l.value < r.value);
		}
	};

	RX_ALWAYS_INLINE static bool advanceFwd(SingleSelectKeyResult& it, IdType bound, IdType& value) noexcept {
		assertrx_dbg(it.direction_ == SingleSelectKeyResult::Direction::Forward);
		switch (it.collectionType_) {
			case SingleSelectKeyResult::Collection::NotSet:
			case SingleSelectKeyResult::Collection::SingleIterator:
				assertrx_dbg(false);
				break;
			case SingleSelectKeyResult::Collection::FlatIdSet:
				if (it.flatIds_.u.fwd.it != it.flatIds_.u.fwd.end) {
					it.flatIds_.u.fwd.it = GallopUpperBound(it.flatIds_.u.fwd.it, it.flatIds_.u.fwd.end, bound);
					if (it.flatIds_.u.fwd.it != it.flatIds_.u.fwd.end) {
						value = *it.flatIds_.u.fwd.it;
						return true;
					}
				}
				break;
			case SingleSelectKeyResult::Collection::TreeIdSet:
				if (it.treeIds_.u.fwd.it != it.treeIds_.u.fwd.end) {
					if (*it.treeIds_.u.fwd.it <= bound) {
						it.treeIds_.u.fwd.it = it.treeIds_.ptr->upper_bound(bound);
					}
					if (it.treeIds_.u.fwd.it != it.treeIds_.u.fwd.end) {
						value = *it.treeIds_.u.fwd.it;
						return true;
					}
				}
				break;
			case SingleSelectKeyResult::Collection::Range:
				if (it.range_.u.fwd.it != it.range_.u.fwd.end) {
					it.range_.u.fwd.it = std::min(it.range_.u.fwd.end, std::max(it.range_.u.fwd.it, bound.Incr()));
					if (it.range_.u.fwd.it != it.range_.u.fwd.end) {
						value = it.range_.u.fwd.it;
						return true;
					}
				}
				break;
		}
		return false;
	}

	RX_ALWAYS_INLINE static bool advanceRev(SingleSelectKeyResult& it, IdType bound, IdType& value) noexcept {
		assertrx_dbg(it.direction_ == SingleSelectKeyResult::Direction::Reverse);

		switch (it.collectionType_) {
			case SingleSelectKeyResult::Collection::NotSet:
			case SingleSelectKeyResult::Collection::SingleIterator:
				assertrx_dbg(false);
				break;
			case SingleSelectKeyResult::Collection::FlatIdSet:
				if (it.flatIds_.u.rev.it != it.flatIds_.u.rev.end) {
					it.flatIds_.u.rev.it = GallopLowerBound(it.flatIds_.u.rev.it, it.flatIds_.u.rev.end, bound);
					if (it.flatIds_.u.rev.it != it.flatIds_.u.rev.end) {
						value = *it.flatIds_.u.rev.it;
						return true;
					}
				}
				break;
			case SingleSelectKeyResult::Collection::TreeIdSet:
				if (it.treeIds_.u.rev.it != it.treeIds_.u.rev.end) {
					if (*it.treeIds_.u.rev.it >= bound) {
						const auto lower = it.treeIds_.ptr->lower_bound(bound);
						it.treeIds_.u.rev.it =
							(lower == it.treeIds_.ptr->begin()) ? it.treeIds_.u.rev.end : std::make_reverse_iterator(lower);
					}
					if (it.treeIds_.u.rev.it != it.treeIds_.u.rev.end) {
						value = *it.treeIds_.u.rev.it;
						return true;
					}
				}
				break;
			case SingleSelectKeyResult::Collection::Range:
				if (it.range_.u.rev.it != it.range_.u.rev.end) {
					it.range_.u.rev.it = std::max(it.range_.u.rev.end, std::min(it.range_.u.rev.it, bound.Decr()));
					if (it.range_.u.rev.it != it.range_.u.rev.end) {
						value = it.range_.u.rev.it;
						return true;
					}
				}
				break;
		}
		return false;
	}

	void buildHeap() {
		assertrx_dbg(GetType() == Type::Forward || GetType() == Type::Reverse);
		if (size() < kMinSetsForHeapSort) {
			return;
		}
		heap_.reserve(size());
		auto it = begin();
		const auto endIt = end();
		for (unsigned pos = 0; it != endIt; ++it, ++pos) {
			IdType value;
			if (isReverse_ ? advanceRev(*it, IdType::Max(), value) : advanceFwd(*it, IdType::Min(), value)) {
				heap_.push_back(HeapEntry{value, pos});
			}
		}
		if (isReverse_) {
			std::make_heap(heap_.begin(), heap_.end(), RevHeapLess{});
		} else {
			std::make_heap(heap_.begin(), heap_.end(), FwdHeapGreater{});
		}
	}

	/// Iterates to a next item of result.
	/// @param minHint - rowId value to start from.
	/// @return true if operation succeed.
	RX_ALWAYS_INLINE bool nextImpl(IdType minHint) noexcept {
		switch (type_) {
			case Type::Forward:
				return nextFwd(minHint);
			case Type::Reverse:
				return nextRev(minHint);
			case Type::SingleRange:
				return nextFwdSingleRange(minHint);
			case Type::SingleIdset:
			case Type::SingleIdSetWithDeferedSort:
				return nextFwdSingleIdset(minHint);
			case Type::RevSingleRange:
				return nextRevSingleRange(minHint);
			case Type::RevSingleIdset:
			case Type::RevSingleIdSetWithDeferedSort:
				return nextRevSingleIdset(minHint);
			case Type::Unsorted:
				return nextUnsorted();
			case Type::UnbuiltSortOrdersIndex:
				return nextUnbuiltSortOrders();
			case Type::None:
			default:
				assertrx_dbg(false);
				return false;
		}
	}

	// Iterates to a next item of result
	// depending on iterator type starting
	// from minHint which is the least rowId.
	// Generic next implementation
	bool nextFwd(IdType minHint) noexcept {
		if (!heap_.empty()) {
			return nextFwdHeap(minHint);
		}
		const auto lastVal = (minHint > lastVal_) ? minHint.Decr() : lastVal_;
		IdType minVal = IdType::Max();
		const auto beg = begin();
		for (auto it = beg, endIt = end(); it != endIt; ++it) {
			if (IdType value; advanceFwd(*it, lastVal, value) && value < minVal) {
				minVal = value;
				lastPos_ = it - beg;
			}
		}
		lastVal_ = minVal;
		return lastVal_ != IdType::Max();
	}
	bool nextRev(IdType maxHint) noexcept {
		if (!heap_.empty()) {
			return nextRevHeap(maxHint);
		}
		const auto lastVal = (maxHint < lastVal_) ? maxHint.Incr() : lastVal_;

		IdType maxVal = IdType::Min();
		const auto beg = begin();
		for (auto it = beg, endIt = end(); it != endIt; ++it) {
			if (IdType value; advanceRev(*it, lastVal, value) && value > maxVal) {
				maxVal = value;
				lastPos_ = it - beg;
			}
		}
		lastVal_ = maxVal;
		return lastVal_ != IdType::Min();
	}
	bool nextFwdHeap(IdType minHint) noexcept {
		const auto bound = (minHint > lastVal_) ? minHint.Decr() : lastVal_;
		while (!heap_.empty()) {
			auto& root = heap_.front();
			if (root.value > bound) {
				lastVal_ = root.value;
				lastPos_ = root.pos;
				return true;
			}
			IdType value;
			if (advanceFwd(operator[](root.pos), bound, value)) {
				root.value = value;
				heapifyRoot<HeapEntry, FwdHeapGreater>(heap_);
			} else {
				removeHeapRoot<FwdHeapGreater>(heap_);
			}
		}
		lastVal_ = IdType::Max();
		return false;
	}
	bool nextRevHeap(IdType maxHint) noexcept {
		const auto bound = (maxHint < lastVal_) ? maxHint.Incr() : lastVal_;
		while (!heap_.empty()) {
			auto& root = heap_.front();
			if (root.value < bound) {
				lastVal_ = root.value;
				lastPos_ = root.pos;
				return true;
			}
			IdType value;
			if (advanceRev(operator[](root.pos), bound, value)) {
				root.value = value;
				heapifyRoot<HeapEntry, RevHeapLess>(heap_);
			} else {
				removeHeapRoot<RevHeapLess>(heap_);
			}
		}
		lastVal_ = IdType::Min();
		return false;
	}
	// Single range next implementation
	bool nextFwdSingleRange(IdType minHint) noexcept {
		if (minHint > lastVal_) {
			lastVal_ = minHint.Decr();
		}

		const auto begIt = begin();
		assertrx_dbg(begIt->direction_ == SingleSelectKeyResult::Direction::Forward);
		assertrx_dbg(begIt->collectionType_ == SingleSelectKeyResult::Collection::Range);
		if (lastVal_ < begIt->range_.u.fwd.begin) {
			lastVal_ = begIt->range_.u.fwd.begin.Decr();
		}

		lastVal_ = (lastVal_ < begIt->range_.u.fwd.end) ? lastVal_.Incr() : begIt->range_.u.fwd.end;
		if (lastVal_ == begIt->range_.u.fwd.end) {
			lastVal_ = IdType::Max();
		}
		return lastVal_ != IdType::Max();
	}
	// Single idset next implementation
	bool nextFwdSingleIdset(IdType minHint) noexcept {
		const auto lastVal = (minHint > lastVal_) ? minHint.Decr() : lastVal_;

		auto it = begin();
		assertrx_dbg(it->direction_ == SingleSelectKeyResult::Direction::Forward);
		if (it->collectionType_ == SingleSelectKeyResult::Collection::TreeIdSet) {
			if (it->treeIds_.u.fwd.it != it->treeIds_.u.fwd.end && *it->treeIds_.u.fwd.it <= lastVal) {
				it->treeIds_.u.fwd.it = it->treeIds_.ptr->upper_bound(lastVal);
			}
			lastVal_ = (it->treeIds_.u.fwd.it != it->treeIds_.u.fwd.end) ? *it->treeIds_.u.fwd.it : IdType::Max();
		} else {
			assertrx_dbg(it->collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
			it->flatIds_.u.fwd.it = GallopUpperBound(it->flatIds_.u.fwd.it, it->flatIds_.u.fwd.end, lastVal);
			lastVal_ = (it->flatIds_.u.fwd.it != it->flatIds_.u.fwd.end) ? *it->flatIds_.u.fwd.it : IdType::Max();
		}
		return lastVal_ != IdType::Max();
	}
	bool nextRevSingleRange(IdType maxHint) noexcept {
		if (maxHint < lastVal_) {
			lastVal_ = maxHint.Incr();
		}

		const auto begIt = begin();
		assertrx_dbg(begIt->direction_ == SingleSelectKeyResult::Direction::Reverse);
		assertrx_dbg(begIt->collectionType_ == SingleSelectKeyResult::Collection::Range);
		if (lastVal_ > begIt->range_.u.rev.begin) {
			lastVal_ = begIt->range_.u.rev.begin.Incr();
		}

		lastVal_ = (lastVal_ > begIt->range_.u.rev.end) ? lastVal_.Decr() : begIt->range_.u.rev.end;
		if (lastVal_ == begIt->range_.u.rev.end) {
			lastVal_ = IdType::Min();
		}
		return lastVal_ != IdType::Min();
	}
	bool nextRevSingleIdset(IdType maxHint) noexcept {
		const auto lastVal = (maxHint < lastVal_) ? maxHint.Incr() : lastVal_;
		auto it = begin();

		assertrx_dbg(it->direction_ == SingleSelectKeyResult::Direction::Reverse);
		if (it->collectionType_ == SingleSelectKeyResult::Collection::TreeIdSet) {
			const auto lower = it->treeIds_.ptr->lower_bound(lastVal);
			if (lower == it->treeIds_.ptr->begin()) {
				it->treeIds_.u.rev.it = it->treeIds_.u.rev.end;
				lastVal_ = IdType::Min();
			} else {
				it->treeIds_.u.rev.it = std::make_reverse_iterator(lower);
				lastVal_ = *it->treeIds_.u.rev.it;
			}
		} else {
			assertrx_dbg(it->collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
			it->flatIds_.u.rev.it = GallopLowerBound(it->flatIds_.u.rev.it, it->flatIds_.u.rev.end, lastVal);
			lastVal_ = (it->flatIds_.u.rev.it != it->flatIds_.u.rev.end) ? *it->flatIds_.u.rev.it : IdType::Min();
		}

		return lastVal_ != IdType::Min();
	}
	// B-tree forward iterator next implementation
	bool nextUnbuiltSortOrders() noexcept {
		assertrx_dbg(begin()->collectionType_ == SingleSelectKeyResult::Collection::SingleIterator);
		auto& iter = *begin()->idxFwdIter_;
		const bool res = iter.Next();
		lastVal_ = iter.Value();
		return res;
	}
	// Unsorted next implementation
	bool nextUnsorted() noexcept {
		if (lastPos_ == size()) {
			return false;
		}
		auto& curRes = operator[](lastPos_);
		assertrx_dbg(curRes.direction_ == SingleSelectKeyResult::Direction::Forward);
		assertrx_dbg(curRes.collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
		if (curRes.flatIds_.u.fwd.it == curRes.flatIds_.u.fwd.end) {
			++lastPos_;
			while (lastPos_ < size()) {
				assertrx_dbg(operator[](lastPos_).direction_ == SingleSelectKeyResult::Direction::Forward);
				assertrx_dbg(operator[](lastPos_).collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
				if (operator[](lastPos_).flatIds_.u.fwd.it != operator[](lastPos_).flatIds_.u.fwd.end) {
					lastVal_ = *(operator[](lastPos_).flatIds_.u.fwd.it++);
					return true;
				}
				++lastPos_;
			}
			return false;
		}
		lastVal_ = *(operator[](lastPos_).flatIds_.u.fwd.it++);
		return true;
	}

	/// Performs ID sets merge and sort in case, when this sort was deferred earlier and still effective with current maxIterations value
	bool applyDeferedSort(int maxIterations) {
		if (deferedExplicitSort && maxIterations > 0 && !distinct_) {
			const auto idsCount = GetMaxIterations();
			if (IsGenericSortRecommended(size(), idsCount, size_t(maxIterations))) {
				[[maybe_unused]] auto merged =
					MergeIdsets(SelectKeyResult::MergeOptions{.genericSort = true, .shrinkResult = false}, idsCount);
				return true;
			}
		}
		return false;
	}

	int totalCalls_ = 0;
	IdType lastVal_ = IdType::Min();
	Type type_ = Type::None;
	bool isUnsorted_ = false;
	ForcedFirst forcedFirst_ = ForcedFirst_False;
	bool isNotOperation_ = false;
	reindexer::IsDistinct distinct_ = IsDistinct_False;
	bool isReverse_ = false;
	size_t lastPos_ = 0;
	int matchedCount_ = 0;
	int indexNo_ = IndexValueType::NotSet;
	HeapT heap_;
};

}  // namespace reindexer
