#pragma once

#include <memory>

#include "core/id_type.h"
#include "core/idset/idset.h"
#include "core/index/indexiterator.h"
#include "core/index/keyentry.h"
#include "core/nsselecter/comparator/comparator_indexed.h"
#include "core/nsselecter/comparator/comparator_not_indexed.h"
#include "estl/heapify.h"

namespace reindexer {

/// Stores result of selecting data for only 1 value of
/// a certain key. i.e. for condition "A>=10 && A<20" it
/// contains only 1 IdSet for one of the following keys:
/// 10, 11, 12, 13, ... 19 (For example all rowIds for
/// the value '10').
class [[nodiscard]] SingleSelectKeyResult {
	friend class SelectIterator;
	friend class SelectKeyResult;

public:
	SingleSelectKeyResult() noexcept {}
	~SingleSelectKeyResult() { destroyState(); }
	explicit SingleSelectKeyResult(IndexIterator::Ptr&& indexForwardIter) noexcept
		: idxFwdIter_{std::move(indexForwardIter)}, collectionType_{Collection::SingleIterator} {
		assertrx(idxFwdIter_ != nullptr);
	}
	template <typename KeyEntryT>
	explicit SingleSelectKeyResult(const KeyEntryT& ids, SortType sortId) noexcept
		requires(concepts::KeyEntryWithSortedIDs<KeyEntryT>)
	{
		auto set = ids.Unsorted().BTree();
		if (!set) {
			assertrx_dbg(ids.Unsorted().IsCommitted());
			collectionType_ = Collection::FlatIdSet;
			new (&flatIds_) FlatIdSet{.storage{}, .view = ids.Sorted(sortId), .u{}};
		} else {
			assertrx(!sortId);
			assertrx_dbg(!ids.Unsorted().IsCommitted());
			collectionType_ = Collection::TreeIdSet;
			new (&treeIds_) TreeIdSet{.ptr = set, .u{}};
		}
	}
	template <typename KeyEntryT>
	explicit SingleSelectKeyResult(const KeyEntryT& ids, const SortedIDsCtx& sortCtx) noexcept
		requires(concepts::KeyEntryWithSortedIDs<KeyEntryT>)
		: SingleSelectKeyResult(ids, sortCtx.SortID()) {}
	template <typename KeyEntryT>
	explicit SingleSelectKeyResult(const KeyEntryT& ids, const SortedIDsCtx& sortCtx) noexcept
		requires(!concepts::KeyEntryWithSortedIDs<KeyEntryT>)
		: flatIds_{FlatIdSet{.storage{}, .view = ids.Sorted(sortCtx), .u{}}}, collectionType_{Collection::FlatIdSet} {
		static_assert(KeyEntryT::IdSetType::IsCommitted());
	}
	explicit SingleSelectKeyResult(IdSetPlain::Ptr&& ids) noexcept
		: flatIds_{FlatIdSet{.storage = std::move(ids), .view{}, .u{}}}, collectionType_{Collection::FlatIdSet} {
		assertrx_dbg(flatIds_.storage);
		flatIds_.view = *flatIds_.storage;
	}
	explicit SingleSelectKeyResult(const IdSetPlain& ids) noexcept
		: flatIds_{FlatIdSet{.storage{}, .view = ids, .u{}}}, collectionType_{Collection::FlatIdSet} {}
	explicit SingleSelectKeyResult(IdType rBegin, IdType rEnd) noexcept
		: range_{Range{.values = std::make_pair(rBegin, rEnd), .u{}}}, collectionType_{Collection::Range} {
		assertrx_dbg(rBegin <= rEnd);
	}
	SingleSelectKeyResult(const SingleSelectKeyResult& other) noexcept { copyStateFrom(other); }
	SingleSelectKeyResult(SingleSelectKeyResult&& other) noexcept { moveStateFrom(std::move(other)); }
	SingleSelectKeyResult& operator=(const SingleSelectKeyResult& other) noexcept {
		if (&other != this) {
			destroyState();
			copyStateFrom(other);
		}
		return *this;
	}
	SingleSelectKeyResult& operator=(SingleSelectKeyResult&& other) noexcept {
		if (this != &other) {
			destroyState();
			moveStateFrom(std::move(other));
		}
		return *this;
	}

	size_t GetMaxIterations(size_t limitIters) const noexcept {
		switch (collectionType_) {
			case Collection::FlatIdSet:
				return flatIds_.view.size();
			case Collection::TreeIdSet:
				return treeIds_.ptr->size();
			case Collection::Range:
				assertrx_dbg(range_.values.second.ToNumber() >= range_.values.first.ToNumber());
				return range_.values.second.ToNumber() - range_.values.first.ToNumber();
			case Collection::SingleIterator: {
				const auto iters = idxFwdIter_->GetMaxIterations(limitIters);
				return (iters == std::numeric_limits<size_t>::max()) ? limitIters : iters;
			}
			case Collection::NotSet:
			default:
				assertrx_dbg(false);
				return 0;
		}
	}

	void Start(bool reverse) {
		switch (collectionType_) {
			case Collection::NotSet:
				throw Error(errLogic, "SingleSelectKeyResult::Start: collection type is not set");
			case Collection::FlatIdSet:
				resetUnionDirection(direction_, flatIds_);
				if (reverse) {
					const auto begin = flatIds_.view.rbegin();
					const auto end = flatIds_.view.rend();
					std::construct_at(&flatIds_.u.rev, begin, end, begin);
				} else {
					const auto begin = flatIds_.view.begin();
					const auto end = flatIds_.view.end();
					std::construct_at(&flatIds_.u.fwd, begin, end, begin);
				}
				break;
			case Collection::TreeIdSet:
				resetUnionDirection(direction_, treeIds_);
				if (reverse) {
					const auto begin = treeIds_.ptr->rbegin();
					const auto end = treeIds_.ptr->rend();
					std::construct_at(&treeIds_.u.rev, begin, end, begin);
				} else {
					const auto begin = treeIds_.ptr->begin();
					const auto end = treeIds_.ptr->end();
					std::construct_at(&treeIds_.u.fwd, begin, end, begin);
				}
				break;
			case Collection::Range:
				resetUnionDirection(direction_, range_);
				if (reverse) {
					const auto begin = range_.values.second.Decr();
					const auto end = range_.values.first.Decr();
					std::construct_at(&range_.u.rev, begin, end, begin);
				} else {
					const auto begin = range_.values.first;
					const auto end = range_.values.second;
					std::construct_at(&range_.u.fwd, begin, end, begin);
				}
				break;
			case Collection::SingleIterator: {
				idxFwdIter_->Start(reverse);
				break;
			}
		}
		direction_ = reverse ? Direction::Reverse : Direction::Forward;
	}

	bool OwnsFlatIDSet() const noexcept { return collectionType_ == Collection::FlatIdSet && flatIds_.storage; }
	IdSetCRef TryGetFlatIDSet() const noexcept { return collectionType_ == Collection::FlatIdSet ? flatIds_.view : IdSetCRef(); }

protected:
	enum class [[nodiscard]] Collection : uint8_t { NotSet, FlatIdSet, TreeIdSet, Range, SingleIterator };
	enum class [[nodiscard]] Direction : uint8_t { NotSet, Forward, Reverse };

	template <typename IterT>
	struct [[nodiscard]] Iters {
		IterT begin;
		IterT end;
		IterT it;
	};
	template <typename FwdIterT, typename RevIterT>
	union [[nodiscard]] ItersUnion {
		struct [[nodiscard]] RevT {};

		ItersUnion() {
			// Does not set active member
		}
		ItersUnion(Iters<FwdIterT> f) : fwd{std::move(f)} {}
		ItersUnion(Iters<RevIterT> r)
			requires(!std::is_same_v<Iters<FwdIterT>, Iters<RevIterT>>)
			: rev{std::move(r)} {}
		ItersUnion(Iters<RevIterT> r, RevT) : rev{std::move(r)} {}

		Iters<FwdIterT> fwd;
		Iters<RevIterT> rev;
	};

	struct [[nodiscard]] FlatIdSet {
		using UnionT = ItersUnion<IdSetCRef::iterator, IdSetCRef::reverse_iterator>;

		FlatIdSet Clone(Direction direction) const noexcept {
			switch (direction) {
				case Direction::Forward:
					return FlatIdSet{.storage = storage, .view = view, .u = UnionT{u.fwd}};
				case Direction::Reverse:
					return FlatIdSet{.storage = storage, .view = view, .u = UnionT{u.rev}};
				case Direction::NotSet:
				default:
					return FlatIdSet{.storage = storage, .view = view, .u = UnionT{}};
			}
		}

		FlatIdSet Extract(Direction direction) && noexcept {
			switch (direction) {
				case Direction::Forward:
					return FlatIdSet{.storage = std::move(storage), .view = view, .u = UnionT{std::move(u.fwd)}};
				case Direction::Reverse:
					return FlatIdSet{.storage = std::move(storage), .view = view, .u = UnionT{std::move(u.rev)}};
				case Direction::NotSet:
				default:
					return FlatIdSet{.storage = std::move(storage), .view = view, .u = UnionT{}};
			}
		}

		IdSetPlain::Ptr storage;
		IdSetCRef view;
		UnionT u;
	};

	struct [[nodiscard]] TreeIdSet {
		using UnionT = ItersUnion<base_idsetset::const_iterator, base_idsetset::const_reverse_iterator>;

		TreeIdSet Clone(Direction direction) const noexcept {
			switch (direction) {
				case Direction::Forward:
					return TreeIdSet{.ptr = ptr, .u = UnionT{u.fwd}};
				case Direction::Reverse:
					return TreeIdSet{.ptr = ptr, .u = UnionT{u.rev}};
				case Direction::NotSet:
				default:
					return TreeIdSet{.ptr = ptr, .u = UnionT{}};
			}
		}

		TreeIdSet Extract(Direction direction) && noexcept {
			switch (direction) {
				case Direction::Forward:
					return TreeIdSet{.ptr = ptr, .u = UnionT{std::move(u.fwd)}};
				case Direction::Reverse:
					return TreeIdSet{.ptr = ptr, .u = UnionT{std::move(u.rev)}};
				case Direction::NotSet:
				default:
					return TreeIdSet{.ptr = ptr, .u = UnionT{}};
			}
		}

		const base_idsetset* ptr;
		UnionT u;
	};

	struct [[nodiscard]] Range {
		using UnionT = ItersUnion<IdType, IdType>;

		Range Clone(Direction direction) const noexcept {
			switch (direction) {
				case Direction::Forward:
					return Range{.values = values, .u = UnionT{u.fwd}};
				case Direction::Reverse:
					return Range{.values = values, .u = UnionT{u.rev, UnionT::RevT{}}};
				case Direction::NotSet:
				default:
					return Range{.values = values, .u = UnionT{}};
			}
		}

		Range Extract(Direction direction) && noexcept {
			switch (direction) {
				case Direction::Forward:
					return Range{.values = values, .u = UnionT{std::move(u.fwd)}};
				case Direction::Reverse:
					return Range{.values = values, .u = UnionT{std::move(u.rev), UnionT::RevT{}}};
				case Direction::NotSet:
				default:
					return Range{.values = values, .u = UnionT{}};
			}
		}

		std::pair<IdType, IdType> values;
		UnionT u;
	};

	void copyStateFrom(const SingleSelectKeyResult& other) noexcept {
		collectionType_ = other.collectionType_;
		direction_ = other.direction_;
		switch (collectionType_) {
			case Collection::NotSet:
				break;
			case Collection::FlatIdSet:
				new (&flatIds_) FlatIdSet(other.flatIds_.Clone(direction_));
				break;
			case Collection::TreeIdSet:
				new (&treeIds_) TreeIdSet(other.treeIds_.Clone(direction_));
				break;
			case Collection::Range:
				new (&range_) Range(other.range_.Clone(direction_));
				break;
			case Collection::SingleIterator:
				new (&idxFwdIter_) IndexIterator::Ptr(other.idxFwdIter_);
				break;
		}
	}

	void moveStateFrom(SingleSelectKeyResult&& other) noexcept {
		collectionType_ = other.collectionType_;
		direction_ = other.direction_;
		switch (collectionType_) {
			case Collection::NotSet:
				break;
			case Collection::FlatIdSet:
				new (&flatIds_) FlatIdSet(std::move(other.flatIds_).Extract(direction_));
				break;
			case Collection::TreeIdSet:
				new (&treeIds_) TreeIdSet(std::move(other.treeIds_).Extract(direction_));
				break;
			case Collection::Range:
				new (&range_) Range(std::move(other.range_).Extract(direction_));
				break;
			case Collection::SingleIterator:
				new (&idxFwdIter_) IndexIterator::Ptr(std::move(other.idxFwdIter_));
				break;
		}
		other.destroyState();
		other.collectionType_ = Collection::NotSet;
		other.direction_ = Direction::NotSet;
	}

	template <typename CollectionT>
	static void resetUnionDirection(Direction direction, CollectionT& coll) noexcept {
		switch (direction) {
			case Direction::NotSet:
				break;
			case Direction::Forward:
				std::destroy_at(&coll.u.fwd);
				break;
			case Direction::Reverse:
				std::destroy_at(&coll.u.rev);
				break;
		}
	}

	void destroyState() noexcept {
		switch (collectionType_) {
			case Collection::NotSet:
				break;
			case Collection::FlatIdSet:
				resetUnionDirection(direction_, flatIds_);
				std::destroy_at(&flatIds_);
				break;
			case Collection::TreeIdSet:
				resetUnionDirection(direction_, treeIds_);
				std::destroy_at(&treeIds_);
				break;
			case Collection::Range:
				resetUnionDirection(direction_, range_);
				std::destroy_at(&range_);
				break;
			case Collection::SingleIterator:
				std::destroy_at(&idxFwdIter_);
				break;
		}
		collectionType_ = Collection::NotSet;
		direction_ = Direction::NotSet;
	}

	union {
		FlatIdSet flatIds_;
		TreeIdSet treeIds_;
		Range range_;
		IndexIterator::Ptr idxFwdIter_;
	};
	Collection collectionType_ = Collection::NotSet;
	Direction direction_ = Direction::NotSet;
};

/// Stores results of selecting data for 1 certain key,
/// i.e. for condition "A>=10 && A<20" there will be
/// 10 SingleSelectKeyResult objects (for each of the
/// following keys: 10, 11, 12, 13, ... 19).
class [[nodiscard]] SelectKeyResult : public h_vector<SingleSelectKeyResult, 1> {
public:
	constexpr static size_t kMinSetsForHeapSort = 16;
	constexpr static size_t kSelectionSortIdsCount = 500;
	constexpr static size_t kMinSetsForGenericSort = 30;

	struct [[nodiscard]] MergeOptions {
		bool genericSort;
		bool shrinkResult;
	};

	bool deferedExplicitSort = false;
	bool cached = false;

	static size_t GetMergeSortCost(size_t idsCount, size_t idsetsCount) noexcept { return idsCount * idsetsCount; }
	static size_t GetGenericSortCost(size_t idsCount) noexcept { return idsCount * log2(idsCount) + 2 * idsCount; }
	static bool IsGenericSortRecommended(size_t idsetsCount, size_t idsCount, size_t maxIterations) noexcept {
		return idsetsCount >= kMinSetsForGenericSort && idsCount &&
			   GetGenericSortCost(idsCount) < GetMergeSortCost(maxIterations, idsetsCount);
	}
	static size_t CostWithDefferedSort(size_t idsetsCount, size_t idsCount, size_t maxIterations) noexcept {
		const auto mrgSortCost = GetMergeSortCost(maxIterations, idsetsCount);
		if (idsetsCount < kMinSetsForGenericSort || !idsCount) {
			return mrgSortCost;
		}
		const auto genSortCost = GetGenericSortCost(idsCount);
		return std::min(genSortCost, mrgSortCost);
	}

	/// Returns total amount of rowIds in all
	/// the SingleSelectKeyResult objects, i.e.
	/// maximum amount of possible iterations.
	/// @return amount of loops.
	size_t GetMaxIterations(size_t limitIters = std::numeric_limits<size_t>::max()) const noexcept {
		size_t cnt = 0;
		for (const SingleSelectKeyResult& r : *this) {
			cnt += r.GetMaxIterations(limitIters);
			if (cnt > limitIters) {
				return limitIters;
			}
		}
		return cnt;
	}

	/// Represents data as one sorted set.
	/// Creates 1 set from all the inner
	/// SingleSelectKeyResult objects. Such
	/// representation makes further work with
	/// the object much easier.
	/// @param opts - merge customization options
	/// @return Pointer to a sorted IdSet object made
	/// from all the SingleSelectKeyResult inner objects.
	IdSetPlain::Ptr MergeIdsets(MergeOptions&& opts, size_t idsCount) {
		IdSetPlain::Ptr mergedIds;
		if (opts.genericSort) {
			mergedIds = mergeGenericSort(idsCount);
		} else if (idsCount < kSelectionSortIdsCount || size() < kMinSetsForHeapSort) {
			mergedIds = mergeSelectionSort(idsCount);
		} else {
			mergedIds = mergeHeapSort(idsCount);
		}
		if (opts.shrinkResult) {
			mergedIds->shrink_to_fit();
		}
		clear();
		deferedExplicitSort = false;
		emplace_back(IdSetPlain::Ptr(mergedIds));
		return mergedIds;
	}

	void MarkCached() noexcept { cached = true; }
	bool IsCached() const noexcept { return cached; }

private:
	IdSetPlain::Ptr mergeGenericSort(size_t idsCount) {
		base_idset ids;
		size_t actualSize = 0;
		ids.resize(idsCount);
		auto rit = ids.begin();
		for (auto it = begin(), endIt = end(); it != endIt; ++it) {
			switch (it->collectionType_) {
				case SingleSelectKeyResult::Collection::NotSet:
				case SingleSelectKeyResult::Collection::Range:
				case SingleSelectKeyResult::Collection::SingleIterator:
					throw Error(errLogic, "Select key result must be flat IdSet or tree IdSet for 'generic sort mode'");
				case SingleSelectKeyResult::Collection::FlatIdSet: {
					const auto sz = it->flatIds_.view.size();
					actualSize += sz;
					std::ranges::copy(it->flatIds_.view, rit);
					rit += sz;
					break;
				}
				case SingleSelectKeyResult::Collection::TreeIdSet: {
					const auto sz = it->treeIds_.ptr->size();
					actualSize += sz;
					std::ranges::copy(*it->treeIds_.ptr, rit);
					rit += sz;
					break;
				}
			}
		}
		assertrx(idsCount == actualSize);
		return IdSetPlain::BuildFromUnsorted(std::move(ids));
	}

	IdSetPlain::Ptr mergeSelectionSort(size_t idsCount) {
		auto mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>();
		mergedIds->reserve(idsCount);

		auto firstSetIt = std::partition(begin(), end(), [](const SingleSelectKeyResult& v) noexcept {
			return v.collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet;
		});
		const auto vecsCnt = firstSetIt - begin();

		h_vector<value_type*, 64> ptrsVec;
		ptrsVec.reserve(size());

		for (auto& v : *this) {
			switch (v.collectionType_) {
				case SingleSelectKeyResult::Collection::NotSet:
				case SingleSelectKeyResult::Collection::Range:
				case SingleSelectKeyResult::Collection::SingleIterator:
					throw Error(errLogic, "Select key result must be flat IdSet or tree IdSet for 'merge selection sort mode'");
				case SingleSelectKeyResult::Collection::FlatIdSet:
				case SingleSelectKeyResult::Collection::TreeIdSet: {
					constexpr bool reverse = false;
					v.Start(reverse);
					ptrsVec.emplace_back(&v);
					break;
				}
			}
		}
		std::span<value_type*> vecSpan(ptrsVec.data(), vecsCnt);
		std::span<value_type*> setSpan(ptrsVec.data() + vecsCnt, size() - vecsCnt);

		IdType min = IdType::Min();
		for (;;) {
			IdType curMin = IdType::Max();
			for (auto vsIt = vecSpan.begin(), vsItEnd = vecSpan.end(); vsIt != vsItEnd;) {
				auto& itvec = (*vsIt)->flatIds_.u.fwd.it;
				const auto vecend = (*vsIt)->flatIds_.u.fwd.end;
				for (;; ++itvec) {
					if (itvec == vecend) {
						std::swap(*vsIt, vecSpan.back());
						vecSpan = std::span<value_type*>(vecSpan.data(), vecSpan.size() - 1);
						--vsItEnd;
						break;
					}
					const auto val = *itvec;
					if (val > min) {
						if (val < curMin) {
							curMin = val;
						}
						++vsIt;
						break;
					}
				}
			}
			for (auto ssIt = setSpan.begin(), ssItEnd = setSpan.end(); ssIt != ssItEnd;) {
				auto& itset = (*ssIt)->treeIds_.u.fwd.it;
				const auto setend = (*ssIt)->treeIds_.u.fwd.end;
				for (;; ++itset) {
					if (itset == setend) {
						std::swap(*ssIt, setSpan.back());
						setSpan = std::span<value_type*>(setSpan.data(), setSpan.size() - 1);
						--ssItEnd;
						break;
					}
					const auto val = *itset;
					if (val > min) {
						if (val < curMin) {
							curMin = val;
						}
						++ssIt;
						break;
					}
				}
			}
			if (curMin == IdType::Max()) {
				break;
			}
			min = curMin;
			mergedIds->AddUnordered(min);
		}
		return mergedIds;
	}

	IdSetPlain::Ptr mergeHeapSort(size_t idsCount) {
		auto mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>();
		mergedIds->reserve(idsCount);

		struct [[nodiscard]] IdSetGreater {
			bool operator()(const value_type* l, const value_type* r) noexcept {
				const auto lval = (l->collectionType_ == SingleSelectKeyResult::Collection::TreeIdSet) ? *(l->treeIds_.u.fwd.it)
																									   : *(l->flatIds_.u.fwd.it);
				const auto rval = (r->collectionType_ == SingleSelectKeyResult::Collection::TreeIdSet) ? *(r->treeIds_.u.fwd.it)
																									   : *(r->flatIds_.u.fwd.it);
				return lval > rval;
			}
		};

		h_vector<value_type*, 64> ptrsVec;
		ptrsVec.reserve(size());
		for (auto& v : *this) {
			switch (v.collectionType_) {
				case SingleSelectKeyResult::Collection::NotSet:
				case SingleSelectKeyResult::Collection::Range:
				case SingleSelectKeyResult::Collection::SingleIterator:
					throw Error(errLogic, "Select key result must be flat IdSet or tree IdSet for 'merge heap sort mode'");
				case SingleSelectKeyResult::Collection::FlatIdSet:
				case SingleSelectKeyResult::Collection::TreeIdSet: {
					constexpr bool reverse = false;
					v.Start(reverse);
					ptrsVec.emplace_back(&v);
					break;
				}
			}
		}
		std::span<value_type*> idsetsSpan(ptrsVec.data(), ptrsVec.size());
		std::make_heap(idsetsSpan.begin(), idsetsSpan.end(), IdSetGreater{});
		IdType min = IdType::Min();
		auto handleMinValue = [&mergedIds, &idsetsSpan, &min](auto& it, auto end) {
			auto val = *it;
			if (val > min) {
				mergedIds->AddUnordered(val);
				min = val;
			}
			do {
				if (++it == end) {
					std::swap(idsetsSpan.front(), idsetsSpan.back());
					idsetsSpan = std::span<value_type*>(idsetsSpan.begin(), idsetsSpan.size() - 1);
					return;
				}
			} while (*it <= min);
		};

		while (!idsetsSpan.empty()) {
			auto& minV = *idsetsSpan.front();
			if (minV.collectionType_ == SingleSelectKeyResult::Collection::TreeIdSet) {
				handleMinValue(minV.treeIds_.u.fwd.it, minV.treeIds_.u.fwd.end);
			} else {
				assertrx_dbg(minV.collectionType_ == SingleSelectKeyResult::Collection::FlatIdSet);
				handleMinValue(minV.flatIds_.u.fwd.it, minV.flatIds_.u.fwd.end);
			}
			heapifyRoot<value_type*, IdSetGreater>(idsetsSpan);
		}
		return mergedIds;
	}
};

using SelectKeyResultsVector = h_vector<SelectKeyResult, 1>;

/// Result of selecting data for
/// each key in a query.
class [[nodiscard]] SelectKeyResults
	: public std::variant<SelectKeyResultsVector, ComparatorNotIndexed, ComparatorIndexed<bool>, ComparatorIndexed<int>,
						  ComparatorIndexed<int64_t>, ComparatorIndexed<double>, ComparatorIndexed<key_string>,
						  ComparatorIndexed<PayloadValue>, ComparatorIndexed<Point>, ComparatorIndexed<Uuid>,
						  ComparatorIndexed<FloatVector>> {
	using Base =
		std::variant<SelectKeyResultsVector, ComparatorNotIndexed, ComparatorIndexed<bool>, ComparatorIndexed<int>,
					 ComparatorIndexed<int64_t>, ComparatorIndexed<double>, ComparatorIndexed<key_string>, ComparatorIndexed<PayloadValue>,
					 ComparatorIndexed<Point>, ComparatorIndexed<Uuid>, ComparatorIndexed<FloatVector>>;

public:
	SelectKeyResults() noexcept : Base{SelectKeyResultsVector{}} {}
	// NOLINTNEXTLINE(bugprone-exception-escape) h_vector has default capacity of 1
	SelectKeyResults(SelectKeyResult&& res) noexcept : Base{SelectKeyResultsVector{std::move(res)}} {}
	template <typename T>
	SelectKeyResults(ComparatorIndexed<T>&& comp) noexcept : Base{std::move(comp)} {}
	SelectKeyResults(ComparatorNotIndexed&& comp) noexcept : Base{std::move(comp)} {}
	void Clear() noexcept {
		auto vec = std::get_if<SelectKeyResultsVector>(this);
		assertrx_dbg(vec);
		if (vec) {
			vec->clear();
		};
	}
	void EmplaceBack(SelectKeyResult&& sr) {
		auto vec = std::get_if<SelectKeyResultsVector>(this);
		assertrx_dbg(vec);
		if (vec) {
			vec->emplace_back(std::move(sr));
		};
	}
	[[nodiscard]] bool IsComparator() const noexcept { return !std::holds_alternative<SelectKeyResultsVector>(AsVariant()); }
	SelectKeyResult&& Front() && {
		auto vec = std::get_if<SelectKeyResultsVector>(this);
		if (vec) {
			return std::move(vec->front());
		};
		throw_assert(false);
	}
	const Base& AsVariant() const& noexcept { return *this; }
	Base& AsVariant() & noexcept { return *this; }
	auto AsVariant() const&& = delete;
};

}  // namespace reindexer
