#pragma once

#include "btreeindexiteratorimpl.h"
#include "core/id_type.h"
#include "core/idset/idset.h"
#include "core/index/indexiterator.h"

namespace reindexer {

template <class IndexMap>
class [[nodiscard]] BtreeIndexIterator final : public IndexIterator {
public:
	BtreeIndexIterator(const IndexMap& idxMap, const IdSet& empty_ids,
					   size_t maxIterationsUpperBound = std::numeric_limits<size_t>::max()) noexcept
		: first_(idxMap.begin()), last_(idxMap.end()), nullValues_(&empty_ids), maxIterationsUpperBound_(maxIterationsUpperBound) {}
	BtreeIndexIterator(const typename IndexMap::iterator& first, const typename IndexMap::iterator& last,
					   size_t maxIterationsUpperBound = std::numeric_limits<size_t>::max()) noexcept
		: first_(first), last_(last), nullValues_(nullptr), maxIterationsUpperBound_(maxIterationsUpperBound) {}

	void Start(bool reverse) final override {
		if (reverse) {
			impl_ = createReverseIterator();
		} else {
			impl_ = createForwardIterator();
		}
		std::visit([](auto& impl) { impl.Start(); }, impl_);
	}

	std::pair<bool, IdType> Next() noexcept final override {
		if (auto* it = std::get_if<ForwardIteratorImpl>(&impl_); it) {
			return it->Next();
		}
		if (auto* it = std::get_if<ReverseIteratorImpl>(&impl_); it) {
			return it->Next();
		}
		std::abort();
	}

	void ExcludeLastSet() noexcept override final {
		if (auto* it = std::get_if<ForwardIteratorImpl>(&impl_); it) {
			it->SkipKey();
			return;
		}
		if (auto* it = std::get_if<ReverseIteratorImpl>(&impl_); it) {
			it->SkipKey();
			return;
		}
		std::abort();
	}

	MaxIterationsEstimate ProbeMaxIterations(size_t limitIters) noexcept override final {
		if (!cachedIters_.Satisfies(limitIters)) {
			auto [iters, fullyScanned] = createReverseIterator().MaxIterations(limitIters);
			if (fullyScanned || !cachedIters_.initialized || iters > cachedIters_.value) {
				cachedIters_ = CachedIters{iters, fullyScanned, true};
			}
		}
		return cachedIters_.fullyScanned ? MaxIterationsEstimate::Exact(cachedIters_.value)
										 : MaxIterationsEstimate::AtLeast(cachedIters_.value);
	}

	MaxIterationsEstimate GetPlanningEstimate() const noexcept override final {
		return cachedIters_.fullyScanned ? MaxIterationsEstimate::Exact(cachedIters_.value)
										 : MaxIterationsEstimate::UpperBound(maxIterationsUpperBound_);
	}

	void SetMaxIterations(size_t iters) noexcept final {
		cachedIters_ = CachedIters{iters, true, true};
		maxIterationsUpperBound_ = iters;
	}

private:
	auto createForwardIterator() {
		if (nullValues_) {
			return index::iterators::BtreeIndexForwardIteratorImpl<IndexMap>(first_, last_, *nullValues_);
		}
		return index::iterators::BtreeIndexForwardIteratorImpl<IndexMap>(first_, last_);
	}

	auto createReverseIterator() {
		if (nullValues_) {
			return index::iterators::BtreeIndexReverseIteratorImpl<IndexMap>(first_, last_, *nullValues_);
		}
		return index::iterators::BtreeIndexReverseIteratorImpl<IndexMap>(first_, last_);
	}

private:
	using ForwardIteratorImpl = index::iterators::BtreeIndexForwardIteratorImpl<IndexMap>;
	using ReverseIteratorImpl = index::iterators::BtreeIndexReverseIteratorImpl<IndexMap>;
	using BtreeIndexIteratorImpl = std::variant<ForwardIteratorImpl, ReverseIteratorImpl>;
	BtreeIndexIteratorImpl impl_;

	const typename IndexMap::const_iterator first_;
	const typename IndexMap::const_iterator last_;

	const IdSet* nullValues_;
	size_t maxIterationsUpperBound_;

	struct [[nodiscard]] CachedIters {
		bool Satisfies(size_t limitIters) const noexcept { return fullyScanned || (initialized && limitIters <= value); }

		size_t value = 0;
		bool fullyScanned = false;
		bool initialized = false;
	} cachedIters_;
};

}  // namespace reindexer
