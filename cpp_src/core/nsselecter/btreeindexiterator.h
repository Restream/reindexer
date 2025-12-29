#pragma once

#include <limits.h>
#include "btreeindexiteratorimpl.h"
#include "core/idset/idset.h"
#include "core/index/indexiterator.h"

namespace reindexer {

template <class IndexMap>
class [[nodiscard]] BtreeIndexIterator final : public IndexIterator {
public:
	BtreeIndexIterator(const IndexMap& idxMap, const IdSet& empty_ids) noexcept
		: first_(idxMap.begin()), last_(idxMap.end()), nullValues_(&empty_ids) {}
	BtreeIndexIterator(const typename IndexMap::iterator& first, const typename IndexMap::iterator& last) noexcept
		: first_(first), last_(last), nullValues_(nullptr) {}
	~BtreeIndexIterator() override final = default;

	void Start(bool reverse) final override {
		if (reverse) {
			impl_ = createReverseIterator();
		} else {
			impl_ = createForwardIterator();
		}
		std::visit([](auto& impl) { impl.Start(); }, impl_);
	}

	bool Next() noexcept final override {
		if (auto* it = std::get_if<ForwardIteratorImpl>(&impl_); it) {
			return next(it);
		}
		if (auto* it = std::get_if<ReverseIteratorImpl>(&impl_); it) {
			return next(it);
		}
		std::abort();
	}

	void ExcludeLastSet() noexcept override final {
		if (auto* it = std::get_if<ForwardIteratorImpl>(&impl_); it) {
			it->SkipKey();
		}
		if (auto* it = std::get_if<ReverseIteratorImpl>(&impl_); it) {
			it->SkipKey();
		}
		std::abort();
	}

	size_t GetMaxIterations(size_t limitIters) noexcept override final {
		auto limit = std::min(kMaxBTreeIterations, limitIters);
		if (!cachedIters_.Valid(limit)) {
			auto [iters, fullyScanned] = createReverseIterator().MaxIterations(limit);
			if (iters >= kMaxBTreeIterations && !fullyScanned) {
				cachedIters_ = CachedIters{std::numeric_limits<size_t>::max(), true};
			} else if (fullyScanned || iters > cachedIters_.value || cachedIters_.value == std::numeric_limits<size_t>::max()) {
				cachedIters_ = CachedIters{iters, fullyScanned};
			}
		}

		return std::min(cachedIters_.value, limitIters);
	}

	void SetMaxIterations(size_t iters) noexcept final { cachedIters_ = CachedIters{iters, true}; }

	IdType Value() const noexcept override final { return lastVal_; }

private:
	auto createForwardIterator() {
		lastVal_ = INT_MIN;
		type_ = Forward;
		if (nullValues_) {
			return index::iterators::BtreeIndexForwardIteratorImpl<IndexMap>(first_, last_, *nullValues_);
		} else {
			return index::iterators::BtreeIndexForwardIteratorImpl<IndexMap>(first_, last_);
		}
	}

	auto createReverseIterator() {
		lastVal_ = INT_MAX;
		type_ = Reverse;
		if (nullValues_) {
			return index::iterators::BtreeIndexReverseIteratorImpl<IndexMap>(first_, last_, *nullValues_);
		} else {
			return index::iterators::BtreeIndexReverseIteratorImpl<IndexMap>(first_, last_);
		}
	}

	bool next(auto* it) noexcept {
		auto [hasValue, rowId] = it->Next(lastVal_);
		if (hasValue) {
			lastVal_ = rowId;
		} else {
			lastVal_ = (type_ == Reverse) ? INT_MAX : INT_MIN;
		}
		return hasValue;
	}

private:
	static constexpr size_t kMaxBTreeIterations = 200'000;

	using ForwardIteratorImpl = index::iterators::BtreeIndexForwardIteratorImpl<IndexMap>;
	using ReverseIteratorImpl = index::iterators::BtreeIndexReverseIteratorImpl<IndexMap>;
	using BtreeIndexIteratorImpl = std::variant<ForwardIteratorImpl, ReverseIteratorImpl>;
	BtreeIndexIteratorImpl impl_;

	const typename IndexMap::const_iterator first_;
	const typename IndexMap::const_iterator last_;

	const IdSet* nullValues_;
	IdType lastVal_ = INT_MIN;

	enum Type { Empty, Reverse, Forward };
	Type type_{Empty};

	struct [[nodiscard]] CachedIters {
		bool Valid(size_t limitIters) const noexcept {
			return fullyScanned || (limitIters <= value && value != std::numeric_limits<size_t>::max());
		}

		size_t value = std::numeric_limits<size_t>::max();
		bool fullyScanned = false;
	} cachedIters_;
};

}  // namespace reindexer
