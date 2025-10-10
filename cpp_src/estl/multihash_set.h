#pragma once

#include <utility>
#include <vector>
#include "estl/h_vector.h"
#include "tools/assertrx.h"

namespace reindexer {

template <typename S, typename K, typename Derived, size_t N, typename H, typename C>
class [[nodiscard]] MultiHashSetImpl {
public:
	using StoringType = S;
	using KeyType = K;
	using StoreType = std::vector<StoringType>;

public:
	using Iterator = typename StoreType::const_iterator;
	MultiHashSetImpl(size_t s) : capacity_{2 * (s + 1)} {
		data_.reserve(s);
		for (auto& i : indexes_) {
			i.resize(capacity_);
		}
	}
	bool empty() const noexcept { return data_.empty(); }
	template <typename... Args>
	std::pair<Iterator, bool> emplace(Args&&... args) {
		return insert(StoringType{std::forward<Args>(args)...});
	}
	std::pair<Iterator, bool> insert(StoringType v) {
		assertrx_throw(size_ * 2 < capacity_);	// rehash is not implemented
		auto [firstIdx, firstHash] = H::hash(Derived::getKey(v));
		assertrx_throw(firstIdx < indexes_.size());
		firstHash %= capacity_;
		for (size_t i : indexes_[firstIdx][firstHash]) {
			if (C::equal(Derived::getKey(v), Derived::getKey(data_[i]))) {
				return std::make_pair(data_.cbegin() + i, false);
			}
		}
		const size_t s = data_.size();
		ptrdiff_t changedIndexes[N];
		size_t changingIndex{0};
		try {
			for (; changingIndex < N; ++changingIndex) {
				if (changingIndex == firstIdx) {
					indexes_[firstIdx][firstHash].push_back(s);
					changedIndexes[firstIdx] = firstHash;
				} else {
					size_t hash;
					try {
						hash = H::hash(changingIndex, Derived::getKey(v)) % capacity_;
					} catch (...) {
						changedIndexes[changingIndex] = -1;
						continue;
					}
					indexes_[changingIndex][hash].push_back(s);
					changedIndexes[changingIndex] = hash;
				}
			}
			data_.emplace_back(std::move(v));
		} catch (...) {
			for (size_t i = 0; i < changingIndex; ++i) {
				if (changedIndexes[i] >= 0) {
					indexes_[i][changedIndexes[i]].pop_back();
				}
			}
			throw;
		}
		++size_;
		return std::make_pair(data_.cbegin() + s, true);
	}
	Iterator find(const KeyType& k) const {
		auto [firstIdx, hash] = H::hash(k);
		assertrx_throw(firstIdx < indexes_.size());
		hash %= capacity_;
		for (const size_t i : indexes_[firstIdx][hash]) {
			if (C::equal(Derived::getKey(data_[i]), k)) {
				return data_.cbegin() + i;
			}
		}
		for (size_t i = 0; i < N; ++i) {
			if (i == firstIdx) {
				continue;
			}
			size_t hash;
			try {
				hash = H::hash(i, k) % capacity_;
			} catch (...) {
				continue;
			}
			for (const size_t j : indexes_[i][hash]) {
				if (C::equal(Derived::getKey(data_[j]), k)) {
					return data_.cbegin() + j;
				}
			}
		}
		return cend();
	}
	Iterator cbegin() const noexcept { return data_.cbegin(); }
	Iterator cend() const noexcept { return data_.cend(); }
	size_t size() const noexcept { return data_.size(); }

private:
	StoreType data_;
	std::array<std::vector<h_vector<size_t, 2>>, N> indexes_;
	size_t size_{0};
	size_t capacity_;
};

template <typename T, size_t N, typename H, typename C>
class [[nodiscard]] MultiHashSet : private MultiHashSetImpl<T, T, MultiHashSet<T, N, H, C>, N, H, C> {
	using Base = MultiHashSetImpl<T, T, MultiHashSet<T, N, H, C>, N, H, C>;

public:
	using Base::Base;
	using Base::emplace;
	using Base::empty;
	using Base::cbegin;
	using Base::cend;
	using Base::insert;
	using Base::find;
	using Base::size;
	using typename Base::Iterator;
	static const typename Base::KeyType& getKey(const typename Base::StoringType& v) noexcept { return v; }
};

}  // namespace reindexer
