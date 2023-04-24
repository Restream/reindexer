#pragma once

#include <utility>
#include <vector>
#include "estl/h_vector.h"
#include "tools/assertrx.h"

namespace reindexer {

template <typename K, typename V, size_t N, typename H, typename C>
class MultiHashMap {
public:
	MultiHashMap(size_t s) : capacity_{2 * s} {
		data_.reserve(s);
		for (auto& i : indexes_) {
			i.resize(capacity_);
		}
	}
	bool insert(K k, V v) {
		assertrx(size_ * 2 < capacity_);  // rehash is not implemented
		auto [firstIdx, firstHash] = H::hash(k);
		assertrx_throw(firstIdx < indexes_.size());
		firstHash %= capacity_;
		for (size_t i : indexes_[firstIdx][firstHash]) {
			if (C::equal(k, data_[i].first)) {
				return false;
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
						hash = H::hash(changingIndex, k) % capacity_;
					} catch (...) {
						changedIndexes[changingIndex] = -1;
						continue;
					}
					indexes_[changingIndex][hash].push_back(s);
					changedIndexes[changingIndex] = hash;
				}
			}
			data_.emplace_back(std::move(k), std::move(v));
		} catch (...) {
			for (size_t i = 0; i < changingIndex; ++i) {
				if (changedIndexes[i] >= 0) {
					indexes_[i][changedIndexes[i]].pop_back();
				}
			}
			throw;
		}
		++size_;
		return true;
	}
	auto find(const K& k) const {
		auto [firstIdx, hash] = H::hash(k);
		assertrx_throw(firstIdx < indexes_.size());
		hash %= capacity_;
		for (const size_t i : indexes_[firstIdx][hash]) {
			if (C::equal(data_[i].first, k)) {
				return data_.cbegin() + i;
			}
		}
		for (size_t i = 0; i < N; ++i) {
			if (i == firstIdx) continue;
			size_t hash;
			try {
				hash = H::hash(i, k) % capacity_;
			} catch (...) {
				continue;
			}
			for (const size_t j : indexes_[i][hash]) {
				if (C::equal(data_[j].first, k)) {
					return data_.cbegin() + j;
				}
			}
		}
		return cend();
	}
	auto cend() const noexcept { return data_.cend(); }

private:
	std::vector<std::pair<K, V>> data_;
	std::array<std::vector<h_vector<size_t, 2>>, N> indexes_;
	size_t size_{0};
	const size_t capacity_;
};

}  // namespace reindexer
