#pragma once

#include <cstring>
#include <functional>
#include "sparse-map/sparse_map.h"
#include "tools/assertrx.h"

template <size_t NumBucketsInPart = (1 << 19), size_t NumBitsToSelectPart = 13, size_t MinLoadFactorPercents = 10,
		  size_t MaxLoadFactorPercents = 65>
struct [[nodiscard]] extendible_hash_map_rebalance_params {
	static constexpr size_t kNumBucketsInPart = NumBucketsInPart;
	static constexpr size_t kNumBitsToSelectPart = NumBitsToSelectPart;

	static_assert(MinLoadFactorPercents > 0 && MinLoadFactorPercents < 100);
	static_assert(MaxLoadFactorPercents > 0 && MaxLoadFactorPercents < 100);
	static_assert(MinLoadFactorPercents < MaxLoadFactorPercents);
	static constexpr float kMinLoadFactor = static_cast<float>(MinLoadFactorPercents) / 100.0f;
	static constexpr float kMaxLoadFactor = static_cast<float>(MaxLoadFactorPercents) / 100.0f;
};

template <class BaseHashMapType, class RebalanceParams, class Key, class T, class Hash = std::hash<Key>,
		  class KeyEqual = std::equal_to<Key> >
class [[nodiscard]] extendible_hash_map {
public:
	using key_type = Key;
	using mapped_type = T;
	using value_type = typename BaseHashMapType::value_type;
	using size_type = std::size_t;
	using difference_type = std::ptrdiff_t;
	using hasher = Hash;
	using reference = value_type&;
	using const_reference = const value_type&;
	using pointer = value_type*;
	using const_pointer = const value_type*;

	static constexpr size_t kNumBucketsInPart = RebalanceParams::kNumBucketsInPart;
	static constexpr size_t kMinPartSize = static_cast<size_t>(kNumBucketsInPart * RebalanceParams::kMinLoadFactor);
	static constexpr size_t kCriticalPartSize = static_cast<size_t>(kNumBucketsInPart * RebalanceParams::kMaxLoadFactor) - 5;
	static constexpr size_t kNumBitsToSelectPart = RebalanceParams::kNumBitsToSelectPart;
	static constexpr float kMaxLoadFactor = RebalanceParams::kMaxLoadFactor;

	static constexpr size_t kMaxBucketsCountForCorrectStats = 128 * kCriticalPartSize;
	static constexpr float kMinLoadFactorToUseStats = 0.1f;
	static constexpr size_t kMaxParts = 1ULL << kNumBitsToSelectPart;
	static_assert(kMaxParts > 0);

	static constexpr size_t kNumRemovesToJoinMinParts = 10000;

	template <bool IsConst>
	class [[nodiscard]] extendible_iterator {
	public:
		friend class extendible_hash_map;

		using base_iterator =
			typename std::conditional<IsConst, typename BaseHashMapType::const_iterator, typename BaseHashMapType::iterator>::type;

		using base_map_pointer = typename std::conditional<IsConst, const extendible_hash_map*, extendible_hash_map*>::type;

		using iterator_category = std::forward_iterator_tag;
		using value_type = typename extendible_hash_map::value_type;
		using difference_type = std::ptrdiff_t;
		using reference = value_type&;
		using pointer = value_type*;

		extendible_iterator(base_map_pointer baseMap, size_t partIdx, base_iterator it = base_iterator()) noexcept
			: baseMap_(baseMap), partIdx_(partIdx), it_(it) {
			if (partIdx_ < baseMap_->partsUsed_ && it_ == baseMap_->parts_[partIdx_]->end()) {
				it_ = findNextUnemptyPart() ? baseMap_->parts_[partIdx_]->begin() : base_iterator();
			}
		}

		extendible_iterator() noexcept = default;

		// Copy constructor from iterator to const_iterator.
		template <bool TIsConst = IsConst, typename std::enable_if<TIsConst>::type* = nullptr>
		extendible_iterator(const extendible_iterator<!TIsConst>& other) noexcept
			: baseMap_(other.baseMap_), partIdx_(other.partIdx_), it_(other.it_) {}

		extendible_iterator(const extendible_iterator& other) = default;
		extendible_iterator(extendible_iterator&& other) = default;
		extendible_iterator& operator=(const extendible_iterator& other) = default;
		extendible_iterator& operator=(extendible_iterator&& other) = default;

		std::conditional_t<IsConst, const_reference, reference> operator*() const { return *it_; }

		std::conditional_t<IsConst, const_pointer, pointer> operator->() const { return std::addressof(*it_); }

		extendible_iterator& operator++() noexcept {
			assertrx_dbg(partIdx_ < baseMap_->partsUsed_);
			++it_;

			if (it_ != baseMap_->parts_[partIdx_]->end()) {
				return *this;
			}

			it_ = findNextUnemptyPart() ? baseMap_->parts_[partIdx_]->begin() : base_iterator();

			return *this;
		}

		extendible_iterator operator++(int) noexcept {
			extendible_iterator tmp(*this);
			++*this;
			return tmp;
		}

		bool operator==(const extendible_iterator& rhs) const noexcept {
			return baseMap_ == rhs.baseMap_ && partIdx_ == rhs.partIdx_ && (partIdx_ == baseMap_->partsUsed_ || it_ == rhs.it_);
		}

		friend bool operator!=(const extendible_iterator& lhs, const extendible_iterator& rhs) noexcept { return !(lhs == rhs); }

	private:
		bool findNextUnemptyPart() noexcept {
			assertrx_dbg(partIdx_ < baseMap_->partsUsed_);
			++partIdx_;
			while (partIdx_ < baseMap_->partsUsed_ && baseMap_->parts_[partIdx_]->empty()) {
				++partIdx_;
			}

			return partIdx_ < baseMap_->partsUsed_;
		}

		base_map_pointer baseMap_;
		size_t partIdx_ = 0;
		base_iterator it_;
	};

	using iterator = extendible_iterator<false>;
	using const_iterator = extendible_iterator<true>;

	explicit extendible_hash_map(const Hash& h = Hash(), const KeyEqual& ke = KeyEqual()) : h_(h), ke_(ke) {
		size_ = 0;
		partsUsed_ = 1;
		// starting from small map
		parts_[0] = std::make_unique<BaseHashMapType>(0, h_, ke_);
		parts_[0]->max_load_factor(kMaxLoadFactor);
		partsSplitCounts_[0] = kMaxParts;
	}

	extendible_hash_map(const extendible_hash_map& hm) : h_(hm.h_), ke_(hm.ke_) {
		size_ = hm.size_;
		partsUsed_ = hm.partsUsed_;
		for (size_t i = 0; i < partsUsed_; ++i) {
			parts_[i] = std::make_unique<BaseHashMapType>(*hm.parts_[i]);
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			partsToUse_[i] = hm.partsToUse_[i];
			partsSplitCounts_[i] = hm.partsSplitCounts_[i];
		}
	}

	extendible_hash_map(extendible_hash_map&& hm) noexcept : h_(std::move(hm.h_)), ke_(std::move(hm.ke_)) {
		size_ = hm.size_;
		partsUsed_ = hm.partsUsed_;
		for (size_t i = 0; i < partsUsed_; ++i) {
			parts_[i] = std::unique_ptr<BaseHashMapType>(hm.parts_[i].release());
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			partsToUse_[i] = hm.partsToUse_[i];
			partsSplitCounts_[i] = hm.partsSplitCounts_[i];
		}
	}

	iterator begin() noexcept { return empty() ? end() : iterator(this, 0, parts_[0]->begin()); }
	const_iterator begin() const noexcept { return empty() ? end() : const_iterator(this, 0, parts_[0]->begin()); }

	iterator end() noexcept { return iterator(this, partsUsed_); }
	const_iterator end() const noexcept { return const_iterator(this, partsUsed_); }

	bool empty() const noexcept { return !size(); }
	size_type size() const noexcept { return size_; }

	size_t max_load_factor() const noexcept { return kMaxLoadFactor; }

	template <class... Args>
	std::pair<iterator, bool> emplace(const Key& key, Args&&... value_type_args) {
		return insert(key, std::forward<Args>(value_type_args)...);
	}

	void erase(iterator it) {
		assertrx_dbg(it.it_ != parts_[it.partIdx_]->end());
		std::ignore = parts_[it.partIdx_]->erase(it.it_);
		--size_;
		removesCounter_++;
		rebalance(it.partIdx_);
	}

	template <class K>
	size_t erase(const K& key) {
		size_t hash = h_(key);
		rebalance(partIdx(hash));

		size_t res = parts_[partIdx(hash)]->erase(key, hash);
		size_ -= res;
		removesCounter_ += res;
		return res;
	}

	std::pair<iterator, bool> insert(const value_type& v) { return insert(v.first, v.second); }

	template <class K, class... Args>
	std::pair<iterator, bool> insert(const K& key, Args&&... value_type_args) {
		size_t hash = h_(key);
		rebalance(partIdx(hash));
		auto res = insert_impl_with_hash(key, hash, std::forward<Args>(value_type_args)...);
		if (res.second) {
			++size_;
		}
		return res;
	}

	T& operator[](const Key& key) {
		size_t hash = h_(key);
		rebalance(partIdx(hash));

		auto res = parts_[partIdx(hash)]->try_emplace_with_hash(hash, key);

		if (res.second) {
			++size_;
		}

		return res.first->second;
	}

	template <class K>
	iterator find(const K& key) {
		size_t hash = h_(key);
		size_t pIdx = partIdx(hash);
		auto it = parts_[pIdx]->find(key, hash);
		if (it != parts_[pIdx]->end()) {
			return iterator(this, pIdx, it);
		}

		return end();
	}

	template <class K>
	const_iterator find(const K& key) const {
		size_t hash = h_(key);
		size_t pIdx = partIdx(hash);
		auto it = parts_[pIdx]->find(key, hash);
		if (it != parts_[pIdx]->end()) {
			return const_iterator(this, pIdx, it);
		}

		return end();
	}

	void add_destroy_task(tsl::detail_sparse_hash::ThreadTaskQueue* q) {
		for (size_t i = 0; i < partsUsed_; ++i) {
			parts_[i]->add_destroy_task(q);
		}
	}

	void dumpStats(std::vector<char>& stats) const {
		stats.resize(0);
		addToStats(stats, partsUsed_);
		addToStats(stats, kMaxParts);

		std::vector<uint8_t> data;
		for (size_t i = 0; i < partsUsed_; ++i) {
			data.resize(0);
			parts_[i]->buckets_stats(data);

			addToStats(stats, parts_[i]->bucket_count());
			addToStats(stats, data.size());
			if (!data.empty()) {
				static_assert(sizeof(char) == sizeof(uint8_t));
				char* b = reinterpret_cast<char*>(&data[0]);
				char* e = b + data.size();
				stats.insert(stats.end(), b, e);
			}
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			addToStats(stats, partsToUse_[i]);
		}
	}

	bool checkStatsCorrectness(const std::vector<char>& stats) const noexcept {
		size_t pos = 0;
		uint32_t partsUsed = 0;
		uint32_t tmp = 0;
		loadFromStats(stats, pos, partsUsed);
		loadFromStats(stats, pos, tmp);
		if (tmp != kMaxParts) {
			return false;
		}
		if (partsUsed > kMaxParts) {
			return false;
		}

		for (size_t i = 0; i < partsUsed; ++i) {
			uint32_t buckets_count = 0;
			loadFromStats(stats, pos, buckets_count);
			if (buckets_count > kMaxBucketsCountForCorrectStats) {
				return false;
			}

			uint32_t sz = 0;
			loadFromStats(stats, pos, sz);

			const uint8_t* b = reinterpret_cast<const uint8_t*>(&stats[pos]);
			if (!BaseHashMapType::stats_correct(buckets_count, b, sz)) {
				return false;
			}
			pos += sz;
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			uint32_t partToUse = 0;
			loadFromStats(stats, pos, partToUse);
			if (partToUse >= partsUsed) {
				return false;
			}
		}

		return true;
	}

	float statsLoadFactor(uint32_t buckets_count, const uint8_t* stats, size_t stats_size) const noexcept {
		size_t buckets_occupied = 0.0;
		for (size_t idx = 0; idx < stats_size; ++idx) {
			buckets_occupied += stats[idx];
		}

		if (buckets_count == 0) {
			return 0.0f;
		}

		return static_cast<float>(static_cast<double>(buckets_occupied) / buckets_count);
	}

	void reserveFromStats(const std::vector<char>& stats) {
		assertrx_throw(size_ == 0);
		size_t pos = 0;

		uint32_t partsUsed = 0, tmp = 0;
		loadFromStats(stats, pos, partsUsed);
		loadFromStats(stats, pos, tmp);
		assertrx_dbg(tmp == kMaxParts);
		assertrx_dbg(partsUsed_ <= partsUsed);

		if (tmp != kMaxParts || partsUsed_ > partsUsed) {
			return;
		}

		partsUsed_ = partsUsed;
		std::vector<uint8_t> bucketsStats;
		for (size_t i = 0; i < partsUsed_; ++i) {
			uint32_t buckets_count = 0;
			loadFromStats(stats, pos, buckets_count);
			uint32_t sz = 0;
			loadFromStats(stats, pos, sz);
			bucketsStats.resize(0);
			const uint8_t* b = reinterpret_cast<const uint8_t*>(&stats[pos]);
			const uint8_t* e = b + sz;
			bucketsStats.insert(bucketsStats.end(), b, e);
			pos += sz;
			assertrx_throw(parts_[i].get() == nullptr || parts_[i]->empty());
			if (statsLoadFactor(buckets_count, b, sz) > kMinLoadFactorToUseStats) {
				parts_[i] = std::make_unique<BaseHashMapType>(buckets_count, bucketsStats, h_, ke_);
			} else {
				parts_[i] = std::make_unique<BaseHashMapType>(0, h_, ke_);
			}
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			loadFromStats(stats, pos, partsToUse_[i]);
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			partsSplitCounts_[i] = 0;
		}

		for (size_t i = 0; i < kMaxParts; ++i) {
			partsSplitCounts_[partsToUse_[i]]++;
		}
	}

	size_t num_parts() const noexcept { return partsUsed_; }

private:
	size_t partIdx(size_t hash) const noexcept { return partsToUse_[hash & (kMaxParts - 1)]; }

	void addToStats(std::vector<char>& stats, uint32_t v) const {
		stats.resize(stats.size() + sizeof(v));
		memcpy(&stats[stats.size() - sizeof(v)], &v, sizeof(v));
	}

	void loadFromStats(const std::vector<char>& stats, size_t& pos, uint32_t& v) const noexcept {
		memcpy(&v, &stats[pos], sizeof(v));
		pos += sizeof(v);
	}

	template <class K, class... Args>
	std::pair<iterator, bool> insert_impl(const K& key, Args&&... value_type_args) {
		size_t hash = h_(key);
		size_t pIdx = partIdx(hash);
		auto res = parts_[pIdx]->try_emplace_with_hash(hash, key, std::forward<Args>(value_type_args)...);
		return {iterator(this, pIdx, res.first), res.second};
	}

	template <class K, class... Args>
	std::pair<iterator, bool> insert_impl_with_hash(const K& key, size_t hash, Args&&... value_type_args) {
		size_t pIdx = partIdx(hash);
		auto res = parts_[pIdx]->try_emplace_with_hash(hash, key, std::forward<Args>(value_type_args)...);
		return {iterator(this, pIdx, res.first), res.second};
	}

	void addNewPart() {
		assertrx_dbg(partsUsed_ < kMaxParts);
		parts_[partsUsed_] = std::make_unique<BaseHashMapType>(kNumBucketsInPart, h_, ke_);
		parts_[partsUsed_]->max_load_factor(kMaxLoadFactor);
		++partsUsed_;
	}

	// NOLINTNEXTLINE(bugprone-exception-escape)
	void rebalanceParts(size_t bigPartIdx, size_t newPartIdx) noexcept {
		bool even = true;
		for (size_t i = 0; i < kMaxParts; ++i) {
			if (partsToUse_[i] == bigPartIdx) {
				if (!even) {
					partsToUse_[i] = newPartIdx;
					partsSplitCounts_[bigPartIdx]--;
					partsSplitCounts_[newPartIdx]++;
				}
				even = !even;
			}
		}

		auto tmpPart = std::make_unique<BaseHashMapType>(kNumBucketsInPart, h_, ke_);
		tmpPart->max_load_factor(kMaxLoadFactor);

		parts_[bigPartIdx].swap(tmpPart);
		for (auto& [k, v] : *tmpPart) {
			[[maybe_unused]] auto res = insert_impl(k, std::move(v));
			assertrx_dbg(res.second);
		}
	}

	// NOLINTNEXTLINE(bugprone-exception-escape)
	void joinPartTo(size_t sourcePartIdx, size_t destinationPartIdx) noexcept {
		assertrx_dbg(sourcePartIdx != destinationPartIdx);
		partsSplitCounts_[destinationPartIdx] += partsSplitCounts_[sourcePartIdx];
		partsSplitCounts_[sourcePartIdx] = 0;

		for (size_t i = 0; i < kMaxParts; ++i) {
			if (partsToUse_[i] == sourcePartIdx) {
				partsToUse_[i] = destinationPartIdx;
			}
		}

		for (auto& [k, v] : *parts_[sourcePartIdx]) {
			[[maybe_unused]] auto res = insert_impl(k, std::move(v));
			assertrx_dbg(res.second);
		}
	}

	void joinParts(size_t partIdx1, size_t partIdx2) {
		assertrx_dbg(partIdx1 != partIdx2);
		if (parts_[partIdx1]->size() > parts_[partIdx2]->size()) {
			std::swap(partIdx1, partIdx2);
		}

		joinPartTo(partIdx1, partIdx2);

		if (partIdx1 != partsUsed_ - 1) {
			std::swap(parts_[partIdx1], parts_[partsUsed_ - 1]);
			std::swap(partsSplitCounts_[partIdx1], partsSplitCounts_[partsUsed_ - 1]);

			for (size_t i = 0; i < kMaxParts; ++i) {
				if (partsToUse_[i] == partsUsed_ - 1) {
					partsToUse_[i] = partIdx1;
				}
			}
		}

		parts_[partsUsed_ - 1].reset();
		--partsUsed_;
	}

	void rebalance(size_t partIdx) {
		if (parts_[partIdx]->size() >= kCriticalPartSize && partsSplitCounts_[partIdx] > 1) [[unlikely]] {
			addNewPart();
			rebalanceParts(partIdx, partsUsed_ - 1);
		} else if (removesCounter_ > kNumRemovesToJoinMinParts && parts_[partIdx]->size() < kMinPartSize && partsUsed_ > 1) [[unlikely]] {
			removesCounter_ = 0;
			size_t smallest = partIdx != 0 ? 0 : 1;
			for (size_t i = 0; i < partsUsed_; ++i) {
				if (i != partIdx && parts_[i]->size() < parts_[smallest]->size()) {
					smallest = i;
				}
			}

			if (parts_[partIdx]->size() + parts_[smallest]->size() <= kCriticalPartSize / 2) {
				joinParts(partIdx, smallest);
			}
		}
	}

	Hash h_;
	KeyEqual ke_;

	size_t size_ = 0;
	size_t partsUsed_ = 1;
	uint32_t partsToUse_[kMaxParts] = {0};
	uint32_t partsSplitCounts_[kMaxParts] = {0};
	std::unique_ptr<BaseHashMapType> parts_[kMaxParts];

	size_t removesCounter_ = 0;
};