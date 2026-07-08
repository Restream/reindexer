// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include <queue>
#include <random>
#include <ranges>

#include "hnsw_interface.h"
#include "hnswlib.h"
#include "priority_queue.h"
#include "visited_list_pool.h"

#include "core/index/float_vector/scalar_quantization/quantizer.h"
#include "core/keyvalue/float_vector.h"

#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"

#include "tools/clock.h"
#include "tools/flagguard.h"
#include "tools/float_comparison.h"
#include "tools/logger.h"
#include "tools/unaligned.h"

#include "vendor/hopscotch/hopscotch_sc_map.h"

namespace hnswlib {
static_assert(sizeof(linklistsizeint) == sizeof(tableint), "Internal link lists logic requires the same size for this types");

RX_ALWAYS_INLINE tableint readLinkListNeighbor(const void* linkList0, size_t neighborIdx) noexcept {
	return reindexer::unaligned::read<tableint>(static_cast<const char*>(linkList0) + sizeof(linklistsizeint) +
												neighborIdx * sizeof(tableint));
}

RX_ALWAYS_INLINE void writeLinkListNeighbor(void* linkList0, size_t neighborIdx, tableint value) noexcept {
	reindexer::unaligned::write<tableint>(static_cast<char*>(linkList0) + sizeof(linklistsizeint) + neighborIdx * sizeof(tableint), value);
}

enum class [[nodiscard]] ExpectConcurrentUpdates : bool { No, Yes };

class [[nodiscard]] LabelOpsMutexLocks {
public:
	using MutexT = reindexer::mutex;

	LabelOpsMutexLocks() = default;
	LabelOpsMutexLocks(tableint max_locks) : mtxs_(max_locks) {}
	inline MutexT& GetLabelOpMutex(labeltype label) const noexcept {
		// calculate hash
		size_t lock_id = label & (mtxs_.size() - 1);
		return mtxs_[lock_id];
	}
	MutexT& operator[](tableint id) const noexcept { return mtxs_[id]; }
	void swap(LabelOpsMutexLocks& o) noexcept { mtxs_.swap(o.mtxs_); }
	size_t allocatedMemSize() const noexcept { return mtxs_.capacity() * sizeof(MutexT); }

private:
	mutable std::vector<MutexT> mtxs_;
};

class [[nodiscard]] DummyMutex {
public:
	constexpr void lock() const noexcept {}
	constexpr void lock_shared() const noexcept {}
	constexpr bool try_lock() const noexcept { return true; }
	constexpr void unlock() const noexcept {}
	constexpr void unlock_shared() const noexcept {}
	const DummyMutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

class [[nodiscard]] LabelOpsDummyLocks {
public:
	using MutexT = DummyMutex;

	LabelOpsDummyLocks() = default;
	LabelOpsDummyLocks(tableint /*max_locks*/) {}
	inline MutexT& GetLabelOpMutex(labeltype /*label*/) const noexcept { return mtx_; }
	MutexT& operator[](tableint /*id*/) const noexcept { return mtx_; }
	void swap(LabelOpsDummyLocks&) const noexcept {}
	size_t allocatedMemSize() const noexcept { return sizeof(mtx_); }

private:
	mutable MutexT mtx_;
};

class [[nodiscard]] UpdateOpsMutexLocks {
public:
	using MutexT = reindexer::read_write_spinlock;

	UpdateOpsMutexLocks() = default;
	UpdateOpsMutexLocks(tableint max_locks) : mtxs_(max_locks) {}
	MutexT& operator[](tableint id) const noexcept { return mtxs_[id]; }
	void swap(UpdateOpsMutexLocks& o) noexcept { mtxs_.swap(o.mtxs_); }
	size_t allocatedMemSize() const noexcept { return mtxs_.capacity() * sizeof(MutexT); }

private:
	mutable std::vector<MutexT> mtxs_;
};

using UpdateOpsDummyLocks = LabelOpsDummyLocks;

struct [[nodiscard]] DummyLocker {
	template <typename Mtx>
	struct [[maybe_unused]] DummyLock {
		explicit DummyLock(auto&&...) noexcept {}
		void unlock() const noexcept {}
		void lock() const noexcept {}
	};

	template <typename Mtx>
	using UniqueLock = DummyLock<Mtx>;
	template <typename Mtx>
	using LockGuard = DummyLock<Mtx>;
	template <typename Mtx>
	using SharedLock = DummyLock<Mtx>;

	template <ExpectConcurrentUpdates>
	using Concurrent = DummyLocker;
};

class [[nodiscard]] RegularLocker {
public:
	template <typename Mtx>
	struct [[nodiscard]] RX_SCOPED_CAPABILITY UniqueLock : public reindexer::unique_lock<Mtx> {
		using Base = reindexer::unique_lock<Mtx>;
		explicit UniqueLock(Mtx& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
		explicit UniqueLock(Mtx& mtx, std::defer_lock_t) RX_EXCLUDES(!mtx) : Base{mtx, std::defer_lock} {}
		explicit UniqueLock(Mtx& mtx, reindexer::SkipLock skipLock) RX_ACQUIRE(mtx) : Base{mtx, std::defer_lock} {
			if (!skipLock) {
				Base::lock();
			}
		}
		void unlock() RX_RELEASE() { Base::unlock(); }
		~UniqueLock() RX_RELEASE() = default;
	};
	template <typename Mtx>
	struct [[nodiscard]] RX_SCOPED_CAPABILITY LockGuard : public reindexer::lock_guard<Mtx> {
		explicit LockGuard(Mtx& mtx) RX_ACQUIRE(mtx) : reindexer::lock_guard<Mtx>{mtx} {}
		~LockGuard() RX_RELEASE() = default;
	};

	template <typename Mtx>
	class [[nodiscard]] RX_SCOPED_CAPABILITY SharedLock : public reindexer::shared_lock<Mtx> {
		using Base = reindexer::shared_lock<Mtx>;

	public:
		explicit SharedLock(Mtx& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
		explicit SharedLock(Mtx& mtx, std::defer_lock_t) noexcept RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
		~SharedLock() RX_RELEASE() = default;
	};

	template <ExpectConcurrentUpdates>
	struct Concurrent;
};

template <>
struct [[nodiscard]] RegularLocker::Concurrent<ExpectConcurrentUpdates::Yes> {
	template <typename Mtx>
	struct [[nodiscard]] RX_SCOPED_CAPABILITY SharedLock : public RegularLocker::SharedLock<Mtx> {
		using Base = RegularLocker::SharedLock<Mtx>;
		explicit SharedLock(Mtx& mtx) noexcept RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
		explicit SharedLock(Mtx& mtx, std::defer_lock_t) noexcept RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
		explicit SharedLock(Mtx& mtx, reindexer::SkipLock skipLock) noexcept RX_ACQUIRE_SHARED(mtx) : Base{mtx, std::defer_lock} {
			if (!skipLock) {
				Base::lock();
			}
		}
		~SharedLock() RX_RELEASE() = default;
	};
};

template <>
struct [[nodiscard]] RegularLocker::Concurrent<ExpectConcurrentUpdates::No> {
	template <typename Mtx>
	struct [[nodiscard]] SharedLock {
		explicit SharedLock(Mtx&) noexcept {}
		explicit SharedLock(Mtx&, std::defer_lock_t) noexcept {}
		explicit SharedLock(Mtx&, reindexer::SkipLock) noexcept {}
		void lock() const noexcept {}
		void unlock() const noexcept {}
	};
};

template <typename ValueT, Synchronization synchronization>
class [[nodiscard]] HierarchicalNSWImpl
	: public HierarchicalNSWInterface<synchronization> {  // NOLINT(clang-analyzer-optin.performance.Padding)
	static_assert(std::is_same_v<ValueT, float> || std::is_same_v<ValueT, uint8_t>, "Unexpected HNSW value type");

	template <typename K, typename V>
	using HashMapT = tsl::hopscotch_sc_map<K, V, std::hash<K>, std::equal_to<K>, std::less<K>, std::allocator<std::pair<const K, V>>, 30,
										   false, tsl::mod_growth_policy<std::ratio<3, 2>>>;
	template <typename K>
	using HashSetT = tsl::hopscotch_sc_set<K, std::hash<K>, std::equal_to<K>, std::less<K>, std::allocator<K>, 30, false,
										   tsl::mod_growth_policy<std::ratio<3, 2>>>;

	using LockVecT = std::conditional_t<synchronization == Synchronization::None, LabelOpsDummyLocks, LabelOpsMutexLocks>;
	using UpdateLockVecT = std::conditional_t<synchronization == Synchronization::None, UpdateOpsDummyLocks, UpdateOpsMutexLocks>;
	using GlobalMutextT = std::conditional_t<synchronization == Synchronization::None, DummyMutex, reindexer::mutex>;

public:
	static const unsigned char DELETE_MARK = 0x01;

	size_t max_elements_;
	mutable std::atomic<size_t> cur_element_count{0};  // current number of elements

	std::unique_ptr<Quantizer> quantizer_;
	DistCalculator<ValueT> fstdistfunc_;

	int maxlevel_ = -1;
	UpdateLockVecT::MutexT enterpoint_lock_;  // Update mutex for enterpoint and maxlevel
	tableint enterpoint_node_ = std::numeric_limits<tableint>::max();

	const size_t M_;
	const size_t maxM0_ = 2 * M_;
	const double mult_ = 1.0 / log(1.0 * M_);
	const size_t ef_construction_;

	const size_t size_links_per_element_ = M_ * sizeof(tableint) + sizeof(linklistsizeint);
	const size_t offsetData_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);

	////// / params that depend on sizeof(ValueT) //////////
	const size_t data_size_;  //  dim * sizeof(ValueT)
	const size_t label_offset_ = offsetData_ + data_size_;
	const size_t hash_offset_ = label_offset_ + sizeof(labeltype);
	const size_t size_data_per_element_ = hash_offset_ + sizeof(uint64_t);
	////////////////////////////////////////////////////

	char* data_level0_memory_ = nullptr;

	std::unique_ptr<VisitedListPool> visited_list_pool_ = std::make_unique<VisitedListPool>(1, max_elements_);

	GlobalMutextT global;
	LockVecT link_list_locks_ = LockVecT(max_elements_);
	UpdateLockVecT data_updates_locks_ = UpdateLockVecT(max_elements_);

	char** linkLists_{nullptr};
	std::vector<int> element_levels_ = std::vector<int>(max_elements_);	 // keeps level of each element

	mutable GlobalMutextT label_lookup_lock;  // lock for label_lookup_
	HashMapT<labeltype, tableint> label_lookup_;

	mutable GlobalMutextT generator_lock_;
	std::default_random_engine level_generator_{uint32_t(reindexer::system_clock_w::now_count())};
	GlobalMutextT update_generator_lock_;
	std::default_random_engine update_probability_generator_{uint32_t(reindexer::system_clock_w::now_count())};

	mutable std::atomic<long> metric_distance_computations = 0;
	mutable std::atomic<long> metric_hops = 0;

	reindexer::ReplaceDeleted allow_replace_deleted_ =
		reindexer::ReplaceDeleted_False;  // flag to replace deleted elements (marked as deleted) during insertions

	GlobalMutextT deleted_elements_lock;  // lock for deleted_elements
	HashSetT<tableint> deleted_elements;  // contains internal ids of deleted elements
	size_t num_deleted_ = 0;			  // number of deleted elements

	std::atomic<int32_t> concurrent_updates_counter_ = 0;

	HierarchicalNSWImpl(reindexer::VectorMetric metric, size_t dim, size_t max_elements, size_t M, size_t ef_construction,
						size_t random_seed, reindexer::ReplaceDeleted allow_replace_deleted)
		requires(std::is_same_v<ValueT, float>)
		: max_elements_(max_elements),
		  fstdistfunc_(metric, dim, max_elements_),
		  M_([M] {
			  if (M > 10'000) {
				  logFmt(LogWarning,
						 "HNSW: M parameter exceeds 10000 which may lead to adverse effects. Cap to 10000 will be applied for the rest "
						 "of the "
						 "processing.");
			  }
			  return std::min<size_t>(M, 10'000ul);
		  }()),
		  ef_construction_(std::max(ef_construction, M_)),
		  data_size_(dim * sizeof(ValueT)),
		  data_level0_memory_([this] {
			  auto res = static_cast<char*>(malloc(max_elements_ * size_data_per_element_));
			  if (res == nullptr) {
				  throw std::runtime_error("Not enough memory: HNSW constructor failed to allocate level0");
			  }
			  return res;
		  }()),
		  linkLists_([this] {
			  auto res = static_cast<char**>(malloc(sizeof(void*) * max_elements_));
			  if (res == nullptr) {
				  throw std::runtime_error("Not enough memory: HierarchicalNSW failed to allocate linklists");
			  }
			  return res;
		  }()),
		  allow_replace_deleted_(allow_replace_deleted) {
		level_generator_.seed(random_seed);
		update_probability_generator_.seed(random_seed + 1);
	}

	HierarchicalNSWImpl(IReader& reader, reindexer::VectorMetric metric, size_t dim, size_t random_seed,
						reindexer::ReplaceDeleted allow_replace_deleted, std::optional<QuantizingParams> quantizingParams)
		: max_elements_(reader.GetVarUInt()),
		  cur_element_count([this, &reader] {
			  const auto cur_count = reader.GetVarUInt();
			  if (cur_count > max_elements_) {
				  throw std::runtime_error("Current elements count is larger than max elements count");
			  }
			  return cur_count;
		  }()),
		  quantizer_([&]() {
			  if constexpr (std::is_same_v<ValueT, uint8_t>) {
				  if (!quantizingParams) {
					  throw std::runtime_error("Quantization config not passed in HNSW constructor");
				  }
				  return std::make_unique<Quantizer>(dim, metric, std::move(*quantizingParams),
													 [this]() noexcept { return cur_element_count.load(std::memory_order_relaxed); });
			  } else {
				  return nullptr;
			  }
		  }()),
		  fstdistfunc_(metric, dim, max_elements_, quantizer_ ? std::optional(quantizer_->Params().alpha_2) : std::nullopt),
		  maxlevel_(reader.GetVarInt()),
		  enterpoint_node_([this, &reader] {
			  const auto enterpoint_node = reader.GetVarUInt();
			  const auto cur_count = cur_element_count.load(std::memory_order_relaxed);
			  if (cur_count) {
				  if (enterpoint_node >= cur_count) {
					  throw std::runtime_error("Incorrect entrypoint node ID");
				  }
			  } else if (enterpoint_node != std::numeric_limits<tableint>::max()) {
				  throw std::runtime_error("Unexpected entrypoint node ID for empty HNSW");
			  }
			  return enterpoint_node;
		  }()),
		  M_(reader.GetVarUInt()),
		  ef_construction_(reader.GetVarUInt()),
		  data_size_(fstdistfunc_.Dim() * sizeof(ValueT)),

		  data_level0_memory_([&] {
			  auto res = static_cast<char*>(malloc(max_elements_ * size_data_per_element_));
			  if (res == nullptr) {
				  throw std::runtime_error("Not enough memory: HNSW constructor failed to allocate level0");
			  }
			  return res;
		  }()),
		  linkLists_([this] {
			  auto res = static_cast<char**>(malloc(sizeof(void*) * max_elements_));
			  if (res == nullptr) {
				  throw std::runtime_error("Not enough memory: HierarchicalNSW failed to allocate linklists");
			  }
			  return res;
		  }()),
		  allow_replace_deleted_(allow_replace_deleted) {
		level_generator_.seed(random_seed);
		update_probability_generator_.seed(random_seed + 1);

		auto origVecBuf = std::make_unique<float[]>(fstdistfunc_.Dim());
		const auto cur_count = cur_element_count.load(std::memory_order_relaxed);
		for (size_t i = 0; i < cur_count; i++) {
			char* cur_element_ptr = data_level0_memory_ + i * size_data_per_element_;
			auto* linkList0 = reinterpret_cast<tableint*>(cur_element_ptr);
			const auto* linkList0Start = linkList0;

			const linklistsizeint list0MarkedSize = reader.GetVarUInt();
			std::memcpy(linkList0++, &list0MarkedSize, sizeof(list0MarkedSize));
			const linklistsizeint list0Size = getListCount(&list0MarkedSize);
			for (size_t j = 0; j < list0Size; ++j) {
				tableint el = reader.GetVarUInt();
				std::memcpy(linkList0++, &el, sizeof(tableint));
			}
			cur_element_ptr += offsetData_;
			float* destPtr = quantizer_ ? origVecBuf.get() : reinterpret_cast<float*>(cur_element_ptr);
			labeltype label = std::numeric_limits<labeltype>::max();
			const bool isDeleted = isListMarkedDeleted(linkList0Start);
			if (isDeleted) {
				std::string_view vector = reader.GetVString();
				std::memcpy(destPtr, vector.data(), vector.size());
			} else {
				label = reader.ReadPkEncodedData(destPtr);
			}
			fstdistfunc_.AddNorm(destPtr, i);

			if (quantizer_) {
				auto from = std::span(origVecBuf.get(), fstdistfunc_.Dim());
				auto to = std::span(reinterpret_cast<uint8_t*>(cur_element_ptr), fstdistfunc_.Dim());
				fstdistfunc_.Sq8CorrectiveOffsets()[i] = quantizer_->Quantize(from, to);
			}

			cur_element_ptr += data_size_;
			std::memcpy(cur_element_ptr, &label, sizeof(labeltype));
			setHashByInternalId(i, isDeleted ? emptyVectorHash() : CalcHash(destPtr));
		}

		initTree([&reader, this](size_t i) -> size_t {
			auto list = reader.GetVString();
			if (list.size()) {
				linkLists_[i] = static_cast<char*>(malloc(list.size()));
				if (linkLists_[i] == nullptr) {
					throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklist");
				}
				std::memcpy(linkLists_[i], list.data(), list.size());
			} else {
				linkLists_[i] = nullptr;
			}
			return list.size();
		});
#ifdef RX_WITH_STDLIB_DEBUG
		if (allow_replace_deleted_) {
			assertrx_dbg(num_deleted_ == deleted_elements.size());
		}
#endif	// RX_WITH_STDLIB_DEBUG
	}

	template <typename OtherValueT, bool IsUint8 = std::is_same_v<ValueT, uint8_t>>
	HierarchicalNSWImpl(const HierarchicalNSWImpl<OtherValueT, synchronization>& other, size_t newMaxElements,
						std::optional<QuantizationConfig> quantizationConfig = std::nullopt)
		requires(std::is_same_v<ValueT, OtherValueT> || (IsUint8 && std::is_same_v<OtherValueT, float>))
		: max_elements_(std::max(other.max_elements_, newMaxElements)),
		  cur_element_count(other.cur_element_count.load()),
		  quantizer_([&]() {
			  if constexpr (std::is_same_v<OtherValueT, float>) {
				  if constexpr (IsUint8) {
					  if (!quantizationConfig) {
						  throw std::runtime_error("Quantization config not passed in HNSW copy(float-to-uint8_t) constructor");
					  }
					  return std::make_unique<Quantizer>(other.fstdistfunc_.Dim(), other.fstdistfunc_.Metric(),
														 QuantizingParams(other, std::move(*quantizationConfig)),
														 [this]() noexcept { return cur_element_count.load(std::memory_order_relaxed); });
				  } else {
					  return nullptr;
				  }
			  } else {
				  assertrx(!quantizationConfig);
				  return std::make_unique<Quantizer>(*other.quantizer_,
													 [this]() noexcept { return cur_element_count.load(std::memory_order_relaxed); });
			  }
		  }()),
		  fstdistfunc_(other.fstdistfunc_, max_elements_, quantizer_ ? std::optional(quantizer_->Params().alpha_2) : std::nullopt),
		  maxlevel_(other.maxlevel_),
		  enterpoint_node_(other.enterpoint_node_),

		  M_(other.M_),
		  maxM0_(other.maxM0_),
		  mult_(other.mult_),
		  ef_construction_(other.ef_construction_),
		  data_size_(fstdistfunc_.Dim() * sizeof(ValueT)),

		  data_level0_memory_([&] {
			  auto res = static_cast<char*>(malloc(max_elements_ * size_data_per_element_));
			  if (res == nullptr) {
				  throw std::runtime_error("Not enough memory: HNSW copy-constructor failed to allocate level0");
			  }

			  if constexpr (std::is_same_v<ValueT, OtherValueT>) {
				  std::memcpy(res, other.data_level0_memory_, cur_element_count * size_data_per_element_);
			  } else {
				  assertrx(quantizer_);

				  const auto dim = fstdistfunc_.Dim();
				  for (tableint id = 0; id < cur_element_count; ++id) {
					  auto from = other.data_level0_memory_ + id * other.size_data_per_element_;
					  auto to = res + id * size_data_per_element_;

					  // copy element links
					  std::memcpy(to, from, offsetData_);

					  fstdistfunc_.Sq8CorrectiveOffsets()[id] =
						  quantizer_->Quantize(std::span(reinterpret_cast<float*>(from + other.offsetData_), dim),
											   std::span(reinterpret_cast<uint8_t*>(to + offsetData_), dim));

					  // copy element label
					  std::memcpy(to + label_offset_, from + other.label_offset_, sizeof(labeltype));

					  // copy original float vector hash
					  std::memcpy(to + hash_offset_, from + other.hash_offset_, sizeof(uint64_t));
				  }
			  }

			  return res;
		  }()),
		  linkLists_([this] {
			  auto res = static_cast<char**>(malloc(sizeof(void*) * max_elements_));
			  if (res == nullptr) {
				  throw std::runtime_error("Not enough memory: HierarchicalNSW failed to allocate linklists");
			  }
			  return res;
		  }()),
		  label_lookup_(other.label_lookup_),
		  level_generator_(other.level_generator_),
		  update_probability_generator_(other.update_probability_generator_),
		  metric_distance_computations(other.metric_distance_computations.load(std::memory_order_relaxed)),
		  metric_hops(other.metric_hops.load(std::memory_order_relaxed)),
		  allow_replace_deleted_(other.allow_replace_deleted_),
		  concurrent_updates_counter_(other.concurrent_updates_counter_.load(std::memory_order_relaxed)) {
		initTree([&other, this](size_t i) {
			const unsigned linkListSize = other.element_levels_[i] > 0 ? size_links_per_element_ * other.element_levels_[i] : 0;
			if (linkListSize) {
				linkLists_[i] = static_cast<char*>(malloc(linkListSize));
				if (linkLists_[i] == nullptr) {
					throw std::runtime_error("Not enough memory: HNSW copy-constructor failed to allocate linklist");
				}
				std::memcpy(linkLists_[i], other.linkLists_[i], linkListSize);
			} else {
				linkLists_[i] = nullptr;
			}
			return linkListSize;
		});
		assertrx_dbg(num_deleted_ == deleted_elements.size());
	}

	bool IsQuantized() const noexcept override { return static_cast<bool>(quantizer_); }

	auto prepareData(const float* data, float norm = 1.f) const {
		// for restoring original vector length and correct quantization
		norm = 1.f / norm;

		auto deleter = [](const ValueT* ptr) noexcept {
			if constexpr (std::is_same_v<ValueT, uint8_t>) {
				delete[] ptr;
			}
		};

		if constexpr (std::is_same_v<ValueT, uint8_t>) {
			assertrx_dbg(IsQuantized());

			std::unique_ptr<uint8_t[]> res;
			auto from = std::span(data, fstdistfunc_.Dim());
			res = std::make_unique<uint8_t[]>(data_size_ + sizeof(CorrectiveOffset));
			auto to = std::span(res.get(), fstdistfunc_.Dim());
			auto correctiveOffset = quantizer_->Quantize(from | std::views::transform([norm](float val) { return norm * val; }), to);
			std::memcpy(res.get() + data_size_, &correctiveOffset, sizeof(CorrectiveOffset));

			return std::unique_ptr<const ValueT, decltype(deleter)>(res.release(), std::move(deleter));
		} else {
			assertrx_dbg(!IsQuantized());
			return std::unique_ptr<const ValueT, decltype(deleter)>(data, std::move(deleter));
		}
	}

	~HierarchicalNSWImpl() { clear(); }

	void clear() {
		free(data_level0_memory_);
		data_level0_memory_ = nullptr;
		for (tableint i = 0; i < cur_element_count; i++) {
			if (element_levels_[i] > 0) {
				free(linkLists_[i]);
			}
		}
		free(static_cast<void*>(linkLists_));
		linkLists_ = nullptr;
		cur_element_count = 0;
		visited_list_pool_.reset(nullptr);
		deleted_elements.clear();
		num_deleted_ = 0;
		quantizer_.reset();
	}

	size_t ElementSize() const noexcept override { return size_data_per_element_; }

	size_t AllocatedMemSize() const noexcept override {
		size_t ret = 0;
		if (visited_list_pool_) {
			ret += visited_list_pool_->allocatedMemSize();
		}
		ret += data_updates_locks_.allocatedMemSize();
		ret += link_list_locks_.allocatedMemSize();
		if (data_level0_memory_) {
			ret += max_elements_ * size_data_per_element_;
		}
		// TODO: This loop may probably be slow; we could switch it to precalculated value
		for (size_t i = 0; i < cur_element_count; ++i) {
			const unsigned int linkListSize = element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
			ret += linkListSize;
		}
		ret += element_levels_.capacity() * sizeof(int);
		ret += label_lookup_.allocated_mem_size();
		ret += deleted_elements.allocated_mem_size();

		ret += sizeof(HierarchicalNSWImpl);
		return ret;
	}

	struct [[nodiscard]] CompareByFirst {
		constexpr bool operator()(const std::pair<float, tableint>& a, const std::pair<float, tableint>& b) const noexcept {
			return a.first < b.first;
		}
	};

	inline labeltype ExternalLabel(tableint internal_id) const override {
		return reindexer::unaligned::read<labeltype>(data_level0_memory_ + internal_id * size_data_per_element_ + label_offset_);
	}

	uint64_t GetHash(labeltype label) const override {
		auto search = label_lookup_.find(label);
		if (search == label_lookup_.end() || IsMarkedDeleted(search->second)) {
			return emptyVectorHash();
		}
		return getHashByInternalId(search->second);
	}

	inline void setExternalLabel(tableint internal_id, labeltype label) const {
		memcpy((data_level0_memory_ + internal_id * size_data_per_element_ + label_offset_), &label, sizeof(labeltype));
	}

	inline uint64_t getHashByInternalId(tableint internal_id) const {
		return reindexer::unaligned::read<uint64_t>(data_level0_memory_ + internal_id * size_data_per_element_ + hash_offset_);
	}

	inline void setHashByInternalId(tableint internal_id, uint64_t hash) const {
		memcpy((data_level0_memory_ + internal_id * size_data_per_element_ + hash_offset_), &hash, sizeof(hash));
	}

	uint64_t CalcHash(const float* data) const noexcept {
		return reindexer::ConstFloatVectorView(std::span{data, fstdistfunc_.Dim()}).Hash();
	}

	static uint64_t emptyVectorHash() noexcept {
		static const auto hash = reindexer::ConstFloatVectorView{}.Hash();
		return hash;
	}

	inline ValueT* getDataByInternalId(tableint internal_id) const {
		return reinterpret_cast<ValueT*>(data_level0_memory_ + internal_id * size_data_per_element_ + offsetData_);
	}

	template <typename LockerT>
	int getRandomLevel(double reverse_size) RX_REQUIRES(!generator_lock_) {
		using LockGuard = typename LockerT::template LockGuard<GlobalMutextT>;
		std::uniform_real_distribution<double> distribution(0.0, 1.0);
		double val;
		{
			LockGuard lck{generator_lock_};
			val = distribution(level_generator_);
		}
		double r = -log(val) * reverse_size;
		return r;
	}

	size_t MaxElements() const noexcept override { return max_elements_; }

	size_t CurrentElementCount() const noexcept override { return cur_element_count; }

	// *NOT* thread safe
	size_t DeletedCountUnsafe() const noexcept override { return num_deleted_; }

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> searchBaseLayer(
		tableint ep_id, const ValueT* data_point, tableint data_point_id, int layer) {
		static_assert(concurrentUpdates == ExpectConcurrentUpdates::No || isSynchronizationPossible<LockerT>(),
					  "Unable to handle concurrent updates without synchronization");
		using SharedLock =
			typename LockerT::template Concurrent<concurrentUpdates>::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;
		using LockGuard = typename LockerT::template LockGuard<typename LockVecT::MutexT>;

		VisitedList* vl = visited_list_pool_->getFreeVisitedList();
		vl_type* visited_array = vl->mass;
		vl_type visited_array_tag = vl->curV;

		using pair_t = std::pair<float, tableint>;
		std::vector<pair_t> container1, container2;
		container1.reserve(256);
		container2.reserve(256);
		PriorityQueue<pair_t, std::vector<pair_t>, CompareByFirst> top_candidates(CompareByFirst(), std::move(container1));
		PriorityQueue<pair_t, std::vector<pair_t>, CompareByFirst> candidateSet(CompareByFirst(), std::move(container2));

		float lowerBound;
		{
			SharedLock lck{data_updates_locks_[ep_id]};
			if (!IsMarkedDeleted(ep_id)) {
				float dist = fstdistfunc_(data_point, data_point_id, getDataByInternalId(ep_id), ep_id);
				top_candidates.emplace(dist, ep_id);
				lowerBound = dist;
				candidateSet.emplace(-dist, ep_id);
			} else {
				lowerBound = std::numeric_limits<float>::max();
				candidateSet.emplace(-lowerBound, ep_id);
			}
		}
		visited_array[ep_id] = visited_array_tag;

		while (!candidateSet.empty()) {
			std::pair<float, tableint> curr_el_pair = candidateSet.top();
			if ((-curr_el_pair.first) > lowerBound && top_candidates.size() == ef_construction_) {
				break;
			}
			candidateSet.pop();

			tableint curNodeNum = curr_el_pair.second;

			LockGuard lock{link_list_locks_[curNodeNum]};

			const void* linkList0 = layer == 0 ? get_linklist0(curNodeNum) : get_linklist(curNodeNum, layer);
			const size_t size = getListCount(static_cast<const linklistsizeint*>(linkList0));
			const tableint firstNeighbor = size > 0 ? readLinkListNeighbor(linkList0, 0) : tableint(0);
			const tableint secondNeighbor = size > 1 ? readLinkListNeighbor(linkList0, 1) : tableint(0);
#if defined(REINDEXER_WITH_SSE)
			if (size > 0) {
				_mm_prefetch(reinterpret_cast<const char*>(visited_array + firstNeighbor), _MM_HINT_T0);
				if (firstNeighbor + 64 < max_elements_) {
					_mm_prefetch(reinterpret_cast<const char*>(visited_array + firstNeighbor + 64), _MM_HINT_T0);
				}
				_mm_prefetch(reinterpret_cast<char*>(getDataByInternalId(firstNeighbor)), _MM_HINT_T0);
				if (size > 1) {
					_mm_prefetch(reinterpret_cast<char*>(getDataByInternalId(secondNeighbor)), _MM_HINT_T0);
				}
			}
#endif	// defined(REINDEXER_WITH_SSE)

			for (size_t j = 0; j < size; j++) {
				const tableint candidate_id =
					j == 0 ? firstNeighbor : (j == 1 && size > 1) ? secondNeighbor : readLinkListNeighbor(linkList0, j);
//                    if (candidate_id == 0) continue;
#if defined(REINDEXER_WITH_SSE)
				if (j + 1 < size) {
					const tableint nextNeighbor =
						j == 0 && size > 1 ? secondNeighbor : readLinkListNeighbor(linkList0, j + 1);
					_mm_prefetch(reinterpret_cast<const char*>(visited_array + nextNeighbor), _MM_HINT_T0);
					_mm_prefetch(reinterpret_cast<char*>(getDataByInternalId(nextNeighbor)), _MM_HINT_T0);
				}
#endif	// defined(REINDEXER_WITH_SSE)
				if (visited_array[candidate_id] == visited_array_tag) {
					continue;
				}
				visited_array[candidate_id] = visited_array_tag;

				SharedLock lck{data_updates_locks_[candidate_id]};
				float dist1 = fstdistfunc_(data_point, data_point_id, getDataByInternalId(candidate_id), candidate_id);
				if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
					candidateSet.emplace(-dist1, candidate_id);
#if REINDEXER_WITH_SSE
					_mm_prefetch(reinterpret_cast<char*>(getDataByInternalId(candidateSet.top().second)), _MM_HINT_T0);
#endif	// REINDEXER_WITH_SSE

					if (!IsMarkedDeleted(candidate_id)) {
						if (top_candidates.size() < ef_construction_) {
							top_candidates.emplace(dist1, candidate_id);
						} else {
							top_candidates.replace_top(dist1, candidate_id);
						}
					}

					if (!top_candidates.empty()) {
						lowerBound = top_candidates.top().first;
					}
				}
			}
		}
		visited_list_pool_->releaseVisitedList(vl);

		return top_candidates;
	}

	struct [[nodiscard]] Layer0SearchState {
	private:
		struct [[nodiscard]] VisitedListPoolDeleter {
			VisitedListPool* pool = nullptr;
			void operator()(VisitedList* vl) const noexcept {
				if (vl && pool) {
					pool->releaseVisitedList(vl);
				}
			}
		};
		using VisitedListPtr = std::unique_ptr<VisitedList, VisitedListPoolDeleter>;

	public:
		using pair_t = std::pair<float, tableint>;
		using MaxHeapQueue = PriorityQueue<pair_t, std::vector<pair_t>, CompareByFirst>;

		enum class [[nodiscard]] Streaming : int { Disabled, Enabled };
		Streaming streamingSearch = Streaming::Disabled;

		void InitVisitedList(VisitedListPool* pool) { visited = {pool->getFreeVisitedList(), {pool}}; }
		VisitedListPtr visited;
		MaxHeapQueue top_candidates{CompareByFirst(), {}}, top_candidates_extras{CompareByFirst(), {}};
		MaxHeapQueue candidate_set{CompareByFirst(), {}};
		float lowerBound = std::numeric_limits<float>::max();
		size_t ef = 0;
		bool bareBoneSearch = true;
	};

	struct [[nodiscard]] StreamingSearchSessionImpl final : public StreamingSearchSession::Impl {
		StreamingSearchSessionImpl() noexcept = default;
		StreamingSearchSessionImpl(const StreamingSearchSessionImpl&) = delete;
		StreamingSearchSessionImpl& operator=(const StreamingSearchSessionImpl&) = delete;
		StreamingSearchSessionImpl(StreamingSearchSessionImpl&&) noexcept = default;
		StreamingSearchSessionImpl& operator=(StreamingSearchSessionImpl&&) noexcept = default;

	private:
		template <typename, Synchronization>
		friend class HierarchicalNSWImpl;

		const HierarchicalNSWInterface<synchronization>* graph_ = nullptr;
		Layer0SearchState state_;

		using QueryHolderT = std::unique_ptr<const ValueT, void (*)(const ValueT*)>;
		QueryHolderT queryHolder_ = QueryHolderT{nullptr, [](const ValueT*) {}};
		const ValueT* queryData_ = nullptr;
		float normCoef_ = 1.f;
	};

	tableint getLayer0EntryPoint(const ValueT* query_data, float normCoef) const {
		tableint currObj = enterpoint_node_;
		float curdist = normCoef * fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), enterpoint_node_);

		for (int level = maxlevel_; level > 0; level--) {
			bool changed = true;
			while (changed) {
				changed = false;
				const void* linkList0 = get_linklist(currObj, level);
				const int size = getListCount(static_cast<const linklistsizeint*>(linkList0));
				metric_hops++;
				metric_distance_computations += size;

				for (int i = 0; i < size; i++) {
					const tableint cand = readLinkListNeighbor(linkList0, i);
					if (cand >= max_elements_) {
						throw std::runtime_error("cand error");
					}
					float d = normCoef * fstdistfunc_(query_data, getDataByInternalId(cand), cand);
					if (d < curdist) {
						curdist = d;
						currObj = cand;
						changed = true;
					}
				}
			}
		}
		return currObj;
	}

	void initLayer0SearchState(Layer0SearchState& state, tableint ep_id, const ValueT* data_point, float normCoef, size_t ef,
							   bool bareBoneSearch) const {
		state.ef = ef;
		state.bareBoneSearch = bareBoneSearch;
		state.InitVisitedList(visited_list_pool_.get());
		vl_type* visited_array = state.visited->mass;
		const vl_type visited_array_tag = state.visited->curV;

		std::vector<typename Layer0SearchState::pair_t> vec1, vec2, vec3;
		vec1.reserve(ef);
		vec2.reserve(ef);
		state.top_candidates = typename Layer0SearchState::MaxHeapQueue{CompareByFirst(), std::move(vec1)};
		state.candidate_set = typename Layer0SearchState::MaxHeapQueue{CompareByFirst(), std::move(vec2)};
		state.top_candidates_extras = typename Layer0SearchState::MaxHeapQueue{CompareByFirst(), std::move(vec3)};

		if (bareBoneSearch || (!IsMarkedDeleted(ep_id))) {
			auto ep_data = getDataByInternalId(ep_id);
			float dist = normCoef * fstdistfunc_(data_point, ep_data, ep_id);
			state.lowerBound = dist;
			if (state.streamingSearch == Layer0SearchState::Streaming::Disabled) {
				state.top_candidates.emplace(dist, ep_id);
			}
			state.candidate_set.emplace(-dist, ep_id);
		} else {
			state.lowerBound = std::numeric_limits<float>::max();
			state.candidate_set.emplace(-state.lowerBound, ep_id);
		}

		visited_array[ep_id] = visited_array_tag;
	}

	bool layer0ShouldStopBeforePop(const Layer0SearchState& state) const {
		if (state.candidate_set.empty()) {
			return true;
		}
		const float candidate_dist = -state.candidate_set.top().first;
		if (state.bareBoneSearch && state.streamingSearch == Layer0SearchState::Streaming::Disabled) {
			return candidate_dist > state.lowerBound;
		}
		return candidate_dist > state.lowerBound && state.top_candidates.size() >= state.ef;
	}

	template <bool collect_metrics = false>
	void runLayer0Step(Layer0SearchState& state, const ValueT* data_point, float normCoef) const {
		assertrx(!state.candidate_set.empty());

		vl_type* visited_array = state.visited->mass;
		const vl_type visited_array_tag = state.visited->curV;

		std::pair<float, tableint> current_node_pair = state.candidate_set.top();
		state.candidate_set.pop();

		if (state.streamingSearch == Layer0SearchState::Streaming::Enabled) {
			const float dist = -current_node_pair.first;
			const tableint id = current_node_pair.second;
			if (state.bareBoneSearch || !IsMarkedDeleted(id)) {
				if (state.top_candidates.size() < state.ef) {
					state.top_candidates.emplace(dist, id);
				} else if (state.lowerBound > dist) {
					state.top_candidates_extras.push(state.top_candidates.replace_top(dist, id));
				}
				state.lowerBound = state.top_candidates.top().first;
			}
		}

		const tableint current_node_id = current_node_pair.second;
		const void* linkList0 = get_linklist0(current_node_id);
		const size_t size = getListCount(static_cast<const linklistsizeint*>(linkList0));
		if constexpr (collect_metrics) {
			metric_hops++;
			metric_distance_computations += size;
		}

		const tableint firstNeighbor = size > 0 ? readLinkListNeighbor(linkList0, 0) : tableint(0);
		const tableint secondNeighbor = size > 1 ? readLinkListNeighbor(linkList0, 1) : tableint(0);
#if defined(REINDEXER_WITH_SSE)
		if (size > 0) {
			_mm_prefetch(reinterpret_cast<const char*>(visited_array + firstNeighbor), _MM_HINT_T0);
			if (firstNeighbor + 64 < max_elements_) {
				_mm_prefetch(reinterpret_cast<const char*>(visited_array + firstNeighbor + 64), _MM_HINT_T0);
			}
			_mm_prefetch(data_level0_memory_ + firstNeighbor * size_data_per_element_ + offsetData_, _MM_HINT_T0);
			if (size > 1) {
				_mm_prefetch(static_cast<const char*>(linkList0) + 2 * sizeof(int), _MM_HINT_T0);
			}
		}
#endif

		for (size_t j = 1; j <= size; j++) {
			const size_t neighborIdx = j - 1;
			const tableint candidate_id = neighborIdx == 0
												? firstNeighbor
												: (neighborIdx == 1 && size > 1) ? secondNeighbor
																				   : readLinkListNeighbor(linkList0, neighborIdx);
#if defined(REINDEXER_WITH_SSE)
			if (j < size) {
				const tableint nextNeighbor =
					j == 1 && size > 1 ? secondNeighbor : readLinkListNeighbor(linkList0, j);
				_mm_prefetch(reinterpret_cast<const char*>(visited_array + nextNeighbor), _MM_HINT_T0);
				_mm_prefetch(data_level0_memory_ + nextNeighbor * size_data_per_element_ + offsetData_, _MM_HINT_T0);
			}
#endif
			if (!(visited_array[candidate_id] == visited_array_tag)) {
				visited_array[candidate_id] = visited_array_tag;

				const ValueT* currObj1 = getDataByInternalId(candidate_id);
				const float dist = normCoef * fstdistfunc_(data_point, currObj1, candidate_id);

				if (state.streamingSearch == Layer0SearchState::Streaming::Enabled) {
					state.candidate_set.emplace(-dist, candidate_id);
				} else {
					const bool flag_consider_candidate = state.top_candidates.size() < state.ef || state.lowerBound > dist;

					if (flag_consider_candidate) {
						state.candidate_set.emplace(-dist, candidate_id);
#if REINDEXER_WITH_SSE
						_mm_prefetch(data_level0_memory_ + state.candidate_set.top().second * size_data_per_element_, _MM_HINT_T0);
#endif

						if (state.bareBoneSearch || !IsMarkedDeleted(candidate_id)) {
							if (state.top_candidates.size() < state.ef) {
								state.top_candidates.emplace(dist, candidate_id);
							} else {
								state.top_candidates.replace_top(dist, candidate_id);
							}
						}

						if (!state.top_candidates.empty()) {
							state.lowerBound = state.top_candidates.top().first;
						}
					}
				}
			}
		}
	}

	// bare_bone_search means there is no check for deletions and stop condition is ignored in return of extra performance
	template <bool bare_bone_search = true, bool collect_metrics = false>
	PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> searchBaseLayerST(
		tableint ep_id, const ValueT* data_point, float normCoef, size_t ef) const {
		Layer0SearchState state;
		initLayer0SearchState(state, ep_id, data_point, normCoef, ef, bare_bone_search);
		while (!layer0ShouldStopBeforePop(state)) {
			runLayer0Step<collect_metrics>(state, data_point, normCoef);
		}
		return std::move(state.top_candidates);
	}

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	void getNeighborsByHeuristic2(
		PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst>& top_candidates,
		const size_t M) {
		static_assert(concurrentUpdates == ExpectConcurrentUpdates::No || isSynchronizationPossible<LockerT>(),
					  "Unable to handle concurrent updates without synchronization");
		using SharedLock =
			typename LockerT::template Concurrent<concurrentUpdates>::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;
		if (top_candidates.size() < M) {
			return;
		}

		PriorityQueue<std::pair<float, tableint>> queue_closest;
		std::vector<std::pair<float, tableint>> return_list;
		while (top_candidates.size() > 0) {
			queue_closest.emplace(-top_candidates.top().first, top_candidates.top().second);
			top_candidates.pop();
		}

		while (queue_closest.size()) {
			if (return_list.size() >= M) {
				break;
			}
			std::pair<float, tableint> curent_pair = queue_closest.top();
			float dist_to_query = -curent_pair.first;
			queue_closest.pop();
			bool good = true;

			for (std::pair<float, tableint> second_pair : return_list) {
				SharedLock lck1{data_updates_locks_[std::min(curent_pair.second, second_pair.second)]};
				SharedLock lck2{data_updates_locks_[std::max(curent_pair.second, second_pair.second)],
								reindexer::SkipLock(curent_pair.second == second_pair.second)};
				float curdist = fstdistfunc_(getDataByInternalId(second_pair.second), second_pair.second,
											 getDataByInternalId(curent_pair.second), curent_pair.second);
				if (curdist < dist_to_query) {
					good = false;
					break;
				}
			}
			if (good) {
				return_list.push_back(curent_pair);
			}
		}

		for (std::pair<float, tableint> curent_pair : return_list) {
			top_candidates.emplace(-curent_pair.first, curent_pair.second);
		}
	}

	linklistsizeint* get_linklist0(tableint internal_id) const {
		return reinterpret_cast<linklistsizeint*>(data_level0_memory_ + internal_id * size_data_per_element_);
	}

	linklistsizeint* get_linklist0(tableint internal_id, char* data_level0_memory_) const {
		return reinterpret_cast<linklistsizeint*>(data_level0_memory_ + internal_id * size_data_per_element_);
	}

	linklistsizeint* get_linklist(tableint internal_id, int level) const {
		return reinterpret_cast<linklistsizeint*>(linkLists_[internal_id] + (level - 1) * size_links_per_element_);
	}

	linklistsizeint* get_linklist_at_level(tableint internal_id, int level) const {
		return level == 0 ? get_linklist0(internal_id) : get_linklist(internal_id, level);
	}

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	tableint mutuallyConnectNewElement(
		tableint cur_c, PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst>& top_candidates,
		int level, bool isUpdate) {
		static_assert(concurrentUpdates == ExpectConcurrentUpdates::No || isSynchronizationPossible<LockerT>(),
					  "Unable to handle concurrent updates without synchronization");
		using UniqueLock = typename LockerT::template UniqueLock<typename LockVecT::MutexT>;
		using LockGuard = typename LockerT::template LockGuard<typename LockVecT::MutexT>;

		size_t Mcurmax = level ? M_ : maxM0_;
		getNeighborsByHeuristic2<LockerT, concurrentUpdates>(top_candidates, M_);
		if (top_candidates.size() > M_) {
			throw std::runtime_error("Should be not be more than M_ candidates returned by the heuristic");
		}

		std::vector<tableint> selectedNeighbors;
		selectedNeighbors.reserve(M_);
		while (top_candidates.size() > 0) {
			selectedNeighbors.push_back(top_candidates.top().second);
			top_candidates.pop();
		}

		tableint next_closest_entry_point = selectedNeighbors.back();

		{
			// lock only during the update
			// because during the addition the lock for cur_c is already acquired
			UniqueLock lock{link_list_locks_[cur_c], reindexer::SkipLock(!isUpdate)};
			linklistsizeint* ll_cur;
			if (level == 0) {
				ll_cur = get_linklist0(cur_c);
			} else {
				ll_cur = get_linklist(cur_c, level);
			}

			if (getListCount(ll_cur) != 0 && !isUpdate) {
				throw std::runtime_error("The newly inserted element should have blank link list");
			}
			setListCount(ll_cur, selectedNeighbors.size());
			for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
				if (readLinkListNeighbor(ll_cur, idx) != 0 && !isUpdate) {
					throw std::runtime_error("Possible memory corruption");
				}
				if (level > element_levels_[selectedNeighbors[idx]]) {
					throw std::runtime_error("Trying to make a link on a non-existent level");
				}

				writeLinkListNeighbor(ll_cur, idx, selectedNeighbors[idx]);
			}
		}

		for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
			LockGuard lock{link_list_locks_[selectedNeighbors[idx]]};

			linklistsizeint* ll_other;
			if (level == 0) {
				ll_other = get_linklist0(selectedNeighbors[idx]);
			} else {
				ll_other = get_linklist(selectedNeighbors[idx], level);
			}

			size_t sz_link_list_other = getListCount(ll_other);

			if (sz_link_list_other > Mcurmax) {
				throw std::runtime_error("Bad value of sz_link_list_other");
			}
			if (selectedNeighbors[idx] == cur_c) {
				throw std::runtime_error("Trying to connect an element to itself");
			}
			if (level > element_levels_[selectedNeighbors[idx]]) {
				throw std::runtime_error("Trying to make a link on a non-existent level");
			}

			bool is_cur_c_present = false;
			if (isUpdate) {
				for (size_t j = 0; j < sz_link_list_other; j++) {
					if (readLinkListNeighbor(ll_other, j) == cur_c) {
						is_cur_c_present = true;
						break;
					}
				}
			}

			// If cur_c is already present in the neighboring connections of `selectedNeighbors[idx]` then no need to modify any
			// connections or run the heuristics.
			if (!is_cur_c_present) {
				if (sz_link_list_other < Mcurmax) {
					writeLinkListNeighbor(ll_other, sz_link_list_other, cur_c);
					setListCount(ll_other, sz_link_list_other + 1);
				} else {
					using SharedLock =
						typename LockerT::template Concurrent<concurrentUpdates>::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;
					// finding the "weakest" element to replace it with the new one
					const auto id2 = selectedNeighbors[idx];
					float d_max = 0;
					{
						SharedLock lck1{data_updates_locks_[std::min(cur_c, id2)]};
						SharedLock lck2{data_updates_locks_[std::max(cur_c, id2)], reindexer::SkipLock(cur_c == id2)};
						d_max = fstdistfunc_(getDataByInternalId(cur_c), cur_c, getDataByInternalId(id2), id2);
					}
					// Heuristic:
					PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> candidates;
					candidates.emplace(d_max, cur_c);

					for (size_t j = 0; j < sz_link_list_other; j++) {
						const auto id1 = readLinkListNeighbor(ll_other, j);
						SharedLock lck1{data_updates_locks_[std::min(id1, id2)]};
						SharedLock lck2{data_updates_locks_[std::max(id1, id2)], reindexer::SkipLock(id1 == id2)};
						candidates.emplace(fstdistfunc_(getDataByInternalId(id1), id1, getDataByInternalId(id2), id2), id1);
					}

					getNeighborsByHeuristic2<LockerT, concurrentUpdates>(candidates, Mcurmax);

					int indx = 0;
					while (candidates.size() > 0) {
						writeLinkListNeighbor(ll_other, indx, candidates.top().second);
						candidates.pop();
						indx++;
					}

					setListCount(ll_other, indx);
					// Nearest K:
					/*int indx = -1;
					for (int j = 0; j < sz_link_list_other; j++) {
						dist_t d = fstdistfunc_(getDataByInternalId(data[j]), getDataByInternalId(rez[idx]), dist_func_param_);
						if (d > d_max) {
							indx = j;
							d_max = d;
						}
					}
					if (indx >= 0) {
						data[indx] = cur_c;
					} */
				}
			}
		}

		return next_closest_entry_point;
	}

	void ResizeIndex(size_t new_max_elements) override {
		if (new_max_elements < cur_element_count) {
			throw std::runtime_error("Cannot resize, max element is less than the current number of elements");
		}

		visited_list_pool_.reset(new VisitedListPool(1, new_max_elements));

		element_levels_.resize(new_max_elements);

		LockVecT(new_max_elements).swap(link_list_locks_);
		UpdateLockVecT(new_max_elements).swap(data_updates_locks_);

		// Reallocate base layer
		char* data_level0_memory_new = static_cast<char*>(realloc(data_level0_memory_, new_max_elements * size_data_per_element_));
		if (data_level0_memory_new == nullptr) {
			throw std::runtime_error("Not enough memory: resizeIndex failed to allocate base layer");
		}
		std::ignore = std::exchange(data_level0_memory_, data_level0_memory_new);

		// Reallocate all other layers
		char** linkLists_new = static_cast<char**>(realloc(static_cast<void*>(linkLists_), sizeof(void*) * new_max_elements));
		if (linkLists_new == nullptr) {
			throw std::runtime_error("Not enough memory: resizeIndex failed to allocate other layers");
		}
		linkLists_ = linkLists_new;

		max_elements_ = new_max_elements;
		fstdistfunc_.Resize(max_elements_);
	}

	// Read-only concurrency expected
	void SaveIndex(IWriter& writer, const std::atomic_int32_t& cancel) override {
		const size_t dim = fstdistfunc_.Dim();
		constexpr size_t kCancelPeriod = 0x3FFFFF;

		writer.PutVarUInt(uint64_t(max_elements_));
		writer.PutVarUInt(uint64_t(cur_element_count.load()));
		writer.PutVarInt(maxlevel_);
		writer.PutVarUInt(enterpoint_node_);
		writer.PutVarUInt(uint32_t(M_));
		writer.PutVarUInt(uint32_t(ef_construction_));

		std::vector<float> dequantizedDeletedVectorsHolder(dim);
		for (size_t i = 0; i < cur_element_count; ++i) {
			if (((i & kCancelPeriod) == kCancelPeriod) && cancel.load(std::memory_order_relaxed)) {
				throw std::runtime_error("HNSW index saving was canceled");
			}

			static_assert(sizeof(linklistsizeint) == sizeof(tableint), "Expecting equality of those sizes here");
			// Write links and label; skip actual data
			const char* cur_element_ptr = data_level0_memory_ + i * size_data_per_element_;
			const linklistsizeint list0Size = getListCount(reinterpret_cast<const linklistsizeint*>(cur_element_ptr)) + 1;
			const bool isDeleted = isListMarkedDeleted(reinterpret_cast<const linklistsizeint*>(cur_element_ptr));
			for (size_t j = 0; j < list0Size; ++j) {
				writer.PutVarUInt(reindexer::unaligned::read<tableint>(cur_element_ptr + j * sizeof(tableint)));
			}
			if (isDeleted) {
				auto data = cur_element_ptr + offsetData_;
				// We have to store full vector for the deleted items
				if (quantizer_) {
					const auto from = std::span{reinterpret_cast<const uint8_t*>(data), dim};
					const auto to = std::span{dequantizedDeletedVectorsHolder};
					std::transform(from.begin(), from.end(), to.begin(), [&](uint8_t val) noexcept { return quantizer_->Dequantize(val); });
					writer.PutVString(std::string_view(reinterpret_cast<const char*>(to.data()), dim * sizeof(float)));
				} else {
					writer.PutVString(std::string_view(data, dim * sizeof(float)));
				}
			} else {
				labeltype label = reindexer::unaligned::read<labeltype>(cur_element_ptr + label_offset_);
				writer.AppendPKByID(label);
			}
		}

		for (size_t i = 0; i < cur_element_count; ++i) {
			if (((i & kCancelPeriod) == kCancelPeriod) && cancel.load(std::memory_order_relaxed)) {
				throw std::runtime_error("HNSW index saving was canceled");
			}

			const unsigned int linkListSize = element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
			writer.PutVString(std::string_view(linkLists_[i], linkListSize));
		}
	}

	template <typename F>
	void initTree(F fillLinkList) {
		for (size_t i = 0; i < cur_element_count; i++) {
			if (HierarchicalNSWImpl::IsMarkedDeleted(i)) {
				num_deleted_ += 1;
				if (allow_replace_deleted_) {
					assertrx_dbg(deleted_elements.count(i) == 0);
					deleted_elements.insert(i);
				}
			} else {
				// Do not store deleted lables in label_lookup_
				label_lookup_[HierarchicalNSWImpl::ExternalLabel(i)] = i;
			}
			const unsigned linkListSize = fillLinkList(i);
			element_levels_[i] = linkListSize / size_links_per_element_;
		}
	}

	tableint getInternalIdByLabel(labeltype label) const {
		auto search = label_lookup_.find(label);
		const bool found = (search != label_lookup_.end());
		if (!found || IsMarkedDeleted(search->second)) {
			using namespace reindexer;
			assertf_dbg(false, "getLabel: Label not found: {}", label);
			throw std::runtime_error("getLabel: Label not found: " + std::to_string(label));
		}
		return search->second;
	}

	const float* FloatPtrByExternalLabel(labeltype label) const override {
		assertrx(!IsQuantized());
		return reinterpret_cast<const float*>(getDataByInternalId(getInternalIdByLabel(label)));
	}

	/*
	 * Marks an element with the given label deleted, does NOT really change the current graph.
	 */
	// *NOT* thread-safe
	void MarkDelete(labeltype label) override {
		auto search = label_lookup_.find(label);
		if (search == label_lookup_.end()) {
			using namespace reindexer;
			assertf_dbg(false, "markDelete: Label not found:  {}", label);
			throw std::runtime_error("markDelete: Label not found: " + std::to_string(label));
		}
		tableint internalId = search->second;

		markDeletedInternal(internalId);
		if (allow_replace_deleted_) {
			label_lookup_.erase(search);
		}
	}

	/*
	 * Uses the last 16 bits of the memory for the linked list size to store the mark,
	 * whereas maxM0_ has to be limited to the lower 16 bits, however, still large enough in almost all cases.
	 */
	// *NOT* thread-safe
	void markDeletedInternal(tableint internalId) {
		assert(internalId < cur_element_count);
		if (!IsMarkedDeleted(internalId)) {
			unsigned char* ll_cur = reinterpret_cast<unsigned char*>(get_linklist0(internalId)) + 2;
			*ll_cur |= DELETE_MARK;
			num_deleted_ += 1;
			if (allow_replace_deleted_) {
				deleted_elements.insert(internalId);
				assertrx_dbg(num_deleted_ == deleted_elements.size());
			}
		} else {
			assertrx_dbg(false);
			throw std::runtime_error("The requested to delete element is already deleted");
		}
	}

	/*
	 * Remove the deleted mark of the node.
	 */
	void unmarkDeletedInternal(tableint internalId) RX_REQUIRES(!deleted_elements_lock) {
		assert(internalId < cur_element_count);
		if (IsMarkedDeleted(internalId)) {
			unsigned char* ll_cur = reinterpret_cast<unsigned char*>(get_linklist0(internalId)) + 2;
			*ll_cur &= ~DELETE_MARK;
			if (allow_replace_deleted_) {
				reindexer::unique_lock lock_deleted_elements(deleted_elements_lock);
				if (deleted_elements.erase(internalId)) {
					num_deleted_ -= 1;
				}
				assertrx_dbg(num_deleted_ == deleted_elements.size());
			} else {
				reindexer::unique_lock lock_deleted_elements(deleted_elements_lock);
				num_deleted_ -= 1;
			}
		} else {
			assertrx_dbg(false);
			throw std::runtime_error("The requested to undelete element is not deleted");
		}
	}

	/*
	 * Checks the first 16 bits of the memory to see if the element is marked deleted.
	 */
	bool isListMarkedDeleted(const linklistsizeint* linkList0) const noexcept {
		const unsigned char* ll_cur = reinterpret_cast<const unsigned char*>(linkList0) + 2;
		return *ll_cur & DELETE_MARK;
	}
	bool IsMarkedDeleted(tableint internalId) const noexcept override { return isListMarkedDeleted(get_linklist0(internalId)); }

	unsigned short int getListCount(const linklistsizeint* ptr) const { return reindexer::unaligned::read<unsigned short int>(ptr); }

	void setListCount(linklistsizeint* ptr, unsigned short int size) const { std::memcpy(ptr, &size, sizeof(size)); }

	consteval static ExpectConcurrentUpdates isConcurrentUpdatesAllowedByIdx() noexcept {
		return synchronization == Synchronization::None ? ExpectConcurrentUpdates::No : ExpectConcurrentUpdates::Yes;
	}
	template <typename LockerT>
	consteval static bool isSynchronizationPossible() noexcept {
		return synchronization == Synchronization::OnInsertions || std::is_same_v<LockerT, RegularLocker>;
	}

	/*
	 * Adds point. Updates the point if it is already in the index.
	 * If replacement of deleted elements is enabled: replaces previously deleted point if any, updating it with new point
	 */
	void AddPointNoLock(const float* data_point, labeltype label) override
		RX_REQUIRES(!deleted_elements_lock, !label_lookup_lock, !global, !enterpoint_lock_, !generator_lock_, !update_generator_lock_) {
		addPoint<DummyLocker>(data_point, label);
	}
	void AddPointConcurrent(const float* data_point, labeltype label) override
		RX_REQUIRES(!deleted_elements_lock, !label_lookup_lock, !global, !enterpoint_lock_, !generator_lock_, !update_generator_lock_) {
		if constexpr (synchronization == Synchronization::None) {
			throw std::logic_error("This HNSW index does not support concurrent insertions");
		} else {
			addPoint<RegularLocker>(data_point, label);
		}
	}

	template <typename LockerT>
	void addPoint(const float* data_point, labeltype label)
		RX_REQUIRES(!deleted_elements_lock, !label_lookup_lock, !global, !enterpoint_lock_, !generator_lock_, !update_generator_lock_) {
		using UniqueLock = typename LockerT::template UniqueLock<GlobalMutextT>;
		using LockGuard = typename LockerT::template LockGuard<GlobalMutextT>;
		if (!allow_replace_deleted_) {
			addPoint<LockerT, ExpectConcurrentUpdates::No>(data_point, label, -1);
			return;
		}
		// check if there is vacant place
		tableint internal_id_replaced = 0;
		reindexer::CounterGuardAIR32 updatesCounter;
		bool is_vacant_place;
		{
			UniqueLock lock_deleted_elements{deleted_elements_lock};
			is_vacant_place = !deleted_elements.empty();
			if (is_vacant_place) {
				internal_id_replaced = *deleted_elements.begin();
				deleted_elements.erase(deleted_elements.begin());
				num_deleted_ -= 1;
				updatesCounter = reindexer::CounterGuardAIR32(concurrent_updates_counter_);
			}
		}

		// if there is no vacant place then add or update point
		// else add point to vacant place
		if (!is_vacant_place) {
			if constexpr (!isSynchronizationPossible<LockerT>()) {
				// No support for concurrent updates
				assertrx_dbg(concurrent_updates_counter_.load(std::memory_order_acquire) == 0);
				addPoint<LockerT, ExpectConcurrentUpdates::No>(data_point, label, -1);
			} else {
				if (concurrent_updates_counter_.load(std::memory_order_acquire)) {
					// Perform parallel updates.
					// Call version with additional data synchronization.
					addPoint<LockerT, ExpectConcurrentUpdates::Yes>(data_point, label, -1);
				} else {
					// No concurrent updates running. Due to higher level logic, new updates will never appear during the insert cycle.
					// Call unsafe version without additional data synchronization.
					addPoint<LockerT, ExpectConcurrentUpdates::No>(data_point, label, -1);
				}
			}
		} else {
			setExternalLabel(internal_id_replaced, label);

			{
				LockGuard lock_table{label_lookup_lock};
				label_lookup_[label] = internal_id_replaced;
			}

			try {
				updatePoint<LockerT>(data_point, internal_id_replaced, 1.0);
			} catch (...) {
				assertrx_dbg(false);

				LockGuard lock_deleted_elements{deleted_elements_lock};
				if (!IsMarkedDeleted(internal_id_replaced)) {
					markDeletedInternal(internal_id_replaced);
					LockGuard lock_table{label_lookup_lock};
					label_lookup_.erase(label);
				} else {
					// Handling direct deleted_elements.erase called above (even if element is marked as 'deleted'
					auto [_, inserted] = deleted_elements.emplace(internal_id_replaced);
					num_deleted_ += int(inserted);
				}

				throw;
			}
		}
	}

	template <typename LockerT>
	void updatePoint(const float* dataPointRaw, tableint internalId, float updateNeighborProbability)
		RX_REQUIRES(!enterpoint_lock_, !update_generator_lock_, !deleted_elements_lock) {
		using LockGuard = typename LockerT::template LockGuard<GlobalMutextT>;
		using UpdateLockGuard = typename LockerT::template LockGuard<typename UpdateLockVecT::MutexT>;
		using SharedLock = typename LockerT::template SharedLock<typename UpdateLockVecT::MutexT>;
		const auto dataPointHolder = prepareData(dataPointRaw);
		const ValueT* dataPoint = dataPointHolder.get();
		{
			// FIXME: There is still logical race, that reduces recall rate - we may update one of the current insertion candidate nodes
			// right after dist calculation Check #2029 for details.
			UpdateLockGuard lck{data_updates_locks_[internalId]};
			// update the feature vector associated with existing point with new vector
			memcpy(getDataByInternalId(internalId), dataPoint, data_size_);
			setHashByInternalId(internalId, CalcHash(dataPointRaw));
			if (quantizer_) {
				quantizer_->UpdateStatistic(dataPointRaw);
				auto correctiveOffsetsPtr =
					reinterpret_cast<const CorrectiveOffset*>(reinterpret_cast<const uint8_t*>(dataPoint) + data_size_);
				std::memmove(&fstdistfunc_.Sq8CorrectiveOffsets()[internalId], correctiveOffsetsPtr, sizeof(CorrectiveOffset));
			}
			fstdistfunc_.AddNorm(dataPointRaw, internalId);
			if (IsMarkedDeleted(internalId)) {
				unmarkDeletedInternal(internalId);
			}
		}

		int maxLevelCopy;
		tableint entryPointCopy;
		{
			SharedLock lck{enterpoint_lock_};
			maxLevelCopy = maxlevel_;
			entryPointCopy = enterpoint_node_;
		}
		// If point to be updated is entry point and graph just contains single element then just return.
		if (entryPointCopy == internalId && cur_element_count == 1) {
			return;
		}

		int elemLevel = element_levels_[internalId];
		std::uniform_real_distribution<float> distribution(0.0, 1.0);

		for (int layer = 0; layer <= elemLevel; layer++) {
			reindexer::fast_hash_set<tableint> sCand;
			reindexer::fast_hash_set<tableint> sNeigh;
			std::vector<tableint> listOneHop = getConnectionsWithLock<LockerT>(internalId, layer);
			if (listOneHop.size() == 0) {
				continue;
			}

			sCand.insert(internalId);

			for (auto&& elOneHop : listOneHop) {
				sCand.insert(elOneHop);

				if (!reindexer::fp::EqualWithinULPs(updateNeighborProbability, 1.0f)) {
					assertrx_dbg(false);  // We do not use this logic
					LockGuard lck{update_generator_lock_};
					if (distribution(update_probability_generator_) > updateNeighborProbability) {
						continue;
					}
				}

				sNeigh.insert(elOneHop);

				std::vector<tableint> listTwoHop = getConnectionsWithLock<LockerT>(elOneHop, layer);
				for (auto&& elTwoHop : listTwoHop) {
					sCand.insert(elTwoHop);
				}
			}

			for (auto&& neigh : sNeigh) {
				// if (neigh == internalId)
				//     continue;

				PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> candidates;
				size_t size = sCand.find(neigh) == sCand.end() ? sCand.size() : sCand.size() - 1;  // sCand guaranteed to have size >= 1
				size_t elementsToKeep = std::min(ef_construction_, size);
				for (auto&& cand : sCand) {
					if (cand == neigh) {
						continue;
					}

					SharedLock lck1{data_updates_locks_[std::min(neigh, cand)]};
					assertrx_dbg(neigh != cand);  // Essential for the second lock
					SharedLock lck2{data_updates_locks_[std::max(neigh, cand)]};
					float distance = fstdistfunc_(getDataByInternalId(neigh), neigh, getDataByInternalId(cand), cand);
					if (candidates.size() < elementsToKeep) {
						candidates.emplace(distance, cand);
					} else {
						if (distance < candidates.top().first) {
							candidates.pop();
							candidates.emplace(distance, cand);
						}
					}
				}

				// Retrieve neighbours using heuristic and set connections.
				getNeighborsByHeuristic2<LockerT, isConcurrentUpdatesAllowedByIdx()>(candidates, layer == 0 ? maxM0_ : M_);

				{
					LockGuard lock{link_list_locks_[neigh]};
					linklistsizeint* ll_cur;
					ll_cur = get_linklist_at_level(neigh, layer);
					size_t candSize = candidates.size();
					setListCount(ll_cur, candSize);
					for (size_t idx = 0; idx < candSize; idx++) {
						writeLinkListNeighbor(ll_cur, idx, candidates.top().second);
						candidates.pop();
					}
				}
			}
		}

		repairConnectionsForUpdate<LockerT>(dataPoint, entryPointCopy, internalId, elemLevel, maxLevelCopy);
	}

	template <typename LockerT>
	void repairConnectionsForUpdate(const ValueT* dataPoint, tableint entryPointInternalId, tableint dataPointInternalId,
									int dataPointLevel, int maxLevel) {
		using LockGuard = typename LockerT::template LockGuard<typename LockVecT::MutexT>;
		using SharedLock = typename LockerT::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;
		tableint currObj = entryPointInternalId;
		if (dataPointLevel < maxLevel) {
			float curdist;
			{
				SharedLock lck{data_updates_locks_[currObj]};
				curdist = fstdistfunc_(dataPoint, dataPointInternalId, getDataByInternalId(currObj), currObj);
			}
			for (int level = maxLevel; level > dataPointLevel; level--) {
				bool changed = true;
				while (changed) {
					changed = false;
					LockGuard lock{link_list_locks_[currObj]};
					const void* linkList0 = get_linklist_at_level(currObj, level);
					const int size = getListCount(static_cast<const linklistsizeint*>(linkList0));
					const tableint firstNeighbor = size > 0 ? readLinkListNeighbor(linkList0, 0) : tableint(0);
					const tableint secondNeighbor = size > 1 ? readLinkListNeighbor(linkList0, 1) : tableint(0);
#if REINDEXER_WITH_SSE
					if (size > 0) {
						_mm_prefetch(reinterpret_cast<char*>(getDataByInternalId(firstNeighbor)), _MM_HINT_T0);
					}
#endif	// REINDEXER_WITH_SSE
					for (int i = 0; i < size; i++) {
#if defined(REINDEXER_WITH_SSE)
						if (i + 1 < size) {
							const tableint nextNeighbor =
								i == 0 && size > 1 ? secondNeighbor : readLinkListNeighbor(linkList0, i + 1);
							_mm_prefetch(reinterpret_cast<char*>(getDataByInternalId(nextNeighbor)), _MM_HINT_T0);
						}
#endif	// defined(REINDEXER_WITH_SSE)
						const tableint cand = i == 0 ? firstNeighbor
													 : (i == 1 && size > 1) ? secondNeighbor
																			  : readLinkListNeighbor(linkList0, i);

						float d;
						{
							SharedLock lck{data_updates_locks_[cand]};
							d = fstdistfunc_(dataPoint, dataPointInternalId, getDataByInternalId(cand), cand);
						}
						if (d < curdist) {
							curdist = d;
							currObj = cand;
							changed = true;
						}
					}
				}
			}
		}

		if (dataPointLevel > maxLevel) {
			throw std::runtime_error("Level of item to be updated cannot be bigger than max level");
		}

		for (int level = dataPointLevel; level >= 0; level--) {
			PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> topCandidates =
				searchBaseLayer<LockerT, isConcurrentUpdatesAllowedByIdx()>(currObj, dataPoint, dataPointInternalId, level);

			PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> filteredTopCandidates;
			while (topCandidates.size() > 0) {
				if (topCandidates.top().second != dataPointInternalId) {
					filteredTopCandidates.push(topCandidates.top());
				}

				topCandidates.pop();
			}

			// Since element_levels_ is being used to get `dataPointLevel`, there could be cases where `topCandidates` could just
			// contain entry point itself. To prevent self loops, the `topCandidates` is filtered and thus can be empty.
			if (filteredTopCandidates.size() > 0) {
				{
					SharedLock lck{data_updates_locks_[entryPointInternalId]};
					bool epDeleted = IsMarkedDeleted(entryPointInternalId);
					if (epDeleted) {
						filteredTopCandidates.emplace(
							fstdistfunc_(dataPoint, dataPointInternalId, getDataByInternalId(entryPointInternalId), entryPointInternalId),
							entryPointInternalId);
						if (filteredTopCandidates.size() > ef_construction_) {
							filteredTopCandidates.pop();
						}
					}
				}

				currObj = mutuallyConnectNewElement<LockerT, isConcurrentUpdatesAllowedByIdx()>(dataPointInternalId, filteredTopCandidates,
																								level, true);
			}
		}
	}

	template <typename LockerT>
	std::vector<tableint> getConnectionsWithLock(tableint internalId, int level) {
		using LockGuard = typename LockerT::template LockGuard<typename LockVecT::MutexT>;
		LockGuard lock{link_list_locks_[internalId]};
		const void* linkList0 = get_linklist_at_level(internalId, level);
		const int size = getListCount(static_cast<const linklistsizeint*>(linkList0));
		std::vector<tableint> result(size);
		if (size > 0) {
			std::memcpy(result.data(), static_cast<const char*>(linkList0) + sizeof(linklistsizeint), size * sizeof(tableint));
		}
		return result;
	}

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	tableint addPoint(const float* data_point_raw, labeltype label, int level)
		RX_REQUIRES(!label_lookup_lock, !enterpoint_lock_, !global, !generator_lock_, !update_generator_lock_, !deleted_elements_lock) {
		static_assert(concurrentUpdates == ExpectConcurrentUpdates::No || isSynchronizationPossible<LockerT>(),
					  "Unable to handle concurrent updates without synchronization");
		using UniqueLock = typename LockerT::template UniqueLock<GlobalMutextT>;
		using DataLockGuard = typename LockerT::template LockGuard<typename LockVecT::MutexT>;
		using UpdateLockGuard = typename LockerT::template LockGuard<typename UpdateOpsMutexLocks::MutexT>;
		using SharedLock = typename LockerT::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;
		using DataUpdatesSharedLock =
			typename LockerT::template Concurrent<concurrentUpdates>::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;

		tableint cur_c = 0;
		{
			// Checking if the element with the same label already exists
			// if so, updating it *instead* of creating a new element.
			UniqueLock lock_table{label_lookup_lock};
			auto search = label_lookup_.find(label);
			if (search != label_lookup_.end()) {
				tableint existingInternalId = search->second;
				if (allow_replace_deleted_) {
					if (IsMarkedDeleted(existingInternalId)) {
						throw std::runtime_error(
							"Can't use addPoint to update deleted elements if replacement of deleted elements is enabled.");
					}
				}
				reindexer::CounterGuardAIR32 updatesCounter(concurrent_updates_counter_);
				assertrx_dbg(false);  // This logic was never be called and may require extra testing
				lock_table.unlock();

				updatePoint<LockerT>(data_point_raw, existingInternalId, 1.0);

				return existingInternalId;
			}

			if (cur_element_count >= max_elements_) {
				throw std::runtime_error("The number of elements exceeds the specified limit");
			}

			cur_c = cur_element_count;
			cur_element_count++;
			label_lookup_[label] = cur_c;
			fstdistfunc_.AddNorm(data_point_raw, cur_c);
		}

		const int curlevel = (level > 0) ? level : getRandomLevel<LockerT>(mult_);

		UniqueLock templock{global};
		DataLockGuard lock_el{link_list_locks_[cur_c]};
		element_levels_[cur_c] = curlevel;

		int maxlevelcopy;
		tableint enterpoint_copy;
		{
			SharedLock lck{enterpoint_lock_};
			maxlevelcopy = maxlevel_;
			enterpoint_copy = enterpoint_node_;
			if (curlevel <= maxlevelcopy && enterpoint_copy != std::numeric_limits<tableint>::max()) {
				lck.unlock();
				templock.unlock();
			}
		}

		tableint currObj = enterpoint_copy;

		memset(data_level0_memory_ + cur_c * size_data_per_element_, 0, size_data_per_element_);

		// Initialisation of the data and label
		setExternalLabel(cur_c, label);
		setHashByInternalId(cur_c, CalcHash(data_point_raw));
		const auto dataPointHolder = prepareData(data_point_raw);
		const ValueT* data_point = dataPointHolder.get();
		memcpy(getDataByInternalId(cur_c), data_point, data_size_);
		if (quantizer_) {
			quantizer_->UpdateStatistic(data_point_raw);
			auto correctiveOffsetsPtr = reinterpret_cast<const uint8_t*>(data_point) + data_size_;
			fstdistfunc_.Sq8CorrectiveOffsets()[cur_c] = reindexer::unaligned::read<CorrectiveOffset>(correctiveOffsetsPtr);
		}
		if (curlevel) {
			linkLists_[cur_c] = static_cast<char*>(malloc(size_links_per_element_ * curlevel + 1));
			if (linkLists_[cur_c] == nullptr) {
				throw std::runtime_error("Not enough memory: addPoint failed to allocate linklist");
			}
			memset(linkLists_[cur_c], 0, size_links_per_element_ * curlevel + 1);
		}

		if (currObj != std::numeric_limits<tableint>::max()) {
			if (curlevel < maxlevelcopy) {
				float curdist;
				{
					DataUpdatesSharedLock lck{data_updates_locks_[currObj]};
					curdist = fstdistfunc_(data_point, cur_c, getDataByInternalId(currObj), currObj);
				}
				for (int level = maxlevelcopy; level > curlevel; level--) {
					bool changed = true;
					while (changed) {
						changed = false;
						DataLockGuard lock{link_list_locks_[currObj]};
						const void* linkList0 = get_linklist(currObj, level);
						const int size = getListCount(static_cast<const linklistsizeint*>(linkList0));

						for (int i = 0; i < size; i++) {
							const tableint cand = readLinkListNeighbor(linkList0, i);
							if (cand >= max_elements_) {
								throw std::runtime_error("cand error");
							}

							float d;
							DataUpdatesSharedLock lck{data_updates_locks_[cand]};
							d = fstdistfunc_(data_point, cur_c, getDataByInternalId(cand), cand);
							if (d < curdist) {
								curdist = d;
								currObj = cand;
								changed = true;
							}
						}
					}
				}
			}

			for (int level = std::min(curlevel, maxlevelcopy); level >= 0; level--) {
				if (level > maxlevelcopy || level < 0) {  // possible?
					throw std::runtime_error("Level error");
				}

				PriorityQueue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> top_candidates =
					searchBaseLayer<LockerT, concurrentUpdates>(currObj, data_point, cur_c, level);

				DataUpdatesSharedLock lck{data_updates_locks_[enterpoint_copy]};
				bool epDeleted = IsMarkedDeleted(enterpoint_copy);
				if (epDeleted) {
					const float dist = fstdistfunc_(data_point, cur_c, getDataByInternalId(enterpoint_copy), enterpoint_copy);
					if (top_candidates.size() < ef_construction_) {
						top_candidates.emplace(dist, enterpoint_copy);
					} else if (top_candidates.top().first > dist) {
						top_candidates.replace_top(dist, enterpoint_copy);
					}
				}
				currObj = mutuallyConnectNewElement<LockerT, concurrentUpdates>(cur_c, top_candidates, level, false);
			}

			if (curlevel > maxlevelcopy) {
				UpdateLockGuard lck{enterpoint_lock_};
				enterpoint_node_ = cur_c;
				maxlevel_ = curlevel;
			}
		} else {
			UpdateLockGuard lck{enterpoint_lock_};
			// Do nothing for the first element
			maxlevel_ = curlevel;
			if (curlevel > maxlevelcopy) {
				enterpoint_node_ = cur_c;
			} else {
				enterpoint_node_ = 0;
			}
		}

		return cur_c;
	}

	// returns norm coefficient for query vector
	float queryNormCoef(std::optional<float> query_data_norm) const {
		const auto isCosineMetricAndQuantized = IsQuantized() && fstdistfunc_.Metric() == reindexer::VectorMetric::Cosine;
		if (isCosineMetricAndQuantized && !query_data_norm) {
			throw std::runtime_error("Norm is required for Cosine-metric during corrective offsets calculation in quantized graph");
		}

		// NOLINTNEXTLINE(bugprone-unchecked-optional-access) false-positive. Check was made above
		return isCosineMetricAndQuantized ? 1.f / *query_data_norm : 1.f;
	}

	StreamingSearchSession BeginStreamingSearch(const float* query_data_raw, std::optional<float> query_data_norm,
												StreamingSearchOptions opts) const override {
		static constexpr size_t kDefaultStreamingEf = 100;

		StreamingSearchSessionImpl sessionImpl;
		auto& state = sessionImpl.state_;
		auto& queryHolder = sessionImpl.queryHolder_;
		auto& queryData = sessionImpl.queryData_;
		auto& normCoef = sessionImpl.normCoef_;

		sessionImpl.graph_ = this;
		state.streamingSearch = Layer0SearchState::Streaming::Enabled;

		if (cur_element_count == 0) {
			return StreamingSearchSession(std::move(sessionImpl));
		}

		normCoef = queryNormCoef(query_data_norm);
		queryHolder = prepareData(query_data_raw, normCoef);
		queryData = queryHolder.get();

		const size_t ef = (opts.ef != 0) ? opts.ef : kDefaultStreamingEf;
		const tableint entryPoint = getLayer0EntryPoint(queryData, normCoef);
		const bool bareBone = !num_deleted_;
		initLayer0SearchState(state, entryPoint, queryData, normCoef, ef, bareBone);

		return StreamingSearchSession(std::move(sessionImpl));
	}

	void mergeExtrasIntoTopCandidates(Layer0SearchState& state) const {
		if (state.top_candidates.size() >= state.ef || state.top_candidates_extras.empty()) {
			return;
		}

		if (!state.top_candidates.empty()) {
			auto topExtras = std::move(state.top_candidates_extras);

			const size_t needFill = state.ef - state.top_candidates.size();
			size_t extraDelta = topExtras.size() > needFill ? topExtras.size() - needFill : 0;
			while (extraDelta-- > 0) {
				state.top_candidates_extras.push(topExtras.top());
				topExtras.pop();
			}

			while (!topExtras.empty() && state.top_candidates.size() < state.ef) {
				state.top_candidates.push(topExtras.top());
				topExtras.pop();
			}
			assertrx(topExtras.empty());
		} else {
			state.top_candidates = std::move(state.top_candidates_extras);
			while (state.top_candidates.size() > state.ef) {
				state.top_candidates_extras.push(state.top_candidates.top());
				state.top_candidates.pop();
			}
		}

		if (!state.top_candidates.empty()) {
			state.lowerBound = state.top_candidates.top().first;
		}
	}

	void emitStreamingBatch(Layer0SearchState& state, StreamingBatch& batch, size_t batchSize) const {
		std::vector<std::pair<float, labeltype>> vec;
		vec.reserve(batchSize);
		batch.results = SearchResultQueue(std::less<std::pair<float, labeltype>>(), std::move(vec));

		auto topCandidates = std::move(state.top_candidates);
		while (topCandidates.size() > batchSize) {
			state.top_candidates.push(topCandidates.top());
			topCandidates.pop();
		}

		while (topCandidates.size() > 0) {
			auto [dist, id] = topCandidates.top();
			batch.results.emplace(dist, ExternalLabel(id));
			topCandidates.pop();
		}
	}

	StreamingBatch ContinueStreamingSearch(StreamingSearchSession& session, size_t batchSize) const override {
		assertrx(session.impl_);
		auto& sessionImpl = static_cast<StreamingSearchSessionImpl&>(*session.impl_);
		auto& state = sessionImpl.state_;
		const auto queryData = sessionImpl.queryData_;
		const auto normCoef = sessionImpl.normCoef_;

		StreamingBatch batch;
		if (sessionImpl.graph_ != this) {
			batch.exhausted = true;
			return batch;
		}
		if (batchSize == 0) {
			return batch;
		}

		const size_t origStateEf = state.ef;
		state.ef = std::max(state.ef, batchSize);

		mergeExtrasIntoTopCandidates(state);

		while (!layer0ShouldStopBeforePop(state)) {
			runLayer0Step(state, queryData, normCoef);
		}

		state.ef = origStateEf;
		emitStreamingBatch(state, batch, batchSize);

		batch.exhausted = state.candidate_set.empty() && state.top_candidates.empty() && state.top_candidates_extras.empty();
		return batch;
	}

	// Read-only concurrency expected
	auto search(const float* query_data_raw, std::optional<float> query_data_norm, size_t ef) const {
		const float normCoef = queryNormCoef(query_data_norm);
		auto query_data_holder = prepareData(query_data_raw, normCoef);
		const tableint currObj = getLayer0EntryPoint(query_data_holder.get(), normCoef);
		auto method = num_deleted_ == 0 ? &HierarchicalNSWImpl::searchBaseLayerST<true> : &HierarchicalNSWImpl::searchBaseLayerST<false>;
		auto top_candidates = (this->*method)(currObj, query_data_holder.get(), normCoef, ef);
		return std::make_tuple(std::move(top_candidates), std::move(query_data_holder), normCoef);
	}

	// Read-only concurrency expected
	SearchResultQueue SearchKnn(const float* query_data_raw, std::optional<float> query_data_norm, size_t k, size_t ef = 0) const override {
		if (cur_element_count == 0) {
			return SearchResultQueue();
		}

		k = std::min<size_t>(k, cur_element_count);

		ef = ef ? ef : k * 3 / 2;

		auto [top_candidates, query_data_holder, normCoef] = search(query_data_raw, query_data_norm, ef);
		while (top_candidates.size() > k) {
			top_candidates.pop();
		}

		std::vector<std::pair<float, labeltype>> container;
		container.reserve(top_candidates.size());
		SearchResultQueue result(std::less<std::pair<float, labeltype>>(), std::move(container));

		while (top_candidates.size() > 0) {
			std::pair<float, tableint> rez = top_candidates.top();
			result.push(std::pair<float, labeltype>(rez.first, ExternalLabel(rez.second)));
			top_candidates.pop();
		}
		return result;
	}

	// Read-only concurrency expected
	SearchResultQueue SearchRange(const float* query_data_raw, std::optional<float> query_data_norm, float radius,
								  size_t ef) const override {
		if (cur_element_count == 0) {
			return SearchResultQueue();
		}

		auto [top_candidates, query_data_holder, normCoef] = search(query_data_raw, query_data_norm, ef);
		auto query_data = query_data_holder.get();

		std::vector<std::pair<float, labeltype>> container;
		container.reserve(top_candidates.size());
		SearchResultQueue result(std::less<std::pair<float, labeltype>>(), std::move(container));

		VisitedList* vl = visited_list_pool_->getFreeVisitedList();
		vl_type* visited_array = vl->mass;
		vl_type visited_array_tag = vl->curV;

		std::queue<std::pair<float, tableint>> radius_queue;
		while (!top_candidates.empty()) {
			const auto& cand = top_candidates.top();
			if (cand.first < radius) {
				radius_queue.emplace(cand);
				result.emplace(cand.first, ExternalLabel(cand.second));
			}
			visited_array[cand.second] = visited_array_tag;
			top_candidates.pop();
		}

		while (!radius_queue.empty()) {
			auto cur = radius_queue.front();
			radius_queue.pop();

			tableint current_id = cur.second;
			const void* linkList0 = get_linklist0(current_id);
			const size_t size = getListCount(static_cast<const linklistsizeint*>(linkList0));

			for (size_t j = 1; j <= size; j++) {
				const tableint candidate_id = readLinkListNeighbor(linkList0, j - 1);
				if (IsMarkedDeleted(candidate_id)) {
					continue;
				}
				if (visited_array[candidate_id] != visited_array_tag) {
					visited_array[candidate_id] = visited_array_tag;
					float dist = normCoef * fstdistfunc_(query_data, getDataByInternalId(candidate_id), candidate_id);
					if (dist < radius) {
						radius_queue.emplace(dist, candidate_id);
						result.emplace(dist, ExternalLabel(candidate_id));
					}
				}
			}
		}

		visited_list_pool_->releaseVisitedList(vl);

		return result;
	}
};

}  // namespace hnswlib
