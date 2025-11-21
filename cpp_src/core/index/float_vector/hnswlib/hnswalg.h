// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include <assert.h>
#include <stdlib.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <queue>
#include <random>
#include "core/index/float_vector/hnswlib/space_cosine.h"
#include "core/index/float_vector/hnswlib/space_ip.h"
#include "core/index/float_vector/hnswlib/space_l2.h"
#include "core/index/float_vector/scalar_quantization/quantizer.h"
#include "hnswlib.h"
#include "tools/errors.h"
#include "vendor/hopscotch/hopscotch_sc_map.h"
#include "visited_list_pool.h"

#include "core/enums.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"
#include "tools/flagguard.h"
#include "tools/logger.h"

namespace hnswlib {
typedef uint32_t tableint;
typedef uint32_t linklistsizeint;
static_assert(sizeof(linklistsizeint) == sizeof(tableint), "Internal link lists logic requires the same size for this types");

enum class [[nodiscard]] Synchronization { None, OnInsertions };

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

template <Synchronization synchronization>
class [[nodiscard]] HierarchicalNSW {
	template <typename K, typename V>
	using HashMapT = tsl::hopscotch_sc_map<K, V, std::hash<K>, std::equal_to<K>, std::less<K>, std::allocator<std::pair<const K, V>>, 30,
										   false, tsl::mod_growth_policy<std::ratio<3, 2>>>;
	template <typename K>
	using HashSetT = tsl::hopscotch_sc_set<K, std::hash<K>, std::equal_to<K>, std::less<K>, std::allocator<K>, 30, false,
										   tsl::mod_growth_policy<std::ratio<3, 2>>>;

	using LockVecT = std::conditional_t<synchronization == Synchronization::None, LabelOpsDummyLocks, LabelOpsMutexLocks>;
	using UpdateLockVecT = std::conditional_t<synchronization == Synchronization::None, UpdateOpsDummyLocks, UpdateOpsMutexLocks>;
	using GlobalMutextT = std::conditional_t<synchronization == Synchronization::None, DummyMutex, reindexer::mutex>;

	using QuantizerT = Quantizer<HierarchicalNSW>;

public:
	static const unsigned char DELETE_MARK = 0x01;

	size_t max_elements_{0};
	mutable std::atomic<size_t> cur_element_count{0};  // current number of elements
	size_t size_data_per_element_{0};
	size_t size_links_per_element_{0};
	size_t num_deleted_{0};	 // number of deleted elements
	size_t M_{0};
	size_t maxM_{0};
	size_t maxM0_{0};
	size_t ef_construction_{0};

	std::unique_ptr<QuantizerT> quantizer_;
	std::vector<CorrectiveOffsets> correctiveOffsets_;

	double mult_{0.0};
	int maxlevel_{0};
	tableint enterpoint_node_{0};
	UpdateLockVecT::MutexT enterpoint_lock_;  // Update mutex for enterpoint and maxlevel

	std::unique_ptr<VisitedListPool> visited_list_pool_{nullptr};

	GlobalMutextT global;
	LockVecT link_list_locks_;
	UpdateLockVecT data_updates_locks_;

	size_t size_links_level0_{0};
	size_t offsetData_{0}, label_offset_{0};

	char* data_level0_memory_{nullptr};
	char** linkLists_{nullptr};
	std::vector<int> element_levels_;  // keeps level of each element

	size_t data_size_{0};

	DistCalculator fstdistfunc_;

	mutable GlobalMutextT label_lookup_lock;  // lock for label_lookup_
	HashMapT<labeltype, tableint> label_lookup_;

	mutable GlobalMutextT generator_lock_;
	std::default_random_engine level_generator_;
	GlobalMutextT update_generator_lock_;
	std::default_random_engine update_probability_generator_;

	mutable std::atomic<long> metric_distance_computations{0};
	mutable std::atomic<long> metric_hops{0};

	reindexer::ReplaceDeleted allow_replace_deleted_ =
		reindexer::ReplaceDeleted_False;  // flag to replace deleted elements (marked as deleted) during insertions

	GlobalMutextT deleted_elements_lock;  // lock for deleted_elements
	HashSetT<tableint> deleted_elements;  // contains internal ids of deleted elements

	std::atomic<int32_t> concurrent_updates_counter_{0};

	HierarchicalNSW(SpaceInterface* s, const HierarchicalNSW& other, size_t newMaxElements)
		: max_elements_(std::max(other.max_elements_, newMaxElements)),
		  cur_element_count(other.cur_element_count.load()),
		  size_data_per_element_(other.size_data_per_element_),
		  num_deleted_(other.num_deleted_),
		  M_(other.M_),
		  maxM_(other.maxM_),
		  maxM0_(other.maxM0_),
		  ef_construction_(other.ef_construction_),
		  mult_(other.mult_),
		  maxlevel_(other.maxlevel_),
		  enterpoint_node_(other.enterpoint_node_),
		  visited_list_pool_(nullptr),
		  offsetData_(other.offsetData_),
		  label_offset_(other.label_offset_),
		  data_level0_memory_(nullptr),
		  linkLists_(nullptr),
		  allow_replace_deleted_(other.allow_replace_deleted_),
		  deleted_elements(other.deleted_elements) {
		initSpace(s);
		std::memcpy(data_level0_memory_, other.data_level0_memory_, cur_element_count * size_data_per_element_);
		fstdistfunc_.CopyValuesFrom(other.fstdistfunc_);
		initTree([&other, this](size_t i) {
			const unsigned linkListSize = other.element_levels_[i] > 0 ? other.size_links_per_element_ * other.element_levels_[i] : 0;
			if (linkListSize) {
				linkLists_[i] = (char*)malloc(linkListSize);
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

	HierarchicalNSW(SpaceInterface* s, size_t max_elements, size_t M, size_t ef_construction, size_t random_seed,
					reindexer::ReplaceDeleted allow_replace_deleted)
		: link_list_locks_(max_elements),
		  data_updates_locks_(max_elements),
		  element_levels_(max_elements),
		  allow_replace_deleted_(allow_replace_deleted) {
		max_elements_ = max_elements;
		num_deleted_ = 0;
		data_size_ = s->get_data_size();
		fstdistfunc_ = DistCalculator{s->get_dist_calculator_param(), max_elements_, &correctiveOffsets_};
		if (M > 10000) {
			logFmt(LogWarning,
				   "HNSW: M parameter exceeds 10000 which may lead to adverse effects. Cap to 10000 will be applied for the rest of the "
				   "processing.");
			M = 10000;
		}
		setM(M);
		ef_construction_ = std::max(ef_construction, M_);

		level_generator_.seed(random_seed);
		update_probability_generator_.seed(random_seed + 1);

		size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
		size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
		offsetData_ = size_links_level0_;
		label_offset_ = size_links_level0_ + data_size_;

		data_level0_memory_ = (char*)malloc(max_elements_ * size_data_per_element_);
		if (data_level0_memory_ == nullptr) {
			throw std::runtime_error("Not enough memory");
		}

		cur_element_count = 0;

		visited_list_pool_ = std::unique_ptr<VisitedListPool>(new VisitedListPool(1, max_elements));

		// initializations for special treatment of the first node
		enterpoint_node_ = std::numeric_limits<tableint>::max();
		maxlevel_ = -1;

		linkLists_ = (char**)malloc(sizeof(void*) * max_elements_);
		if (linkLists_ == nullptr) {
			throw std::runtime_error("Not enough memory: HierarchicalNSW failed to allocate linklists");
		}
		size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
		mult_ = getMultFromM(M_);
	}

	auto prepareData(const void* data) const {
		auto deleter = [quantized = bool(quantizer_)](const void* ptr) noexcept {
			if (quantized) {
				delete[] (static_cast<uint8_t*>(const_cast<void*>(ptr)));
			}
		};

		std::unique_ptr<uint8_t[]> res;
		if (quantizer_) {
			auto from = std::span(static_cast<const float*>(data), data_size_);
			res = std::make_unique<uint8_t[]>(data_size_ + sizeof(CorrectiveOffsets));
			auto to = std::span(res.get(), data_size_);
			auto correctiveOffset = quantizer_->Quantize(from, to);
			std::memcpy(res.get() + data_size_, &correctiveOffset, sizeof(CorrectiveOffsets));
		}

		return std::unique_ptr<const void, decltype(deleter)>(res ? res.release() : data, std::move(deleter));
	}

	template <typename LockerT>
	void Quantize(float quantile = 1.f, size_t sampleSize = QuantizerT::kDefaultSampleSize,
				  Sq8NonLinearCorrection nonLinearCorrection = Sq8NonLinearCorrection::Disabled) {
		using UniqueLock = typename LockerT::template UniqueLock<GlobalMutextT>;

		assertrx_throw(!quantizer_);

		// FIXME: think about external synchronization with namespace write-(or possibly read-) lock
		// during background index quantization
		UniqueLock lck{global};
		quantizer_ = std::make_unique<QuantizerT>(*this, quantile, sampleSize, nonLinearCorrection);
		correctiveOffsets_.resize(max_elements_);

		auto data_level0_memory_old = data_level0_memory_;
		auto size_data_per_element_old = size_data_per_element_;
		auto label_offset_old = label_offset_;

		std::unique_ptr<hnswlib::SpaceInterface> space;
		const auto dim = fstdistfunc_.Dims();
		switch (fstdistfunc_.Metric()) {
			case hnswlib::MetricType::L2:
				space = std::make_unique<L2SpaceSq8>(dim);
				break;
			case hnswlib::MetricType::COSINE:
				space = std::make_unique<CosineSpaceSq8>(dim);
				break;
			case hnswlib::MetricType::INNER_PRODUCT:
				space = std::make_unique<InnerProductSpaceSq8>(dim);
				break;
			case hnswlib::MetricType::NONE:
			default:
				assert(false);
				break;
		}

		initSpace(space.get());
		fstdistfunc_.UpdateQuantizeParams(quantizer_);

		unsigned size = cur_element_count;
		for (tableint internal_id = 0; internal_id < size; ++internal_id) {
			auto oldEl = data_level0_memory_old + internal_id * size_data_per_element_old;
			auto newEl = data_level0_memory_ + internal_id * size_data_per_element_;

			// copy element links
			std::memcpy(newEl, oldEl, size_links_level0_);

			// copy quantized element data
			auto from = std::span(reinterpret_cast<float*>(oldEl + offsetData_), dim);
			auto to = std::span(reinterpret_cast<uint8_t*>(newEl + offsetData_), dim);

			correctiveOffsets_[internal_id] = quantizer_->Quantize(from, to);

			// copy element label
			std::memcpy(newEl + label_offset_, oldEl + label_offset_old, sizeof(labeltype));
		}

		free(data_level0_memory_old);
	}

	template <typename LockerT>
	void Requantize(bool force = false) {
		using UniqueLock = typename LockerT::template UniqueLock<GlobalMutextT>;

		assertrx_throw(quantizer_);

		if (!force && !quantizer_->NeedRequantize()) {
			return;
		}

		// FIXME: think about external synchronization with namespace write-(or possibly read-) lock
		// during background index quantization
		UniqueLock lck{global};

		auto dequantizer = quantizer_->PrepareToRequantize();
		const auto dim = fstdistfunc_.Dims();
		for (auto [_, internal_id] : label_lookup_) {
			auto point = std::span(reinterpret_cast<uint8_t*>(getDataByInternalId(internal_id)), dim);
			correctiveOffsets_[internal_id] = quantizer_->Requantize(point, dequantizer);
		}

		fstdistfunc_.UpdateQuantizeParams(quantizer_);
	}

	~HierarchicalNSW() { clear(); }

	static double getMultFromM(size_t M) noexcept { return 1 / log(1.0 * M); }

	void setM(size_t M) noexcept {
		M_ = M;
		maxM0_ = 2 * M;
		maxM_ = M;
	}

	void clear() {
		free(data_level0_memory_);
		data_level0_memory_ = nullptr;
		for (tableint i = 0; i < cur_element_count; i++) {
			if (element_levels_[i] > 0) {
				free(linkLists_[i]);
			}
		}
		free(linkLists_);
		linkLists_ = nullptr;
		cur_element_count = 0;
		visited_list_pool_.reset(nullptr);
		deleted_elements.clear();
		num_deleted_ = 0;
		quantizer_.reset();
		correctiveOffsets_.clear();
	}

	size_t allocatedMemSize() const noexcept {
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

		return ret;
	}

	struct [[nodiscard]] CompareByFirst {
		constexpr bool operator()(const std::pair<float, tableint>& a, const std::pair<float, tableint>& b) const noexcept {
			return a.first < b.first;
		}
	};

	inline labeltype getExternalLabel(tableint internal_id) const {
		labeltype return_label;
		memcpy(&return_label, (data_level0_memory_ + internal_id * size_data_per_element_ + label_offset_), sizeof(labeltype));
		return return_label;
	}

	inline void setExternalLabel(tableint internal_id, labeltype label) const {
		memcpy((data_level0_memory_ + internal_id * size_data_per_element_ + label_offset_), &label, sizeof(labeltype));
	}

	inline char* getDataByInternalId(tableint internal_id) const {
		return (data_level0_memory_ + internal_id * size_data_per_element_ + offsetData_);
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
		return (int)r;
	}

	size_t getMaxElements() const noexcept { return max_elements_; }

	size_t getCurrentElementCount() const noexcept { return cur_element_count; }

	// *NOT* thread safe
	size_t getDeletedCountUnsafe() { return num_deleted_; }

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> searchBaseLayer(
		tableint ep_id, const void* data_point, tableint data_point_id, int layer) {
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
		std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> top_candidates(CompareByFirst(), std::move(container1));
		std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> candidateSet(CompareByFirst(), std::move(container2));

		float lowerBound;
		{
			SharedLock lck{data_updates_locks_[ep_id]};
			if (!isMarkedDeleted(ep_id)) {
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

			int* data;	// = (int *)(linkList0_ + curNodeNum * size_links_per_element0_);
			if (layer == 0) {
				data = (int*)get_linklist0(curNodeNum);
			} else {
				data = (int*)get_linklist(curNodeNum, layer);
				//                    data = (int *) (linkLists_[curNodeNum] + (layer - 1) * size_links_per_element_);
			}
			size_t size = getListCount((linklistsizeint*)data);
			tableint* datal = (tableint*)(data + 1);
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)  // Asan reports overflow on prefetch
			_mm_prefetch((char*)(visited_array + *(data + 1)), _MM_HINT_T0);
			_mm_prefetch((char*)(visited_array + *(data + 1) + 64), _MM_HINT_T0);
			_mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
			_mm_prefetch(getDataByInternalId(*(datal + 1)), _MM_HINT_T0);
#endif	// defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

			for (size_t j = 0; j < size; j++) {
				tableint candidate_id = *(datal + j);
//                    if (candidate_id == 0) continue;
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)  // Asan reports overflow on prefetch
				_mm_prefetch((char*)(visited_array + *(datal + j + 1)), _MM_HINT_T0);
				_mm_prefetch(getDataByInternalId(*(datal + j + 1)), _MM_HINT_T0);
#endif	// defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
				if (visited_array[candidate_id] == visited_array_tag) {
					continue;
				}
				visited_array[candidate_id] = visited_array_tag;

				SharedLock lck{data_updates_locks_[candidate_id]};
				float dist1 = fstdistfunc_(data_point, data_point_id, getDataByInternalId(candidate_id), candidate_id);
				if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
					candidateSet.emplace(-dist1, candidate_id);
#if REINDEXER_WITH_SSE
					_mm_prefetch(getDataByInternalId(candidateSet.top().second), _MM_HINT_T0);
#endif	// REINDEXER_WITH_SSE

					if (!isMarkedDeleted(candidate_id)) {
						top_candidates.emplace(dist1, candidate_id);
					}

					if (top_candidates.size() > ef_construction_) {
						top_candidates.pop();
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

	// bare_bone_search means there is no check for deletions and stop condition is ignored in return of extra performance
	template <bool bare_bone_search = true, bool collect_metrics = false>
	std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> searchBaseLayerST(
		tableint ep_id, const void* data_point, size_t ef, BaseFilterFunctor* isIdAllowed = nullptr,
		BaseSearchStopCondition<float>* stop_condition = nullptr) const {
		VisitedList* vl = visited_list_pool_->getFreeVisitedList();
		vl_type* visited_array = vl->mass;
		vl_type visited_array_tag = vl->curV;

		using pair_t = std::pair<float, tableint>;
		std::vector<pair_t> container1, container2;
		container1.reserve(ef);
		container2.reserve(ef);
		std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> top_candidates(CompareByFirst(), std::move(container1));
		std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> candidate_set(CompareByFirst(), std::move(container2));

		float lowerBound;
		if (bare_bone_search || (!isMarkedDeleted(ep_id) && ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(ep_id))))) {
			auto ep_data = getDataByInternalId(ep_id);
			float dist = fstdistfunc_(data_point, ep_data, ep_id);
			lowerBound = dist;
			top_candidates.emplace(dist, ep_id);
			if (!bare_bone_search && stop_condition) {
				stop_condition->add_point_to_result(getExternalLabel(ep_id), ep_data, dist);
			}
			candidate_set.emplace(-dist, ep_id);
		} else {
			lowerBound = std::numeric_limits<float>::max();
			candidate_set.emplace(-lowerBound, ep_id);
		}

		visited_array[ep_id] = visited_array_tag;

		while (!candidate_set.empty()) {
			std::pair<float, tableint> current_node_pair = candidate_set.top();
			float candidate_dist = -current_node_pair.first;

			bool flag_stop_search;
			if (bare_bone_search) {
				flag_stop_search = candidate_dist > lowerBound;
			} else {
				if (stop_condition) {
					flag_stop_search = stop_condition->should_stop_search(candidate_dist, lowerBound);
				} else {
					flag_stop_search = candidate_dist > lowerBound && top_candidates.size() == ef;
				}
			}
			if (flag_stop_search) {
				break;
			}
			candidate_set.pop();

			tableint current_node_id = current_node_pair.second;
			int* data = (int*)get_linklist0(current_node_id);
			size_t size = getListCount((linklistsizeint*)data);
			//                bool cur_node_deleted = isMarkedDeleted(current_node_id);
			if (collect_metrics) {
				metric_hops++;
				metric_distance_computations += size;
			}

#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)  // Asan reports overflow on prefetch
			_mm_prefetch((char*)(visited_array + *(data + 1)), _MM_HINT_T0);
			_mm_prefetch((char*)(visited_array + *(data + 1) + 64), _MM_HINT_T0);
			_mm_prefetch(data_level0_memory_ + (*(data + 1)) * size_data_per_element_ + offsetData_, _MM_HINT_T0);
			_mm_prefetch((char*)(data + 2), _MM_HINT_T0);
#endif	// defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

			for (size_t j = 1; j <= size; j++) {
				int candidate_id = *(data + j);
//                    if (candidate_id == 0) continue;
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)  // Asan reports overflow on prefetch
				_mm_prefetch((char*)(visited_array + *(data + j + 1)), _MM_HINT_T0);
				_mm_prefetch(data_level0_memory_ + (*(data + j + 1)) * size_data_per_element_ + offsetData_,
							 _MM_HINT_T0);	////////////
#endif										// defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
				if (!(visited_array[candidate_id] == visited_array_tag)) {
					visited_array[candidate_id] = visited_array_tag;

					char* currObj1 = (getDataByInternalId(candidate_id));
					float dist = fstdistfunc_(data_point, currObj1, candidate_id);
					bool flag_consider_candidate;
					if (!bare_bone_search && stop_condition) {
						flag_consider_candidate = stop_condition->should_consider_candidate(dist, lowerBound);
					} else {
						flag_consider_candidate = top_candidates.size() < ef || lowerBound > dist;
					}

					if (flag_consider_candidate) {
						candidate_set.emplace(-dist, candidate_id);
#if REINDEXER_WITH_SSE
						_mm_prefetch(data_level0_memory_ + candidate_set.top().second * size_data_per_element_,
									 _MM_HINT_T0);	////////////////////////
#endif												// REINDEXER_WITH_SSE

						if (bare_bone_search ||
							(!isMarkedDeleted(candidate_id) && ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(candidate_id))))) {
							top_candidates.emplace(dist, candidate_id);
							if (!bare_bone_search && stop_condition) {
								stop_condition->add_point_to_result(getExternalLabel(candidate_id), currObj1, dist);
							}
						}

						bool flag_remove_extra = false;
						if (!bare_bone_search && stop_condition) {
							flag_remove_extra = stop_condition->should_remove_extra();
						} else {
							flag_remove_extra = top_candidates.size() > ef;
						}
						while (flag_remove_extra) {
							tableint id = top_candidates.top().second;
							top_candidates.pop();
							if (!bare_bone_search && stop_condition) {
								stop_condition->remove_point_from_result(getExternalLabel(id), getDataByInternalId(id), dist);
								flag_remove_extra = stop_condition->should_remove_extra();
							} else {
								flag_remove_extra = top_candidates.size() > ef;
							}
						}

						if (!top_candidates.empty()) {
							lowerBound = top_candidates.top().first;
						}
					}
				}
			}
		}

		visited_list_pool_->releaseVisitedList(vl);
		return top_candidates;
	}

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	void getNeighborsByHeuristic2(
		std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst>& top_candidates,
		const size_t M) {
		static_assert(concurrentUpdates == ExpectConcurrentUpdates::No || isSynchronizationPossible<LockerT>(),
					  "Unable to handle concurrent updates without synchronization");
		using SharedLock =
			typename LockerT::template Concurrent<concurrentUpdates>::template SharedLock<typename UpdateOpsMutexLocks::MutexT>;
		if (top_candidates.size() < M) {
			return;
		}

		std::priority_queue<std::pair<float, tableint>> queue_closest;
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
		return (linklistsizeint*)(data_level0_memory_ + internal_id * size_data_per_element_);
	}

	linklistsizeint* get_linklist0(tableint internal_id, char* data_level0_memory_) const {
		return (linklistsizeint*)(data_level0_memory_ + internal_id * size_data_per_element_);
	}

	linklistsizeint* get_linklist(tableint internal_id, int level) const {
		return (linklistsizeint*)(linkLists_[internal_id] + (level - 1) * size_links_per_element_);
	}

	linklistsizeint* get_linklist_at_level(tableint internal_id, int level) const {
		return level == 0 ? get_linklist0(internal_id) : get_linklist(internal_id, level);
	}

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	tableint mutuallyConnectNewElement(
		tableint cur_c,
		std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst>& top_candidates, int level,
		bool isUpdate) {
		static_assert(concurrentUpdates == ExpectConcurrentUpdates::No || isSynchronizationPossible<LockerT>(),
					  "Unable to handle concurrent updates without synchronization");
		using UniqueLock = typename LockerT::template UniqueLock<typename LockVecT::MutexT>;
		using LockGuard = typename LockerT::template LockGuard<typename LockVecT::MutexT>;

		size_t Mcurmax = level ? maxM_ : maxM0_;
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

			if (*ll_cur && !isUpdate) {
				throw std::runtime_error("The newly inserted element should have blank link list");
			}
			setListCount(ll_cur, selectedNeighbors.size());
			tableint* data = (tableint*)(ll_cur + 1);
			for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
				if (data[idx] && !isUpdate) {
					throw std::runtime_error("Possible memory corruption");
				}
				if (level > element_levels_[selectedNeighbors[idx]]) {
					throw std::runtime_error("Trying to make a link on a non-existent level");
				}

				data[idx] = selectedNeighbors[idx];
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

			tableint* data = (tableint*)(ll_other + 1);

			bool is_cur_c_present = false;
			if (isUpdate) {
				for (size_t j = 0; j < sz_link_list_other; j++) {
					if (data[j] == cur_c) {
						is_cur_c_present = true;
						break;
					}
				}
			}

			// If cur_c is already present in the neighboring connections of `selectedNeighbors[idx]` then no need to modify any connections
			// or run the heuristics.
			if (!is_cur_c_present) {
				if (sz_link_list_other < Mcurmax) {
					data[sz_link_list_other] = cur_c;
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
					std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> candidates;
					candidates.emplace(d_max, cur_c);

					for (size_t j = 0; j < sz_link_list_other; j++) {
						const auto id1 = data[j];
						SharedLock lck1{data_updates_locks_[std::min(id1, id2)]};
						SharedLock lck2{data_updates_locks_[std::max(id1, id2)], reindexer::SkipLock(id1 == id2)};
						candidates.emplace(fstdistfunc_(getDataByInternalId(data[j]), id1, getDataByInternalId(id2), id2), data[j]);
					}

					getNeighborsByHeuristic2<LockerT, concurrentUpdates>(candidates, Mcurmax);

					int indx = 0;
					while (candidates.size() > 0) {
						data[indx] = candidates.top().second;
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

	ResizeResult resizeIndex(size_t new_max_elements) {
		if (new_max_elements < cur_element_count) {
			throw std::runtime_error("Cannot resize, max element is less than the current number of elements");
		}

		visited_list_pool_.reset(new VisitedListPool(1, new_max_elements));

		element_levels_.resize(new_max_elements);

		LockVecT(new_max_elements).swap(link_list_locks_);
		UpdateLockVecT(new_max_elements).swap(data_updates_locks_);

		// Reallocate base layer
		char* data_level0_memory_new = (char*)realloc(data_level0_memory_, new_max_elements * size_data_per_element_);
		if (data_level0_memory_new == nullptr) {
			throw std::runtime_error("Not enough memory: resizeIndex failed to allocate base layer");
		}
		const void* data_level0_memory_old = std::exchange(data_level0_memory_, data_level0_memory_new);

		// Reallocate all other layers
		char** linkLists_new = (char**)realloc(linkLists_, sizeof(void*) * new_max_elements);
		if (linkLists_new == nullptr) {
			throw std::runtime_error("Not enough memory: resizeIndex failed to allocate other layers");
		}
		linkLists_ = linkLists_new;

		max_elements_ = new_max_elements;
		fstdistfunc_.Resize(max_elements_);
		return {data_level0_memory_old, data_level0_memory_new};
	}

	// Read-only concurrency expected
	void saveIndex(IWriter& writer, const std::atomic_int32_t& cancel) {
		if (quantizer_) {
			throw std::runtime_error("Saving of the quantized hnsw-graph not supported yet.");
		}
		constexpr size_t kCancelPeriod = 0x3FFFFF;

		writer.PutVarUInt(uint64_t(max_elements_));
		writer.PutVarUInt(uint64_t(cur_element_count.load()));
		writer.PutVarInt(maxlevel_);
		writer.PutVarUInt(enterpoint_node_);
		writer.PutVarUInt(uint32_t(M_));
		writer.PutVarUInt(uint32_t(ef_construction_));

		for (size_t i = 0; i < cur_element_count; ++i) {
			if (((i & kCancelPeriod) == kCancelPeriod) && cancel.load(std::memory_order_relaxed)) {
				throw std::runtime_error("HNSW index saving was canceled");
			}

			static_assert(sizeof(linklistsizeint) == sizeof(tableint), "Expecting equality of those sizes here");
			// Write links and label; skip actual data
			const char* cur_element_ptr = data_level0_memory_ + i * size_data_per_element_;
			const auto* linkList0 = reinterpret_cast<const tableint*>(cur_element_ptr);
			const linklistsizeint list0Size = getListCount(reinterpret_cast<const linklistsizeint*>(linkList0)) + 1;
			const bool isDeleted = isListMarkedDeleted(linkList0);
			for (size_t j = 0; j < list0Size; ++j) {
				writer.PutVarUInt(*(linkList0++));
			}
			if (isDeleted) {
				// We have to store full vector for the deleted items
				writer.PutVString(std::string_view(cur_element_ptr + offsetData_, data_size_));
			} else {
				labeltype label;
				std::memcpy(&label, (cur_element_ptr + label_offset_), sizeof(labeltype));
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

	void initSpace(SpaceInterface* s) {
		data_size_ = s->get_data_size();

		auto newDistCalculator = DistCalculator{s->get_dist_calculator_param(), max_elements_, &correctiveOffsets_};
		if (fstdistfunc_.Metric() == MetricType::COSINE) {	// copying makes sense only for the Cosine metric.
			newDistCalculator.CopyValuesFrom(fstdistfunc_);
		}

		fstdistfunc_ = std::move(newDistCalculator);

		size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
		size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
		offsetData_ = size_links_level0_;
		label_offset_ = size_links_level0_ + data_size_;

		data_level0_memory_ = (char*)malloc(max_elements_ * size_data_per_element_);
		if (data_level0_memory_ == nullptr) {
			throw std::runtime_error("Not enough memory: loadIndex failed to allocate level0");
		}
	}

	template <typename F>
	void initTree(F fillLinkList) {
		size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

		size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
		LockVecT(max_elements_).swap(link_list_locks_);
		UpdateLockVecT(max_elements_).swap(data_updates_locks_);

		visited_list_pool_.reset(new VisitedListPool(1, max_elements_));

		linkLists_ = (char**)malloc(sizeof(void*) * max_elements_);
		if (linkLists_ == nullptr) {
			throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklists");
		}
		element_levels_ = std::vector<int>(max_elements_);
		for (size_t i = 0; i < cur_element_count; i++) {
			if (isMarkedDeleted(i)) {
				num_deleted_ += 1;
				if (allow_replace_deleted_) {
					assertrx_dbg(deleted_elements.count(i) == 0);
					deleted_elements.insert(i);
				}
			} else {
				// Do not store deleted lables in label_lookup_
				label_lookup_[getExternalLabel(i)] = i;
			}
			const unsigned linkListSize = fillLinkList(i);
			element_levels_[i] = linkListSize / size_links_per_element_;
		}
	}

	// *NOT* thread-safe
	void loadIndex(IReader& reader, SpaceInterface* s) {
		clear();

		max_elements_ = reader.GetVarUInt();
		const auto cur_count = reader.GetVarUInt();
		cur_element_count.store(cur_count);
		if (cur_count > max_elements_) {
			throw std::runtime_error("Current elements count is larger than max elements count");
		}
		maxlevel_ = reader.GetVarInt();
		enterpoint_node_ = reader.GetVarUInt();
		if (cur_count) {
			if (enterpoint_node_ >= cur_count) {
				throw std::runtime_error("Incorrect entrypoint node ID");
			}
		} else if (enterpoint_node_ != std::numeric_limits<tableint>::max()) {
			throw std::runtime_error("Unexpected entrypoint node ID for empty HNSW");
		}
		setM(reader.GetVarUInt());
		mult_ = getMultFromM(M_);
		ef_construction_ = reader.GetVarUInt();

		initSpace(s);
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
			cur_element_ptr += size_links_level0_;
			labeltype label = std::numeric_limits<labeltype>::max();
			if (isListMarkedDeleted(linkList0Start)) {
				std::string_view vector = reader.GetVString();
				std::memcpy(cur_element_ptr, vector.data(), vector.size());
				fstdistfunc_.AddNorm(vector.data(), i);
			} else {
				label = reader.ReadPkEncodedData(cur_element_ptr);
				fstdistfunc_.AddNorm(cur_element_ptr, i);
			}
			cur_element_ptr += data_size_;
			std::memcpy(cur_element_ptr, &label, sizeof(labeltype));
		}

		initTree([&reader, this](size_t i) -> size_t {
			auto list = reader.GetVString();
			if (list.size()) {
				linkLists_[i] = (char*)malloc(list.size());
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

	tableint getInternalIdByLabel(labeltype label) const {
		auto search = label_lookup_.find(label);
		const bool found = (search != label_lookup_.end());
		if (!found || isMarkedDeleted(search->second)) {
			using namespace reindexer;
			assertf_dbg(false, "getLabel: Label not found: {}", label);
			throw std::runtime_error("getLabel: Label not found: " + std::to_string(label));
		}
		return search->second;
	}

	const char* ptrByExternalLabel(labeltype label) const { return getDataByInternalId(getInternalIdByLabel(label)); }

	/*
	 * Marks an element with the given label deleted, does NOT really change the current graph.
	 */
	// *NOT* thread-safe
	void markDelete(labeltype label) {
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
		if (!isMarkedDeleted(internalId)) {
			unsigned char* ll_cur = ((unsigned char*)get_linklist0(internalId)) + 2;
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
		if (isMarkedDeleted(internalId)) {
			unsigned char* ll_cur = ((unsigned char*)get_linklist0(internalId)) + 2;
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
		const unsigned char* ll_cur = ((const unsigned char*)linkList0) + 2;
		return *ll_cur & DELETE_MARK;
	}
	bool isMarkedDeleted(tableint internalId) const noexcept { return isListMarkedDeleted(get_linklist0(internalId)); }

	unsigned short int getListCount(const linklistsizeint* ptr) const { return *((const unsigned short int*)ptr); }

	void setListCount(linklistsizeint* ptr, unsigned short int size) const {
		*((unsigned short int*)(ptr)) = *((unsigned short int*)&size);
	}

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
	void addPointNoLock(const void* data_point, labeltype label)
		RX_REQUIRES(!deleted_elements_lock, !label_lookup_lock, !global, !enterpoint_lock_, !generator_lock_, !update_generator_lock_) {
		addPoint<DummyLocker>(data_point, label);
	}
	void addPointConcurrent(const void* data_point, labeltype label)
		RX_REQUIRES(!deleted_elements_lock, !label_lookup_lock, !global, !enterpoint_lock_, !generator_lock_, !update_generator_lock_) {
		if constexpr (synchronization == Synchronization::None) {
			throw std::logic_error("This HNSW index does not support concurrent insertions");
		}
		addPoint<RegularLocker>(data_point, label);
	}

	template <typename LockerT>
	void addPoint(const void* data_point, labeltype label)
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
				if (!isMarkedDeleted(internal_id_replaced)) {
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
	void updatePoint(const void* dataPointRaw, tableint internalId, float updateNeighborProbability)
		RX_REQUIRES(!enterpoint_lock_, !update_generator_lock_, !deleted_elements_lock) {
		using LockGuard = typename LockerT::template LockGuard<GlobalMutextT>;
		using UpdateLockGuard = typename LockerT::template LockGuard<typename UpdateLockVecT::MutexT>;
		using SharedLock = typename LockerT::template SharedLock<typename UpdateLockVecT::MutexT>;
		const auto dataPointHolder = prepareData(dataPointRaw);
		const void* dataPoint = dataPointHolder.get();
		{
			// FIXME: There is still logical race, that reduces recall rate - we may update one of the current insertion candidate nodes
			// right after dist calculation Check #2029 for details.
			UpdateLockGuard lck{data_updates_locks_[internalId]};
			// update the feature vector associated with existing point with new vector
			memcpy(getDataByInternalId(internalId), dataPoint, data_size_);
			if (quantizer_) {
				quantizer_->UpdateStatistic(dataPointRaw);
				auto correctiveOffsetsPtr =
					reinterpret_cast<const CorrectiveOffsets*>(reinterpret_cast<const uint8_t*>(dataPoint) + data_size_);
				std::memmove(&correctiveOffsets_[internalId], correctiveOffsetsPtr, sizeof(CorrectiveOffsets));
			}
			fstdistfunc_.AddNorm(dataPointRaw, internalId);
			if (isMarkedDeleted(internalId)) {
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

				std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> candidates;
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
				getNeighborsByHeuristic2<LockerT, isConcurrentUpdatesAllowedByIdx()>(candidates, layer == 0 ? maxM0_ : maxM_);

				{
					LockGuard lock{link_list_locks_[neigh]};
					linklistsizeint* ll_cur;
					ll_cur = get_linklist_at_level(neigh, layer);
					size_t candSize = candidates.size();
					setListCount(ll_cur, candSize);
					tableint* data = (tableint*)(ll_cur + 1);
					for (size_t idx = 0; idx < candSize; idx++) {
						data[idx] = candidates.top().second;
						candidates.pop();
					}
				}
			}
		}

		repairConnectionsForUpdate<LockerT>(dataPoint, entryPointCopy, internalId, elemLevel, maxLevelCopy);
	}

	template <typename LockerT>
	void repairConnectionsForUpdate(const void* dataPoint, tableint entryPointInternalId, tableint dataPointInternalId, int dataPointLevel,
									int maxLevel) {
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
					unsigned int* data;
					LockGuard lock{link_list_locks_[currObj]};
					data = get_linklist_at_level(currObj, level);
					int size = getListCount(data);
					tableint* datal = (tableint*)(data + 1);
#if REINDEXER_WITH_SSE
					_mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
#endif	// REINDEXER_WITH_SSE
					for (int i = 0; i < size; i++) {
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)  // Asan reports overflow on prefetch
						_mm_prefetch(getDataByInternalId(*(datal + i + 1)), _MM_HINT_T0);
#endif	// defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
						tableint cand = datal[i];

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
			std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> topCandidates =
				searchBaseLayer<LockerT, isConcurrentUpdatesAllowedByIdx()>(currObj, dataPoint, dataPointInternalId, level);

			std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> filteredTopCandidates;
			while (topCandidates.size() > 0) {
				if (topCandidates.top().second != dataPointInternalId) {
					filteredTopCandidates.push(topCandidates.top());
				}

				topCandidates.pop();
			}

			// Since element_levels_ is being used to get `dataPointLevel`, there could be cases where `topCandidates` could just contain
			// entry point itself. To prevent self loops, the `topCandidates` is filtered and thus can be empty.
			if (filteredTopCandidates.size() > 0) {
				{
					SharedLock lck{data_updates_locks_[entryPointInternalId]};
					bool epDeleted = isMarkedDeleted(entryPointInternalId);
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
		unsigned int* data = get_linklist_at_level(internalId, level);
		int size = getListCount(data);
		std::vector<tableint> result(size);
		tableint* ll = (tableint*)(data + 1);
		memcpy(result.data(), ll, size * sizeof(tableint));
		return result;
	}

	template <typename LockerT, ExpectConcurrentUpdates concurrentUpdates>
	tableint addPoint(const void* data_point_raw, labeltype label, int level)
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
					if (isMarkedDeleted(existingInternalId)) {
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
			if (curlevel <= maxlevelcopy && enterpoint_node_ >= 0) {
				lck.unlock();
				templock.unlock();
			}
		}

		tableint currObj = enterpoint_copy;

		memset(data_level0_memory_ + cur_c * size_data_per_element_, 0, size_data_per_element_);

		// Initialisation of the data and label
		setExternalLabel(cur_c, label);
		const auto dataPointHolder = prepareData(data_point_raw);
		const void* data_point = dataPointHolder.get();
		memcpy(getDataByInternalId(cur_c), data_point, data_size_);
		if (quantizer_) {
			quantizer_->UpdateStatistic(data_point_raw);
			auto correctiveOffsetsPtr =
				reinterpret_cast<const CorrectiveOffsets*>(reinterpret_cast<const uint8_t*>(data_point) + data_size_);
			std::memmove(&correctiveOffsets_[cur_c], correctiveOffsetsPtr, sizeof(CorrectiveOffsets));
		}
		if (curlevel) {
			linkLists_[cur_c] = (char*)malloc(size_links_per_element_ * curlevel + 1);
			if (linkLists_[cur_c] == nullptr) {
				throw std::runtime_error("Not enough memory: addPoint failed to allocate linklist");
			}
			memset(linkLists_[cur_c], 0, size_links_per_element_ * curlevel + 1);
		}

		if ((signed)currObj != -1) {
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
						unsigned int* data;
						DataLockGuard lock{link_list_locks_[currObj]};
						data = get_linklist(currObj, level);
						int size = getListCount(data);

						tableint* datal = (tableint*)(data + 1);
						for (int i = 0; i < size; i++) {
							tableint cand = datal[i];
							if (cand < 0 || cand > max_elements_) {
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

				std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> top_candidates =
					searchBaseLayer<LockerT, concurrentUpdates>(currObj, data_point, cur_c, level);

				DataUpdatesSharedLock lck{data_updates_locks_[enterpoint_copy]};
				bool epDeleted = isMarkedDeleted(enterpoint_copy);
				if (epDeleted) {
					top_candidates.emplace(fstdistfunc_(data_point, cur_c, getDataByInternalId(enterpoint_copy), enterpoint_copy),
										   enterpoint_copy);
					if (top_candidates.size() > ef_construction_) {
						top_candidates.pop();
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

	// Read-only concurrency expected
	auto getTopCandidates(const void* query_data_raw, size_t ef, BaseFilterFunctor* isIdAllowed) const {
		const auto query_data_holder = prepareData(query_data_raw);
		auto query_data = query_data_holder.get();

		tableint currObj = enterpoint_node_;
		float curdist = fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), enterpoint_node_);

		for (int level = maxlevel_; level > 0; level--) {
			bool changed = true;
			while (changed) {
				changed = false;
				unsigned int* data;

				data = (unsigned int*)get_linklist(currObj, level);
				int size = getListCount(data);
				metric_hops++;
				metric_distance_computations += size;

				tableint* datal = (tableint*)(data + 1);
				for (int i = 0; i < size; i++) {
					tableint cand = datal[i];
					if (cand < 0 || cand > max_elements_) {
						throw std::runtime_error("cand error");
					}
					float d = fstdistfunc_(query_data, getDataByInternalId(cand), cand);
					if (d < curdist) {
						curdist = d;
						currObj = cand;
						changed = true;
					}
				}
			}
		}

		if (!num_deleted_ && !isIdAllowed) {
			return searchBaseLayerST<true>(currObj, query_data, ef, isIdAllowed);
		} else {
			return searchBaseLayerST<false>(currObj, query_data, ef, isIdAllowed);
		}
	}

	// Read-only concurrency expected
	std::priority_queue<std::pair<float, labeltype>> searchKnn(const void* query_data, size_t k, size_t ef = 0,
															   BaseFilterFunctor* isIdAllowed = nullptr) const {
		std::vector<std::pair<float, labeltype>> container;
		container.reserve(k);
		std::priority_queue<std::pair<float, labeltype>> result(std::less<std::pair<float, labeltype>>(), std::move(container));

		if (cur_element_count == 0) {
			return result;
		}

		ef = ef ? ef : k * 3 / 2;
		auto top_candidates = getTopCandidates(query_data, ef, isIdAllowed);
		while (top_candidates.size() > k) {
			top_candidates.pop();
		}
		while (top_candidates.size() > 0) {
			std::pair<float, tableint> rez = top_candidates.top();
			result.push(std::pair<float, labeltype>(rez.first, getExternalLabel(rez.second)));
			top_candidates.pop();
		}
		return result;
	}

	// Read-only concurrency expected
	std::priority_queue<std::pair<float, labeltype>> searchRange(const void* query_data, float radius, size_t ef,
																 BaseFilterFunctor* isIdAllowed = nullptr) const {
		if (cur_element_count == 0) {
			return std::priority_queue<std::pair<float, labeltype>>();
		}

		auto top_candidates = getTopCandidates(query_data, ef, isIdAllowed);

		std::vector<std::pair<float, labeltype>> container;
		container.reserve(top_candidates.size());
		std::priority_queue<std::pair<float, labeltype>> result(std::less<std::pair<float, labeltype>>(), std::move(container));

		VisitedList* vl = visited_list_pool_->getFreeVisitedList();
		vl_type* visited_array = vl->mass;
		vl_type visited_array_tag = vl->curV;

		std::queue<std::pair<float, tableint>> radius_queue;
		while (!top_candidates.empty()) {
			const auto& cand = top_candidates.top();
			if (cand.first < radius) {
				radius_queue.emplace(cand);
				result.emplace(cand.first, cand.second);
			}
			visited_array[cand.second] = visited_array_tag;
			top_candidates.pop();
		}

		while (!radius_queue.empty()) {
			auto cur = radius_queue.front();
			radius_queue.pop();

			tableint current_id = cur.second;
			int* data = (int*)get_linklist0(current_id);
			size_t size = getListCount((linklistsizeint*)data);

			for (size_t j = 1; j <= size; j++) {
				int candidate_id = *(data + j);
				if (isMarkedDeleted(candidate_id)) {
					continue;
				}
				if (visited_array[candidate_id] != visited_array_tag) {
					visited_array[candidate_id] = visited_array_tag;
					if ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(candidate_id))) {
						float dist = fstdistfunc_(query_data, getDataByInternalId(candidate_id), candidate_id);
						if (dist < radius) {
							radius_queue.emplace(dist, candidate_id);
							result.emplace(dist, getExternalLabel(candidate_id));
						}
					}
				}
			}
		}

		visited_list_pool_->releaseVisitedList(vl);

		return result;
	}

	// Read-only concurrency expected
	std::vector<std::pair<float, labeltype>> searchStopConditionClosest(const void* query_data,
																		BaseSearchStopCondition<float>& stop_condition,
																		BaseFilterFunctor* isIdAllowed = nullptr) const {
		std::vector<std::pair<float, labeltype>> result;
		if (cur_element_count == 0) {
			return result;
		}

		tableint currObj = enterpoint_node_;
		float curdist = fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), enterpoint_node_);

		for (int level = maxlevel_; level > 0; level--) {
			bool changed = true;
			while (changed) {
				changed = false;
				unsigned int* data;

				data = (unsigned int*)get_linklist(currObj, level);
				int size = getListCount(data);
				metric_hops++;
				metric_distance_computations += size;

				tableint* datal = (tableint*)(data + 1);
				for (int i = 0; i < size; i++) {
					tableint cand = datal[i];
					if (cand < 0 || cand > max_elements_) {
						throw std::runtime_error("cand error");
					}
					float d = fstdistfunc_(query_data, getDataByInternalId(cand), cand);

					if (d < curdist) {
						curdist = d;
						currObj = cand;
						changed = true;
					}
				}
			}
		}

		std::priority_queue<std::pair<float, tableint>, std::vector<std::pair<float, tableint>>, CompareByFirst> top_candidates;
		top_candidates = searchBaseLayerST<false>(currObj, query_data, 0, isIdAllowed, &stop_condition);

		size_t sz = top_candidates.size();
		result.resize(sz);
		while (!top_candidates.empty()) {
			result[--sz] = top_candidates.top();
			top_candidates.pop();
		}

		stop_condition.filter_results(result);

		return result;
	}

	void checkIntegrity() {
		int connections_checked = 0;
		std::vector<int> inbound_connections_num(cur_element_count, 0);
		for (int i = 0; i < cur_element_count; i++) {
			for (int l = 0; l <= element_levels_[i]; l++) {
				linklistsizeint* ll_cur = get_linklist_at_level(i, l);
				int size = getListCount(ll_cur);
				tableint* data = (tableint*)(ll_cur + 1);
				reindexer::fast_hash_set<tableint> s;
				for (int j = 0; j < size; j++) {
					assert(data[j] < cur_element_count);
					assert(data[j] != i);
					inbound_connections_num[data[j]]++;
					s.insert(data[j]);
					connections_checked++;
				}
				assert(s.size() == size);
			}
		}
		if (cur_element_count > 1) {
			int min1 = inbound_connections_num[0], max1 = inbound_connections_num[0];
			for (int i = 0; i < cur_element_count; i++) {
				assert(inbound_connections_num[i] > 0);
				min1 = std::min(inbound_connections_num[i], min1);
				max1 = std::max(inbound_connections_num[i], max1);
			}
			std::cout << "Min inbound: " << min1 << ", Max inbound:" << max1 << "\n";
		}
		std::cout << "integrity ok, checked " << connections_checked << " connections\n";
	}
};

using HierarchicalNSWST = HierarchicalNSW<Synchronization::None>;

using HierarchicalNSWMT = HierarchicalNSW<Synchronization::OnInsertions>;

}  // namespace hnswlib
