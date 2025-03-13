#pragma once

#include "estl/fast_hash_set.h"
#include "vendor/hopscotch/hopscotch_sc_map.h"
#include "visited_list_pool.h"
#include "hnswlib.h"
#include "tools/errors.h"
#include <atomic>
#include <random>
#include <stdlib.h>
#include <assert.h>
#include <memory>

#include "estl/shared_mutex.h"
#include "estl/mutex.h"
#include "tools/flagguard.h"

namespace hnswlib {
typedef uint32_t tableint;
typedef uint32_t linklistsizeint;
static_assert(sizeof(linklistsizeint) == sizeof(tableint), "Internal link lists logic requires the same size for this types");

enum class Synchronization {
    None,
    OnInsertions
};

class LabelOpsMutexLocks {
public:
    constexpr static bool is_locking = true;
    using mutex_t = std::mutex;

    LabelOpsMutexLocks() = default;
    LabelOpsMutexLocks(tableint max_locks) : mtxs_(max_locks) {}
    inline mutex_t& GetLabelOpMutex(labeltype label) const noexcept {
        // calculate hash
        size_t lock_id = label & (mtxs_.size() - 1);
        return mtxs_[lock_id];
    }
    mutex_t& operator[](tableint id) const noexcept {
        return mtxs_[id];
    }
    void swap(LabelOpsMutexLocks& o) noexcept {
        mtxs_.swap(o.mtxs_);
    }
    size_t allocatedMemSize() const noexcept {
        return mtxs_.capacity() * sizeof(mutex_t);
    }

private:
    mutable std::vector<mutex_t> mtxs_;
};

class DummyMutex {
public:
    constexpr void lock() const noexcept {}
    constexpr void lock_shared() const noexcept {}
    constexpr bool try_lock() const noexcept { return true; }
    constexpr void unlock() const noexcept {}
    constexpr void unlock_shared() const noexcept {}
};

class LabelOpsDummyLocks {
public:
    constexpr static bool is_locking = false;
    using mutex_t = DummyMutex;

    LabelOpsDummyLocks() = default;
    LabelOpsDummyLocks(tableint /*max_locks*/) {}
    inline mutex_t& GetLabelOpMutex(labeltype /*label*/) const noexcept {
        return mtx_;
    }
    mutex_t& operator[](tableint /*id*/) const noexcept {
        return mtx_;
    }
    void swap(LabelOpsDummyLocks&) const noexcept {
    }
    size_t allocatedMemSize() const noexcept {
        return sizeof(mtx_);
    }

private:
    mutable mutex_t mtx_;
};

class UpdateOpsMutexLocks {
public:
    constexpr static bool is_locking = true;
    using mutex_t = reindexer::read_write_spinlock;

    UpdateOpsMutexLocks() = default;
    UpdateOpsMutexLocks(tableint max_locks) : mtxs_(max_locks) {}
    mutex_t& operator[](tableint id) const noexcept {
        return mtxs_[id];
    }
    void swap(UpdateOpsMutexLocks& o) noexcept {
        mtxs_.swap(o.mtxs_);
    }
    size_t allocatedMemSize() const noexcept {
        return mtxs_.capacity() * sizeof(mutex_t);
    }

private:
    mutable std::vector<mutex_t> mtxs_;
};

using UpdateOpsDummyLocks = LabelOpsDummyLocks;


template<typename dist_t, Synchronization synchronization>
class HierarchicalNSW final : public AlgorithmInterface<dist_t> {
    using Base = AlgorithmInterface<dist_t>;

    template <typename K, typename V>
    using HashMapT = tsl::hopscotch_sc_map<K, V, std::hash<K>, std::equal_to<K>, std::less<K>,
                     std::allocator<std::pair<const K, V>>, 30, false, tsl::mod_growth_policy<std::ratio<3, 2>>>;
    template <typename K>
    using HashSetT = tsl::hopscotch_sc_set<K, std::hash<K>, std::equal_to<K>, std::less<K>,
                     std::allocator<K>, 30, false, tsl::mod_growth_policy<std::ratio<3, 2>>>;

    using lock_vec_t = std::conditional_t<synchronization == Synchronization::None, LabelOpsDummyLocks, LabelOpsMutexLocks>;
    using update_lock_vec_t = std::conditional_t<synchronization == Synchronization::None, UpdateOpsDummyLocks, UpdateOpsMutexLocks>;
#if REINDEX_USE_STD_SHARED_MUTEX
    using data_updates_shared_lock = std::shared_lock<typename update_lock_vec_t::mutex_t>;
#else
    using data_updates_shared_lock = reindexer::shared_lock<typename update_lock_vec_t::mutex_t>;
#endif
    using data_updates_unique_lock = std::lock_guard<typename update_lock_vec_t::mutex_t>;

 public:
    static const unsigned char DELETE_MARK = 0x01;

    size_t max_elements_{0};
    mutable std::atomic<size_t> cur_element_count{0};  // current number of elements
    size_t size_data_per_element_{0};
    size_t size_links_per_element_{0};
    size_t num_deleted_{0};  // number of deleted elements
    size_t M_{0};
    size_t maxM_{0};
    size_t maxM0_{0};
    size_t ef_construction_{0};

    double mult_{0.0};
    int maxlevel_{0};

    std::unique_ptr<VisitedListPool> visited_list_pool_{nullptr};

    typename lock_vec_t::mutex_t global;
    lock_vec_t link_list_locks_;
    update_lock_vec_t data_updates_locks_;

    tableint enterpoint_node_{0};

    size_t size_links_level0_{0};
    size_t offsetData_{0}, label_offset_{ 0 };

    char *data_level0_memory_{nullptr};
    char **linkLists_{nullptr};
    std::vector<int> element_levels_;  // keeps level of each element

    size_t data_size_{0};

    DistCalculator<dist_t> fstdistfunc_;

    mutable typename lock_vec_t::mutex_t label_lookup_lock;     // lock for label_lookup_
    HashMapT<labeltype, tableint> label_lookup_;

    mutable typename lock_vec_t::mutex_t generator_lock_;
    std::default_random_engine level_generator_;
    typename lock_vec_t::mutex_t update_generator_lock_;
    std::default_random_engine update_probability_generator_;

    mutable std::atomic<long> metric_distance_computations{0};
    mutable std::atomic<long> metric_hops{0};

    reindexer::ReplaceDeleted allow_replace_deleted_ = reindexer::ReplaceDeleted_False;  // flag to replace deleted elements (marked as deleted) during insertions

    typename lock_vec_t::mutex_t deleted_elements_lock;  // lock for deleted_elements
    HashSetT<tableint> deleted_elements;  // contains internal ids of deleted elements

    std::atomic<int32_t> concurrent_updates_counter_{0};


    HierarchicalNSW(SpaceInterface<dist_t> *) {
    }

    HierarchicalNSW(SpaceInterface<dist_t> *s, const HierarchicalNSW& other, size_t newMaxElements)
    :
        max_elements_(std::max(other.max_elements_, newMaxElements)),
        cur_element_count(other.cur_element_count.load()),
        size_data_per_element_(other.size_data_per_element_),
        num_deleted_(other.num_deleted_),
        M_(other.M_),
        maxM_(other.maxM_),
        maxM0_(other.maxM0_),
        ef_construction_(other.ef_construction_),
        mult_(other.mult_),
        maxlevel_(other.maxlevel_),
        visited_list_pool_(nullptr),
        enterpoint_node_(other.enterpoint_node_),
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
                             linkLists_[i] = (char *) malloc(linkListSize);
                             if (linkLists_[i] == nullptr) {
                                 throw std::runtime_error("Not enough memory: HNSW copy-constructor failed to allocate linklist");
                             }
                             std::memcpy(linkLists_[i], other.linkLists_[i], linkListSize);
                         } else {
                             linkLists_[i] = nullptr;
                         }
                         return linkListSize;
                }
            );
            assertrx_dbg(num_deleted_ == deleted_elements.size());
        }


    HierarchicalNSW(
        SpaceInterface<dist_t> *s,
        size_t max_elements,
        size_t M = 16,
        size_t ef_construction = 200,
        size_t random_seed = 100,
        reindexer::ReplaceDeleted allow_replace_deleted = reindexer::ReplaceDeleted_False)
        : link_list_locks_(max_elements),
            data_updates_locks_(max_elements),
            element_levels_(max_elements),
            allow_replace_deleted_(allow_replace_deleted) {
        max_elements_ = max_elements;
        num_deleted_ = 0;
        data_size_ = s->get_data_size();
        fstdistfunc_ = DistCalculator{s->get_dist_calculator_param(), max_elements_};
        if ( M > 10000 ) {
            HNSWERR << "warning: M parameter exceeds 10000 which may lead to adverse effects." << std::endl;
            HNSWERR << "         Cap to 10000 will be applied for the rest of the processing." << std::endl;
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

        data_level0_memory_ = (char *) malloc(max_elements_ * size_data_per_element_);
        if (data_level0_memory_ == nullptr)
            throw std::runtime_error("Not enough memory");

        cur_element_count = 0;

        visited_list_pool_ = std::unique_ptr<VisitedListPool>(new VisitedListPool(1, max_elements));

        // initializations for special treatment of the first node
        enterpoint_node_ = std::numeric_limits<tableint>::max();
        maxlevel_ = -1;

        linkLists_ = (char **) malloc(sizeof(void *) * max_elements_);
        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: HierarchicalNSW failed to allocate linklists");
        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
        mult_ = getMultFromM(M_);
    }


    ~HierarchicalNSW() {
        clear();
    }

    static double getMultFromM(size_t M) noexcept {
        return 1 / log(1.0 * M);
    }

    void setM(size_t M) noexcept {
        M_ = M;
        maxM0_ = 2 * M;
        maxM_ = M;
    }

    void clear() {
        free(data_level0_memory_);
        data_level0_memory_ = nullptr;
        for (tableint i = 0; i < cur_element_count; i++) {
            if (element_levels_[i] > 0)
                free(linkLists_[i]);
        }
        free(linkLists_);
        linkLists_ = nullptr;
        cur_element_count = 0;
        visited_list_pool_.reset(nullptr);
        deleted_elements.clear();
        num_deleted_ = 0;
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


    struct CompareByFirst {
        constexpr bool operator()(std::pair<dist_t, tableint> const& a,
            std::pair<dist_t, tableint> const& b) const noexcept {
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


    inline labeltype *getExternalLabeLp(tableint internal_id) const {
        return (labeltype *) (data_level0_memory_ + internal_id * size_data_per_element_ + label_offset_);
    }


    inline char *getDataByInternalId(tableint internal_id) const {
        return (data_level0_memory_ + internal_id * size_data_per_element_ + offsetData_);
    }


    int getRandomLevel(double reverse_size) {
        std::uniform_real_distribution<double> distribution(0.0, 1.0);
        double val;
        {
            std::lock_guard lck(generator_lock_);
            val = distribution(level_generator_);
        }
        double r = -log(val) * reverse_size;
        return (int) r;
    }

    size_t getMaxElements() const noexcept override {
        return max_elements_;
    }

    size_t getCurrentElementCount() const noexcept override {
        return cur_element_count;
    }

    size_t getDeletedCount() {
        std::lock_guard lck(deleted_elements_lock);
        return num_deleted_;
    }

    template<bool with_concurrent_updates>
    std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
    searchBaseLayer(tableint ep_id, const void *data_point, tableint data_point_id, int layer) {
        VisitedList *vl = visited_list_pool_->getFreeVisitedList();
        vl_type *visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        using pair_t = std::pair<dist_t, tableint>;
        std::vector<pair_t> container1, container2;
        container1.reserve(256);
        container2.reserve(256);
        std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> top_candidates(CompareByFirst(), std::move(container1));
        std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> candidateSet(CompareByFirst(), std::move(container2));

        dist_t lowerBound;
        {
            data_updates_shared_lock lck;
            if constexpr (with_concurrent_updates) {
                lck = data_updates_shared_lock(data_updates_locks_[ep_id]);
            }
            if (!isMarkedDeleted(ep_id)) {
                dist_t dist = fstdistfunc_(data_point, data_point_id, getDataByInternalId(ep_id), ep_id);
                top_candidates.emplace(dist, ep_id);
                lowerBound = dist;
                candidateSet.emplace(-dist, ep_id);
            } else {
                lowerBound = std::numeric_limits<dist_t>::max();
                candidateSet.emplace(-lowerBound, ep_id);
            }
        }
        visited_array[ep_id] = visited_array_tag;

        while (!candidateSet.empty()) {
            std::pair<dist_t, tableint> curr_el_pair = candidateSet.top();
            if ((-curr_el_pair.first) > lowerBound && top_candidates.size() == ef_construction_) {
                break;
            }
            candidateSet.pop();

            tableint curNodeNum = curr_el_pair.second;

            std::unique_lock lock(link_list_locks_[curNodeNum]);

            int *data;  // = (int *)(linkList0_ + curNodeNum * size_links_per_element0_);
            if (layer == 0) {
                data = (int*)get_linklist0(curNodeNum);
            } else {
                data = (int*)get_linklist(curNodeNum, layer);
//                    data = (int *) (linkLists_[curNodeNum] + (layer - 1) * size_links_per_element_);
            }
            size_t size = getListCount((linklistsizeint*)data);
            tableint *datal = (tableint *) (data + 1);
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) // Asan reports overflow on prefetch
            _mm_prefetch((char *) (visited_array + *(data + 1)), _MM_HINT_T0);
            _mm_prefetch((char *) (visited_array + *(data + 1) + 64), _MM_HINT_T0);
            _mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
            _mm_prefetch(getDataByInternalId(*(datal + 1)), _MM_HINT_T0);
#endif // defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN)

            for (size_t j = 0; j < size; j++) {
                tableint candidate_id = *(datal + j);
//                    if (candidate_id == 0) continue;
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) // Asan reports overflow on prefetch
                _mm_prefetch((char *) (visited_array + *(datal + j + 1)), _MM_HINT_T0);
                _mm_prefetch(getDataByInternalId(*(datal + j + 1)), _MM_HINT_T0);
#endif // defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN)
                if (visited_array[candidate_id] == visited_array_tag) continue;
                visited_array[candidate_id] = visited_array_tag;

                data_updates_shared_lock lck;
                if constexpr (with_concurrent_updates) {
                    lck = data_updates_shared_lock(data_updates_locks_[candidate_id]);
                }
                dist_t dist1 = fstdistfunc_(data_point, data_point_id, getDataByInternalId(candidate_id), candidate_id);
                if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
                    candidateSet.emplace(-dist1, candidate_id);
#if REINDEXER_WITH_SSE
                    _mm_prefetch(getDataByInternalId(candidateSet.top().second), _MM_HINT_T0);
#endif // REINDEXER_WITH_SSE

                    if (!isMarkedDeleted(candidate_id))
                        top_candidates.emplace(dist1, candidate_id);

                    if (top_candidates.size() > ef_construction_)
                        top_candidates.pop();

                    if (!top_candidates.empty())
                        lowerBound = top_candidates.top().first;
                }
            }
        }
        visited_list_pool_->releaseVisitedList(vl);

        return top_candidates;
    }


    // bare_bone_search means there is no check for deletions and stop condition is ignored in return of extra performance
    template <bool bare_bone_search = true, bool collect_metrics = false>
    std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
    searchBaseLayerST(
        tableint ep_id,
        const void *data_point,
        size_t ef,
        BaseFilterFunctor* isIdAllowed = nullptr,
        BaseSearchStopCondition<dist_t>* stop_condition = nullptr) const {
        VisitedList *vl = visited_list_pool_->getFreeVisitedList();
        vl_type *visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        using pair_t = std::pair<dist_t, tableint>;
        std::vector<pair_t> container1, container2;
        container1.reserve(ef);
        container2.reserve(ef);
        std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> top_candidates(CompareByFirst(), std::move(container1));
        std::priority_queue<pair_t, std::vector<pair_t>, CompareByFirst> candidate_set(CompareByFirst(), std::move(container1));

        dist_t lowerBound;
        if (bare_bone_search ||
            (!isMarkedDeleted(ep_id) && ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(ep_id))))) {
            auto ep_data = getDataByInternalId(ep_id);
            dist_t dist = fstdistfunc_(data_point, ep_data, ep_id);
            lowerBound = dist;
            top_candidates.emplace(dist, ep_id);
            if (!bare_bone_search && stop_condition) {
                stop_condition->add_point_to_result(getExternalLabel(ep_id), ep_data, dist);
            }
            candidate_set.emplace(-dist, ep_id);
        } else {
            lowerBound = std::numeric_limits<dist_t>::max();
            candidate_set.emplace(-lowerBound, ep_id);
        }

        visited_array[ep_id] = visited_array_tag;

        while (!candidate_set.empty()) {
            std::pair<dist_t, tableint> current_node_pair = candidate_set.top();
            dist_t candidate_dist = -current_node_pair.first;

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
            int *data = (int *) get_linklist0(current_node_id);
            size_t size = getListCount((linklistsizeint*)data);
//                bool cur_node_deleted = isMarkedDeleted(current_node_id);
            if (collect_metrics) {
                metric_hops++;
                metric_distance_computations+=size;
            }

#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) // Asan reports overflow on prefetch
            _mm_prefetch((char *) (visited_array + *(data + 1)), _MM_HINT_T0);
            _mm_prefetch((char *) (visited_array + *(data + 1) + 64), _MM_HINT_T0);
            _mm_prefetch(data_level0_memory_ + (*(data + 1)) * size_data_per_element_ + offsetData_, _MM_HINT_T0);
            _mm_prefetch((char *) (data + 2), _MM_HINT_T0);
#endif // defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN)

            for (size_t j = 1; j <= size; j++) {
                int candidate_id = *(data + j);
//                    if (candidate_id == 0) continue;
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) // Asan reports overflow on prefetch
                _mm_prefetch((char *) (visited_array + *(data + j + 1)), _MM_HINT_T0);
                _mm_prefetch(data_level0_memory_ + (*(data + j + 1)) * size_data_per_element_ + offsetData_,
                                _MM_HINT_T0);  ////////////
#endif // defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN)
                if (!(visited_array[candidate_id] == visited_array_tag)) {
                    visited_array[candidate_id] = visited_array_tag;

                    char *currObj1 = (getDataByInternalId(candidate_id));
                    dist_t dist = fstdistfunc_(data_point, currObj1, candidate_id);
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
                                        _MM_HINT_T0);  ////////////////////////
#endif // REINDEXER_WITH_SSE

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

                        if (!top_candidates.empty())
                            lowerBound = top_candidates.top().first;
                    }
                }
            }
        }

        visited_list_pool_->releaseVisitedList(vl);
        return top_candidates;
    }

    template<bool with_concurrent_updates>
    void getNeighborsByHeuristic2(
        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> &top_candidates,
        const size_t M) {
        if (top_candidates.size() < M) {
            return;
        }

        std::priority_queue<std::pair<dist_t, tableint>> queue_closest;
        std::vector<std::pair<dist_t, tableint>> return_list;
        while (top_candidates.size() > 0) {
            queue_closest.emplace(-top_candidates.top().first, top_candidates.top().second);
            top_candidates.pop();
        }

        while (queue_closest.size()) {
            if (return_list.size() >= M)
                break;
            std::pair<dist_t, tableint> curent_pair = queue_closest.top();
            dist_t dist_to_query = -curent_pair.first;
            queue_closest.pop();
            bool good = true;

            for (std::pair<dist_t, tableint> second_pair : return_list) {
                data_updates_shared_lock lck1, lck2;
                if constexpr (with_concurrent_updates) {
                    lck1 = data_updates_shared_lock(data_updates_locks_[std::min(curent_pair.second, second_pair.second)]);
                    if (curent_pair.second != second_pair.second) {
                        lck2 = data_updates_shared_lock(data_updates_locks_[std::max(curent_pair.second, second_pair.second)]);
                    }
                }
                dist_t curdist = fstdistfunc_(getDataByInternalId(second_pair.second), second_pair.second,
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

        for (std::pair<dist_t, tableint> curent_pair : return_list) {
            top_candidates.emplace(-curent_pair.first, curent_pair.second);
        }
    }


    linklistsizeint *get_linklist0(tableint internal_id) const {
        return (linklistsizeint *) (data_level0_memory_ + internal_id * size_data_per_element_);
    }


    linklistsizeint *get_linklist0(tableint internal_id, char *data_level0_memory_) const {
        return (linklistsizeint *) (data_level0_memory_ + internal_id * size_data_per_element_);
    }


    linklistsizeint *get_linklist(tableint internal_id, int level) const {
        return (linklistsizeint *) (linkLists_[internal_id] + (level - 1) * size_links_per_element_);
    }


    linklistsizeint *get_linklist_at_level(tableint internal_id, int level) const {
        return level == 0 ? get_linklist0(internal_id) : get_linklist(internal_id, level);
    }


    template<bool with_concurrent_updates>
    tableint mutuallyConnectNewElement(
        tableint cur_c,
        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> &top_candidates,
        int level,
        bool isUpdate) {
        size_t Mcurmax = level ? maxM_ : maxM0_;
        getNeighborsByHeuristic2<with_concurrent_updates>(top_candidates, M_);
        if (top_candidates.size() > M_)
            throw std::runtime_error("Should be not be more than M_ candidates returned by the heuristic");

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
            std::unique_lock lock(link_list_locks_[cur_c], std::defer_lock);
            if (isUpdate) {
                lock.lock();
            }
            linklistsizeint *ll_cur;
            if (level == 0)
                ll_cur = get_linklist0(cur_c);
            else
                ll_cur = get_linklist(cur_c, level);

            if (*ll_cur && !isUpdate) {
                throw std::runtime_error("The newly inserted element should have blank link list");
            }
            setListCount(ll_cur, selectedNeighbors.size());
            tableint *data = (tableint *) (ll_cur + 1);
            for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
                if (data[idx] && !isUpdate)
                    throw std::runtime_error("Possible memory corruption");
                if (level > element_levels_[selectedNeighbors[idx]])
                    throw std::runtime_error("Trying to make a link on a non-existent level");

                data[idx] = selectedNeighbors[idx];
            }
        }

        for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
            std::lock_guard lock(link_list_locks_[selectedNeighbors[idx]]);

            linklistsizeint *ll_other;
            if (level == 0)
                ll_other = get_linklist0(selectedNeighbors[idx]);
            else
                ll_other = get_linklist(selectedNeighbors[idx], level);

            size_t sz_link_list_other = getListCount(ll_other);

            if (sz_link_list_other > Mcurmax)
                throw std::runtime_error("Bad value of sz_link_list_other");
            if (selectedNeighbors[idx] == cur_c)
                throw std::runtime_error("Trying to connect an element to itself");
            if (level > element_levels_[selectedNeighbors[idx]])
                throw std::runtime_error("Trying to make a link on a non-existent level");

            tableint *data = (tableint *) (ll_other + 1);

            bool is_cur_c_present = false;
            if (isUpdate) {
                for (size_t j = 0; j < sz_link_list_other; j++) {
                    if (data[j] == cur_c) {
                        is_cur_c_present = true;
                        break;
                    }
                }
            }

            // If cur_c is already present in the neighboring connections of `selectedNeighbors[idx]` then no need to modify any connections or run the heuristics.
            if (!is_cur_c_present) {
                if (sz_link_list_other < Mcurmax) {
                    data[sz_link_list_other] = cur_c;
                    setListCount(ll_other, sz_link_list_other + 1);
                } else {
                    // finding the "weakest" element to replace it with the new one
                    const auto id2 = selectedNeighbors[idx];
                    dist_t d_max = fstdistfunc_(getDataByInternalId(cur_c), cur_c, getDataByInternalId(selectedNeighbors[idx]), id2);
                    // Heuristic:
                    std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> candidates;
                    candidates.emplace(d_max, cur_c);

                    for (size_t j = 0; j < sz_link_list_other; j++) {
                        const auto id1 = data[j], id2 = selectedNeighbors[idx];
                        candidates.emplace(
                                fstdistfunc_(getDataByInternalId(data[j]), id1, getDataByInternalId(selectedNeighbors[idx]), id2),
                            data[j]);
                    }

                    getNeighborsByHeuristic2<with_concurrent_updates>(candidates, Mcurmax);

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


    typename Base::ResizeResult resizeIndex(size_t new_max_elements) override {
        if (new_max_elements < cur_element_count)
            throw std::runtime_error("Cannot resize, max element is less than the current number of elements");

        visited_list_pool_.reset(new VisitedListPool(1, new_max_elements));

        element_levels_.resize(new_max_elements);

        lock_vec_t(new_max_elements).swap(link_list_locks_);
        update_lock_vec_t(new_max_elements).swap(data_updates_locks_);

        // Reallocate base layer
        char * data_level0_memory_new = (char *) realloc(data_level0_memory_, new_max_elements * size_data_per_element_);
        if (data_level0_memory_new == nullptr)
            throw std::runtime_error("Not enough memory: resizeIndex failed to allocate base layer");
        const void* data_level0_memory_old = std::exchange(data_level0_memory_, data_level0_memory_new);

        // Reallocate all other layers
        char ** linkLists_new = (char **) realloc(linkLists_, sizeof(void *) * new_max_elements);
        if (linkLists_new == nullptr)
            throw std::runtime_error("Not enough memory: resizeIndex failed to allocate other layers");
        linkLists_ = linkLists_new;

        max_elements_ = new_max_elements;
        fstdistfunc_.Resize(max_elements_);
          return {data_level0_memory_old, data_level0_memory_new};
    }

    size_t indexFileSize() const {
        size_t size = 0;
        size += sizeof(max_elements_);
        size += sizeof(cur_element_count);
        size += sizeof(size_data_per_element_);
        size += sizeof(label_offset_);
        size += sizeof(offsetData_);
        size += sizeof(maxlevel_);
        size += sizeof(enterpoint_node_);
        size += sizeof(maxM_);

        size += sizeof(maxM0_);
        size += sizeof(M_);
        size += sizeof(mult_);
        size += sizeof(ef_construction_);

        size += cur_element_count * size_data_per_element_;

        for (size_t i = 0; i < cur_element_count; i++) {
            unsigned int linkListSize = element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
            size += sizeof(linkListSize);
            size += linkListSize;
        }
        return size;
    }

    void saveIndex(IWriter& writer, const std::atomic_int32_t& cancel) {
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
            const auto* linkList0 = reinterpret_cast<const tableint *>(cur_element_ptr);
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

    void initSpace(SpaceInterface<dist_t> *s) {
        data_size_ = s->get_data_size();
        fstdistfunc_ = DistCalculator{s->get_dist_calculator_param(), max_elements_};

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
        offsetData_ = size_links_level0_;
        label_offset_ = size_links_level0_ + data_size_;

        data_level0_memory_ = (char *) malloc(max_elements_ * size_data_per_element_);
        if (data_level0_memory_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate level0");
    }

    template<typename F>
    void initTree(F fillLinkList) {
        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        lock_vec_t(max_elements_).swap(link_list_locks_);
        update_lock_vec_t(max_elements_).swap(data_updates_locks_);

        visited_list_pool_.reset(new VisitedListPool(1, max_elements_));

        linkLists_ = (char **) malloc(sizeof(void *) * max_elements_);
        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklists");
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

    void loadIndex(IReader& reader, SpaceInterface<dist_t> *s) {
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
            auto* linkList0 = reinterpret_cast<tableint *>(cur_element_ptr);
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
                        linkLists_[i] = (char *) malloc(list.size());
                        if (linkLists_[i] == nullptr) {
                            throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklist");
                        }
                        std::memcpy(linkLists_[i], list.data(), list.size());
                    } else {
                        linkLists_[i] = nullptr;
                    }
                    return list.size();
                }
        );
#ifdef RX_WITH_STDLIB_DEBUG
        if (allow_replace_deleted_) {
                assertrx_dbg(num_deleted_ == deleted_elements.size());
        }
#endif // RX_WITH_STDLIB_DEBUG
    }

    tableint getInternalIdByLabel(labeltype label) const {
        // lock all operations with element by label
        // std::unique_lock <std::mutex> lock_label(getLabelOpMutex(label));

        // std::unique_lock <std::mutex> lock_table(label_lookup_lock);
        auto search = label_lookup_.find(label);
        const bool found = (search != label_lookup_.end());
        if (!found || isMarkedDeleted(search->second)) {
                using namespace reindexer;
                assertf_dbg(false, "getLabel: Label not found: {}", label);
                throw std::runtime_error("getLabel: Label not found: " + std::to_string(label));
        }
        return search->second;
    }

    const char* ptrByExternalLabel(labeltype label) const override {
        return getDataByInternalId(getInternalIdByLabel(label));
    }

    /*
    * Marks an element with the given label deleted, does NOT really change the current graph.
    */
    // *NOT* thread-safe
    void markDelete(labeltype label) {
        // lock all operations with element by label
        // std::unique_lock <std::mutex> lock_label(getLabelOpMutex(label));

        // std::unique_lock <std::mutex> lock_table(label_lookup_lock);
        auto search = label_lookup_.find(label);
        if (search == label_lookup_.end()) {
                using namespace reindexer;
                assertf_dbg(false, "markDelete: Label not found:  {}", label);
                throw std::runtime_error("markDelete: Label not found: " + std::to_string(label));
        }
        tableint internalId = search->second;
        // lock_table.unlock();

        markDeletedInternal(internalId);
    }


    /*
    * Uses the last 16 bits of the memory for the linked list size to store the mark,
    * whereas maxM0_ has to be limited to the lower 16 bits, however, still large enough in almost all cases.
    */
    // *NOT* thread-safe
    void markDeletedInternal(tableint internalId) {
        assert(internalId < cur_element_count);
        if (!isMarkedDeleted(internalId)) {
            unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId))+2;
            *ll_cur |= DELETE_MARK;
            num_deleted_ += 1;
            if (allow_replace_deleted_) {
                deleted_elements.insert(internalId);
                assertrx_dbg(num_deleted_ == deleted_elements.size());
            }
        } else {
            throw std::runtime_error("The requested to delete element is already deleted");
        }
    }


    /*
    * Remove the deleted mark of the node.
    */
    void unmarkDeletedInternal(tableint internalId) {
        assert(internalId < cur_element_count);
        if (isMarkedDeleted(internalId)) {
            unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
            *ll_cur &= ~DELETE_MARK;
            if (allow_replace_deleted_) {
                std::unique_lock lock_deleted_elements(deleted_elements_lock);
                if (deleted_elements.erase(internalId)) {
                    num_deleted_ -= 1;
                }
                assertrx_dbg(num_deleted_ == deleted_elements.size());
            } else {
                std::unique_lock lock_deleted_elements(deleted_elements_lock);
                num_deleted_ -= 1;
            }
        } else {
            throw std::runtime_error("The requested to undelete element is not deleted");
        }
    }


    /*
    * Checks the first 16 bits of the memory to see if the element is marked deleted.
    */
    bool isListMarkedDeleted(const linklistsizeint* linkList0) const noexcept {
        const unsigned char *ll_cur = ((const unsigned char*)linkList0) + 2;
        return *ll_cur & DELETE_MARK;
    }
    bool isMarkedDeleted(tableint internalId) const noexcept {
        return isListMarkedDeleted(get_linklist0(internalId));
    }


    unsigned short int getListCount(const linklistsizeint * ptr) const {
        return *((const unsigned short int *)ptr);
    }


    void setListCount(linklistsizeint * ptr, unsigned short int size) const {
        *((unsigned short int*)(ptr))=*((unsigned short int *)&size);
    }


    /*
    * Adds point. Updates the point if it is already in the index.
    * If replacement of deleted elements is enabled: replaces previously deleted point if any, updating it with new point
    */
    void addPoint(const void *data_point, labeltype label, reindexer::ReplaceDeleted replace_deleted = reindexer::ReplaceDeleted_False) override {
        if (!allow_replace_deleted_ && replace_deleted) {
            throw std::runtime_error("Replacement of deleted elements is disabled in constructor");
        }

        // lock all operations with element by label
        // std::unique_lock <std::mutex> lock_label(getLabelOpMutex(label));
        if (!replace_deleted) {
            addPoint<false>(data_point, label, -1);
            return;
        }
        // check if there is vacant place
        tableint internal_id_replaced;
        std::unique_lock lock_deleted_elements(deleted_elements_lock);
        bool is_vacant_place = !deleted_elements.empty();
        reindexer::CounterGuardAIR32 updatesCounter;
        if (is_vacant_place) {
            internal_id_replaced = *deleted_elements.begin();
            deleted_elements.erase(internal_id_replaced);
            num_deleted_ -= 1;
            updatesCounter = reindexer::CounterGuardAIR32(concurrent_updates_counter_);
        }
        lock_deleted_elements.unlock();

        // if there is no vacant place then add or update point
        // else add point to vacant place
        if (!is_vacant_place) {
            if (concurrent_updates_counter_.load(std::memory_order_acquire)) {
                // There are concurrent updates running.
                // Calling version with extra data synchronization.
                addPoint<true>(data_point, label, -1);
            } else {
                // There are no concurrent updates running. Due to higher level logic, new updates will never appear during the insertion loop.
                // Calling unsafe version without extra data synchronization.
                addPoint<false>(data_point, label, -1);
            }
        } else {
            // we assume that there are no concurrent operations on deleted element
            labeltype label_replaced = getExternalLabel(internal_id_replaced);
            setExternalLabel(internal_id_replaced, label);

            {
                std::lock_guard lock_table(label_lookup_lock);
                auto foundReplaced = label_lookup_.find(label_replaced);
                // Erase replced label from label_lookup, if it was not updated yet and still points to the our's new data
                if (foundReplaced != label_lookup_.end() && foundReplaced.value() == internal_id_replaced) {
                    label_lookup_.erase(foundReplaced);
                }
                label_lookup_[label] = internal_id_replaced;
            }

            try {
                updatePoint(data_point, internal_id_replaced, 1.0);
            } catch(...) {
                std::lock_guard lock_table(label_lookup_lock);
                std::unique_lock lock_deleted_elements(deleted_elements_lock);
                if (!isMarkedDeleted(internal_id_replaced)) {
                    markDeletedInternal(internal_id_replaced);
                }
                // Handling direct deleted_elements.erase called above
                auto [_, inserted] = deleted_elements.emplace(internal_id_replaced);
                if (inserted) {
                    num_deleted_ -= 1;
                }
                throw;
            }
        }
    }


    void updatePoint(const void *dataPoint, tableint internalId, float updateNeighborProbability) {
        {
            // FIXME: There is still logical race, that reduces recall rate - we may update one of the current insertion candidate nodes right after dist calculation
            // Check #2029 for details.
            data_updates_unique_lock lck(data_updates_locks_[internalId]);
            // update the feature vector associated with existing point with new vector
            memcpy(getDataByInternalId(internalId), dataPoint, data_size_);
            fstdistfunc_.AddNorm(dataPoint, internalId);
            if (isMarkedDeleted(internalId)) {
                unmarkDeletedInternal(internalId);
            }
        }

        int maxLevelCopy = maxlevel_;
        tableint entryPointCopy = enterpoint_node_;
        // If point to be updated is entry point and graph just contains single element then just return.
        if (entryPointCopy == internalId && cur_element_count == 1)
            return;

        int elemLevel = element_levels_[internalId];
        std::uniform_real_distribution<float> distribution(0.0, 1.0);

        for (int layer = 0; layer <= elemLevel; layer++) {
            reindexer::fast_hash_set<tableint> sCand;
            reindexer::fast_hash_set<tableint> sNeigh;
            std::vector<tableint> listOneHop = getConnectionsWithLock(internalId, layer);
            if (listOneHop.size() == 0)
                continue;

            sCand.insert(internalId);

            for (auto&& elOneHop : listOneHop) {
                sCand.insert(elOneHop);

                if (updateNeighborProbability != 1.0) {
                    assertrx_dbg(false); // We do not use this logic
                    std::lock_guard lck(update_generator_lock_);
                    if (distribution(update_probability_generator_) > updateNeighborProbability)
                        continue;
                }

                sNeigh.insert(elOneHop);

                std::vector<tableint> listTwoHop = getConnectionsWithLock(elOneHop, layer);
                for (auto&& elTwoHop : listTwoHop) {
                    sCand.insert(elTwoHop);
                }
            }

            for (auto&& neigh : sNeigh) {
                // if (neigh == internalId)
                //     continue;

                std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> candidates;
                size_t size = sCand.find(neigh) == sCand.end() ? sCand.size() : sCand.size() - 1;  // sCand guaranteed to have size >= 1
                size_t elementsToKeep = std::min(ef_construction_, size);
                for (auto&& cand : sCand) {
                    if (cand == neigh)
                        continue;

                    data_updates_shared_lock lck1(data_updates_locks_[std::min(neigh, cand)]);
                    assertrx_dbg(neigh != cand); // Essential for the second lock
                    data_updates_shared_lock lck2(data_updates_locks_[std::max(neigh, cand)]);
                    dist_t distance = fstdistfunc_(getDataByInternalId(neigh), neigh, getDataByInternalId(cand), cand);
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
                getNeighborsByHeuristic2<true>(candidates, layer == 0 ? maxM0_ : maxM_);

                {
                    std::unique_lock lock(link_list_locks_[neigh]);
                    linklistsizeint *ll_cur;
                    ll_cur = get_linklist_at_level(neigh, layer);
                    size_t candSize = candidates.size();
                    setListCount(ll_cur, candSize);
                    tableint *data = (tableint *) (ll_cur + 1);
                    for (size_t idx = 0; idx < candSize; idx++) {
                        data[idx] = candidates.top().second;
                        candidates.pop();
                    }
                }
            }
        }

        repairConnectionsForUpdate(dataPoint, entryPointCopy, internalId, elemLevel, maxLevelCopy);
    }


    void repairConnectionsForUpdate(
        const void *dataPoint,
        tableint entryPointInternalId,
        tableint dataPointInternalId,
        int dataPointLevel,
        int maxLevel) {
        tableint currObj = entryPointInternalId;
        if (dataPointLevel < maxLevel) {
            dist_t curdist;
            {
                data_updates_shared_lock lck(data_updates_locks_[currObj]);
                curdist = fstdistfunc_(dataPoint, dataPointInternalId, getDataByInternalId(currObj), currObj);
            }
            for (int level = maxLevel; level > dataPointLevel; level--) {
                bool changed = true;
                while (changed) {
                    changed = false;
                    unsigned int *data;
                    std::unique_lock lock(link_list_locks_[currObj]);
                    data = get_linklist_at_level(currObj, level);
                    int size = getListCount(data);
                    tableint *datal = (tableint *) (data + 1);
#if REINDEXER_WITH_SSE
                    _mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
#endif // REINDEXER_WITH_SSE
                    for (int i = 0; i < size; i++) {
#if defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN) // Asan reports overflow on prefetch
                        _mm_prefetch(getDataByInternalId(*(datal + i + 1)), _MM_HINT_T0);
#endif // defined(REINDEXER_WITH_SSE) && !defined(REINDEX_WITH_ASAN)
                        tableint cand = datal[i];

                        dist_t d;
                        {
                            data_updates_shared_lock lck(data_updates_locks_[cand]);
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

        if (dataPointLevel > maxLevel)
            throw std::runtime_error("Level of item to be updated cannot be bigger than max level");

        for (int level = dataPointLevel; level >= 0; level--) {
            std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> topCandidates =
                searchBaseLayer<true>(currObj, dataPoint, dataPointInternalId,  level);

            std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> filteredTopCandidates;
            while (topCandidates.size() > 0) {
                if (topCandidates.top().second != dataPointInternalId)
                    filteredTopCandidates.push(topCandidates.top());

                topCandidates.pop();
            }

            // Since element_levels_ is being used to get `dataPointLevel`, there could be cases where `topCandidates` could just contains entry point itself.
            // To prevent self loops, the `topCandidates` is filtered and thus can be empty.
            if (filteredTopCandidates.size() > 0) {
                {
                    data_updates_shared_lock lck(data_updates_locks_[entryPointInternalId]);
                    bool epDeleted = isMarkedDeleted(entryPointInternalId);
                    if (epDeleted) {
                        filteredTopCandidates.emplace(fstdistfunc_(dataPoint, dataPointInternalId, getDataByInternalId(entryPointInternalId), entryPointInternalId), entryPointInternalId);
                        if (filteredTopCandidates.size() > ef_construction_)
                            filteredTopCandidates.pop();
                    }
                }

                currObj = mutuallyConnectNewElement<true>(dataPointInternalId, filteredTopCandidates, level, true);
            }
        }
    }


    std::vector<tableint> getConnectionsWithLock(tableint internalId, int level) {
        std::unique_lock lock(link_list_locks_[internalId]);
        unsigned int *data = get_linklist_at_level(internalId, level);
        int size = getListCount(data);
        std::vector<tableint> result(size);
        tableint *ll = (tableint *) (data + 1);
        memcpy(result.data(), ll, size * sizeof(tableint));
        return result;
    }


    template<bool with_concurrent_updates>
    tableint addPoint(const void *data_point, labeltype label, int level) {
        tableint cur_c = 0;
        {
            // Checking if the element with the same label already exists
            // if so, updating it *instead* of creating a new element.
            std::unique_lock lock_table(label_lookup_lock);
            auto search = label_lookup_.find(label);
            if (search != label_lookup_.end()) {
                tableint existingInternalId = search->second;
                if (allow_replace_deleted_) {
                    if (isMarkedDeleted(existingInternalId)) {
                        throw std::runtime_error("Can't use addPoint to update deleted elements if replacement of deleted elements is enabled.");
                    }
                }
                reindexer::CounterGuardAIR32 updatesCounter(concurrent_updates_counter_);
                assertrx_dbg(false); // This logic was never be called and may require extra testing
                lock_table.unlock();

                updatePoint(data_point, existingInternalId, 1.0);

                return existingInternalId;
            }

            if (cur_element_count >= max_elements_) {
                throw std::runtime_error("The number of elements exceeds the specified limit");
            }

            cur_c = cur_element_count;
            cur_element_count++;
            label_lookup_[label] = cur_c;
            fstdistfunc_.AddNorm(data_point, cur_c);
        }

        int curlevel = getRandomLevel(mult_);
        if (level > 0)
            curlevel = level;

        std::unique_lock templock(global);
        std::lock_guard lock_el(link_list_locks_[cur_c]);
        element_levels_[cur_c] = curlevel;

        int maxlevelcopy = maxlevel_;
        if (curlevel <= maxlevelcopy) {
            templock.unlock();
        }
        tableint currObj = enterpoint_node_;
        tableint enterpoint_copy = enterpoint_node_;

        memset(data_level0_memory_ + cur_c * size_data_per_element_, 0, size_data_per_element_);

        // Initialisation of the data and label
        memcpy(getExternalLabeLp(cur_c), &label, sizeof(labeltype));
        memcpy(getDataByInternalId(cur_c), data_point, data_size_);

        if (curlevel) {
            linkLists_[cur_c] = (char *) malloc(size_links_per_element_ * curlevel + 1);
            if (linkLists_[cur_c] == nullptr)
                throw std::runtime_error("Not enough memory: addPoint failed to allocate linklist");
            memset(linkLists_[cur_c], 0, size_links_per_element_ * curlevel + 1);
        }

        if ((signed)currObj != -1) {
            if (curlevel < maxlevelcopy) {
                dist_t curdist;
                {
                    data_updates_shared_lock lck;
                    if constexpr (with_concurrent_updates) {
                        lck = data_updates_shared_lock(data_updates_locks_[currObj]);
                    }
                    curdist = fstdistfunc_(data_point, cur_c, getDataByInternalId(currObj), currObj);
                }
                for (int level = maxlevelcopy; level > curlevel; level--) {
                    bool changed = true;
                    while (changed) {
                        changed = false;
                        unsigned int *data;
                        std::lock_guard lock(link_list_locks_[currObj]);
                        data = get_linklist(currObj, level);
                        int size = getListCount(data);

                        tableint *datal = (tableint *) (data + 1);
                        for (int i = 0; i < size; i++) {
                            tableint cand = datal[i];
                            if (cand < 0 || cand > max_elements_)
                                throw std::runtime_error("cand error");

                            dist_t d;
                            data_updates_shared_lock lck;
                            if constexpr (with_concurrent_updates) {
                                lck = data_updates_shared_lock(data_updates_locks_[cand]);
                            }
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
                if (level > maxlevelcopy || level < 0)  // possible?
                    throw std::runtime_error("Level error");

                std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates =
                    searchBaseLayer<with_concurrent_updates>(currObj, data_point, cur_c, level);

                data_updates_shared_lock lck;
                if constexpr (with_concurrent_updates) {
                    lck = data_updates_shared_lock(data_updates_locks_[enterpoint_copy]);
                }
                bool epDeleted = isMarkedDeleted(enterpoint_copy);
                if (epDeleted) {
                    top_candidates.emplace(fstdistfunc_(data_point, cur_c, getDataByInternalId(enterpoint_copy), enterpoint_copy), enterpoint_copy);
                    if (top_candidates.size() > ef_construction_)
                        top_candidates.pop();
                }
                currObj = mutuallyConnectNewElement<with_concurrent_updates>(cur_c, top_candidates, level, false);
            }
        } else {
            // Do nothing for the first element
            enterpoint_node_ = 0;
            maxlevel_ = curlevel;
        }

        // Releasing lock for the maximum level
        if (curlevel > maxlevelcopy) {
            enterpoint_node_ = cur_c;
            maxlevel_ = curlevel;
        }
        return cur_c;
    }


    std::priority_queue<std::pair<dist_t, labeltype >>
    searchKnn(const void *query_data, size_t k, size_t ef = 0, BaseFilterFunctor* isIdAllowed = nullptr) const override {
        std::vector<std::pair<dist_t, labeltype>> container;
        container.reserve(k);
        std::priority_queue<std::pair<dist_t, labeltype>> result(std::less<std::pair<dist_t, labeltype>>(), std::move(container));

        if (cur_element_count == 0) return result;

        tableint currObj = enterpoint_node_;
        dist_t curdist = fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), enterpoint_node_);

        for (int level = maxlevel_; level > 0; level--) {
            bool changed = true;
            while (changed) {
                changed = false;
                unsigned int *data;

                data = (unsigned int *) get_linklist(currObj, level);
                int size = getListCount(data);
                metric_hops++;
                metric_distance_computations+=size;

                tableint *datal = (tableint *) (data + 1);
                for (int i = 0; i < size; i++) {
                    tableint cand = datal[i];
                    if (cand < 0 || cand > max_elements_)
                        throw std::runtime_error("cand error");
                    dist_t d = fstdistfunc_(query_data, getDataByInternalId(cand), cand);
                    if (d < curdist) {
                        curdist = d;
                        currObj = cand;
                        changed = true;
                    }
                }
            }
        }

        ef = ef ? ef : k * 3 / 2;
        std::vector<std::pair<dist_t, tableint>> container2;
        container2.reserve(ef);
        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates(CompareByFirst(), std::move(container2));
        bool bare_bone_search = !num_deleted_ && !isIdAllowed;
        if (bare_bone_search) {
            top_candidates = searchBaseLayerST<true>(
                    currObj, query_data, ef, isIdAllowed);
        } else {
            top_candidates = searchBaseLayerST<false>(
                    currObj, query_data, ef, isIdAllowed);
        }

        while (top_candidates.size() > k) {
            top_candidates.pop();
        }
        while (top_candidates.size() > 0) {
            std::pair<dist_t, tableint> rez = top_candidates.top();
            result.push(std::pair<dist_t, labeltype>(rez.first, getExternalLabel(rez.second)));
            top_candidates.pop();
        }
        return result;
    }


    std::vector<std::pair<dist_t, labeltype >>
    searchStopConditionClosest(
        const void *query_data,
        BaseSearchStopCondition<dist_t>& stop_condition,
        BaseFilterFunctor* isIdAllowed = nullptr) const {
        std::vector<std::pair<dist_t, labeltype >> result;
        if (cur_element_count == 0) return result;

        tableint currObj = enterpoint_node_;
        dist_t curdist = fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), enterpoint_node_);

        for (int level = maxlevel_; level > 0; level--) {
            bool changed = true;
            while (changed) {
                changed = false;
                unsigned int *data;

                data = (unsigned int *) get_linklist(currObj, level);
                int size = getListCount(data);
                metric_hops++;
                metric_distance_computations+=size;

                tableint *datal = (tableint *) (data + 1);
                for (int i = 0; i < size; i++) {
                    tableint cand = datal[i];
                    if (cand < 0 || cand > max_elements_)
                        throw std::runtime_error("cand error");
                    dist_t d = fstdistfunc_(query_data, getDataByInternalId(cand), cand);

                    if (d < curdist) {
                        curdist = d;
                        currObj = cand;
                        changed = true;
                    }
                }
            }
        }

        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates;
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
        std::vector <int > inbound_connections_num(cur_element_count, 0);
        for (int i = 0; i < cur_element_count; i++) {
            for (int l = 0; l <= element_levels_[i]; l++) {
                linklistsizeint *ll_cur = get_linklist_at_level(i, l);
                int size = getListCount(ll_cur);
                tableint *data = (tableint *) (ll_cur + 1);
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
            for (int i=0; i < cur_element_count; i++) {
                assert(inbound_connections_num[i] > 0);
                min1 = std::min(inbound_connections_num[i], min1);
                max1 = std::max(inbound_connections_num[i], max1);
            }
            std::cout << "Min inbound: " << min1 << ", Max inbound:" << max1 << "\n";
        }
        std::cout << "integrity ok, checked " << connections_checked << " connections\n";
    }
};

template<typename dist_t>
using HierarchicalNSWST = HierarchicalNSW<dist_t, Synchronization::None>;

template<typename dist_t>
using HierarchicalNSWMT = HierarchicalNSW<dist_t, Synchronization::OnInsertions>;

}  // namespace hnswlib

