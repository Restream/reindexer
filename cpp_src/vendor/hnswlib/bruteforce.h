#pragma once

#include "vendor/hopscotch/hopscotch_sc_map.h"
#include <assert.h>

namespace hnswlib {
template<typename dist_t>
class BruteforceSearch final : public AlgorithmInterface<dist_t> {
    using Base = AlgorithmInterface<dist_t>;    template <typename K, typename V>
    using HashMapT = tsl::hopscotch_sc_map<K, V, std::hash<K>, std::equal_to<K>, std::less<K>,
                     std::allocator<std::pair<const K, V>>, 30, false, tsl::mod_growth_policy<std::ratio<3, 2>>>;

 public:
    char *data_;
    size_t maxelements_;
    size_t cur_element_count;
    size_t size_per_element_;

    size_t data_size_;
    DistCalculator<dist_t> fstdistfunc_;
    // std::mutex index_lock;

    HashMapT<labeltype, size_t> dict_external_to_internal;

    BruteforceSearch(SpaceInterface <dist_t> *s)
        : data_(nullptr),
            maxelements_(0),
            cur_element_count(0),
            size_per_element_(0),
            data_size_(0) {
    }

    BruteforceSearch(SpaceInterface<dist_t> *s, const BruteforceSearch& other, size_t newMaxElements)
        : data_(nullptr),
            maxelements_(std::max(other.maxelements_, newMaxElements)),
            cur_element_count(other.cur_element_count),
            size_per_element_(other.size_per_element_),
            data_size_(0),
            dict_external_to_internal(other.dict_external_to_internal) {
        init(s);
        std::memcpy(data_, other.data_, other.maxelements_ * size_per_element_);
        fstdistfunc_.CopyValuesFrom(other.fstdistfunc_);
    }


    BruteforceSearch(SpaceInterface <dist_t> *s, size_t maxElements) {
        maxelements_ = maxElements;
        data_size_ = s->get_data_size();
        fstdistfunc_ = DistCalculator{s->get_dist_calculator_param(), maxelements_};
        size_per_element_ = data_size_ + sizeof(labeltype);
        data_ = (char *) malloc(maxElements * size_per_element_);
        if (data_ == nullptr)
            throw std::runtime_error("Not enough memory: BruteforceSearch failed to allocate data");
        cur_element_count = 0;
    }


    ~BruteforceSearch() {
        free(data_);
    }


    size_t getMaxElements() const noexcept override {
        return maxelements_;
    }

    size_t getCurrentElementCount() const noexcept override {
        return cur_element_count;
    }

    const char* ptrByIdx(int idx) const noexcept {
        return data_ + size_per_element_ * idx;
    }
    char* ptrByIdx(int idx) noexcept {
        return const_cast<char*>(const_cast<const BruteforceSearch&>(*this).ptrByIdx(idx));
    }

    const char* ptrByExternalLabel(labeltype label) const override {
        auto search = dict_external_to_internal.find(label);
        if (search == dict_external_to_internal.end()) {
            throw std::runtime_error("Label not found");
        }
        return ptrByIdx(search->second);
    }

    void addPoint(const void *datapoint, labeltype label, reindexer::ReplaceDeleted replace_deleted = reindexer::ReplaceDeleted_False) override {
        int idx;
        {
            // std::unique_lock<std::mutex> lock(index_lock);

            auto search = dict_external_to_internal.find(label);
            if (search != dict_external_to_internal.end()) {
                idx = search->second;
            } else {
                if (cur_element_count >= maxelements_) {
                    throw std::runtime_error("The number of elements exceeds the specified limit\n");
                }
                idx = cur_element_count;
                dict_external_to_internal[label] = idx;
                cur_element_count++;
            }
            fstdistfunc_.AddNorm(datapoint, idx);
        }
        const auto ptr = ptrByIdx(idx);
        memcpy(ptr + data_size_, &label, sizeof(labeltype));
        memcpy(ptr, datapoint, data_size_);
    }


    void removePoint(labeltype cur_external) {
        // std::unique_lock<std::mutex> lock(index_lock);

        auto found = dict_external_to_internal.find(cur_external);
        if (found == dict_external_to_internal.end()) {
            return;
        }

        size_t cur_c = found->second;
        dict_external_to_internal.erase(found);
        if (cur_c + 1 != cur_element_count) {
            labeltype label = *((labeltype*)(data_ + size_per_element_ * (cur_element_count-1) + data_size_));
            dict_external_to_internal[label] = cur_c;
            memcpy(data_ + size_per_element_ * cur_c,
                  data_ + size_per_element_ * (cur_element_count - 1),
                  data_size_+sizeof(labeltype));
            fstdistfunc_.MoveNorm(cur_element_count - 1, cur_c);
        } else {
            fstdistfunc_.EraseNorm(cur_c);
        }
        cur_element_count--;
    }

    std::priority_queue<std::pair<dist_t, labeltype >>
    searchKnn(const void *query_data, size_t k, size_t /*ef*/ = 0, BaseFilterFunctor* isIdAllowed = nullptr) const override {
        assert(k <= cur_element_count);
        std::priority_queue<std::pair<dist_t, labeltype >> topResults;
        if (cur_element_count == 0) return topResults;
        for (int i = 0; i < k; i++) {
            dist_t dist = fstdistfunc_(query_data, data_ + size_per_element_ * i, i);
            //std::cout << "New dist " << dist << " for " << i << " was ";
            labeltype label = *((labeltype*) (data_ + size_per_element_ * i + data_size_));
            if ((!isIdAllowed) || (*isIdAllowed)(label)) {
                topResults.emplace(dist, label);
                //std::cout << "emplaced\n";
            } else {
                //std::cout << "skipped\n";
            }
        }
        dist_t lastdist = topResults.empty() ? std::numeric_limits<dist_t>::max() : topResults.top().first;
        for (int i = k; i < cur_element_count; i++) {
            dist_t dist = fstdistfunc_(query_data, data_ + size_per_element_ * i, i);
            //std::cout << "New dist " << dist << " for " << i << " was ";
            if (dist <= lastdist) {
                labeltype label = *((labeltype *) (data_ + size_per_element_ * i + data_size_));
                if ((!isIdAllowed) || (*isIdAllowed)(label)) {
                    topResults.emplace(dist, label);
                    //std::cout << "emplaced\n";
                } else {
                    //std::cout << "skipped(2)\n";
                }
                if (topResults.size() > k) {
                    //std::cout << "Dist " << topResults.top().first << " was removed from top\n";
                    topResults.pop();
                }

                if (!topResults.empty()) {
                    lastdist = topResults.top().first;
                }
            } else {
               //std::cout << "skipped(1)\n";
            }
        }
        return topResults;
    }

    // Unused
    // void saveIndex(const std::string &location) override {
    //     std::ofstream output(location, std::ios::binary);
    //     std::streampos position;

    //     writeBinaryPOD(output, maxelements_);
    //     writeBinaryPOD(output, size_per_element_);
    //     writeBinaryPOD(output, cur_element_count);

    //     output.write(data_, maxelements_ * size_per_element_);

    //     output.close();
    // }


    void init(SpaceInterface<dist_t> *s) {
        data_size_ = s->get_data_size();
        fstdistfunc_ = DistCalculator{s->get_dist_calculator_param(), maxelements_};
        size_per_element_ = data_size_ + sizeof(labeltype);
        data_ = (char *) malloc(maxelements_ * size_per_element_);
        if (data_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate data");
    }

    typename Base::ResizeResult resizeIndex(size_t new_max_elements) override {
        if (new_max_elements < cur_element_count) {
            throw std::runtime_error("Cannot resize, max element is less than the current number of elements");
        }
        char* newData = (char*)realloc(data_, new_max_elements * size_per_element_);
        if (newData == nullptr) {
            throw std::runtime_error("Not enough memory: resizeIndex failed to allocate data");
        }
        maxelements_ = new_max_elements;
        fstdistfunc_.Resize(maxelements_);
        if (newData != data_) {
            data_ = newData;
            return {std::exchange(data_, newData), newData};
        } else {
            return {data_, data_};
        }
    }

    // Unused
    // void loadIndex(const std::string &location, SpaceInterface<dist_t> *s) {
    //     std::ifstream input(location, std::ios::binary);
    //     std::streampos position;

    //     readBinaryPOD(input, maxelements_);
    //     readBinaryPOD(input, size_per_element_);
    //     readBinaryPOD(input, cur_element_count);

    //     init(s);

    //     input.read(data_, maxelements_ * size_per_element_);

    //     input.close();
    // }
};
}  // namespace hnswlib
