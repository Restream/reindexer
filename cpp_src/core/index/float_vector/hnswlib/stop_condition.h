// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once
#include <assert.h>
#include <unordered_map>
#include "space_ip.h"
#include "space_l2.h"

namespace hnswlib {

template <typename DOCIDTYPE>
class [[nodiscard]] BaseMultiVectorSpace : public SpaceInterface {
public:
	virtual DOCIDTYPE get_doc_id(const void* datapoint) = 0;

	virtual void set_doc_id(void* datapoint, DOCIDTYPE doc_id) = 0;
};

template <typename DOCIDTYPE>
class [[nodiscard]] MultiVectorL2Space final : public BaseMultiVectorSpace<DOCIDTYPE> {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t vector_size_;
	size_t dim_;

public:
	MultiVectorL2Space(size_t dim) {
		fstdistfunc_ = L2Sqr<float>;
#if REINDEXER_WITH_SSE
		if (dim % 16 == 0) {
			fstdistfunc_ = L2SqrSIMD16Ext;
		} else if (dim > 16) {
			fstdistfunc_ = L2SqrSIMD16ExtResiduals;
		} else if (dim % 4 == 0) {
			fstdistfunc_ = L2SqrSIMD4Ext;
		} else if (dim > 4) {
			fstdistfunc_ = L2SqrSIMD4ExtResiduals;
		}
#endif	// REINDEXER_WITH_SSE
		dim_ = dim;
		vector_size_ = dim * sizeof(float);
		data_size_ = vector_size_ + sizeof(DOCIDTYPE);
	}

	size_t get_data_size() override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() override { return {.f = fstdistfunc_, .metric = MetricType::L2, .dims = dim_}; }

	void* get_dist_func_param() override { return &dim_; }

	DOCIDTYPE get_doc_id(const void* datapoint) override { return *(DOCIDTYPE*)((char*)datapoint + vector_size_); }

	void set_doc_id(void* datapoint, DOCIDTYPE doc_id) override { *(DOCIDTYPE*)((char*)datapoint + vector_size_) = doc_id; }
};

template <typename DOCIDTYPE>
class [[nodiscard]] MultiVectorInnerProductSpace final : public BaseMultiVectorSpace<DOCIDTYPE> {
	DISTFUNC fstdistfunc_;
	size_t data_size_;
	size_t vector_size_;
	size_t dim_;

public:
	MultiVectorInnerProductSpace(size_t dim) {
		fstdistfunc_ = InnerProductDistance<float>;
#if REINDEXER_WITH_SSE
		if (dim % 16 == 0) {
			fstdistfunc_ = InnerProductDistanceSIMD16Ext;
		} else if (dim > 16) {
			fstdistfunc_ = InnerProductDistanceSIMD16ExtResiduals;
		} else if (dim % 4 == 0) {
			fstdistfunc_ = InnerProductDistanceSIMD4Ext;
		} else if (dim > 4) {
			fstdistfunc_ = InnerProductDistanceSIMD4ExtResiduals;
		}
#endif
		vector_size_ = dim * sizeof(float);
		data_size_ = vector_size_ + sizeof(DOCIDTYPE);
	}

	size_t get_data_size() override { return data_size_; }

	DistCalculatorParam get_dist_calculator_param() override {
		return {.f = fstdistfunc_, .metric = MetricType::INNER_PRODUCT, .dims = dim_};
	}

	void* get_dist_func_param() override { return &dim_; }

	DOCIDTYPE get_doc_id(const void* datapoint) override { return *(DOCIDTYPE*)((char*)datapoint + vector_size_); }

	void set_doc_id(void* datapoint, DOCIDTYPE doc_id) override { *(DOCIDTYPE*)((char*)datapoint + vector_size_) = doc_id; }
};

template <typename DOCIDTYPE, typename dist_t>
class [[nodiscard]] MultiVectorSearchStopCondition final : public BaseSearchStopCondition<dist_t> {
	size_t curr_num_docs_;
	size_t num_docs_to_search_;
	size_t ef_collection_;
	std::unordered_map<DOCIDTYPE, size_t> doc_counter_;
	std::priority_queue<std::pair<dist_t, DOCIDTYPE>> search_results_;
	BaseMultiVectorSpace<DOCIDTYPE>& space_;

public:
	MultiVectorSearchStopCondition(BaseMultiVectorSpace<DOCIDTYPE>& space, size_t num_docs_to_search, size_t ef_collection = 10)
		: space_(space) {
		curr_num_docs_ = 0;
		num_docs_to_search_ = num_docs_to_search;
		ef_collection_ = std::max(ef_collection, num_docs_to_search);
	}

	void add_point_to_result(labeltype label, const void* datapoint, dist_t dist) override {
		DOCIDTYPE doc_id = space_.get_doc_id(datapoint);
		if (doc_counter_[doc_id] == 0) {
			curr_num_docs_ += 1;
		}
		search_results_.emplace(dist, doc_id);
		doc_counter_[doc_id] += 1;
	}

	void remove_point_from_result(labeltype label, const void* datapoint, dist_t dist) override {
		DOCIDTYPE doc_id = space_.get_doc_id(datapoint);
		doc_counter_[doc_id] -= 1;
		if (doc_counter_[doc_id] == 0) {
			curr_num_docs_ -= 1;
		}
		search_results_.pop();
	}

	bool should_stop_search(dist_t candidate_dist, dist_t lowerBound) override {
		bool stop_search = candidate_dist > lowerBound && curr_num_docs_ == ef_collection_;
		return stop_search;
	}

	bool should_consider_candidate(dist_t candidate_dist, dist_t lowerBound) override {
		bool flag_consider_candidate = curr_num_docs_ < ef_collection_ || lowerBound > candidate_dist;
		return flag_consider_candidate;
	}

	bool should_remove_extra() override {
		bool flag_remove_extra = curr_num_docs_ > ef_collection_;
		return flag_remove_extra;
	}

	void filter_results(std::vector<std::pair<dist_t, labeltype>>& candidates) override {
		while (curr_num_docs_ > num_docs_to_search_) {
			dist_t dist_cand = candidates.back().first;
			dist_t dist_res = search_results_.top().first;
			assert(dist_cand == dist_res);
			DOCIDTYPE doc_id = search_results_.top().second;
			doc_counter_[doc_id] -= 1;
			if (doc_counter_[doc_id] == 0) {
				curr_num_docs_ -= 1;
			}
			search_results_.pop();
			candidates.pop_back();
		}
	}

	~MultiVectorSearchStopCondition() {}
};

template <typename dist_t>
class [[nodiscard]] EpsilonSearchStopCondition final : public BaseSearchStopCondition<dist_t> {
	float epsilon_;
	size_t min_num_candidates_;
	size_t max_num_candidates_;
	size_t curr_num_items_;

public:
	EpsilonSearchStopCondition(float epsilon, size_t min_num_candidates, size_t max_num_candidates) {
		assert(min_num_candidates <= max_num_candidates);
		epsilon_ = epsilon;
		min_num_candidates_ = min_num_candidates;
		max_num_candidates_ = max_num_candidates;
		curr_num_items_ = 0;
	}

	void add_point_to_result(labeltype label, const void* datapoint, dist_t dist) override { curr_num_items_ += 1; }

	void remove_point_from_result(labeltype label, const void* datapoint, dist_t dist) override { curr_num_items_ -= 1; }

	bool should_stop_search(dist_t candidate_dist, dist_t lowerBound) override {
		if (candidate_dist > lowerBound && curr_num_items_ == max_num_candidates_) {
			// new candidate can't improve found results
			return true;
		}
		if (candidate_dist > epsilon_ && curr_num_items_ >= min_num_candidates_) {
			// new candidate is out of epsilon region and
			// minimum number of candidates is checked
			return true;
		}
		return false;
	}

	bool should_consider_candidate(dist_t candidate_dist, dist_t lowerBound) override {
		bool flag_consider_candidate = curr_num_items_ < max_num_candidates_ || lowerBound > candidate_dist;
		return flag_consider_candidate;
	}

	bool should_remove_extra() {
		bool flag_remove_extra = curr_num_items_ > max_num_candidates_;
		return flag_remove_extra;
	}

	void filter_results(std::vector<std::pair<dist_t, labeltype>>& candidates) override {
		while (!candidates.empty() && candidates.back().first > epsilon_) {
			candidates.pop_back();
		}
		while (candidates.size() > max_num_candidates_) {
			candidates.pop_back();
		}
	}
};
}  // namespace hnswlib
