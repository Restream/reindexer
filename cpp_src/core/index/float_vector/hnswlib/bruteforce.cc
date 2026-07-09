// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#include "bruteforce.h"

#include "tools/unaligned.h"

namespace hnswlib {

BruteforceSearch::BruteforceSearch(reindexer::VectorMetric metric, size_t dim, size_t maxElements)
	: dataSize_(dim * sizeof(float)), maxElements_(maxElements), dist_(metric, dim, maxElements_), data_([this] {
		  auto res = static_cast<char*>(malloc(maxElements_ * sizePerElement_));
		  if (res == nullptr) {
			  throw std::runtime_error("Not enough memory: BruteforceSearch failed to allocate data");
		  }
		  return res;
	  }()) {}

BruteforceSearch::BruteforceSearch(const BruteforceSearch& other, size_t newMaxElements)
	: dataSize_(other.dataSize_),
	  maxElements_(std::max(other.maxElements_, newMaxElements)),
	  curElementCount_(other.curElementCount_),
	  dist_(other.dist_, maxElements_),
	  data_([this, &other] {
		  auto res = static_cast<char*>(malloc(maxElements_ * sizePerElement_));
		  if (res == nullptr) {
			  throw std::runtime_error("Not enough memory: BruteforceSearch failed to allocate data");
		  }
		  assertrx(sizePerElement_ == other.sizePerElement_);
		  std::memcpy(res, other.data_, other.maxElements_ * sizePerElement_);
		  return res;
	  }()),
	  dictExternalToInternal_(other.dictExternalToInternal_) {}

const float* BruteforceSearch::FloatPtrByExternalLabel(labeltype label) const {
	auto search = dictExternalToInternal_.find(label);
	if (search == dictExternalToInternal_.end()) {
		throw std::runtime_error("Label not found");
	}
	return floatPtrByIdx(search->second);
}

void BruteforceSearch::AddPointNoLock(reindexer::ConstFloatVectorView vect, reindexer::FloatVectorId id) {
	int idx;
	const auto label = id.AsNumber();

	auto search = dictExternalToInternal_.find(label);
	if (search != dictExternalToInternal_.end()) {
		idx = search->second;
	} else {
		if (curElementCount_ >= maxElements_) {
			throw std::runtime_error("The number of elements exceeds the specified limit\n");
		}
		idx = curElementCount_;
		dictExternalToInternal_[label] = idx;
		curElementCount_++;
	}
	dist_.AddNorm(vect.Data(), idx);

	auto ptr = ptrByIdx(idx);
	memcpy(ptr, vect.Data(), dataSize_);
	memcpy(ptr + dataSize_, &label, sizeof(labeltype));
}

void BruteforceSearch::AddPointConcurrent(reindexer::ConstFloatVectorView, reindexer::FloatVectorId) {
	throw std::logic_error("This brute force index does not support concurrent insertions");
}

void BruteforceSearch::RemovePoint(labeltype cur_external) {
	auto found = dictExternalToInternal_.find(cur_external);
	if (found == dictExternalToInternal_.end()) {
		return;
	}

	size_t cur_c = found->second;
	dictExternalToInternal_.erase(found);
	if (cur_c + 1 != curElementCount_) {
		dictExternalToInternal_[label(curElementCount_ - 1)] = cur_c;
		memcpy(data_ + sizePerElement_ * cur_c, data_ + sizePerElement_ * (curElementCount_ - 1), dataSize_ + sizeof(labeltype));
		dist_.MoveNorm(curElementCount_ - 1, cur_c);
	} else {
		dist_.EraseNorm(cur_c);
	}
	curElementCount_--;
}

void BruteforceSearch::ResizeIndex(size_t newMaxElements) {
	if (newMaxElements < curElementCount_) {
		throw std::runtime_error("Cannot resize, max element is less than the current number of elements");
	}
	char* newData = static_cast<char*>(realloc(data_, newMaxElements * sizePerElement_));
	if (newData == nullptr) {
		throw std::runtime_error("Not enough memory: resizeIndex failed to allocate data");
	}
	maxElements_ = newMaxElements;
	dist_.Resize(maxElements_);
	if (newData != data_) {
		data_ = newData;
	}
}

SearchResultQueue BruteforceSearch::SearchKnn(const float* query_data, std::optional<float> /*query_data_norm*/, size_t k,
											  size_t /*ef*/) const {
	using pair_t = std::pair<float, labeltype>;
	if (curElementCount_ == 0 || k == 0) {
		return SearchResultQueue();
	}
	std::vector<pair_t> container;

	k = std::min<size_t>(k, curElementCount_);
	container.reserve(k);
	PriorityQueue<pair_t> topResults(std::less<pair_t>(), std::move(container));
	for (int i = 0; i < int(k); i++) {
		float dist = dist_(query_data, floatPtrByIdx(i), i);
		topResults.emplace(dist, label(i));
	}
	float lastdist = topResults.top().first;
	for (int i = int(k); i < int(curElementCount_); i++) {
		float dist = dist_(query_data, floatPtrByIdx(i), i);
		if (dist < lastdist) {
			topResults.replace_top(dist, label(i));
			lastdist = topResults.top().first;
		}
	}
	return topResults;
}

SearchResultQueue BruteforceSearch::SearchRange(const float* query_data, std::optional<float> /*query_data_norm*/, float radius,
												size_t /*ef*/) const {
	SearchResultQueue topResults;
	if (curElementCount_ == 0) {
		return topResults;
	}

	for (int i = 0; i < int(curElementCount_); i++) {
		float dist = dist_(query_data, floatPtrByIdx(i), i);
		if (dist < radius) {
			topResults.emplace(dist, label(i));
		}
	}
	return topResults;
}

labeltype BruteforceSearch::label(int idx) const noexcept {
	return reindexer::unaligned::read<labeltype>(ptrByIdx(idx) + dataSize_);
}

char* BruteforceSearch::ptrByIdx(int idx) noexcept { return data_ + sizePerElement_ * idx; }
const char* BruteforceSearch::ptrByIdx(int idx) const noexcept { return data_ + sizePerElement_ * idx; }
const float* BruteforceSearch::floatPtrByIdx(int idx) const noexcept { return reinterpret_cast<const float*>(ptrByIdx(idx)); }

}  // namespace hnswlib
