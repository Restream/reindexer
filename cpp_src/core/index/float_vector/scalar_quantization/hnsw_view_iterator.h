#pragma once

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

#include "tools/assertrx.h"

namespace hnswlib {

static constexpr uint8_t kQuantizeBits = 8;
static constexpr float KSq8Max = (1 << kQuantizeBits) - 1;

enum class ScalarQuantizeType { Full, Partial, Component };

// The template parameter DataHandlerT is needed to avoid cyclic references
// in header files for the case of header-only implementation.
template <typename DataHandlerT>
class HNSWIterator {
public:
	explicit HNSWIterator(ScalarQuantizeType type, const DataHandlerT& map, const std::vector<uint32_t>& paritalSplitIndexes = {}) noexcept
		: type(type), map_(map), paritalSplitIndexes_(paritalSplitIndexes) {}

	HNSWIterator& operator++() noexcept { return next(); }
	HNSWIterator& operator+=(size_t _offset) noexcept { return offset(_offset); }
	HNSWIterator operator+(size_t offset) noexcept {
		auto res = *this;
		res.offset(offset);
		return res;
	}

	float& operator*() const noexcept {
		return reinterpret_cast<float*>(
			map_.getDataByInternalId(type == ScalarQuantizeType::Partial ? paritalSplitIndexes_[internalId_] : internalId_))[compIdx_];
	}

	bool operator==(const HNSWIterator& other) const noexcept { return internalId_ == other.internalId_ && compIdx_ == other.compIdx_; }
	bool operator!=(const HNSWIterator& other) const noexcept { return !(*this == other); }

	const ScalarQuantizeType type;

private:
	static void next(auto& dim1, auto& dim2, size_t size) {
		dim1 = (dim1 + 1) % size;
		dim2 += dim1 == 0;
	}
	static void offset(auto& dim1, auto& dim2, size_t offset, size_t size) {
		auto newDim1 = dim1 + offset % size;
		dim2 += offset / size + newDim1 / size;
		dim1 = newDim1 % size;
	}

	HNSWIterator& next() {
		switch (type) {
			case ScalarQuantizeType::Full:
			case ScalarQuantizeType::Partial: {
				next(compIdx_, internalId_, map_.fstdistfunc_.Dims());
				break;
			}
			case ScalarQuantizeType::Component: {
				next(internalId_, compIdx_, map_.cur_element_count);
				break;
			}
			default:
				assertrx(false);
		}
		return *this;
	}

	HNSWIterator& offset(size_t _offset) {
		switch (type) {
			case ScalarQuantizeType::Full:
			case ScalarQuantizeType::Partial: {
				offset(compIdx_, internalId_, _offset, map_.fstdistfunc_.Dims());
				break;
			}
			case ScalarQuantizeType::Component: {
				offset(internalId_, compIdx_, _offset, map_.cur_element_count);
				break;
			}
			default:
				assertrx(false);
		}

		if (type == ScalarQuantizeType::Partial) {
			internalId_ = std::min<int>(internalId_, paritalSplitIndexes_.size());
		}
		return *this;
	}

	uint32_t internalId_ = 0;  // in partial split case it is an index in vector paritalSplitIndexes_
	int compIdx_ = 0;
	const DataHandlerT& map_;

	const std::vector<uint32_t>& paritalSplitIndexes_ = {};
};

static constexpr auto kPartialSampleBatchSize = 20;

template <typename DataHandlerT>
class HNSWViewIterator {
	size_t step(const DataHandlerT& map) const noexcept {
		switch (type) {
			case ScalarQuantizeType::Full:
				return map.fstdistfunc_.Dims() * map.cur_element_count;
			case ScalarQuantizeType::Partial:
				return map.fstdistfunc_.Dims() * kPartialSampleBatchSize;
			case ScalarQuantizeType::Component:
				return map.cur_element_count;
			default:
				std::abort();
		}
	}

public:
	explicit HNSWViewIterator(const ScalarQuantizeType type, const DataHandlerT& map, const std::vector<uint32_t>& paritalSplitIndexes = {})
		: type(type), step_(step(map)), begin_(HNSWIterator(type, map, paritalSplitIndexes)), end_(begin_ + step_) {}

	HNSWIterator<DataHandlerT> begin() const noexcept { return begin_; }
	HNSWIterator<DataHandlerT> end() const noexcept { return end_; }

	auto& operator*() noexcept { return *this; }
	HNSWViewIterator& operator++() noexcept { return (*this) += 1; }

	bool operator==(const HNSWViewIterator& other) const noexcept { return begin_ == other.begin_ && end_ == other.end_; }
	bool operator!=(const HNSWViewIterator& other) const noexcept { return !(*this == other); }

	HNSWViewIterator& operator+=(size_t offset) noexcept {
		begin_ += offset * step_;
		end_ += offset * step_;
		return *this;
	}

	HNSWViewIterator operator+(size_t offset) noexcept {
		HNSWViewIterator res = *this;
		res += offset;
		return res;
	}

	const ScalarQuantizeType type;

private:
	const size_t step_;
	HNSWIterator<DataHandlerT> begin_;
	HNSWIterator<DataHandlerT> end_;
};

template <typename DataHandlerT>
class HNSWView {
	size_t size(const DataHandlerT& map) const noexcept {
		switch (type) {
			case ScalarQuantizeType::Full:
				return 1;
			case ScalarQuantizeType::Partial:
				return paritalSplitIndexes_.size() / kPartialSampleBatchSize + paritalSplitIndexes_.size() % kPartialSampleBatchSize;
			case ScalarQuantizeType::Component:
				return map.fstdistfunc_.Dims();
			default:
				std::abort();
		}
	}

public:
	static auto GetParitalSampleIndexes(size_t sampleSize, size_t size) {
		sampleSize = std::min(sampleSize, size);
		std::vector<uint32_t> vectorsToTake(sampleSize);
		std::iota(vectorsToTake.begin(), vectorsToTake.end(), 0);
		for (size_t i = sampleSize; i < size; i++) {
			size_t j = std::rand() % (i + 1);
			if (j < sampleSize) {
				vectorsToTake[j] = i;
			}
		}
		std::sort(vectorsToTake.begin(), vectorsToTake.end());
		return vectorsToTake;
	}

	explicit HNSWView(ScalarQuantizeType type, const DataHandlerT& map)
		: type(type), begin_(HNSWViewIterator(type, map)), end_(begin_ + size(map)) {}

	explicit HNSWView(ScalarQuantizeType type, const DataHandlerT& map, size_t sampleSize)
		: type(type),
		  paritalSplitIndexes_(GetParitalSampleIndexes(sampleSize, map.cur_element_count)),
		  begin_(HNSWViewIterator(type, map, paritalSplitIndexes_)),
		  end_(begin_ + size(map)) {
		assertrx(type == ScalarQuantizeType::Partial);
	}

	// Use this constructor only for testing
	explicit HNSWView(ScalarQuantizeType type, const DataHandlerT& map, std::vector<uint32_t> paritalSplitIndexes)
		: type(type),
		  paritalSplitIndexes_(std::move(paritalSplitIndexes)),
		  begin_(HNSWViewIterator(type, map, paritalSplitIndexes_)),
		  end_(begin_ + size(map)) {}

	HNSWViewIterator<DataHandlerT> begin() const noexcept { return begin_; }
	HNSWViewIterator<DataHandlerT> end() const noexcept { return end_; }

	const ScalarQuantizeType type;

private:
	std::vector<uint32_t> paritalSplitIndexes_ = {};

	HNSWViewIterator<DataHandlerT> begin_;
	HNSWViewIterator<DataHandlerT> end_;
};

}  // namespace hnswlib
