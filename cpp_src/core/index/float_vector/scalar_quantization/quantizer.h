#pragma once

#include <optional>
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "hnsw_view_iterator.h"

namespace hnswlib {

struct ComponentNthQuantiles : std::vector<std::pair<float, float>> {
	static constexpr auto kMin = std::numeric_limits<float>::lowest();
	static constexpr auto kMax = std::numeric_limits<float>::max();

	explicit ComponentNthQuantiles(size_t size) : std::vector<std::pair<float, float>>(size, {kMax, kMin}) {}

	auto GetUpdated(const void* dataPtr) const noexcept {
		auto data = static_cast<const float*>(dataPtr);
		std::pair<float, float> pair;
		std::optional<reindexer::fast_hash_map<int, std::pair<float, float>>> res;
		for (size_t i = 0; i < size(); ++i) {
			if (merge(operator[](i), data[i], pair)) {
				if (!res) {
					res = reindexer::fast_hash_map<int, std::pair<float, float>>{};
				}
				(*res)[i] = std::move(pair);
			}
		}

		return res;
	}

	void Update(const reindexer::fast_hash_map<int, std::pair<float, float>>& data) noexcept {
		const auto origMinmax = std::pair<float, float>{min, max};
		auto minmax = origMinmax;
		std::pair<float, float> pair;
		for (const auto& [idx, p] : data) {
			if (merge(minmax, p, pair)) {
				minmax = std::move(pair);
			}

			auto& comp = operator[](idx);
			if (merge(comp, p, pair)) {
				comp = std::move(pair);
			}
		}

		if (minmax != origMinmax) {
			min = minmax.first;
			max = minmax.second;
		}
	}

	float min = kMax;
	float max = kMin;

private:
	bool merge(const auto& orig, float value, auto& res) const noexcept {
		if (orig.first <= value && value <= orig.second) {
			return false;
		}

		res = std::make_pair(std::min(orig.first, value), std::max(orig.second, value));
		return true;
	}
	bool merge(const auto& orig, const auto& other, auto& res) const noexcept {
		if (orig.first <= other.first && other.second <= orig.second) {
			return false;
		}

		res = std::make_pair(std::min(orig.first, other.first), std::max(orig.second, other.second));
		return true;
	}
};

// The template parameter DataHandlerT is needed to avoid cyclic references
// in header files for the case of header-only implementation.
template <typename DataHandlerT>
class Quantizer {
public:
	constexpr static size_t kDefaultPartialSamplesize = 20'000;

	std::pair<float, float> FindNthMinMax(auto&& s, size_t n = 0) const {
		float min, max;
		size_t cnt = 0;
		reindexer::fast_hash_set<size_t> scannedIndexes;
		do {
			min = std::numeric_limits<float>::max();
			max = std::numeric_limits<float>::lowest();
			size_t minIdx = -1, maxIdx = -1;
			size_t i = 0;
			for (auto el : s) {
				if (scannedIndexes.count(i) > 0) {
					i++;
					continue;
				}

				if (el < min) {
					min = el;
					minIdx = i;
				}
				if (el > max) {
					max = el;
					maxIdx = i;
				}
				i++;
			}
			if (n > 0) {
				scannedIndexes.insert(minIdx);
				scannedIndexes.insert(maxIdx);
			}
		} while (++cnt < n);
		return {min, max};
	}

	explicit Quantizer(const ScalarQuantizeType sq8Type, DataHandlerT& dataHandler, float quantile = 1.f,
					   size_t sampleSize = kDefaultPartialSamplesize)
		: sq8Type(sq8Type),
		  dataHandler_(dataHandler),
		  quantile_(quantile),
		  paritalSamplesize_(sampleSize),
		  quantiles_(sq8Type == ScalarQuantizeType::Component ? dataHandler_.fstdistfunc_.Dims() : 0) {}

	void Quantize(const auto& from, auto& to) const noexcept {
		switch (sq8Type) {
			case ScalarQuantizeType::Full:
			case ScalarQuantizeType::Partial:
				std::transform(from.begin(), from.end(), to.begin(),
							   [this](float val) noexcept { return float2uint8t(val, quantiles_.min, quantiles_.max); });
				break;
			case ScalarQuantizeType::Component: {
				std::transform(from.begin(), from.end(), quantiles_.begin(), to.begin(),
							   [](float val, const auto& iComponentMinMax) noexcept {
								   return float2uint8t(val, iComponentMinMax.first, iComponentMinMax.second);
							   });
				break;
			}
			default:
				assertrx(false);
		}
	}

	void Init() {
		switch (sq8Type) {
			case ScalarQuantizeType::Full: {
				const auto& quantiles1 = dataHandler_.Component1Quantiles();
				std::tie(quantiles_.min, quantiles_.max) =
					quantile_ == 1.f ? std::pair(quantiles1.min, quantiles1.max)
									 : FindNthMinMax(*HNSWView(sq8Type, dataHandler_).begin(),
													 (1 - quantile_) * dataHandler_.fstdistfunc_.Dims() * dataHandler_.cur_element_count);
				break;
			}
			case ScalarQuantizeType::Partial: {
				quantiles_.min = 0.f;
				quantiles_.max = 0.f;
				size_t size = 0;
				for (auto& sample : HNSWView(sq8Type, dataHandler_, paritalSamplesize_)) {
					auto [min, max] = FindNthMinMax(sample);
					quantiles_.min += min;
					quantiles_.max += max;
					size++;
				}
				quantiles_.min /= size;
				quantiles_.max /= size;
				break;
			}
			case ScalarQuantizeType::Component: {
				quantiles_ = componentQuantiles();
				break;
			}
			default:
				assertrx(false);
		}
	}

	std::pair<float, float> MinMax() const noexcept { return {quantiles_.min, quantiles_.max}; }
	const std::vector<std::pair<float, float>>& ComponentQuantiles() const noexcept { return quantiles_; }
	size_t Dims() const noexcept { return dataHandler_.fstdistfunc_.Dims(); }

	const ScalarQuantizeType sq8Type = ScalarQuantizeType::Partial;

private:
	static RX_ALWAYS_INLINE uint8_t float2uint8t(float val, float min, float max) noexcept {
		return std::clamp(KSq8Max * (val - min) / (max - min), 0.f, KSq8Max);
	}

	ComponentNthQuantiles componentQuantiles() const {
		assertrx_throw(sq8Type == ScalarQuantizeType::Component);

		if (quantile_ == 1.f) {
			return dataHandler_.Component1Quantiles();
		}

		auto dim = dataHandler_.fstdistfunc_.Dims();
		ComponentNthQuantiles res(dim);

		size_t i = 0;
		for (auto v : HNSWView(sq8Type, dataHandler_)) {
			auto& [min, max] = res[i++] = FindNthMinMax(v, (1 - quantile_) * dim);
			if (min < res.min) {
				res.min = min;
			}
			if (max > res.max) {
				res.max = max;
			}
		}
		return res;
	}

	const DataHandlerT& dataHandler_;
	const float quantile_ = 1.f;
	const size_t paritalSamplesize_ = kDefaultPartialSamplesize;

	ComponentNthQuantiles quantiles_;
};

};	// namespace hnswlib
