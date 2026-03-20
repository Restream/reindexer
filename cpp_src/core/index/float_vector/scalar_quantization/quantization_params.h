#pragma once

#include "core/quantization_config.h"
#include "estl/fast_hash_set.h"
#include "hnsw_view_iterator.h"
#include "tools/errors.h"

namespace hnswlib {

static inline constexpr unsigned kQuantizationParamsVersion = 1;

std::pair<float, float> FindNthMinMax(auto&& s, size_t dataSize, float quantile) {
	float min, max;
	const size_t n = 0.5f * (1 - quantile) * dataSize;
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

struct [[nodiscard]] QuantizingParams {
	QuantizingParams() = default;
	QuantizingParams(const auto& hnsw, QuantizationConfig conf) : config(std::move(conf)) {
		const auto dim = hnsw.fstdistfunc_.Dims();
		minQ = 0.f;
		maxQ = 0.f;
		size_t size = 0;
		for (auto& sample : HNSWView(hnsw, config.sampleSize)) {
			auto [min, max] = FindNthMinMax(sample, dim * kSampleBatchSize, Quantile(dim));
			minQ += min;
			maxQ += max;
			size++;
		}
		minQ /= size;
		maxQ /= size;

		alpha = (maxQ - minQ) / kSq8Range;
		alpha_2 = std::pow(alpha, 2.f);
		delta = 0.5 * std::pow(minQ, 2.f) * dim;
	}

	template <typename ReaderT>
	void Deserialize(ReaderT& reader) {
		auto version = reader.GetVarUInt();
		if (version != kQuantizationParamsVersion) {
			throw reindexer::Error(errParams, "Invalid quantization parameters version during deserialization: expected {}, got {}",
								   kQuantizationParamsVersion, version);
		}

		config.Deserialize(reader);

		minQ = reader.GetFloat();
		maxQ = reader.GetFloat();
		alpha = reader.GetFloat();
		alpha_2 = reader.GetFloat();
		delta = reader.GetFloat();
	}

	template <typename WriterT>
	void Serialize(WriterT& writer) const {
		writer.PutVarUInt(kQuantizationParamsVersion);

		config.Serialize(writer);

		writer.PutFloat(minQ);
		writer.PutFloat(maxQ);
		writer.PutFloat(alpha);
		writer.PutFloat(alpha_2);
		writer.PutFloat(delta);
	}

	float Quantile(size_t dim) const noexcept { return config.quantile ? *config.quantile : std::clamp(1.f - 1.f / (dim + 1), 0.95f, 1.f); }

	QuantizationConfig config;
	float minQ = std::numeric_limits<float>::max();
	float maxQ = std::numeric_limits<float>::lowest();
	float alpha = 0.f;
	float alpha_2 = 1.f;
	float delta = 0.f;
};
}  // namespace hnswlib