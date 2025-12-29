#pragma once
#include "estl/fast_hash_set.h"
#include "hnsw_view_iterator.h"

namespace hnswlib {
struct [[nodiscard]] CorrectiveOffsets {
	float precalc = 0.f;
	float roundingErr = 0.f;
	float negOutlierErr = 0.f;
	float posOutlierErr = 0.f;
};

struct [[nodiscard]] QuantizingConfig {
	float quantile = 1.f;
	size_t sampleSize = kDefaultSampleSize;
	Sq8NonLinearCorrection nonLinearCorrection = Sq8NonLinearCorrection::Disabled;
};

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
	QuantizingParams(const auto& hnsw, QuantizingConfig conf) : config(std::move(conf)) {
		minQ = 0.f;
		maxQ = 0.f;
		size_t size = 0;
		for (auto& sample : HNSWView(hnsw, config.sampleSize)) {
			auto [min, max] = FindNthMinMax(sample, hnsw.fstdistfunc_.Dims() * kSampleBatchSize, config.quantile);
			minQ += min;
			maxQ += max;
			size++;
		}
		minQ /= size;
		maxQ /= size;

		alpha = (maxQ - minQ) / sq8Range;
		alpha_2 = std::pow(alpha, 2.f);
		delta = 0.5 * std::pow(minQ, 2.f) * hnsw.fstdistfunc_.Dims();
	}

	template <typename ReaderT>
	void Deserialize(ReaderT& reader) {
		config.quantile = reader.GetFloat();
		config.sampleSize = reader.GetVarUInt();
		config.nonLinearCorrection = Sq8NonLinearCorrection(reader.GetVarInt());

		sq8Range = reader.GetFloat();
		uint8offset = reader.GetFloat();

		minQ = reader.GetFloat();
		maxQ = reader.GetFloat();
		alpha = reader.GetFloat();
		alpha_2 = reader.GetFloat();
		delta = reader.GetFloat();
	}

	template <typename WriterT>
	void Serialize(WriterT& writer) const {
		writer.PutFloat(config.quantile);
		writer.PutVarUInt(uint64_t{config.sampleSize});
		writer.PutVarInt(int32_t(config.nonLinearCorrection));

		writer.PutFloat(sq8Range);
		writer.PutFloat(uint8offset);

		writer.PutFloat(minQ);
		writer.PutFloat(maxQ);
		writer.PutFloat(alpha);
		writer.PutFloat(alpha_2);
		writer.PutFloat(delta);
	}

	QuantizingConfig config;
	float sq8Range = KDefaultSq8Range - (config.nonLinearCorrection == Sq8NonLinearCorrection::Enabled ? 2 : 0);
	float uint8offset = config.nonLinearCorrection == Sq8NonLinearCorrection::Enabled ? 1.f : 0.f;

	float minQ = std::numeric_limits<float>::max();
	float maxQ = std::numeric_limits<float>::lowest();
	float alpha = 0.f;
	float alpha_2 = 1.f;
	float delta = 0.f;
};
}  // namespace hnswlib