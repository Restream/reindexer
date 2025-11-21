#if RX_WITH_BUILTIN_ANN_INDEXES

#include "hnsw_index.h"
#include "core/query/knn_search_params.h"
#include "knn_ctx.h"
#include "knn_raw_result.h"
#include "tools/logger.h"
#include "tools/normalize.h"

namespace reindexer {

static_assert(sizeof(IdType) == sizeof(hnswlib::labeltype), "Expecting 1-to-1 mapping");

static void PrintVecInstructionsLevel(std::string_view indexType, std::string_view name, VectorMetric metric, LogCreation log) {
	std::string vecInstructions = "disabled";
	switch (metric) {
		case VectorMetric::L2:
			if (vector_dists::L2WithAVX512()) {
				vecInstructions = "avx512";
			} else if (vector_dists::L2WithAVX()) {
				vecInstructions = "avx";
			} else if (vector_dists::L2WithSSE()) {
				vecInstructions = "sse";
			}
			break;
		case VectorMetric::InnerProduct:
			if (vector_dists::InnerProductWithAVX512()) {
				vecInstructions = "avx512";
			} else if (vector_dists::InnerProductWithAVX()) {
				vecInstructions = "avx";
			} else if (vector_dists::InnerProductWithSSE()) {
				vecInstructions = "sse";
			}
			break;
		case VectorMetric::Cosine:
			if (hnswlib::CosineWithAVX512()) {
				vecInstructions = "avx512";
			} else if (hnswlib::CosineWithAVX()) {
				vecInstructions = "avx";
			} else if (hnswlib::CosineWithSSE()) {
				vecInstructions = "sse";
			}
			break;
		default:
			throw Error(errLogic, "Attempt to construct {} index '{}' with unknown metric: {}", indexType, name, int(metric));
	}
	if (log) {
		logFmt(LogInfo, "Creating {} index '{}'; Vector instructions level: {}", indexType, name, vecInstructions);
	}
}

constexpr static ReplaceDeleted kHNSWAllowReplaceDeleted = ReplaceDeleted_True;
constexpr static int kHnswRandomSeed = 100;

template <>
HnswIndexBase<hnswlib::HierarchicalNSWST>::HnswIndexBase(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
														 size_t currentNsSize, LogCreation log)
	: Base{idef, std::move(payloadType), std::move(fields)},
	  space_{newSpace(Dimension().Value(), metric_)},
	  map_{std::make_unique<hnswlib::HierarchicalNSWST>(space_.get(), std::max(idef.Opts().FloatVector().StartSize(), currentNsSize),
														idef.Opts().FloatVector().M(), idef.Opts().FloatVector().EfConstruction(),
														kHnswRandomSeed, kHNSWAllowReplaceDeleted)} {
	PrintVecInstructionsLevel("singlethread HNSW", idef.Name(), idef.Opts().FloatVector().Metric(), log);
}

template <>
HnswIndexBase<hnswlib::HierarchicalNSWMT>::HnswIndexBase(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
														 size_t currentNsSize, LogCreation log)
	: Base{idef, std::move(payloadType), std::move(fields)},
	  space_{newSpace(Dimension().Value(), metric_)},
	  map_{std::make_unique<hnswlib::HierarchicalNSWMT>(space_.get(), std::max(idef.Opts().FloatVector().StartSize(), currentNsSize),
														idef.Opts().FloatVector().M(), idef.Opts().FloatVector().EfConstruction(),
														kHnswRandomSeed, kHNSWAllowReplaceDeleted)} {
	PrintVecInstructionsLevel("multithread HNSW", idef.Name(), idef.Opts().FloatVector().Metric(), log);
}

template <>
HnswIndexBase<hnswlib::BruteforceSearch>::HnswIndexBase(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
														size_t currentNsSize, LogCreation log)
	: Base{idef, std::move(payloadType), std::move(fields)},
	  space_{newSpace(Dimension().Value(), metric_)},
	  map_{std::make_unique<hnswlib::BruteforceSearch>(space_.get(), std::max(idef.Opts().FloatVector().StartSize(), currentNsSize))} {
	PrintVecInstructionsLevel("bruteforce", idef.Name(), idef.Opts().FloatVector().Metric(), log);
}

template <typename Map>
HnswIndexBase<Map>::HnswIndexBase(const HnswIndexBase& other, size_t newCapacity)
	: Base{other}, space_{newSpace(Dimension().Value(), metric_)}, map_{std::make_unique<Map>(space_.get(), *other.map_, newCapacity)} {}

template <typename Map>
std::unique_ptr<hnswlib::SpaceInterface> HnswIndexBase<Map>::newSpace(size_t dimension, VectorMetric metric) {
	switch (metric) {
		case VectorMetric::L2:
			return std::make_unique<hnswlib::L2Space>(dimension);
		case VectorMetric::InnerProduct:
			return std::make_unique<hnswlib::InnerProductSpace>(dimension);
		case VectorMetric::Cosine:
			return std::make_unique<hnswlib::CosineSpace>(dimension);
	}
	throw_as_assert;
}

template <typename Map>
void HnswIndexBase<Map>::clearMap() noexcept {
	// This method is used in exception handling. It potentially may throw, but we will not be able to handle this exception properly
	const auto& fvOpts = Opts().FloatVector();
	map_.reset();
	space_.reset();
	space_ = newSpace(Dimension().Value(), metric_);
	if constexpr (std::is_same_v<Map, hnswlib::BruteforceSearch>) {
		map_ = std::make_unique<Map>(space_.get(), fvOpts.StartSize());
	} else {
		map_ = std::make_unique<Map>(space_.get(), fvOpts.StartSize(), fvOpts.M(), fvOpts.EfConstruction(), kHnswRandomSeed,
									 kHNSWAllowReplaceDeleted);
	}
}

template <typename Map>
Variant HnswIndexBase<Map>::upsert(ConstFloatVectorView vect, IdType id, bool& clearCache) {
	if (map_->getCurrentElementCount() >= map_->getMaxElements()) {
		std::ignore = map_->resizeIndex(newSize(map_->getMaxElements()));
	}
	clearCache = true;
	map_->addPointNoLock(vect.Data(), id);
	vect.Strip();
	return Variant{vect};
}

template <typename Map>
Variant HnswIndexBase<Map>::upsertConcurrent(ConstFloatVectorView, IdType, bool&) {
	throw Error(errLogic, "Index (HNSW/bruteforce) {} does not suppor upsertions", Name());
}

template <>
Variant HnswIndexBase<hnswlib::HierarchicalNSWMT>::upsertConcurrent(ConstFloatVectorView vect, IdType id, bool& clearCache) {
	if (map_->getCurrentElementCount() >= map_->getMaxElements()) [[unlikely]] {
		throw Error(errLogic, "Unable to resize '{}' HNSW index during concurrent upsert. Expecting reserve before upsertion", Name());
	}
	clearCache = true;
	map_->addPointConcurrent(vect.Data(), id);
	vect.Strip();
	return Variant{vect};
}

template <>
void HnswIndexBase<hnswlib::BruteforceSearch>::Delete(const Variant&, IdType id, MustExist, StringsHolder&, bool&) {
	map_->removePoint(id);
}

template <>
void HnswIndexBase<hnswlib::HierarchicalNSWMT>::Delete(const Variant&, IdType id, MustExist, StringsHolder&, bool&) {
	map_->markDelete(id);
}

template <>
void HnswIndexBase<hnswlib::HierarchicalNSWST>::Delete(const Variant&, IdType id, MustExist, StringsHolder&, bool&) {
	map_->markDelete(id);
}

template <typename Map>
std::unique_ptr<Index> HnswIndexBase<Map>::Clone(size_t newCapacity) const {
	return std::unique_ptr<HnswIndexBase<Map>>{new HnswIndexBase<Map>{*this, newCapacity}};
}

template <>
IndexMemStat HnswIndexBase<hnswlib::BruteforceSearch>::GetMemStat(const RdxContext& ctx) noexcept {
	auto stats = FloatVectorIndex::GetMemStat(ctx);
	const auto uniqKeysCount = map_->getCurrentElementCount();
	stats.isBuilt = true;  // Bruteforce is always 'built'
	stats.uniqKeysCount += uniqKeysCount;
	stats.dataSize += uniqKeysCount * sizeof(FloatType) * Dimension().Value();	// TOOD maybe map_->size_per_element_
	stats.indexingStructSize +=
		map_->dict_external_to_internal.allocated_mem_size() + (map_->getMaxElements() - uniqKeysCount) * map_->size_per_element_;
	return stats;
}

template <typename Map>
IndexMemStat HnswIndexBase<Map>::GetMemStat(const RdxContext& ctx) noexcept {
	auto stats = FloatVectorIndex::GetMemStat(ctx);
	const auto uniqKeysCount = map_->getCurrentElementCount() - map_->getDeletedCountUnsafe();
	stats.isBuilt = true;  // HNSW is always 'built'
	stats.uniqKeysCount += uniqKeysCount;
	stats.dataSize += uniqKeysCount * sizeof(FloatType) * Dimension().Value();
	stats.indexingStructSize += map_->allocatedMemSize() + sizeof(Map);
	stats.indexingStructSize -= stats.dataSize;	 // Don't calculate actual data size twice
	return stats;
}

template <typename Map>
template <typename ParamsT>
HnswKnnRawResult HnswIndexBase<Map>::search(const float* key, const ParamsT& params) const {
	std::optional<size_t> k = params.K();
	std::optional<float> radius = params.Radius() ? params.Radius() : Opts().FloatVector().Radius();
	size_t ef = 0;
	if constexpr (std::is_same_v<ParamsT, HnswSearchParams>) {
		ef = params.Ef();
	}
	if (radius) {
		auto res = map_->searchRange(key, metric_ == VectorMetric::L2 ? *radius : -*radius, ef);
		if (k) {
			while (res.size() > std::min(*k, map_->getCurrentElementCount())) {
				res.pop();
			}
		}
		return res;
	} else {
		assertrx_dbg(k && *k != 0);
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access) K cannot be empty if radius is empty
		return map_->searchKnn(key, std::min(*k, map_->getCurrentElementCount()), ef);
	}
}

template <typename Map>
HnswKnnRawResult HnswIndexBase<Map>::selectRawImpl(ConstFloatVectorView key, const KnnSearchParams& params) const {
	h_vector<float, 2048> normalizedStorage;
	const float* keyData = key.Data();
	if (metric_ == VectorMetric::Cosine) {
		const auto dims = key.Dimension().Value();
		normalizedStorage.resize(uint32_t(dims));
		ann::NormalizeCopyVector(key.Data(), int32_t(dims), normalizedStorage.data());
		keyData = normalizedStorage.data();
	}
	return std::is_same_v<Map, hnswlib::BruteforceSearch> ? search(keyData, params.BruteForce()) : search(keyData, params.Hnsw());
}

template <typename Map>
KnnRawResult HnswIndexBase<Map>::selectRaw(ConstFloatVectorView key, const KnnSearchParams& params) const {
	return {selectRawImpl(key, params), metric_};
}

template <typename Map>
SelectKeyResult HnswIndexBase<Map>::select(ConstFloatVectorView key, const KnnSearchParams& params, KnnCtx& ctx) const {
	auto knnRes = selectRawImpl(key, params);
	h_vector<RankT, 128> dists;
	base_idset idset;
	if (!knnRes.empty()) {
		dists.resize(knnRes.size());
		idset.resize(knnRes.size());
		size_t lastSameDist = idset.size() - 1;
		const auto sortSameDist = ctx.NeedSort() ? [&](size_t i) {
			bool newDist{false};
			switch (metric_) {
				case VectorMetric::L2: newDist = dists[lastSameDist] > dists[i]; break;
				case VectorMetric::Cosine:
				case VectorMetric::InnerProduct: newDist = dists[lastSameDist] < dists[i]; break;
			}
			if (newDist) {
				std::sort(idset.begin() + i + 1, idset.begin() + lastSameDist + 1);
				lastSameDist = i;
			}
		} : std::function{[](size_t){}};
		for (auto i = knnRes.size(); !knnRes.empty(); knnRes.pop()) {
			--i;
			const auto& res = knnRes.top();
			switch (metric_) {
				case VectorMetric::L2:
					dists[i] = RankT{res.first};
					break;
				case VectorMetric::InnerProduct:
				case VectorMetric::Cosine:
					// IP and cosine metrics are sorted in reverse order in HNSW and has opposite sign
					dists[i] = RankT{-res.first};
					break;
			}
			idset[i] = res.second;
			sortSameDist(i);
		}
		if (ctx.NeedSort()) {
			std::sort(idset.begin(), idset.begin() + lastSameDist + 1);
		}
	}
	IdSet::Ptr resSet = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
	resSet->SetUnordered(IdSetPlain{std::move(idset)});
	ctx.Add(std::move(dists));
	SelectKeyResult result;
	result.emplace_back(std::move(resSet));
	return result;
}

template <typename Map>
ConstFloatVectorView HnswIndexBase<Map>::getFloatVectorView(IdType rowId) const {
	const FloatType* ptr = reinterpret_cast<const FloatType*>(map_->ptrByExternalLabel(rowId));
	return ConstFloatVectorView{std::span<const float>{ptr, Dimension().Value()}};
}

template <typename Map>
void HnswIndexBase<Map>::GrowFor(size_t newElementsCount) {
	const auto requiredSize = newElementsCount + map_->getCurrentElementCount();
	if (requiredSize > map_->getMaxElements()) {
		std::ignore = map_->resizeIndex(requiredSize);
	}
}

template <>
FloatVectorIndex::StorageCacheWriteResult HnswIndexBase<hnswlib::BruteforceSearch>::WriteIndexCache(
	WrSerializer&, PKGetterF&&, bool /*isCompositePK*/, const std::atomic_int32_t& /*cancel*/) noexcept {
	// Cache is not required for bruteforce index
	return StorageCacheWriteResult{.err = {}, .isCacheable = false};
}

template <typename Map>
FloatVectorIndex::StorageCacheWriteResult HnswIndexBase<Map>::WriteIndexCache(WrSerializer& wser, PKGetterF&& getPK, bool isCompositePK,
																			  const std::atomic_int32_t& cancel) noexcept {
	auto res = StorageCacheWriteResult{.err = {}, .isCacheable = true};
	if (!getPK) [[unlikely]] {
		res.err = Error(errParams, "HNSWIndex::WriteIndexCache:{}: PK getter is nullptr", Name());
		return res;
	}

	class [[nodiscard]] Writer final : public hnswlib::IWriter, private WriterBase {
	public:
		Writer(std::string_view name, WrSerializer& ser, PKGetterF&& getPK, bool isCompositePK) noexcept
			: hnswlib::IWriter(), WriterBase{ser, std::move(getPK), isCompositePK}, name_{name} {}

		void PutVarUInt(uint32_t v) override { ser_.PutVarUint(v); }
		void PutVarUInt(uint64_t v) override { ser_.PutVarUint(v); }
		void PutVarInt(int64_t v) override { ser_.PutVarint(v); }
		void PutVarInt(int32_t v) override { ser_.PutVarint(v); }
		void PutVString(std::string_view slice) override { ser_.PutVString(slice); }
		void AppendPKByID(hnswlib::labeltype label) override {
			static_assert(std::numeric_limits<hnswlib::labeltype>::min() >= 0, "Unexpected labeltype limit. Extra check is required");
			if (label > size_t(std::numeric_limits<IdType>::max())) [[unlikely]] {
				throw Error(errLogic, "HNSWIndex::WriteIndexCache:{}: internal id {} is out of range", name_, label);
			}
			writePK(IdType(label));
		}
		size_t Size() const noexcept { return ser_.Len(); }
		size_t Capacity() const noexcept { return ser_.Cap(); }

	private:
		std::string_view name_;
	};

	try {
		if (map_->getDeletedCountUnsafe() > map_->getCurrentElementCount() / 2) {
			res.err = Error{errParams, "Too many deleted elements: {}/{}. Do not creating cache (full rebuild is recommended)",
							map_->getDeletedCountUnsafe(), map_->getCurrentElementCount()};
			return res;
		}
		Writer writer(Name(), wser, std::move(getPK), isCompositePK);
		writer.PutVarUInt(kStorageMagic);
		map_->saveIndex(writer, cancel);
	} catch (Error& err) {
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		res.err = std::move(err);
	} catch (const std::exception& err) {
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		res.err = Error{errLogic, err.what()};
	} catch (...) {
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		res.err = Error{errLogic, "Unexpected exception"};
	}
	return res;
}

template <>
Error HnswIndexBase<hnswlib::BruteforceSearch>::LoadIndexCache(std::string_view /*data*/, bool /*isCompositePK*/,
															   VecDataGetterF&& /*getVectorData*/) {
	return Error(errLogic, "{}:Bruteforce index can not be loaded from binary cache", Name());
}

template <typename Map>
Error HnswIndexBase<Map>::LoadIndexCache(std::string_view data, bool isCompositePK, VecDataGetterF&& getVectorData) {
	if (!getVectorData) [[unlikely]] {
		return Error(errParams, "HNSWIndex::LoadIndexCache:{}: vector data getter is nullptr", Name());
	}

	class [[nodiscard]] Reader final : public hnswlib::IReader, private LoaderBase {
	public:
		Reader(std::string_view name, std::string_view data, VecDataGetterF&& getVectorData, bool isCompositePK) noexcept
			: hnswlib::IReader(), LoaderBase{std::move(getVectorData), isCompositePK}, name_{name}, ser_{data} {}

		uint64_t GetVarUInt() override { return ser_.GetVarUInt(); }
		int64_t GetVarInt() override { return ser_.GetVarint(); }
		std::string_view GetVString() override { return ser_.GetVString(); }
		hnswlib::labeltype ReadPkEncodedData(char* destBuf) override {
			using namespace std::string_view_literals;
			return hnswlib::labeltype(readPKEncodedData(destBuf, ser_, name_, "HNSWIndex"sv));
		}
		size_t RemainingSize() const noexcept { return ser_.Len() - ser_.Pos(); }

	private:
		std::string_view name_;
		Serializer ser_;
	};

	try {
		Reader reader(Name(), data, std::move(getVectorData), isCompositePK);
		const uint64_t magic = reader.GetVarUInt();
		if (magic != kStorageMagic) {
			throw std::runtime_error("Incorrect HNSW storage magic");
		}
		map_->loadIndex(reader, space_.get());
		if (reader.RemainingSize()) {
			throw Error(errLogic, "HNSWIndex::LoadIndexCache:{} has unparsed data: {} bytes", Name(), reader.RemainingSize());
		}
	} catch (Error& err) {
		clearMap();
		assertf_dbg(false, "Unexpected error: {}", err.what());	 // Don't expect this error in test scenarios
		return std::move(err);
	} catch (const std::exception& err) {
		clearMap();
		assertf_dbg(false, "Unexpected std::exception: {}", err.what());  // Don't expect this error in test scenarios
		return Error{errLogic, "HNSWIndex::LoadIndexCache:{}: {}", Name(), err.what()};
	} catch (...) {
		clearMap();
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		return Error{errLogic, "HNSWIndex::LoadIndexCache:{}: unexpected exception", Name()};
	}
	return {};
}

template <typename Map>
size_t HnswIndexBase<Map>::newSize(size_t currentSize) noexcept {
	if (currentSize > 500'000) {
		return currentSize * 1.3;
	} else if (currentSize > 200'000) {
		return currentSize * 1.5;
	} else if (currentSize > 50'000) {
		return currentSize * 2;
	} else {
		return std::max<size_t>(100, currentSize * 4);
	}
}

std::unique_ptr<Index> HnswIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, size_t currentNsSize,
									 LogCreation log) {
	switch (idef.Opts().FloatVector().Multithreading()) {
		case MultithreadingMode::SingleThread:
			return std::make_unique<HnswIndexST>(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
		case MultithreadingMode::MultithreadTransactions:
			return std::make_unique<HnswIndexMT>(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
		default:
			throw Error(errLogic, "Unsupported multithreading mode: {}", int(idef.Opts().FloatVector().Multithreading()));
	}
}

std::unique_ptr<Index> BruteForceVectorIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, size_t currentNsSize,
												 LogCreation log) {
	return std::make_unique<BruteForceVectorIndex>(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
}

}  // namespace reindexer

#endif	// RX_WITH_BUILTIN_ANN_INDEXES
