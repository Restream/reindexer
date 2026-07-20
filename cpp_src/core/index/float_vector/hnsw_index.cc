#if RX_WITH_BUILTIN_ANN_INDEXES

#include "core/idset/idset.h"
#include "core/index/float_vector/float_vector_id.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/float_vector/hnswlib/type_consts.h"
#include "core/rank_t.h"

#include "core/index/float_vector/hnswlib/bruteforce.h"
#include "core/index/float_vector/hnswlib/hnsw.h"
#include "core/query/knn_search_params.h"
#include "hnsw_index.h"
#include "knn_ctx.h"
#include "knn_raw_result.h"
#include "tools/cpucheck.h"
#include "tools/logger.h"

using HierarchicalNSWST = hnswlib::HierarchicalNSW<hnswlib::Synchronization::None>;
using HierarchicalNSWMT = hnswlib::HierarchicalNSW<hnswlib::Synchronization::OnInsertions>;

namespace reindexer {

static_assert(sizeof(hnswlib::labeltype) == sizeof(std::declval<FloatVectorId>().AsNumber()));

static void PrintVecInstructionsLevel(std::string_view indexType, std::string_view name, VectorMetric metric, LogCreation log) {
	std::string vecInstructions = "disabled";
#if REINDEXER_WITH_SSE
	if (IsAVX512Allowed()) {
		vecInstructions = "avx512";
	} else if (IsAVX2Allowed()) {
		vecInstructions = "avx2";
	} else if (IsAVXAllowed()) {
		vecInstructions = "avx";
	} else {
		vecInstructions = "sse";
	}
#endif
	if (metric != VectorMetric::L2 && metric != VectorMetric::InnerProduct && metric != VectorMetric::Cosine) {
		throw Error(errLogic, "Attempt to construct {} index '{}' with unknown metric: {}", indexType, name, int(metric));
	}
	if (log) {
		logFmt(LogInfo, "Creating {} index '{}'; Vector instructions level: {}", indexType, name, vecInstructions);
	}
}

template <typename Map>
HnswIndexBase<Map>::HnswIndexBase(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, size_t currentNsSize,
								  LogCreation log)
	: Base{idef, std::move(payloadType), std::move(fields)},
	  map_{Opts().IsArray(),
		   metric_,
		   Dimension().Value(),
		   std::max(idef.Opts().FloatVector().StartSize(), currentNsSize),
		   idef.Opts().FloatVector().M(),
		   idef.Opts().FloatVector().EfConstruction()} {
	PrintVecInstructionsLevel(fmt::format("{} HNSW", std::is_same_v<Map, HierarchicalNSWST> ? "singlethread" : "multithread"), idef.Name(),
							  idef.Opts().FloatVector().Metric(), log);
}

template <>
HnswIndexBase<hnswlib::BruteforceSearch>::HnswIndexBase(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
														size_t currentNsSize, LogCreation log)
	: Base{idef, std::move(payloadType), std::move(fields)},
	  map_{metric_, Dimension().Value(), std::max(idef.Opts().FloatVector().StartSize(), currentNsSize)} {
	PrintVecInstructionsLevel("bruteforce", idef.Name(), idef.Opts().FloatVector().Metric(), log);
}

template <typename Map>
HnswIndexBase<Map>::HnswIndexBase(const HnswIndexBase& other, size_t newCapacity, IndexCloneKind kind)
	: Base{other, kind}, map_{other.map_, newCapacity} {}

template <typename Map>
void HnswIndexBase<Map>::clearMap() noexcept {
	// This method is used in exception handling. It potentially may throw, but we will not be able to handle this exception properly
	const auto& fvOpts = Opts().FloatVector();
	map_.Reset();
	try {
		map_ = Map(Opts().IsArray(), metric_, Dimension().Value(), fvOpts.StartSize(), fvOpts.M(), fvOpts.EfConstruction());
	} catch (std::exception& e) {
		logFmt(LogError, "Unexpected critical exception: {}. Termination...", e.what());
		std::abort();
	}
}

template <>
void HnswIndexBase<hnswlib::BruteforceSearch>::clearMap() noexcept = delete;

template <typename Map>
Variant HnswIndexBase<Map>::upsert(ConstFloatVectorView vect, FloatVectorId id, bool& clearCache) {
	if (auto curMaxSize = map_.MaxElements(); map_.CurrentElementCount() >= curMaxSize) {
		map_.ResizeIndex(newSize(curMaxSize));
	}
	clearCache = true;
	map_.AddPointNoLock(vect, id);
	vect.Strip();
	return Variant{vect};
}

template <typename Map>
Variant HnswIndexBase<Map>::upsertConcurrent(ConstFloatVectorView, FloatVectorId, bool&) {
	throw Error(errLogic, "Index (HNSW/bruteforce) {} does not suppor upsertions", Name());
}

template <>
Variant HnswIndexBase<HierarchicalNSWMT>::upsertConcurrent(ConstFloatVectorView vect, FloatVectorId id, bool& clearCache) {
	if (map_.CurrentElementCount() >= map_.MaxElements()) [[unlikely]] {
		throw Error(errLogic, "Unable to resize '{}' HNSW index during concurrent upsert. Expecting reserve before upsertion", Name());
	}
	clearCache = true;
	map_.AddPointConcurrent(vect, id);
	vect.Strip();
	return Variant{vect};
}

template <typename Map>
void HnswIndexBase<Map>::del(FloatVectorId id, MustExist) {
	map_.MarkDelete(id);
}

template <>
void HnswIndexBase<hnswlib::BruteforceSearch>::del(FloatVectorId id, MustExist) {
	map_.RemovePoint(id.AsNumber());
}

template <typename Map>
std::unique_ptr<Index> HnswIndexBase<Map>::Clone(size_t newCapacity, IndexCloneKind kind) const {
	return std::unique_ptr<HnswIndexBase<Map>>{new HnswIndexBase<Map>{*this, newCapacity, kind}};
}

template <>
IndexMemStat HnswIndexBase<hnswlib::BruteforceSearch>::GetMemStat(const RdxContext& ctx) const noexcept {
	auto stats = FloatVectorIndex::GetMemStat(ctx);
	const auto uniqKeysCount = map_.CurrentElementCount();
	stats.isBuilt = true;  // Bruteforce is always 'built'
	stats.uniqKeysCount += uniqKeysCount;
	stats.dataSize += uniqKeysCount * map_.ElementSize();
	stats.indexingStructSize += map_.AllocatedMemSize();
	return stats;
}

template <typename Map>
IndexMemStat HnswIndexBase<Map>::GetMemStat(const RdxContext& ctx) const noexcept {
	auto stats = FloatVectorIndex::GetMemStat(ctx);
	const auto uniqKeysCount = map_.CurrentElementCount() - map_.DeletedCountUnsafe();
	stats.isBuilt = true;  // HNSW is always 'built'
	stats.isQuantized = map_.IsQuantized();
	stats.uniqKeysCount += uniqKeysCount;
	stats.dataSize += uniqKeysCount * map_.ElementSize();
	stats.indexingStructSize += map_.AllocatedMemSize();
	stats.indexingStructSize -= stats.dataSize;	 // Don't calculate actual data size twice
	return stats;
}

template <typename Map>
bool HnswIndexBase<Map>::IsSupportMultithreadTransactions() const noexcept {
	return std::is_same_v<Map, HierarchicalNSWMT>;
}

template <typename Map>
hnswlib::SearchResultQueue HnswIndexBase<Map>::search(ConstFloatVectorView key, const KnnSearchParams& params) const {
	static constexpr bool isBF = std::is_same_v<Map, hnswlib::BruteforceSearch>;

	h_vector<float, 2048> normalizedStorage;
	std::optional<float> normL2 = std::nullopt;
	const float* keyData = key.Data();
	if (metric_ == VectorMetric::Cosine) {
		const auto dims = key.Dimension().Value();
		normalizedStorage.resize(uint32_t(dims));
		normL2 = 1.f / ann::NormalizeCopyVector(key.Data(), int32_t(dims), normalizedStorage.data());
		keyData = normalizedStorage.data();
	}

	auto getCommonParams = [this](const auto& params) -> std::tuple<std::optional<size_t>, std::optional<float>> {
		return {params.K(), params.Radius() ? params.Radius() : Opts().FloatVector().Radius()};
	};

	auto [k, radius] = isBF ? getCommonParams(params.BruteForce()) : getCommonParams(params.Hnsw());

	size_t ef = 0;
	if constexpr (!std::is_same_v<Map, hnswlib::BruteforceSearch>) {
		ef = params.Hnsw().Ef();
	}

	if (radius) {
		return map_.SearchRange(keyData, normL2, metric_ == VectorMetric::L2 ? *radius : -*radius, ef);
	} else {
		assertrx_dbg(k && *k != 0);
		// NOLINTNEXTLINE (bugprone-unchecked-optional-access) K cannot be empty if radius is empty
		return map_.SearchKnn(keyData, normL2, *k, ef);
	}
}

template <typename Map>
void HnswIndexBase<Map>::removeOverK(auto& ids, h_vector<RankT, 128>& dists, const auto& params) const {
	assertrx_dbg(ids.size() == dists.size());
	if (params.K() && (params.Radius() || Opts().FloatVector().Radius())) {
		const auto k = *params.K();
		if (ids.size() > k) {
			ids.resize(k);
			dists.resize(k);
		}
	}
}

template <typename Map>
KnnRawResult HnswIndexBase<Map>::selectRaw(ConstFloatVectorView key, const KnnSearchParams& params) const {
	auto knnRes = search(key, params);
	HnswKnnRawResult knnRawRes(knnRes.size());
	for (auto i = knnRes.size(); !knnRes.empty(); knnRes.pop()) {
		--i;
		switch (metric_) {
			case VectorMetric::L2:
				knnRawRes.Dists()[i] = RankT(knnRes.top().first);
				break;
			case VectorMetric::InnerProduct:
			case VectorMetric::Cosine:
				// IP and cosine metrics are sorted in reverse order in HNSW and has opposite sign
				knnRawRes.Dists()[i] = RankT(-knnRes.top().first);
				break;
		}
		knnRawRes.Ids()[i] = FloatVectorId::FromNumber(knnRes.top().second).RowId();
	}
	if (Opts().IsArray()) {
		removeDuplicateRowId(knnRawRes.Ids(), knnRawRes.Dists(), knnRawRes.Ids().size());
	}
	std::is_same_v<Map, hnswlib::BruteforceSearch> ? removeOverK(knnRawRes.Ids(), knnRawRes.Dists(), params.BruteForce())
												   : removeOverK(knnRawRes.Ids(), knnRawRes.Dists(), params.Hnsw());
	return {std::move(knnRawRes), metric_};
}

template <typename Map>
SelectKeyResult HnswIndexBase<Map>::select(ConstFloatVectorView key, const KnnSearchParams& params, KnnCtx& ctx) const {
	auto knnRes = search(key, params);
	h_vector<RankT, 128> dists;
	base_idset idset;
	if (!knnRes.empty()) {
		dists.resize(knnRes.size());
		idset.resize(knnRes.size());
		size_t lastSameDist = idset.size() - 1;
		const auto sortSameDist = [&](size_t i) {
			if (ctx.NeedSort()) {
				bool newDist{false};
				switch (metric_) {
					case VectorMetric::L2:
						newDist = dists[lastSameDist] > dists[i];
						break;
					case VectorMetric::Cosine:
					case VectorMetric::InnerProduct:
						newDist = dists[lastSameDist] < dists[i];
						break;
				}
				if (newDist) {
					std::sort(idset.begin() + i + 1, idset.begin() + lastSameDist + 1);
					lastSameDist = i;
				}
			}
		};
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
			idset[i] = FloatVectorId::FromNumber(res.second).RowId();
			sortSameDist(i);
		}
		if (ctx.NeedSort()) {
			std::sort(idset.begin(), idset.begin() + lastSameDist + 1);
		}
		if (Opts().IsArray()) {
			removeDuplicateRowId(idset, dists, idset.size());
		}
		std::is_same_v<Map, hnswlib::BruteforceSearch> ? removeOverK(idset, dists, params.BruteForce())
													   : removeOverK(idset, dists, params.Hnsw());
	}
	IdSetPlain::Ptr resSet = make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>(std::move(idset));
	SelectKeyResult result;
	result.emplace_back(std::move(resSet));
	ctx.Add(std::move(dists));
	return result;
}

namespace {

struct [[nodiscard]] HnswStreamingSessionImpl final : KnnStreamingSession::Impl {
	// Keeps the normalized Cosine query alive for the whole session: hnswlib stores a raw pointer (no copy for float).
	h_vector<float, 2048> queryStorage;
	hnswlib::StreamingSearchSession session;

	template <typename Map>
	HnswStreamingSessionImpl(const Map& map, ConstFloatVectorView key, VectorMetric metric, size_t ef)
		: session{beginHnswStreamingSession(map, key, metric, ef, queryStorage)} {}

private:
	template <typename Map>
	hnswlib::StreamingSearchSession beginHnswStreamingSession(const Map& map, ConstFloatVectorView key, VectorMetric metric, size_t ef,
															  h_vector<float, 2048>& queryStorage) {
		const float* keyData = key.Data();
		std::optional<float> normL2 = std::nullopt;
		if (metric == VectorMetric::Cosine) {
			const auto dims = key.Dimension().Value();
			queryStorage.resize(uint32_t(dims));
			normL2 = 1.f / ann::NormalizeCopyVector(key.Data(), int32_t(dims), queryStorage.data());
			keyData = queryStorage.data();
		}
		return map.BeginStreamingSearch(keyData, normL2, hnswlib::StreamingSearchOptions{.ef = ef});
	}
};
}  // namespace

template <typename Map>
KnnStreamingSession HnswIndexBase<Map>::beginStreaming(ConstFloatVectorView key, size_t ef) const {
	return KnnStreamingSession{std::make_unique<HnswStreamingSessionImpl>(map_, key, metric_, ef)};
}

template <typename Map>
void HnswIndexBase<Map>::continueStreaming(KnnStreamingSession& session, size_t batchSize, KnnStreamingBatch& out) const {
	auto* impl = static_cast<HnswStreamingSessionImpl*>(session.GetImpl());
	assertrx_throw(impl);
	auto hnswBatch = map_.ContinueStreamingSearch(impl->session, batchSize);
	auto& results = hnswBatch.results;

	out.exhausted = hnswBatch.exhausted;
	const size_t n = results.size();
	out.ids.resize(n);
	out.ranks.resize(n);
	if (n == 0) {
		return;
	}
	for (auto i = n; !results.empty(); results.pop()) {
		--i;
		switch (metric_) {
			case VectorMetric::L2:
				out.ranks[i] = RankT{results.top().first};
				break;
			case VectorMetric::InnerProduct:
			case VectorMetric::Cosine:
				// IP and cosine metrics are sorted in reverse order in HNSW and have the opposite sign
				out.ranks[i] = RankT{-results.top().first};
				break;
		}
		out.ids[i] = FloatVectorId::FromNumber(results.top().second).RowId();
	}
}

template <>
KnnStreamingSession HnswIndexBase<hnswlib::BruteforceSearch>::beginStreaming(ConstFloatVectorView, size_t) const {
	throw Error(errQueryExec, "Streaming KNN search (KNN query without 'k' and 'radius') is supported for HNSW indexes only");
}

template <>
void HnswIndexBase<hnswlib::BruteforceSearch>::continueStreaming(KnnStreamingSession&, size_t, KnnStreamingBatch&) const {
	throw Error(errQueryExec, "Streaming KNN search (KNN query without 'k' and 'radius') is supported for HNSW indexes only");
}

template <typename Map>
ConstFloatVectorView HnswIndexBase<Map>::getFloatVectorViewImpl(FloatVectorId id) const {
	if (IsQuantized()) {
		return ConstFloatVectorView::CreateStripped(Dimension());
	} else {
		return ConstFloatVectorView{std::span{map_.FloatPtrByExternalLabel(id.AsNumber()), Dimension().Value()}};
	}
}

template <typename Map>
void HnswIndexBase<Map>::GrowFor(size_t newElementsCount) {
	const auto requiredSize = newElementsCount + map_.CurrentElementCount();

	if (const auto curMaxSize = map_.MaxElements(); requiredSize > curMaxSize) {
		map_.ResizeIndex(requiredSize);
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
		Writer(std::string_view name, WrSerializer& ser, PKGetterF&& getPK, bool isCompositePK, IsArray isArray) noexcept
			: hnswlib::IWriter(), WriterBase{ser, std::move(getPK), isCompositePK, isArray}, name_{name} {}

		void PutVarUInt(uint32_t v) override { ser_.PutVarUint(v); }
		void PutVarUInt(uint64_t v) override { ser_.PutVarUint(v); }
		void PutVarInt(int64_t v) override { ser_.PutVarint(v); }
		void PutVarInt(int32_t v) override { ser_.PutVarint(v); }
		void PutFloat(float v) override { ser_.PutFloat(v); }
		void PutVString(std::string_view slice) override { ser_.PutVString(slice); }
		void AppendPKByID(hnswlib::labeltype label) override {
			static_assert(std::numeric_limits<hnswlib::labeltype>::min() >= 0, "Unexpected labeltype limit. Extra check is required");
			const auto fvId = FloatVectorId::FromNumber(label);
			const IdType rowId = fvId.RowId();
			if (rowId > IdType::Max()) [[unlikely]] {
				throw Error(errLogic, "HNSWIndex::WriteIndexCache:{}: internal id {} is out of range", name_, rowId.ToNumber());
			}
			writePK(fvId);
		}
		size_t Size() const noexcept { return ser_.Len(); }
		size_t Capacity() const noexcept { return ser_.Cap(); }

	private:
		std::string_view name_;
	};

	try {
		if (map_.DeletedCountUnsafe() > map_.CurrentElementCount() / 2) {
			res.err = Error{errParams, "Too many deleted elements: {}/{}. Do not creating cache (full rebuild is recommended)",
							map_.DeletedCountUnsafe(), map_.CurrentElementCount()};
			return res;
		}
		Writer writer(Name(), wser, std::move(getPK), isCompositePK, Opts().IsArray());
		writer.PutVarUInt(kStorageMagic);
		map_.SaveIndex(writer, cancel);
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
															   FloatVectorIndexRawDataInserter&& /*getVectorData*/, LoadWithQuantizer) {
	return Error(errLogic, "{}:Bruteforce index can not be loaded from binary cache", Name());
}

template <typename Map>
Error HnswIndexBase<Map>::LoadIndexCache(std::string_view data, bool isCompositePK, FloatVectorIndexRawDataInserter&& getVectorData,
										 LoadWithQuantizer withQuantizer) {
	class [[nodiscard]] Reader final : public hnswlib::IReader, private LoaderBase {
	public:
		Reader(std::string_view name, std::string_view data, FloatVectorIndexRawDataInserter&& getVectorData, bool isCompositePK,
			   LoadWithQuantizer withQuantizer, IsArray isArray) noexcept
			: hnswlib::IReader(),
			  LoaderBase{std::move(getVectorData), isCompositePK, isArray},
			  name_{name},
			  ser_{data},
			  withQuantizer_(withQuantizer) {}

		uint64_t GetVarUInt() override { return ser_.GetVarUInt(); }
		int64_t GetVarInt() override { return ser_.GetVarint(); }
		float GetFloat() override { return ser_.GetFloat(); }
		std::string_view GetVString() override { return ser_.GetVString(); }
		hnswlib::labeltype ReadPkEncodedData(float* destBuf) override {
			using namespace std::string_view_literals;
			return hnswlib::labeltype(readPKEncodedData(destBuf, ser_, name_, "HNSWIndex"sv).AsNumber());
		}
		size_t RemainingSize() const noexcept { return ser_.Len() - ser_.Pos(); }

		bool WithQuantizer() const override { return *withQuantizer_; }

	private:
		std::string_view name_;
		Serializer ser_;
		LoadWithQuantizer withQuantizer_;
	};

	try {
		Reader reader(Name(), data, std::move(getVectorData), isCompositePK, withQuantizer, Opts().IsArray());
		const uint64_t magic = reader.GetVarUInt();
		if (magic != kStorageMagic) {
			throw std::runtime_error("Incorrect HNSW storage magic");
		}
		map_.LoadIndex(reader);
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
uint64_t HnswIndexBase<Map>::GetHash(FloatVectorId id) const {
	return map_.GetHash(id);
}

template <>
uint64_t HnswIndexBase<hnswlib::BruteforceSearch>::GetHash(FloatVectorId id) const {
	return FloatVectorIndex::getFloatVectorView(id).Hash();
}

template <typename Map>
bool HnswIndexBase<Map>::QuantizationAvailable() const {
	return map_.QuantizationAvailable() &&
		   (opts_.FloatVector().QuantizationConfig() &&
			(map_.CurrentElementCount() >= opts_.FloatVector().QuantizationConfig()->quantizationThreshold));
}

template <typename Map>
bool HnswIndexBase<Map>::IsQuantized() const {
	return map_.IsQuantized();
}

template <typename Map>
void HnswIndexBase<Map>::Quantize() {
	if constexpr (!std::is_same_v<Map, hnswlib::BruteforceSearch>) {
		if (auto config = opts_.FloatVector().QuantizationConfig(); config) {
			map_.Quantize(*config);
		} else {
			throw Error(errLogic, "Empty quantization config");
		}
	} else {
		assertrx(false);
	}
}

template <typename Map>
void HnswIndexBase<Map>::SwitchMapOnQuantized() {
	if constexpr (!std::is_same_v<Map, hnswlib::BruteforceSearch>) {
		map_.SwitchMapOnQuantized();
	} else {
		assertrx(false);
	}
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
			return std::make_unique<HnswIndexBase<HierarchicalNSWST>>(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
		case MultithreadingMode::MultithreadTransactions:
			return std::make_unique<HnswIndexBase<HierarchicalNSWMT>>(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
		default:
			throw Error(errLogic, "Unsupported multithreading mode: {}", int(idef.Opts().FloatVector().Multithreading()));
	}
}

std::unique_ptr<Index> BruteForceVectorIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, size_t currentNsSize,
												 LogCreation log) {
	return std::make_unique<HnswIndexBase<hnswlib::BruteforceSearch>>(idef, std::move(payloadType), std::move(fields), currentNsSize, log);
}

}  // namespace reindexer

#endif	// RX_WITH_BUILTIN_ANN_INDEXES
