#include "core/id_type.h"
#include "estl/fast_hash_set.h"
#if RX_WITH_FAISS_ANN_INDEXES

#include "core/query/knn_search_params.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/clone_index.h"
#include "faiss/impl/AuxIndexStructures.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"
#include "ivf_index.h"
#include "knn_ctx.h"
#include "knn_raw_result.h"
#include "sort/pdqsort.hpp"
#include "tools/blas_extension.h"
#include "tools/logger.h"
#include "tools/normalize.h"

#include <numeric>

#ifdef RX_WITH_OPENMP
#include <omp.h>
#endif	// RX_WITH_OPENMP

#if REINDEXER_WITH_SSE
#include "tools/cpucheck.h"
#endif	// REINDEXER_WITH_SSE

#ifdef __linux__
#include <pthread.h>
#include <sys/resource.h>

static uint64_t gettid_ivf() noexcept {
	pthread_t tid = pthread_self();
	uint64_t thread_id = 0;
	memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
	return thread_id;
}

#endif	// __linux__

namespace reindexer {

IvfIndex::IvfIndex(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, LogCreation log)
	: Base{idef, PayloadType{payloadType}, FieldsSet{fields}},
	  nCentroids_{Base::Opts().FloatVector().NCentroids()},
	  space_{newSpace(Dimension().Value(), metric_)} {
	blas_ext::CheckIfBLASAvailable();

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
	if (auto metric = Base::Opts().FloatVector().Metric();
		metric != VectorMetric::L2 && metric != VectorMetric::InnerProduct && metric != VectorMetric::Cosine) {
		throw Error(errLogic, "Attempt to construct IVF index '{}' with unknown metric: {}", Base::Name(), int(metric));
	}
	if (log) {
		logFmt(LogInfo, "Creating IVF index '{}'; Vector instructions level: {}", Base::Name(), vecInstructions);
	}
}

IvfIndex::IvfIndex(const IvfIndex& other)
	: Base{other},
	  nCentroids_{other.nCentroids_},
	  space_{other.map_ ? nullptr : static_cast<faiss::IndexFlat*>(faiss::clone_index(other.space_.get()))},
	  n2FvId_{other.n2FvId_},
	  fvId2N_{other.fvId2N_},
	  map_{other.map_ ? static_cast<faiss::IndexIVFFlat*>(faiss::clone_index(other.map_.get())) : nullptr} {}

// NOLINTBEGIN(bugprone-unchecked-string-to-number-conversion)
static const int kIVFMTMode = std::getenv("RX_IVF_MT") ? atoi(std::getenv("RX_IVF_MT")) : 0;
static const unsigned kIVFOMPThreads =
	std::min(hardware_concurrency(), std::getenv("RX_IVF_OMP_THREADS") ? unsigned(atoi(std::getenv("RX_IVF_OMP_THREADS"))) : 8u);
// NOLINTEND(bugprone-unchecked-string-to-number-conversion)

Variant IvfIndex::upsert(ConstFloatVectorView vect, FloatVectorId id, bool& clearCache) {
	if (map_) {
		const faiss::idx_t faissId = id.AsNumber();
		map_->add_with_ids(1, vect.Data(), &faissId);
	} else {
		space_->add(1, vect.Data());
		fvId2N_[id] = n2FvId_.size();
		n2FvId_.push_back(id.AsNumber());
		assertrx_dbg(n2FvId_.size() == size_t(space_->ntotal));
		if (size_t(space_->ntotal) > ivfTrainingSize(nCentroids_)) {
			auto space = newSpace(Dimension().Value(), metric_);
			auto idx = std::make_unique<faiss::IndexIVFFlat>(space.get(), Dimension().Value(), nCentroids_, faissMetric(),
															 metric_ == VectorMetric::Cosine);

			trainIdx(*idx, space_->get_xb(), space_->get_xb_norms(), space_->ntotal);
			idx->add_with_ids(space_->ntotal, space_->get_xb(), space_->get_xb_norms(), n2FvId_.data());
			space_ = std::move(space);
			map_ = std::move(idx);
			fvId2N_ = IDHashMapT<FloatVectorId, size_t>();
			n2FvId_ = std::vector<faiss::idx_t>();
			map_->parallel_mode = kIVFMTMode;
		}
	}
	clearCache = true;
	vect.Strip();
	return Variant{vect};
}

Variant IvfIndex::upsertConcurrent(ConstFloatVectorView, FloatVectorId, bool&) {
	throw Error(errLogic, "IVF indexes do not support concurrent upsertions");
}

void IvfIndex::del(FloatVectorId id, MustExist mustExist, IsLast) {
	if (map_) {
		const faiss::idx_t faissId = id.AsNumber();
		map_->remove_ids(faiss::IDSelectorArray{1, &faissId});
	} else {
		const auto it = fvId2N_.find(id);
		if (it == fvId2N_.end()) {
			assertrx_throw(!mustExist);
			return;
		}
		const faiss::idx_t faissId = it->second;
		space_->remove_ids(faiss::IDSelectorArray{1, &faissId});
		assertrx_throw(it->second < n2FvId_.size());
		n2FvId_.erase(n2FvId_.begin() + it->second);
		fvId2N_.erase(it);
		for (auto& r : fvId2N_) {
			if (faiss::idx_t(r.second) > faissId) {
				--r.second;
			}
		}
	}
}

struct [[nodiscard]] IVFSearchArgsKnn {
	const float* keyData;
	size_t k;
	std::optional<float> radius;
	faiss::IVFSearchParameters params;
};

template <bool isArray>
static h_vector<float, 128> search(base_idset& idset, const IVFSearchArgsKnn& args, const auto& map, const auto& sortSameDist,
								   const auto& prepareId) {
	h_vector<faiss::idx_t, 128> ids(args.k);
	h_vector<float, 128> dists(args.k);
	map->search(1, args.keyData, args.k, dists.data(), ids.data(), &args.params);
	fast_hash_set<IdType> addedIds;
	if constexpr (isArray) {
		addedIds.reserve(args.k);
	}
	idset.reserve(idset.size() + args.k);
	for (size_t i = 0; i < args.k; ++i) {
		const auto id = ids[i];
		if (id < 0) {
			break;
		}
		const IdType rowId = FloatVectorId::FromNumber(prepareId(id)).RowId();
		if constexpr (isArray) {
			if (!addedIds.insert(rowId).second) {
				continue;
			}
			dists[idset.size()] = dists[i];
		}
		sortSameDist(idset.size(), dists.data());
		idset.push_back(rowId);
	}
	if constexpr (isArray) {
		dists.resize(idset.size());
	}
	return dists;
}

static IvfKnnRawResult searchRaw(const IVFSearchArgsKnn& args, const auto& map, const auto& prepareId) {
	IvfKnnRawResult rawResults(args.k);

	auto& ids = rawResults.Ids();
	auto& dists = rawResults.Dists();

	map->search(1, args.keyData, args.k, dists.data(), ids.data(), &args.params);
	size_t i = 0;
	for (; i < args.k; ++i) {
		auto& id = ids[i];
		if (id < 0) {
			break;
		}
		id = prepareId(id);
	}
	return rawResults;
}

static IvfKnnRawResult searchRaw(const IVFSearchArgsKnn& args, const faiss::IndexIVFFlat& map) {
	IvfKnnRawResult rawResults(args.k);
	map.search(1, args.keyData, args.k, rawResults.Dists().data(), rawResults.Ids().data(), &args.params);
	return rawResults;
}

template <bool isArray>
static h_vector<float, 128> search(base_idset& idset, const IVFSearchArgsKnn& args, const auto& map, VectorMetric metric,
								   const auto& sortSameDist, const auto& prepareId) {
	assertrx_throw(args.radius);

	faiss::RangeSearchResult result(1);
	map->range_search(1, args.keyData, *args.radius, &result, &args.params);

	auto size = result.lims[1] - result.lims[0];

	std::vector<size_t> permut(size);
	std::iota(permut.begin(), permut.end(), 0);
	std::span distances(result.distances + result.lims[0], result.distances + result.lims[1]);
	std::span labels(result.labels + result.lims[0], result.labels + result.lims[1]);
	auto comparator =
		metric == VectorMetric::L2 ? [](float lhs, float rhs) { return lhs < rhs; } : [](float lhs, float rhs) { return lhs > rhs; };
	boost::sort::pdqsort_branchless(permut.begin(), permut.end(), [&comparator, &distances](size_t lhs, size_t rhs) {
		return comparator(distances[lhs], distances[rhs]);
	});

	h_vector<float, 128> dists;
	dists.reserve(size);

	fast_hash_set<IdType> addedIds;
	if constexpr (isArray) {
		addedIds.reserve(size);
	}
	idset.reserve(idset.size() + size);
	for (size_t i = 0; i < size; ++i) {
		const auto id = labels[permut[i]];
		if (id < 0) {
			break;
		}
		const IdType rowId = FloatVectorId::FromNumber(prepareId(id)).RowId();
		if constexpr (isArray) {
			if (!addedIds.insert(rowId).second) {
				continue;
			}
		}
		dists.emplace_back(distances[permut[i]]);
		sortSameDist(idset.size(), dists.data());
		idset.push_back(rowId);
		if (idset.size() == args.k) {
			break;
		}
	}

	return dists;
}

template <bool isArray>
static IvfKnnRawResult searchRaw(const IVFSearchArgsKnn& args, const auto& map, VectorMetric metric, const auto& prepareId) {
	assertrx_throw(args.radius);

	faiss::RangeSearchResult rangeRes(1);
	map->range_search(1, args.keyData, *args.radius, &rangeRes, &args.params);

	auto size = rangeRes.lims[1] - rangeRes.lims[0];

	std::vector<size_t> permut(size);
	std::iota(permut.begin(), permut.end(), 0);
	std::span distances(rangeRes.distances + rangeRes.lims[0], rangeRes.distances + rangeRes.lims[1]);
	std::span labels(rangeRes.labels + rangeRes.lims[0], rangeRes.labels + rangeRes.lims[1]);
	auto comparator =
		metric == VectorMetric::L2 ? [](float lhs, float rhs) { return lhs < rhs; } : [](float lhs, float rhs) { return lhs > rhs; };
	boost::sort::pdqsort_branchless(permut.begin(), permut.end(), [&comparator, &distances](size_t lhs, size_t rhs) {
		return comparator(distances[lhs], distances[rhs]);
	});

	IvfKnnRawResult rawResults;
	rawResults.Reserve(size);

	auto& ids = rawResults.Ids();
	auto& dists = rawResults.Dists();

	fast_hash_set<IdType> addedIds;
	if constexpr (isArray) {
		addedIds.reserve(size);
	}
	for (size_t i = 0; i < size; ++i) {
		const auto id = labels[permut[i]];
		if (id < 0) {
			break;
		}
		const faiss::idx_t preparedId = prepareId(id);
		const IdType rowId = FloatVectorId::FromNumber(preparedId).RowId();
		if constexpr (isArray) {
			if (!addedIds.insert(rowId).second) {
				continue;
			}
		}
		ids.emplace_back(preparedId);
		dists.emplace_back(distances[permut[i]]);
		if (ids.size() == args.k) {
			break;
		}
	}

	return rawResults;
}

SelectKeyResult IvfIndex::select(ConstFloatVectorView key, const KnnSearchParams& p, KnnCtx& ctx) const {
	const auto prepareId = [this](size_t i) noexcept { return n2FvId_[i]; };
	const bool withIVF = bool(map_);
	const bool isArray = *Opts().IsArray();
	if (isArray && withIVF) {
		return select<true>(key, p, ctx, map_, std::identity{});
	} else if (isArray && !withIVF) {
		return select<true>(key, p, ctx, space_, prepareId);
	} else if (!isArray && withIVF) {
		return select<false>(key, p, ctx, map_, std::identity{});
	} else {
		return select<false>(key, p, ctx, space_, prepareId);
	}
}

template <bool isArray>
SelectKeyResult IvfIndex::select(ConstFloatVectorView key, const KnnSearchParams& p, KnnCtx& ctx, const auto& map,
								 const auto& prepareId) const {
	const auto params = p.Ivf();
	const auto k = params.K() ? *params.K() : 0;
	const auto radius = params.Radius() ? params.Radius() : Opts().FloatVector().Radius();
	base_idset idset;
	idset.reserve(k);
	h_vector<float, 2048> normalizedStorage;
	const float* keyData = key.Data();
	if (metric_ == VectorMetric::Cosine) {
		const auto dims = key.Dimension().Value();
		normalizedStorage.resize(uint32_t(dims));
		std::ignore = ann::NormalizeCopyVector(key.Data(), int32_t(dims), normalizedStorage.data());
		keyData = normalizedStorage.data();
	}

	size_t firstSameDist{0};
	const auto sortSameDist = [&](size_t i, const float* distsData) {
		bool newDist{false};
		switch (metric_) {
			case VectorMetric::L2:
				newDist = distsData[firstSameDist] < distsData[i];
				break;
			case VectorMetric::Cosine:
			case VectorMetric::InnerProduct:
				newDist = distsData[firstSameDist] > distsData[i];
				break;
		}
		if (newDist) {
			boost::sort::pdqsort_branchless(idset.begin() + firstSameDist, idset.end());
			firstSameDist = i;
		}
	};
	const auto empty = [](...) noexcept {};
	const bool withSort = *ctx.NeedSort();

	faiss::IVFSearchParameters param;
	param.nprobe = params.NProbe();

	h_vector<float, 128> dists;

	IVFSearchArgsKnn args{keyData, k, radius, param};
	if (radius) {
		if (withSort) {
			dists = search<isArray>(idset, args, map, metric_, sortSameDist, prepareId);
		} else {
			dists = search<isArray>(idset, args, map, metric_, empty, prepareId);
		}
	} else {
		assertrx_throw(k > 0);

		if (withSort) {
			dists = search<isArray>(idset, args, map, sortSameDist, prepareId);
		} else {
			dists = search<isArray>(idset, args, map, empty, prepareId);
		}
	}

	if (withSort) {
		boost::sort::pdqsort_branchless(idset.begin() + firstSameDist, idset.end());
	}
	IdSetPlain::Ptr resSet = make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>(std::move(idset));
	SelectKeyResult result;
	result.emplace_back(std::move(resSet));
	ctx.Add(std::move(dists));
	return result;
}

KnnRawResult IvfIndex::selectRaw(ConstFloatVectorView vect, const KnnSearchParams& params) const {
	if (*Opts().IsArray()) {
		return selectRaw<true>(vect, params);
	} else {
		return selectRaw<false>(vect, params);
	}
}

template <bool isArray>
KnnRawResult IvfIndex::selectRaw(ConstFloatVectorView key, const KnnSearchParams& p) const {
	const auto params = p.Ivf();
	const auto k = params.K() ? *params.K() : 0;
	const auto radius = params.Radius() ? params.Radius() : Opts().FloatVector().Radius();
	h_vector<float, 2048> normalizedStorage;
	const float* keyData = key.Data();
	if (metric_ == VectorMetric::Cosine) {
		const auto dims = key.Dimension().Value();
		normalizedStorage.resize(uint32_t(dims));
		std::ignore = ann::NormalizeCopyVector(key.Data(), int32_t(dims), normalizedStorage.data());
		keyData = normalizedStorage.data();
	}

	const auto prepareId = [this](size_t i) noexcept { return n2FvId_[i]; };

	faiss::IVFSearchParameters param;
	param.nprobe = params.NProbe();

	IVFSearchArgsKnn args{keyData, k, radius, param};
	if (radius) {
		if (map_) {
			return {searchRaw<isArray>(args, map_, metric_, std::identity{}), metric_};
		} else {
			return {searchRaw<isArray>(args, space_, metric_, prepareId), metric_};
		}
	} else {
		assertrx_throw(k > 0);
		IvfKnnRawResult rawResults;
		if (map_) {
			rawResults = searchRaw(args, *map_);
		} else {
			rawResults = searchRaw(args, space_, prepareId);
		}
		if (isArray) {
			removeDuplicateRowId(rawResults.Ids(), rawResults.Dists(), rawResults.Ids().size());
		}
		return {std::move(rawResults), metric_};
	}
}

std::unique_ptr<Index> IvfIndex::Clone(size_t) const { return std::unique_ptr<IvfIndex>{new IvfIndex{*this}}; }

IndexMemStat IvfIndex::GetMemStat(const RdxContext& ctx) const noexcept {
	auto stats = FloatVectorIndex::GetMemStat(ctx);
	size_t uniqKeysCount;
	if (map_) {
		uniqKeysCount = map_->unique_ids_count();
		stats.indexingStructSize += map_->allocated_mem_size();
	} else {
		uniqKeysCount = n2FvId_.size();
		stats.indexingStructSize += n2FvId_.capacity() * sizeof(faiss::idx_t) + fvId2N_.allocated_mem_size() + space_->allocated_mem_size();
	}
	stats.isBuilt = bool(map_);
	stats.uniqKeysCount += uniqKeysCount;
	stats.dataSize += uniqKeysCount * sizeof(float) * Dimension().Value();
	stats.indexingStructSize -= stats.dataSize;	 // Do not calculate actual data size twice
	return stats;
}

void IvfIndex::reconstruct(FloatVectorId id, FloatVector& vect) const {
	if (map_) {
		map_->reconstruct(id.AsNumber(), vect.RawData());
	} else {
		const auto it = fvId2N_.find(id);
		assertrx_throw(it != fvId2N_.end());
		space_->reconstruct(it->second, vect.RawData());
	}
}

void IvfIndex::trainIdx(faiss::IndexIVFFlat& idx, const float* vecs, const float* norms, size_t vecsCount) {
	idx.set_direct_map_type(faiss::DirectMap::Type::Hashtable);
#ifdef RX_WITH_OPENMP
	// omp_set_num_teams(kIVFOMPThreads);
	omp_set_num_threads(kIVFOMPThreads);
#endif	// RX_WITH_OPENMP
#ifdef __linux__
	const auto tid = gettid_ivf();
	const int prio = getpriority(PRIO_PROCESS, tid);
	setpriority(PRIO_PROCESS, gettid_ivf(), 15);
#endif	// __linux__
	idx.train(vecsCount, vecs, norms);
#ifdef __linux__
	setpriority(PRIO_PROCESS, tid, prio);
#endif	// __linux__
}

ConstFloatVectorView IvfIndex::getFloatVectorViewImpl(FloatVectorId id) const {
	if (map_) {
		return map_->getView(id.AsNumber());
	} else {
		const auto it = fvId2N_.find(id);
		assertrx_throw(it != fvId2N_.end());
		return space_->getView(it->second);
	}
}

FloatVectorIndex::StorageCacheWriteResult IvfIndex::WriteIndexCache(WrSerializer& wser, PKGetterF&& getPK, bool isCompositePK,
																	const std::atomic_int32_t& cancel) noexcept {
	auto res = StorageCacheWriteResult{.err = {}, .isCacheable = map_ ? true : false};

	if (!getPK) [[unlikely]] {
		res.err = Error(errParams, "IvfIndex::WriteIndexCache:{}: PK getter is nullptr", Name());
		return res;
	}

	class [[nodiscard]] SerializerWriter final : public faiss::IOWriter, private WriterBase {
	public:
		SerializerWriter(std::string _name, WrSerializer& ser, PKGetterF&& getPK, bool isCompositePK, IsArray isArray) noexcept
			: faiss::IOWriter(), WriterBase{ser, std::move(getPK), isCompositePK, isArray} {
			name = std::move(_name);
		}

		size_t operator()(const void* ptr, size_t size, size_t nitems) override {
			const auto bytes = size * nitems;
			ser_.Write(std::string_view(static_cast<const char*>(ptr), bytes));
			return nitems;
		}
		void AppendPKByID(faiss::idx_t id) override {
			const auto fvId = FloatVectorId::FromNumber(id);
			const IdType rowId = fvId.RowId();
			if (rowId < IdType::Zero() || rowId > IdType::Max()) [[unlikely]] {
				throw Error(errLogic, "IvfIndex::WriteIndexCache:{}: internal id {} is out of range", name, rowId.ToNumber());
			}
			writePK(fvId);
		}
		int filedescriptor() override {
			throw Error(errLogic, "Unexpected call to SerializerWriter::filedescriptor(). Serializer name is '{}'", name);
		}
		void PutVarUInt(uint64_t v) { ser_.PutVarUint(v); }
		size_t Size() const noexcept { return ser_.Len(); }
		size_t Capacity() const noexcept { return ser_.Cap(); }
	};

	try {
		if (map_) {	 // No cache required if map_ is not created yet
			SerializerWriter writer(Name(), wser, std::move(getPK), isCompositePK, Opts().IsArray());
			writer.PutVarUInt(kStorageMagic);
			faiss::write_index(map_.get(), &writer, cancel, true);
		}
	} catch (Error& err) {
		assertf_dbg(false, "Error: '{}'", err.what());	// Don't expect this error in test scenarios
		res.err = std::move(err);
	} catch (const std::exception& err) {
		assertf_dbg(false, "Error: '{}'", err.what());	// Don't expect this error in test scenarios
		res.err = Error{errLogic, err.what()};
	} catch (...) {
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		res.err = Error{errLogic, "Unexpected exception"};
	}
	return res;
}

Error IvfIndex::LoadIndexCache(std::string_view data, bool isCompositePK, FloatVectorIndexRawDataInserter&& getVectorData,
							   LoadWithQuantizer, uint8_t /*version*/) {
	class [[nodiscard]] ViewReader final : public faiss::IOReader, private LoaderBase {
	public:
		ViewReader(std::string _name, std::string_view view, FloatVectorIndexRawDataInserter&& getVectorData, bool isCompositePK,
				   IsArray isArray) noexcept
			: faiss::IOReader(), LoaderBase{std::move(getVectorData), isCompositePK, isArray}, view_{view} {
			name = std::move(_name);
		}

		size_t operator()(void* ptr, size_t size, size_t nitems) override {
			if (view_.empty()) {
				return 0;
			}
			if (size_t nremain = view_.size() / size; nremain < nitems) {
				nitems = nremain;
			}
			if (auto bytesToRead = size * nitems; bytesToRead > 0) {
				memcpy(ptr, view_.data(), bytesToRead);
				view_ = view_.substr(bytesToRead);
			}
			return nitems;
		}
		faiss::idx_t ReadPKEncodedData(uint8_t* destBuf) override {
			using namespace std::string_view_literals;
			Serializer ser(view_);
			const FloatVectorId ifId = readPKEncodedData(destBuf, ser, name, "IVFIndex"sv);
			view_ = view_.substr(ser.Pos());
			return faiss::idx_t(ifId.AsNumber());
		}
		int filedescriptor() override {
			throw Error(errLogic, "IVFFlat::LoadIndexCache:{}: unexpected call to ViewReader::filedescriptor()", name);
		}
		uint64_t GetVarUInt() {
			Serializer ser(view_);
			const auto v = ser.GetVarUInt();
			view_ = view_.substr(ser.Pos());
			return v;
		}
		size_t RemainingSize() const noexcept { return view_.size(); }

	private:
		std::string_view view_;
	};

	map_.reset();
	space_.reset();

	try {
		ViewReader reader(Name(), data, std::move(getVectorData), isCompositePK, Opts().IsArray());
		const uint64_t magic = reader.GetVarUInt();
		if (magic != kStorageMagic) {
			throw std::runtime_error("Incorrect IVF storage magic");
		}
		std::unique_ptr<faiss::Index> idx(faiss::read_index(&reader));
		if (auto map = dynamic_cast<faiss::IndexIVFFlat*>(idx.get()); map) {
			if (auto space = dynamic_cast<faiss::IndexFlat*>(map->quantizer); space) {
				map_ = std::unique_ptr<faiss::IndexIVFFlat>(static_cast<faiss::IndexIVFFlat*>(idx.release()));
				map_->own_fields = false;
				space_ = std::unique_ptr<faiss::IndexFlat>(space);
			} else {
				throw Error(errLogic, "IVFFlat::LoadIndexCache:{} has unexpected quantizer type", Name());
			}
		} else {
			throw Error(errLogic, "IVFFlat::LoadIndexCache:{} has unexpected index type", Name());
		}
		if (reader.RemainingSize()) {
			throw Error(errLogic, "IVFFlat::LoadIndexCache:{} has unparsed data: {} bytes", Name(), reader.RemainingSize());
		}
	} catch (Error& err) {
		clearMap();
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		return err;
	} catch (const std::exception& err) {
		clearMap();
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		return Error{errLogic, "IVFFlat::LoadIndexCache:{}: {}", Name(), err.what()};
	} catch (...) {
		clearMap();
		assertrx_dbg(false);  // Don't expect this error in test scenarios
		return Error{errLogic, "IVFFlat::LoadIndexCache:{}: unexpected exception", Name()};
	}
	return {};
}

void IvfIndex::RebuildCentroids(float dataPart) {
	if (map_) {
		dataPart = std::min(dataPart, 1.0f);
		dataPart = std::max(0.0f, dataPart);

		auto space = newSpace(Dimension().Value(), metric_);
		auto idx = std::make_unique<faiss::IndexIVFFlat>(space.get(), Dimension().Value(), nCentroids_, faissMetric(),
														 metric_ == VectorMetric::Cosine);

		assertrx_dbg(map_->direct_map.type == faiss::DirectMap::Type::Hashtable);
		auto vecsCount = std::max(uint32_t(map_->unique_ids_count() * dataPart), uint32_t(ivfTrainingSize(nCentroids_)));
		vecsCount = vecsCount > map_->unique_ids_count() ? map_->unique_ids_count() : vecsCount;

		const uint32_t dims = Dimension().Value();
		std::unique_ptr<float[]> data = std::make_unique<float[]>(dims * vecsCount);
		std::vector<float> norms;
		if (metric_ == VectorMetric::Cosine) {
			norms.reserve(vecsCount);
		}
		auto outPtr = data.get();
		const auto endPtr = outPtr + vecsCount * dims;
		for (auto& idsPair : map_->direct_map.hashtable) {
			if (outPtr == endPtr) {
				break;
			}
			const auto listNo = faiss::lo_listno(idsPair.second);
			const auto offset = faiss::lo_offset(idsPair.second);
			auto vecPtr = reinterpret_cast<const float*>(map_->invlists->get_single_code(listNo, offset));
			std::memcpy(outPtr, vecPtr, dims * sizeof(float));
			if (metric_ == VectorMetric::Cosine) {
				norms.emplace_back(*map_->invlists->get_single_norm(listNo, offset));
			}
			outPtr += dims;
		}
		trainIdx(*idx, data.get(), norms.size() ? norms.data() : nullptr, vecsCount);
		data.reset();
		for (auto& idsPair : map_->direct_map.hashtable) {
			const auto listNo = faiss::lo_listno(idsPair.second);
			const auto offset = faiss::lo_offset(idsPair.second);
			auto vecPtr = reinterpret_cast<const float*>(map_->invlists->get_single_code(listNo, offset));
			auto normPtr = map_->invlists->get_single_norm(listNo, offset);
			idx->add_with_ids(1, vecPtr, normPtr, &idsPair.first);
		}
		space_ = std::move(space);
		map_ = std::move(idx);
		map_->parallel_mode = kIVFMTMode;
	}
}

std::unique_ptr<faiss::IndexFlat> IvfIndex::newSpace(size_t dimension, VectorMetric metric) {
	switch (metric) {
		case VectorMetric::L2:
			return std::make_unique<faiss::IndexFlatL2>(dimension);
		case VectorMetric::InnerProduct:
			return std::make_unique<faiss::IndexFlatIP>(dimension);
		case VectorMetric::Cosine:
			return std::make_unique<faiss::IndexFlatCosine>(dimension);
	}
	throw_as_assert;
}

void IvfIndex::clearMap() noexcept {
	// This method is used in exception handling. It potentially may throw, but we will not be able to handle this exception properly
	map_.reset();
	space_.reset();
	try {
		space_ = newSpace(Dimension().Value(), metric_);
	} catch (std::exception& e) {
		logFmt(LogError, "Unexpected critical exception: {}. Termination...", e.what());
		std::abort();
	}
}

faiss::MetricType IvfIndex::faissMetric() const noexcept {
	switch (metric_) {
		case VectorMetric::L2:
			return faiss::METRIC_L2;
		case VectorMetric::Cosine:
		case VectorMetric::InnerProduct:
			return faiss::METRIC_INNER_PRODUCT;
	}
	std::abort();
}

std::unique_ptr<Index> IvfIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, LogCreation log) {
	return std::make_unique<IvfIndex>(idef, std::move(payloadType), std::move(fields), log);
}

}  // namespace reindexer

#endif	// RX_WITH_FAISS_ANN_INDEXES
