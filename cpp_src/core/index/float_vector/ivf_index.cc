#if RX_WITH_FAISS_ANN_INDEXES

#include "ivf_index.h"
#include "core/query/knn_search_params.h"
#include "core/selectfunc/ctx/knn_ctx.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/clone_index.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"
#include "tools/blas_extension.h"
#include "tools/distances/ip_dist.h"
#include "tools/distances/l2_dist.h"
#include "tools/logger.h"
#include "tools/normalize.h"

#include <omp.h>
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

IvfIndex::IvfIndex(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, CreationLog log)
	: Base{idef, PayloadType{payloadType}, FieldsSet{fields}},
	  nCentroids_{Base::Opts().FloatVector().NCentroids()},
	  space_{newSpace(Dimension().Value(), metric_)} {
	blas_ext::CheckIfBLASAvailable();

	std::string vecInstructions = "disabled";
	switch (Base::Opts().FloatVector().Metric()) {
		case VectorMetric::L2:
			if (vector_dists::L2WithAVX512()) {
				vecInstructions = "avx512";
			} else if (vector_dists::L2WithAVX2()) {
				vecInstructions = "avx2";
			} else if (vector_dists::L2WithAVX()) {
				vecInstructions = "avx";
			} else if (vector_dists::L2WithSSE()) {
				vecInstructions = "sse";
			}
			break;
		case VectorMetric::InnerProduct:
		case VectorMetric::Cosine:
			if (vector_dists::InnerProductWithAVX512()) {
				vecInstructions = "avx512";
			} else if (vector_dists::InnerProductWithAVX2()) {
				vecInstructions = "avx2";
			} else if (vector_dists::InnerProductWithAVX()) {
				vecInstructions = "avx";
			} else if (vector_dists::InnerProductWithSSE()) {
				vecInstructions = "sse";
			}
			break;
		default:
			throw Error(errLogic, "Attempt to construct IVF index '{}' with unknow metric: {}", Base::Name(),
						int(Base::Opts().FloatVector().Metric()));
	}
	if (log == Index::CreationLog::Yes) {
		logFmt(LogInfo, "Creating IVF index '{}'; Vector instructions level: {}", Base::Name(), vecInstructions);
	}
}

IvfIndex::IvfIndex(const IvfIndex& other)
	: Base{other},
	  nCentroids_{other.nCentroids_},
	  space_{other.map_ ? nullptr : static_cast<faiss::IndexFlat*>(faiss::clone_index(other.space_.get()))},
	  n2RowId_{other.n2RowId_},
	  rowId2N_{other.rowId2N_},
	  map_{other.map_ ? static_cast<faiss::IndexIVFFlat*>(faiss::clone_index(other.map_.get())) : nullptr} {}

static const int kIVFMTMode = std::getenv("RX_IVF_MT") ? atoi(std::getenv("RX_IVF_MT")) : 0;
static const unsigned kIVFOMPThreads =
	std::min(hardware_concurrency(), std::getenv("RX_IVF_OMP_THREADS") ? unsigned(atoi(std::getenv("RX_IVF_OMP_THREADS"))) : 8u);

Variant IvfIndex::upsert(ConstFloatVectorView vect, IdType id, bool& clearCache) {
	if (map_) {
		const faiss::idx_t faissId{id};
		map_->add_with_ids(1, vect.Data(), &faissId);
	} else {
		space_->add(1, vect.Data());
		rowId2N_[id] = n2RowId_.size();
		n2RowId_.push_back(id);
		assertrx_dbg(n2RowId_.size() == size_t(space_->ntotal));
		if (space_->ntotal > ivfTrainingSize(nCentroids_)) {
			auto space = newSpace(Dimension().Value(), metric_);
			auto idx = std::make_unique<faiss::IndexIVFFlat>(space.get(), Dimension().Value(), nCentroids_, faissMetric(),
															 metric_ == VectorMetric::Cosine);

			trainIdx(*idx, space_->get_xb(), space_->get_xb_norms(), space_->ntotal);
			idx->add_with_ids(space_->ntotal, space_->get_xb(), space_->get_xb_norms(), n2RowId_.data());
			space_ = std::move(space);
			map_ = std::move(idx);
			rowId2N_ = IDHashMapT<IdType, size_t>();
			n2RowId_ = std::vector<faiss::idx_t>();
			map_->parallel_mode = kIVFMTMode;
		}
	}
	clearCache = true;
	vect.Strip();
	return Variant{vect};
}

void IvfIndex::Delete(const Variant&, IdType id, StringsHolder&, bool&) {
	if (map_) {
		const faiss::idx_t faissId = id;
		map_->remove_ids(faiss::IDSelectorArray{1, &faissId});
	} else {
		const auto it = rowId2N_.find(id);
		assertrx_throw(it != rowId2N_.end());
		const faiss::idx_t faissId = it->second;
		space_->remove_ids(faiss::IDSelectorArray{1, &faissId});
		assertrx_throw(it->second < n2RowId_.size());
		n2RowId_.erase(n2RowId_.begin() + it->second);
		rowId2N_.erase(it);
		for (auto& r : rowId2N_) {
			if (faiss::idx_t(r.second) > faissId) {
				--r.second;
			}
		}
	}
}

SelectKeyResult IvfIndex::select(ConstFloatVectorView key, const KnnSearchParams& p, KnnCtx& ctx) const {
	const auto params = p.Ivf();
	const auto k = params.K();
	h_vector<float, 128> dists(k);
	h_vector<faiss::idx_t, 128> ids(k);
	SelectKeyResult result;
	IdSet::Ptr resSet = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
	base_idset idset;
	idset.reserve(k);
	h_vector<float, 2048> normalizedStorage;
	const float* keyData = key.Data();
	if (metric_ == VectorMetric::Cosine) {
		const auto dims = key.Dimension().Value();
		normalizedStorage.resize(uint32_t(dims));
		ann::NormalizeCopyVector(key.Data(), int32_t(dims), normalizedStorage.data());
		keyData = normalizedStorage.data();
	}
	size_t firstSameDist{0};
	const auto sortSameDist = ctx.NeedSort() ? [&, distsData = dists.data()](size_t i){
		bool newDist{false};
		switch (metric_) {
			case VectorMetric::L2: newDist = distsData[firstSameDist] < distsData[i]; break;
			case VectorMetric::Cosine:
			case VectorMetric::InnerProduct: newDist = distsData[firstSameDist] > distsData[i]; break;
		}
		if (newDist) {
			std::sort(idset.begin() + firstSameDist, idset.end());
			firstSameDist = i;
		}
	 } : std::function{[](size_t){}};
	if (map_) {
		faiss::IVFSearchParameters param;
		param.nprobe = params.NProbe();
		map_->search(1, keyData, k, dists.data(), ids.data(), &param);
		for (size_t i = 0, s = ids.size(); i < s; ++i) {
			const auto id = ids[i];
			if (id < 0) {
				break;
			}
			sortSameDist(i);
			idset.push_back(id);
		}
	} else {
		space_->search(1, keyData, k, dists.data(), ids.data());
		for (size_t i = 0, s = ids.size(); i < s; ++i) {
			const auto id = ids[i];
			if (id < 0) {
				break;
			}
			sortSameDist(i);
			idset.push_back(n2RowId_[id]);
		}
	}
	if (ctx.NeedSort()) {
		std::sort(idset.begin() + firstSameDist, idset.end());
	}
	resSet->SetUnordered(std::move(idset));
	ctx.Add(std::span<float>{dists.data(), resSet->size()});
	result.emplace_back(std::move(resSet));
	return result;
}

std::unique_ptr<Index> IvfIndex::Clone(size_t) const { return std::unique_ptr<IvfIndex>{new IvfIndex{*this}}; }

IndexMemStat IvfIndex::GetMemStat(const RdxContext& ctx) noexcept {
	auto stats = FloatVectorIndex::GetMemStat(ctx);
	size_t uniqKeysCount;
	if (map_) {
		uniqKeysCount = map_->unique_ids_count();
		stats.indexingStructSize += map_->allocated_mem_size();
	} else {
		uniqKeysCount = n2RowId_.size();
		stats.indexingStructSize +=
			n2RowId_.capacity() * sizeof(faiss::idx_t) + rowId2N_.allocated_mem_size() + space_->allocated_mem_size();
	}
	stats.isBuilt = bool(map_);
	stats.uniqKeysCount += uniqKeysCount;
	stats.dataSize += uniqKeysCount * sizeof(float) * Dimension().Value();
	stats.indexingStructSize -= stats.dataSize;	 // Do not calculate actual data size twice
	return stats;
}

void IvfIndex::reconstruct(IdType rowId, FloatVector& vect) const {
	if (map_) {
		map_->reconstruct(rowId, vect.RawData());
	} else {
		const auto it = rowId2N_.find(rowId);
		assertrx_throw(it != rowId2N_.end());
		space_->reconstruct(it->second, vect.RawData());
	}
}

void IvfIndex::trainIdx(faiss::IndexIVFFlat& idx, const float* vecs, const float* norms, size_t vecsCount) {
	idx.set_direct_map_type(faiss::DirectMap::Type::Hashtable);
	// omp_set_num_teams(kIVFOMPThreads);
	omp_set_num_threads(kIVFOMPThreads);
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

FloatVector IvfIndex::getFloatVector(IdType id) const {
	auto result = FloatVector::CreateNotInitialized(Dimension());
	reconstruct(id, result);
	return result;
}

ConstFloatVectorView IvfIndex::getFloatVectorView(IdType id) const {
	if (map_) {
		return map_->getView(id);
	} else {
		const auto it = rowId2N_.find(id);
		assertrx_throw(it != rowId2N_.end());
		return space_->getView(it->second);
	}
}

FloatVectorIndex::StorageCacheWriteResult IvfIndex::WriteIndexCache(WrSerializer& wser, PKGetterF&& getPK, bool isCompositePK,
																	const std::atomic_int32_t& cancel) noexcept {
	auto res = StorageCacheWriteResult{.err = {}, .isCacheable = map_ ? true : false};

	if rx_unlikely (!getPK) {
		res.err = Error(errParams, "IvfIndex::WriteIndexCache:{}: PK getter is nullptr", Name());
		return res;
	}

	class SerializerWriter final : public faiss::IOWriter, private WriterBase {
	public:
		SerializerWriter(std::string _name, WrSerializer& ser, PKGetterF&& getPK, bool isCompositePK) noexcept
			: faiss::IOWriter(), WriterBase{ser, std::move(getPK), isCompositePK} {
			name = std::move(_name);
		}

		size_t operator()(const void* ptr, size_t size, size_t nitems) override {
			const auto bytes = size * nitems;
			ser_.Write(std::string_view(static_cast<const char*>(ptr), bytes));
			return nitems;
		}
		void AppendPKByID(faiss::idx_t id) override {
			if rx_unlikely (id < 0 || id > std::numeric_limits<IdType>::max()) {
				throw Error(errLogic, "IvfIndex::WriteIndexCache:{}: internal id {} is out of range", name, id);
			}
			writePK(IdType(id));
		}
		int filedescriptor() override {
			throw Error(errLogic, "Unexpcted call to SerializerWriter::filedescriptor(). Serializer name is '{}'", name);
		}
		void PutVarUInt(uint64_t v) { ser_.PutVarUint(v); }
		size_t Size() const noexcept { return ser_.Len(); }
		size_t Capacity() const noexcept { return ser_.Cap(); }
	};

	try {
		if (map_) {	 // No cache required if map_ is not created yet
			SerializerWriter writer(Name(), wser, std::move(getPK), isCompositePK);
			writer.PutVarUInt(kStorageMagic);
			faiss::write_index(map_.get(), &writer, cancel, true);
		}
	} catch (Error& err) {
		assertrx_dbg(false);  // Do not expecting this error in test scenarious
		res.err = std::move(err);
	} catch (const std::exception& err) {
		assertrx_dbg(false);  // Do not expecting this error in test scenarious
		res.err = Error{errLogic, err.what()};
	} catch (...) {
		assertrx_dbg(false);  // Do not expecting this error in test scenarious
		res.err = Error{errLogic, "Unexpected exception"};
	}
	return res;
}

Error IvfIndex::LoadIndexCache(std::string_view data, bool isCompositePK, VecDataGetterF&& getVectorData) {
	if rx_unlikely (!getVectorData) {
		return Error(errParams, "IvfIndex::LoadIndexCache:{}: vector data getter is nullptr", Name());
	}

	class ViewReader final : public faiss::IOReader, private LoaderBase {
	public:
		ViewReader(std::string _name, std::string_view view, VecDataGetterF&& getVectorData, bool isCompositePK) noexcept
			: faiss::IOReader(), LoaderBase{std::move(getVectorData), isCompositePK}, view_{view} {
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
			const IdType itemID = readPKEncodedData(destBuf, ser, name, "IVFIndex"sv);
			view_ = view_.substr(ser.Pos());
			return faiss::idx_t(itemID);
		}
		int filedescriptor() override {
			throw Error(errLogic, "IVFFlat::LoadIndexCache:{}: unexpcted call to ViewReader::filedescriptor()", name);
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
		ViewReader reader(Name(), data, std::move(getVectorData), isCompositePK);
		const uint64_t magic = reader.GetVarUInt();
		if (magic != kStorageMagic) {
			throw std::runtime_error("Incorrect IVF storage magic");
		}
		std::unique_ptr<faiss::Index> idx(faiss::read_index(&reader));
		if (faiss::IndexIVFFlat* map = dynamic_cast<faiss::IndexIVFFlat*>(idx.get()); map) {
			if (faiss::IndexFlat* space = dynamic_cast<faiss::IndexFlat*>(map->quantizer); space) {
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
		assertrx_dbg(false);  // Do not expecting this error in test scenarious
		return err;
	} catch (const std::exception& err) {
		clearMap();
		assertrx_dbg(false);  // Do not expecting this error in test scenarious
		return Error{errLogic, "IVFFlat::LoadIndexCache:{}: {}", Name(), err.what()};
	} catch (...) {
		clearMap();
		assertrx_dbg(false);  // Do not expecting this error in test scenarious
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
	space_ = newSpace(Dimension().Value(), metric_);
}

faiss::MetricType IvfIndex::faissMetric() const noexcept {
	switch (metric_) {
		case VectorMetric::L2:
			return faiss::METRIC_L2;
		case VectorMetric::Cosine:
		case VectorMetric::InnerProduct:
			return faiss::METRIC_INNER_PRODUCT;
	}
	throw_as_assert;
}

std::unique_ptr<Index> IvfIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, Index::CreationLog log) {
	return std::make_unique<IvfIndex>(idef, std::move(payloadType), std::move(fields), log);
}

}  // namespace reindexer

#endif	// RX_WITH_FAISS_ANN_INDEXES
