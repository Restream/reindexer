#include "hnsw.h"
#include "core/index/float_vector/hnswlib/hnswalg.h"

namespace hnswlib {

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::SwitchMapOnQuantized() {
	if (!impl_.GetWritable()->IsQuantized()) {
		logFmt(LogError,
			   "Failed to swap quantized HNSW graph maps. The temporary quantized map is empty, while the main map is not "
			   "quantized.");
		assertrx_throw(false);
	}
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::AddPointNoLock(reindexer::ConstFloatVectorView vect, reindexer::FloatVectorId id) {
	impl_.GetWritable()->AddPointNoLock(vect.Data(), id.AsNumber());
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::AddPointConcurrent(reindexer::ConstFloatVectorView vect, reindexer::FloatVectorId id) {
	impl_.GetWritable()->AddPointConcurrent(vect.Data(), id.AsNumber());
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::ResizeIndex(size_t newSize) {
	impl_.GetWritable()->ResizeIndex(newSize);
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::Quantize(const hnswlib::QuantizationConfig& config) {
	if (!impl_.QuantizationAvailable()) {
		throw reindexer::Error(errLogic, "Index already quantized");
	}

	impl_.Quantize(config);
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::SaveIndex(IWriter& writer, const std::atomic_int32_t& cancel) const {
	serializeQuantizingParams(writer);
	impl_->SaveIndex(writer, cancel);
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::LoadIndex(IReader& reader) {
	if (auto quantizingParams = deserializeQuantizingParams(reader); quantizingParams && reader.WithQuantizer()) {
		impl_.template Load<QuantizedHnswT>(reader, quantizingParams);
	} else {
		impl_.template Load<HnswT>(reader, quantizingParams);
	}
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::serializeQuantizingParams(IWriter& writer) const {
	const bool isQuantized = IsQuantized();
	writer.PutVarUInt(uint32_t(isQuantized ? 1 : 0));
	if (isQuantized) {
		static_cast<const QuantizedHnswT&>(*impl_).quantizer_->Params().Serialize(writer);
	}
}

template <Synchronization synchronization>
std::optional<hnswlib::QuantizingParams> HierarchicalNSW<synchronization>::deserializeQuantizingParams(IReader& reader) {
	std::optional<hnswlib::QuantizingParams> params;
	if (bool needQuantize = reader.GetVarUInt(); needQuantize) {
		params.emplace();
		params->Deserialize(reader);
	}
	return params;
}

template <Synchronization synchronization>
HierarchicalNSW<synchronization>::Impl::Impl(reindexer::IsArray isArray, reindexer::VectorMetric metric, size_t dim, size_t maxElements,
											 size_t M, size_t efConstruction)
	: ptr_(std::make_unique<HnswT>(metric, dim, maxElements, M, efConstruction, kHnswRandomSeed, kHNSWAllowReplaceDeleted)),
	  isArray_(isArray) {}

template <Synchronization synchronization>
template <typename T>
void HierarchicalNSW<synchronization>::Impl::Load(IReader& reader, std::optional<QuantizingParams> quantizingParams) {
	assertrx(!ptr_->IsQuantized());
	assertrx(ptr_->CurrentElementCount() == 0);

	const auto& hnsw = static_cast<const HnswT&>(*ptr_);

	const auto metric = hnsw.fstdistfunc_.Metric();
	const auto dim = hnsw.fstdistfunc_.Dim();
	const auto maxElements = hnsw.MaxElements();
	const auto M = hnsw.M_;
	const auto efConstruction = hnsw.ef_construction_;

	const auto rollback = [&]() {
		ptr_ = std::make_unique<HnswT>(metric, dim, maxElements, M, efConstruction, kHnswRandomSeed, kHNSWAllowReplaceDeleted);
	};

	Reset();
	try {
		ptr_ = std::make_unique<T>(reader, metric, dim, kHnswRandomSeed, kHNSWAllowReplaceDeleted, std::move(quantizingParams));
	} catch (const std::exception& e) {
		logFmt(LogError, "Exception during deserialization hsnw-graph: {}. Trying reload origin empty instance", e.what());
		rollback();
		throw;
	} catch (...) {
		logFmt(LogError, "Unexpected exception during deserialization hsnw-graph. Trying reload origin empty instance");
		rollback();
		throw;
	}
}

template <Synchronization synchronization>
HierarchicalNSW<synchronization>::Impl::Impl(const HierarchicalNSW<synchronization>::Impl& other, size_t newCapacity)
	: isArray_(other.isArray_) {
	assertrx_throw(other.ptr_);
	if (other.quantizedPtr_) {
		assertrx_throw(*other.quantizedPtr_);
	}

	auto makeMap = [newCapacity]<typename T>(const T& map) -> PtrT { return std::make_unique<T>(map, newCapacity); };
	const auto& map = other.quantizedPtr_ ? **other.quantizedPtr_ : *other.ptr_;
	ptr_ = map.IsQuantized() ? makeMap(static_cast<const QuantizedHnswT&>(map)) : makeMap(static_cast<const HnswT&>(map));
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::Impl::Reset() noexcept {
	ptr_.reset();
	quantizedPtr_ = std::nullopt;
}

template <Synchronization synchronization>
void HierarchicalNSW<synchronization>::Impl::Quantize(const hnswlib::QuantizationConfig& config) {
	assertrx(!ptr_->IsQuantized());
	assertrx(!quantizedPtr_);

	quantizedPtr_ = std::make_unique<QuantizedHnswT>(static_cast<const HnswT&>(*ptr_), ptr_->MaxElements(), std::optional(config));
}

template class HierarchicalNSW<Synchronization::None>;
template class HierarchicalNSW<Synchronization::OnInsertions>;

}  // namespace hnswlib
