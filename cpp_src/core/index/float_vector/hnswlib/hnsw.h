#pragma once

#include "core/index/float_vector/float_vector_id.h"
#include "core/index/float_vector/scalar_quantization/quantization_params.h"
#include "core/keyvalue/float_vector.h"
#include "hnsw_interface.h"
namespace hnswlib {

template <typename ValueT, Synchronization synchronization>
class HierarchicalNSWImpl;

template <Synchronization synchronization>
class [[nodiscard]] HierarchicalNSW {
public:
	template <typename... Args>
	explicit HierarchicalNSW(Args&&... args) noexcept : impl_(std::forward<Args>(args)...) {}

	explicit HierarchicalNSW(const HierarchicalNSW& other, size_t newCapacity) : impl_(other.impl_, newCapacity) {}

	RX_ALWAYS_INLINE size_t MaxElements() const noexcept { return impl_->MaxElements(); }
	RX_ALWAYS_INLINE size_t CurrentElementCount() const noexcept { return impl_->CurrentElementCount(); }
	RX_ALWAYS_INLINE size_t DeletedCountUnsafe() const noexcept { return impl_->DeletedCountUnsafe(); }
	RX_ALWAYS_INLINE size_t AllocatedMemSize() const noexcept { return impl_->AllocatedMemSize(); }
	RX_ALWAYS_INLINE size_t ElementSize() const noexcept { return impl_->ElementSize(); }

	RX_ALWAYS_INLINE labeltype ExternalLabel(tableint internalId) const { return impl_->ExternalLabel(internalId); }
	RX_ALWAYS_INLINE bool IsMarkedDeleted(tableint internalId) const noexcept { return impl_->IsMarkedDeleted(internalId); }
	RX_ALWAYS_INLINE const float* FloatPtrByExternalLabel(labeltype label) const { return impl_->FloatPtrByExternalLabel(label); }

	RX_ALWAYS_INLINE void MarkDelete(reindexer::FloatVectorId id) {
		impl_.GetWritable()->MarkDelete(id.AsNumber());
	}

	void AddPointNoLock(reindexer::ConstFloatVectorView vect, reindexer::FloatVectorId id);
	void AddPointConcurrent(reindexer::ConstFloatVectorView vect, reindexer::FloatVectorId id);

	void ResizeIndex(size_t newMaxElements);

	void SaveIndex(IWriter& writer, const std::atomic_int32_t& cancel) const;
	void LoadIndex(IReader& reader);

	RX_ALWAYS_INLINE SearchResultQueue SearchKnn(const float* queryDataRaw, std::optional<float> queryDataNorm, size_t k,
												 size_t ef = 0) const {
		return impl_->SearchKnn(queryDataRaw, queryDataNorm, k, ef);
	}
	RX_ALWAYS_INLINE SearchResultQueue SearchRange(const float* queryDataRaw, std::optional<float> queryDataNorm, float radius,
												   size_t ef) const {
		return impl_->SearchRange(queryDataRaw, queryDataNorm, radius, ef);
	}

	RX_ALWAYS_INLINE bool IsQuantized() const noexcept { return impl_->IsQuantized(); }
	RX_ALWAYS_INLINE bool QuantizationAvailable() const noexcept { return impl_.QuantizationAvailable(); }
	void Quantize(const QuantizationConfig& config);
	void SwitchMapOnQuantized();

	RX_ALWAYS_INLINE size_t GetHash(reindexer::FloatVectorId id) const {
		return impl_->GetHash(id.AsNumber());
	}

	RX_ALWAYS_INLINE void Reset() noexcept { return impl_.Reset(); }

private:
	using HnswT = HierarchicalNSWImpl<float, synchronization>;
	using QuantizedHnswT = HierarchicalNSWImpl<uint8_t, synchronization>;

	constexpr static reindexer::ReplaceDeleted kHNSWAllowReplaceDeleted = reindexer::ReplaceDeleted_True;
	constexpr static int kHnswRandomSeed = 100;

	void serializeQuantizingParams(IWriter& writer) const;
	std::optional<hnswlib::QuantizingParams> deserializeQuantizingParams(IReader& reader);

	class [[nodiscard]] Impl {
	public:
		explicit Impl(reindexer::IsArray isArray, reindexer::VectorMetric metric, size_t dim, size_t maxElements, size_t M,
					  size_t efConstruction);
		explicit Impl(const Impl& other, size_t newCapacity);

		Impl& operator=(Impl&& other) & noexcept {	// TODO remove this #2478
			ptr_ = std::move(other.ptr_);
			quantizedPtr_ = std::move(other.quantizedPtr_);
			isArray_ = std::move(other.isArray_);
			return *this;
		}

		// This method must only be used in index-modifying operations (like upsert/delete/resize) under the namespace writeLock,
		// in no case under namespace readLock.
		auto& GetWritable() { return get(); }

		const auto& operator->() const noexcept { return get(); }
		const auto& operator*() const noexcept { return *get(); }

		template <typename T>
		void Load(IReader&, std::optional<QuantizingParams>);

		void Quantize(const hnswlib::QuantizationConfig& config);

		bool QuantizationAvailable() const { return !ptr_->IsQuantized() && !quantizedPtr_; }

		void Reset() noexcept;

	private:
		using PtrT = std::unique_ptr<HierarchicalNSWInterface<synchronization>>;

		auto& get() {
			if (quantizedPtr_) {
				ptr_ = std::move(*quantizedPtr_);
				quantizedPtr_ = std::nullopt;
			}
			return ptr_;
		}

		const auto& get() const noexcept { return ptr_; }

		PtrT ptr_;
		std::optional<PtrT> quantizedPtr_;

		reindexer::IsArray isArray_;
	} impl_;
};

}  // namespace hnswlib
