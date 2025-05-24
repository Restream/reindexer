#pragma once

#include "core/index/index.h"

namespace reindexer {

class KnnCtx;
class KnnSearchParams;

class FloatVectorIndex : public Index {
public:
	using VecDataGetterF = std::function<IdType(const VariantArray& pk, void*)>;
	using PKGetterF = std::function<VariantArray(IdType)>;

protected:
	FloatVectorIndex(const FloatVectorIndex&);

	class WriterBase {
	protected:
		WriterBase(WrSerializer& ser, PKGetterF&& getPK, bool isCompositePK) noexcept
			: ser_{ser}, getPK_{std::move(getPK)}, isCompositePK_{isCompositePK} {}

		void writePK(IdType);

		WrSerializer& ser_;
		PKGetterF getPK_;

	private:
		const bool isCompositePK_;
	};

	class LoaderBase {
	protected:
		LoaderBase(VecDataGetterF&& getVectorData, bool isCompositePK) noexcept
			: getVectorData_{std::move(getVectorData)}, isCompositePK_{isCompositePK} {}

		IdType readPKEncodedData(void* destBuf, Serializer& ser, std::string_view name, std::string_view idxType) {
			VariantArray keys;
			if (!isCompositePK_) {
				keys.emplace_back(ser.GetVariant());
			} else {
				const auto len = ser.GetVarUInt();
				if rx_unlikely (!len) {
					throw Error(errLogic, "{}::LoadIndexCache:{}: serialized PK array is empty", idxType, name);
				}
				keys.reserve(len);
				for (size_t i = 0; i < len; ++i) {
					keys.emplace_back(ser.GetVariant());
				}
			}
			const IdType itemID = getVectorData_(keys, destBuf);
			if rx_unlikely (itemID < 0) {
				throw Error(errLogic, "{}::LoadIndexCache:{}: unable to find indexed item with requested PK", idxType, name);
			}
			return itemID;
		}

	private:
		VecDataGetterF getVectorData_;
		const bool isCompositePK_;
	};

public:
	struct StorageCacheWriteResult {
		Error err;
		bool isCacheable = false;
	};

	FloatVectorIndex(const IndexDef&, PayloadType&&, FieldsSet&&);
	void Delete(const VariantArray& keys, IdType, StringsHolder&, bool& clearCache) override final;
	using Index::Delete;
	SelectKeyResults SelectKey(const VariantArray&, CondType, SortType, const SelectContext&, const RdxContext&) override final;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType, bool& clearCache) override final;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override final;
	Variant UpsertConcurrent(const Variant& key, IdType id, bool& clearCache);
	SelectKeyResult Select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&, const RdxContext&) const;
	void Commit() override final;
	void UpdateSortedIds(const UpdateSortedContext&) noexcept override final {}
	const void* ColumnData() const noexcept override final { return nullptr; }
	bool HoldsStrings() const noexcept override final { return false; }
	void ReconfigureCache(const NamespaceCacheConfigData&) noexcept override final {}
	IndexMemStat GetMemStat(const RdxContext&) noexcept override;
	IndexPerfStat GetIndexPerfStat() override;
	FloatVector GetFloatVector(IdType) const;
	ConstFloatVectorView GetFloatVectorView(IdType) const;
	[[nodiscard]] uint64_t GetHash(IdType rowId) const { return GetFloatVectorView(rowId).Hash(); }
	[[nodiscard]] reindexer::FloatVectorDimension Dimension() const noexcept {
		return reindexer::FloatVectorDimension(Opts().FloatVector().Dimension());
	}
	RankedTypeQuery RankedType() const noexcept override final { return ToRankedTypeQuery(metric_); }
	[[nodiscard]] reindexer::FloatVectorDimension FloatVectorDimension() const noexcept override final { return Dimension(); }
	virtual StorageCacheWriteResult WriteIndexCache(WrSerializer&, PKGetterF&&, bool isCompositePK,
													const std::atomic_int32_t& cancel) noexcept = 0;
	virtual Error LoadIndexCache(std::string_view data, bool isCompositePK, VecDataGetterF&& getVecData) = 0;
	virtual void RebuildCentroids(float /*dataPart*/) {}

private:
	virtual SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&) const = 0;
	virtual Variant upsert(ConstFloatVectorView, IdType id, bool& clearCache) = 0;
	virtual Variant upsertConcurrent(ConstFloatVectorView, IdType id, bool& clearCache) = 0;

	virtual FloatVector getFloatVector(IdType) const = 0;
	virtual ConstFloatVectorView getFloatVectorView(IdType) const = 0;

	void checkVectorDims(ConstFloatVectorView);
	Variant upsertEmptyVectImpl(IdType);

	IndexMemStat memStat_;
	Index::KeyEntry emptyValues_;
	std::mutex emptyValuesInsertionMtx_;  // Mutex for multithreading insertion into emptyValues_

protected:
	VectorMetric metric_;
};

}  // namespace reindexer
