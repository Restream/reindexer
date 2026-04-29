#pragma once

#include <unordered_set>
#include "core/enums.h"
#include "core/index/index.h"
#include "core/namespace/float_vector_data_access.h"
#include "estl/mutex.h"
#include "faiss/MetricType.h"
#include "float_vector_id.h"
#include "sparse-map/sparse_map.h"

namespace reindexer {

class FloatVectorsKeeper;
class KnnCtx;
class KnnSearchParams;
class KnnRawResult;

class [[nodiscard]] FloatVectorIndex : public Index {
public:
	using PKGetterF = std::function<VariantArray(IdType)>;

protected:
	FloatVectorIndex(const FloatVectorIndex&);

	class [[nodiscard]] WriterBase {
	protected:
		WriterBase(WrSerializer& ser, PKGetterF&& getPK, bool isCompositePK, IsArray isArray) noexcept
			: ser_{ser}, getPK_{std::move(getPK)}, isCompositePK_{isCompositePK}, isArray_{isArray} {}

		void writePK(FloatVectorId);

		WrSerializer& ser_;
		PKGetterF getPK_;

	private:
		const bool isCompositePK_;
		const IsArray isArray_;
	};

	class [[nodiscard]] LoaderBase {
	protected:
		LoaderBase(FloatVectorIndexRawDataInserter&& getVectorData, bool isCompositePK, IsArray isArray) noexcept
			: getVectorData_{std::move(getVectorData)}, isCompositePK_{isCompositePK}, isArray_{isArray} {}

		FloatVectorId readPKEncodedData(void* destBuf, Serializer& ser, std::string_view name, std::string_view idxType);

	private:
		FloatVectorIndexRawDataInserter getVectorData_;
		const bool isCompositePK_;
		const IsArray isArray_;
	};

public:
	struct [[nodiscard]] StorageCacheWriteResult {
		Error err;
		bool isCacheable = false;
	};

	FloatVectorIndex(const IndexDef&, PayloadType&&, FieldsSet&&);
	void Delete(const VariantArray& keys, IdType, MustExist mustExist, StringsHolder&, bool& clearCache) override final;
	void Delete(const Variant&, IdType, reindexer::MustExist, StringsHolder&, bool&) override final;
	SelectKeyResults SelectKey(const VariantArray&, CondType, SortType, const SelectContext&, const RdxContext&) override final;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType, bool& clearCache) override final;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override final;
	Variant Upsert(ConstFloatVectorView, FloatVectorId, bool& clearCache);
	Variant UpsertConcurrent(const Variant& key, FloatVectorId id, bool& clearCache);
	bool RefreshCompositeKey(const Variant& key) noexcept override final;
	SelectKeyResult Select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&, const RdxContext&) const;
	KnnRawResult SelectRaw(ConstFloatVectorView, const KnnSearchParams&, const RdxContext&) const;
	void Commit() override final;

	void UpdateSortedIds(const IUpdateSortedContext&) override final { assertrx_dbg(!IsSupportSortedIdsBuild()); }
	bool IsSupportSortedIdsBuild() const noexcept override final { return false; }

	const void* ColumnData() const noexcept override final { return nullptr; }
	bool HoldsStrings() const noexcept override final { return false; }
	void ReconfigureCache(const NamespaceCacheConfigData&) noexcept override final {}
	IndexMemStat GetMemStat(const RdxContext&) const noexcept override;
	IndexPerfStat GetIndexPerfStat() override;
	virtual uint64_t GetHash(FloatVectorId) const = 0;
	reindexer::FloatVectorDimension Dimension() const noexcept { return reindexer::FloatVectorDimension(Opts().FloatVector().Dimension()); }
	QueryRankType RankedType() const noexcept override final { return ToQueryRankType(metric_); }
	reindexer::FloatVectorDimension FloatVectorDimension() const noexcept override final { return Dimension(); }
	FloatVectorsKeeper& GetKeeper() const noexcept { return *keeper_; }
	virtual StorageCacheWriteResult WriteIndexCache(WrSerializer&, PKGetterF&&, bool isCompositePK,
													const std::atomic_int32_t& cancel) noexcept = 0;
	virtual Error LoadIndexCache(std::string_view data, bool isCompositePK, FloatVectorIndexRawDataInserter&& getVecData, LoadWithQuantizer,
								 uint8_t version) = 0;
	virtual void RebuildCentroids(float) {}
	void ResetIndexPerfStat() override;
	void EnablePerfStat(bool);
	void SetOpts(const IndexOpts& opts) override;

	virtual bool IsQuantized() const = 0;
	virtual bool QuantizationAvailable() const = 0;
	virtual void Quantize(size_t itemsCount) = 0;
	virtual void SwitchMapOnQuantized() = 0;

	void CopyEmptyValues(const FloatVectorIndex& other) {
		emptyValues_ = other.emptyValues_;
		emptyVectors_ = other.emptyVectors_;
		emptyKeys_ = other.emptyKeys_;
		emptyVectorsCounters_ = other.emptyVectorsCounters_;
	}

protected:
	template <typename Ids, typename Ranks>
	static void removeDuplicateRowId(Ids& ids, Ranks& ranks, size_t count) {
		struct [[nodiscard]] {
			IdType operator()(IdType v) const noexcept { return v; }
			IdType operator()(faiss::idx_t v) const noexcept { return FloatVectorId::FromNumber(v).RowId(); }
		} static constexpr toRowId{};
		assertrx_dbg(ids.size() >= count);
		assertrx_dbg(ranks.size() >= count);
		fast_hash_set<IdType> addedIds;
		addedIds.reserve(count);
		size_t to = 0;
		for (size_t from = 0; from < count; ++from) {
			if (addedIds.insert(toRowId(ids[from])).second) {
				ids[to] = ids[from];
				ranks[to] = ranks[from];
				++to;
			}
		}
		ids.erase(ids.begin() + to, ids.end());
		ranks.erase(ranks.begin() + to, ranks.end());
	}

	ConstFloatVectorView getFloatVectorView(FloatVectorId) const;

private:
	friend class FloatVectorExtractor;

	virtual SelectKeyResult select(ConstFloatVectorView, const KnnSearchParams&, KnnCtx&) const = 0;
	virtual KnnRawResult selectRaw(ConstFloatVectorView, const KnnSearchParams&) const = 0;
	virtual Variant upsert(ConstFloatVectorView, FloatVectorId, bool& clearCache) = 0;
	virtual Variant upsertConcurrent(ConstFloatVectorView, FloatVectorId id, bool& clearCache) = 0;
	virtual void del(FloatVectorId, MustExist, IsLast) = 0;

	virtual ConstFloatVectorView getFloatVectorViewImpl(FloatVectorId) const = 0;

	Variant upsertEmptyVectImpl(FloatVectorId);

	void checkForSelect(ConstFloatVectorView) const;
	void checkVectorDims(ConstFloatVectorView, std::string_view operation) const;

	template <typename T>
	void checkAndExecFuncOnEmbeders(T fun);

	IndexMemStat memStat_;
	Index::KeyEntry emptyKeys_;
	tsl::sparse_set<FloatVectorId> emptyVectors_;
	tsl::sparse_map<IdType, size_t> emptyVectorsCounters_;
	tsl::sparse_set<IdType> emptyValues_;
	mutex emptyValuesInsertionMtx_;	 // Mutex for multithreading insertion into emptyValues_, emptyVectors_ and emptyKeys_
	std::shared_ptr<FloatVectorsKeeper> keeper_;

protected:
	VectorMetric metric_;
};

}  // namespace reindexer
