#pragma once

#include <cstdint>
#include <limits>
#include <vector>
#include "core/definitions/indexdef.h"
#include "core/definitions/indexopts.h"
#include "core/idset/idset.h"
#include "core/index/keyentry.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespacestat.h"
#include "core/namespace/ns_ft_func_interface.h"
#include "core/nsselecter/ranks_holder.h"
#include "core/payload/payloadiface.h"
#include "core/perfstatcounter.h"
#include "core/selectkeyresult.h"
#include "ft_preselect.h"
#include "indexiterator.h"

namespace reindexer {

class RdxContext;
class StringsHolder;
struct NamespaceCacheConfigData;
class FtFunction;
enum class [[nodiscard]] IndexOptimization : int8_t { Partial, Full };

// Logical — copy-tx: snapshot idx_map, skip sortOrders_/pkSortedIds_/isBuilt_ (rebuilt on copy)
// Snapshot — preserve sorted optimizer state. Has to be used under exclusive lock, when background optimization is idle
enum class [[nodiscard]] IndexCloneKind { Logical, Snapshot };

class [[nodiscard]] Index {
	struct [[nodiscard]] SelectFuncCtx {
		SelectFuncCtx(FtFunction& func, RanksHolder::Ptr& r, int idxNo, NsFtFuncInterface n) noexcept
			: selectFunc{func}, ranks{r}, indexNo{idxNo}, nm{n} {}
		FtFunction& selectFunc;
		RanksHolder::Ptr& ranks;
		int indexNo;
		// Index does not own the namespace, so the interface is only valid within the select call
		NsFtFuncInterface nm;
	};

public:
	struct [[nodiscard]] SelectOpts {
		SelectOpts() noexcept
			: itemsCountInNamespace(0),
			  maxIterations(std::numeric_limits<int>::max()),
			  distinct(0),
			  disableIdSetCache(0),
			  forceComparator(0),
			  unbuiltSortOrders(0),
			  indexesNotOptimized(0),
			  inTransaction{0},
			  rankSortType(0),
			  strictMode(StrictModeNone) {}
		unsigned itemsCountInNamespace;
		int maxIterations;
		unsigned distinct : 1;
		unsigned disableIdSetCache : 1;
		unsigned forceComparator : 1;
		unsigned unbuiltSortOrders : 1;
		unsigned indexesNotOptimized : 1;
		unsigned inTransaction : 1;
		unsigned rankSortType : 3;
		StrictMode strictMode;
	};
	struct [[nodiscard]] SelectContext {
		SelectOpts opts;
		std::optional<SelectFuncCtx> selectFuncCtx;
	};
	struct [[nodiscard]] OrderedConditionEstimate {
		size_t keys;
		size_t ids;
		size_t indexSize;
		bool complete;
	};
	using KeyEntry = reindexer::KeyEntry<IdSet>;
	using KeyEntryPlain = reindexer::KeyEntry<IdSetPlain>;
	using KeyEntryPK = reindexer::KeyEntry<IdSetUnique>;

	Index(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields);
	Index& operator=(const Index&) = delete;
	virtual ~Index() = default;
	virtual Variant Upsert(const Variant& key, IdType id, bool& clearCache) = 0;
	virtual void Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) = 0;
	virtual void Delete(const Variant& key, IdType id, reindexer::MustExist mustExist, StringsHolder&, bool& clearCache) = 0;
	virtual void Delete(const VariantArray& keys, IdType id, reindexer::MustExist mustExist, StringsHolder&, bool& clearCache) = 0;
	virtual bool RefreshCompositeKey(const Variant& key, IdType id) noexcept = 0;
	virtual void HashTablesStats(std::vector<char>& stats) const { stats.resize(0); }
	virtual void ReserveHashTables(const std::vector<char>& /*stats*/) {}

	virtual SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, const SelectContext&,
									   const RdxContext&) = 0;
	// NOLINTBEGIN(*-unnecessary-value-param)
	virtual SelectKeyResults SelectKey(const VariantArray&, CondType, const SelectContext&, FtPreselectT&&, const RdxContext&) {
		assertrx(0);
		std::abort();
	}
	// NOLINTEND(*-unnecessary-value-param)
	virtual WasCanceled Commit(const index::ICancelable&) = 0;
	/// Whether background Clean() has useful work. Used to avoid spawning clean workers when none are needed.
	virtual bool NeedsClean() const noexcept { return false; }
	virtual void Clean(const index::ICancelable&, bool /*enablePerfCounters*/) {}
	virtual void CommitFulltext() {}
	virtual WasCanceled MakeSortOrders(index::IUpdateSortedContext&, const index::ICancelable&) { return WasCanceled_False; }

	virtual WasCanceled UpdateSortedIds(const index::IUpdateSortedContext& ctx, const index::ICancelable&) = 0;
	virtual bool IsSupportSortedIdsBuild() const noexcept = 0;

	virtual size_t Size() const noexcept { return 0; }
	virtual std::unique_ptr<Index> Clone(size_t newCapacity, IndexCloneKind kind) const = 0;
	virtual bool IsOrdered() const noexcept { return false; }
	virtual bool IsFulltext() const noexcept { return false; }
	virtual bool IsUuid() const noexcept { return false; }
	virtual reindexer::FloatVectorDimension FloatVectorDimension() const noexcept {
		assertrx(0);
		std::abort();
	}
	virtual QueryRankType RankedType() const noexcept {
		assertrx(0);
		std::abort();
	}
	virtual IndexMemStat GetMemStat(const RdxContext&) const = 0;
	virtual int64_t GetTTLValue() const noexcept { return 0; }
	virtual IndexIterator::Ptr CreateIterator() const { return nullptr; }

	virtual bool IsDestroyPartSupported() const noexcept { return false; }
	virtual void AddDestroyTask(tsl::detail_sparse_hash::ThreadTaskQueue&) {}
	virtual const void* ColumnData() const noexcept = 0;

	const PayloadType& GetPayloadType() const& { return payloadType_; }
	const PayloadType& GetPayloadType() const&& = delete;
	void UpdatePayloadType(PayloadType&& payloadType) { payloadType_ = std::move(payloadType); }
	/// Estimate how many btree keys and row IDs an ordered condition would touch.
	/// Probing is capped at `cap` keys. `indexSize` is used as a fallback for incomplete estimates.
	virtual OrderedConditionEstimate EstimateOrderedCondition(CondType /*cond*/, const VariantArray& /*keys*/, size_t cap) const {
		return OrderedConditionEstimate{.keys = cap, .ids = cap, .indexSize = Size(), .complete = false};
	}

	static std::unique_ptr<Index> New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields_,
									  const NamespaceCacheConfigData& cacheCfg, size_t currentNsSize, LogCreation = LogCreation_False);

	KeyValueType KeyType() const noexcept { return keyType_; }
	KeyValueType SelectKeyType() const noexcept { return selectKeyType_; }
	const FieldsSet& Fields() const& noexcept { return fields_; }
	const FieldsSet& Fields() const&& = delete;
	const std::string& Name() const& noexcept { return name_; }
	const std::string& Name() const&& = delete;
	IndexType Type() const noexcept { return type_; }
	const std::vector<IdType>& SortOrders() const { return sortOrders_; }
	const IndexOpts& Opts() const { return opts_; }
	virtual void SetOpts(const IndexOpts& opts) { opts_ = opts; }
	void SetFields(FieldsSet&& fields) { fields_ = std::move(fields); }
	SortType SortId() const noexcept { return sortId_; }
	virtual void SetSortedIdxCount(unsigned sortedIdxCount) { sortedIdxCount_ = sortedIdxCount; }
	virtual reindexer::FtPreselectT FtPreselect(const RdxContext&) {
		assertrx(0);
		std::abort();
	}
	virtual bool EnablePreselectBeforeFt() const { return false; }

	PerfStatCounterMT& GetSelectPerfCounter() { return selectPerfCounter_; }
	PerfStatCounterMT& GetCommitPerfCounter() { return commitPerfCounter_; }
	PerfStatCounterMT& GetCleanPerfCounter() { return cleanPerfCounter_; }

	virtual IndexPerfStat GetIndexPerfStat();
	virtual void ResetIndexPerfStat();

	virtual bool HoldsStrings() const noexcept = 0;
	virtual void DestroyCache() {}
	virtual void ClearCache() {}
	virtual bool IsBuilt() const noexcept { return isBuilt_; }
	virtual void MarkBuilt() noexcept { isBuilt_ = true; }
	virtual void EnableUpdatesCountingMode(bool) {}
	virtual void ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) = 0;
	virtual bool IsSupportMultithreadTransactions() const noexcept { return false; }
	virtual void GrowFor(size_t /*newElementsCount*/) {}
	bool IsFloatVector() const noexcept;

	virtual void Dump(std::ostream& os, std::string_view step = "  ", std::string_view offset = "") const { dump(os, step, offset); }

protected:
	Index(const Index& other, IndexCloneKind kind);

	// Index type. Can be one of enum IndexType
	IndexType type_;
	// Name of index (usually name of field).
	std::string name_;
	// Vector or ids, sorted by this index. Available only for ordered indexes
	std::vector<IdType> sortOrders_;

	SortType sortId_ = 0;
	// Index options
	IndexOpts opts_;
	// Payload type of items
	mutable PayloadType payloadType_;

	// Fields in index
	FieldsSet fields_;

	// Perfstat counter
	PerfStatCounterMT commitPerfCounter_;
	PerfStatCounterMT selectPerfCounter_;
	PerfStatCounterMT cleanPerfCounter_;
	KeyValueType keyType_ = KeyValueType::Undefined{};
	KeyValueType selectKeyType_ = KeyValueType::Undefined{};
	// Count of sorted indexes in namespace to reserve additional space in idsets
	unsigned sortedIdxCount_ = 0;
	bool isBuilt_{false};

private:
	template <typename S>
	void dump(S& os, std::string_view step, std::string_view offset) const;
};

constexpr unsigned kMaxSelectivityPercentForIdset = 30u;

// Max btree keys packed as explicit idsets for non-distinct IndexOrdered::SelectKey
constexpr size_t kMaxExplicitBtreeKeyCount = 50;
// Max btree keys packed as explicit idsets for Distinct SelectKey
constexpr size_t kMaxExplicitBtreeKeyCountDistinct = 500;
// Cheap probe cap for EstimateOrderedCondition ranking (may differ from SelectKey pack caps)
constexpr size_t kAdviceOrderedConditionProbeKeyCap = 50;

}  // namespace reindexer
