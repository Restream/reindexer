#pragma once

#include "aggregationresult.h"
#include "core/item.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "core/namespace/incarnationtags.h"
#include "core/namespace/stringsholder.h"
#include "core/rdxcontext.h"
#include "itemref.h"
#include "tools/serializer.h"

namespace reindexer {

class Schema;
class TagsMatcher;
class PayloadType;
class WrSerializer;
class NsContext;
struct ResultFetchOpts;
struct ItemImplRawData;
class NamespaceImpl;

namespace builders {
struct CsvOrdering;
}  // namespace builders
using builders::CsvOrdering;

class FieldsFilter;

namespace joins {
class NamespaceResults;
class ItemIterator;
}  // namespace joins

/// LocalQueryResults is an interface for iterating over documents, returned by Query from Reindexer.<br>
/// *Lifetime*: LocalQueryResults uses Copy-On-Write semantics, so it has independent lifetime and state - e.g., acquired from Reindexer.
/// LocalQueryResults cannot be externally changed or deleted even in case of changing origin data in DB.<br>
/// *Thread safety*: LocalQueryResults is thread safe.

class [[nodiscard]] LocalQueryResults {
	template <typename QR>
	class [[nodiscard]] IteratorImpl {
	public:
		using NsNamesCache = h_vector<h_vector<std::string, 1>, 1>;

		IteratorImpl(IteratorImpl&&) noexcept = default;
		IteratorImpl(const IteratorImpl&) = default;
		IteratorImpl& operator=(IteratorImpl&&) noexcept = default;
		IteratorImpl& operator=(const IteratorImpl&) = default;
		IteratorImpl(QR& qr, size_t idx) noexcept : qr_{&qr}, idx_{idx} {}
		IteratorImpl(QR* qr, size_t idx, Error&& err, NsNamesCache&& nsCache) noexcept
			: qr_{qr}, idx_{idx}, err_{std::move(err)}, nsNamesCache{std::move(nsCache)} {}
		IteratorImpl(QR* qr, size_t idx, const Error& err, const NsNamesCache& nsCache)
			: qr_{qr}, idx_{idx}, err_{err}, nsNamesCache{nsCache} {}
		operator IteratorImpl<std::add_const_t<QR>>() const& { return {qr_, idx_, err_, nsNamesCache}; }
		operator IteratorImpl<std::add_const_t<QR>>() && noexcept { return {qr_, idx_, std::move(err_), std::move(nsNamesCache)}; }
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true) noexcept;
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true) noexcept;
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true) noexcept;
		Error GetProtobuf(WrSerializer& wrser) noexcept;
		Error GetCSV(WrSerializer& wrser, CsvOrdering& ordering) noexcept;

		// use enableHold = false only if you are sure that the item will be destroyed before the LocalQueryResults
		Item GetItem(bool enableHold = true);
		joins::ItemIterator GetJoined();
		auto& GetItemRef() const& noexcept { return qr_->items_.GetItemRef(idx_); }
		auto GetItemRef() const&& noexcept = delete;
		auto& GetItemRefRanked() const& { return qr_->items_.GetItemRefRanked(idx_); }
		auto GetItemRefRanked() const&& = delete;
		ItemRefVariant GetItemRefVariant() const { return qr_->items_.GetItemRefVariant(idx_); }
		reindexer::IsRanked IsRanked() const noexcept { return qr_->items_.IsRanked(); }
		lsn_t GetLSN() const noexcept { return GetItemRef().Value().GetLSN(); }
		bool IsRaw() const noexcept { return GetItemRef().Raw(); }
		std::string_view GetRaw() const noexcept {
			const auto& itemRef = GetItemRef();
			assertrx(itemRef.Raw());
			return std::string_view(reinterpret_cast<char*>(itemRef.Value().Ptr()), itemRef.Value().GetCapacity());
		}
		IteratorImpl& operator++() noexcept {
			idx_++;
			return *this;
		}
		IteratorImpl& operator+(int delta) noexcept {
			idx_ += delta;
			return *this;
		}
		const QR* Owner() const& noexcept { return qr_; }
		auto Owner() && = delete;
		size_t Idx() const noexcept { return idx_; }

		Error Status() const noexcept { return err_; }
		bool operator==(const IteratorImpl<const QR>& other) const noexcept { return idx_ == other.Idx(); }
		bool operator==(const IteratorImpl<std::remove_const_t<QR>>& other) const noexcept { return idx_ == other.Idx(); }
		bool operator!=(const IteratorImpl<const QR>& other) const noexcept { return !operator==(other); }
		bool operator!=(const IteratorImpl<std::remove_const_t<QR>>& other) const noexcept { return !operator==(other); }
		IteratorImpl& operator*() noexcept { return *this; }
		static IteratorImpl SwitchQueryResultsPtrUnsafe(IteratorImpl&& it, QR& qr) {
			it.qr_ = &qr;
			return std::move(it);
		}

		operator IteratorImpl<std::add_const_t<QR>>() const&& = delete;

		const FieldsFilter& GetFieldsFilter() const noexcept { return qr_->getFieldsFilter(qr_->items_.GetItemRef(idx_).Nsid()); }

	private:
		QR* qr_{nullptr};
		size_t idx_{0};
		Error err_;
		NsNamesCache nsNamesCache;
	};

public:
	using NamespaceImplPtr = intrusive_ptr<NamespaceImpl>;
	using ConstIterator = IteratorImpl<const LocalQueryResults>;
	using Iterator = IteratorImpl<LocalQueryResults>;

	LocalQueryResults();
	LocalQueryResults(const ItemRefVector::ConstIterator& b, const ItemRefVector::ConstIterator& e);
	LocalQueryResults(const LocalQueryResults&) = delete;
	LocalQueryResults(LocalQueryResults&&) noexcept;
	~LocalQueryResults();
	LocalQueryResults& operator=(const LocalQueryResults&) = delete;
	LocalQueryResults& operator=(LocalQueryResults&& obj) noexcept;
	template <typename... Args>
	void AddItemRef(Args&&... args) {
		items_.EmplaceBack(std::forward<Args>(args)...);
	}
	template <typename... Args>
	void AddItemRef(RankT r, Args&&... args) {
		items_.EmplaceBack(r, std::forward<Args>(args)...);
	}

	// Make sure that the LocalQueryResults will be destroyed before the item
	// or if data from the item are contained in namespace added to the LocalQueryResults
	void AddItemNoHold(Item& item, lsn_t nsIncarnationTag, bool withData = false);
	std::string Dump() const;
	void Erase(const ItemRefVector::Iterator& begin, const ItemRefVector::Iterator& end) { items_.Erase(begin, end); }
	size_t Count() const noexcept { return items_.Size(); }
	size_t TotalCount() const noexcept { return totalCount; }
	const std::string& GetExplainResults() const& noexcept { return explainResults; }
	auto GetExplainResults() const&& = delete;
	std::string&& MoveExplainResults() & noexcept { return std::move(explainResults); }
	const std::vector<AggregationResult>& GetAggregationResults() const& noexcept { return aggregationResults; }
	const std::vector<AggregationResult>& GetAggregationResults() const&& = delete;
	void Clear();
	h_vector<std::string_view, 1> GetNamespaces() const;
	NsShardsIncarnationTags GetIncarnationTags() const;
	bool IsCacheEnabled() const noexcept { return !nonCacheableData; }
	void SetOutputShardId(int shardId) noexcept { outputShardId = shardId; }
	CsvOrdering MakeCSVTagOrdering(unsigned limit, unsigned offset) const;

	ConstIterator cbegin() const noexcept { return ConstIterator{*this, 0}; }
	ConstIterator begin() const noexcept { return cbegin(); }
	ConstIterator cend() const noexcept { return ConstIterator{*this, items_.Size()}; }
	ConstIterator end() const noexcept { return cend(); }
	ConstIterator operator[](size_t idx) const noexcept { return ConstIterator{*this, idx}; }
	Iterator begin() noexcept { return Iterator{*this, 0}; }
	Iterator end() noexcept { return Iterator{*this, items_.Size()}; }

	std::vector<joins::NamespaceResults> joined_;
	std::vector<AggregationResult> aggregationResults;
	int totalCount = 0;
	bool haveRank = false;
	bool nonCacheableData = false;
	bool needOutputRank = false;
	int outputShardId = ShardingKeyType::ProxyOff;	// flag+value

	struct Context;
	// precalc context size
	// sizeof(PayloadType) + sizeof(TagsMatcher) + sizeof(FieldsFilter) + sizeof(shared_ptr) + sizeof(int64);
	static constexpr int kSizeofContext = 408;

	// Order of storing contexts for namespaces:
	// [0]      - main NS context
	// [1;N]    - contexts of all the merged namespaces
	// [N+1; M] - contexts of all the joined namespaces for all the merged namespaces:
	using ContextsVector = h_vector<Context, 1, kSizeofContext>;
	ContextsVector ctxs;

	void addNSContext(const PayloadType& type, const TagsMatcher& tagsMatcher, const FieldsFilter& fieldsFilter,
					  std::shared_ptr<const Schema> schema, lsn_t nsIncarnationTag);
	void addNSContext(const QueryResults& baseQr, size_t nsid, lsn_t nsIncarnationTag);
	const TagsMatcher& getTagsMatcher(int nsid) const& noexcept;
	auto getTagsMatcher(int nsid) const&& noexcept = delete;
	const PayloadType& getPayloadType(int nsid) const& noexcept;
	auto getPayloadType(int nsid) const&& noexcept = delete;
	const FieldsFilter& getFieldsFilter(int nsid) const& noexcept;
	auto getFieldsFilter(int nsid) const&& noexcept = delete;

	TagsMatcher& getTagsMatcher(int nsid) & noexcept;
	auto getTagsMatcher(int nsid) && noexcept = delete;
	PayloadType& getPayloadType(int nsid) & noexcept;
	auto getPayloadType(int nsid) && noexcept = delete;
	std::shared_ptr<const Schema> getSchema(int nsid) const noexcept;
	int getNsNumber(int nsid) const noexcept;
	int getMergedNSCount() const noexcept;
	ItemRefVector& Items() & noexcept { return items_; }
	const ItemRefVector& Items() const& noexcept { return items_; }
	auto Items() const&& noexcept = delete;
	auto Items() && = delete;
	int GetJoinedNsCtxIndex(int nsid) const noexcept;

	void SaveRawData(ItemImplRawData&&);

	// Add owning ns pointer
	// noLock has always to be 'true' (i.e. this method can only be called under Namespace's lock)
	void AddNamespace(NamespaceImplPtr, bool noLock);
	// Add non-owning ns pointer
	// noLock has always to be 'true' (i.e. this method can only be called under Namespace's lock)
	void AddNamespace(NamespaceImpl*, bool noLock);
	void RemoveNamespace(const NamespaceImpl* ns);
	bool IsNamespaceAdded(const NamespaceImpl* ns) const noexcept {
		return std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder& nsData) { return nsData.ns == ns; }) !=
			   nsData_.cend();
	}
	FloatVectorsHolderMap& GetFloatVectorsHolder() & noexcept { return floatVectorsHolder_; }

	std::string explainResults;

private:
	class EncoderDatasourceWithJoins;

	void encodeJSON(int idx, WrSerializer& ser, ConstIterator::NsNamesCache&) const;
	ItemRefVector items_;
	std::vector<ItemImplRawData> rawDataHolder_;
	friend class FtFunctionsHolder;

	class [[nodiscard]] NsDataHolder {
	public:
		NsDataHolder(NamespaceImplPtr&& _ns, StringsHolderPtr&& strHldr) noexcept;
		NsDataHolder(NamespaceImpl* _ns, StringsHolderPtr&& strHldr) noexcept;
		NsDataHolder(const NsDataHolder&) = delete;
		NsDataHolder(NsDataHolder&&) noexcept = default;
		NsDataHolder& operator=(const NsDataHolder&) = delete;
		NsDataHolder& operator=(NsDataHolder&&) = default;

		const StringsHolderPtr::element_type* StrHolderPtr() const& noexcept { return strHolder_.get(); }

		const NamespaceImpl* ns{nullptr};

	private:
		NamespaceImplPtr nsPtr_;
		StringsHolderPtr strHolder_;
	};

	h_vector<NsDataHolder, 1> nsData_;
	std::vector<key_string> stringsHolder_;
	FloatVectorsHolderMap floatVectorsHolder_;
};

}  // namespace reindexer
