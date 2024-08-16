#pragma once

#include "aggregationresult.h"
#include "core/item.h"
#include "core/namespace/stringsholder.h"
#include "core/payload/payloadvalue.h"
#include "core/rdxcontext.h"
#include "itemref.h"
#include "tools/serializer.h"

namespace reindexer {

const std::string_view kWALParamLsn = "lsn";
const std::string_view kWALParamItem = "item";

class Schema;
class TagsMatcher;
class PayloadType;
class WrSerializer;
struct NsContext;
struct ResultFetchOpts;
class SelectFunctionsHolder;
class NamespaceImpl;
struct CsvOrdering;

namespace joins {
class NamespaceResults;
class ItemIterator;
}  // namespace joins

/// QueryResults is an interface for iterating over documents, returned by Query from Reindexer.<br>
/// *Lifetime*: QueryResults uses Copy-On-Write semantics, so it has independent lifetime and state - e.g., acquired from Reindexer.
/// QueryResults cannot be externaly changed or deleted even in case of changing origin data in DB.<br>
/// *Thread safety*: QueryResults is thread safe.

class QueryResults {
public:
	using NamespaceImplPtr = intrusive_ptr<NamespaceImpl>;

	QueryResults(int flags = 0);
	QueryResults(const ItemRefVector::const_iterator& b, const ItemRefVector::const_iterator& e);
	QueryResults(std::initializer_list<ItemRef> l);
	QueryResults(const QueryResults&) = delete;
	QueryResults(QueryResults&&) noexcept;
	~QueryResults();
	QueryResults& operator=(const QueryResults&) = delete;
	QueryResults& operator=(QueryResults&& obj) noexcept;
	void Add(const ItemRef&);
	// use enableHold = false only if you are sure that the queryResults will be destroyed before the item
	// or if data from the item are contained in namespace added to the queryResults
	// enableHold is ignored when withData = false
	void AddItem(Item& item, bool withData = false, bool enableHold = true);
	std::string Dump() const;
	void Erase(ItemRefVector::iterator begin, ItemRefVector::iterator end);
	size_t Count() const noexcept { return items_.size(); }
	size_t TotalCount() const noexcept { return totalCount; }
	const std::string& GetExplainResults() const& noexcept { return explainResults; }
	const std::string& GetExplainResults() const&& = delete;
	std::string&& MoveExplainResults() & noexcept { return std::move(explainResults); }
	const std::vector<AggregationResult>& GetAggregationResults() const& noexcept { return aggregationResults; }
	const std::vector<AggregationResult>& GetAggregationResults() const&& = delete;
	void Clear();
	h_vector<std::string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return !nonCacheableData; }

	CsvOrdering MakeCSVTagOrdering(unsigned limit, unsigned offset) const;

	class Iterator {
	public:
		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true);
		Error GetProtobuf(WrSerializer& wrser, bool withHdrLen = true);
		Error GetCSV(WrSerializer& wrser, CsvOrdering& ordering) noexcept;

		// use enableHold = false only if you are sure that the item will be destroyed before the queryResults
		Item GetItem(bool enableHold = true);
		joins::ItemIterator GetJoined();
		const ItemRef& GetItemRef() const noexcept { return qr_->items_[idx_]; }
		int64_t GetLSN() const noexcept { return qr_->items_[idx_].Value().GetLSN(); }
		bool IsRaw() const noexcept { return qr_->items_[idx_].Raw(); }
		std::string_view GetRaw() const noexcept {
			auto& itemRef = qr_->items_[idx_];
			assertrx(itemRef.Raw());
			return std::string_view(reinterpret_cast<char*>(itemRef.Value().Ptr()), itemRef.Value().GetCapacity());
		}
		Iterator& operator++() noexcept {
			idx_++;
			return *this;
		}
		Iterator& operator+(int delta) noexcept {
			idx_ += delta;
			return *this;
		}

		Error Status() const noexcept { return err_; }
		bool operator==(const Iterator& other) const noexcept { return idx_ == other.idx_; }
		bool operator!=(const Iterator& other) const noexcept { return !operator==(other); }
		Iterator& operator*() noexcept { return *this; }

		const QueryResults* qr_;
		int idx_;
		Error err_;
		using NsNamesCache = h_vector<h_vector<std::string, 1>, 1>;
		NsNamesCache nsNamesCache;
	};

	Iterator begin() const noexcept { return Iterator{this, 0, errOK, {}}; }
	Iterator end() const noexcept { return Iterator{this, int(items_.size()), errOK, {}}; }
	Iterator operator[](int idx) const noexcept { return Iterator{this, idx, errOK, {}}; }

	std::vector<joins::NamespaceResults> joined_;
	std::vector<AggregationResult> aggregationResults;
	int totalCount = 0;
	bool haveRank = false;
	bool nonCacheableData = false;
	bool needOutputRank = false;

	struct Context;
	// precalc context size
	static constexpr int kSizeofContext = 264;	// sizeof(PayloadType) + sizeof(TagsMatcher) + sizeof(FieldsSet) + sizeof(shared_ptr);

	// Order of storing contexts for namespaces:
	// [0]      - main NS context
	// [1;N]    - contexts of all the merged namespaces
	// [N+1; M] - contexts of all the joined namespaces for all the merged namespaces:
	using ContextsVector = h_vector<Context, 1, kSizeofContext>;
	ContextsVector ctxs;

	void addNSContext(const PayloadType& type, const TagsMatcher& tagsMatcher, const FieldsSet& fieldsFilter,
					  std::shared_ptr<const Schema> schema);
	const TagsMatcher& getTagsMatcher(int nsid) const noexcept;
	const PayloadType& getPayloadType(int nsid) const noexcept;
	const FieldsSet& getFieldsFilter(int nsid) const noexcept;
	TagsMatcher& getTagsMatcher(int nsid) noexcept;
	PayloadType& getPayloadType(int nsid) noexcept;
	std::shared_ptr<const Schema> getSchema(int nsid) const noexcept;
	int getNsNumber(int nsid) const noexcept;
	int getMergedNSCount() const noexcept { return ctxs.size(); }
	ItemRefVector& Items() noexcept { return items_; }
	const ItemRefVector& Items() const { return items_; }
	int GetJoinedNsCtxIndex(int nsid) const noexcept;
	// Add owning ns pointer
	// noLock has always to be 'true' (i.e. this method can only be called unders Namespace's lock)
	void AddNamespace(NamespaceImplPtr, bool noLock);
	// Add non-owning ns pointer
	// noLock has always to be 'true' (i.e. this method can only be called unders Namespace's lock)
	void AddNamespace(NamespaceImpl*, bool noLock);
	void RemoveNamespace(const NamespaceImpl* ns);
	bool IsNamespaceAdded(const NamespaceImpl* ns) const noexcept {
		return std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder& nsData) { return nsData.ns == ns; }) !=
			   nsData_.cend();
	}
	void MarkAsWALQuery() noexcept { isWalQuery_ = true; }
	bool IsWALQuery() const noexcept { return isWalQuery_; }

	std::string explainResults;

private:
	class EncoderDatasourceWithJoins;
	class EncoderAdditionalDatasource;
	void encodeJSON(int idx, WrSerializer& ser, Iterator::NsNamesCache&) const;

public:
	ItemRefVector items_;
	std::optional<RdxActivityContext> activityCtx_;
	friend InternalRdxContext;
	friend SelectFunctionsHolder;
	class NsDataHolder {
	public:
		NsDataHolder(NamespaceImplPtr&& _ns, StringsHolderPtr&& strHldr) noexcept;
		NsDataHolder(NamespaceImpl* _ns, StringsHolderPtr&& strHldr) noexcept;
		NsDataHolder(const NsDataHolder&) = delete;
		NsDataHolder(NsDataHolder&&) noexcept = default;
		NsDataHolder& operator=(const NsDataHolder&) = delete;
		NsDataHolder& operator=(NsDataHolder&&) = default;

	private:
		NamespaceImplPtr nsPtr_;

	public:
		NamespaceImpl* ns;
		StringsHolderPtr strHolder;
	};

	bool isWalQuery_ = false;
	h_vector<NsDataHolder, 1> nsData_;
	std::vector<key_string> stringsHolder_;
};

}  // namespace reindexer
