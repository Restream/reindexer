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
	QueryResults(int flags = 0);
	QueryResults(const ItemRefVector::const_iterator &b, const ItemRefVector::const_iterator &e);
	QueryResults(std::initializer_list<ItemRef> l);
	QueryResults(const QueryResults &) = delete;
	QueryResults(QueryResults &&) noexcept;
	~QueryResults();
	QueryResults &operator=(const QueryResults &) = delete;
	QueryResults &operator=(QueryResults &&obj) noexcept;
	void Add(const ItemRef &);
	// use enableHold = false only if you are sure that the queryResults will be destroyed before the item
	// or if data from the item are contained in namespace added to the queryResults
	// enableHold is ignored when withData = false
	void AddItem(Item &item, bool withData = false, bool enableHold = true);
	std::string Dump() const;
	void Erase(ItemRefVector::iterator begin, ItemRefVector::iterator end);
	size_t Count() const { return items_.size(); }
	size_t TotalCount() const { return totalCount; }
	const std::string &GetExplainResults() const { return explainResults; }
	const std::vector<AggregationResult> &GetAggregationResults() const { return aggregationResults; }
	void Clear();
	h_vector<std::string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return !nonCacheableData; }

	CsvOrdering MakeCSVTagOrdering(unsigned limit, unsigned offset) const;

	class Iterator {
	public:
		Error GetJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer &wrser, bool withHdrLen = true);
		Error GetProtobuf(WrSerializer &wrser, bool withHdrLen = true);
		[[nodiscard]] Error GetCSV(WrSerializer &wrser, CsvOrdering &ordering) noexcept;

		// use enableHold = false only if you are sure that the item will be destroyed before the queryResults
		Item GetItem(bool enableHold = true);
		joins::ItemIterator GetJoined();
		const ItemRef &GetItemRef() const { return qr_->items_[idx_]; }
		int64_t GetLSN() const { return qr_->items_[idx_].Value().GetLSN(); }
		bool IsRaw() const;
		std::string_view GetRaw() const;
		Iterator &operator++();
		Iterator &operator+(int delta);
		const Error &Status() const noexcept { return err_; }
		bool operator!=(const Iterator &) const;
		bool operator==(const Iterator &) const;
		Iterator &operator*() { return *this; }

		const QueryResults *qr_;
		int idx_;
		Error err_;
	};

	Iterator begin() const { return Iterator{this, 0, errOK}; }
	Iterator end() const { return Iterator{this, int(items_.size()), errOK}; }
	Iterator operator[](int idx) const { return Iterator{this, idx, errOK}; }

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

	void addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const FieldsSet &fieldsFilter,
					  std::shared_ptr<const Schema> schema);
	const TagsMatcher &getTagsMatcher(int nsid) const;
	const PayloadType &getPayloadType(int nsid) const;
	const FieldsSet &getFieldsFilter(int nsid) const;
	TagsMatcher &getTagsMatcher(int nsid);
	PayloadType &getPayloadType(int nsid);
	std::shared_ptr<const Schema> getSchema(int nsid) const;
	int getNsNumber(int nsid) const;
	int getMergedNSCount() const;
	ItemRefVector &Items() { return items_; }
	const ItemRefVector &Items() const { return items_; }
	int GetJoinedNsCtxIndex(int nsid) const;
	void AddNamespace(std::shared_ptr<NamespaceImpl>, const NsContext &);
	void RemoveNamespace(const NamespaceImpl *ns);
	bool IsNamespaceAdded(const NamespaceImpl *ns) const noexcept {
		return std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder &nsData) { return nsData.ns.get() == ns; }) !=
			   nsData_.cend();
	}
	void MarkAsWALQuery() noexcept { isWalQuery_ = true; }
	bool IsWALQuery() const noexcept { return isWalQuery_; }

	std::string explainResults;

protected:
	class EncoderDatasourceWithJoins;
	class EncoderAdditionalDatasource;

public:
	void encodeJSON(int idx, WrSerializer &ser) const;
	ItemRefVector items_;
	std::optional<RdxActivityContext> activityCtx_;
	friend InternalRdxContext;
	friend SelectFunctionsHolder;
	struct NsDataHolder {
		NsDataHolder(std::shared_ptr<NamespaceImpl> &&ns_, StringsHolderPtr &&strHldr) noexcept
			: ns{std::move(ns_)}, strHolder{std::move(strHldr)} {}
		NsDataHolder(const NsDataHolder &) = delete;
		NsDataHolder(NsDataHolder &&) noexcept = default;
		NsDataHolder &operator=(const NsDataHolder &) = delete;
		NsDataHolder &operator=(NsDataHolder &&) = default;

		std::shared_ptr<NamespaceImpl> ns;
		StringsHolderPtr strHolder;
	};

	bool isWalQuery_ = false;
	h_vector<NsDataHolder, 1> nsData_;
	std::vector<key_string> stringsHolder_;
};

}  // namespace reindexer
