#pragma once

#include <unordered_map>
#include "aggregationresult.h"
#include "core/item.h"
#include "core/payload/payloadvalue.h"
#include "estl/h_vector.h"
#include "tools/serializer.h"

namespace reindexer {

using std::string;
using std::unordered_map;
using std::unique_ptr;

static const int kDefaultQueryResultsSize = 32;
struct ItemRef {
	ItemRef() : id(0) {}
	ItemRef(IdType iid, const PayloadValue &ivalue, uint16_t iproc = 0, uint16_t insid = 0, bool iraw = false)
		: id(iid), proc(iproc), raw(iraw), nsid(insid), value(ivalue) {}

	IdType id;
	uint16_t proc : 15;
	uint16_t raw : 1;
	uint16_t nsid;
	PayloadValue value;
};

using ItemRefVector = h_vector<ItemRef, kDefaultQueryResultsSize>;

class TagsMatcher;
class PayloadType;
class WrSerializer;
class QRVector;

/// QueryResults is the interface for iterating documents, returned by Query from Reindexer.<br>
/// *Lifetime*: QueryResults is uses Copy-On-Write semantics, so it have independent lifetime and state - e.g., aquired from Reindexer
/// QueryResults will not be changed externaly, even in case, when origin data in database was changed, or deleted.<br>
/// *Thread safety*: QueryResults is thread safe.

class QueryResults {
public:
	QueryResults(std::initializer_list<ItemRef> l);

	QueryResults(int flags = 0);
	QueryResults(const QueryResults &) = delete;
	QueryResults(QueryResults &&);
	~QueryResults();
	QueryResults &operator=(const QueryResults &) = delete;
	QueryResults &operator=(QueryResults &&obj) noexcept;
	void Add(const ItemRef &i);
	void AddItem(Item &item, bool withData = false);
	void Dump() const;
	void Erase(ItemRefVector::iterator begin, ItemRefVector::iterator end);
	size_t Count() const { return items_.size(); }
	size_t TotalCount() const { return totalCount; }
	const string &GetExplainResults() const { return explainResults; }
	const vector<AggregationResult> &GetAggregationResults() const { return aggregationResults; }
	void Clear();
	h_vector<string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const { return !nonCacheableData; }

	class Iterator {
	public:
		Error GetJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer &wrser, bool withHdrLen = true);
		Item GetItem();
		const QRVector &GetJoined();
		const ItemRef &GetItemRef() const { return qr_->items_[idx_]; }
		int64_t GetLSN() const { return qr_->items_[idx_].value.GetLSN(); }
		bool IsRaw() const;
		string_view GetRaw() const;
		Iterator &operator++();
		Iterator &operator+(int delta);
		Error Status() { return err_; }
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

	// Only movable map. MSVC bug workaround
	template <typename A, typename B>
	class nc_map : public unordered_map<A, B> {
	public:
		nc_map(){};
		nc_map(nc_map &&other) noexcept {
			for (auto &it : other) unordered_map<A, B>::emplace(std::move(it.first), std::move(it.second));
			other.clear();
		};
		nc_map(const nc_map &) = delete;
	};

	// joinded fields 0 - 1st joined ns, 1 - second joined
	vector<nc_map<IdType, QRVector>> joined_;  // joinded items
	vector<AggregationResult> aggregationResults;
	int totalCount = 0;
	bool haveProcent = false;
	bool nonCacheableData = false;

	struct Context;
	// precalc context size
	static constexpr int kSizeofContext = 128;  // sizeof(void *) * 2 + sizeof(void *) * 3 + 32 + sizeof(void *);
	using ContextsVector = h_vector<Context, 1, kSizeofContext>;
	ContextsVector ctxs;

	void addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const FieldsSet &fieldsFilter);
	const TagsMatcher &getTagsMatcher(int nsid) const;
	const PayloadType &getPayloadType(int nsid) const;
	TagsMatcher &getTagsMatcher(int nsid);
	PayloadType &getPayloadType(int nsid);
	int getMergedNSCount() const;
	void lockResults();
	ItemRefVector &Items() { return items_; }
	const ItemRefVector &Items() const { return items_; }

	string explainResults;

protected:
	class EncoderDatasourceWithJoins;

private:
	void unlockResults();
	void encodeJSON(int idx, WrSerializer &ser) const;
	bool lockedResults_ = false;
	ItemRefVector items_;
};

class QRVector : public h_vector<QueryResults, 2> {
public:
	using h_vector<QueryResults, 2>::h_vector;
};

}  // namespace reindexer
