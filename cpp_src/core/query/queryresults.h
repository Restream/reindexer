#pragma once

#include <unordered_map>
#include "core/item.h"
#include "estl/h_vector.h"
namespace reindexer {

using std::string;
using std::unordered_map;

static const int kDefaultQueryResultsSize = 32;
struct ItemRef {
	ItemRef(IdType iid = 0, int iversion = 0) : id(iid), version(iversion) {}
	ItemRef(IdType iid, int iversion, const PayloadValue &ivalue, uint8_t iproc = 0, uint8_t insid = 0)
		: id(iid), version(iversion), proc(iproc), nsid(insid), value(ivalue) {}
	IdType id;
	int16_t version;
	uint8_t proc;
	uint8_t nsid;
	PayloadValue value;
};

using ItemRefVector = h_vector<ItemRef, kDefaultQueryResultsSize>;

class TagsMatcher;
class PayloadType;
class JsonPrintFilter;
class WrSerializer;

/// QueryResults is the interface for iterating documents, returned by Query from Reindexer.<br>
/// *Lifetime*: QueryResults is uses Copy-On-Write semantics, so it have independent lifetime and state - e.g., aquired from Reindexer
/// QueryResults will not be changed externaly, even in case, when origin data in database was changed, or deleted.<br>
/// *Thread safety*: QueryResults is thread safe.

class QueryResults : private ItemRefVector {
public:
	using ItemRefVector::iterator;
	using ItemRefVector::begin;
	using ItemRefVector::end;
	using ItemRefVector::size;
	using ItemRefVector::empty;
	using ItemRefVector::at;
	using ItemRefVector::difference_type;
	using ItemRefVector::operator[];

	QueryResults(std::initializer_list<ItemRef> l);

	QueryResults();
	QueryResults(const QueryResults &) = delete;
	QueryResults(QueryResults &&);
	~QueryResults();
	QueryResults &operator=(const QueryResults &) = delete;
	QueryResults &operator=(QueryResults &&obj) noexcept;

	void Add(const ItemRef &i);
	void AddItem(Item &item);
	void Dump() const;
	void Erase(iterator begin, iterator end);
	void GetJSON(int idx, WrSerializer &wrser, bool withHdrLen = true) const;
	void GetCJSON(int idx, WrSerializer &wrser, bool withHdrLen = true) const;

	Item GetItem(int idx) const;

	// joinded fields 0 - 1st joined ns, 1 - second joined
	unordered_map<IdType, vector<QueryResults>> joined_;  // joinded items
	h_vector<double> aggregationResults;
	int totalCount = 0;
	bool haveProcent = false;

	struct Context;
	// precalc context size
	static constexpr int kSizeofContext = 100;  // sizeof(void *) * 2 + sizeof(void *) * 3 + 32 + sizeof(void *);
	using ContextsVector = h_vector<Context, 1, kSizeofContext>;
	ContextsVector ctxs;

	void addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const JsonPrintFilter &jsonFilter);
	const TagsMatcher &getTagsMatcher(int nsid) const;
	const PayloadType &getPayloadType(int nsid) const;
	TagsMatcher &getTagsMatcher(int nsid);
	PayloadType &getPayloadType(int nsid);
	int getMergedNSCount() const;

protected:
	class JsonEncoderDatasourceWithJoins;

private:
	void encodeJSON(int idx, WrSerializer &ser) const;
};

}  // namespace reindexer
