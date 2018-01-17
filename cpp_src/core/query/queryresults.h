#pragma once

#include <unordered_map>
#include "core/cjson/jsonencoder.h"
#include "core/item.h"
#include "estl/h_vector.h"
namespace reindexer {

using std::string;
using std::unordered_map;
class Namespace;

static const int kDefaultQueryResultsSize = 32;
class QueryResults : public h_vector<ItemRef, kDefaultQueryResultsSize> {
	friend class Namespace;
	friend class NsSelecter;
	friend class NsDescriber;

public:
	QueryResults(std::initializer_list<ItemRef> l) : h_vector<ItemRef, kDefaultQueryResultsSize>(l) {}

	QueryResults() = default;
	QueryResults(const QueryResults &) = delete;
	QueryResults(QueryResults &&) = default;
	~QueryResults();
	QueryResults &operator=(const QueryResults &qr) = delete;
	QueryResults &operator=(QueryResults &&qr) = default;

	void Add(const ItemRef &i);
	void Dump() const;
	void GetJSON(int idx, WrSerializer &wrser, bool withHdrLen = true) const;
	Item *GetItem(int idx) const;

	// joinded fields 0 - 1st joined ns, 1 - second joined
	unordered_map<IdType, vector<QueryResults>> joined_;  // joinded items
	h_vector<double> aggregationResults;
	int totalCount = 0;
	bool haveProcent = false;

	// protected:
	struct Context {
		Context() : tagsMatcher_(nullptr) {}
		Context(PayloadType::Ptr type, TagsMatcher tagsMatcher, JsonPrintFilter jsonFilter)
			: type_(type), tagsMatcher_(tagsMatcher), jsonFilter_(jsonFilter) {}

		PayloadType::Ptr type_;
		TagsMatcher tagsMatcher_;
		JsonPrintFilter jsonFilter_;
	};
	h_vector<Context, 1> ctxs;

	class JsonEncoderDatasourceWithJoins : public IJsonEncoderDatasourceWithJoins {
	public:
		JsonEncoderDatasourceWithJoins(const vector<QueryResults> &joined, const h_vector<Context, 1> &ctxs);
		~JsonEncoderDatasourceWithJoins();

		size_t GetJoinedRowsCount() final;
		size_t GetJoinedRowItemsCount(size_t rowId) final;
		ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) final;
		const string &GetJoinedItemNamespace(size_t rowid) final;

	private:
		const vector<QueryResults> &joined_;
		const h_vector<Context, 1> &ctxs_;
	};

private:
	void EncodeJSON(int idx, WrSerializer &ser) const;
};

}  // namespace reindexer
