#pragma once

#include "core/payload/payloadiface.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class Serializer;

class JsonPrintFilter {
public:
	JsonPrintFilter(){};
	JsonPrintFilter(const TagsMatcher &tagsMatcher, const h_vector<string, 4> &filter);
	bool Match(int tag) const { return !filter_.size() || (tag < int(filter_.size()) && filter_[tag]); }

protected:
	h_vector<uint8_t, 32> filter_;
};

class IJsonEncoderDatasourceWithJoins {
public:
	IJsonEncoderDatasourceWithJoins() = default;
	virtual ~IJsonEncoderDatasourceWithJoins() = default;

	virtual size_t GetJoinedRowsCount() = 0;
	virtual size_t GetJoinedRowItemsCount(size_t rowId) = 0;
	virtual ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) = 0;
	virtual const string &GetJoinedItemNamespace(size_t rowid) = 0;
};

class JsonEncoder {
public:
	JsonEncoder(const TagsMatcher &tagsMatcher, const JsonPrintFilter &filter);
	void Encode(ConstPayload *pl, WrSerializer &wrSer);
	void Encode(ConstPayload *pl, WrSerializer &wrSer, IJsonEncoderDatasourceWithJoins &ds);

protected:
	bool encodeJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser, bool &first, bool visible);
	bool encodeJoinedItem(WrSerializer &wrSer, ConstPayload &pl);
	bool encodeJoinedItems(WrSerializer &wrSer, IJsonEncoderDatasourceWithJoins &ds, size_t joinedIdx);

	key_string &getPlTuple(ConstPayload *pl, key_string &plTuple);

	const TagsMatcher &tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];
	const JsonPrintFilter &filter_;
};

}  // namespace reindexer
