#pragma once

#include "core/payload/payloadiface.h"
#include "jsonprintfilter.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class Serializer;

class IJsonEncoderDatasourceWithJoins {
public:
	IJsonEncoderDatasourceWithJoins() = default;
	virtual ~IJsonEncoderDatasourceWithJoins() = default;

	virtual size_t GetJoinedRowsCount() = 0;
	virtual size_t GetJoinedRowItemsCount(size_t rowId) = 0;
	virtual ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) = 0;
	virtual const string &GetJoinedItemNamespace(size_t rowid) = 0;
	virtual const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) = 0;
	virtual const JsonPrintFilter &GetJoinedItemJsonFilter(size_t rowid) = 0;
};

class JsonEncoder {
public:
	JsonEncoder(const TagsMatcher &tagsMatcher, const JsonPrintFilter &filter);
	void Encode(ConstPayload *pl, WrSerializer &wrSer);
	void Encode(ConstPayload *pl, WrSerializer &wrSer, IJsonEncoderDatasourceWithJoins &ds);

protected:
	bool encodeJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser, bool &first, bool visible);
	bool encodeJoinedItem(WrSerializer &wrSer, ConstPayload &pl);
	void encodeJoinedItems(WrSerializer &wrSer, IJsonEncoderDatasourceWithJoins &ds, size_t joinedIdx, bool &first);

	string_view getPlTuple(ConstPayload *pl);

	const TagsMatcher &tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];
	const JsonPrintFilter &filter_;
	key_string tmpPlTuple_;
};

}  // namespace reindexer
