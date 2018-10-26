#pragma once

#include "cjsonbuilder.h"
#include "core/keyvalue/key_string.h"
#include "core/payload/payloadiface.h"
#include "fieldextractor.h"
#include "jsonbuilder.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class Serializer;
class JsonBuilder;

class IEncoderDatasourceWithJoins {
public:
	IEncoderDatasourceWithJoins() = default;
	virtual ~IEncoderDatasourceWithJoins() = default;

	virtual size_t GetJoinedRowsCount() = 0;
	virtual size_t GetJoinedRowItemsCount(size_t rowId) = 0;
	virtual ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) = 0;
	virtual const string &GetJoinedItemNamespace(size_t rowid) = 0;
	virtual const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) = 0;
	virtual const FieldsSet &GetJoinedItemFieldsFilter(size_t rowid) = 0;
};

template <typename Builder>
class BaseEncoder {
public:
	BaseEncoder(const TagsMatcher *tagsMatcher, const FieldsSet *filter = nullptr);
	void Encode(ConstPayload *pl, Builder &builder, IEncoderDatasourceWithJoins *ds = nullptr);
	void Encode(string_view tuple, Builder &wrSer);

protected:
	bool encode(ConstPayload *pl, Serializer &rdser, Builder &builder, bool visible);
	void encodeJoinedItems(Builder &builder, IEncoderDatasourceWithJoins *ds, size_t joinedIdx);

	key_string buildPayloadTuple(ConstPayload *pl);
	string_view getPlTuple(ConstPayload *pl);

	const TagsMatcher *tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];
	const FieldsSet *filter_;
	key_string tmpPlTuple_;
	TagsPath curTagsPath_;
};

using JsonEncoder = BaseEncoder<JsonBuilder>;
using CJsonEncoder = BaseEncoder<CJsonBuilder>;

}  // namespace reindexer
