#pragma once

#include "cjsonbuilder.h"
#include "core/keyvalue/key_string.h"
#include "core/payload/payloadiface.h"
#include "fieldextractor.h"
#include "jsonbuilder.h"
#include "tagslengths.h"
#include "tools/serializer.h"

namespace reindexer {

class TagsMatcher;
class JsonBuilder;
class MsgPackBuilder;
class ProtobufBuilder;

class IEncoderDatasourceWithJoins {
public:
	IEncoderDatasourceWithJoins() = default;
	virtual ~IEncoderDatasourceWithJoins() = default;

	virtual size_t GetJoinedRowsCount() const = 0;
	virtual size_t GetJoinedRowItemsCount(size_t rowId) const = 0;
	virtual ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) const = 0;
	virtual const string &GetJoinedItemNamespace(size_t rowid) = 0;
	virtual const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) = 0;
	virtual const FieldsSet &GetJoinedItemFieldsFilter(size_t rowid) = 0;
};

template <typename Builder>
class IAdditionalDatasource {
public:
	virtual void PutAdditionalFields(Builder &) const = 0;
	virtual IEncoderDatasourceWithJoins *GetJoinsDatasource() = 0;
};

template <typename Builder>
class BaseEncoder {
public:
	BaseEncoder(const TagsMatcher *tagsMatcher, const FieldsSet *filter = nullptr);
	void Encode(ConstPayload *pl, Builder &builder, IAdditionalDatasource<Builder> * = nullptr);
	void Encode(string_view tuple, Builder &wrSer, IAdditionalDatasource<Builder> *);

	const TagsLengths &GetTagsMeasures(ConstPayload *pl, IEncoderDatasourceWithJoins *ds = nullptr);

protected:
	bool encode(ConstPayload *pl, Serializer &rdser, Builder &builder, bool visible);
	void encodeJoinedItems(Builder &builder, IEncoderDatasourceWithJoins *ds, size_t joinedIdx);
	bool collectTagsSizes(ConstPayload *pl, Serializer &rdser);
	void collectJoinedItemsTagsSizes(IEncoderDatasourceWithJoins *ds, size_t rowid);

	string_view getPlTuple(ConstPayload *pl);

	const TagsMatcher *tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];
	const FieldsSet *filter_;
	WrSerializer tmpPlTuple_;
	TagsPath curTagsPath_;
	IndexedTagsPath indexedTagsPath_;
	TagsLengths tagsLengths_;
};

using JsonEncoder = BaseEncoder<JsonBuilder>;
using CJsonEncoder = BaseEncoder<CJsonBuilder>;
using MsgPackEncoder = BaseEncoder<MsgPackBuilder>;
using ProtobufEncoder = BaseEncoder<ProtobufBuilder>;

}  // namespace reindexer
