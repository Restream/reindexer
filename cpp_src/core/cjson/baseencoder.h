#pragma once

#include "cjsonbuilder.h"
#include "core/keyvalue/key_string.h"
#include "core/payload/payloadiface.h"
#include "fieldextractor.h"
#include "jsonbuilder.h"
#include "tools/serializer.h"

namespace reindexer {

class TagsMatcher;
class JsonBuilder;

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
class BaseEncoder {
public:
	BaseEncoder(const TagsMatcher *tagsMatcher, const FieldsSet *filter = nullptr);
	void Encode(ConstPayload *pl, Builder &builder, IEncoderDatasourceWithJoins *ds = nullptr);
	void Encode(string_view tuple, Builder &wrSer);

protected:
	bool encode(ConstPayload *pl, Serializer &rdser, Builder &builder, bool visible);
	void encodeJoinedItems(Builder &builder, IEncoderDatasourceWithJoins *ds, size_t joinedIdx);

	string_view getPlTuple(ConstPayload *pl);

	const TagsMatcher *tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];
	const FieldsSet *filter_;
	WrSerializer tmpPlTuple_;
	TagsPath curTagsPath_;
};

using JsonEncoder = BaseEncoder<JsonBuilder>;
using CJsonEncoder = BaseEncoder<CJsonBuilder>;

}  // namespace reindexer
