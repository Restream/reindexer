#pragma once

#include "cjsonbuilder.h"
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
class CsvBuilder;

class IEncoderDatasourceWithJoins {
public:
	IEncoderDatasourceWithJoins() = default;
	virtual ~IEncoderDatasourceWithJoins() = default;

	virtual size_t GetJoinedRowsCount() const noexcept = 0;
	virtual size_t GetJoinedRowItemsCount(size_t rowId) const = 0;
	virtual ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) = 0;
	virtual const std::string &GetJoinedItemNamespace(size_t rowid) = 0;
	virtual const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) = 0;
	virtual const FieldsSet &GetJoinedItemFieldsFilter(size_t rowid) noexcept = 0;
};

template <typename Builder>
class IAdditionalDatasource {
public:
	virtual void PutAdditionalFields(Builder &) const = 0;
	virtual IEncoderDatasourceWithJoins *GetJoinsDatasource() noexcept = 0;
};

template <typename Builder>
class BaseEncoder {
public:
	BaseEncoder(const TagsMatcher *tagsMatcher, const FieldsSet *filter = nullptr);
	void Encode(ConstPayload &pl, Builder &builder,
				const h_vector<IAdditionalDatasource<Builder> *, 2> &dss = h_vector<IAdditionalDatasource<Builder> *, 2>());
	void Encode(std::string_view tuple, Builder &wrSer,
				const h_vector<IAdditionalDatasource<Builder> *, 2> &dss = h_vector<IAdditionalDatasource<Builder> *, 2>());

	const TagsLengths &GetTagsMeasures(ConstPayload &pl, IEncoderDatasourceWithJoins *ds = nullptr);

protected:
	using IndexedTagsPathInternalT = IndexedTagsPathImpl<16>;
	constexpr static bool kWithTagsPathTracking = std::is_same_v<ProtobufBuilder, Builder>;
	constexpr static bool kWithFieldExtractor = std::is_same_v<FieldsExtractor, Builder>;

	struct DummyTagsPathScope {
		DummyTagsPathScope(TagsPath & /*tagsPath*/, int16_t /*tagName*/) noexcept {}
	};
	using PathScopeT = std::conditional_t<kWithTagsPathTracking, TagsPathScope<TagsPath>, DummyTagsPathScope>;

	bool encode(ConstPayload *pl, Serializer &rdser, Builder &builder, bool visible);
	void encodeJoinedItems(Builder &builder, IEncoderDatasourceWithJoins *ds, size_t joinedIdx);
	bool collectTagsSizes(ConstPayload &pl, Serializer &rdser);
	void collectJoinedItemsTagsSizes(IEncoderDatasourceWithJoins *ds, size_t rowid);

	std::string_view getPlTuple(ConstPayload &pl);

	const TagsMatcher *tagsMatcher_;
	int fieldsoutcnt_[kMaxIndexes];
	const FieldsSet *filter_;
	WrSerializer tmpPlTuple_;
	TagsPath curTagsPath_;
	IndexedTagsPathInternalT indexedTagsPath_;
	TagsLengths tagsLengths_;
	ScalarIndexesSetT objectScalarIndexes_;
};

using JsonEncoder = BaseEncoder<JsonBuilder>;
using CJsonEncoder = BaseEncoder<CJsonBuilder>;
using MsgPackEncoder = BaseEncoder<MsgPackBuilder>;
using ProtobufEncoder = BaseEncoder<ProtobufBuilder>;
using CsvEncoder = BaseEncoder<CsvBuilder>;

}  // namespace reindexer
