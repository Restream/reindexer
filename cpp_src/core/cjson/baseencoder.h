#pragma once

#include <optional>

#include "core/cjson/multidimensional_array_checker.h"
#include "core/payload/payloadiface.h"
#include "core/rank_t.h"
#include "estl/concepts.h"
#include "fields_explorer/field_array_analizer.h"
#include "fields_explorer/fields_array_value_extractor.h"
#include "tagslengths.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {

class TagsMatcher;
class FieldsFilter;

namespace builders {
class CsvBuilder;
class ProtobufBuilder;
class CJsonBuilder;
class MsgPackBuilder;
}  // namespace builders
using builders::CsvBuilder;
using builders::ProtobufBuilder;
using builders::CJsonBuilder;
using builders::MsgPackBuilder;

template <typename Builder>
class [[nodiscard]] IJoinsDatasource;

template <typename Builder>
class [[nodiscard]] BaseEncoder {
public:
	struct [[nodiscard]] AdditionalFields {
		std::optional<RankT> rank;
		std::optional<int> shardId;

		void Put(Builder&) const;
	};

	struct [[nodiscard]] Context {
		IJoinsDatasource<Builder>* joins = nullptr;
		AdditionalFields fields;
	};

	explicit BaseEncoder(const TagsMatcher* tagsMatcher, const FieldsFilter* filter);
	void Encode(ConstPayload& pl, Builder& builder, const Context& ctx = Context());
	void Encode(std::string_view tuple, Builder& wrSer, const Context& ctx = Context());

	const TagsLengths& GetTagsMeasures(ConstPayload& pl, IJoinsDatasource<Builder>* ds = nullptr);

private:
	using IndexedTagsPathInternalT = IndexedTagsPathImpl<16>;
	constexpr static bool kWithTagsPathTracking = std::is_same_v<ProtobufBuilder, Builder>;
	constexpr static bool kWithFieldArrayAnalizer = concepts::OneOf<Builder, cjson::FieldArrayAnalizer, cjson::FieldsArrayValueExtractor>;
	constexpr static bool kWithMultidimensionalArrayChecker = std::is_same_v<Builder, MultidimensionalArrayChecker>;

	struct [[nodiscard]] DummyTagsPathScope {
		DummyTagsPathScope(TagsPath&, TagName) noexcept {}
	};
	using PathScopeT = std::conditional_t<kWithTagsPathTracking, TagsPathScope<TagsPath>, DummyTagsPathScope>;

	template <concepts::TagNameOrIndex TagType, typename BuilderT>
	bool encode(ConstPayload* pl, Serializer& rdser, BuilderT&& builder, TagType indexedTag);
	template <concepts::TagNameOrIndex TagType, typename BuilderT>
	bool encodeImpl(ConstPayload* pl, ctag ctag, Serializer& rdser, BuilderT&& builder, TagType indexedTag);
	void encodeJoinedItems(Builder& builder, IJoinsDatasource<Builder>* ds, size_t joinedIdx);
	bool collectTagsSizes(ConstPayload& pl, Serializer& rdser);
	void collectJoinedItemsTagsSizes(IJoinsDatasource<Builder>* ds, size_t joinedField);

	std::string_view getPlTuple(ConstPayload& pl);

	const TagsMatcher* tagsMatcher_{nullptr};
	std::array<int, kMaxIndexes> fieldsoutcnt_;
	const FieldsFilter* filter_{nullptr};
	WrSerializer tmpPlTuple_;
	TagsPath curTagsPath_;
	IndexedTagsPathInternalT indexedTagsPath_;
	TagsLengths tagsLengths_;
	ScalarIndexesSetT objectScalarIndexes_;
};

template <typename Builder>
using EncoderContext = typename BaseEncoder<Builder>::Context;
using JsonEncoder = BaseEncoder<JsonBuilder>;
using CJsonEncoder = BaseEncoder<CJsonBuilder>;
using MsgPackEncoder = BaseEncoder<MsgPackBuilder>;
using ProtobufEncoder = BaseEncoder<ProtobufBuilder>;
using CsvEncoder = BaseEncoder<CsvBuilder>;

}  // namespace reindexer
