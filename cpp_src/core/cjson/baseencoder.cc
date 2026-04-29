#include "baseencoder.h"
#include <cstdlib>
#include <type_traits>
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "core/cjson/fields_explorer/field_size_evaluator.h"
#include "core/cjson/fields_explorer/fields_array_value_extractor.h"
#include "core/cjson/fields_explorer/fields_value_extractor.h"
#include "core/cjson/tagspath.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/p_string.h"
#include "core/queryresults/fields_filter.h"
#include "csvbuilder.h"
#include "field_extractor_grouping.h"
#include "jsonbuilder.h"
#include "msgpackbuilder.h"
#include "multidimensional_array_checker.h"
#include "protobufbuilder.h"
#include "tagsmatcher.h"
#include "tools/assertrx.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer {

namespace {
bool isEmptyName(TagIndex) noexcept { return false; }
bool isEmptyName(TagName name) noexcept { return name.IsEmpty(); }
}  // namespace

template <typename Builder>
BaseEncoder<Builder>::BaseEncoder(const TagsMatcher* tagsMatcher, const FieldsFilter* filter)
	: tagsMatcher_(tagsMatcher), filter_(filter) {}

template <typename Builder>
void BaseEncoder<Builder>::Encode(std::string_view tuple, Builder& builder, const h_vector<IAdditionalDatasource<Builder>*, 2>& dss) {
	Serializer rdser(tuple);
	builder.SetTagsMatcher(tagsMatcher_);
	if constexpr (kWithTagsPathTracking) {
		builder.SetTagsPath(&curTagsPath_);
	}

	[[maybe_unused]] const ctag begTag = rdser.GetCTag();
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object();
	while (encode(nullptr, rdser, objNode, TagName::Empty()));
	for (auto ds : dss) {
		if (ds) {
			if (const auto joinsDs = ds->GetJoinsDatasource()) {
				for (size_t i = 0; i < joinsDs->GetJoinedRowsCount(); ++i) {
					encodeJoinedItems(objNode, joinsDs, i);
				}
			}
			ds->PutAdditionalFields(objNode);
		}
	}
}

template <typename Builder>
void BaseEncoder<Builder>::Encode(ConstPayload& pl, Builder& builder, const h_vector<IAdditionalDatasource<Builder>*, 2>& dss) {
	Serializer rdser(getPlTuple(pl));
	if (rdser.Eof()) {
		return;
	}

	objectScalarIndexes_.reset();
	std::fill(fieldsoutcnt_.begin(), fieldsoutcnt_.end(), 0);
	builder.SetTagsMatcher(tagsMatcher_);
	if constexpr (kWithTagsPathTracking) {
		builder.SetTagsPath(&curTagsPath_);
	}
	[[maybe_unused]] const ctag begTag = rdser.GetCTag();
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object();
	while (encode(&pl, rdser, objNode, TagName::Empty()));
	for (auto ds : dss) {
		if (ds) {
			if (const auto joinsDs = ds->GetJoinsDatasource()) {
				for (size_t i = 0, cnt = joinsDs->GetJoinedRowsCount(); i < cnt; ++i) {
					encodeJoinedItems(objNode, joinsDs, i);
				}
			}
			ds->PutAdditionalFields(objNode);
		}
	}
}

template <typename Builder>
const TagsLengths& BaseEncoder<Builder>::GetTagsMeasures(ConstPayload& pl, IEncoderDatasourceWithJoins* ds) {
	tagsLengths_.clear();
	Serializer rdser(getPlTuple(pl));
	if (!rdser.Eof()) {
		[[maybe_unused]] const ctag beginTag = rdser.GetCTag();
		assertrx(beginTag.Type() == TAG_OBJECT);

		tagsLengths_.reserve(kMaxIndexes);
		tagsLengths_.push_back(StartObject);

		while (collectTagsSizes(pl, rdser)) {
		}

		if (ds && ds->GetJoinedRowsCount() > 0) {
			for (size_t i = 0, rows = ds->GetJoinedRowsCount(); i < rows; ++i) {
				collectJoinedItemsTagsSizes(ds, i);
			}
		}

		size_t endPos = 0;
		std::ignore = computeObjectLength(tagsLengths_, 0, endPos);
	}
	return tagsLengths_;
}

template <typename Builder>
void BaseEncoder<Builder>::collectJoinedItemsTagsSizes(IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) {
		return;
	}

	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		std::ignore = subEnc.GetTagsMeasures(pl, nullptr);
	}
}

template <typename Builder>
void BaseEncoder<Builder>::encodeJoinedItems(Builder& builder, IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) {
		return;
	}

	std::string nsTagName("joined_" + ds->GetJoinedItemNamespace(rowid));
	auto arrNode = builder.Array(nsTagName);

	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		subEnc.Encode(pl, arrNode);
	}
}
namespace {
struct [[nodiscard]] DummyBuilder {};

template <typename Builder, typename... Args>
auto GetArrayBuilder(Builder&& builder, Args&&... args) {
	if constexpr (!std::is_same_v<std::decay_t<decltype(builder)>, DummyBuilder>) {
		return builder.Array(std::forward<Args>(args)...);
	} else {
		return builder;
	}
}

template <typename Builder, typename... Args>
auto GetObjectBuilder(Builder&& builder, Args&&... args) {
	if constexpr (!std::is_same_v<std::decay_t<decltype(builder)>, DummyBuilder>) {
		return builder.Object(std::forward<Args>(args)...);
	} else {
		return builder;
	}
}
}  // namespace

template <typename Builder>
template <concepts::TagNameOrIndex TagT, typename BuilderT>
bool BaseEncoder<Builder>::encode(ConstPayload* pl, Serializer& rdser, BuilderT&& builder, TagT indexedTag) {
	constexpr bool kIsNotDummy = !std::is_same_v<std::decay_t<BuilderT>, DummyBuilder>;

	const ctag tag = rdser.GetCTag();

	const int tagField = tag.Field();
	const TagName tagName = tag.Name();

	if (tag == kCTagEnd) {
		if constexpr (kWithFieldArrayAnalizer && kIsNotDummy) {
			if (filter_ && indexedTagsPath_.size() && indexedTagsPath_.back().IsTagIndexNotAll()) {
				const auto field = builder.TargetField();
				if (field >= 0 && !builder.IsHavingOffset() && filter_->Match(indexedTagsPath_)) {
					builder.OnScopeEnd(fieldsoutcnt_[field]);
				}
			}
		}
		return false;
	}

	if constexpr (std::is_same_v<TagT, TagName>) {
		assertrx_dbg(indexedTag.IsEmpty());
		indexedTag = tagName;
	} else {
		assertrx_dbg(tag.Name().IsEmpty());
	}

	PathScopeT pathScope(curTagsPath_, tagName);
	TagsPathScope<IndexedTagsPathInternalT> indexedPathScope(indexedTagsPath_,
															 kIsNotDummy ? IndexedPathNode{indexedTag} : IndexedPathNode{TagName::Empty()});

	if (tagField >= 0) {
		if (!pl) [[unlikely]] {
			throw Error(errParams, "Trying to encode index field {} without payload", tagField);
		}
		assertrx(tagField < pl->NumFields());
	}

	bool visible = kIsNotDummy;
	if (visible && filter_ && !isEmptyName(indexedTag)) {
		if (tagField >= 0 && pl->Field(tagField).t_.IsFloatVector()) {
			visible = filter_->ContainsVector(tagField) || filter_->ContainsVector(indexedTagsPath_);
		} else if (tagField < 0 && tagsMatcher_ &&
				   tagsMatcher_->tags2field(indexedTagsPath_).ValueType().template Is<KeyValueType::FloatVector>()) {
			visible = filter_->ContainsVector(indexedTagsPath_);
		} else {
			visible = filter_->Match(indexedTagsPath_);
		}
	}

	return visible ? encodeImpl(pl, tag, rdser, builder, indexedTag) : encodeImpl(pl, tag, rdser, DummyBuilder{}, indexedTag);
}

template <typename Builder>
template <concepts::TagNameOrIndex TagT, typename BuilderT>
bool BaseEncoder<Builder>::encodeImpl(ConstPayload* pl, ctag tag, Serializer& rdser, BuilderT&& builder, TagT indexedTag) {
	constexpr bool kIsNotDummy = !std::is_same_v<std::decay_t<BuilderT>, DummyBuilder>;
	const int tagField = tag.Field();
	const TagType tagType = tag.Type();

	// get field from indexed field
	if (tagField >= 0) {
		const auto& f = pl->Type().Field(tagField);
		if (!f.IsArray() && objectScalarIndexes_.test(tagField)) {
			throw Error(errParams, "Non-array field '{}' [{}] from '{}' can only be encoded once.", f.Name(), tagField, pl->Type().Name());
		}
		int& cnt = fieldsoutcnt_[tagField];
		switch (tagType) {
			case TAG_ARRAY: {
				auto count = rdser.GetVarUInt();
				if constexpr (kIsNotDummy) {
					f.Type().EvaluateOneOf(
						[&](KeyValueType::Bool) { builder.Array(indexedTag, pl->GetArray<bool>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Int) { builder.Array(indexedTag, pl->GetArray<int>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Int64) { builder.Array(indexedTag, pl->GetArray<int64_t>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Double) { builder.Array(indexedTag, pl->GetArray<double>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::String) { builder.Array(indexedTag, pl->GetArray<p_string>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Uuid) { builder.Array(indexedTag, pl->GetArray<Uuid>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::FloatVector) {
							if (pl->Field(tagField).t_.IsArray()) {
								if (pl->GetFieldLen(tagField) == 0) {
									assertrx_dbg(count == 0);
									assertrx_dbg(cnt == 0);
									std::ignore = builder.Array(indexedTag);
									return;
								}
							} else {
								assertrx_dbg(cnt == 0);	 // Offset is always zero
								objectScalarIndexes_.set(tagField);
							}
							const auto value = pl->Get(tagField, cnt);
							const auto view = ConstFloatVectorView(value);
							assertrx_dbg(unsigned(view.Dimension()) == count);
							if constexpr (kWithFieldArrayAnalizer) {
								builder.Put(indexedTag, value, cnt);
							} else if constexpr (kWithMultidimensionalArrayChecker) {
								builder.Array(indexedTag, view.Dimension().Value(), cnt, TreatAsSingleElement_True);
							} else {
								if (view.IsStripped()) [[unlikely]] {
									throw Error(errLogic, "Attempt to serialize stripped vector");
								}
								builder.Array(indexedTag, view.Span(), cnt, TreatAsSingleElement_True);
							}
							count = 1;
						},
						[&](KeyValueType::Float) {
							// Indexed field can not contain float array now
							assertrx(false);
							abort();
						},
						[](concepts::OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined,
										   KeyValueType::Composite> auto) noexcept {
							assertrx(false);
							abort();
						});
				}
				cnt += int(count);
				break;
			}
			case TAG_NULL:
				if (!f.IsArray()) {
					objectScalarIndexes_.set(tagField);
				}
				if constexpr (kIsNotDummy) {
					if (f.IsFloatVector() && (!f.IsArray() || std::is_same_v<TagT, TagIndex>)) {
						if constexpr (std::is_same_v<TagT, TagName>) {
							builder.Array(indexedTag, std::span<const float>{}, 0);
						} else {
							builder.Array(indexedTag, std::span<const float>{}, indexedTag.AsNumber());
						}
					} else {
						builder.Null(indexedTag);
					}
				}
				break;
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_END:
			case TAG_OBJECT:
			case TAG_UUID:
			case TAG_FLOAT:
				if (!f.IsArray()) {
					objectScalarIndexes_.set(tagField);
				}
				if constexpr (kIsNotDummy) {
					builder.Put(indexedTag, pl->Get(tagField, cnt), cnt);
				}
				++cnt;
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				const carraytag atag = rdser.GetCArrayTag();
				const TagType atagType = atag.Type();
				const auto atagCount = atag.Count();
				if (atagType == TAG_OBJECT) {
					auto arrNode = GetArrayBuilder(builder, indexedTag);
					for (size_t i = 0; i < atagCount; ++i) {
						std::ignore = encode(pl, rdser, arrNode, TagIndex{i});
					}
				} else if constexpr (kIsNotDummy) {
					builder.Array(indexedTag, rdser, atagType, atagCount);
				} else {
					const KeyValueType kvt{atagType};
					for (size_t i = 0; i < atagCount; ++i) {
						rdser.SkipRawVariant(kvt);
					}
				}
				break;
			}
			case TAG_OBJECT: {
				auto objNode = GetObjectBuilder(builder, indexedTag);
				while (encode(pl, rdser, objNode, TagName::Empty()));
				break;
			}
			case TAG_NULL:
				if constexpr (kIsNotDummy) {
					if (tagsMatcher_) {
						const auto fieldProp = tagsMatcher_->tags2field(indexedTagsPath_);
						if (fieldProp.ValueType().template Is<KeyValueType::FloatVector>() &&
							(!fieldProp.IsArray() || std::is_same_v<TagT, TagIndex>)) {
							if constexpr (std::is_same_v<TagT, TagName>) {
								builder.Array(indexedTag, std::span<const float>{}, 0);
							} else {
								builder.Array(indexedTag, std::span<const float>{}, indexedTag.AsNumber());
							}
							break;
						}
					}
				}
				[[fallthrough]];
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_END:
			case TAG_UUID:
			case TAG_FLOAT: {
				const KeyValueType kvt{tagType};
				if constexpr (kIsNotDummy) {
					Variant value = rdser.GetRawVariant(kvt);
					if constexpr (std::is_same_v<TagT, TagName>) {
						builder.Put(indexedTag, std::move(value), 0);
					} else {
						builder.Put(indexedTag, std::move(value), indexedTag.AsNumber());
					}
				} else {
					rdser.SkipRawVariant(kvt);
				}
			}
		}
	}

	return true;
}

template <typename Builder>
bool BaseEncoder<Builder>::collectTagsSizes(ConstPayload& pl, Serializer& rdser) {
	const ctag tag = rdser.GetCTag();
	if (tag == kCTagEnd) {
		tagsLengths_.push_back(EndObject);
		return false;
	}
	const TagName tagName = tag.Name();

	if (!tagName.IsEmpty() && filter_) {
		indexedTagsPath_.emplace_back(tagName);
	}

	const int tagField = tag.Field();
	const TagType tagType = tag.Type();
	tagsLengths_.push_back(kStandardFieldSize);

	// get field from indexed field
	if (tagField >= 0) {
		assertrx(tagField < pl.NumFields());
		switch (tagType) {
			case TAG_ARRAY: {
				const auto count = rdser.GetVarUInt();
				const bool isFloatVector = pl.Field(tagField).t_.IsFloatVector();
				if (!isFloatVector || !filter_ || filter_->ContainsVector(tagField) || filter_->ContainsVector(indexedTagsPath_)) {
#ifdef RX_WITH_STDLIB_DEBUG
					if (isFloatVector) {
						assertrx_dbg(count == ConstFloatVectorView(pl.Get(tagField, 0)).Dimension().Value());
					}
#endif	// RX_WITH_STDLIB_DEBUG
					tagsLengths_.back() = count;
				} else {
					tagsLengths_.pop_back();
				}
				break;
			}
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_NULL:
			case TAG_OBJECT:
			case TAG_END:
			case TAG_UUID:
			case TAG_FLOAT:
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				const auto serPos = rdser.Pos();
				const carraytag atag = rdser.GetCArrayTag();
				const auto atagCount = atag.Count();
				const auto atagType = atag.Type();
				tagsLengths_.back() = atagCount;
				if (atagType == TAG_OBJECT) {
					if (tagsMatcher_ && tagsMatcher_->tags2field(indexedTagsPath_).ValueType().template Is<KeyValueType::FloatVector>() &&
						!filter_->ContainsVector(indexedTagsPath_)) {
						tagsLengths_.pop_back();
						rdser.SetPos(serPos);
						skipCjsonTag(tag, rdser);
					} else {
						for (size_t i = 0; i < atagCount; i++) {
							tagsLengths_.push_back(StartArrayItem);
							std::ignore = collectTagsSizes(pl, rdser);
							tagsLengths_.push_back(EndArrayItem);
						}
					}
				} else {
					const KeyValueType kvt{atagType};
					for (size_t i = 0; i < atagCount; i++) {
						rdser.SkipRawVariant(kvt);
					}
				}
				break;
			}
			case TAG_OBJECT:
				tagsLengths_.back() = StartObject;
				while (collectTagsSizes(pl, rdser)) {
				}
				break;
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_NULL:
			case TAG_END:
			case TAG_UUID:
			case TAG_FLOAT:
				rdser.SkipRawVariant(KeyValueType{tagType});
		}
	}
	if (!tagName.IsEmpty() && filter_) {
		indexedTagsPath_.pop_back();
	}

	return true;
}

template <typename Builder>
std::string_view BaseEncoder<Builder>::getPlTuple(ConstPayload& pl) {
	VariantArray kref;
	pl.Get(0, kref);

	std::string_view tuple(kref[0]);

	if (tagsMatcher_ && tuple.empty()) {
		tmpPlTuple_.Reset();
		ScalarIndexesSetT objectScalarIndexes;
		buildPayloadTuple(pl, tagsMatcher_, tmpPlTuple_, objectScalarIndexes);
		return tmpPlTuple_.Slice();
	}

	return tuple;
}

template class BaseEncoder<JsonBuilder>;
template class BaseEncoder<CJsonBuilder>;
template class BaseEncoder<MsgPackBuilder>;
template class BaseEncoder<ProtobufBuilder>;
template class BaseEncoder<cjson::FieldsValueExtractor>;
template class BaseEncoder<cjson::FieldSizeEvaluator>;
template class BaseEncoder<CsvBuilder>;
template class BaseEncoder<MultidimensionalArrayChecker>;
template class BaseEncoder<FieldsExtractorGrouping>;
template class BaseEncoder<cjson::FieldsArrayValueExtractor>;
template class BaseEncoder<cjson::FieldArrayAnalizer>;

}  // namespace reindexer
