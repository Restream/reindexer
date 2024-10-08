#include "baseencoder.h"
#include <cstdlib>
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "core/keyvalue/p_string.h"
#include "csvbuilder.h"
#include "jsonbuilder.h"
#include "msgpackbuilder.h"
#include "protobufbuilder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

template <typename Builder>
BaseEncoder<Builder>::BaseEncoder(const TagsMatcher* tagsMatcher, const FieldsSet* filter) : tagsMatcher_(tagsMatcher), filter_(filter) {}

template <typename Builder>
void BaseEncoder<Builder>::Encode(std::string_view tuple, Builder& builder, IAdditionalDatasource<Builder>* ds) {
	Serializer rdser(tuple);
	builder.SetTagsMatcher(tagsMatcher_);
	if constexpr (kWithTagsPathTracking) {
		builder.SetTagsPath(&curTagsPath_);
	}

	[[maybe_unused]] const ctag begTag = rdser.GetCTag();
	assertrx(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object(nullptr);
	while (encode(nullptr, rdser, objNode, true));
	if (ds) {
		assertrx(!ds->GetJoinsDatasource());
		ds->PutAdditionalFields(objNode);
	}
}

template <typename Builder>
void BaseEncoder<Builder>::Encode(ConstPayload& pl, Builder& builder, IAdditionalDatasource<Builder>* ds) {
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
	Builder objNode = builder.Object(nullptr);
	while (encode(&pl, rdser, objNode, true));

	if (ds) {
		if (const auto joinsDs = ds->GetJoinsDatasource()) {
			for (size_t i = 0, cnt = joinsDs->GetJoinedRowsCount(); i < cnt; ++i) {
				encodeJoinedItems(objNode, joinsDs, i);
			}
		}
		ds->PutAdditionalFields(objNode);
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
		computeObjectLength(tagsLengths_, 0, endPos);
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
		subEnc.GetTagsMeasures(pl, nullptr);
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

template <typename Builder>
bool BaseEncoder<Builder>::encode(ConstPayload* pl, Serializer& rdser, Builder& builder, bool visible) {
	const ctag tag = rdser.GetCTag();
	const TagType tagType = tag.Type();

	if (tagType == TAG_END) {
		if constexpr (kWithFieldExtractor) {
			if (visible && filter_ && indexedTagsPath_.size() && indexedTagsPath_.back().IsWithIndex()) {
				const auto field = builder.TargetField();
				if (field >= 0 && !builder.IsHavingOffset() && filter_->match(indexedTagsPath_)) {
					builder.OnScopeEnd(fieldsoutcnt_[field]);
				}
			}
		}
		return false;
	}
	const int tagName = tag.Name();
	PathScopeT pathScope(curTagsPath_, tagName);
	TagsPathScope<IndexedTagsPathInternalT> indexedPathScope(indexedTagsPath_, visible ? tagName : 0);
	if (visible && filter_ && tagName) {
		visible = filter_->match(indexedTagsPath_);
	}

	const int tagField = tag.Field();

	// get field from indexed field
	if (tagField >= 0) {
		if (!pl) {
			throw Error(errParams, "Trying to encode index field %d without payload", tagField);
		}
		const auto& f = pl->Type().Field(tagField);
		if (!f.IsArray() && objectScalarIndexes_.test(tagField)) {
			throw Error(errParams, "Non-array field '%s' [%d] from '%s' can only be encoded once.", f.Name(), tagField, pl->Type().Name());
		}
		assertrx(tagField < pl->NumFields());
		int& cnt = fieldsoutcnt_[tagField];
		switch (tagType) {
			case TAG_ARRAY: {
				const auto count = rdser.GetVarUint();
				if (visible) {
					pl->Type().Field(tagField).Type().EvaluateOneOf(
						[&](KeyValueType::Bool) { builder.Array(tagName, pl->GetArray<bool>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Int) { builder.Array(tagName, pl->GetArray<int>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Int64) { builder.Array(tagName, pl->GetArray<int64_t>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Double) { builder.Array(tagName, pl->GetArray<double>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::String) { builder.Array(tagName, pl->GetArray<p_string>(tagField).subspan(cnt, count), cnt); },
						[&](KeyValueType::Uuid) { builder.Array(tagName, pl->GetArray<Uuid>(tagField).subspan(cnt, count), cnt); },
						[](OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite>) noexcept {
							assertrx(false);
							abort();
						});
				}
				cnt += int(count);
				break;
			}
			case TAG_NULL:
				objectScalarIndexes_.set(tagField);
				if (visible) {
					builder.Null(tagName);
				}
				break;
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_END:
			case TAG_OBJECT:
			case TAG_UUID:
				objectScalarIndexes_.set(tagField);
				if (visible) {
					builder.Put(tagName, pl->Get(tagField, cnt), cnt);
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
					if (visible) {
						auto arrNode = builder.Array(tagName);
						auto& lastIdxTag = indexedTagsPath_.back();
						for (size_t i = 0; i < atagCount; ++i) {
							lastIdxTag.SetIndex(i);
							encode(pl, rdser, arrNode, true);
						}
					} else {
						thread_local static Builder arrNode;
						for (size_t i = 0; i < atagCount; ++i) {
							encode(pl, rdser, arrNode, false);
						}
					}
				} else if (visible) {
					builder.Array(tagName, rdser, atagType, atagCount);
				} else {
					const KeyValueType kvt{atagType};
					for (size_t i = 0; i < atagCount; ++i) {
						rdser.SkipRawVariant(kvt);
					}
				}
				break;
			}
			case TAG_OBJECT: {
				if (visible) {
					auto objNode = builder.Object(tagName);
					while (encode(pl, rdser, objNode, true));
				} else {
					thread_local static Builder objNode;
					while (encode(pl, rdser, objNode, false));
				}
				break;
			}
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_BOOL:
			case TAG_STRING:
			case TAG_NULL:
			case TAG_END:
			case TAG_UUID: {
				const KeyValueType kvt{tagType};
				if (visible) {
					Variant value = rdser.GetRawVariant(kvt);
					builder.Put(tagName, std::move(value), 0);
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
	const TagType tagType = tag.Type();
	if (tagType == TAG_END) {
		tagsLengths_.push_back(EndObject);
		return false;
	}
	const int tagName = tag.Name();

	if (tagName && filter_) {
		curTagsPath_.push_back(tagName);
	}

	int tagField = tag.Field();
	tagsLengths_.push_back(kStandardFieldSize);

	// get field from indexed field
	if (tagField >= 0) {
		assertrx(tagField < pl.NumFields());
		switch (tagType) {
			case TAG_ARRAY: {
				const auto count = rdser.GetVarUint();
				tagsLengths_.back() = count;
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
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				const carraytag atag = rdser.GetCArrayTag();
				const auto atagCount = atag.Count();
				const auto atagType = atag.Type();
				tagsLengths_.back() = atagCount;
				if (atagType == TAG_OBJECT) {
					for (size_t i = 0; i < atagCount; i++) {
						tagsLengths_.push_back(StartArrayItem);
						collectTagsSizes(pl, rdser);
						tagsLengths_.push_back(EndArrayItem);
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
			case TAG_UUID: {
				rdser.SkipRawVariant(KeyValueType{tagType});
			}
		}
	}
	if (tagName && filter_) {
		curTagsPath_.pop_back();
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
		buildPayloadTuple(pl, tagsMatcher_, tmpPlTuple_);
		return tmpPlTuple_.Slice();
	}

	return tuple;
}

template class BaseEncoder<JsonBuilder>;
template class BaseEncoder<CJsonBuilder>;
template class BaseEncoder<MsgPackBuilder>;
template class BaseEncoder<ProtobufBuilder>;
template class BaseEncoder<FieldsExtractor>;
template class BaseEncoder<CsvBuilder>;

}  // namespace reindexer
