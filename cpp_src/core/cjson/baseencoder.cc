#include "baseencoder.h"
#include <cstdlib>
#include "cjsonbuilder.h"
#include "core/keyvalue/p_string.h"
#include "jsonbuilder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

template <typename Builder>
BaseEncoder<Builder>::BaseEncoder(const TagsMatcher* tagsMatcher, const FieldsSet* filter) : tagsMatcher_(tagsMatcher), filter_(filter) {}

template <typename Builder>
void BaseEncoder<Builder>::Encode(string_view tuple, Builder& builder) {
	Serializer rdser(tuple);
	builder.SetTagsMatcher(tagsMatcher_);

	ctag begTag = rdser.GetVarUint();
	(void)begTag;
	assert(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object(nullptr);
	while (encode(nullptr, rdser, objNode, true))
		;
}

template <typename Builder>
void BaseEncoder<Builder>::Encode(ConstPayload* pl, Builder& builder, IEncoderDatasourceWithJoins* ds) {
	Serializer rdser(getPlTuple(pl));
	if (rdser.Eof()) {
		return;
	}

	for (int i = 0; i < pl->NumFields(); ++i) fieldsoutcnt_[i] = 0;
	builder.SetTagsMatcher(tagsMatcher_);
	ctag begTag = rdser.GetVarUint();
	(void)begTag;
	assert(begTag.Type() == TAG_OBJECT);
	Builder objNode = builder.Object(nullptr);

	while (encode(pl, rdser, objNode, true))
		;

	if (!ds || !ds->GetJoinedRowsCount()) return;

	for (size_t i = 0; i < ds->GetJoinedRowsCount(); ++i) {
		encodeJoinedItems(objNode, ds, i);
	}
}

template <typename Builder>
void BaseEncoder<Builder>::encodeJoinedItems(Builder& builder, IEncoderDatasourceWithJoins* ds, size_t rowid) {
	const size_t itemsCount = ds->GetJoinedRowItemsCount(rowid);
	if (!itemsCount) return;

	string nsTagName("joined_" + ds->GetJoinedItemNamespace(rowid));
	auto arrNode = builder.Array(nsTagName.c_str());

	BaseEncoder<Builder> subEnc(&ds->GetJoinedItemTagsMatcher(rowid), &ds->GetJoinedItemFieldsFilter(rowid));
	for (size_t i = 0; i < itemsCount; ++i) {
		ConstPayload pl(ds->GetJoinedItemPayload(rowid, i));
		subEnc.Encode(&pl, arrNode);
	}
}

template <typename Builder>
bool BaseEncoder<Builder>::encode(ConstPayload* pl, Serializer& rdser, Builder& builder, bool visible) {
	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();

	if (tagType == TAG_END) return false;

	if (tag.Name() && filter_) {
		curTagsPath_.push_back(tag.Name());
		visible = visible && filter_->match(curTagsPath_);
	}

	int tagField = tag.Field();

	// get field from indexed field
	if (tagField >= 0) {
		assert(tagField < pl->NumFields());

		int* cnt = &fieldsoutcnt_[tagField];

		switch (tagType) {
			case TAG_ARRAY: {
				int count = rdser.GetVarUint();
				if (visible) {
					switch (pl->Type().Field(tagField).Type()) {
						case KeyValueBool:
							builder.Array(tag.Name(), pl->GetArray<bool>(tagField).subspan((*cnt), count));
							break;
						case KeyValueInt:
							builder.Array(tag.Name(), pl->GetArray<int>(tagField).subspan((*cnt), count));
							break;
						case KeyValueInt64:
							builder.Array(tag.Name(), pl->GetArray<int64_t>(tagField).subspan((*cnt), count));
							break;
						case KeyValueDouble:
							builder.Array(tag.Name(), pl->GetArray<double>(tagField).subspan((*cnt), count));
							break;
						case KeyValueString:
							builder.Array(tag.Name(), pl->GetArray<p_string>(tagField).subspan((*cnt), count));
							break;
						default:
							std::abort();
					}
				}
				(*cnt) += count;
				break;
			}
			case TAG_NULL:
				if (visible) builder.Null(tag.Name());
				break;
			default:
				if (visible) builder.Put(tag.Name(), pl->Get(tagField, (*cnt)));
				(*cnt)++;
				break;
		}
	} else {
		switch (tagType) {
			case TAG_ARRAY: {
				carraytag atag = rdser.GetUInt32();
				if (atag.Tag() == TAG_OBJECT) {
					auto arrNode = visible ? builder.Array(tag.Name()) : Builder();
					for (int i = 0; i < atag.Count(); i++) encode(pl, rdser, arrNode, visible);
				} else if (visible) {
					builder.Array(tag.Name(), rdser, atag.Tag(), atag.Count());
				} else {
					for (int i = 0; i < atag.Count(); i++) rdser.GetRawVariant(KeyValueType(atag.Tag()));
				}
				break;
			}
			case TAG_OBJECT: {
				auto objNode = visible ? builder.Object(tag.Name()) : Builder();
				while (encode(pl, rdser, objNode, visible))
					;
				break;
			}
			default: {
				Variant value = rdser.GetRawVariant(KeyValueType(tagType));
				if (visible) builder.Put(tag.Name(), value);
			}
		}
	}
	if (tag.Name() && filter_) curTagsPath_.pop_back();

	return true;
}

template <typename Builder>
string_view BaseEncoder<Builder>::getPlTuple(ConstPayload* pl) {
	VariantArray kref;
	pl->Get(0, kref);

	p_string tuple(kref[0]);

	if (tagsMatcher_ && tuple.size() == 0) {
		tmpPlTuple_ = buildPayloadTuple(pl);
		return string_view(*tmpPlTuple_);
	}

	return string_view(tuple);
}

template <typename Builder>
key_string BaseEncoder<Builder>::buildPayloadTuple(ConstPayload* pl) {
	WrSerializer wrser;
	CJsonBuilder builder(wrser, CJsonBuilder::TypeObject);

	for (int field = 1; field < pl->NumFields(); ++field) {
		const PayloadFieldType& fieldType = pl->Type().Field(field);
		if (fieldType.JsonPaths().size() < 1 || fieldType.JsonPaths()[0].empty()) continue;

		VariantArray keyRefs;
		pl->Get(field, keyRefs);

		int tagName = tagsMatcher_->name2tag(fieldType.JsonPaths()[0].c_str());
		assert(tagName != 0);

		if (fieldType.IsArray()) {
			builder.ArrayRef(tagName, field, keyRefs.size());
		} else {
			assert(keyRefs.size() == 1);
			builder.Ref(tagName, keyRefs[0], field);
		}
	}
	builder.End();
	return make_key_string(reinterpret_cast<const char*>(wrser.Buf()), wrser.Len());
}

template class BaseEncoder<JsonBuilder>;
template class BaseEncoder<CJsonBuilder>;
template class BaseEncoder<FieldsExtractor>;

}  // namespace reindexer
