#include "cjsonmodifier.h"
#include "cjsontools.h"
#include "core/type_consts_helpers.h"
#include "jsondecoder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

const std::string_view kWrongFieldsAmountMsg = "Number of fields for update should be > 0";

class CJsonModifier::Context {
public:
	Context(const IndexedTagsPath &fieldPath, const VariantArray &v, WrSerializer &ser, std::string_view tuple, FieldModifyMode m,
			const Payload *pl = nullptr)
		: value(v), wrser(ser), rdser(tuple), mode(m), payload(pl) {
		jsonPath.reserve(fieldPath.size());
		for (const IndexedPathNode &node : fieldPath) {
			isForAllItems_ = isForAllItems_ || node.IsForAllItems();
			jsonPath.emplace_back(node.NameTag());
		}
		if (fieldPath.back().IsArrayNode()) {
			updateArrayElements = true;
			if (mode == FieldModeSet && value.empty()) {
				throw Error(errParams, "Array item should not be an empty value");
			}
		}
		fieldsArrayOffsets.fill(0);
	}
	[[nodiscard]] bool IsForAllItems() const noexcept { return isForAllItems_; }

	const VariantArray &value;
	WrSerializer &wrser;
	Serializer rdser;
	TagsPath jsonPath;
	IndexedTagsPath currObjPath;
	FieldModifyMode mode = FieldModeSet;
	bool fieldUpdated = false;
	bool updateArrayElements = false;
	const Payload *payload = nullptr;
	std::array<unsigned, kMaxIndexes> fieldsArrayOffsets;

private:
	bool isForAllItems_ = false;
};

void CJsonModifier::SetFieldValue(std::string_view tuple, const IndexedTagsPath &fieldPath, const VariantArray &val, WrSerializer &ser,
								  const Payload &pl) {
	auto ctx = initState(tuple, fieldPath, val, ser, &pl, FieldModifyMode::FieldModeSet);
	updateFieldInTuple(ctx);
	if (!ctx.fieldUpdated && !ctx.IsForAllItems()) {
		throw Error(errParams, "[SetFieldValue] Requested field or array's index was not found");
	}
}

void CJsonModifier::SetObject(std::string_view tuple, const IndexedTagsPath &fieldPath, const VariantArray &val, WrSerializer &ser,
							  const Payload &pl) {
	auto ctx = initState(tuple, fieldPath, val, ser, &pl, FieldModifyMode::FieldModeSetJson);
	buildCJSON(ctx);
	if (!ctx.fieldUpdated && !ctx.IsForAllItems()) {
		throw Error(errParams, "[SetObject] Requested field or array's index was not found");
	}
}

void CJsonModifier::RemoveField(std::string_view tuple, const IndexedTagsPath &fieldPath, WrSerializer &wrser) {
	auto ctx = initState(tuple, fieldPath, {}, wrser, nullptr, FieldModeDrop);
	dropFieldInTuple(ctx);
}

CJsonModifier::Context CJsonModifier::initState(std::string_view tuple, const IndexedTagsPath &fieldPath, const VariantArray &val,
												WrSerializer &ser, const Payload *pl, FieldModifyMode mode) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear<false>();
	Context ctx(fieldPath, val, ser, tuple, mode, pl);
	fieldPath_ = fieldPath;

	return ctx;
}

void CJsonModifier::updateObject(Context &ctx, int tagName) const {
	ctx.fieldUpdated = true;
	JsonDecoder jsonDecoder(tagsMatcher_);
	if (ctx.value.IsArrayValue()) {
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeArray, &tagsMatcher_, tagName);
		for (const auto &item : ctx.value) {
			auto objBuilder = cjsonBuilder.Object(nullptr);
			jsonDecoder.Decode(std::string_view(item), objBuilder, ctx.jsonPath);
		}
		return;
	}

	assertrx(ctx.value.size() == 1);
	CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeObject, &tagsMatcher_, tagName);
	jsonDecoder.Decode(std::string_view(ctx.value.front()), cjsonBuilder, ctx.jsonPath);
}

void CJsonModifier::insertField(Context &ctx) const {
	ctx.fieldUpdated = true;
	assertrx(ctx.currObjPath.size() < fieldPath_.size());

	int nestedObjects = 0;
	for (size_t i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		const int tagName = fieldPath_[i].NameTag();
		const bool finalTag = (i == fieldPath_.size() - 1);
		if (finalTag) {
			if (ctx.mode == FieldModeSetJson) {
				updateObject(ctx, tagName);
				continue;
			}

			const int field = tagsMatcher_.tags2field(ctx.jsonPath.data(), fieldPath_.size());
			const TagType tagType = determineUpdateTagType(ctx, field);
			isIndexed(field) ? putCJsonRef(tagType, tagName, field, ctx.value, ctx.wrser)
							 : putCJsonValue(tagType, tagName, ctx.value, ctx.wrser);
			continue;
		}

		ctx.wrser.PutCTag(ctag{TAG_OBJECT, tagName});
		++nestedObjects;
	}

	while (nestedObjects-- > 0) {
		ctx.wrser.PutCTag(kCTagEnd);
	}
	ctx.currObjPath.clear();
}

bool CJsonModifier::needToInsertField(const Context &ctx) const {
	assertrx_throw(!fieldPath_.empty());
	if (ctx.fieldUpdated || fieldPath_.back().IsArrayNode()) return false;
	if (ctx.currObjPath.size() >= fieldPath_.size()) return false;
	assertrx_throw(ctx.currObjPath.size() <= fieldPath_.size());
	for (unsigned i = 0; i < ctx.currObjPath.size(); ++i) {
		if (fieldPath_[i] != ctx.currObjPath[i]) {
			return false;
		}
	}
	if (ctx.IsForAllItems()) {
		throw Error(errParams, "Unable to insert new field with 'all items ([*])' syntax");
	}
	for (unsigned i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		if (fieldPath_[i].IsArrayNode()) {
			return false;
		}
	}
	return true;
}

TagType CJsonModifier::determineUpdateTagType(const Context &ctx, int field) const {
	if (isIndexed(field)) {
		const PayloadFieldType &fieldType = pt_.Field(field);
		if (!fieldType.IsArray() || ctx.updateArrayElements || !ctx.value.IsNullValue()) {
			for (auto &v : ctx.value) {
				if (!fieldType.Type().IsSame(v.Type())) {
					throw Error(errParams, "Inserted field %s type [%s] doesn't match it's index type [%s]", fieldType.Name(),
								v.Type().Name(), fieldType.Type().Name());
				}
			}
		}
	}

	if (ctx.updateArrayElements || ctx.value.IsArrayValue()) {
		return TAG_ARRAY;
	} else if (ctx.value.IsNullValue() || ctx.value.empty()) {
		return TAG_NULL;
	}
	return arrayKvType2Tag(ctx.value);
}

bool CJsonModifier::checkIfFoundTag(Context &ctx, bool isLastItem) const {
	if (tagsPath_.empty() || !fieldPath_.Compare(tagsPath_)) return false;

	const auto &backFieldPath = fieldPath_.back();
	if (!backFieldPath.IsArrayNode() || ((!backFieldPath.IsForAllItems() || isLastItem))) {
		ctx.fieldUpdated = true;
	}

	return true;
}

void CJsonModifier::setArray(Context &ctx) const {
	auto type = arrayKvType2Tag(ctx.value);
	ctx.wrser.PutCArrayTag(carraytag{ctx.value.size(), type});
	const bool isObjsArr = (type == TAG_OBJECT);
	for (const auto &item : ctx.value) {
		if (isObjsArr) {
			type = item.Type().ToTagType();
			ctx.wrser.PutCTag(ctag{type});
		}
		copyCJsonValue(type, item, ctx.wrser);
	}
}

void CJsonModifier::writeCTag(const ctag &tag, Context &ctx) {
	bool tagMatched = checkIfFoundTag(ctx);
	const TagType tagType = tag.Type();
	const int field = tag.Field();
	const int tagName = tag.Name();
	if (tagType == TAG_ARRAY) {
		const auto count = ctx.rdser.GetVarUint();
		if (!tagMatched || !ctx.fieldUpdated) {
			auto &lastTag = tagsPath_.back();
			for (uint64_t i = 0; i < count; ++i) {
				lastTag.SetIndex(i);
				const bool isLastItem = (i + 1 == count);
				tagMatched = checkIfFoundTag(ctx, isLastItem);
				if (tagMatched && ctx.fieldUpdated) {
					break;
				}
			}
		}

		if (tagMatched && ctx.fieldUpdated) {
			const auto resultTagType = determineUpdateTagType(ctx, field);
			ctx.wrser.PutCTag(ctag{resultTagType, tagName, field});
			if (resultTagType == TAG_ARRAY) {
				ctx.wrser.PutVarUint(ctx.updateArrayElements ? count : ctx.value.size());
			}
			return;
		}

		ctx.wrser.PutCTag(ctag{tagType, tagName, field});
		ctx.wrser.PutVarUint(count);
		return;
	}

	if (!tagMatched) {
		ctx.wrser.PutCTag(ctag{tagType, tagName, field});
		return;
	}

	if (ctx.updateArrayElements) {
		throw Error(errParams, "Unable to update scalar value by index");
	}
	const auto resultTagType = determineUpdateTagType(ctx, field);
	ctx.wrser.PutCTag(ctag{resultTagType, tagName, field});
	if (resultTagType == TAG_ARRAY) {
		ctx.wrser.PutVarUint(ctx.value.size());
	}
}

void CJsonModifier::updateArray(TagType atagType, uint32_t count, int tagName, Context &ctx) {
	assertrx_throw(!ctx.value.IsArrayValue());	// Unable to update array's element with array-value

	Variant value;
	if (!ctx.value.empty()) {
		value = ctx.value.front();
	}

	// situation is possible when array was homogeneous, and new element of different type is added
	// in this case array must change type and become heterogeneous
	const auto valueType = value.Type().ToTagType();
	assertrx((atagType != valueType) || (atagType != TAG_OBJECT));

	ctx.wrser.PutCArrayTag(carraytag{count, TAG_OBJECT});

	for (uint32_t i = 0; i < count; i++) {
		tagsPath_.back().SetIndex(i);
		const bool isLastItem = (i + 1 == count);
		if (checkIfFoundTag(ctx, isLastItem)) {
			(atagType == TAG_OBJECT) ? skipCjsonTag(ctag{ctx.rdser.GetCTag().Type()}, ctx.rdser, &ctx.fieldsArrayOffsets)
									 : skipCjsonTag(ctag{atagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
			ctx.wrser.PutCTag(ctag{valueType});
			copyCJsonValue(valueType, value, ctx.wrser);

			ctx.fieldUpdated = true;
			continue;  // next item
		}

		switch (atagType) {
			case TAG_OBJECT: {
				TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName, i);
				updateFieldInTuple(ctx);
				break;
			}
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_STRING:
			case TAG_ARRAY:
			case TAG_NULL:
			case TAG_BOOL:
			case TAG_END:
			case TAG_UUID:
				// array tag type updated (need store as object)
				ctx.wrser.PutCTag(ctag{atagType});
				copyCJsonValue(atagType, ctx.rdser, ctx.wrser);
				break;
		}
	}

	assertrx_throw(ctx.fieldUpdated);
}

void CJsonModifier::copyArray(int tagName, Context &ctx) {
	const carraytag atag = ctx.rdser.GetCArrayTag();
	const TagType atagType = atag.Type();
	const auto count = atag.Count();

	// store position in serializer
	const auto rdserPos = ctx.rdser.Pos();
	const auto wrserLen = ctx.wrser.Len();

	ctx.wrser.PutCArrayTag(atag);

	for (uint32_t i = 0; i < count; i++) {
		tagsPath_.back().SetIndex(i);
		const bool isLastItem = (i + 1 == count);
		// update item
		if (checkIfFoundTag(ctx, isLastItem)) {
			if (ctx.value.IsArrayValue()) {
				throw Error(errParams, "Unable to update array's element with array-value");
			}
			Variant value;
			if (!ctx.value.empty()) {
				value = ctx.value.front();
			}
			// situation is possible when array was homogeneous, and new element of different type is added
			const auto valueType = value.Type().ToTagType();
			if ((atagType != valueType) && (atagType != TAG_OBJECT)) {
				// back to beginning of array and rewrite as an array of objects
				ctx.rdser.SetPos(rdserPos);
				ctx.wrser.Reset(wrserLen);
				updateArray(atagType, count, tagName, ctx);
				return;	 // array updated - stop processing
			}

			// type of array not changed - simple rewrite item
			auto vtagType = atagType;
			if (atagType == TAG_OBJECT) {
				vtagType = ctx.rdser.GetCTag().Type();
				ctx.wrser.PutCTag(ctag{valueType});
			}
			skipCjsonTag(ctag{vtagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
			copyCJsonValue(valueType, value, ctx.wrser);

			ctx.fieldUpdated = true;
			continue;  // next item
		}

		// copy item as is
		switch (atagType) {
			case TAG_OBJECT: {
				TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName, i);
				updateFieldInTuple(ctx);
				break;
			}
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_STRING:
			case TAG_ARRAY:
			case TAG_NULL:
			case TAG_BOOL:
			case TAG_END:
			case TAG_UUID:
				copyCJsonValue(atagType, ctx.rdser, ctx.wrser);
				break;
		}
	}
}

bool CJsonModifier::updateFieldInTuple(Context &ctx) {
	const ctag tag = ctx.rdser.GetCTag();

	if (tag == kCTagEnd) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}
	const TagType tagType = tag.Type();
	const int field = tag.Field();
	const int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	if (isIndexed(field)) {
		writeCTag(tag, ctx);
		return true;
	}

	const bool tagMatched = checkIfFoundTag(ctx);
	const auto resultTagType = tagMatched ? determineUpdateTagType(ctx, field) : tagType;
	ctx.wrser.PutCTag(ctag{resultTagType, tagName, field});

	if (tagMatched) {
		if (ctx.updateArrayElements && tagType != TAG_ARRAY) {
			throw Error(errParams, "Unable to update scalar value by index");
		}
		if (resultTagType != TAG_NULL) {
			if (resultTagType == TAG_ARRAY) {
				setArray(ctx);
			} else if (ctx.value.empty()) {
				throw Error(errLogic, "Update value for field [%s] cannot be empty", tagsMatcher_.tag2name(tagName));
			} else if (ctx.value.size() == 1) {
				const auto item = ctx.value.front();
				copyCJsonValue(item.Type().ToTagType(), item, ctx.wrser);
			} else {
				throw Error(errParams, "Unexpected value to update");
			}
		}
		skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
		return true;
	}

	if (tagType == TAG_OBJECT) {
		TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName);
		while (updateFieldInTuple(ctx)) {
		}
		return true;
	}

	(tagType == TAG_ARRAY) ? copyArray(tagName, ctx) : copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
	return true;
}

bool CJsonModifier::dropFieldInTuple(Context &ctx) {
	const ctag tag = ctx.rdser.GetCTag();
	if (tag == kCTagEnd) {
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}

	const int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = (!ctx.fieldUpdated && fieldPath_.Compare(tagsPath_));
	if (tagMatched) {
		skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
		ctx.fieldUpdated = true;
		return true;
	}

	const int field = tag.Field();
	const TagType tagType = tag.Type();
	ctx.wrser.PutCTag(ctag{tagType, tagName, field});

	if (isIndexed(field)) {
		if (tagType == TAG_ARRAY) {
			const auto count = ctx.rdser.GetVarUint();
			ctx.wrser.PutVarUint(count);
		}
		return true;
	}

	if (tagType == TAG_OBJECT) {
		TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName);
		while (dropFieldInTuple(ctx)) {
		}
		return true;
	}

	if (tagType == TAG_ARRAY) {
		carraytag atag = ctx.rdser.GetCArrayTag();
		const TagType atagType = atag.Type();
		const auto size = int(atag.Count());
		tagMatched = (fieldPath_.back().IsArrayNode() && tagsPath_ == fieldPath_);
		if (tagMatched) {
			atag = carraytag(fieldPath_.back().IsForAllItems() ? 0 : size - 1, atagType);
			ctx.fieldUpdated = true;
		}

		ctx.wrser.PutCArrayTag(atag);
		for (int i = 0; i < size; ++i) {
			tagsPath_.back().SetIndex(i);
			if (tagMatched && (i == fieldPath_.back().Index() || fieldPath_.back().IsForAllItems())) {
				skipCjsonTag(ctag{atagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
				continue;
			}

			switch (atagType) {
				case TAG_OBJECT: {
					TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName, i);
					dropFieldInTuple(ctx);
					break;
				}
				case TAG_VARINT:
				case TAG_STRING:
				case TAG_DOUBLE:
				case TAG_BOOL:
				case TAG_ARRAY:
				case TAG_NULL:
				case TAG_END:
				case TAG_UUID:
					copyCJsonValue(atagType, ctx.rdser, ctx.wrser);
					break;
			}
		}
		return true;
	}

	copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
	return true;
}

void CJsonModifier::embedFieldValue(TagType type, int field, Context &ctx, size_t idx) const {
	if (isIndexed(field)) {
		assertrx(ctx.payload);
		const Variant v = ctx.payload->Get(field, ctx.fieldsArrayOffsets[field] + idx);
		copyCJsonValue(type, v, ctx.wrser);
		return;
	}

	copyCJsonValue(type, ctx.rdser, ctx.wrser);
}

bool CJsonModifier::buildCJSON(Context &ctx) {
	const auto tag = ctx.rdser.GetCTag();
	if (tag == kCTagEnd) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}
	TagType tagType = tag.Type();
	const int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = fieldPath_.Compare(tagsPath_);
	if (tagMatched) {
		tagType = TAG_OBJECT;
	} else {
		ctx.wrser.PutCTag(ctag{tagType, tagName});
	}

	if (tagType == TAG_OBJECT) {
		if (tagMatched) {
			skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
			updateObject(ctx, tagName);
			return true;
		}

		TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName);
		while (buildCJSON(ctx)) {
		}
		return true;
	}

	const auto field = tag.Field();
	if (tagType == TAG_ARRAY) {
		const carraytag atag{isIndexed(field) ? carraytag(ctx.rdser.GetVarUint(), pt_.Field(tag.Field()).Type().ToTagType())
											  : ctx.rdser.GetCArrayTag()};
		ctx.wrser.PutCArrayTag(atag);
		const auto arrSize = atag.Count();
		for (size_t i = 0; i < arrSize; ++i) {
			tagsPath_.back().SetIndex(i);
			tagMatched = fieldPath_.Compare(tagsPath_);
			if (tagMatched) {
				updateObject(ctx, 0);
				skipCjsonTag(ctx.rdser.GetCTag(), ctx.rdser, &ctx.fieldsArrayOffsets);
				continue;
			}

			switch (atag.Type()) {
				case TAG_OBJECT: {
					TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName);
					buildCJSON(ctx);
					break;
				}
				case TAG_VARINT:
				case TAG_DOUBLE:
				case TAG_STRING:
				case TAG_BOOL:
				case TAG_ARRAY:
				case TAG_NULL:
				case TAG_END:
				case TAG_UUID:
					embedFieldValue(atag.Type(), field, ctx, i);
					break;
			}
		}

		if (isIndexed(field)) {
			ctx.fieldsArrayOffsets[field] += arrSize;
		}
		return true;
	}

	embedFieldValue(tagType, field, ctx, 0);
	if (isIndexed(field)) {
		ctx.fieldsArrayOffsets[field] += 1;
	}
	return true;
}

}  // namespace reindexer
