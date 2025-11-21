#include "cjsonmodifier.h"
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "jsondecoder.h"
#include "sparse_validator.h"
#include "tagsmatcher.h"
#include "tools/assertrx.h"
#include "tools/serializer.h"

namespace reindexer {

using namespace item_fields_validator;

const std::string_view kWrongFieldsAmountMsg = "Number of fields for update should be > 0";

class [[nodiscard]] CJsonModifier::Context {
public:
	Context(const IndexedTagsPath& fieldPath, const VariantArray& v, WrSerializer& ser, std::string_view tuple, FieldModifyMode m,
			FloatVectorsHolderVector& fvHolder, const Payload* pl = nullptr)
		: value(v), wrser(ser), rdser(tuple), mode(m), payload(pl), floatVectorsHolder(fvHolder) {
		jsonPath.reserve(fieldPath.size());
		for (const IndexedPathNode& node : fieldPath) {
			if (node.IsTagIndex()) {
				isForAllItems_ = isForAllItems_ || node.GetTagIndex().IsAll();
			} else {
				jsonPath.emplace_back(node.GetTagName());
			}
		}

		if (fieldPath.back().IsTagIndex()) {
			updateArrayElements = true;
			if (mode == FieldModeSet && value.empty()) {
				throw Error(errParams, "Array item should not be an empty value");
			}
		}
		fieldsArrayOffsets.fill(0);
	}
	bool IsForAllItems() const noexcept { return isForAllItems_; }

	const VariantArray& value;
	WrSerializer& wrser;
	Serializer rdser;
	TagsPath jsonPath;
	IndexedTagsPath currObjPath;
	FieldModifyMode mode = FieldModeSet;
	bool fieldUpdated = false;
	bool updateArrayElements = false;
	const Payload* payload = nullptr;
	std::array<unsigned, kMaxIndexes> fieldsArrayOffsets;
	FloatVectorsHolderVector& floatVectorsHolder;

private:
	bool isForAllItems_ = false;
};

void CJsonModifier::SetFieldValue(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser,
								  const Payload& pl, FloatVectorsHolderVector& floatVectorsHolder) {
	auto ctx = initState(tuple, fieldPath, val, ser, &pl, FieldModifyMode::FieldModeSet, floatVectorsHolder);
	std::ignore = updateFieldInTuple(ctx);
	if (!ctx.fieldUpdated && !ctx.IsForAllItems()) {
		throw Error(errParams, "[SetFieldValue] Requested field or array's index was not found");
	}
}

void CJsonModifier::SetObject(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser,
							  const Payload& pl, FloatVectorsHolderVector& floatVectorsHolder) {
	auto ctx = initState(tuple, fieldPath, val, ser, &pl, FieldModifyMode::FieldModeSetJson, floatVectorsHolder);
	std::ignore = buildCJSON(ctx);
	if (!ctx.fieldUpdated && !ctx.IsForAllItems()) {
		throw Error(errParams, "[SetObject] Requested field or array's index was not found");
	}
}

void CJsonModifier::RemoveField(std::string_view tuple, const IndexedTagsPath& fieldPath, WrSerializer& wrser) {
	thread_local FloatVectorsHolderVector floatVectorsHolder;
	auto ctx = initState(tuple, fieldPath, {}, wrser, nullptr, FieldModeDrop, floatVectorsHolder);
	std::ignore = dropFieldInTuple(ctx, JustCopy_False);
}

CJsonModifier::Context CJsonModifier::initState(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val,
												WrSerializer& ser, const Payload* pl, FieldModifyMode mode,
												FloatVectorsHolderVector& floatVectorsHolder) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear<false>();
	Context ctx(fieldPath, val, ser, tuple, mode, floatVectorsHolder, pl);
	fieldPath_ = fieldPath;

	return ctx;
}

void CJsonModifier::updateObject(Context& ctx, TagName tagName) const {
	ctx.fieldUpdated = true;
	JsonDecoder jsonDecoder(tagsMatcher_);
	if (ctx.value.IsArrayValue()) {
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeArray, &tagsMatcher_, tagName);
		for (const auto& item : ctx.value) {
			auto objBuilder = cjsonBuilder.Object();
			jsonDecoder.Decode(std::string_view(item), objBuilder, ctx.jsonPath, ctx.floatVectorsHolder);
		}
		return;
	}

	assertrx(ctx.value.size() == 1);
	CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeObject, &tagsMatcher_, tagName);
	jsonDecoder.Decode(std::string_view(ctx.value.front()), cjsonBuilder, ctx.jsonPath, ctx.floatVectorsHolder);
}

void CJsonModifier::insertField(Context& ctx) const {
	ctx.fieldUpdated = true;
	assertrx_dbg(ctx.currObjPath.size() < fieldPath_.size());

	int nestedObjects = 0;
	for (size_t i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		assertrx(fieldPath_[i].IsTagName());
		const TagName tagName = fieldPath_[i].GetTagName();
		const bool finalTag = (i == fieldPath_.size() - 1);
		if (finalTag) {
			if (ctx.mode == FieldModeSetJson) {
				updateObject(ctx, tagName);
				continue;
			}

			const auto field = tagsMatcher_.tags2field(ctx.jsonPath);
			const auto updateTagType = determineUpdateTagType(ctx, field.IndexNumber());
			if (updateTagType.isFloatVectorRef) {
				ctx.wrser.PutCTag(ctag{TAG_ARRAY, tagName, field.IndexNumber()});
				ctx.wrser.PutVarUint(uint32_t(updateTagType.valueDims));
			} else if (field.IsRegularIndex()) {
				putCJsonRef(updateTagType.rawType, tagName, field.IndexNumber(), ctx.value, ctx.wrser);
			} else {
				putCJsonValue(updateTagType.rawType, tagName, ctx.value, ctx.wrser);
			}
		} else {
			ctx.wrser.PutCTag(ctag{TAG_OBJECT, tagName});
			++nestedObjects;
		}
	}

	while (nestedObjects-- > 0) {
		ctx.wrser.PutCTag(kCTagEnd);
	}
	ctx.currObjPath.clear();
}

bool CJsonModifier::needToInsertField(const Context& ctx) const {
	assertrx_throw(!fieldPath_.empty());
	if (ctx.fieldUpdated || fieldPath_.back().IsTagIndex()) {
		return false;
	}
	if (ctx.currObjPath.size() >= fieldPath_.size()) {
		return false;
	}
	for (unsigned i = 0; i < ctx.currObjPath.size(); ++i) {
		if (fieldPath_[i] != ctx.currObjPath[i]) {
			return false;
		}
	}
	if (ctx.IsForAllItems()) {
		throw Error(errParams, "Unable to insert new field with 'all items ([*])' syntax");
	}
	for (unsigned i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		if (fieldPath_[i].IsTagIndex()) {
			return false;
		}
	}
	return true;
}

CJsonModifier::UpdateTagType CJsonModifier::determineUpdateTagType(const Context& ctx, int field) const {
	if (isIndexed(field)) {
		const PayloadFieldType& fieldType = pt_.Field(field);
		if (!fieldType.IsArray() || ctx.updateArrayElements || !ctx.value.IsNullValue()) {
			for (auto& v : ctx.value) {
				if (!fieldType.Type().IsSame(v.Type())) {
					throw Error(errParams, "Inserted field {} type [{}] doesn't match it's index type [{}]", fieldType.Name(),
								v.Type().Name(), fieldType.Type().Name());
				}
			}
		}
		if (fieldType.IsFloatVector() && !ctx.value.IsNullValue()) {
			if (ctx.value.empty()) {
				throw Error(errParams, "Attempt to insert empty VariantArray into single float vector field [{}]", fieldType.Name());
			}
			if (ctx.value.size() != 1) {
				throw Error(errParams, "Attempt to insert multiple ({}) float vectors into single float vector field [{}]",
							ctx.value.size(), fieldType.Name());
			}
			auto vec = ConstFloatVectorView(ctx.value[0]);
			if (!vec.Dimension().IsZero() && vec.Dimension() != fieldType.FloatVectorDimension()) {
				throw Error(errParams, "Float vector field [{}] expects {} dimensions, but got {}", fieldType.Name(),
							fieldType.FloatVectorDimension().Value(), vec.Dimension().Value());
			}
			return UpdateTagType{TAG_ARRAY, vec.Dimension()};
		}
	}

	if (ctx.updateArrayElements || ctx.value.IsArrayValue()) {
		return UpdateTagType{TAG_ARRAY};
	} else if (ctx.value.IsNullValue() || ctx.value.empty()) {
		return UpdateTagType{TAG_NULL};
	}
	return UpdateTagType{arrayKvType2Tag(ctx.value)};
}

bool CJsonModifier::checkIfFoundTag(Context& ctx, TagType tag, bool isLastItem) const {
	if (tagsPath_.empty() || !Compare<NotIgnoreLeftTrailingIndexes>(fieldPath_, tagsPath_)) {
		return false;
	}

	const auto& backFieldPath = fieldPath_.back();
	if (!backFieldPath.IsTagIndex() || !backFieldPath.GetTagIndex().IsAll() || isLastItem) {
		if (tag != TAG_NULL) {
			// NULL tag has to be reinserted instead of simple update
			ctx.fieldUpdated = true;
		}
	}

	return true;
}

void CJsonModifier::writeCTag(const ctag& tag, Context& ctx) {
	const TagType tagType = tag.Type();
	bool tagMatched = checkIfFoundTag(ctx, tagType);
	const int field = tag.Field();
	const TagName tagName = tag.Name();
	if (tagType == TAG_ARRAY) {
		const auto count = ctx.rdser.GetVarUInt();
		if (!tagMatched || !ctx.fieldUpdated) {
			for (uint64_t i = 0; i < count; ++i) {
				TagsPathScope pathGuard{tagsPath_, TagIndex{i}};
				const bool isLastItem = (i + 1 == count);
				tagMatched = checkIfFoundTag(ctx, TAG_ARRAY, isLastItem);
				if (tagMatched && ctx.fieldUpdated) {
					break;
				}
			}
		}

		if (tagMatched && ctx.fieldUpdated) {
			const auto resultTagType = determineUpdateTagType(ctx, field);
			ctx.wrser.PutCTag(ctag{resultTagType.rawType, tagName, field});
			if (resultTagType.isFloatVectorRef) {
				assertrx_throw(!ctx.updateArrayElements);
				ctx.wrser.PutVarUint(uint32_t(resultTagType.valueDims));
			} else if (resultTagType.rawType == TAG_ARRAY) {
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
	ctx.wrser.PutCTag(ctag{resultTagType.rawType, tagName, field});
	if (resultTagType.rawType == TAG_ARRAY) {
		if (resultTagType.isFloatVectorRef) {
			ctx.wrser.PutVarUint(uint32_t(resultTagType.valueDims));
		} else {
			ctx.wrser.PutVarUint(ctx.value.size());
		}
	}
}

void CJsonModifier::updateArray(TagType atagType, uint32_t count, TagName tagName, Context& ctx) {
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
		tagsPath_.back().GetTagIndexRef() = TagIndex{i};
		const bool isLastItem = (i + 1 == count);
		if (checkIfFoundTag(ctx, TAG_ARRAY, isLastItem)) {
			(atagType == TAG_OBJECT) ? skipCjsonTag(ctag{ctx.rdser.GetCTag().Type()}, ctx.rdser, &ctx.fieldsArrayOffsets)
									 : skipCjsonTag(ctag{atagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
			ctx.wrser.PutCTag(ctag{valueType});
			copyCJsonValue(valueType, value, ctx.wrser);

			ctx.fieldUpdated = true;
			continue;  // next item
		}

		switch (atagType) {
			case TAG_OBJECT: {
				TagsPathScope currPathNameGuard(ctx.currObjPath, tagName);
				TagsPathScope currPathIndexGuard(ctx.currObjPath, TagIndex{i});
				std::ignore = updateFieldInTuple(ctx);
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
			case TAG_FLOAT:
				// array tag type updated (need store as object)
				ctx.wrser.PutCTag(ctag{atagType});
				copyCJsonValue(atagType, ctx.rdser, ctx.wrser, kNoValidation);
				break;
		}
	}

	assertrx_throw(ctx.fieldUpdated);
}

void CJsonModifier::copyArray(TagName tagName, Context& ctx) {
	const carraytag atag = ctx.rdser.GetCArrayTag();
	const TagType atagType = atag.Type();
	const auto count = atag.Count();

	// store position in serializer
	const auto rdserPos = ctx.rdser.Pos();
	const auto wrserLen = ctx.wrser.Len();

	ctx.wrser.PutCArrayTag(atag);

	for (uint32_t i = 0; i < count; i++) {
		TagsPathScope pathIndexGuard{tagsPath_, TagIndex{i}};
		const bool isLastItem = (i + 1 == count);
		// update item
		if (checkIfFoundTag(ctx, TAG_ARRAY, isLastItem)) {
			if (ctx.value.IsArrayValue()) {
				// TODO #2233
				throw Error(errParams, "Unable to update array's element with array-value");
			}
			Variant value;
			if (!ctx.value.empty()) {
				value = ctx.value.front();
			}
			// situation is possible when array was homogeneous, and new element of different type is added
			if (atagType != TAG_OBJECT && (atagType != value.Type().ToTagType() || ctx.value.IsArrayValue())) {
				// back to beginning of array and rewrite as an array of objects
				ctx.rdser.SetPos(rdserPos);
				ctx.wrser.Reset(wrserLen);
				updateArray(atagType, count, tagName, ctx);
				return;	 // array updated - stop processing
			}

			// type of array not changed - simple rewrite item
			updateArrayItem(atagType, value, ctx);
			ctx.fieldUpdated = true;
		} else {
			// copy item as is
			switch (atagType) {
				case TAG_OBJECT: {
					TagsPathScope currPathNameGuard(ctx.currObjPath, tagName);
					TagsPathScope currPathIndexGuard(ctx.currObjPath, TagIndex{i});
					std::ignore = updateFieldInTuple(ctx);
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
				case TAG_FLOAT:
					copyCJsonValue(atagType, ctx.rdser, ctx.wrser, kNoValidation);
					break;
			}
		}
	}
}

void CJsonModifier::updateArrayItem(TagType arrayType, const Variant& newValue, Context& ctx) const {
	const auto newValueType = newValue.Type().ToTagType();
	if (arrayType == TAG_OBJECT) {
		const auto elemTag = ctx.rdser.GetCTag();
		if (isIndexed(elemTag.Field())) {
			if (elemTag.Type() == TAG_ARRAY) {
				std::ignore = ctx.rdser.GetVarUInt();
				if (ctx.value.IsArrayValue()) {
					// TODO #2233
				} else {
					ctx.wrser.PutCTag(ctag{newValueType, TagName::Empty(), elemTag.Field()});
				}
			} else {
				if (ctx.value.IsArrayValue()) {
					// TODO #2233
				} else {
					ctx.wrser.PutCTag(elemTag);
				}
			}
		} else {
			int field = -1;
			if (elemTag.Type() == TAG_ARRAY) {
				field = tagsMatcher_.tags2field(ctx.jsonPath).IndexNumber();
			}
			ctx.wrser.PutCTag(ctag{newValueType, TagName::Empty(), field});
			skipCjsonTag(ctag{elemTag.Type()}, ctx.rdser, &ctx.fieldsArrayOffsets);
			if (field < 0) {
				copyCJsonValue(newValueType, newValue, ctx.wrser);
			}
		}
	} else {
		skipCjsonTag(ctag{arrayType}, ctx.rdser, &ctx.fieldsArrayOffsets);
		copyCJsonValue(newValueType, newValue, ctx.wrser);
	}
}

bool CJsonModifier::updateFieldInTuple(Context& ctx) {
	const ctag tag = ctx.rdser.GetCTag();

	if (tag == kCTagEnd) {
		if (needToInsertField(ctx)) {
			insertField(ctx);
		}
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}
	const TagName tagName = tag.Name();
	const int field = tag.Field();
	const TagType tagType = tag.Type();

	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	if (isIndexed(field)) {
		writeCTag(tag, ctx);
		return true;
	}

	const bool tagMatched = checkIfFoundTag(ctx, tagType);
	if (tagMatched && tagType == TAG_NULL) {
		// Just skip NULL tags - value will be inserted later.
		return true;
	}
	const auto resultTagType = tagMatched ? determineUpdateTagType(ctx, field) : UpdateTagType{tagType};
	ctx.wrser.PutCTag(ctag{resultTagType.rawType, tagName, field});

	if (tagMatched) {
		if (ctx.updateArrayElements && (tagType != TAG_ARRAY || resultTagType.isFloatVectorRef)) {
			throw Error(errParams, "Unable to update scalar value by index");
		}
		if (resultTagType.rawType != TAG_NULL) {
			if (resultTagType.isFloatVectorRef) {
				ctx.wrser.PutCArrayTag(carraytag{uint32_t(resultTagType.valueDims), TAG_FLOAT});
			} else if (resultTagType.rawType == TAG_ARRAY) {
				auto type = arrayKvType2Tag(ctx.value);
				ctx.wrser.PutCArrayTag(carraytag{uint32_t(ctx.value.size()), type});
				const bool isObjsArr = (type == TAG_OBJECT);
				for (const auto& item : ctx.value) {
					if (isObjsArr) {
						type = item.Type().ToTagType();
						ctx.wrser.PutCTag(ctag{type});
					}
					copyCJsonValue(type, item, ctx.wrser);
				}
			} else if (ctx.value.empty()) {
				throw Error(errLogic, "Update value for field [{}] cannot be empty", tagsMatcher_.tag2name(tagName));
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

	if (tagType == TAG_ARRAY) {
		copyArray(tagName, ctx);
	} else {
		copyCJsonValue(tagType, ctx.rdser, ctx.wrser, kNoValidation);
	}
	return true;
}

bool CJsonModifier::dropFieldInTuple(Context& ctx, JustCopy justCopy) {
	const ctag tag = ctx.rdser.GetCTag();
	if (tag == kCTagEnd) {
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}

	const TagName tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = !justCopy && !ctx.fieldUpdated && Compare<NotIgnoreLeftTrailingIndexes>(fieldPath_, tagsPath_);
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
			const auto count = ctx.rdser.GetVarUInt();
			ctx.wrser.PutVarUint(count);
		}
		return true;
	}

	if (tagType == TAG_OBJECT) {
		TagsPathScope pathScopeObj(ctx.currObjPath, tagName);
		while (dropFieldInTuple(ctx, justCopy)) {
		}
		return true;
	}

	if (tagType == TAG_ARRAY) {
		carraytag atag = ctx.rdser.GetCArrayTag();
		const TagType atagType = atag.Type();
		const auto size = atag.Count();
		tagMatched = !justCopy && fieldPath_.back().IsTagIndex() &&
					 (fieldPath_.back().GetTagIndex().IsAll() || fieldPath_.back().GetTagIndex().AsNumber() < size) &&
					 Compare<NotIgnoreTrailingIndexes>({fieldPath_.data(), fieldPath_.size() - 1}, tagsPath_);
		if (tagMatched) {
			atag = carraytag(fieldPath_.back().GetTagIndex().IsAll() ? 0 : size - 1, atagType);
			ctx.fieldUpdated = true;
		}

		ctx.wrser.PutCArrayTag(atag);
		for (size_t i = 0; i < size; ++i) {
			TagsPathScope tagsPathScopeIndex(tagsPath_, TagIndex{i});
			if (tagMatched && fieldPath_.back().GetTagIndex() == TagIndex{i}) {
				if (atagType == TAG_OBJECT) {
					skipCjsonTag(ctx.rdser.GetCTag(), ctx.rdser, &ctx.fieldsArrayOffsets);
				} else {
					skipCjsonTag(ctag{atagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
				}
			} else {
				switch (atagType) {
					case TAG_OBJECT: {
						TagsPathScope currPathNameGuard(ctx.currObjPath, tagName);
						TagsPathScope currPathIndexGuard(ctx.currObjPath, TagIndex{i});
						std::ignore = dropFieldInTuple(ctx, JustCopy{justCopy || tagMatched});
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
					case TAG_FLOAT:
						copyCJsonValue(atagType, ctx.rdser, ctx.wrser, kNoValidation);
						break;
				}
			}
		}
		return true;
	}

	copyCJsonValue(tagType, ctx.rdser, ctx.wrser, kNoValidation);
	return true;
}

void CJsonModifier::embedFieldValue(TagType type, int field, Context& ctx, size_t idx) const {
	if (isIndexed(field)) {
		assertrx(ctx.payload);
		const Variant v = ctx.payload->Get(field, ctx.fieldsArrayOffsets[field] + idx);
		copyCJsonValue(type, v, ctx.wrser);
		return;
	}

	copyCJsonValue(type, ctx.rdser, ctx.wrser, kNoValidation);
}

bool CJsonModifier::buildCJSON(Context& ctx) {
	const auto tag = ctx.rdser.GetCTag();
	if (tag == kCTagEnd) {
		if (needToInsertField(ctx)) {
			insertField(ctx);
		}
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}
	TagType tagType = tag.Type();
	const TagName tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> tagsPathScope(tagsPath_, tagName);

	bool tagMatched = Compare<NotIgnoreLeftTrailingIndexes>(fieldPath_, tagsPath_);
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
		const bool isIndexedField = isIndexed(field);
		bool isFloatVector = isIndexedField && pt_.Field(tag.Field()).Type().Is<KeyValueType::FloatVector>();
		if (isFloatVector) {
			// Embed float vector into CJSON as float array
			const carraytag atag(ctx.rdser.GetVarUInt(), TAG_FLOAT);
			ctx.wrser.PutCArrayTag(atag);
			assertrx_dbg(ctx.fieldsArrayOffsets[field] == 0);
			auto value = ctx.payload->Get(field, ctx.fieldsArrayOffsets[field]);
			const auto view = ConstFloatVectorView(value);
			if (view.IsStripped()) [[unlikely]] {
				throw Error(errLogic, "CJsonModifier: Attempt to serialize stripped vector into CJSON");
			}
			const auto span = view.Span();
			assertrx_dbg(span.size() == atag.Count());
			for (float v : span) {
				ctx.wrser.PutFloat(v);
			}
			ctx.fieldsArrayOffsets[field] += 1;
		} else {
			const carraytag atag{isIndexedField ? carraytag(ctx.rdser.GetVarUInt(), pt_.Field(tag.Field()).Type().ToTagType())
												: ctx.rdser.GetCArrayTag()};
			ctx.wrser.PutCArrayTag(atag);
			const auto arrSize = atag.Count();
			for (size_t i = 0; i < arrSize; ++i) {
				TagsPathScope tagsPathIndexScope(tagsPath_, TagIndex{i});
				tagMatched = Compare<NotIgnoreLeftTrailingIndexes>(fieldPath_, tagsPath_);
				if (tagMatched) {
					updateObject(ctx, TagName::Empty());
					skipCjsonTag(ctx.rdser.GetCTag(), ctx.rdser, &ctx.fieldsArrayOffsets);
					continue;
				}

				switch (atag.Type()) {
					case TAG_OBJECT: {
						TagsPathScope<IndexedTagsPath> pathScopeObj(ctx.currObjPath, tagName);
						std::ignore = buildCJSON(ctx);
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
					case TAG_FLOAT:
						embedFieldValue(atag.Type(), field, ctx, i);
						break;
				}
			}

			if (isIndexed(field)) {
				ctx.fieldsArrayOffsets[field] += arrSize;
			}
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
