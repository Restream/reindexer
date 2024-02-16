#include "cjsonmodifier.h"
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

		std::fill(std::begin(fieldsArrayOffsets), std::end(fieldsArrayOffsets), 0);
	}
	bool IsForAllItems() const noexcept { return isForAllItems_; }

	const VariantArray &value;
	WrSerializer &wrser;
	Serializer rdser;
	TagsPath jsonPath;
	IndexedTagsPath currObjPath;
	FieldModifyMode mode;
	bool fieldUpdated = false;
	bool updateArrayElements = false;
	const Payload *payload = nullptr;
	std::array<unsigned, kMaxIndexes> fieldsArrayOffsets;

private:
	bool isForAllItems_ = false;
};

void CJsonModifier::SetFieldValue(std::string_view tuple, IndexedTagsPath fieldPath, const VariantArray &val, WrSerializer &ser,
								  const Payload &pl) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear<false>();
	Context ctx(fieldPath, val, ser, tuple, FieldModeSet, &pl);
	fieldPath_ = std::move(fieldPath);
	updateFieldInTuple(ctx);
	if (!ctx.fieldUpdated && !ctx.IsForAllItems()) {
		throw Error(errParams, "[SetFieldValue] Requested field or array's index was not found");
	}
}

void CJsonModifier::SetObject(std::string_view tuple, IndexedTagsPath fieldPath, const VariantArray &val, WrSerializer &ser,
							  const Payload &pl) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear<false>();
	Context ctx(fieldPath, val, ser, tuple, FieldModeSetJson, &pl);
	fieldPath_ = std::move(fieldPath);
	buildCJSON(ctx);
	if (!ctx.fieldUpdated && !ctx.IsForAllItems()) {
		throw Error(errParams, "[SetObject] Requested field or array's index was not found");
	}
}

void CJsonModifier::RemoveField(std::string_view tuple, IndexedTagsPath fieldPath, WrSerializer &wrser) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear<false>();
	Context ctx(fieldPath, {}, wrser, tuple, FieldModeDrop);
	fieldPath_ = std::move(fieldPath);
	dropFieldInTuple(ctx);
}

void CJsonModifier::updateObject(Context &ctx, int tagName) {
	JsonDecoder jsonDecoder(tagsMatcher_);
	if (ctx.value.IsArrayValue()) {
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeArray, &tagsMatcher_, tagName);
		for (size_t i = 0; i < ctx.value.size(); ++i) {
			auto objBuilder = cjsonBuilder.Object(nullptr);
			jsonDecoder.Decode(std::string_view(ctx.value[i]), objBuilder, ctx.jsonPath);
		}
	} else {
		assertrx(ctx.value.size() == 1);
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeObject, &tagsMatcher_, tagName);
		jsonDecoder.Decode(std::string_view(ctx.value.front()), cjsonBuilder, ctx.jsonPath);
	}
	ctx.fieldUpdated = true;
}

void CJsonModifier::updateField(Context &ctx, size_t idx) {
	assertrx(idx < ctx.value.size());
	copyCJsonValue(kvType2Tag(ctx.value[idx].Type()), ctx.value[idx], ctx.wrser);
}

void CJsonModifier::insertField(Context &ctx) {
	ctx.fieldUpdated = true;
	assertrx(ctx.currObjPath.size() < fieldPath_.size());

	int nestedObjects = 0;
	for (size_t i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		int tagName = fieldPath_[i].NameTag();
		const bool finalTag = (i == fieldPath_.size() - 1);
		if (finalTag) {
			if (ctx.mode == FieldModeSetJson) {
				updateObject(ctx, tagName);
			} else {
				const int field = tagsMatcher_.tags2field(ctx.jsonPath.data(), fieldPath_.size());
				const TagType tagType = determineUpdateTagType(ctx, field);
				if (field > 0) {
					putCJsonRef(tagType, tagName, field, ctx.value, ctx.wrser);
				} else {
					putCJsonValue(tagType, tagName, ctx.value, ctx.wrser);
				}
			}
		} else {
			ctx.wrser.PutCTag(ctag{TAG_OBJECT, tagName});
			++nestedObjects;
		}
	}

	while (nestedObjects-- > 0) ctx.wrser.PutCTag(kCTagEnd);
	ctx.currObjPath.clear();
}

bool CJsonModifier::needToInsertField(const Context &ctx) {
	if (ctx.fieldUpdated) return false;
	if (fieldPath_.back().IsArrayNode()) return false;
	if (ctx.currObjPath.size() < fieldPath_.size()) {
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
	return false;
}

TagType CJsonModifier::determineUpdateTagType(const Context &ctx, int field) {
	if (field >= 0) {
		const PayloadFieldType &fieldType = pt_.Field(field);
		if (!fieldType.IsArray() || ctx.updateArrayElements || !ctx.value.IsNullValue()) {
			for (auto &v : ctx.value) {
				if (!fieldType.Type().IsSame(v.Type())) {
					throw Error(errParams, "Inserted field %s type [%s] doesn't match it's index type [%s]", fieldType.Name(),
								v.Type().Name(), fieldType.Type().Name());
				}
			}
		}
	} else if (ctx.value.size() > 1) {
		const auto type = kvType2Tag(ctx.value.front().Type());
		for (auto it = ctx.value.begin() + 1, end = ctx.value.end(); it != end; ++it) {
			if (type != kvType2Tag(it->Type())) {
				throw Error(errParams, "Unable to update field with heterogeneous array. Type[0] is [%s] and type[%d] is [%s]",
							TagTypeToStr(type), it - ctx.value.begin(), TagTypeToStr(kvType2Tag(it->Type())));
			}
		}
	}

	if (ctx.updateArrayElements || ctx.value.IsArrayValue()) {
		return TAG_ARRAY;
	} else if (ctx.value.IsNullValue() || ctx.value.empty()) {
		return TAG_NULL;
	}
	return kvType2Tag(ctx.value.front().Type());
}

bool CJsonModifier::checkIfFoundTag(Context &ctx, bool isLastItem) {
	if (tagsPath_.empty()) return false;
	bool result = fieldPath_.Compare(tagsPath_);
	if (result) {
		if (fieldPath_.back().IsArrayNode()) {
			if (fieldPath_.back().IsForAllItems()) {
				if (isLastItem) ctx.fieldUpdated = true;
			} else {
				ctx.fieldUpdated = true;
			}
		} else {
			ctx.fieldUpdated = true;
		}
	}
	return result;
}

bool CJsonModifier::updateFieldInTuple(Context &ctx) {
	const ctag tag = ctx.rdser.GetCTag();

	if (tag == kCTagEnd) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}
	TagType tagType = tag.Type();
	const int field = tag.Field();
	const int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = checkIfFoundTag(ctx);
	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			const int count = ctx.rdser.GetVarUint();
			if (!tagMatched || !ctx.fieldUpdated) {
				auto &lastTag = tagsPath_.back();
				for (int i = 0; i < count; ++i) {
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
					if (ctx.updateArrayElements) {
						ctx.wrser.PutVarUint(count);
					} else {
						ctx.wrser.PutVarUint(ctx.value.size());
					}
				}
			} else {
				ctx.wrser.PutCTag(ctag{tagType, tagName, field});
				ctx.wrser.PutVarUint(count);
			}
		} else {
			if (tagMatched) {
				if (ctx.updateArrayElements) {
					throw Error(errParams, "Unable to update scalar value by index");
				}
				const auto resultTagType = determineUpdateTagType(ctx, field);
				ctx.wrser.PutCTag(ctag{resultTagType, tagName, field});

				if (resultTagType == TAG_ARRAY) {
					ctx.wrser.PutVarUint(ctx.value.size());
				}
			} else {
				ctx.wrser.PutCTag(ctag{tagType, tagName, field});
			}
		}
	} else {
		const auto resultTagType = tagMatched ? determineUpdateTagType(ctx, field) : tagType;
		ctx.wrser.PutCTag(ctag{resultTagType, tagName, field});
		if (tagMatched) {
			if (ctx.updateArrayElements && tagType != TAG_ARRAY) {
				throw Error(errParams, "Unable to update scalar value by index");
			}
			if (resultTagType != TAG_NULL) {
				if (resultTagType == TAG_ARRAY) {
					ctx.wrser.PutCArrayTag(carraytag{ctx.value.size(), kvType2Tag(ctx.value.ArrayType())});
				} else if (ctx.value.empty()) {
					throw Error(errLogic, "Update value for field [%s] cannot be empty", tagsMatcher_.tag2name(tagName));
				}
				for (size_t i = 0, size = ctx.value.size(); i < size; ++i) {
					updateField(ctx, i);
				}
			}
			skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
		} else if (tagType == TAG_OBJECT) {
			TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
			while (updateFieldInTuple(ctx)) {
			}
		} else if (tagType == TAG_ARRAY) {
			const carraytag atag = ctx.rdser.GetCArrayTag();
			ctx.wrser.PutCArrayTag(atag);
			const TagType atagType = atag.Type();
			const auto count = atag.Count();
			for (unsigned i = 0; i < count; i++) {
				tagsPath_.back().SetIndex(i);
				const bool isLastItem = (i + 1 == atag.Count());
				if (checkIfFoundTag(ctx, isLastItem)) {
					if (ctx.value.IsArrayValue()) {
						throw Error(errParams, "Unable to update non-indexed array's element with array-value");
					}
					copyCJsonValue(atagType, ctx.value.front(), ctx.wrser);
					skipCjsonTag(ctag{atagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
				} else {
					switch (atagType) {
						case TAG_OBJECT: {
							TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName, i);
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
		} else {
			copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
		}
	}

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

	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			const auto count = ctx.rdser.GetVarUint();
			ctx.wrser.PutVarUint(count);
		}
	} else {
		if (tagType == TAG_OBJECT) {
			TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
			while (dropFieldInTuple(ctx)) {
			}
		} else if (tagType == TAG_ARRAY) {
			carraytag atag = ctx.rdser.GetCArrayTag();
			const TagType atagType = atag.Type();
			const int size = atag.Count();
			tagMatched = (fieldPath_.back().IsArrayNode() && tagsPath_ == fieldPath_);
			if (tagMatched) {
				atag = carraytag(fieldPath_.back().IsForAllItems() ? 0 : size - 1, atagType);
				ctx.fieldUpdated = true;
			}
			ctx.wrser.PutCArrayTag(atag);
			for (int i = 0; i < size; i++) {
				tagsPath_.back().SetIndex(i);
				if (tagMatched && (i == fieldPath_.back().Index() || fieldPath_.back().IsForAllItems())) {
					skipCjsonTag(ctag{atagType}, ctx.rdser, &ctx.fieldsArrayOffsets);
				} else {
					switch (atagType) {
						case TAG_OBJECT: {
							TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName, i);
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
			}
		} else {
			copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
		}
	}

	return true;
}

void CJsonModifier::embedFieldValue(TagType type, int field, Context &ctx, size_t idx) {
	if (field < 0) {
		copyCJsonValue(type, ctx.rdser, ctx.wrser);
	} else {
		assertrx(ctx.payload);
		Variant v = ctx.payload->Get(field, ctx.fieldsArrayOffsets[field] + idx);
		copyCJsonValue(type, v, ctx.wrser);
	}
}

bool CJsonModifier::buildCJSON(Context &ctx) {
	const ctag tag = ctx.rdser.GetCTag();
	if (tag == kCTagEnd) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}
	TagType tagType = tag.Type();
	const int tagName = tag.Name();
	const auto field = tag.Field();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	const bool embeddedField = (field < 0);
	bool tagMatched = fieldPath_.Compare(tagsPath_);
	if (!tagMatched) {
		ctx.wrser.PutCTag(ctag{tagType, tagName});
	} else {
		tagType = TAG_OBJECT;
	}

	if (tagType == TAG_OBJECT) {
		if (tagMatched) {
			skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
			updateObject(ctx, tagName);
		} else {
			TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
			while (buildCJSON(ctx)) {
			}
		}
	} else if (tagType == TAG_ARRAY) {
		const carraytag atag{embeddedField ? ctx.rdser.GetCArrayTag()
										   : carraytag(ctx.rdser.GetVarUint(), kvType2Tag(pt_.Field(tag.Field()).Type()))};
		ctx.wrser.PutCArrayTag(atag);
		const auto arrSize = atag.Count();
		for (size_t i = 0; i < arrSize; ++i) {
			tagsPath_.back().SetIndex(i);
			tagMatched = fieldPath_.Compare(tagsPath_);
			if (tagMatched) {
				updateObject(ctx, 0);
				skipCjsonTag(ctx.rdser.GetCTag(), ctx.rdser, &ctx.fieldsArrayOffsets);
			} else {
				switch (atag.Type()) {
					case TAG_OBJECT: {
						TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
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
		}
		if (field >= 0) {
			ctx.fieldsArrayOffsets[field] += arrSize;
		}
	} else {
		embedFieldValue(tagType, field, ctx, 0);
		if (field >= 0) {
			ctx.fieldsArrayOffsets[field] += 1;
		}
	}

	return true;
}

}  // namespace reindexer
