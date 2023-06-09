#include "cjsonmodifier.h"
#include "core/keyvalue/p_string.h"
#include "core/type_consts_helpers.h"
#include "jsondecoder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

const std::string_view kWrongFieldsAmountMsg = "Number of fields for update should be > 0";

struct CJsonModifier::Context {
	Context(const IndexedTagsPath &fieldPath, const VariantArray &v, WrSerializer &ser, std::string_view tuple, FieldModifyMode m,
			const Payload *pl = nullptr)
		: value(v), wrser(ser), rdser(tuple), mode(m), payload(pl) {
		for (const IndexedPathNode &node : fieldPath) {
			jsonPath.emplace_back(node.NameTag());
		}
		if (mode == FieldModeSet && fieldPath.back().IsArrayNode() && value.empty()) {
			throw Error(errParams, "Array item should not be an empty value");
		}
		std::fill(std::begin(fieldsArrayOffsets), std::end(fieldsArrayOffsets), 0);
	}
	const VariantArray &value;
	WrSerializer &wrser;
	Serializer rdser;
	TagsPath jsonPath;
	IndexedTagsPath currObjPath;
	FieldModifyMode mode;
	const Payload *payload = nullptr;
	bool fieldUpdated = false;
	std::array<unsigned, maxIndexes> fieldsArrayOffsets;
};

CJsonModifier::CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt) : pt_(std::move(pt)), tagsMatcher_(tagsMatcher) {}

void CJsonModifier::SetFieldValue(std::string_view tuple, IndexedTagsPath fieldPath, const VariantArray &val, WrSerializer &ser) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear();
	Context ctx(fieldPath, val, ser, tuple, FieldModeSet, nullptr);
	fieldPath_ = std::move(fieldPath);
	updateFieldInTuple(ctx);
	if (!ctx.fieldUpdated && !fieldPath_.back().IsForAllItems()) {
		throw Error(errParams, "[SetFieldValue] Requested field or array's index was not found");
	}
}

void CJsonModifier::SetObject(std::string_view tuple, IndexedTagsPath fieldPath, const VariantArray &val, WrSerializer &ser,
							  const Payload *pl) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear();
	Context ctx(fieldPath, val, ser, tuple, FieldModeSetJson, pl);
	fieldPath_ = std::move(fieldPath);
	buildCJSON(ctx);
	if (!ctx.fieldUpdated && !fieldPath_.back().IsForAllItems()) {
		throw Error(errParams, "[SetObject] Requested field or array's index was not found");
	}
}

void CJsonModifier::RemoveField(std::string_view tuple, IndexedTagsPath fieldPath, WrSerializer &wrser) {
	if (fieldPath.empty()) {
		throw Error(errLogic, kWrongFieldsAmountMsg);
	}
	tagsPath_.clear();
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
				int field = tagsMatcher_.tags2field(ctx.jsonPath.data(), fieldPath_.size());
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

bool CJsonModifier::needToInsertField(Context &ctx) {
	if (ctx.fieldUpdated) return false;
	if (fieldPath_.back().IsArrayNode()) return false;
	if (ctx.currObjPath.size() < fieldPath_.size()) {
		bool correctPath = true;
		for (size_t i = 0; i < ctx.currObjPath.size(); ++i) {
			if (fieldPath_[i] != ctx.currObjPath[i]) {
				correctPath = false;
				break;
			}
		}
		if (correctPath) {
			bool containsArrayIndex = false;
			for (auto &node : fieldPath_) {
				if (node.IsArrayNode()) {
					containsArrayIndex = true;
					break;
				}
			}
			return !containsArrayIndex || fieldPath_.size() == ctx.currObjPath.size();
		}
		return false;
	}
	return false;
}

TagType CJsonModifier::determineUpdateTagType(const Context &ctx, int field) {
	if (field != -1) {
		const PayloadFieldType &fieldType = pt_.Field(field);
		if (ctx.value.size() > 0 && !fieldType.Type().IsSame(ctx.value.front().Type())) {
			throw Error(errParams, "Inserted field %s type [%s] doesn't match it's index type [%s]", fieldType.Name(),
						TagTypeToStr(kvType2Tag(ctx.value.front().Type())), TagTypeToStr(kvType2Tag(fieldType.Type())));
		}
	}
	if (ctx.value.IsArrayValue()) {
		return TAG_ARRAY;
	} else if (ctx.value.empty()) {
		return TAG_NULL;
	} else {
		return kvType2Tag(ctx.value.front().Type());
	}
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

	TagType tagType = tag.Type();
	if (tagType == TAG_END) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}

	int field = tag.Field();
	int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = checkIfFoundTag(ctx);
	if (tagMatched && field < 0) {
		tagType = determineUpdateTagType(ctx);
	}

	ctx.wrser.PutCTag(ctag{tagType, tagName, field});

	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			auto count = ctx.rdser.GetVarUint();
			if (tagMatched) {
				count = (ctx.value.empty() || ctx.value.IsNullValue()) ? 0 : ctx.value.size();
			}
			ctx.wrser.PutVarUint(count);
		}
	} else {
		if (tagType == TAG_OBJECT) {
			TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
			while (updateFieldInTuple(ctx)) {
			}
		} else if (tagType == TAG_ARRAY) {
			if (tagMatched) {
				skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
				ctx.wrser.PutCArrayTag(carraytag{ctx.value.size(), ctx.value.ArrayType().ToTagType()});
				for (size_t i = 0; i < ctx.value.size(); ++i) {
					updateField(ctx, i);
				}
			} else {
				const carraytag atag = ctx.rdser.GetCArrayTag();
				ctx.wrser.PutCArrayTag(atag);
				const TagType atagType = atag.Type();
				const auto count = atag.Count();
				for (size_t i = 0; i < count; ++i) {
					tagsPath_.back().SetIndex(i);
					const bool isLastItem = (i == count - 1);
					tagMatched = checkIfFoundTag(ctx, isLastItem);
					if (tagMatched) {
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
			}
		} else {
			if (tagMatched) {
				if (tagType != TAG_NULL) {
					if (ctx.value.empty()) {
						throw Error(errLogic, "Update value for field [%s] cannot be empty", tagsMatcher_.tag2name(tagName));
					}
					updateField(ctx, 0);
				}
				skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
			} else {
				copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
			}
		}
	}

	return true;
}

bool CJsonModifier::dropFieldInTuple(Context &ctx) {
	const ctag tag = ctx.rdser.GetCTag();
	const TagType tagType = tag.Type();
	if (tagType == TAG_END) {
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}

	int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = (!ctx.fieldUpdated && fieldPath_.Compare(tagsPath_));
	if (tagMatched) {
		skipCjsonTag(tag, ctx.rdser, &ctx.fieldsArrayOffsets);
		ctx.fieldUpdated = true;
		return true;
	}

	int field = tag.Field();
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
	TagType tagType = tag.Type();
	if (tagType == TAG_END) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutCTag(kCTagEnd);
		return false;
	}

	const int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	const auto field = tag.Field();
	bool embeddedField = (field < 0);
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
