
#include "cjsonmodifier.h"
#include "core/keyvalue/p_string.h"
#include "jsondecoder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

const string_view kWrongFieldsAmountMsg = "Number of fields for update should be > 0";

struct CJsonModifier::Context {
	Context(const IndexedTagsPath &fieldPath, const VariantArray &v, WrSerializer &ser, string_view tuple, FieldModifyMode m,
			const Payload *pl = nullptr)
		: value(v), wrser(ser), rdser(tuple), mode(m), payload(pl) {
		for (const IndexedPathNode &node : fieldPath) {
			jsonPath.emplace_back(node.NameTag());
		}
		if (mode == FieldModeSet && fieldPath.back().IsArrayNode() && value.empty()) {
			throw Error(errParams, "Array item should not be an empty value");
		}
	}
	const VariantArray &value;
	WrSerializer &wrser;
	Serializer rdser;
	TagsPath jsonPath;
	IndexedTagsPath currObjPath;
	FieldModifyMode mode;
	const Payload *payload = nullptr;
	bool fieldUpdated = false;
};

CJsonModifier::CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt) : pt_(pt), tagsMatcher_(tagsMatcher) {}

Error CJsonModifier::SetFieldValue(string_view tuple, IndexedTagsPath fieldPath, const VariantArray &val, WrSerializer &ser) {
	if (fieldPath.empty()) {
		return Error(errLogic, kWrongFieldsAmountMsg);
	}
	try {
		tagsPath_.clear();
		Context ctx(fieldPath, val, ser, tuple, FieldModeSet, nullptr);
		fieldPath_ = std::move(fieldPath);
		updateFieldInTuple(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error CJsonModifier::SetObject(string_view tuple, IndexedTagsPath fieldPath, const VariantArray &val, WrSerializer &ser,
							   const Payload *pl) {
	if (fieldPath.empty()) {
		return Error(errLogic, kWrongFieldsAmountMsg);
	}
	try {
		tagsPath_.clear();
		Context ctx(fieldPath, val, ser, tuple, FieldModeSetJson, pl);
		fieldPath_ = std::move(fieldPath);
		buildCJSON(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error CJsonModifier::RemoveField(string_view tuple, IndexedTagsPath fieldPath, WrSerializer &wrser) {
	if (fieldPath.empty()) {
		return Error(errLogic, kWrongFieldsAmountMsg);
	}
	try {
		tagsPath_.clear();
		Context ctx(fieldPath, {}, wrser, tuple, FieldModeDrop);
		fieldPath_ = std::move(fieldPath);
		dropFieldInTuple(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void CJsonModifier::updateObject(Context &ctx, int tagName) {
	JsonDecoder jsonDecoder(tagsMatcher_);
	if (ctx.value.IsArrayValue()) {
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeArray, &tagsMatcher_, tagName);
		for (size_t i = 0; i < ctx.value.size(); ++i) {
			CJsonBuilder objBuilder = cjsonBuilder.Object(nullptr);
			jsonDecoder.Decode(string_view(ctx.value[i]), objBuilder, ctx.jsonPath);
		}
	} else {
		assert(ctx.value.size() == 1);
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeObject, &tagsMatcher_, tagName);
		jsonDecoder.Decode(string_view(ctx.value.front()), cjsonBuilder, ctx.jsonPath);
	}
	ctx.fieldUpdated = true;
}

void CJsonModifier::updateField(Context &ctx, size_t idx) {
	assert(idx < ctx.value.size());
	copyCJsonValue(kvType2Tag(ctx.value[idx].Type()), ctx.value[idx], ctx.wrser);
}

void CJsonModifier::insertField(Context &ctx) {
	ctx.fieldUpdated = true;
	assert(ctx.currObjPath.size() < fieldPath_.size());

	int nestedObjects = 0;
	for (size_t i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		int tagName = fieldPath_[i].NameTag();
		bool finalTag = (i == fieldPath_.size() - 1);
		if (finalTag) {
			if (ctx.mode == FieldModeSetJson) {
				updateObject(ctx, tagName);
			} else {
				int field = tagsMatcher_.tags2field(ctx.jsonPath.data(), fieldPath_.size());
				int tagType = determineUpdateTagType(ctx, field);
				if (field > 0) {
					putCJsonRef(tagType, tagName, field, ctx.value, ctx.wrser);
				} else {
					putCJsonValue(tagType, tagName, ctx.value, ctx.wrser);
				}
			}
		} else {
			ctx.wrser.PutVarUint(static_cast<int>(ctag(TAG_OBJECT, tagName)));
			++nestedObjects;
		}
	}

	while (nestedObjects-- > 0) ctx.wrser.PutVarUint(TAG_END);
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
		return correctPath;
	}
	return false;
}

int CJsonModifier::determineUpdateTagType(const Context &ctx, int field) {
	if (field != -1) {
		const PayloadFieldType &fieldType = pt_.Field(field);
		if (ctx.value.size() > 0 && fieldType.Type() != ctx.value.front().Type()) {
			throw Error(errParams, "Inserted field %s type [%s] doesn't match it's index type [%s]", fieldType.Name(),
						ctag(kvType2Tag(ctx.value.front().Type())).TypeName(), ctag(kvType2Tag(fieldType.Type())).TypeName());
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
	ctag tag = ctx.rdser.GetVarUint();

	int tagType = tag.Type();
	if (tagType == TAG_END) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutVarUint(TAG_END);
		return false;
	}

	int field = tag.Field();
	int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = checkIfFoundTag(ctx);
	if (tagMatched && field < 0) {
		tagType = determineUpdateTagType(ctx);
	}

	ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));

	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			int count = ctx.rdser.GetVarUint();
			if (tagMatched) {
				count = ctx.value.IsNullValue() ? 0 : ctx.value.size();
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
				skipCjsonTag(tag, ctx.rdser);
				ctx.wrser.PutUInt32(int(carraytag(ctx.value.size(), ctx.value.ArrayType())));
				for (size_t i = 0; i < ctx.value.size(); ++i) {
					updateField(ctx, i);
				}
			} else {
				carraytag atag = ctx.rdser.GetUInt32();
				ctx.wrser.PutUInt32(static_cast<int>(atag));
				for (int i = 0; i < atag.Count(); i++) {
					tagsPath_.back().SetIndex(i);
					bool isLastItem = (i == atag.Count() - 1);
					tagMatched = checkIfFoundTag(ctx, isLastItem);
					if (tagMatched) {
						copyCJsonValue(atag.Tag(), ctx.value.front(), ctx.wrser);
						skipCjsonTag(atag.Tag(), ctx.rdser);
					} else {
						switch (atag.Tag()) {
							case TAG_OBJECT: {
								TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, IndexedPathNode(tagName, i));
								updateFieldInTuple(ctx);
								break;
							}
							default:
								copyCJsonValue(atag.Tag(), ctx.rdser, ctx.wrser);
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
				skipCjsonTag(tag, ctx.rdser);
			} else {
				copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
			}
		}
	}

	return true;
}

bool CJsonModifier::dropFieldInTuple(Context &ctx) {
	ctag tag = ctx.rdser.GetVarUint();
	int tagType = tag.Type();
	if (tagType == TAG_END) {
		ctx.wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool tagMatched = (!ctx.fieldUpdated && fieldPath_.Compare(tagsPath_));
	if (tagMatched) {
		skipCjsonTag(tag, ctx.rdser);
		ctx.fieldUpdated = true;
		return true;
	}

	int field = tag.Field();
	ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));

	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			int count = ctx.rdser.GetVarUint();
			ctx.wrser.PutVarUint(count);
		}
	} else {
		if (tagType == TAG_OBJECT) {
			TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
			while (dropFieldInTuple(ctx)) {
			}
		} else if (tagType == TAG_ARRAY) {
			carraytag atag = ctx.rdser.GetUInt32();
			int size = atag.Count();
			tagMatched = (fieldPath_.back().IsArrayNode() && tagsPath_ == fieldPath_);
			if (tagMatched) {
				atag = carraytag(fieldPath_.back().IsForAllItems() ? 0 : atag.Count() - 1, atag.Tag());
				ctx.fieldUpdated = true;
			}
			ctx.wrser.PutUInt32(static_cast<int>(atag));
			for (int i = 0; i < size; i++) {
				tagsPath_.back().SetIndex(i);
				if (tagMatched && (i == fieldPath_.back().Index() || fieldPath_.back().IsForAllItems())) {
					skipCjsonTag(atag.Tag(), ctx.rdser);
				} else {
					switch (atag.Tag()) {
						case TAG_OBJECT: {
							TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, IndexedPathNode(tagName, i));
							dropFieldInTuple(ctx);
							break;
						}
						default:
							copyCJsonValue(atag.Tag(), ctx.rdser, ctx.wrser);
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

void CJsonModifier::embedFieldValue(int type, int field, Context &ctx, size_t idx) {
	if (field < 0) {
		copyCJsonValue(type, ctx.rdser, ctx.wrser);
	} else {
		assert(ctx.payload);
		VariantArray v;
		ctx.payload->Get(field, v);
		assert(idx < v.size());
		copyCJsonValue(type, v[idx], ctx.wrser);
	}
}

bool CJsonModifier::buildCJSON(Context &ctx) {
	ctag tag = ctx.rdser.GetVarUint();
	int tagType = tag.Type();
	if (tagType == TAG_END) {
		if (needToInsertField(ctx)) insertField(ctx);
		ctx.wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();
	TagsPathScope<IndexedTagsPath> pathScope(tagsPath_, tagName);

	bool embeddedField = (tag.Field() < 0);
	bool tagMatched = fieldPath_.Compare(tagsPath_);
	if (!tagMatched) ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName)));
	if (tagMatched) tagType = TAG_OBJECT;

	if (tagType == TAG_OBJECT) {
		if (tagMatched) {
			skipCjsonTag(tag, ctx.rdser);
			updateObject(ctx, tagName);
		} else {
			TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
			while (buildCJSON(ctx)) {
			}
		}
	} else if (tagType == TAG_ARRAY) {
		carraytag atag(0);
		if (embeddedField) {
			atag = ctx.rdser.GetUInt32();
		} else {
			uint64_t count = ctx.rdser.GetVarUint();
			KeyValueType kvType = pt_.Field(tag.Field()).Type();
			atag = carraytag(count, kvType2Tag(kvType));
		}
		ctx.wrser.PutUInt32(static_cast<int>(atag));
		for (int i = 0; i < atag.Count(); i++) {
			tagsPath_.back().SetIndex(i);
			tagMatched = fieldPath_.Compare(tagsPath_);
			if (tagMatched) {
				updateObject(ctx, 0);
				skipCjsonTag(ctx.rdser.GetVarUint(), ctx.rdser);
			} else {
				switch (atag.Tag()) {
					case TAG_OBJECT: {
						TagsPathScope<IndexedTagsPath> pathScope(ctx.currObjPath, tagName);
						buildCJSON(ctx);
						break;
					}
					default:
						embedFieldValue(atag.Tag(), tag.Field(), ctx, i);
						break;
				}
			}
		}
	} else {
		embedFieldValue(tagType, tag.Field(), ctx, 0);
	}

	return true;
}

}  // namespace reindexer
