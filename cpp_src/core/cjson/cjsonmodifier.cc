
#include "cjsonmodifier.h"
#include "core/keyvalue/p_string.h"
#include "jsondecoder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

string_view kWrongFieldsAmountMsg = "Number of fields for update should be > 0";

struct CJsonModifier::Context {
	Context(const VariantArray &v, WrSerializer &ser, string_view tuple, FieldModifyMode m, const Payload *pl = nullptr)
		: value(v), wrser(ser), rdser(tuple), mode(m), payload(pl) {}
	bool isModeSet() const { return mode == FieldModeSet; }
	bool isModeSetJson() const { return mode == FieldModeSetJson; }
	bool isModeDrop() const { return mode == FieldModeDrop; }
	bool dropThisTag(bool tagMatched) const { return tagMatched && isModeDrop(); }
	bool updateThisTag(bool tagMatched) const { return tagMatched && isModeSet(); }
	const VariantArray &value;
	WrSerializer &wrser;
	Serializer rdser;
	TagsPath currObjPath;
	bool fieldUpdated = false;
	FieldModifyMode mode;
	const Payload *payload = nullptr;
};

CJsonModifier::CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt) : pt_(pt), tagsMatcher_(tagsMatcher) {}

Error CJsonModifier::SetFieldValue(string_view tuple, TagsPath path, const VariantArray &val, WrSerializer &ser) {
	if (path.empty()) return Error(errLogic, kWrongFieldsAmountMsg);
	try {
		fieldPath_ = std::move(path);
		tagsPath_.clear();
		Context ctx(val, ser, tuple, FieldModeSet, nullptr);
		buildTuple(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error CJsonModifier::SetObject(string_view tuple, TagsPath path, const VariantArray &val, WrSerializer &ser, const Payload *pl) {
	if (path.empty()) return Error(errLogic, kWrongFieldsAmountMsg);
	try {
		fieldPath_ = std::move(path);
		tagsPath_.clear();
		Context ctx(val, ser, tuple, FieldModeSetJson, pl);
		buildCJSON(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error CJsonModifier::RemoveField(string_view tuple, TagsPath fieldPath, WrSerializer &wrser) {
	try {
		fieldPath_ = std::move(fieldPath);
		tagsPath_.clear();
		Context ctx({}, wrser, tuple, FieldModeDrop);
		buildTuple(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void CJsonModifier::copyValue(int type, int field, Context &ctx, size_t idx) {
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

void CJsonModifier::updateObject(Context &ctx, int tagName) {
	JsonDecoder jsonDecoder(tagsMatcher_);
	if (ctx.value.IsArrayValue()) {
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeArray, &tagsMatcher_, tagName);
		for (size_t i = 0; i < ctx.value.size(); ++i) {
			CJsonBuilder objBuilder = cjsonBuilder.Object(nullptr);
			jsonDecoder.Decode(string_view(ctx.value[i]), objBuilder, fieldPath_);
		}
	} else {
		assert(ctx.value.size() == 1);
		CJsonBuilder cjsonBuilder(ctx.wrser, ObjType::TypeObject, &tagsMatcher_, tagName);
		jsonDecoder.Decode(string_view(ctx.value.front()), cjsonBuilder, fieldPath_);
	}
}

void CJsonModifier::updateField(Context &ctx, size_t idx) {
	if (ctx.isModeSet()) {
		assert(idx < ctx.value.size());
		copyCJsonValue(kvType2Tag(ctx.value[idx].Type()), ctx.value[idx], ctx.wrser);
	}
}

void CJsonModifier::putNewField(Context &ctx) {
	ctx.fieldUpdated = true;
	if (ctx.isModeDrop()) return;

	assert(ctx.currObjPath.size() < fieldPath_.size());

	int nestedObjects = 0;
	for (size_t i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
		bool finalTag = (i == fieldPath_.size() - 1);
		int tagName = fieldPath_[i];
		if (finalTag) {
			if (ctx.isModeSetJson()) {
				updateObject(ctx, tagName);
			} else {
				int field = tagsMatcher_.tags2field(fieldPath_.data(), fieldPath_.size());
				putCJsonValue(determineUpdateTagType(ctx, field), tagName, ctx.value, ctx.wrser);
			}
		} else {
			ctx.wrser.PutVarUint(static_cast<int>(ctag(TAG_OBJECT, tagName)));
			++nestedObjects;
		}
	}
	while (nestedObjects-- > 0) ctx.wrser.PutVarUint(TAG_END);
	ctx.currObjPath.clear();
}

bool CJsonModifier::checkIfPathCorrect(Context &ctx) {
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
			throw Error(errParams, "Inserted field type doesn't match it's index type: %s",
						ctag(kvType2Tag(ctx.value.front().Type())).TypeName());
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

bool CJsonModifier::buildTuple(Context &ctx) {
	ctag tag = ctx.rdser.GetVarUint();
	int tagType = tag.Type();

	if (tagType == TAG_END) {
		if (!ctx.fieldUpdated) {
			if (checkIfPathCorrect(ctx)) putNewField(ctx);
		}
		ctx.wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();
	if (tagName) {
		(void)tagsMatcher_.tag2name(tagName);
		tagsPath_.push_back(tagName);
	}

	bool tagMatched = (fieldPath_ == tagsPath_);
	int field = tag.Field();

	if (!ctx.dropThisTag(tagMatched)) {
		if (tagMatched && field < 0) tagType = determineUpdateTagType(ctx);
		ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));
	}
	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			int count = ctx.rdser.GetVarUint();
			if (tagMatched) {
				ctx.fieldUpdated = true;
				count = ctx.value.IsNullValue() ? 0 : ctx.value.size();
			}
			if (!ctx.dropThisTag(tagMatched)) {
				ctx.wrser.PutVarUint(count);
			}
		}
	} else if (field < 0) {
		if (tagType == TAG_OBJECT) {
			if (ctx.dropThisTag(tagMatched)) {
				skipCjsonTag(tag, ctx.rdser);
			} else {
				bool nestedObject = (!ctx.fieldUpdated && tagName > 0);
				if (nestedObject) ctx.currObjPath.push_back(tagName);
				while (buildTuple(ctx)) {
				}
				if (nestedObject && !ctx.currObjPath.empty()) ctx.currObjPath.pop_back();
			}
		} else if (tagType == TAG_ARRAY) {
			if (tagMatched) {
				ctx.fieldUpdated = true;
				skipCjsonTag(tag, ctx.rdser);
				if (!ctx.dropThisTag(tagMatched)) {
					ctx.wrser.PutUInt32(int(carraytag(ctx.value.size(), ctx.value.ArrayType())));
					for (size_t i = 0; i < ctx.value.size(); ++i) {
						updateField(ctx, i);
					}
				}
			} else {
				carraytag atag = ctx.rdser.GetUInt32();
				ctx.wrser.PutUInt32(static_cast<int>(atag));
				for (int i = 0; i < atag.Count(); i++) {
					switch (atag.Tag()) {
						case TAG_OBJECT:
							ctx.currObjPath.push_back(tagName);
							buildTuple(ctx);
							ctx.currObjPath.pop_back();
							break;
						default:
							copyCJsonValue(atag.Tag(), ctx.rdser, ctx.wrser);
							break;
					}
				}
			}
		} else {
			if (tagMatched) {
				if (!ctx.dropThisTag(tagMatched) && ctx.value.empty()) throw Error(errLogic, "Update value cannot be empty");
				updateField(ctx, 0);
				skipCjsonTag(tag, ctx.rdser);
				ctx.fieldUpdated = true;
			} else {
				copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
			}
		}
	}

	if (tagName) tagsPath_.pop_back();
	return true;
}

bool CJsonModifier::buildCJSON(Context &ctx) {
	ctag tag = ctx.rdser.GetVarUint();
	int tagType = tag.Type();
	if (tagType == TAG_END) {
		if (!ctx.fieldUpdated) {
			if (checkIfPathCorrect(ctx)) putNewField(ctx);
		}
		ctx.wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();
	if (tagName) {
		(void)tagsMatcher_.tag2name(tagName);
		tagsPath_.push_back(tagName);
	}

	bool embeddedField = (tag.Field() < 0);
	bool tagMatched = (fieldPath_ == tagsPath_);
	if (!tagMatched) ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName)));
	if (tagMatched) tagType = TAG_OBJECT;

	if (tagType == TAG_OBJECT) {
		if (tagMatched) {
			skipCjsonTag(tag, ctx.rdser);
			updateObject(ctx, tagName);
			ctx.fieldUpdated = true;
		} else {
			bool nestedObject = (!ctx.fieldUpdated && tagName > 0);
			if (nestedObject) ctx.currObjPath.push_back(tagName);
			while (buildCJSON(ctx)) {
			}
			if (nestedObject && !ctx.currObjPath.empty()) ctx.currObjPath.pop_back();
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
			switch (atag.Tag()) {
				case TAG_OBJECT:
					ctx.currObjPath.push_back(tagName);
					buildCJSON(ctx);
					ctx.currObjPath.pop_back();
					break;
				default:
					copyValue(atag.Tag(), tag.Field(), ctx, i);
					break;
			}
		}
	} else {
		copyValue(tagType, tag.Field(), ctx, 0);
	}

	if (tagName) tagsPath_.pop_back();
	return true;
}

}  // namespace reindexer
