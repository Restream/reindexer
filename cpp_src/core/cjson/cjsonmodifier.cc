
#include "cjsonmodifier.h"
#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

CJsonModifier::Context::Context(const VariantArray &v, WrSerializer &ser, string_view tuple, FieldModifyMode m)
	: value(v), wrser(ser), rdser(tuple), mode(m) {}

CJsonModifier::CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt) : pt_(pt), tagsMatcher_(tagsMatcher) {}

Error CJsonModifier::SetFieldValue(string_view tuple, const TagsPath &fieldPath, const VariantArray &value, WrSerializer &wrser) {
	if (fieldPath.empty()) return Error(errLogic, "Number of fields for update should be > 0");

	try {
		fieldPath_ = fieldPath;
		tagsPath_.clear();
		Context ctx(value, wrser, tuple, FieldModeSet);
		buildTuple(ctx);
	} catch (const Error &err) {
		return err;
	}

	return errOK;
}

Error CJsonModifier::RemoveFieldValue(string_view tuple, const TagsPath &fieldPath, WrSerializer &wrser) {
	try {
		fieldPath_ = fieldPath;
		tagsPath_.clear();
		Context ctx({}, wrser, tuple, FieldModeDrop);
		buildTuple(ctx);
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void CJsonModifier::updateField(Context &ctx, size_t idx) {
	if (ctx.isModeSet()) {
		assert(idx < ctx.value.size());
		copyCJsonValue(kvType2Tag(ctx.value[idx].Type()), ctx.value[idx], ctx.wrser);
	}
}

void CJsonModifier::putNewTags(Context &ctx) {
	if (ctx.isModeSet()) {
		assert(ctx.currObjPath.size() < fieldPath_.size());
		int newNestedObjects = 0;

		for (size_t i = ctx.currObjPath.size(); i < fieldPath_.size(); ++i) {
			bool objectTag = (i != fieldPath_.size() - 1);
			int tagType = objectTag ? TAG_OBJECT : kvType2Tag(ctx.value.front().Type());
			int tagName = fieldPath_[i];
			if (objectTag) {
				ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName)));
				++newNestedObjects;
			} else {
				putCJsonValue(tagType, tagName, ctx.value, ctx.wrser);
			}
		}

		while (newNestedObjects-- > 0) ctx.wrser.PutVarUint(TAG_END);
		ctx.currObjPath.clear();
	}
	ctx.fieldUpdated = true;
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

int CJsonModifier::determineUpdateTagType(const Context &ctx) {
	if (ctx.value.empty()) {
		return TAG_NULL;
	} else if (ctx.value.size() > 1) {
		return TAG_ARRAY;
	} else {
		return kvType2Tag(ctx.value.front().Type());
	}
}

bool CJsonModifier::buildTuple(Context &ctx) {
	ctag tag = ctx.rdser.GetVarUint();
	int tagType = tag.Type();

	if (tagType == TAG_END) {
		if (!ctx.fieldUpdated) {
			if (checkIfPathCorrect(ctx)) putNewTags(ctx);
		}
		ctx.wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();
	if (tagName) {
		(void)tagsMatcher_.tag2name(tagName);
		tagsPath_.push_back(tagName);
	}

	int field = tag.Field();
	bool exactMatch = (fieldPath_ == tagsPath_);
	bool tagToDrop = (exactMatch && ctx.isModeDrop());

	if (!tagToDrop) {
		if (exactMatch) {
			if (tagType == TAG_OBJECT) throw Error(errLogic, "Cannot update entire object!");
			if (field < 0) tagType = determineUpdateTagType(ctx);
		}
		ctx.wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));
	}

	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			int count = ctx.rdser.GetVarUint();
			if (exactMatch) {
				ctx.fieldUpdated = true;
				count = ctx.value.IsNullValue() ? 0 : ctx.value.size();
			}
			if (!tagToDrop) {
				ctx.wrser.PutVarUint(count);
			}
		}
	} else if (field < 0) {
		if (tagType == TAG_OBJECT) {
			if (tagToDrop) {
				skipCjsonTag(tag, ctx.rdser);
			} else {
				// We always skip main object tag
				bool nestedObject = (!ctx.fieldUpdated && tagName > 0);
				if (nestedObject) ctx.currObjPath.push_back(tagName);
				while (buildTuple(ctx)) {
				}
				if (nestedObject && !ctx.currObjPath.empty()) ctx.currObjPath.pop_back();
			}
		} else if (tagType == TAG_ARRAY) {
			if (exactMatch) {
				ctx.fieldUpdated = true;
				skipCjsonTag(tag, ctx.rdser);
				if (!tagToDrop) {
					int arrayTagType = ctx.value.empty() ? TAG_NULL : kvType2Tag(ctx.value.front().Type());
					ctx.wrser.PutUInt32(int(carraytag(ctx.value.size(), arrayTagType)));
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
			if (exactMatch) {
				if (!tagToDrop && ctx.value.empty()) throw Error(errLogic, "Update value cannot be empty");
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

}  // namespace reindexer
