
#include "cjsonmodifier.h"
#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

CJsonModifier::CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt) : pt_(pt), tagsMatcher_(tagsMatcher) {}
CJsonModifier::Context::Context(const VariantArray &v, WrSerializer &ser, const string_view &tuple) : value(v), wrser(ser), rdser(tuple) {}

Error CJsonModifier::SetFieldValue(const string_view &tuple, const TagsPath &fieldPath, const VariantArray &value, WrSerializer &wrser) {
	if (fieldPath.empty()) return Error(errLogic, "Number of fields for update should be > 0");
	if (value.empty()) return Error(errLogic, "Update value cannot be empty");

	try {
		fieldPath_ = fieldPath;
		tagsPath_.clear();
		wrser.Reset();
		Context ctx(value, wrser, tuple);
		buildTuple(ctx);
	} catch (const Error &err) {
		return err;
	}

	return errOK;
}

void CJsonModifier::replaceField(int tagType, Context &ctx, size_t idx) {
	assert(idx < ctx.value.size());
	copyCJsonValue(tagType, ctx.value[idx], ctx.wrser);
	ctx.fieldUpdated = true;
}

void CJsonModifier::putNewTags(Context &ctx) {
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

	bool exactMatch = false;
	int field = tag.Field();
	// if (field < 0) {
	exactMatch = (fieldPath_ == tagsPath_);
	if (exactMatch && (tagType == TAG_OBJECT)) throw Error(errLogic, "Cannot update entire object!");
	// }

	ctx.wrser.PutVarUint(static_cast<int>(tag));
	if (field >= 0) {
		if (tagType == TAG_ARRAY) {
			int count = ctx.rdser.GetVarUint();
			if (exactMatch) {
				count = ctx.value.size();
				ctx.fieldUpdated = true;
			}
			ctx.wrser.PutVarUint(count);
		}
	} else if (field < 0) {
		if (tagType == TAG_OBJECT) {
			// We always skip main object tag
			bool nestedObject = (!ctx.fieldUpdated && tagName > 0);
			if (nestedObject) ctx.currObjPath.push_back(tagName);
			while (buildTuple(ctx)) {
			}
			if (nestedObject && !ctx.currObjPath.empty()) ctx.currObjPath.pop_back();
		} else if (tagType == TAG_ARRAY) {
			if (exactMatch) {
				skipCjsonTag(tag, ctx.rdser);
				int arrayTagType = ctx.value.empty() ? TAG_NULL : kvType2Tag(ctx.value.front().Type());
				ctx.wrser.PutUInt32(int(carraytag(ctx.value.size(), arrayTagType)));
				for (size_t i = 0; i < ctx.value.size(); ++i) {
					replaceField(kvType2Tag(ctx.value[i].Type()), ctx, i);
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
				replaceField(tagType, ctx, 0);
				skipCjsonTag(tag, ctx.rdser);
			} else {
				copyCJsonValue(tagType, ctx.rdser, ctx.wrser);
			}
		}
	}

	if (tagName) tagsPath_.pop_back();
	return true;
}

}  // namespace reindexer
