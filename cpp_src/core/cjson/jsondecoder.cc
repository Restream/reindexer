#include "jsondecoder.h"
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "tagsmatcher.h"
#include "tools/json2kv.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

namespace reindexer {

JsonDecoder::JsonDecoder(TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher), filter_(nullptr) {}
JsonDecoder::JsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter) : tagsMatcher_(tagsMatcher), filter_(filter) {}

Error JsonDecoder::Decode(Payload *pl, WrSerializer &wrser, const gason::JsonValue &v) {
	try {
		tagsPath_.clear();
		CJsonBuilder builder(wrser, ObjType::TypePlain, &tagsMatcher_);
		decodeJson(pl, builder, v, 0, true);
	}

	catch (const Error &err) {
		return err;
	}
	return 0;
}

void JsonDecoder::decodeJsonObject(Payload *pl, CJsonBuilder &builder, const gason::JsonValue &v, bool match) {
	for (auto elem : v) {
		int tagName = tagsMatcher_.name2tag(elem->key, true);
		assert(tagName);
		tagsPath_.push_back(tagName);
		int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());
		if (filter_) {
			if (field >= 0)
				match = filter_->contains(field);
			else
				match = match && filter_->match(tagsPath_);
		}

		if (field < 0) {
			decodeJson(pl, builder, elem->value, tagName, match);
		} else if (match) {
			// Indexed field. extract it
			auto &f = pl->Type().Field(field);
			if (elem->value.getTag() == gason::JSON_ARRAY) {
				if (!f.IsArray()) {
					throw Error(errLogic, "Error parsing json field '%s' - got array, expected scalar %s", f.Name(),
								Variant::TypeName(f.Type()));
				}
				int count = 0;
				for (auto subelem : elem->value) {
					(void)subelem;
					count++;
				}
				int pos = pl->ResizeArray(field, count, true);
				for (auto subelem : elem->value) {
					pl->Set(field, pos++, jsonValue2Variant(subelem->value, f.Type(), f.Name()));
				}
				builder.ArrayRef(tagName, field, count);
			} else if (elem->value.getTag() != gason::JSON_NULL) {
				Variant v = jsonValue2Variant(elem->value, f.Type(), f.Name());
				pl->Set(field, {v}, true);
				builder.Ref(tagName, v, field);
			} else {
				builder.Null(tagName);
			}
		}
		tagsPath_.pop_back();
	}
}

// Split original JSON into 2 parts:
// 1. PayloadFields - fields from json found by 'jsonPath' tags
// 2. stripped binary packed JSON without fields values found by 'jsonPath' tags
void JsonDecoder::decodeJson(Payload *pl, CJsonBuilder &builder, const gason::JsonValue &v, int tagName, bool match) {
	auto jsonTag = v.getTag();
	if (!match && jsonTag != gason::JSON_OBJECT) return;
	switch (jsonTag) {
		case gason::JSON_NUMBER: {
			int64_t value = v.toNumber();
			builder.Put(tagName, int64_t(value));
		} break;
		case gason::JSON_DOUBLE: {
			double value = v.toDouble();
			builder.Put(tagName, value);
		} break;
		case gason::JSON_STRING:
			builder.Put(tagName, v.toString());
			break;
		case gason::JSON_TRUE:
			builder.Put(tagName, true);
			break;
		case gason::JSON_FALSE:
			builder.Put(tagName, false);
			break;
		case gason::JSON_NULL:
			builder.Null(tagName);
			break;
		case gason::JSON_ARRAY: {
			ObjType type;
			if (gason::isHomogeneousArray(v)) {
				type = ObjType::TypeArray;
			} else {
				type = ObjType::TypeObjectArray;
			}
			auto arrNode = builder.Array(tagName, type);
			for (auto elem : v) {
				decodeJson(pl, arrNode, elem->value, 0, match);
			}
			break;
		}
		case gason::JSON_OBJECT: {
			auto objNode = builder.Object(tagName);
			if (pl) {
				decodeJsonObject(pl, objNode, v, match);
			} else {
				decodeJsonObject(v, objNode);
			}
			break;
		}
	}
}

class TagsPathGuard {
public:
	TagsPathGuard(TagsPath &tagsPath, int tagName) : tagsPath_(tagsPath) { tagsPath_.push_back(tagName); }
	~TagsPathGuard() { tagsPath_.pop_back(); }

public:
	TagsPath &tagsPath_;
};

void JsonDecoder::decodeJsonObject(const gason::JsonValue &root, CJsonBuilder &builder) {
	for (auto elem : root) {
		int tagName = tagsMatcher_.name2tag(elem->key, true);
		TagsPathGuard tagsPathGuard(tagsPath_, tagName);
		decodeJson(nullptr, builder, elem->value, tagName, true);
	}
}

void JsonDecoder::Decode(string_view json, CJsonBuilder &builder, const TagsPath &fieldPath) {
	tagsPath_ = fieldPath;
	gason::JsonParser jsonParser;
	gason::JsonNode root = jsonParser.Parse(json);
	decodeJsonObject(root.value, builder);
}

}  // namespace reindexer
