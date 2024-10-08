#include "jsondecoder.h"
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "tagsmatcher.h"
#include "tools/flagguard.h"
#include "tools/json2kv.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

namespace reindexer {

Error JsonDecoder::Decode(Payload& pl, WrSerializer& wrser, const gason::JsonValue& v) {
	try {
		objectScalarIndexes_.reset();
		tagsPath_.clear();
		CJsonBuilder builder(wrser, ObjType::TypePlain, &tagsMatcher_);
		decodeJson(&pl, builder, v, 0, true);
	}

	catch (const Error& err) {
		return err;
	}
	return {};
}

void JsonDecoder::decodeJsonObject(Payload& pl, CJsonBuilder& builder, const gason::JsonValue& v, bool match) {
	for (const auto& elem : v) {
		int tagName = tagsMatcher_.name2tag(elem.key, true);
		assertrx(tagName);
		tagsPath_.emplace_back(tagName);
		int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());
		if (filter_) {
			if (field >= 0) {
				match = filter_->contains(field);
			} else {
				match = match && filter_->match(tagsPath_);
			}
		}

		if (field < 0) {
			decodeJson(&pl, builder, elem.value, tagName, match);
		} else if (match) {
			// Indexed field. extract it
			const auto& f = pl.Type().Field(field);
			switch (elem.value.getTag()) {
				case gason::JSON_ARRAY: {
					if rx_unlikely (!f.IsArray()) {
						throw Error(errLogic, "Error parsing json field '%s' - got array, expected scalar %s", f.Name(), f.Type().Name());
					}
					int count = 0;
					for (auto& subelem : elem.value) {
						(void)subelem;
						++count;
					}
					int pos = pl.ResizeArray(field, count, true);
					for (auto& subelem : elem.value) {
						pl.Set(field, pos++, jsonValue2Variant(subelem.value, f.Type(), f.Name()));
					}
					builder.ArrayRef(tagName, field, count);
				} break;
				case gason::JSON_NULL:
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, field, isInArray(), "json");
					objectScalarIndexes_.set(field);
					builder.Null(tagName);
					break;
				case gason::JSON_NUMBER:
				case gason::JSON_DOUBLE:
				case gason::JSON_OBJECT:
				case gason::JSON_STRING:
				case gason::JSON_TRUE:
				case gason::JSON_FALSE: {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, field, isInArray(), "json");
					objectScalarIndexes_.set(field);
					Variant value = jsonValue2Variant(elem.value, f.Type(), f.Name());
					builder.Ref(tagName, value, field);
					pl.Set(field, std::move(value), true);
				} break;
				default:
					throw Error(errLogic, "Unexpected '%d' tag", elem.value.getTag());
			}
		} else {
			// objectScalarIndexes_.set(field); - do not change objectScalarIndexes_ value for the filtered out fields
		}
		tagsPath_.pop_back();
	}
}

// Split original JSON into 2 parts:
// 1. PayloadFields - fields from json found by 'jsonPath' tags
// 2. stripped binary packed JSON without fields values found by 'jsonPath' tags
void JsonDecoder::decodeJson(Payload* pl, CJsonBuilder& builder, const gason::JsonValue& v, int tagName, bool match) {
	auto jsonTag = v.getTag();
	if (!match && jsonTag != gason::JSON_OBJECT) {
		return;
	}
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
			CounterGuardIR32 g(arrayLevel_);
			ObjType type = (gason::isHomogeneousArray(v)) ? ObjType::TypeArray : ObjType::TypeObjectArray;
			auto arrNode = builder.Array(tagName, type);
			for (const auto& elem : v) {
				decodeJson(pl, arrNode, elem.value, 0, match);
			}
			break;
		}
		case gason::JSON_OBJECT: {
			auto objNode = builder.Object(tagName);
			if (pl) {
				decodeJsonObject(*pl, objNode, v, match);
			} else {
				decodeJsonObject(v, objNode);
			}
			break;
		}
		default:
			throw Error(errLogic, "Unexpected '%d' tag", jsonTag);
	}
}

class TagsPathGuard {
public:
	TagsPathGuard(TagsPath& tagsPath, int tagName) noexcept : tagsPath_(tagsPath) { tagsPath_.emplace_back(tagName); }
	~TagsPathGuard() { tagsPath_.pop_back(); }

public:
	TagsPath& tagsPath_;
};

void JsonDecoder::decodeJsonObject(const gason::JsonValue& root, CJsonBuilder& builder) {
	for (const auto& elem : root) {
		int tagName = tagsMatcher_.name2tag(elem.key, true);
		TagsPathGuard tagsPathGuard(tagsPath_, tagName);
		decodeJson(nullptr, builder, elem.value, tagName, true);
	}
}

void JsonDecoder::Decode(std::string_view json, CJsonBuilder& builder, const TagsPath& fieldPath) {
	try {
		objectScalarIndexes_.reset();
		tagsPath_ = fieldPath;
		gason::JsonParser jsonParser;
		gason::JsonNode root = jsonParser.Parse(json);
		decodeJsonObject(root.value, builder);
	} catch (gason::Exception& e) {
		throw Error(errParseJson, "JSONDecoder: %s", e.what());
	}
}

}  // namespace reindexer
