#include "jsondecoder.h"
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "tagsmatcher.h"
#include "tools/flagguard.h"
#include "tools/json2kv.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

namespace reindexer {

Error JsonDecoder::Decode(Payload& pl, WrSerializer& wrser, const gason::JsonValue& v, FloatVectorsHolderVector& floatVectorsHolder) {
	try {
		objectScalarIndexes_.reset();
		tagsPath_.clear();
		CJsonBuilder builder(wrser, ObjType::TypePlain, &tagsMatcher_);
		decodeJson(&pl, builder, v, TagName::Empty(), floatVectorsHolder, Matched_True);
	}

	catch (const Error& err) {
		return err;
	}
	return {};
}

void JsonDecoder::decodeJsonObject(Payload& pl, CJsonBuilder& builder, const gason::JsonValue& v,
								   FloatVectorsHolderVector& floatVectorsHolder, Matched matched) {
	using namespace std::string_view_literals;
	for (const auto& elem : v) {
		const TagName tagName = tagsMatcher_.name2tag(elem.key, CanAddField_True);
		assertrx(!tagName.IsEmpty());
		tagsPath_.emplace_back(tagName);
		const int field = tagsMatcher_.tags2field(tagsPath_);
		if (filter_) {
			if (field >= 0) {
				matched = Matched(filter_->contains(field));
			} else {
				matched &= filter_->match(tagsPath_);
			}
		}

		if (field < 0) {
			decodeJson(&pl, builder, elem.value, tagName, floatVectorsHolder, matched);
		} else if (matched) {
			// Indexed field. extract it
			const auto& f = pl.Type().Field(field);
			switch (elem.value.getTag()) {
				case gason::JsonTag::JSON_NULL:
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, field, isInArray(), "json");
					objectScalarIndexes_.set(field);
					builder.Null(tagName);
					break;
				case gason::JsonTag::ARRAY:
					if (f.Type().Is<KeyValueType::FloatVector>()) {
						validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, field, isInArray(), "json");
						validateArrayFieldRestrictions(f, 1, "json");
						objectScalarIndexes_.set(field);
						Variant value = jsonValue2Variant(elem.value, f.Type(), f.Name(), &floatVectorsHolder);
						assertrx_dbg(value.Type().Is<KeyValueType::FloatVector>());
						const auto count = ConstFloatVectorView(value).Dimension().Value();
						pl.Set(field, std::move(value));
						builder.ArrayRef(tagName, field, int(count));
					} else {
						if rx_unlikely (!f.IsArray()) {
							throwUnexpectedArrayError("json"sv, f);
						}
						int count = 0;
						for (auto& subelem : elem.value) {
							(void)subelem;
							++count;
						}
						validateArrayFieldRestrictions(f, count, "json");
						int pos = pl.ResizeArray(field, count, true);
						for (auto& subelem : elem.value) {
							pl.Set(field, pos++, jsonValue2Variant(subelem.value, f.Type(), f.Name(), nullptr));
						}
						builder.ArrayRef(tagName, field, count);
					}
					break;
				case gason::JsonTag::NUMBER:
				case gason::JsonTag::DOUBLE:
				case gason::JsonTag::OBJECT:
				case gason::JsonTag::STRING:
				case gason::JsonTag::JTRUE:
				case gason::JsonTag::JFALSE: {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, field, isInArray(), "json");
					validateArrayFieldRestrictions(f, 1, "json");
					objectScalarIndexes_.set(field);
					Variant value = jsonValue2Variant(elem.value, f.Type(), f.Name(), nullptr);
					builder.Ref(tagName, value.Type(), field);
					pl.Set(field, std::move(value), true);
				} break;
				case gason::JsonTag::EMPTY:
				default:
					throw Error(errLogic, "Unexpected '{}' tag", elem.value.getTag());
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
void JsonDecoder::decodeJson(Payload* pl, CJsonBuilder& builder, const gason::JsonValue& v, TagName tagName,
							 FloatVectorsHolderVector& floatVectorsHolder, Matched matched) {
	auto jsonTag = v.getTag();
	if (!matched && jsonTag != gason::JsonTag::OBJECT) {
		return;
	}
	switch (jsonTag) {
		case gason::JsonTag::NUMBER: {
			int64_t value = v.toNumber();
			builder.Put(tagName, int64_t(value));
		} break;
		case gason::JsonTag::DOUBLE: {
			double value = v.toDouble();
			builder.Put(tagName, value);
		} break;
		case gason::JsonTag::STRING:
			builder.Put(tagName, v.toString());
			break;
		case gason::JsonTag::JTRUE:
			builder.Put(tagName, true);
			break;
		case gason::JsonTag::JFALSE:
			builder.Put(tagName, false);
			break;
		case gason::JsonTag::JSON_NULL:
			builder.Null(tagName);
			break;
		case gason::JsonTag::ARRAY: {
			CounterGuardIR32 g(arrayLevel_);
			ObjType type = (gason::isHomogeneousArray(v)) ? ObjType::TypeArray : ObjType::TypeObjectArray;
			auto arrNode = builder.Array(tagName, type);
			for (const auto& elem : v) {
				decodeJson(pl, arrNode, elem.value, TagName::Empty(), floatVectorsHolder, matched);
			}
			break;
		}
		case gason::JsonTag::OBJECT: {
			auto objNode = builder.Object(tagName);
			if (pl) {
				decodeJsonObject(*pl, objNode, v, floatVectorsHolder, matched);
			} else {
				decodeJsonObject(v, objNode, floatVectorsHolder);
			}
			break;
		}
		case gason::JsonTag::EMPTY:
		default:
			throw Error(errLogic, "Unexpected '{}' tag", jsonTag);
	}
}

class TagsPathGuard {
public:
	TagsPathGuard(TagsPath& tagsPath, TagName tagName) noexcept : tagsPath_(tagsPath) { tagsPath_.emplace_back(tagName); }
	~TagsPathGuard() { tagsPath_.pop_back(); }

public:
	TagsPath& tagsPath_;
};

void JsonDecoder::decodeJsonObject(const gason::JsonValue& root, CJsonBuilder& builder, FloatVectorsHolderVector& floatVectorsHolder) {
	for (const auto& elem : root) {
		const TagName tagName = tagsMatcher_.name2tag(elem.key, CanAddField_True);
		if (tagName.IsEmpty()) {
			throw Error(errParseJson, "Unsupported JSON format. Unnamed field detected");
		}
		TagsPathGuard tagsPathGuard(tagsPath_, tagName);
		decodeJson(nullptr, builder, elem.value, tagName, floatVectorsHolder, Matched_True);
	}
}

void JsonDecoder::Decode(std::string_view json, CJsonBuilder& builder, const TagsPath& fieldPath,
						 FloatVectorsHolderVector& floatVectorsHolder) {
	try {
		objectScalarIndexes_.reset();
		tagsPath_ = fieldPath;
		gason::JsonParser jsonParser;
		gason::JsonNode root = jsonParser.Parse(json);
		decodeJsonObject(root.value, builder, floatVectorsHolder);
	} catch (gason::Exception& e) {
		throw Error(errParseJson, "JSONDecoder: {}", e.what());
	}
}

}  // namespace reindexer
