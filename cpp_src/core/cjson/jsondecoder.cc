#include "jsondecoder.h"
#include <string_view>
#include "cjsonbuilder.h"
#include "cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "sparse_validator.h"
#include "tagsmatcher.h"
#include "tools/assertrx.h"
#include "tools/flagguard.h"
#include "tools/json2kv.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

namespace reindexer {

using namespace item_fields_validator;

Error JsonDecoder::Decode(Payload& pl, WrSerializer& wrser, const gason::JsonValue& v, FloatVectorsHolderVector& floatVectorsHolder) {
	try {
		objectScalarIndexes_.reset();
		tagsPath_.clear();
		CJsonBuilder builder(wrser, ObjType::TypePlain, &tagsMatcher_);
		decodeJson(&pl, builder, v, TagName::Empty(), floatVectorsHolder, Matched_True);
	} catch (const Error& err) {
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
		const auto field = tagsMatcher_.tags2field(tagsPath_);
		if (filter_) {
			if (field.IsRegularIndex()) {
				matched = Matched(filter_->contains(field.IndexNumber()));
			} else {
				matched &= filter_->match(tagsPath_);
			}
		}

		if (!field.IsRegularIndex()) {
			if (field.IsIndexed()) {  // sparse index
				decodeJsonSparse(&pl, builder, elem.value, tagName, floatVectorsHolder, matched,
								 SparseValidator{field.ValueType(), field.IsArray(), field.ArrayDim(), field.SparseNumber(), tagsMatcher_,
												 isInArray(), kJSONFmt});
			} else {
				decodeJson(&pl, builder, elem.value, tagName, floatVectorsHolder, matched);
			}
		} else if (matched) {
			const auto indexNumber = field.IndexNumber();
			// Indexed field. extract it
			const auto& f = pl.Type().Field(indexNumber);
			switch (elem.value.getTag()) {
				case gason::JsonTag::ARRAY:
					if (f.Type().Is<KeyValueType::FloatVector>()) {
						validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, indexNumber, isInArray(), kJSONFmt);
						validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(), 1, kJSONFmt);
						objectScalarIndexes_.set(indexNumber);
						Variant value =
							jsonValue2Variant(elem.value, f.Type(), f.Name(), &floatVectorsHolder, ConvertToString_False, ConvertNull_True);
						assertrx_dbg(value.Type().Is<KeyValueType::FloatVector>());
						const auto count = ConstFloatVectorView(value).Dimension().Value();
						pl.Set(indexNumber, std::move(value));
						builder.ArrayRef(tagName, indexNumber, int(count));
					} else {
						if (!f.IsArray()) [[unlikely]] {
							throwUnexpectedArrayError(f.Name(), f.Type(), kJSONFmt);
						}
						const auto [isHetero, outterSize, fullSize] = analizeHeteroArray(elem.value);
						validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(), fullSize, kJSONFmt);
						int pos = pl.ResizeArray(indexNumber, fullSize, Append_True);
						if (isHetero) {
							builder.HeteroArray(tagName, outterSize);
							decodeHeteroArray(pl, builder, elem.value, indexNumber, pos, f.Type(), f.Name());
						} else {
							assertrx(outterSize == fullSize);
							for (auto& subelem : elem.value) {
								pl.Set(
									indexNumber, pos++,
									jsonValue2Variant(subelem.value, f.Type(), f.Name(), nullptr, ConvertToString_False, ConvertNull_True));
							}
							builder.ArrayRef(tagName, indexNumber, outterSize);
						}
					}
					break;
				case gason::JsonTag::JSON_NULL:
					if (f.Type().Is<KeyValueType::FloatVector>() || (f.IsArray() && !isInArray())) {
						validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, indexNumber, isInArray(), kJSONFmt);
						objectScalarIndexes_.set(indexNumber);
						if (f.Type().Is<KeyValueType::FloatVector>()) {
							pl.Set(indexNumber, Variant{ConstFloatVectorView{}});
							builder.ArrayRef(tagName, indexNumber, 0);
						} else {
							builder.Null(tagName);
						}
						break;
					}
					[[fallthrough]];
				case gason::JsonTag::NUMBER:
				case gason::JsonTag::DOUBLE:
				case gason::JsonTag::OBJECT:
				case gason::JsonTag::STRING:
				case gason::JsonTag::JTRUE:
				case gason::JsonTag::JFALSE: {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, indexNumber, isInArray(), kJSONFmt);
					validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(), 1, kJSONFmt);
					objectScalarIndexes_.set(indexNumber);
					Variant value = jsonValue2Variant(elem.value, f.Type(), f.Name(), nullptr, ConvertToString_False, ConvertNull_True);
					builder.Ref(tagName, value.Type(), indexNumber);
					pl.Set(indexNumber, std::move(value), Append_True);
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

JsonDecoder::HeteroArrayAnalizeResult JsonDecoder::analizeHeteroArray(const gason::JsonValue& array) const {
	HeteroArrayAnalizeResult result;
	for (const auto& v : array) {
		++result.outterSize_;
		if (v.isArray()) {
			result.isHetero_ = true;
			result.fullSize_ += analizeHeteroArray(v.value).fullSize_;
		} else {
			++result.fullSize_;
		}
	}
	return result;
}

JsonDecoder::HeteroArrayFastAnalizeResult JsonDecoder::fastAnalizeHeteroArray(const gason::JsonValue& array) const {
	HeteroArrayFastAnalizeResult result;
	for (const auto& v : array) {
		++result.outterSize_;
		if (v.isArray()) {
			result.isHetero_ = true;
		}
	}
	return result;
}

void JsonDecoder::decodeHeteroArray(Payload& pl, CJsonBuilder& builder, const gason::JsonValue& array, int indexNumber, int& pos,
									KeyValueType fieldType, std::string_view fieldName) const {
	for (const auto& elem : array) {
		if (elem.isArray()) {
			const auto [isHetero, size] = fastAnalizeHeteroArray(elem.value);
			if (isHetero) {
				builder.HeteroArray(TagName::Empty(), size);
				decodeHeteroArray(pl, builder, elem.value, indexNumber, pos, fieldType, fieldName);
			} else {
				for (const auto& subElem : elem.value) {
					pl.Set(indexNumber, pos++,
						   jsonValue2Variant(subElem.value, fieldType, fieldName, nullptr, ConvertToString_False, ConvertNull_True));
				}
				builder.ArrayRef(TagName::Empty(), indexNumber, size);
			}
		} else {
			pl.Set(indexNumber, pos++,
				   jsonValue2Variant(elem.value, fieldType, fieldName, nullptr, ConvertToString_False, ConvertNull_True));
			builder.Ref(TagName::Empty(), fieldType, indexNumber);
		}
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
		case gason::JsonTag::NUMBER:
			builder.Put(tagName, int64_t(v.toNumber()));
			break;
		case gason::JsonTag::DOUBLE:
			builder.Put(tagName, v.toDouble());
			break;
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
			const ObjType type = (gason::isHomogeneousArray(v)) ? ObjType::TypeArray : ObjType::TypeObjectArray;
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

void JsonDecoder::decodeJsonSparse(Payload* pl, CJsonBuilder& builder, const gason::JsonValue& v, TagName tagName,
								   FloatVectorsHolderVector& floatVectorsHolder, Matched matched, const SparseValidator& validator) {
	auto jsonTag = v.getTag();
	if (!matched && jsonTag != gason::JsonTag::OBJECT) {
		return;
	}
	switch (jsonTag) {
		case gason::JsonTag::JSON_NULL:
			builder.Null(tagName);
			break;
		case gason::JsonTag::NUMBER:
		case gason::JsonTag::DOUBLE:
		case gason::JsonTag::STRING:
		case gason::JsonTag::JTRUE:
		case gason::JsonTag::JFALSE: {
			Variant value = jsonValue2Variant(v, validator.Type(), validator.Name(), nullptr, ConvertToString_True, ConvertNull_False);
			validator(value);
			builder.Put(tagName, value);
			break;
		}
		case gason::JsonTag::ARRAY: {
			auto arrayElementsValidation = validator.Array();
			CounterGuardIR32 g(arrayLevel_);
			decodeJsonArraySparse(pl, builder, v, tagName, floatVectorsHolder, matched, arrayElementsValidation);
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

void JsonDecoder::decodeJsonArraySparse(Payload* pl, CJsonBuilder& builder, const gason::JsonValue& v, TagName tagName,
										FloatVectorsHolderVector& floatVectorsHolder, Matched matched,
										SparseArrayValidator& arrayElementsValidation) {
	const ObjType arrType = (gason::isHomogeneousArray(v)) ? ObjType::TypeArray : ObjType::TypeObjectArray;
	auto arrNode = builder.Array(tagName, arrType);
	for (const auto& elem : v) {
		const auto elemType = elem.value.getTag();
		if (elemType == gason::JsonTag::OBJECT) {
			decodeJson(pl, arrNode, elem.value, TagName::Empty(), floatVectorsHolder, matched);
			std::ignore = arrayElementsValidation.Elem();
		} else if (elemType == gason::JsonTag::ARRAY) {
			decodeJsonArraySparse(pl, arrNode, elem.value, TagName::Empty(), floatVectorsHolder, matched, arrayElementsValidation);
		} else {
			Variant value = jsonValue2Variant(elem.value, arrayElementsValidation.Type(), arrayElementsValidation.Name(), nullptr,
											  ConvertToString_True, ConvertNull_False);
			arrayElementsValidation.Elem()(value);
			arrNode.Put(TagName::Empty(), value);
		}
	}
}

class [[nodiscard]] TagsPathGuard {
public:
	TagsPathGuard(TagsPath& tagsPath, TagName tagName) : tagsPath_(tagsPath) { tagsPath_.emplace_back(tagName); }
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
		if (!(root.isArray() || root.isObject())) {
			throw Error(errParseJson, "Json node must be an object or array '{}'", json);
		}

		decodeJsonObject(root.value, builder, floatVectorsHolder);
	} catch (gason::Exception& e) {
		throw Error(errParseJson, "JSONDecoder: {}", e.what());
	}
}

}  // namespace reindexer
