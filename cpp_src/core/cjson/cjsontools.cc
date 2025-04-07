#include "cjsontools.h"
#include <iomanip>
#include "cjsonbuilder.h"
#include "core/type_consts_helpers.h"

namespace reindexer {

TagType arrayKvType2Tag(const VariantArray& values) {
	if (values.empty()) {
		return TAG_NULL;
	}

	auto it = values.begin();
	const auto type = it->Type().ToTagType();

	++it;
	for (auto end = values.end(); it != end; ++it) {
		if (type != it->Type().ToTagType()) {
			return TAG_OBJECT;	// heterogeneously array detected
		}
	}
	return type;
}

void copyCJsonValue(TagType tagType, const Variant& value, WrSerializer& wrser) {
	if (value.Type().Is<KeyValueType::Null>()) {
		return;
	}
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(static_cast<double>(value.convert(KeyValueType::Double{})));
			break;
		case TAG_VARINT:
			value.Type().EvaluateOneOf(
				[&](KeyValueType::Int) { wrser.PutVarint(value.As<int>()); },
				[&](KeyValueType::Int64) { wrser.PutVarint(value.As<int64_t>()); },
				[&](OneOf<KeyValueType::Double, KeyValueType::Float, KeyValueType::Bool, KeyValueType::String, KeyValueType::Composite,
						  KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Uuid,
						  KeyValueType::FloatVector>) { wrser.PutVarint(static_cast<int64_t>(value.convert(KeyValueType::Int64{}))); });
			break;
		case TAG_BOOL:
			wrser.PutBool(static_cast<bool>(value.convert(KeyValueType::Bool{})));
			break;
		case TAG_STRING:
			wrser.PutVString(static_cast<std::string_view>(value.convert(KeyValueType::String{})));
			break;
		case TAG_UUID:
			wrser.PutUuid(value.convert(KeyValueType::Uuid{}).As<Uuid>());
			break;
		case TAG_FLOAT:
			wrser.PutFloat(static_cast<float>(value.convert(KeyValueType::Float{})));
			break;
		case TAG_NULL:
			break;
		case TAG_OBJECT:
			wrser.PutVariant(value);
			break;
		case TAG_ARRAY:
		case TAG_END:
			throw Error(errParseJson, "Unexpected cjson typeTag '{}' while parsing value", TagTypeToStr(tagType));
	}
}

void putCJsonRef(TagType tagType, TagName tagName, int tagField, const VariantArray& values, WrSerializer& wrser) {
	if (values.IsArrayValue()) {
		wrser.PutCTag(ctag{TAG_ARRAY, tagName, tagField});
		wrser.PutVarUint(values.size());
	} else if (values.size() == 1) {
		wrser.PutCTag(ctag{tagType, tagName, tagField});
	}
}

void putCJsonValue(TagType tagType, TagName tagName, const VariantArray& values, WrSerializer& wrser) {
	if (values.IsArrayValue()) {
		const TagType elemType = arrayKvType2Tag(values);
		wrser.PutCTag(ctag{TAG_ARRAY, tagName});
		wrser.PutCArrayTag(carraytag{values.size(), elemType});
		if (elemType == TAG_OBJECT) {
			for (const Variant& value : values) {
				auto itemType = value.Type().ToTagType();
				wrser.PutCTag(ctag{itemType});
				copyCJsonValue(itemType, value, wrser);
			}
		} else {
			for (const Variant& value : values) {
				copyCJsonValue(elemType, value, wrser);
			}
		}
	} else if (values.size() == 1) {
		wrser.PutCTag(ctag{tagType, tagName});
		copyCJsonValue(tagType, values.front(), wrser);
	} else {
		throw Error(errParams, "Unexpected value to update json value");
	}
}

void copyCJsonValue(TagType tagType, Serializer& rdser, WrSerializer& wrser) {
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(rdser.GetDouble());
			break;
		case TAG_VARINT:
			wrser.PutVarint(rdser.GetVarint());
			break;
		case TAG_BOOL:
			wrser.PutBool(rdser.GetBool());
			break;
		case TAG_STRING:
			wrser.PutVString(rdser.GetVString());
			break;
		case TAG_NULL:
			break;
		case TAG_UUID:
			wrser.PutUuid(rdser.GetUuid());
			break;
		case TAG_FLOAT:
			wrser.PutFloat(rdser.GetFloat());
			break;
		case TAG_OBJECT:
			wrser.PutVariant(rdser.GetVariant());
			break;
		case TAG_END:
		case TAG_ARRAY:
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '{}' while parsing value", int(tagType));
	}
}

void skipCjsonTag(ctag tag, Serializer& rdser, std::array<unsigned, kMaxIndexes>* fieldsArrayOffsets) {
	switch (tag.Type()) {
		case TAG_ARRAY: {
			const auto field = tag.Field();
			const bool embeddedField = (field < 0);
			if (embeddedField) {
				const carraytag atag = rdser.GetCArrayTag();
				const auto count = atag.Count();
				if (atag.Type() == TAG_OBJECT) {
					for (size_t i = 0; i < count; ++i) {
						skipCjsonTag(rdser.GetCTag(), rdser);
					}
				} else {
					for (size_t i = 0; i < count; ++i) {
						skipCjsonTag(ctag{atag.Type()}, rdser);
					}
				}
			} else {
				const auto len = rdser.GetVarUInt();
				if (fieldsArrayOffsets) {
					(*fieldsArrayOffsets)[field] += len;
				}
			}
		} break;
		case TAG_OBJECT:
			for (ctag otag{rdser.GetCTag()}; otag != kCTagEnd; otag = rdser.GetCTag()) {
				skipCjsonTag(otag, rdser, fieldsArrayOffsets);
			}
			break;
		case TAG_VARINT:
		case TAG_STRING:
		case TAG_DOUBLE:
		case TAG_END:
		case TAG_BOOL:
		case TAG_NULL:
		case TAG_UUID:
		case TAG_FLOAT: {
			const auto field = tag.Field();
			const bool embeddedField = (field < 0);
			if (embeddedField) {
				rdser.SkipRawVariant(KeyValueType{tag.Type()});
			} else if (fieldsArrayOffsets) {
				(*fieldsArrayOffsets)[field] += 1;
			}
		} break;
		default:
			throw Error(errParseJson, "skipCjsonTag: unexpected ctag type value: {}", int(tag.Type()));
	}
}

Variant cjsonValueToVariant(TagType tagType, Serializer& rdser, KeyValueType dstType) {
	return rdser
		.GetRawVariant(dstType.Is<KeyValueType::Int>() && tagType == TAG_VARINT ? KeyValueType{KeyValueType::Int{}} : KeyValueType{tagType})
		.convert(dstType);
}

template <typename T>
void buildPayloadTuple(const PayloadIface<T>& pl, const TagsMatcher* tagsMatcher, WrSerializer& wrser) {
	CJsonBuilder builder(wrser, ObjType::TypeObject);
	for (int field = 1, numFields = pl.NumFields(); field < numFields; ++field) {
		const PayloadFieldType& fieldType = pl.Type().Field(field);
		if (fieldType.JsonPaths().size() < 1 || fieldType.JsonPaths()[0].empty()) {
			continue;
		}

		const TagName tagName = tagsMatcher->name2tag(fieldType.JsonPaths()[0]);
		assertf(!tagName.IsEmpty(), "ns={}, field={}", pl.Type().Name(), fieldType.JsonPaths()[0]);

		if (fieldType.IsFloatVector()) {
			const auto value = pl.Get(field, 0);
			const auto count = ConstFloatVectorView(value).Dimension().Value();
			builder.ArrayRef(tagName, field, int(count));
		} else if (fieldType.IsArray()) {
			builder.ArrayRef(tagName, field, pl.GetArrayLen(field));
		} else {
			builder.Ref(tagName, pl.Get(field, 0).Type(), field);
		}
	}
}

template void buildPayloadTuple<const PayloadValue>(const PayloadIface<const PayloadValue>&, const TagsMatcher*, WrSerializer&);
template void buildPayloadTuple<PayloadValue>(const PayloadIface<PayloadValue>&, const TagsMatcher*, WrSerializer&);

void throwUnexpectedNestedArrayError(std::string_view parserName, const PayloadFieldType& f) {
	throw Error(errLogic, "Error parsing {} field '{}' - got value nested into the array, but expected scalar {}", parserName, f.Name(),
				f.Type().Name());
}

void throwScalarMultipleEncodesError(const Payload& pl, const PayloadFieldType& f, int field) {
	throw Error(errLogic, "Non-array field '{}' [{}] from '{}' can only be encoded once.", f.Name(), field, pl.Type().Name());
}

void throwUnexpectedArrayError(std::string_view parserName, const PayloadFieldType& fieldRef) {
	throw Error(errLogic, "Error parsing {} field '{}' - got array, expected scalar {}", parserName, fieldRef.Name(),
				fieldRef.Type().Name());
}

void throwUnexpectedArraySizeForFloatVectorError(std::string_view parserName, const PayloadFieldType& fieldRef, size_t size) {
	throw Error(errLogic, "Error parsing {} field '{}' - got array of size {}, expected float_vector of size {}", parserName,
				fieldRef.Name(), size, fieldRef.FloatVectorDimension().Value());
}

void throwUnexpectedArrayTypeForFloatVectorError(std::string_view parserName, const PayloadFieldType& fieldRef) {
	throw Error(errLogic, "Error parsing {} field '{}' - got array of non-double values, expected array convertible to {}", parserName,
				fieldRef.Name(), fieldRef.Type().Name());
}

void throwUnexpectedArraySizeError(std::string_view parserName, const PayloadFieldType& f, int arraySize) {
	throw Error(errParams, "{} array field '{}' for this index type must contain {} elements, but got {}", parserName, f.Name(),
				f.ArrayDims(), arraySize);
}

static void skipCjsonValue(TagType type, Serializer& cjson) {
	switch (type) {
		case TAG_VARINT:
			cjson.GetVarint();
			break;
		case TAG_DOUBLE:
			cjson.GetDouble();
			break;
		case TAG_STRING:
			cjson.GetPVString();
			break;
		case TAG_BOOL:
			cjson.GetVarUInt();
			break;
		case TAG_UUID:
			cjson.GetUuid();
			break;
		case TAG_FLOAT:
			cjson.GetFloat();
			break;
		case TAG_NULL:
		case TAG_OBJECT:
		case TAG_ARRAY:
		case TAG_END:
			assertrx(0);
	}
}

static void dumpCjsonValue(TagType type, Serializer& cjson, std::ostream& dump) {
	switch (type) {
		case TAG_VARINT:
			dump << cjson.GetVarint();
			break;
		case TAG_DOUBLE:
			dump << cjson.GetDouble();
			break;
		case TAG_STRING:
			dump << '"' << std::string_view{cjson.GetPVString()} << '"';
			break;
		case TAG_BOOL:
			dump << std::boolalpha << bool(cjson.GetVarUInt());
			break;
		case TAG_UUID:
			dump << std::string{cjson.GetUuid()};
			break;
		case TAG_FLOAT:
			dump << cjson.GetFloat();
			break;
		case TAG_NULL:
		case TAG_OBJECT:
		case TAG_ARRAY:
		case TAG_END:
			assertrx(0);
	}
}

template <typename PL>
static void dumpCjsonObject(Serializer& cjson, std::ostream& dump, const TagsMatcher* tm, const PL* pl, std::string_view tab,
							unsigned indentLevel) {
	static constexpr uint32_t kMaxArrayOutput = 3;
	const auto indent = [&dump, tab](unsigned indLvl) {
		for (unsigned i = 0; i < indLvl; ++i) {
			dump << tab;
		}
	};
	VariantArray buf;
	while (!cjson.Eof()) {
		const ctag tag = cjson.GetCTag();
		const TagType type = tag.Type();
		const auto name = tag.Name();
		const auto field = tag.Field();
		if (type == TAG_END) {
			assertrx(indentLevel > 0);
			--indentLevel;
		}
		indent(indentLevel);
		dump << std::left << std::setw(10) << TagTypeToStr(type);
		dump << std::right << std::setw(4) << name.AsNumber();
		dump << std::right << std::setw(4) << field;
		if (tm && !name.IsEmpty()) {
			dump << " \"" << tm->tag2name(name) << '"';
		}
		if (field >= 0) {
			switch (type) {
				case TAG_VARINT:
				case TAG_DOUBLE:
				case TAG_FLOAT:
				case TAG_STRING:
				case TAG_BOOL:
				case TAG_UUID:
					if (pl) {
						buf.clear<false>();
						pl->Get(field, buf);
						assertrx(buf.size() == 1);
						dump << " -> " << buf[0].As<std::string>();
					}
					break;
				case TAG_ARRAY: {
					dump << '\n';
					indent(indentLevel + 1);
					const uint32_t count = cjson.GetVarUInt();
					dump << "Count: " << count;
					if (pl) {
						dump << " -> [";
						buf.clear<false>();
						pl->Get(field, buf);
						if (pl->Type().Field(field).IsFloatVector()) {
							assertrx(buf.size() == 1);
							const ConstFloatVectorView vect{buf[0]};
							if (vect.IsEmpty()) {
								dump << " -> <empty>";
							} else {
								dump << " -> " << vect.Dimension().Value();
								if (vect.IsStripped()) {
									dump << "[<stripped>]";
								} else {
									dump << '[';
									for (uint32_t i = 0; i < std::min(uint32_t(vect.Dimension()), kMaxArrayOutput); ++i) {
										if (i != 0) {
											dump << ", ";
										}
										dump << vect.Data()[i];
									}
									if (uint32_t(vect.Dimension()) > kMaxArrayOutput) {
										dump << ", ...]";
									} else {
										dump << ']';
									}
								}
							}
						} else {
							assertrx(buf.size() == count);
							for (size_t i = 0; i < std::min(count, kMaxArrayOutput); ++i) {
								if (i != 0) {
									dump << ", ";
								}
								dump << buf[i].As<std::string>();
							}
							if (count > kMaxArrayOutput) {
								dump << " ...]";
							} else {
								dump << ']';
							}
						}
					}
				} break;
				case TAG_NULL:
				case TAG_OBJECT:
				case TAG_END:
					assertrx(0);
			}
			dump << '\n';
		} else {
			dump << '\n';
			switch (type) {
				case TAG_VARINT:
				case TAG_DOUBLE:
				case TAG_STRING:
				case TAG_BOOL:
				case TAG_UUID:
				case TAG_FLOAT:
					indent(indentLevel + 1);
					dumpCjsonValue(type, cjson, dump);
					dump << '\n';
					break;
				case TAG_NULL:
					break;
				case TAG_ARRAY: {
					const carraytag arr = cjson.GetCArrayTag();
					const auto count = arr.Count();
					if (arr.Type() == TAG_OBJECT) {
						indent(indentLevel + 1);
						dump << "<heterogeneous> count: " << count << '\n';
						for (uint32_t i = 0; i < count; ++i) {
							const ctag t = cjson.GetCTag();
							assertrx(t.Name().IsEmpty());
							assertrx(t.Field() < 0);
							indent(indentLevel + 2);
							dump << TagTypeToStr(t.Type());
							dump << ": ";
							if (t.Type() == TAG_OBJECT) {
								dump << "{\n";
								dumpCjsonObject(cjson, dump, tm, pl, tab, indentLevel + 3);
								indent(indentLevel + 2);
								dump << "}\n";
							} else {
								dumpCjsonValue(t.Type(), cjson, dump);
								dump << '\n';
							}
						}
					} else {
						indent(indentLevel + 1);
						dump << TagTypeToStr(arr.Type()) << " count: " << count << '\n';
						indent(indentLevel + 2);
						dump << '[';
						if (arr.Type() == TAG_OBJECT && count > 0) {
							dump << '\n';
							for (size_t i = 0; i < count; ++i) {
								indent(indentLevel + 3);
								dump << TagTypeToStr(arr.Type()) << '\n';
								dumpCjsonObject(cjson, dump, tm, pl, tab, indentLevel + 4);
							}
							indent(indentLevel + 2);
						} else {
							uint32_t i = 0;
							for (; i < std::min(count, kMaxArrayOutput); ++i) {
								if (i != 0) {
									dump << ", ";
								}
								dumpCjsonValue(arr.Type(), cjson, dump);
							}
							for (; i < count; ++i) {
								skipCjsonValue(arr.Type(), cjson);
							}
							if (count > kMaxArrayOutput) {
								dump << ", ...";
							}
						}
						dump << "]\n";
					}
				} break;
				case TAG_OBJECT:
					dumpCjsonObject(cjson, dump, tm, pl, tab, indentLevel + 1);
					break;
				case TAG_END:
					return;
			}
		}
	}
}

void DumpCjson(Serializer& cjson, std::ostream& dump, const Payload* pl, const TagsMatcher* tm, std::string_view tab) {
	const auto osFlags = dump.flags();
	dump.exceptions(std::ostream::failbit | std::ostream::badbit);
	dump << "TAG_TYPE NAME FIELD\n";
	dumpCjsonObject(cjson, dump, tm, pl, tab, 0);
	dump.flags(osFlags);
}

void DumpCjson(Serializer& cjson, std::ostream& dump, const ConstPayload* pl, const TagsMatcher* tm, std::string_view tab) {
	const auto osFlags = dump.flags();
	dump.exceptions(std::ostream::failbit | std::ostream::badbit);
	dump << "TAG_TYPE NAME FIELD\n";
	dumpCjsonObject(cjson, dump, tm, pl, tab, 0);
	dump.flags(osFlags);
}

}  // namespace reindexer
