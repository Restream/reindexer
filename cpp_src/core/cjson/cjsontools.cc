#include "cjsontools.h"
#include <iomanip>
#include "cjsonbuilder.h"
#include "core/type_consts_helpers.h"
#include "sparse_validator.h"
#include "tools/use_pmr.h"

#ifdef USE_PMR
#include <memory_resource>
#endif

namespace reindexer {

using namespace item_fields_validator;

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
				[&](concepts::OneOf<KeyValueType::Double, KeyValueType::Float, KeyValueType::Bool, KeyValueType::String,
									KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null,
									KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
					wrser.PutVarint(static_cast<int64_t>(value.convert(KeyValueType::Int64{})));
				});
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
		wrser.PutCArrayTag(carraytag{uint32_t(values.size()), elemType});
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

template <typename Validator>
void copyCJsonValue(TagType tagType, Serializer& rdser, WrSerializer& wrser, const Validator& validator) {
	switch (tagType) {
		case TAG_DOUBLE: {
			const double val = rdser.GetDouble();
			validator(val);
			wrser.PutDouble(val);
		} break;
		case TAG_VARINT: {
			const int64_t val = rdser.GetVarint();
			validator(val);
			wrser.PutVarint(val);
		} break;
		case TAG_BOOL: {
			const bool val = rdser.GetBool();
			validator(val);
			wrser.PutBool(val);
		} break;
		case TAG_STRING: {
			const auto val = rdser.GetVString();
			validator(val);
			wrser.PutVString(val);
		} break;
		case TAG_NULL:
			break;
		case TAG_UUID: {
			const auto val = rdser.GetUuid();
			validator(val);
			wrser.PutUuid(val);
		} break;
		case TAG_FLOAT: {
			const auto val = rdser.GetFloat();
			validator(val);
			wrser.PutFloat(val);
		} break;
		case TAG_OBJECT:
			wrser.PutVariant(rdser.GetVariant());
			break;
		case TAG_END:
		case TAG_ARRAY:
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '{}' while parsing value", int(tagType));
	}
}
template void copyCJsonValue(TagType, Serializer&, WrSerializer&, const NoValidation&);
template void copyCJsonValue(TagType, Serializer&, WrSerializer&, const SparseValidator&);
template void copyCJsonValue(TagType, Serializer&, WrSerializer&, const SparseArrayValidator&);

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
						skipCjsonTag(rdser.GetCTag(), rdser, fieldsArrayOffsets);
					}
				} else {
					for (size_t i = 0; i < count; ++i) {
						skipCjsonTag(ctag{atag.Type()}, rdser, fieldsArrayOffsets);
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

template <typename CJsonBuilderT, typename PayloadIfaceT>
static void CJsonBuilderRef(CJsonBuilderT& builder, const PayloadIfaceT& pl, const TagName& tagName, int field) {
	const PayloadFieldType& fieldType = pl.Type().Field(field);

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

namespace {
struct PathElementNodeTree {
#ifdef USE_PMR
	using MapT = std::pmr::map<TagName, PathElementNodeTree>;

	explicit PathElementNodeTree(std::pmr::memory_resource* buffer) : children_(buffer) {}
#else
	using MapT = std::map<TagName, PathElementNodeTree>;

	explicit PathElementNodeTree() = default;
#endif

	void Add(std::span<const TagName>&& tags, int indexNo) {
		indexNo_ = indexNo;
		if (tags.empty()) {
			return;
		}

#ifdef USE_PMR
		if (!children_.contains(tags.front())) {
			children_.emplace(tags.front(), PathElementNodeTree(children_.get_allocator().resource()));
		}
		children_.at(tags.front()).Add({std::next(tags.begin()), tags.end()}, indexNo);

#else
		children_[tags.front()].Add({std::next(tags.begin()), tags.end()}, indexNo);
#endif
	}

	template <typename BuilderT, typename PayloadIfaceT>
	void Travers(BuilderT& builder, const PayloadIfaceT& pl, TagName tagName = TagName::Empty()) const noexcept {
		if (children_.empty()) {
			CJsonBuilderRef(builder, pl, tagName, indexNo_);
			return;
		}
		auto child = tagName.IsEmpty() ? std::move(builder) : builder.Object(tagName);
		for (const auto& [tag, node] : children_) {
			node.Travers(child, pl, tag);
		}
	}

private:
	int indexNo_ = -1;	// it only makes sense for leaf nodes
	MapT children_;
};
}  // namespace

template <typename T>
void buildPayloadTuple(const PayloadIface<T>& pl, const TagsMatcher* tagsMatcher, WrSerializer& wrser) {
#ifdef USE_PMR
	int bufSize = 0;
	for (int field = 1, numFields = pl.NumFields(); field < numFields; ++field) {
		const PayloadFieldType& fieldType = pl.Type().Field(field);
		assertf_dbg(!fieldType.JsonPaths().empty() && !fieldType.JsonPaths()[0].empty(), "Wrong JsonPaths for field={}, ns={}", field,
					pl.Type().Name());
		bufSize += std::count(fieldType.JsonPaths()[0].begin(), fieldType.JsonPaths()[0].end(), '.') + 1;
	}

	h_vector<std::byte, 1024> buffer;
	buffer.resize((sizeof(PathElementNodeTree::MapT::node_type) + sizeof(PathElementNodeTree::MapT::value_type)) * bufSize);
	std::pmr::monotonic_buffer_resource pool(buffer.data(), buffer.size());
	PathElementNodeTree root(&pool);
#else
	PathElementNodeTree root;
#endif

	for (int field = 1, numFields = pl.NumFields(); field < numFields; ++field) {
		const PayloadFieldType& fieldType = pl.Type().Field(field);
		assertf_dbg(!fieldType.JsonPaths().empty() && !fieldType.JsonPaths()[0].empty(), "Wrong JsonPaths for field={}, ns={}", field,
					pl.Type().Name());
		const TagsPath tagsPath = tagsMatcher->path2tag(fieldType.JsonPaths()[0]);
		root.Add(std::span{tagsPath.begin(), tagsPath.end()}, field);
	}

	CJsonBuilder builder(wrser, ObjType::TypeObject);
	root.Travers(builder, pl);
}

template void buildPayloadTuple<const PayloadValue>(const PayloadIface<const PayloadValue>&, const TagsMatcher*, WrSerializer&);
template void buildPayloadTuple<PayloadValue>(const PayloadIface<PayloadValue>&, const TagsMatcher*, WrSerializer&);

CJsonNestedArrayAnalizeResult analizeNestedArray(size_t count, Serializer& rdser) {
	CJsonNestedArrayAnalizeResult result;
	for (size_t i = 0; i < count; ++i) {
		const auto tag = rdser.GetCTag();
		if (tag.Type() == TAG_ARRAY) {
			result.isNested = true;
			const auto nestedArr = rdser.GetCArrayTag();
			const auto nestedArrCount = nestedArr.Count();
			const auto nestedArrType = nestedArr.Type();
			if (nestedArrType == TAG_OBJECT) {
				result.size += analizeNestedArray(nestedArrCount, rdser).size;
			} else {
				const ctag nestedTag{nestedArrType};
				for (size_t j = 0; j < nestedArrCount; ++j) {
					skipCjsonTag(nestedTag, rdser);
				}
				result.size += nestedArrCount;
			}
		} else {
			skipCjsonTag(tag, rdser);
			++result.size;
		}
	}
	return result;
}

void throwUnexpectedNestedArrayError(std::string_view parserName, std::string_view fieldName, KeyValueType fieldType) {
	throw Error(errLogic, "Error parsing {} field '{}' - got value nested into the array, but expected scalar {}", parserName, fieldName,
				fieldType.Name());
}

void throwScalarMultipleEncodesError(const Payload& pl, const PayloadFieldType& f, int field) {
	throw Error(errLogic, "Non-array field '{}' [{}] from '{}' can only be encoded once.", f.Name(), field, pl.Type().Name());
}

void throwUnexpectedArrayError(std::string_view fieldName, KeyValueType fieldType, std::string_view parserName) {
	throw Error(errLogic, "Error parsing {} field '{}' - got array, expected scalar {}", parserName, fieldName, fieldType.Name());
}

void throwUnexpectedArraySizeForFloatVectorError(std::string_view parserName, const PayloadFieldType& fieldRef, size_t size) {
	throw Error(errLogic, "Error parsing {} field '{}' - got array of size {}, expected float_vector of size {}", parserName,
				fieldRef.Name(), size, fieldRef.FloatVectorDimension().Value());
}

void throwUnexpectedArrayTypeForFloatVectorError(std::string_view parserName, const PayloadFieldType& fieldRef) {
	throw Error(errLogic, "Error parsing {} field '{}' - got array of non-double values, expected array convertible to {}", parserName,
				fieldRef.Name(), fieldRef.Type().Name());
}

void throwUnexpectedArraySizeError(std::string_view parserName, std::string_view fieldName, size_t fieldArrayDim, size_t arraySize) {
	throw Error(errParams, "{} array field '{}' for this index type must contain {} elements, but got {}", parserName, fieldName,
				fieldArrayDim, arraySize);
}

void throwUnexpectedObjectInIndex(std::string_view fieldName, std::string_view parserName) {
	throw Error(errLogic, "Error parsing {} field '{}' - unable to use object in this context", parserName, fieldName);
}

void throwUnexpected(std::string_view fieldName, KeyValueType expectedType, KeyValueType obtainedType, std::string_view parserName) {
	throwUnexpected(fieldName, expectedType, obtainedType.Name(), parserName);
}

void throwUnexpected(std::string_view fieldName, KeyValueType expectedType, std::string_view obtainedType, std::string_view parserName) {
	throw Error(errLogic, "Error parsing {} field '{}' - got {}, expected {}", parserName, fieldName, obtainedType, expectedType.Name());
}

template <typename PL>
class CJsonDumper {
	static constexpr uint32_t kMaxArrayOutput = 3;

public:
	CJsonDumper(Serializer& cjson, std::ostream& dump, const PL* pl, const TagsMatcher* tm, std::string_view tab)
		: cjson_{cjson}, dump_{dump}, pl_{pl}, tm_{tm}, tab_{tab} {}

	void Dump() {
		using namespace std::string_view_literals;
		const auto osFlags = dump_.flags();
		dump_.exceptions(std::ostream::failbit | std::ostream::badbit);
		dump_ << "TAG_TYPE NAME FIELD\n"sv;
		dumpCjsonObject(0);
		dump_.flags(osFlags);
	}

private:
	void skipCjsonValue(TagType type) const {
		switch (type) {
			case TAG_VARINT:
				// NOLINTNEXTLINE (bugprone-unused-return-value)
				cjson_.GetVarint();
				break;
			case TAG_DOUBLE:
				// NOLINTNEXTLINE (bugprone-unused-return-value)
				cjson_.GetDouble();
				break;
			case TAG_STRING:
				cjson_.SkipPVString();
				break;
			case TAG_BOOL:
				// NOLINTNEXTLINE (bugprone-unused-return-value)
				cjson_.GetVarUInt();
				break;
			case TAG_UUID:
				cjson_.SkipUuid();
				break;
			case TAG_FLOAT:
				// NOLINTNEXTLINE (bugprone-unused-return-value)
				cjson_.GetFloat();
				break;
			case TAG_NULL:
				break;
			case TAG_OBJECT:
			case TAG_ARRAY:
			case TAG_END:
			default:
				assertrx(0);
		}
	}

	void dumpCjsonValue(TagType type) const {
		switch (type) {
			case TAG_VARINT:
				dump_ << cjson_.GetVarint();
				break;
			case TAG_DOUBLE:
				dump_ << cjson_.GetDouble();
				break;
			case TAG_STRING:
				dump_ << '"' << std::string_view{cjson_.GetPVString()} << '"';
				break;
			case TAG_BOOL:
				dump_ << std::boolalpha << bool(cjson_.GetVarUInt());
				break;
			case TAG_UUID:
				dump_ << std::string{cjson_.GetUuid()};
				break;
			case TAG_FLOAT:
				dump_ << cjson_.GetFloat();
				break;
			case TAG_NULL:
				break;
			case TAG_OBJECT:
			case TAG_ARRAY:
			case TAG_END:
			default:
				assertrx(0);
		}
	}

	void dumpCjsonArray(unsigned indentLevel) {
		using namespace std::string_view_literals;
		const carraytag arr = cjson_.GetCArrayTag();
		const auto count = arr.Count();
		indent(indentLevel);
		++indentLevel;
		if (arr.Type() == TAG_OBJECT) {
			dump_ << "<heterogeneous> count: "sv << count << '\n';
			indent(indentLevel);
			dump_ << "[\n"sv;
			for (uint32_t i = 0; i < count; ++i) {
				const ctag t = cjson_.GetCTag();
				assertrx(t.Name().IsEmpty());
				const auto field = t.Field();
				const auto tagType = t.Type();
				if (field >= 0) {
					dumpCTag(tagType, TagName::Empty(), field, indentLevel + 1);
					dumpIndexedField(field, tagType, indentLevel + 2);
				} else {
					indent(indentLevel + 1);
					dump_ << TagTypeToStr(tagType);
					dump_ << ": "sv;
					switch (tagType) {
						case TAG_OBJECT:
							dump_ << "{\n"sv;
							dumpCjsonObject(indentLevel + 2);
							indent(indentLevel + 1);
							dump_ << "}\n"sv;
							break;
						case TAG_ARRAY:
							dump_ << '\n';
							dumpCjsonArray(indentLevel + 2);
							break;
						case TAG_VARINT:
						case TAG_DOUBLE:
						case TAG_STRING:
						case TAG_BOOL:
						case TAG_UUID:
						case TAG_FLOAT:
						case TAG_NULL:
							dumpCjsonValue(tagType);
							dump_ << '\n';
							break;
						case TAG_END:
						default:
							assertrx(0);
					}
				}
			}
			indent(indentLevel);
			dump_ << "]\n"sv;
		} else {
			dump_ << TagTypeToStr(arr.Type()) << " count: "sv << count << '\n';
			indent(indentLevel);
			dump_ << '[';
			uint32_t i = 0;
			for (; i < std::min(count, kMaxArrayOutput); ++i) {
				if (i != 0) {
					dump_ << ", "sv;
				}
				dumpCjsonValue(arr.Type());
			}
			for (; i < count; ++i) {
				skipCjsonValue(arr.Type());
			}
			if (count > kMaxArrayOutput) {
				dump_ << ", ..."sv;
			}
			dump_ << "]\n"sv;
		}
	}

	void dumpIndexedField(int field, TagType tagType, unsigned indentLevel) {
		using namespace std::string_view_literals;
		VariantArray buf;
		auto& cnt = fieldsOutCnt_[field];
		switch (tagType) {
			case TAG_VARINT:
			case TAG_DOUBLE:
			case TAG_FLOAT:
			case TAG_STRING:
			case TAG_BOOL:
			case TAG_UUID:
				if (pl_) {
					buf.clear<false>();
					pl_->Get(field, buf);
					assertf(buf.size() > cnt, "{} > {}", buf.size(), cnt);
					dump_ << " -> "sv;
					dumpVariant(buf[cnt++]);
				}
				break;
			case TAG_ARRAY: {
				dump_ << '\n';
				indent(indentLevel);
				const uint32_t count = cjson_.GetVarUInt();
				dump_ << "Count: "sv << count;
				if (pl_) {
					dump_ << " -> ["sv;
					buf.clear<false>();
					pl_->Get(field, buf);
					if (pl_->Type().Field(field).IsFloatVector()) {
						assertf(buf.size() == 1, "{}", buf.size());
						++cnt;
						assertf(cnt == 1, "{}", cnt);
						const ConstFloatVectorView vect{buf[0]};
						if (vect.IsEmpty()) {
							dump_ << " -> <empty>"sv;
						} else {
							dump_ << " -> " << vect.Dimension().Value();
							if (vect.IsStripped()) {
								dump_ << "[<stripped>]"sv;
							} else {
								dump_ << '[';
								for (uint32_t i = 0; i < std::min(uint32_t(vect.Dimension()), kMaxArrayOutput); ++i) {
									if (i != 0) {
										dump_ << ", "sv;
									}
									dump_ << vect.Data()[i];
								}
								if (uint32_t(vect.Dimension()) > kMaxArrayOutput) {
									dump_ << ", ...]"sv;
								} else {
									dump_ << ']';
								}
							}
						}
					} else {
						assertf(buf.size() >= cnt + count, "{} >= {} + {}", buf.size(), cnt, count);
						for (size_t i = 0; i < std::min(count, kMaxArrayOutput); ++i) {
							if (i != 0) {
								dump_ << ", "sv;
							}
							dumpVariant(buf[cnt + i]);
						}
						if (count > kMaxArrayOutput) {
							dump_ << ", ...]"sv;
						} else {
							dump_ << ']';
						}
						cnt += count;
					}
				}
			} break;
			case TAG_NULL:
			case TAG_OBJECT:
			case TAG_END:
			default:
				assertrx(0);
		}
		dump_ << '\n';
	}

	void dumpCjsonObject(unsigned indentLevel) {
		using namespace std::string_view_literals;
		while (!cjson_.Eof()) {
			const ctag tag = cjson_.GetCTag();
			const TagType tagType = tag.Type();
			const auto field = tag.Field();
			if (tagType == TAG_END) {
				assertf(indentLevel > 0, "{}", indentLevel);
				--indentLevel;
			}
			dumpCTag(tagType, tag.Name(), field, indentLevel);
			if (field >= 0) {
				dumpIndexedField(field, tagType, indentLevel + 1);
			} else {
				dump_ << '\n';
				switch (tagType) {
					case TAG_VARINT:
					case TAG_DOUBLE:
					case TAG_STRING:
					case TAG_BOOL:
					case TAG_UUID:
					case TAG_FLOAT:
						indent(indentLevel + 1);
						dumpCjsonValue(tagType);
						dump_ << '\n';
						break;
					case TAG_NULL:
						break;
					case TAG_ARRAY:
						dumpCjsonArray(indentLevel + 1);
						break;
					case TAG_OBJECT:
						dumpCjsonObject(indentLevel + 1);
						break;
					case TAG_END:
						return;
					default:
						assertrx(0);
				}
			}
		}
	}

	void dumpCTag(TagType tagType, TagName tagName, int field, unsigned indentLevel) {
		using namespace std::string_view_literals;
		indent(indentLevel);
		dump_ << std::left << std::setw(10) << TagTypeToStr(tagType);
		dump_ << std::right << std::setw(4) << tagName.AsNumber();
		dump_ << std::right << std::setw(4) << field;
		if (tm_ && !tagName.IsEmpty()) {
			dump_ << " \""sv << tm_->tag2name(tagName) << '"';
		}
	}

	void dumpVariant(const Variant& v) {
		const bool isStr = v.Type().template Is<KeyValueType::String>();
		if (isStr) {
			dump_ << '"';
		}
		dump_ << v.template As<std::string>();
		if (isStr) {
			dump_ << '"';
		}
	}

	void indent(unsigned indLvl) const {
		for (unsigned i = 0; i < indLvl; ++i) {
			dump_ << tab_;
		}
	}

	Serializer& cjson_;
	std::ostream& dump_;
	const PL* pl_;
	const TagsMatcher* tm_;
	std::string_view tab_;
	std::array<size_t, kMaxIndexes> fieldsOutCnt_{};
};

void DumpCjson(Serializer& cjson, std::ostream& dump, const Payload* pl, const TagsMatcher* tm, std::string_view tab) {
	CJsonDumper dumper{cjson, dump, pl, tm, tab};
	dumper.Dump();
}

void DumpCjson(Serializer& cjson, std::ostream& dump, const ConstPayload* pl, const TagsMatcher* tm, std::string_view tab) {
	CJsonDumper dumper{cjson, dump, pl, tm, tab};
	dumper.Dump();
}

}  // namespace reindexer
