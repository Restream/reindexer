#include "jsonbuilder.h"

namespace reindexer::builders {

JsonBuilder::JsonBuilder(WrSerializer& ser, ObjType type, const TagsMatcher* tm, bool emitTrailingForFloat)
	: ser_(&ser), tm_(tm), type_(type), emitTrailingForFloat_(emitTrailingForFloat) {
	switch (type_) {
		case ObjType::TypeArray:
			(*ser_) << '[';
			break;
		case ObjType::TypeObject:
			(*ser_) << '{';
			break;
		case ObjType::TypeObjectArray:
		case ObjType::TypePlain:
			break;
	}
}

std::string_view JsonBuilder::getNameByTag(TagName tagName) { return tagName.IsEmpty() ? std::string_view{} : tm_->tag2name(tagName); }

void JsonBuilder::End() {
	switch (type_) {
		case ObjType::TypeArray:
			(*ser_) << ']';
			break;
		case ObjType::TypeObject:
			(*ser_) << '}';
			break;
		case ObjType::TypeObjectArray:
		case ObjType::TypePlain:
			break;
	}
	type_ = ObjType::TypePlain;
}

JsonBuilder JsonBuilder::Object(std::string_view name, int /*size*/) {
	putName(name);
	return JsonBuilder(*ser_, ObjType::TypeObject, tm_, emitTrailingForFloat_);
}

JsonBuilder JsonBuilder::Array(std::string_view name, int /*size*/) {
	putName(name);
	return JsonBuilder(*ser_, ObjType::TypeArray, tm_, emitTrailingForFloat_);
}

void JsonBuilder::putName(std::string_view name) {
	if (count_++) {
		(*ser_) << ',';
	}
	if (name.data()) {	// -V547
		ser_->PrintJsonString(name);
		(*ser_) << ':';
	}
}

void JsonBuilder::Put(std::string_view name, std::string_view arg, int /*offset*/) {
	putName(name);
	ser_->PrintJsonString(arg);
}

void JsonBuilder::Put(std::string_view name, Uuid arg, int /*offset*/) {
	putName(name);
	ser_->PrintJsonUuid(arg);
}

void JsonBuilder::Raw(std::string_view name, std::string_view arg) {
	putName(name);
	(*ser_) << arg;
}

void JsonBuilder::Null(std::string_view name) {
	putName(name);
	(*ser_) << "null";
}

void JsonBuilder::Put(std::string_view name, const Variant& kv, int offset) {
	kv.Type().EvaluateOneOf(
		[&](KeyValueType::Int) { Put(name, int(kv), offset); }, [&](KeyValueType::Int64) { Put(name, int64_t(kv), offset); },
		[&](KeyValueType::Double) { Put(name, double(kv), offset); }, [&](KeyValueType::Float) { Put(name, float(kv), offset); },
		[&](KeyValueType::String) { Put(name, std::string_view(kv), offset); }, [&](KeyValueType::Null) { Null(name); },
		[&](KeyValueType::Bool) { Put(name, bool(kv), offset); },
		[&](KeyValueType::Tuple) {
			auto arrNode = Array(name);
			for (auto& val : kv.getCompositeValues()) {
				arrNode.Put({nullptr, 0}, val, offset);
			}
		},
		[&](KeyValueType::Uuid) { Put(name, Uuid{kv}, offset); },
		[](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) noexcept {});
}

}  // namespace reindexer::builders
