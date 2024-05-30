#include "jsonbuilder.h"

namespace reindexer {

JsonBuilder::JsonBuilder(WrSerializer &ser, ObjType type, const TagsMatcher *tm, bool emitTrailingForFloat)
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

std::string_view JsonBuilder::getNameByTag(int tagName) { return tagName ? tm_->tag2name(tagName) : std::string_view(); }

JsonBuilder &JsonBuilder::End() {
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

	return *this;
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
	if (count_++) (*ser_) << ',';
	if (name.data()) {	// -V547
		ser_->PrintJsonString(name);
		(*ser_) << ':';
	}
}

JsonBuilder &JsonBuilder::Put(std::string_view name, std::string_view arg, int /*offset*/) {
	putName(name);
	ser_->PrintJsonString(arg);
	return *this;
}

JsonBuilder &JsonBuilder::Put(std::string_view name, Uuid arg, int /*offset*/) {
	putName(name);
	ser_->PrintJsonUuid(arg);
	return *this;
}

JsonBuilder &JsonBuilder::Raw(std::string_view name, std::string_view arg) {
	putName(name);
	(*ser_) << arg;
	return *this;
}

JsonBuilder &JsonBuilder::Null(std::string_view name) {
	putName(name);
	(*ser_) << "null";
	return *this;
}

JsonBuilder &JsonBuilder::Put(std::string_view name, const Variant &kv, int offset) {
	kv.Type().EvaluateOneOf(
		[&](KeyValueType::Int) { Put(name, int(kv), offset); }, [&](KeyValueType::Int64) { Put(name, int64_t(kv), offset); },
		[&](KeyValueType::Double) { Put(name, double(kv), offset); },
		[&](KeyValueType::String) { Put(name, std::string_view(kv), offset); }, [&](KeyValueType::Null) { Null(name); },
		[&](KeyValueType::Bool) { Put(name, bool(kv), offset); },
		[&](KeyValueType::Tuple) {
			auto arrNode = Array(name);
			for (auto &val : kv.getCompositeValues()) {
				arrNode.Put({nullptr, 0}, val, offset);
			}
		},
		[&](KeyValueType::Uuid) { Put(name, Uuid{kv}, offset); }, [](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) noexcept {});
	return *this;
}

}  // namespace reindexer
