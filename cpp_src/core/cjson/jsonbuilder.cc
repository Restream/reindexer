#include "jsonbuilder.h"
#include "tools/json2kv.h"

namespace reindexer {

JsonBuilder::JsonBuilder(WrSerializer &ser, ObjType type, const TagsMatcher *tm) : ser_(&ser), tm_(tm), type_(type) {
	switch (type_) {
		case ObjType::TypeArray:
			(*ser_) << '[';
			break;
		case ObjType::TypeObject:
			(*ser_) << '{';
			break;
		default:
			break;
	}
}

JsonBuilder::~JsonBuilder() { End(); }

std::string_view JsonBuilder::getNameByTag(int tagName) { return tagName ? tm_->tag2name(tagName) : std::string_view(); }

JsonBuilder &JsonBuilder::End() {
	switch (type_) {
		case ObjType::TypeArray:
			(*ser_) << ']';
			break;
		case ObjType::TypeObject:
			(*ser_) << '}';
			break;
		default:
			break;
	}
	type_ = ObjType::TypePlain;

	return *this;
}

JsonBuilder JsonBuilder::Object(std::string_view name, int /*size*/) {
	putName(name);
	return JsonBuilder(*ser_, ObjType::TypeObject, tm_);
}

JsonBuilder JsonBuilder::Array(std::string_view name, int /*size*/) {
	putName(name);
	return JsonBuilder(*ser_, ObjType::TypeArray, tm_);
}

void JsonBuilder::putName(std::string_view name) {
	if (count_++) (*ser_) << ',';
	if (name.data()) {	// -V547
		ser_->PrintJsonString(name);
		(*ser_) << ':';
	}
}

JsonBuilder &JsonBuilder::Put(std::string_view name, std::string_view arg) {
	putName(name);
	ser_->PrintJsonString(arg);
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

JsonBuilder &JsonBuilder::Put(std::string_view name, const Variant &kv) {
	switch (kv.Type()) {
		case KeyValueInt:
			return Put(name, int(kv));
		case KeyValueInt64:
			return Put(name, int64_t(kv));
		case KeyValueDouble:
			return Put(name, double(kv));
		case KeyValueString:
			return Put(name, std::string_view(kv));
		case KeyValueNull:
			return Null(name);
		case KeyValueBool:
			return Put(name, bool(kv));
		case KeyValueTuple: {
			auto arrNode = Array(name);
			for (auto &val : kv.getCompositeValues()) {
				arrNode.Put({nullptr, 0}, val);
			}
			return *this;
		}
		default:
			break;
	}
	return *this;
}

}  // namespace reindexer
