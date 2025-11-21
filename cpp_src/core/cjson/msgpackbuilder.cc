#include "msgpackbuilder.h"
#include "core/type_consts_helpers.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

namespace reindexer::builders {

MsgPackBuilder::MsgPackBuilder(WrSerializer& wrser, ObjType type, size_t size)
	: tm_(nullptr), tagsLengths_(nullptr), type_(type), tagIndex_(nullptr) {
	msgpack_packer_init(&packer_, reinterpret_cast<void*>(&wrser), msgpack_wrserializer_write);
	init(size);
}

MsgPackBuilder::MsgPackBuilder(msgpack_packer& packer, ObjType type, size_t size)
	: tm_(nullptr), packer_(packer), tagsLengths_(nullptr), type_(type), tagIndex_(nullptr) {
	init(size);
}

MsgPackBuilder::MsgPackBuilder(WrSerializer& wrser, const TagsLengths* tagsLengths, int* startTag, ObjType type, const TagsMatcher* tm)
	: tm_(tm), tagsLengths_(tagsLengths), type_(type), tagIndex_(startTag) {
	assertrx(startTag);
	msgpack_packer_init(&packer_, reinterpret_cast<void*>(&wrser), msgpack_wrserializer_write);
	init(KUnknownFieldSize);
}

MsgPackBuilder::MsgPackBuilder(msgpack_packer& packer, const TagsLengths* tagsLengths, int* startTag, ObjType type, const TagsMatcher* tm)
	: tm_(tm), packer_(packer), tagsLengths_(tagsLengths), type_(type), tagIndex_(startTag) {
	assertrx(startTag);
	init(KUnknownFieldSize);
}

void MsgPackBuilder::Array(concepts::TagNameOrIndex auto tagName, Serializer& ser, TagType tagType, int count) {
	skipTag();
	packKeyName(tagName);
	packArray(count);
	while (count--) {
		packCJsonValue(tagType, ser);
	}
}
template void MsgPackBuilder::Array(TagName, Serializer&, TagType, int);
template void MsgPackBuilder::Array(TagIndex, Serializer&, TagType, int);

void MsgPackBuilder::Json(std::string_view name, std::string_view arg) {
	gason::JsonParser parser;
	auto root = parser.Parse(arg);
	appendJsonObject(name, root);
}

void MsgPackBuilder::End() {
	switch (type_) {
		case ObjType::TypeObjectArray:
		case ObjType::TypeArray:
			break;
		case ObjType::TypeObject:
			skipTag();
			skipTagIfEqual(TagValues::EndArrayItem);
			break;
		case ObjType::TypePlain:
			break;
	}
	type_ = ObjType::TypePlain;
}

void MsgPackBuilder::init(int size) {
	if (size == KUnknownFieldSize && type_ != ObjType::TypePlain) {
		size = getTagSize();
	}
	switch (type_) {
		case ObjType::TypeObjectArray:
		case ObjType::TypeArray:
			packArray(size);
			break;
		case ObjType::TypeObject:
			packMap(size);
			break;
		case ObjType::TypePlain:
			break;
	}
}

void MsgPackBuilder::packCJsonValue(TagType tagType, Serializer& rdser) {
	switch (tagType) {
		case TAG_DOUBLE:
			packValue(rdser.GetDouble());
			break;
		case TAG_VARINT:
			packValue(rdser.GetVarint());
			break;
		case TAG_BOOL:
			packValue(rdser.GetBool());
			break;
		case TAG_STRING:
			packValue(rdser.GetVString());
			break;
		case TAG_UUID:
			packValue(rdser.GetUuid());
			break;
		case TAG_FLOAT:
			packValue(rdser.GetFloat());
			break;
		case TAG_NULL:
			packNil();
			break;
		case TAG_END:
		case TAG_ARRAY:
		case TAG_OBJECT:
			throw Error(errParseJson, "Unexpected cjson typeTag '{}' while parsing value", TagTypeToStr(tagType));
	}
}

void MsgPackBuilder::appendJsonObject(std::string_view name, const gason::JsonNode& obj) {
	auto type = obj.value.getTag();
	switch (type) {
		case gason::JsonTag::STRING: {
			Put(name, obj.As<std::string_view>(), 0);
			break;
		}
		case gason::JsonTag::NUMBER:
			Put(name, obj.As<int64_t>(), 0);
			break;
		case gason::JsonTag::DOUBLE: {
			Put(name, obj.As<double>(), 0);
			break;
		}
		case gason::JsonTag::OBJECT:
		case gason::JsonTag::ARRAY: {
			int size = 0;
			for (const auto& node : obj) {
				(void)node;
				++size;
			}
			if (type == gason::JsonTag::OBJECT) {
				auto pack = Object(name, size);
				for (const auto& node : obj) {
					pack.appendJsonObject(std::string_view(node.key), node);
				}
			} else {
				auto pack = Array(name, size);
				for (const auto& node : obj) {
					pack.appendJsonObject(std::string_view(), node);
				}
			}
			break;
		}
		case gason::JsonTag::JTRUE: {
			Put(std::string_view(obj.key), true, 0);
			break;
		}
		case gason::JsonTag::JFALSE: {
			Put(std::string_view(obj.key), false, 0);
			break;
		}
		case gason::JsonTag::JSON_NULL: {
			Null(std::string_view(obj.key));
			break;
		}
		case gason::JsonTag::EMPTY:
		default:
			throw(Error(errLogic, "Unexpected json tag for Object: {}", int(obj.value.getTag())));
	}
}

}  // namespace reindexer::builders
