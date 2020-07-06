#include "msgpackbuilder.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"
#include "vendor/msgpack/msgpack.h"

namespace reindexer {

MsgPackBuilder::MsgPackBuilder(WrSerializer &wrser, ObjType type, size_t size)
	: tm_(nullptr), tagsLengths_(nullptr), type_(type), tagIndex_(nullptr) {
	msgpack_packer_init(&packer_, reinterpret_cast<void *>(&wrser), msgpack_wrserializer_write);
	init(size);
}

MsgPackBuilder::MsgPackBuilder(msgpack_packer &packer, ObjType type, size_t size)
	: tm_(nullptr), packer_(packer), tagsLengths_(nullptr), type_(type), tagIndex_(nullptr) {
	init(size);
}

MsgPackBuilder::MsgPackBuilder(WrSerializer &wrser, const TagsLengths *tagsLengths, int *startTag, ObjType type, TagsMatcher *tm)
	: tm_(tm), tagsLengths_(tagsLengths), type_(type), tagIndex_(startTag) {
	assert(startTag);
	msgpack_packer_init(&packer_, reinterpret_cast<void *>(&wrser), msgpack_wrserializer_write);
	init(KUnknownFieldSize);
}

MsgPackBuilder::MsgPackBuilder(msgpack_packer &packer, const TagsLengths *tagsLengths, int *startTag, ObjType type, TagsMatcher *tm)
	: tm_(tm), packer_(packer), tagsLengths_(tagsLengths), type_(type), tagIndex_(startTag) {
	assert(startTag);
	init(KUnknownFieldSize);
}

MsgPackBuilder::~MsgPackBuilder() { End(); }

void MsgPackBuilder::Array(int tagName, Serializer &ser, int tagType, int count) {
	checkIfCorrectArray(tagName);
	skipTag();
	packKeyName(tagName);
	packArray(count);
	while (count--) packCJsonValue(tagType, ser);
}

MsgPackBuilder &MsgPackBuilder::Json(string_view name, string_view arg) {
	gason::JsonParser parser;
	auto root = parser.Parse(arg);
	appendJsonObject(name, root);
	return *this;
}

MsgPackBuilder &MsgPackBuilder::End() {
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
	return *this;
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

void MsgPackBuilder::packCJsonValue(int tagType, Serializer &rdser) {
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
			packValue(string(rdser.GetVString()));
			break;
		case TAG_NULL:
			packNil();
			break;
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", ctag(tagType).TypeName());
	}
}

void MsgPackBuilder::appendJsonObject(string_view name, const gason::JsonNode &obj) {
	auto type = obj.value.getTag();
	switch (type) {
		case gason::JSON_STRING: {
			Put(name, obj.As<string_view>());
			break;
		}
		case gason::JSON_NUMBER: {
			Put(name, obj.As<int64_t>());
			break;
		}
		case gason::JSON_DOUBLE: {
			Put(name, obj.As<double>());
			break;
		}
		case gason::JSON_OBJECT:
		case gason::JSON_ARRAY: {
			int size = 0;
			for (const auto &node : obj) {
				(void)node;
				++size;
			}
			if (type == gason::JSON_OBJECT) {
				auto pack = Object(name, size);
				for (const auto &node : obj) {
					pack.appendJsonObject(string_view(node.key), node);
				}
			} else {
				auto pack = Array(name, size);
				for (const auto &node : obj) {
					pack.appendJsonObject(string_view(), node);
				}
			}
			break;
		}
		case gason::JSON_TRUE: {
			Put(string_view(obj.key), true);
			break;
		}
		case gason::JSON_FALSE: {
			Put(string_view(obj.key), false);
			break;
		}
		case gason::JSON_NULL: {
			Null(string_view(obj.key));
			break;
		}
		default:
			throw(Error(errLogic, "Unexpected json tag: %d", obj.value.getTag()));
	}
}

}  // namespace reindexer
