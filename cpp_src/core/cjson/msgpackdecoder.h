#pragma once

#include "core/payload/payloadiface.h"
#include "tools/errors.h"
#include "vendor/msgpack/msgpackparser.h"

struct msgpack_object;

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class FloatVectorsHolderVector;
class CJsonBuilder;

class MsgPackDecoder {
public:
	explicit MsgPackDecoder(TagsMatcher& tagsMatcher) noexcept : tm_(tagsMatcher) {}
	Error Decode(std::string_view buf, Payload& pl, WrSerializer& wrser, size_t& offset, FloatVectorsHolderVector&);

private:
	void decode(Payload& pl, CJsonBuilder&, const msgpack_object& obj, int tagName, FloatVectorsHolderVector&);

	int decodeKeyToTag(const msgpack_object_kv& obj);

	template <typename T>
	void setValue(Payload& pl, CJsonBuilder&, const T& value, int tagName);
	bool isInArray() const noexcept { return arrayLevel_ > 0; }

	TagsMatcher& tm_;
	TagsPath tagsPath_;
	MsgPackParser parser_;
	int32_t arrayLevel_ = 0;
	ScalarIndexesSetT objectScalarIndexes_;
};

constexpr std::string_view ToString(msgpack_object_type type);

}  // namespace reindexer
