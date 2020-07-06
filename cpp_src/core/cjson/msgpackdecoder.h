#pragma once

#include "core/cjson/cjsonbuilder.h"
#include "core/payload/payloadiface.h"
#include "tools/errors.h"

struct msgpack_object;

namespace reindexer {

class TagsMatcher;
class WrSerializer;

class MsgPackDecoder {
public:
	explicit MsgPackDecoder(TagsMatcher* tagsMatcher);
	Error Decode(string_view buf, Payload* pl, WrSerializer& wrser, size_t& offset);

private:
	void decode(Payload* pl, CJsonBuilder& builder, const msgpack_object& obj, int tagName);
	void iterateOverArray(const msgpack_object* begin, const msgpack_object* end, Payload* pl, CJsonBuilder& builder);

	template <typename T>
	void setValue(Payload* pl, CJsonBuilder& builder, const T& value, int tagName);

	TagsMatcher* tm_;
	TagsPath tagsPath_;
};

}  // namespace reindexer
