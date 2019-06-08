#pragma once

#include "core/payload/payloadiface.h"
#include "gason/gason.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class CJsonBuilder;

class JsonDecoder {
public:
	JsonDecoder(TagsMatcher &tagsMatcher);
	JsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter);
	Error Decode(Payload *pl, WrSerializer &wrSer, const gason::JsonValue &v);

protected:
	void decodeJson(Payload *pl, CJsonBuilder &builder, const gason::JsonValue &v, int tag, bool match);
	void decodeJsonObject(Payload *pl, CJsonBuilder &builder, const gason::JsonValue &v, bool match);

	TagsMatcher &tagsMatcher_;
	TagsPath tagsPath_;
	const FieldsSet *filter_;
};

}  // namespace reindexer
