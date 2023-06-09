#pragma once

#include "cjsonbuilder.h"
#include "core/payload/payloadiface.h"
#include "gason/gason.h"

namespace reindexer {

class JsonDecoder {
public:
	explicit JsonDecoder(TagsMatcher &tagsMatcher);
	JsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter);
	Error Decode(Payload &pl, WrSerializer &wrSer, const gason::JsonValue &v);
	void Decode(std::string_view json, CJsonBuilder &builder, const TagsPath &fieldPath);

private:
	void decodeJsonObject(const gason::JsonValue &root, CJsonBuilder &builder);
	void decodeJsonObject(Payload &pl, CJsonBuilder &builder, const gason::JsonValue &v, bool match);
	void decodeJson(Payload *pl, CJsonBuilder &builder, const gason::JsonValue &v, int tag, bool match);
	bool isInArray() const noexcept { return arrayLevel_ > 0; }

	TagsMatcher &tagsMatcher_;
	TagsPath tagsPath_;
	const FieldsSet *filter_;
	int32_t arrayLevel_ = 0;
};

}  // namespace reindexer
