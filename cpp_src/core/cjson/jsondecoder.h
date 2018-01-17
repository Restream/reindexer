#pragma once

#include "core/item.h"

namespace reindexer {

class JsonDecoder {
public:
	JsonDecoder(TagsMatcher &tagsMatcher);
	Error Decode(Payload *pl, WrSerializer &wrSer, JsonValue &v);

protected:
	void decodeJson(Payload *pl, WrSerializer &wrser, JsonValue &v, int tag);
	void decodeJsonObject(Payload *pl, WrSerializer &wrser, JsonValue &v);
	TagsMatcher &tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];

	h_vector<int, 8> tagsPath_;
};

}  // namespace reindexer
