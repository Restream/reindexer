#pragma once

#include "core/payload/payloadiface.h"

namespace reindexer {

class TagsMatcher;
class Serializer;
class WrSerializer;
class CJsonDecoder {
public:
	CJsonDecoder(TagsMatcher &tagsMatcher);
	Error Decode(Payload *pl, Serializer &rdSer, WrSerializer &wrSer);

protected:
	bool decodeCJson(Payload *pl, Serializer &rdser, WrSerializer &wrser);

	TagsMatcher &tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];

	h_vector<int, 8> tagsPath_;
	Error lastErr_;
};

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);

}  // namespace reindexer
