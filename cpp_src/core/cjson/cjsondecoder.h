#pragma once

#include "core/payload/payloadiface.h"

class ctag;

namespace reindexer {

class TagsMatcher;
class Serializer;
class WrSerializer;

class CJsonDecoder {
public:
	CJsonDecoder(TagsMatcher &tagsMatcher);
	CJsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet &filter);

	Error Decode(Payload *pl, Serializer &rdSer, WrSerializer &wrSer);

protected:
	bool decodeCJson(Payload *pl, Serializer &rdser, WrSerializer &wrser);

	TagsMatcher &tagsMatcher_;
	FieldsSet filter_;
	h_vector<int, 8> tagsPath_;
	Error lastErr_;
};

void skipCjsonTag(ctag tag, Serializer &rdser);
void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);
KeyRef cjsonValueToKeyRef(int tag, Serializer &rdser, const PayloadFieldType &pt, Error &err);

}  // namespace reindexer
