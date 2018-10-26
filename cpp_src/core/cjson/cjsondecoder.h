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
	CJsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter);

	Error Decode(Payload *pl, Serializer &rdSer, WrSerializer &wrSer);

protected:
	bool decodeCJson(Payload *pl, Serializer &rdser, WrSerializer &wrser, bool match);

	TagsMatcher &tagsMatcher_;
	const FieldsSet *filter_;
	TagsPath tagsPath_;
	Error lastErr_;
};

}  // namespace reindexer
