#pragma once

#include "core/payload/payloadiface.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class Serializer;

class CJsonEncoder {
public:
	CJsonEncoder(const TagsMatcher &tagsMatcher);

	void Encode(ConstPayload *pl, WrSerializer &wrSer);

protected:
	bool encodeCJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser);

	const TagsMatcher &tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];
};

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);

}  // namespace reindexer
