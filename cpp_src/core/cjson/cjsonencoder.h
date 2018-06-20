#pragma once

#include "core/payload/payloadiface.h"
#include "jsonprintfilter.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class Serializer;

class CJsonEncoder {
public:
	CJsonEncoder(const TagsMatcher &tagsMatcher, const JsonPrintFilter &filter);

	void Encode(ConstPayload *pl, WrSerializer &wrSer);

protected:
	bool encodeCJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser, bool match = true);

	int fieldsoutcnt_[maxIndexes];
	const TagsMatcher &tagsMatcher_;
	const JsonPrintFilter &filter_;
};

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);

}  // namespace reindexer
