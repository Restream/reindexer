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
	KeyRefs ExtractFieldValue(const Payload *pl, const string &jsonPath);
	KeyRefs ExtractFieldValue(const Payload *pl, const TagsPath &fieldTags);

protected:
	bool encodeCJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser, bool match = true);
	bool getValueFromTuple(Serializer &rdser, const TagsPath &fieldTags, const Payload *pl, KeyRefs &res, bool arrayElements = false);

	int fieldsoutcnt_[maxIndexes];
	const TagsMatcher &tagsMatcher_;
	const JsonPrintFilter &filter_;
	int depthLevel;
};

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);

}  // namespace reindexer
