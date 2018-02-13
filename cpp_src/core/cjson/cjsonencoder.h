#pragma once

#include "core/item.h"

namespace reindexer {

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
