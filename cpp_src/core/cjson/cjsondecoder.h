#pragma once

#include "core/item.h"

namespace reindexer {

class CJsonDecoder {
public:
	CJsonDecoder(TagsMatcher &tagsMatcher);
	Error Decode(Payload *pl, Serializer &rdSer, WrSerializer &wrSer);

protected:
	bool decodeCJson(Payload *pl, Serializer &rdser, WrSerializer &wrser);

	TagsMatcher &tagsMatcher_;
	int fieldsoutcnt_[maxIndexes];

	h_vector<int, 8> tagsPath_;
};

}  // namespace reindexer
