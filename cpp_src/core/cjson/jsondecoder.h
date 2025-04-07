#pragma once

#include "core/keyvalue/float_vectors_holder.h"
#include "core/payload/payloadiface.h"
#include "gason/gason.h"

namespace reindexer {

class CJsonBuilder;

class JsonDecoder {
public:
	explicit JsonDecoder(TagsMatcher& tagsMatcher, const FieldsSet* filter = nullptr) noexcept
		: tagsMatcher_(tagsMatcher), filter_(filter) {}
	Error Decode(Payload& pl, WrSerializer& wrSer, const gason::JsonValue& v, FloatVectorsHolderVector&);
	void Decode(std::string_view json, CJsonBuilder&, const TagsPath& fieldPath, FloatVectorsHolderVector&);

private:
	void decodeJsonObject(const gason::JsonValue& root, CJsonBuilder&, FloatVectorsHolderVector&);
	void decodeJsonObject(Payload& pl, CJsonBuilder&, const gason::JsonValue& v, FloatVectorsHolderVector&, Matched);
	void decodeJson(Payload* pl, CJsonBuilder&, const gason::JsonValue& v, TagName, FloatVectorsHolderVector&, Matched);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tagsMatcher_;
	TagsPath tagsPath_;
	const FieldsSet* filter_{nullptr};
	int32_t arrayLevel_{0};
	ScalarIndexesSetT objectScalarIndexes_;
};

}  // namespace reindexer
