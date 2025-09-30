#pragma once

#include "core/payload/payloadiface.h"
#include "gason/gason.h"

namespace reindexer {

class FloatVectorsHolderVector;

namespace builders {
class CJsonBuilder;
}  // namespace builders
using builders::CJsonBuilder;

namespace item_fields_validator {

class SparseValidator;

}  // namespace item_fields_validator

class [[nodiscard]] JsonDecoder {
public:
	explicit JsonDecoder(TagsMatcher& tagsMatcher, const FieldsSet* filter = nullptr) noexcept
		: tagsMatcher_(tagsMatcher), filter_(filter) {}
	Error Decode(Payload& pl, WrSerializer& wrSer, const gason::JsonValue& v, FloatVectorsHolderVector&);
	void Decode(std::string_view json, CJsonBuilder&, const TagsPath& fieldPath, FloatVectorsHolderVector&);

private:
	void decodeJsonObject(const gason::JsonValue& root, CJsonBuilder&, FloatVectorsHolderVector&);
	void decodeJsonObject(Payload&, CJsonBuilder&, const gason::JsonValue&, FloatVectorsHolderVector&, Matched);
	void decodeJson(Payload*, CJsonBuilder&, const gason::JsonValue&, TagName, FloatVectorsHolderVector&, Matched);
	void decodeJsonSparse(Payload*, CJsonBuilder&, const gason::JsonValue&, TagName, FloatVectorsHolderVector&, Matched,
						  const item_fields_validator::SparseValidator&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tagsMatcher_;
	TagsPath tagsPath_;
	const FieldsSet* filter_{nullptr};
	int32_t arrayLevel_{0};
	ScalarIndexesSetT objectScalarIndexes_;
};

}  // namespace reindexer
