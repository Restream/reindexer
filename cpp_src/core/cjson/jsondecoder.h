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
class SparseArrayValidator;

}  // namespace item_fields_validator

class [[nodiscard]] JsonDecoder {
public:
	explicit JsonDecoder(TagsMatcher& tagsMatcher, const FieldsSet* filter = nullptr) noexcept
		: tagsMatcher_(tagsMatcher), filter_(filter) {}
	Error Decode(Payload& pl, WrSerializer& wrSer, const gason::JsonValue& v, FloatVectorsHolderVector&);
	void Decode(std::string_view json, CJsonBuilder&, const TagsPath& fieldPath, FloatVectorsHolderVector&);

private:
	struct [[nodiscard]] HeteroArrayFastAnalizeResult {
		bool isHetero_{false};
		size_t outterSize_{0};
	};
	struct [[nodiscard]] HeteroArrayAnalizeResult {
		bool isHetero_{false};
		size_t outterSize_{0};
		size_t fullSize_{0};
	};
	HeteroArrayAnalizeResult analizeHeteroArray(const gason::JsonValue& array) const;
	HeteroArrayFastAnalizeResult fastAnalizeHeteroArray(const gason::JsonValue& array) const;
	void decodeHeteroArray(Payload&, CJsonBuilder&, const gason::JsonValue& array, int indexNumber, int& pos, KeyValueType fieldType,
						   std::string_view fieldName) const;
	void decodeJsonObject(const gason::JsonValue& root, CJsonBuilder&, FloatVectorsHolderVector&);
	void decodeJsonObject(Payload&, CJsonBuilder&, const gason::JsonValue&, FloatVectorsHolderVector&, Matched);
	void decodeJson(Payload*, CJsonBuilder&, const gason::JsonValue&, TagName, FloatVectorsHolderVector&, Matched);
	void decodeJsonSparse(Payload*, CJsonBuilder&, const gason::JsonValue&, TagName, FloatVectorsHolderVector&, Matched,
						  const item_fields_validator::SparseValidator&);
	void decodeJsonArraySparse(Payload*, CJsonBuilder&, const gason::JsonValue&, TagName, FloatVectorsHolderVector&, Matched,
							   item_fields_validator::SparseArrayValidator&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tagsMatcher_;
	TagsPath tagsPath_;
	const FieldsSet* filter_{nullptr};
	int32_t arrayLevel_{0};
	ScalarIndexesSetT objectScalarIndexes_;
};

}  // namespace reindexer
