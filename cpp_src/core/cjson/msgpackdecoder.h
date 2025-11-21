#pragma once

#include "core/payload/payloadiface.h"
#include "tools/errors.h"
#include "vendor/msgpack/msgpackparser.h"

struct msgpack_object;

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class FloatVectorsHolderVector;

namespace builders {
class CJsonBuilder;
}  // namespace builders
using builders::CJsonBuilder;

class [[nodiscard]] MsgPackDecoder {
public:
	explicit MsgPackDecoder(TagsMatcher& tagsMatcher) noexcept : tm_(tagsMatcher) {}
	Error Decode(std::string_view buf, Payload& pl, WrSerializer& wrser, size_t& offset, FloatVectorsHolderVector&) noexcept;

private:
	struct [[nodiscard]] ArrayAnalizeResult {
		ObjType arrayType_{ObjType::TypeArray};
		bool isNested_{false};
		size_t outterSize_{0};
		size_t fullSize_{0};
	};
	struct [[nodiscard]] FastArrayAnalizeResult {
		bool isNested_{false};
		size_t outterSize_{0};
	};
	static ArrayAnalizeResult analizeArray(const msgpack_object* const begin, const msgpack_object* const end);
	static FastArrayAnalizeResult fastAnalizeArray(const msgpack_object* const begin, const msgpack_object* const end);
	void decode(Payload& pl, CJsonBuilder& builder, const msgpack_object& obj, TagName, FloatVectorsHolderVector&);
	template <typename Validator>
	void decode(Payload& pl, CJsonBuilder& builder, const msgpack_object& obj, TagName, FloatVectorsHolderVector&, const Validator&);
	void decodeFloatVector(Payload&, const PayloadFieldType&, int indexNumber, CJsonBuilder&, TagName, const msgpack_object* const begin,
						   const msgpack_object* const end, size_t arraySize, FloatVectorsHolderVector&);
	void decodeArray(Payload&, const PayloadFieldType&, int& plFieldPos, int indexNumber, CJsonBuilder&, TagName,
					 const msgpack_object* const begin, const msgpack_object* const end, size_t arraySize);
	void decodeNestedArray(Payload&, const PayloadFieldType&, int& plFieldPos, int indexNumber, CJsonBuilder&,
						   const msgpack_object* const begin, const msgpack_object* const end);
	Variant msgpackValue2Variant(const PayloadFieldType&, const msgpack_object&);

	TagName decodeKeyToTag(const msgpack_object_kv& obj);

	template <typename T, typename Validator>
	void setValue(Payload&, CJsonBuilder&, const T& value, TagName, const Validator&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tm_;
	TagsPath tagsPath_;
	MsgPackParser parser_;
	int32_t arrayLevel_ = 0;
	ScalarIndexesSetT objectScalarIndexes_;
};

constexpr std::string_view ToString(msgpack_object_type type) noexcept;

}  // namespace reindexer
