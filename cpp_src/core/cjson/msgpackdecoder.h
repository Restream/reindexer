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
	explicit MsgPackDecoder(std::string_view buf, Payload& pl, WrSerializer& wrser, TagsMatcher& tagsMatcher, size_t& offset,
							FloatVectorsHolderVector& floatVectorsHolder, ScalarIndexesSetT& objectScalarIndexes) noexcept
		: tm_(tagsMatcher),
		  data_(buf),
		  pl_(pl),
		  wrSer_(wrser),
		  offset_(offset),
		  floatVectorsHolder_(floatVectorsHolder),
		  objectScalarIndexes_(objectScalarIndexes) {}
	Error Decode() noexcept;

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
	void decode(CJsonBuilder& builder, const msgpack_object& obj, TagName);
	template <typename Validator>
	void decode(CJsonBuilder& builder, const msgpack_object& obj, TagName, const Validator&);
	void decodeFloatVector(const PayloadFieldType&, int indexNumber, CJsonBuilder&, TagName, const msgpack_object* const begin,
						   const msgpack_object* const end, size_t arraySize);
	void decodeArray(const PayloadFieldType&, int& plFieldPos, int indexNumber, CJsonBuilder&, TagName, const msgpack_object* const begin,
					 const msgpack_object* const end, size_t arraySize);
	void decodeNestedArray(const PayloadFieldType&, int& plFieldPos, int indexNumber, CJsonBuilder&, const msgpack_object* const begin,
						   const msgpack_object* const end);
	Variant msgpackValue2Variant(const PayloadFieldType&, const msgpack_object&);

	TagName decodeKeyToTag(const msgpack_object_kv& obj);

	template <typename T, typename Validator>
	void setValue(CJsonBuilder&, const T& value, TagName, const Validator&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }

	TagsMatcher& tm_;
	TagsPath tagsPath_;
	MsgPackParser parser_;
	int32_t arrayLevel_ = 0;

	std::string_view data_;
	Payload& pl_;
	WrSerializer& wrSer_;
	size_t& offset_;
	FloatVectorsHolderVector& floatVectorsHolder_;
	ScalarIndexesSetT& objectScalarIndexes_;
};

constexpr std::string_view ToString(msgpack_object_type type) noexcept;

}  // namespace reindexer
