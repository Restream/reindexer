#include "payloadfieldtype.h"
#include <sstream>
#include "core/index/index.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "estl/one_of.h"
#include "payloadfieldvalue.h"

namespace reindexer {

PayloadFieldType::PayloadFieldType(const Index& index, const IndexDef& indexDef) noexcept
	: type_(index.KeyType()),
	  name_(indexDef.Name()),
	  jsonPaths_(indexDef.JsonPaths()),
	  offset_(0),
	  arrayDims_(0),
	  isArray_(index.Opts().IsArray()) {
	// Float indexed fields are not supported (except for the float_vector for KNN indexes)
	assertrx_throw(!type_.Is<KeyValueType::Float>());
	if (index.IsFloatVector()) {
		arrayDims_ = index.FloatVectorDimension().Value();
		assertrx(type_.Is<KeyValueType::FloatVector>());
	} else if (indexDef.IndexType() == IndexType::IndexRTree) {
		arrayDims_ = 2;
	}
}

size_t PayloadFieldType::Sizeof() const noexcept { return IsArray() ? sizeof(PayloadFieldValue::Array) : ElemSizeof(); }

size_t PayloadFieldType::ElemSizeof() const noexcept {
	return Type().EvaluateOneOf(
		[](KeyValueType::Bool) noexcept { return sizeof(bool); }, [](KeyValueType::Int) noexcept { return sizeof(int); },
		[](OneOf<KeyValueType::Int64>) noexcept { return sizeof(int64_t); },
		[](OneOf<KeyValueType::Uuid>) noexcept { return sizeof(Uuid); }, [](KeyValueType::Double) noexcept { return sizeof(double); },
		[](KeyValueType::String) noexcept { return sizeof(p_string); },
		[](KeyValueType::FloatVector) noexcept { return sizeof(ConstFloatVectorView); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Float>) noexcept
			-> size_t {
			assertrx(0);
			abort();
		});
}

std::string PayloadFieldType::ToString() const {
	std::stringstream ss;
	ss << "{ type: " << type_.Name() << ", name: " << name_ << ", jsonpaths: [";
	for (auto jit = jsonPaths_.cbegin(); jit != jsonPaths_.cend(); ++jit) {
		if (jit != jsonPaths_.cbegin()) {
			ss << ", ";
		}
		ss << *jit;
	}
	ss << "] }";
	return ss.str();
}

}  // namespace reindexer
