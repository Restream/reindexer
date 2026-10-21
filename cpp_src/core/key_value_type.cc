#include "key_value_type.h"
#include "core/type_consts_helpers.h"
#include "tools/errors.h"

namespace reindexer {

std::string_view KeyValueType::Name() const noexcept {
	using namespace std::string_view_literals;
	switch (value_) {
		case KVT::Int64:
			return "int64"sv;
		case KVT::Double:
			return "double"sv;
		case KVT::Float:
			return "float"sv;
		case KVT::String:
			return "string"sv;
		case KVT::Bool:
			return "bool"sv;
		case KVT::Null:
			return "null"sv;
		case KVT::Int:
			return "int"sv;
		case KVT::Undefined:
			return "undefined"sv;
		case KVT::Composite:
			return "composite"sv;
		case KVT::Tuple:
			return "tuple"sv;
		case KVT::Uuid:
			return "uuid"sv;
		case KVT::FloatVector:
			return "float_vector"sv;
	}
	assertrx(0);
	std::abort();
}

template <typename T>
[[noreturn]] void throwKVTExceptionImpl(std::string_view msg, const T& v) {
	throw Error(errParams, fmt::format("{}: '{}'", msg, v));
}
void KeyValueType::throwKVTException(std::string_view msg, std::string_view v) { throwKVTExceptionImpl(msg, v); }
void KeyValueType::throwKVTException(std::string_view msg, int v) { throwKVTExceptionImpl(msg, v); }
void KeyValueType::throwKVTException(std::string_view msg, TagType t) { throwKVTExceptionImpl(msg, TagTypeToStr(t)); }

}  // namespace reindexer
