#include "tools/serilize/serializer.h"

#include <string_view>

#include "core/keyvalue/p_string.h"
#include "tools/errors.h"

namespace reindexer {

void Serializer::SkipPVString() { std::ignore = getPVStringPtr(); }
p_string Serializer::GetPVString() { return p_string(getPVStringPtr()); }

p_string Serializer::GetPSlice() {
	auto ret = reinterpret_cast<const l_string_hdr*>(buf_ + pos_);
	auto l = GetUInt32();
	checkbound(pos_, l, len_);
	pos_ += l;
	return p_string(ret);
}

[[noreturn]] void Serializer::throwUnderflowError(uint64_t pos, uint64_t need, uint64_t len) {
	throw Error(errParseBin, "Binary buffer underflow. Need more {} bytes, pos={},len={}", (pos + need) - len, pos, len);
}

[[noreturn]] void Serializer::throwScanIntError(std::string_view type) {
	throw Error(errParseBin, "Binary buffer broken - {} failed: pos={},len={}", type, pos_, len_);
}

[[noreturn]] void Serializer::throwUnknownTypeError(std::string_view type) {
	throw Error(errParseBin, "Unknown type {} while parsing binary buffer", type);
}

Variant Serializer::getPVStringVariant() { return Variant(GetPVString()); }

const v_string_hdr* Serializer::getPVStringPtr() {
	auto ret = reinterpret_cast<const v_string_hdr*>(buf_ + pos_);
	auto l = GetVarUInt();
	checkbound(pos_, l, len_);
	pos_ += l;
	return ret;
}

}  // namespace reindexer

