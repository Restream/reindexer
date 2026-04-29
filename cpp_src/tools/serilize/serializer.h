#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "core/cjson/ctag.h"
#include "core/enums.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "core/rank_t.h"
#include "tools/varint.h"

namespace reindexer {

struct p_string;
struct v_string_hdr;
class chunk;

class [[nodiscard]] Serializer {
public:
	Serializer(const void* buf, size_t len) noexcept : buf_(static_cast<const uint8_t*>(buf)), len_(len), pos_(0) {}
	explicit Serializer(std::string_view buf) noexcept : buf_(reinterpret_cast<const uint8_t*>(buf.data())), len_(buf.length()), pos_(0) {}
	bool Eof() const noexcept { return pos_ >= len_; }
	RX_ALWAYS_INLINE KeyValueType GetKeyValueType() { return KeyValueType::FromNumber(GetVarUInt()); }
	Variant GetVariant() {
		const KeyValueType type = GetKeyValueType();
		if (type.Is<KeyValueType::Tuple>()) {
			VariantArray compositeValues;
			uint64_t count = GetVarUInt();
			compositeValues.reserve(count);
			for (size_t i = 0; i < count; ++i) {
				compositeValues.emplace_back(GetVariant());
			}
			return Variant(compositeValues);
		} else if (type.Is<KeyValueType::FloatVector>()) {
			return Variant(GetFloatVectorView());
		} else {
			return GetRawVariant(type);
		}
	}
	Variant GetRawVariant(KeyValueType type) {
		return type.EvaluateOneOf(
			[this](KeyValueType::Int) { return Variant(int(GetVarint())); },
			[this](KeyValueType::Bool) { return Variant(bool(GetVarUInt())); },
			[this](KeyValueType::Int64) { return Variant(int64_t(GetVarint())); },
			[this](KeyValueType::Double) { return Variant(GetDouble()); }, [this](KeyValueType::String) { return getPVStringVariant(); },
			[](KeyValueType::Null) noexcept { return Variant(); }, [this](KeyValueType::Uuid) { return Variant{GetUuid()}; },
			[this](KeyValueType::Float) { return Variant(GetFloat()); },
			[this,
			 &type](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto)
				-> Variant { throwUnknownTypeError(type.Name()); });
	}
	void SkipRawVariant(KeyValueType type) {
		type.EvaluateOneOf(
			[this](KeyValueType::Int) { std::ignore = GetVarint(); }, [this](KeyValueType::Bool) { std::ignore = GetVarUInt(); },
			[this](KeyValueType::Int64) { std::ignore = GetVarint(); }, [this](KeyValueType::Double) { std::ignore = GetDouble(); },
			[this](KeyValueType::String) { std::ignore = getPVStringPtr(); }, [](KeyValueType::Null) noexcept {},
			[this](KeyValueType::Uuid) { std::ignore = GetUuid(); }, [this](KeyValueType::Float) { std::ignore = GetFloat(); },
			[this, &type](
				concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) {
				throwUnknownTypeError(type.Name());
			});
	}
	RX_ALWAYS_INLINE std::string_view GetSlice() {
		auto l = GetUInt32();
		std::string_view b(reinterpret_cast<const char*>(buf_ + pos_), l);
		checkbound(pos_, b.size(), len_);
		pos_ += b.size();
		return b;
	}
	RX_ALWAYS_INLINE uint8_t GetUInt8() {
		uint8_t ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	RX_ALWAYS_INLINE uint32_t GetUInt32() {
		uint32_t ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	RX_ALWAYS_INLINE uint64_t GetUInt64() {
		uint64_t ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	RX_ALWAYS_INLINE double GetDouble() {
		double ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	RX_ALWAYS_INLINE float GetFloat() {
		float ret;
		checkbound(pos_, sizeof(ret), len_);
		memcpy(&ret, buf_ + pos_, sizeof(ret));
		pos_ += sizeof(ret);
		return ret;
	}
	RX_ALWAYS_INLINE RankT GetRank() { return RankT{GetFloat()}; }
	ConstFloatVectorView GetFloatVectorView() {
		using Float = ConstFloatVectorView::DataType;
		unsigned dim = GetVarUInt();
		if (dim == 0) {
			return {};
		} else {
			const bool isStripped = dim & 1;
			dim >>= 1;
			if (isStripped) {
				return ConstFloatVectorView::CreateStripped(FloatVectorDimension(dim));
			} else {
				const size_t memSize = dim * sizeof(Float);
				checkbound(pos_, memSize, len_);
				const auto position = pos_;
				pos_ += memSize;
				return ConstFloatVectorView{std::span{reinterpret_cast<Float*>(buf_ + position), dim}};
			}
		}
	}
	void SkipUuid() { std::ignore = GetUuid(); }
	Uuid GetUuid() {
		const uint64_t v1 = GetUInt64();
		const uint64_t v2 = GetUInt64();
		return Uuid{v1, v2};
	}
	RX_ALWAYS_INLINE int64_t GetVarint() {
		auto l = scan_varint(len_ - pos_, buf_ + pos_);
		if (l == 0) {
			using namespace std::string_view_literals;
			throwScanIntError("scan_varint"sv);
		}

		checkbound(pos_, l, len_);
		pos_ += l;
		return parse_int64(l, buf_ + pos_ - l);
	}
	RX_ALWAYS_INLINE uint64_t GetVarUInt() {  // -V1071
		auto l = scan_varint(len_ - pos_, buf_ + pos_);
		if (l == 0) {
			using namespace std::string_view_literals;
			throwScanIntError("scan_varuint"sv);
		}
		checkbound(pos_, l, len_);
		pos_ += l;
		return parse_uint64(l, buf_ + pos_ - l);
	}
	RX_ALWAYS_INLINE ctag GetCTag() { return ctag::Deserialize(*this); }
	RX_ALWAYS_INLINE carraytag GetCArrayTag() { return carraytag::Deserialize(*this); }
	RX_ALWAYS_INLINE std::string_view GetVString() {
		auto l = GetVarUInt();
		checkbound(pos_, l, len_);
		pos_ += l;
		return {reinterpret_cast<const char*>(buf_ + pos_ - l), std::string_view::size_type(l)};
	}
	void SkipPVString();
	p_string GetPVString();
	p_string GetPSlice();
	Uuid GetStrUuid() { return Uuid{GetVString()}; }
	RX_ALWAYS_INLINE bool GetBool() { return bool(GetVarUInt()); }
	size_t Pos() const noexcept { return pos_; }
	void SetPos(size_t p) noexcept { pos_ = p; }
	const uint8_t* Buf() const noexcept { return buf_; }
	size_t Len() const noexcept { return len_; }
	void Reset() noexcept { pos_ = 0; }

private:
	RX_ALWAYS_INLINE void checkbound(uint64_t pos, uint64_t need, uint64_t len) {
		if (pos + need > len) {
			throwUnderflowError(pos, need, len);
		}
	}
	[[noreturn]] void throwUnderflowError(uint64_t pos, uint64_t need, uint64_t len);
	[[noreturn]] void throwScanIntError(std::string_view type);
	[[noreturn]] void throwUnknownTypeError(std::string_view type);
	Variant getPVStringVariant();
	const v_string_hdr* getPVStringPtr();

	const uint8_t* buf_{nullptr};
	size_t len_{0};
	size_t pos_{0};
};

}  // namespace reindexer

