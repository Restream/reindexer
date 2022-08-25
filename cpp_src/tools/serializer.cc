#include "serializer.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "tools/errors.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/itoa/itoa.h"

namespace reindexer {

Variant Serializer::GetRawVariant(KeyValueType type) {
	switch (type) {
		case KeyValueInt:
			return Variant(int(GetVarint()));
		case KeyValueBool:
			return Variant(bool(GetVarUint()));
		case KeyValueInt64:
			return Variant(int64_t(GetVarint()));
		case KeyValueDouble:
			return Variant(GetDouble());
		case KeyValueString:
			return Variant(GetPVString());
		case KeyValueNull:
			return Variant();
		default:
			throw Error(errParseBin, "Unknown type %d while parsing binary buffer", type);
	}
}

p_string Serializer::GetPVString() {
	auto ret = reinterpret_cast<const v_string_hdr *>(buf_ + pos_);
	auto l = GetVarUint();
	checkbound(pos_, l, len_);
	pos_ += l;
	return p_string(ret);
}

p_string Serializer::GetPSlice() {
	auto ret = reinterpret_cast<const l_string_hdr *>(buf_ + pos_);
	auto l = GetUInt32();
	checkbound(pos_, l, len_);
	pos_ += l;
	return p_string(ret);
}

[[noreturn]] void Serializer::throwUnderflowError(uint64_t pos, uint64_t need, uint64_t len) {
	throw Error(errParseBin, "Binary buffer underflow. Need more %d bytes, pos=%d,len=%d", (pos + need) - len, pos, len);
}

[[noreturn]] void Serializer::throwScanIntError(std::string_view type) {
	throw Error(errParseBin, "Binary buffer broken - %s failed: pos=%d,len=%d", type, pos_, len_);
}

static unsigned uint32ByteSize(uint32_t value) noexcept {
	unsigned bytes = 1;
	if (value >= 0x80) {
		++bytes;
		value >>= 7;
		if (value >= 0x80) {
			++bytes;
			value >>= 7;
			if (value >= 0x80) {
				++bytes;
				value >>= 7;
				if (value >= 0x80) {
					++bytes;
					value >>= 7;
				}
			}
		}
	}
	return bytes;
}

void WrSerializer::VStringHelper::End() {
	if (ser_) {
		int size = ser_->len_ - pos_;
		if (size < 0) {
			throw Error(errParseBin, "Size of object is unexpedetly negative: %d", size);
		}
		if (size == 0) {
			ser_->grow(1);
			uint32_pack(0, ser_->buf_ + pos_);
			++ser_->len_;
		} else {
			unsigned bytesToGrow = uint32ByteSize(size);
			ser_->grow(bytesToGrow);
			ser_->len_ += bytesToGrow;
			memmove(&ser_->buf_[0] + pos_ + bytesToGrow, &ser_->buf_[0] + pos_, size);
			uint32_pack(size, ser_->buf_ + pos_);
		}
		ser_ = nullptr;
	}
}

WrSerializer &WrSerializer::operator<<(double v) {
	grow(32);
	double_conversion::StringBuilder builder(reinterpret_cast<char *>(buf_ + len_), 32);
	int flags =
		double_conversion::DoubleToStringConverter::UNIQUE_ZERO | double_conversion::DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN;
	double_conversion::DoubleToStringConverter dc(flags, NULL, NULL, 'e', -6, 21, 0, 0);

	dc.ToShortest(v, &builder);
	len_ += builder.position();

	return *this;
}

void WrSerializer::PrintJsonString(std::string_view str) {
	const char *s = str.data();
	size_t l = str.size();
	grow(l * 6 + 3);
	char *d = reinterpret_cast<char *>(buf_ + len_);
	*d++ = '"';

	while (l--) {
		unsigned c = *s++;
		switch (c) {
			case '\b':
				*d++ = '\\';
				*d++ = 'b';
				break;
			case '\f':
				*d++ = '\\';
				*d++ = 'f';
				break;
			case '\n':
				*d++ = '\\';
				*d++ = 'n';
				break;
			case '\r':
				*d++ = '\\';
				*d++ = 'r';
				break;
			case '\t':
				*d++ = '\\';
				*d++ = 't';
				break;
			case '\\':
				*d++ = '\\';
				*d++ = '\\';
				break;
			case '"':
				*d++ = '\\';
				*d++ = '"';
				break;
			default:
				if (c < 0x20) {
					*d++ = '\\';
					*d++ = 'u';
					d = u32toax(c, d, 4);
				} else {
					*d++ = c;
				}
		}
	}
	*d++ = '"';
	len_ = d - reinterpret_cast<char *>(buf_);
}

const int kHexDumpBytesInRow = 16;

void WrSerializer::PrintHexDump(std::string_view str) {
	grow((kHexDumpBytesInRow * 4 + 12) * (1 + (str.size() / kHexDumpBytesInRow)));

	char *d = reinterpret_cast<char *>(buf_ + len_);

	for (int row = 0; row < int(str.size()); row += kHexDumpBytesInRow) {
		d = u32toax(row, d, 8);
		*d++ = ' ';
		*d++ = ' ';
		for (int i = row; i < row + kHexDumpBytesInRow; i++) {
			if (i < int(str.size())) {
				d = u32toax(unsigned(str[i]) & 0xFF, d, 2);
			} else {
				*d++ = ' ';
				*d++ = ' ';
			}
			*d++ = ' ';
		}
		*d++ = ' ';
		for (int i = row; i < row + kHexDumpBytesInRow; i++) {
			char c = (i < int(str.size()) && unsigned(str[i]) > 0x20) ? str[i] : '.';
			*d++ = c;
		}
		*d++ = '\n';
	}
	len_ = d - reinterpret_cast<char *>(buf_);
}

std::unique_ptr<uint8_t[]> WrSerializer::DetachLStr() {
	reinterpret_cast<l_string_hdr *>(buf_)->length = len_ - sizeof(uint32_t);
	return DetachBuf();
}

}  // namespace reindexer
