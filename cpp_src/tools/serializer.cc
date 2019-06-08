#include "tools/serializer.h"
#include <vendor/double-conversion/double-conversion.h>
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "estl/chunk_buf.h"
#include "itoa/itoa.h"
#include "tools/errors.h"
#include "tools/varint.h"

namespace reindexer {

Serializer::Serializer(const void *_buf, int _len) : buf(static_cast<const uint8_t *>(_buf)), len(_len), pos(0) {}
Serializer::Serializer(const string_view &buf) : buf(reinterpret_cast<const uint8_t *>(buf.data())), len(buf.length()), pos(0) {}

bool Serializer::Eof() { return pos >= len; }

Variant Serializer::GetVariant() {
	KeyValueType type = KeyValueType(GetVarUint());
	switch (type) {
		case KeyValueTuple: {
			VariantArray compositeValues;
			uint64_t count = GetVarUint();
			compositeValues.reserve(count);
			for (size_t i = 0; i < count; ++i) {
				compositeValues.push_back(GetVariant());
			}
			return Variant(compositeValues);
		}
		default:
			return GetRawVariant(type);
	}
}

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

inline static void checkbound(int pos, int need, int len) {
	if (pos + need > len) {
		throw Error(errParseBin, "Binary buffer underflow. Need more %d bytes, pos=%d,len=%d", need, pos, len);
	}
}

string_view Serializer::GetSlice() {
	uint32_t l = GetUInt32();
	string_view b(reinterpret_cast<const char *>(buf + pos), l);
	checkbound(pos, b.size(), len);
	pos += b.size();
	return b;
}

uint32_t Serializer::GetUInt32() {
	uint32_t ret;
	checkbound(pos, sizeof(ret), len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

uint64_t Serializer::GetUInt64() {
	uint64_t ret;
	checkbound(pos, sizeof(ret), len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

double Serializer::GetDouble() {
	double ret;
	checkbound(pos, sizeof(ret), len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

int64_t Serializer::GetVarint() {
	int l = scan_varint(len - pos, buf + pos);
	if (l == 0) {
		throw Error(errParseBin, "Binary buffer broken - scan_varint failed: pos=%d,len=%d", pos, len);
	}

	checkbound(pos, l, len);
	pos += l;
	return unzigzag64(parse_uint64(l, buf + pos - l));
}

uint64_t Serializer::GetVarUint() {
	int l = scan_varint(len - pos, buf + pos);
	if (l == 0) {
		throw Error(errParseBin, "Binary buffer broken - scan_varint failed: pos=%d,len=%d", pos, len);
	}
	checkbound(pos, l, len);
	pos += l;
	return parse_uint64(l, buf + pos - l);
}

string_view Serializer::GetVString() {
	int l = GetVarUint();
	checkbound(pos, l, len);
	pos += l;
	return string_view(reinterpret_cast<const char *>(buf + pos - l), l);
}

p_string Serializer::GetPVString() {
	auto ret = reinterpret_cast<const v_string_hdr *>(buf + pos);
	int l = GetVarUint();
	checkbound(pos, l, len);
	pos += l;
	return p_string(ret);
}

bool Serializer::GetBool() { return bool(GetVarUint()); }

WrSerializer::WrSerializer() : buf_(inBuf_), len_(0), cap_(sizeof(inBuf_)) {}

WrSerializer::WrSerializer(chunk &&ch) : buf_(ch.data_), len_(ch.len_), cap_(ch.cap_) {
	if (!buf_) {
		buf_ = inBuf_;
		cap_ = sizeof(inBuf_);
		len_ = 0;
	}
	ch.data_ = nullptr;
	ch.len_ = 0;
	ch.cap_ = 0;
	ch.offset_ = 0;
}

WrSerializer::~WrSerializer() {
	if (buf_ != inBuf_) delete[] buf_;
}

void WrSerializer::PutVariant(const Variant &kv) {
	PutVarUint(kv.Type());
	switch (kv.Type()) {
		case KeyValueTuple: {
			auto compositeValues = kv.getCompositeValues();
			PutVarUint(compositeValues.size());
			for (auto &v : compositeValues) {
				PutVariant(v);
			}
			break;
		}
		default:
			PutRawVariant(kv);
	}
}

void WrSerializer::PutRawVariant(const Variant &kv) {
	switch (kv.Type()) {
		case KeyValueBool:
			PutBool(bool(kv));
			break;
		case KeyValueInt64:
			PutVarint(int64_t(kv));
			break;
		case KeyValueInt:
			PutVarint(int(kv));
			break;
		case KeyValueDouble:
			PutDouble(double(kv));
			break;
		case KeyValueString:
			PutVString(string_view(kv));
			break;
		case KeyValueNull:
			break;
		default:
			fprintf(stderr, "Unknown keyType %d\n", int(kv.Type()));
			abort();
	}
}

void WrSerializer::PutSlice(const string_view &slice) {
	PutUInt32(slice.size());
	grow(slice.size());
	memcpy(&buf_[len_], slice.data(), slice.size());
	len_ += slice.size();
}

WrSerializer::SliceHelper WrSerializer::StartSlice() {
	size_t savePos = len_;
	PutUInt32(0);
	return SliceHelper(this, savePos);
}

WrSerializer::SliceHelper::~SliceHelper() {
	if (ser_) {
		uint32_t sliceSize = ser_->len_ - pos_ - sizeof(uint32_t);
		memcpy(&ser_->buf_[pos_], &sliceSize, sizeof(sliceSize));
	}
}

void WrSerializer::PutUInt32(uint32_t v) {
	grow(sizeof v);
	memcpy(&buf_[len_], &v, sizeof v);
	len_ += sizeof v;
}

void WrSerializer::PutUInt64(uint64_t v) {
	grow(sizeof v);
	memcpy(&buf_[len_], &v, sizeof v);
	len_ += sizeof v;
}

void WrSerializer::PutDouble(double v) {
	grow(sizeof v);
	memcpy(&buf_[len_], &v, sizeof v);
	len_ += sizeof v;
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

void WrSerializer::grow(size_t sz) {
	if (len_ + sz > cap_) {
		Reserve((cap_ * 2) + sz + 0x1000);
	}
}

void WrSerializer::Reserve(size_t cap) {
	if (cap > cap_) {
		cap_ = std::max(cap, cap_);
		uint8_t *b = new uint8_t[cap_];
		memcpy(b, buf_, len_);
		if (buf_ != inBuf_) delete[] buf_;
		buf_ = b;
	}
}

void WrSerializer::Fill(char c, size_t count) {
	grow(count);
	memset(&buf_[len_], c, count);
	len_ += count;
}

void WrSerializer::PutBool(bool v) {
	grow(1);
	len_ += boolean_pack(v, buf_ + len_);
}

void WrSerializer::PutVString(string_view str) {
	grow(str.size() + 10);
	len_ += string_pack(str.data(), str.size(), buf_ + len_);
}

void WrSerializer::PrintJsonString(string_view str) {
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

void WrSerializer::PrintHexDump(string_view str) {
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

uint8_t *WrSerializer::Buf() const { return buf_; }

chunk WrSerializer::DetachChunk() {
	chunk ch;
	if (buf_ == inBuf_) {
		ch.append(Slice());
	} else {
		ch.data_ = buf_;
		ch.cap_ = cap_;
		ch.len_ = len_;
	}
	buf_ = inBuf_;
	cap_ = sizeof(inBuf_);
	len_ = 0;
	return ch;
}

void WrSerializer::Write(string_view slice) {
	grow(slice.size());
	memcpy(&buf_[len_], slice.data(), slice.size());
	len_ += slice.size();
}

}  // namespace reindexer
