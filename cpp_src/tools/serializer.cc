#include "tools/serializer.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <cstring>
#include "itoa/itoa.h"
#include "tools/errors.h"

#include "tools/varint.h"
namespace reindexer {

Serializer::Serializer(const void *_buf, int _len) : buf(static_cast<const uint8_t *>(_buf)), len(_len), pos(0) {}

bool Serializer::Eof() { return pos >= len; }

KeyValue Serializer::GetValue() {
	int type = GetVarUint();
	switch (type) {
		case KeyValueInt:
			return KeyValue(int(GetVarint()));
		case KeyValueInt64:
			return KeyValue(int64_t(GetVarint()));
		case KeyValueDouble:
			return KeyValue(GetDouble());
		case KeyValueString:
			return KeyValue(GetVString().ToString());
		case KeyValueComposite: {
			KeyValues compositeValues;
			uint64_t count = GetVarUint();
			for (size_t i = 0; i < count; ++i) {
				compositeValues.push_back(GetValue());
			}
			return KeyValue(std::move(compositeValues));
		}
		default:
			throw Error(errParseBin, "Unknown type %d while parsing binary buffer", type);
	}
}

inline static void checkbound(int pos, int need, int len) {
	if (pos + need > len) {
		throw Error(errParseBin, "Binary buffer underflow. Need more %d bytes, pos=%d,len=%d", need, pos, len);
	}
}

Slice Serializer::GetSlice() {
	uint32_t l = GetUInt32();
	Slice b(reinterpret_cast<const char *>(buf + pos), l);
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

double Serializer::GetDouble() {
	double ret;
	checkbound(pos, sizeof(ret), len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

int64_t Serializer::GetVarint() {
	int l = scan_varint(len - pos, buf + pos);
	checkbound(pos, l, len);
	pos += l;
	return unzigzag64(parse_uint64(l, buf + pos - l));
}
uint64_t Serializer::GetVarUint() {
	int l = scan_varint(len - pos, buf + pos);
	checkbound(pos, l, len);
	pos += l;
	return parse_uint64(l, buf + pos - l);
}

Slice Serializer::GetVString() {
	int l = GetVarUint();
	checkbound(pos, l, len);
	pos += l;
	return Slice(reinterpret_cast<const char *>(buf + pos - l), l);
}

p_string Serializer::GetPVString() {
	auto ret = reinterpret_cast<const v_string_hdr *>(buf + pos);
	int l = GetVarUint();
	checkbound(pos, l, len);
	pos += l;
	return p_string(ret);
}

bool Serializer::GetBool() { return bool(GetVarUint()); }

WrSerializer::WrSerializer(bool allowInBuf)
	: buf_(allowInBuf ? this->inBuf_ : nullptr), len_(0), cap_(allowInBuf ? sizeof(this->inBuf_) : 0) {}

WrSerializer::~WrSerializer() {
	if (buf_ != inBuf_) free(buf_);
}

void WrSerializer::PutValue(const KeyValue &kv) {
	PutVarUint(kv.Type());
	switch (kv.Type()) {
		case KeyValueInt:
			PutVarint(kv.As<int>());
			break;
		case KeyValueInt64:
			PutVarint(kv.As<int64_t>());
			break;
		case KeyValueDouble:
			PutDouble(kv.As<double>());
			break;
		case KeyValueString:
			PutVString(kv.As<string>());
			break;
		default:
			abort();
	}
}

void WrSerializer::PutSlice(const Slice &slice) {
	PutUInt32(slice.size());
	grow(slice.size());
	memcpy(&buf_[len_], slice.data(), slice.size());
	len_ += slice.size();
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

void WrSerializer::grow(size_t sz) {
	if (len_ + sz > cap_ || !buf_) {
		Reserve((cap_ * 2) + sz + 0x1000);
	}
}

void WrSerializer::Reserve(size_t cap) {
	if (cap > cap_ || !buf_) {
		cap_ = std::max(cap, cap_);
		uint8_t *b = reinterpret_cast<uint8_t *>(malloc(cap_));
		if (!b) throw std::bad_alloc();
		if (buf_) {
			memcpy(b, buf_, len_);
			if (buf_ != inBuf_) free(buf_);
		}
		buf_ = b;
	}
}

void WrSerializer::Printf(const char *fmt, ...) {
	int ret = 0, sz = 100;
	va_list args;
	va_start(args, fmt);
	for (;;) {
		grow(sz);

		va_list cargs;
		va_copy(cargs, args);
		ret = vsnprintf(reinterpret_cast<char *>(buf_ + len_), cap_ - len_, fmt, cargs);
		va_end(cargs);
		if (ret < 0) {
			abort();
		}
		if (size_t(ret) < cap_ - len_) break;
		sz = ret + 1;
	}
	va_end(args);
	len_ += ret;
}

void WrSerializer::PutVarint(int64_t v) {
	grow(10);
	len_ += sint64_pack(v, buf_ + len_);
}

void WrSerializer::PutVarUint(uint64_t v) {
	grow(10);
	len_ += uint64_pack(v, buf_ + len_);
}

void WrSerializer::PutBool(bool v) {
	grow(1);
	len_ += boolean_pack(v, buf_ + len_);
}

void WrSerializer::PutVString(const char *str) {
	grow(strlen(str) + 10);
	len_ += string_pack(str, buf_ + len_);
}

void WrSerializer::PutVString(const Slice &str) {
	grow(str.size() + 10);
	len_ += string_pack(str.data(), str.size(), buf_ + len_);
}

void WrSerializer::PrintJsonString(const Slice &str) {
	const char *s = str.data();
	size_t l = str.size();
	grow(l * 6 + 3);
	char *d = reinterpret_cast<char *>(buf_ + len_);
	*d++ = '"';

	while (l--) {
		int c = *s++;
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
			case '&':
				*d++ = '\\';
				*d++ = 'u';
				*d++ = '0';
				*d++ = '0';
				*d++ = '2';
				*d++ = '6';
				break;
			default:
				*d++ = c;
		}
	}
	*d++ = '"';
	len_ = d - reinterpret_cast<char *>(buf_);
}

void WrSerializer::Print(int k) {
	grow(32);
	char *b = i32toa(k, reinterpret_cast<char *>(buf_ + len_));
	len_ = b - reinterpret_cast<char *>(buf_);
}

void WrSerializer::Print(int64_t k) {
	grow(32);
	char *b = i64toa(k, reinterpret_cast<char *>(buf_ + len_));
	len_ = b - reinterpret_cast<char *>(buf_);
}

uint8_t *WrSerializer::DetachBuffer() {
	uint8_t *b = buf_;
	buf_ = nullptr;
	cap_ = 0;
	return b;
}

uint8_t *WrSerializer::Buf() const { return buf_; }

}  // namespace reindexer
