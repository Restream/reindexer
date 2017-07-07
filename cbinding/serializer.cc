#include "cbinding/serializer.h"
#include <stdarg.h>
#include <stdio.h>
#include <cstring>
#include "itoa/itoa.h"

#include "tools/packers.h"
namespace reindexer {

Serializer::Serializer(const void *_buf, int _len) : buf(static_cast<const uint8_t *>(_buf)), len(_len), pos(0) {}

bool Serializer::Eof() { return pos >= len; }

KeyValue Serializer::GetValue() {
	switch (GetInt()) {
		case KeyValueInt:
			return KeyValue(GetInt());
		case KeyValueInt64:
			return KeyValue(GetInt64());
		case KeyValueDouble:
			return KeyValue(GetDouble());
		case KeyValueString:
			return KeyValue(GetString());
		default:
			abort();
	}
}

KeyRef Serializer::GetRef() {
	switch (GetInt()) {
		case KeyValueInt:
			return KeyRef(GetInt());
		case KeyValueInt64:
			return KeyRef(GetInt64());
		case KeyValueDouble:
			return KeyRef(GetDouble());
		case KeyValueString:
			return KeyRef(GetPString());
		default:
			abort();
	}
}

string Serializer::GetString() {
	int l = GetInt();
	char *ret = (char *)(buf + pos);
	assert(pos + l <= len);
	pos += l;
	return string(ret, l);
}

p_string Serializer::GetPString() {
	l_string_hdr *ret = (l_string_hdr *)(buf + pos);
	int l = GetInt();
	assert(pos + l <= len);
	pos += l;
	return p_string(ret);
}

Slice Serializer::GetSlice() {
	int l = GetInt();
	Slice b((const char *)buf + pos, l);
	assert(pos + b.size() <= len);
	pos += b.size();
	return b;
}

int Serializer::GetInt() {
	int ret;
	assert(pos + sizeof(ret) <= len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

int64_t Serializer::GetInt64() {
	int64_t ret;
	assert(pos + sizeof(ret) <= len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

double Serializer::GetDouble() {
	double ret;
	assert(pos + sizeof(ret) <= len);
	memcpy(&ret, buf + pos, sizeof(ret));
	pos += sizeof(ret);
	return ret;
}

int64_t Serializer::GetVarint() {
	int l = scan_varint(len - pos, buf + pos);
	assert(pos + l <= len);
	pos += l;
	return unzigzag64(parse_uint64(l, buf + pos - l));
}
uint64_t Serializer::GetVarUint() {
	int l = scan_varint(len - pos, buf + pos);
	assert(pos + l <= len);
	pos += l;
	return parse_uint64(l, buf + pos - l);
}

Slice Serializer::GetVString() {
	int l = GetVarUint();
	assert(pos + l <= len);
	pos += l;
	return Slice((const char *)buf + pos - l, l);
}

bool Serializer::GetBool() { return bool(GetVarUint()); }

WrSerializer::WrSerializer(bool allowInBuf)
	: buf_(allowInBuf ? this->inBuf_ : nullptr), len_(0), cap_(allowInBuf ? sizeof(this->inBuf_) : 0) {}

WrSerializer::~WrSerializer() {
	if (buf_ != inBuf_) free(buf_);
}

void WrSerializer::PutString(const string &str) { PutSlice(Slice(str)); }

void WrSerializer::PutSlice(const Slice &slice) {
	PutInt(slice.size());
	grow(slice.size());
	memcpy(&buf_[len_], slice.data(), slice.size());
	len_ += slice.size();
}

void WrSerializer::PutInt(int v) {
	grow(sizeof v);
	memcpy(&buf_[len_], &v, sizeof v);
	len_ += sizeof v;
}

void WrSerializer::PutInt64(int64_t v) {
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
		cap_ += (cap_ * 2) + sz + 0x1000;
		uint8_t *b = (uint8_t *)malloc(cap_);
		if (!b) throw std::bad_alloc();
		if (buf_) {
			memcpy(b, buf_, len_);
			if (buf_ != inBuf_) free(buf_);
		}
		buf_ = b;
	}
}

void WrSerializer::Printf(const char *fmt, ...) {
	grow(100);

	va_list args;
	va_start(args, fmt);
	len_ += vsnprintf((char *)buf_ + len_, len_ - cap_, fmt, args);
	va_end(args);
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

void WrSerializer::PrintJsonString(const Slice &str) {
	const char *s = str.data();
	size_t l = str.size();
	grow(l * 6 + 3);
	char *d = (char *)buf_ + len_;
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
				PutChars("\\u0026");
				break;
			default:
				*d++ = c;
		}
	}
	*d++ = '"';
	len_ = d - (char *)buf_;
}

void WrSerializer::Print(int k) {
	grow(32);
	char *b = i32toa(k, (char *)buf_ + len_);
	len_ = b - (char *)buf_;
}

void WrSerializer::Print(int64_t k) {
	grow(32);
	char *b = i64toa(k, (char *)buf_ + len_);
	len_ = b - (char *)buf_;
}

void WrSerializer::PrintKeyRefToJson(KeyRef kr, int tag) {
	if (tag == TAG_NULL) {
		PutChars("null");
		return;
	}

	switch (kr.Type()) {
		case KeyValueInt:
			if ((tag & 0x7) != TAG_BOOL)
				Print((int)kr);
			else
				PutChars((int)kr ? "true" : "false");
			break;
		case KeyValueInt64:
			Print((int64_t)kr);
			break;
		case KeyValueDouble:
			Printf("%g", (double)kr);
			break;
		case KeyValueString:
			PrintJsonString((p_string)kr);
			break;
		default:
			PutChars("null");
	}
}

uint8_t *WrSerializer::DetachBuffer() {
	uint8_t *b = buf_;
	buf_ = nullptr;
	cap_ = 0;
	return b;
}

uint8_t *WrSerializer::Buf() { return buf_; }

}  // namespace reindexer
