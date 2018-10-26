#pragma once

#include <functional>
#include "core/keyvalue/variant.h"
#include "estl/string_view.h"

namespace reindexer {

using std::move;
using std::string;
struct p_string;

class Serializer {
public:
	Serializer(const void *_buf, int _len);
	Serializer(const string_view &buf);
	bool Eof();
	Variant GetVariant();
	Variant GetRawVariant(KeyValueType type);
	string_view GetSlice();
	uint32_t GetUInt32();
	uint64_t GetUInt64();
	double GetDouble();

	int64_t GetVarint();
	uint64_t GetVarUint();
	string_view GetVString();
	p_string GetPVString();
	bool GetBool();
	size_t Pos() { return pos; }
	void SetPos(size_t p) { pos = p; }

protected:
	const uint8_t *buf;
	size_t len;
	size_t pos;
};

class WrSerializer {
public:
	WrSerializer();
	WrSerializer(const WrSerializer &) = delete;
	WrSerializer(WrSerializer &&other) : len_(other.len_), cap_(other.cap_) {
		if (other.buf_ == other.inBuf_) {
			buf_ = inBuf_;
			memcpy(buf_, other.buf_, other.len_ * sizeof(other.inBuf_[0]));
		} else {
			buf_ = other.buf_;
			other.buf_ = nullptr;
		}

		other.len_ = 0;
		other.cap_ = 0;
	}
	~WrSerializer();
	WrSerializer &operator=(const WrSerializer &) = delete;
	WrSerializer &operator=(WrSerializer &&other) noexcept {
		if (this != &other) {
			if (buf_ != inBuf_) delete[] buf_;

			len_ = other.len_;
			cap_ = other.cap_;

			if (other.buf_ == other.inBuf_) {
				buf_ = inBuf_;
				memcpy(buf_, other.buf_, other.len_ * sizeof(other.inBuf_[0]));
			} else {
				buf_ = other.buf_;
				other.buf_ = nullptr;
			}

			other.len_ = 0;
			other.cap_ = 0;
		}

		return *this;
	}

	// Put variant
	void PutVariant(const Variant &kv);
	void PutRawVariant(const Variant &kv);

	// Put slice with 4 bytes len header
	void PutSlice(const string_view &slice);
	void PutSlice(std::function<void()> func);

	// Put raw data
	void PutUInt32(uint32_t);

	void PutUInt64(uint64_t);
	void PutDouble(double);
	void PutChar(char c) {
		if (len_ + 1 >= cap_) grow(1);
		buf_[len_++] = c;
	}
	void PutChars(const char *s) {
		while (*s) PutChar(*s++);
	}
	void PutChars(const string_view &sv) {
		for (auto c : sv) PutChar(c);
	}
	void Printf(const char *fmt, ...)
#ifndef _MSC_VER
		__attribute__((format(printf, 2, 3)))
#endif
		;
	void Print(int);
	void Print(int64_t);
	void PrintJsonString(const string_view &str);
	void PrintHexDump(const string_view &str);

	// Protobuf formt like functions
	void PutVarint(int64_t v);
	void PutVarUint(uint64_t v);
	void PutBool(bool v);
	void PutVString(const string_view &str);

	// Buffer manipulation functions
	void Write(const string_view &buf);
	uint8_t *Buf() const;
	void Reset() { len_ = 0; }
	size_t Len() const { return len_; }
	void Reserve(size_t cap);
	string_view Slice() const { return string_view(reinterpret_cast<const char *>(buf_), len_); }

protected:
	void grow(size_t sz);
	uint8_t *buf_;
	size_t len_;
	size_t cap_;
	uint8_t inBuf_[0x200];
};

}  // namespace reindexer
