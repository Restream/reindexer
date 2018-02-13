#pragma once

#include "core/keyvalue/keyref.h"
#include "core/keyvalue/keyvalue.h"

namespace reindexer {

using std::move;
using std::string;

class Serializer {
public:
	Serializer(const void *_buf, int _len);
	bool Eof();
	KeyValue GetValue();
	Slice GetSlice();
	uint32_t GetUInt32();
	double GetDouble();

	int64_t GetVarint();
	uint64_t GetVarUint();
	Slice GetVString();
	p_string GetPVString();
	bool GetBool();
	size_t Pos() { return pos; }

protected:
	const uint8_t *buf;
	size_t len;
	size_t pos;
};

class WrSerializer {
public:
	WrSerializer(bool allowInBuf = true);
	WrSerializer(const WrSerializer &) = delete;
	WrSerializer(WrSerializer &&other) : len_(other.len_), cap_(other.cap_) {
		if (other.buf_ == other.inBuf_) {
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
			if (buf_ != inBuf_) free(buf_);

			len_ = other.len_;
			cap_ = other.cap_;

			if (other.buf_ == other.inBuf_) {
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

	// Put value
	void PutValue(const KeyValue &kv);

	// Put slice with 4 bytes len header
	void PutSlice(const Slice &slice);

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
	void Printf(const char *fmt, ...);
	void Print(int);
	void Print(int64_t);

	void PrintJsonString(const Slice &str);

	// Protobuf formt like functions
	void PutVarint(int64_t v);
	void PutVarUint(uint64_t v);
	void PutVString(const char *);
	void PutBool(bool v);
	void PutVString(const Slice &str);

	// Buffer manipulation functions
	uint8_t *DetachBuffer();
	uint8_t *Buf();
	void Reset() { len_ = 0; }
	size_t Len() { return len_; }
	void Reserve(size_t cap);

protected:
	void grow(size_t sz);
	uint8_t *buf_;
	size_t len_;
	size_t cap_;
	uint8_t inBuf_[0x200];
};

}  // namespace reindexer
