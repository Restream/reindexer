#pragma once

#include "core/keyvalue.h"

namespace reindexer {

using std::string;

class Serializer {
public:
	Serializer(const void *_buf, int _len);
	bool Eof();
	KeyValue GetValue();
	KeyRef GetRef();
	string GetString();
	p_string GetPString();
	Slice GetSlice();
	int GetInt();
	int64_t GetInt64();
	double GetDouble();

	int64_t GetVarint();
	uint64_t GetVarUint();
	Slice GetVString();
	bool GetBool();

protected:
	const uint8_t *buf;
	size_t len;
	size_t pos;
};

enum { TAG_VARINT, TAG_DOUBLE, TAG_STRING, TAG_ARRAY, TAG_BOOL, TAG_NULL, TAG_OBJECT, TAG_END };

class WrSerializer {
public:
	WrSerializer(bool allowInBuf = true);
	~WrSerializer();

	// Put string or slice with 4 bytes len header
	void PutString(const string &);
	void PutSlice(const Slice &slice);

	// Put raw data
	void PutInt(int);
	void PutInt64(int64_t);
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

	void PrintKeyRefToJson(KeyRef kr, int tag);
	void PrintJsonString(const Slice &str);

	// Protobuf formt like functions
	void PutVarint(int64_t v);
	void PutVarUint(uint64_t v);
	void PutVString(const char *);
	void PutBool(bool v);

	// Buffer manipulation functions
	uint8_t *DetachBuffer();
	uint8_t *Buf();
	void Reset() { len_ = 0; }
	size_t Len() { return len_; };

protected:
	void grow(size_t sz);
	uint8_t *buf_;
	size_t len_;
	size_t cap_;
	uint8_t inBuf_[0x10000];
};

}  // namespace reindexer