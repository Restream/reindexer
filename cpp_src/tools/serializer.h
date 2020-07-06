#pragma once

#include <functional>
#include "core/keyvalue/variant.h"
#include "estl/string_view.h"
#include "tools/varint.h"

char *i32toa(int32_t value, char *buffer);
char *i64toa(int64_t value, char *buffer);

namespace reindexer {

using std::move;
using std::string;

struct p_string;
class chunk;

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
	p_string GetPSlice();
	bool GetBool();
	size_t Pos() { return pos; }
	void SetPos(size_t p) { pos = p; }
	const uint8_t *Buf() const { return buf; }
	size_t Len() const { return len; }

protected:
	const uint8_t *buf;
	size_t len;
	size_t pos;
};

class WrSerializer {
public:
	WrSerializer();
	WrSerializer(chunk &&);
	WrSerializer(const WrSerializer &) = delete;
	WrSerializer(WrSerializer &&other) : len_(other.len_), cap_(other.cap_) {
		if (other.buf_ == other.inBuf_) {
			buf_ = inBuf_;
			memcpy(buf_, other.buf_, other.len_ * sizeof(other.inBuf_[0]));
		} else {
			buf_ = other.buf_;
			other.buf_ = other.inBuf_;
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
				other.buf_ = other.inBuf_;
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

	struct SliceHelper {
		SliceHelper(WrSerializer *ser, size_t pos) : ser_(ser), pos_(pos) {}
		SliceHelper(const SliceHelper &) = delete;
		SliceHelper operator=(const SliceHelper &) = delete;
		SliceHelper(SliceHelper &&other) noexcept : ser_(other.ser_), pos_(other.pos_) { other.ser_ = nullptr; }
		SliceHelper &operator=(SliceHelper &&other) noexcept {
			if (this != &other) {
				ser_ = other.ser_;
				pos_ = other.pos_;
				other.ser_ = nullptr;
			}
			return *this;
		}
		~SliceHelper();

		WrSerializer *ser_;
		size_t pos_;
	};

	SliceHelper StartSlice();

	// Put raw data
	void PutUInt32(uint32_t);
	void PutUInt64(uint64_t);
	void PutDouble(double);

	template <typename T, typename std::enable_if<sizeof(T) == 8 && std::is_integral<T>::value>::type * = nullptr>
	WrSerializer &operator<<(T k) {
		grow(32);
		char *b = i64toa(k, reinterpret_cast<char *>(buf_ + len_));
		len_ = b - reinterpret_cast<char *>(buf_);
		return *this;
	}
	template <typename T, typename std::enable_if<sizeof(T) <= 4 && std::is_integral<T>::value>::type * = nullptr>
	WrSerializer &operator<<(T k) {
		grow(32);
		char *b = i32toa(k, reinterpret_cast<char *>(buf_ + len_));
		len_ = b - reinterpret_cast<char *>(buf_);
		return *this;
	}

	WrSerializer &operator<<(char c) {
		if (len_ + 1 >= cap_) grow(1);
		buf_[len_++] = c;
		return *this;
	}
	WrSerializer &operator<<(string_view sv) {
		Write(sv);
		return *this;
	}
	WrSerializer &operator<<(const char *sv) {
		Write(string_view(sv));
		return *this;
	}
	WrSerializer &operator<<(bool v) {
		Write(v ? "true"_sv : "false"_sv);
		return *this;
	}
	WrSerializer &operator<<(double v);

	void PrintJsonString(string_view str);
	void PrintHexDump(string_view str);
	void Fill(char c, size_t count);

	template <typename T, typename std::enable_if<sizeof(T) == 8 && std::is_integral<T>::value>::type * = nullptr>
	void PutVarint(T v) {
		grow(10);
		len_ += sint64_pack(v, buf_ + len_);
	}

	template <typename T, typename std::enable_if<sizeof(T) == 8 && std::is_integral<T>::value>::type * = nullptr>
	void PutVarUint(T v) {
		grow(10);
		len_ += uint64_pack(v, buf_ + len_);
	}

	template <typename T, typename std::enable_if<sizeof(T) <= 4 && std::is_integral<T>::value>::type * = nullptr>
	void PutVarint(T v) {
		grow(10);
		len_ += sint32_pack(v, buf_ + len_);
	}

	template <typename T, typename std::enable_if<sizeof(T) <= 4 && std::is_integral<T>::value>::type * = nullptr>
	void PutVarUint(T v) {
		grow(10);
		len_ += uint32_pack(v, buf_ + len_);
	}

	template <typename T, typename std::enable_if<std::is_enum<T>::value>::type * = nullptr>
	void PutVarUint(T v) {
		assert(v >= 0 && v < 128);
		if (len_ + 1 >= cap_) grow(1);
		buf_[len_++] = v;
	}

	void PutBool(bool v);
	void PutVString(string_view str);

	// Buffer manipulation functions
	void Write(string_view buf);
	uint8_t *Buf() const;
	std::unique_ptr<uint8_t[]> DetachBuf();
	chunk DetachChunk();
	void Reset(size_t len = 0) { len_ = len; }
	size_t Len() const { return len_; }
	size_t Cap() const { return cap_; }
	void Reserve(size_t cap);
	string_view Slice() const { return string_view(reinterpret_cast<const char *>(buf_), len_); }
	const char *c_str() {
		grow(1);
		buf_[len_] = 0;
		return reinterpret_cast<const char *>(buf_);
	}

protected:
	void grow(size_t sz);
	uint8_t *buf_;
	size_t len_;
	size_t cap_;
	uint8_t inBuf_[0x100];
};

int msgpack_wrserializer_write(void *data, const char *buf, size_t len);

}  // namespace reindexer
