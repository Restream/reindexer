#pragma once

#include <string_view>
#include <type_traits>
#include "msgpack.h"
#include "tools/errors.h"

enum MsgPackTag : int {
	MSGPACK_NULL = 0x0,
	MSGPACK_BOOLEAN = 0x1,
	MSGPACK_POSITIVE_INTEGER = 0x2,
	MSGPACK_NEGATIVE_INTEGER = 0x3,
	MSGPACK_FLOAT32 = 0x0a,
	MSGPACK_FLOAT64 = 0x04,
	MSGPACK_FLOAT = 0x04,
	MSGPACK_STRING = 0x05,
	MSGPACK_ARRAY = 0x06,
	MSGPACK_MAP = 0x07,
};
inline constexpr int format_as(MsgPackTag v) noexcept { return int(v); }

struct MsgPackValue {
	explicit MsgPackValue(const msgpack_object* p = nullptr);
	MsgPackValue operator[](std::string_view key) const;

	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type* = nullptr>
	T As(T defval = T(), T minv = std::numeric_limits<T>::lowest(), T maxv = std::numeric_limits<T>::max()) const {
		if (!isValid()) {
			return defval;
		}
		T v;
		MsgPackTag tag = getTag();
		switch (tag) {
			case MSGPACK_NEGATIVE_INTEGER:
				v = T(p->via.i64);
				break;
			case MSGPACK_POSITIVE_INTEGER:
				v = T(p->via.u64);
				break;
			case MSGPACK_FLOAT32:
			case MSGPACK_FLOAT64:
				v = T(p->via.f64);
				break;
			case MSGPACK_NULL:
			case MSGPACK_BOOLEAN:
			case MSGPACK_STRING:
			case MSGPACK_ARRAY:
			case MSGPACK_MAP:
			default:
				throw reindexer::Error(errParseMsgPack, "Impossible to convert type [{}] to number", tag);
		}
		if (v < minv || v > maxv) {
			throw reindexer::Error(errParams, fmt::format("Value is out of bounds: [{},{}]", minv, maxv));
		}
		return v;
	}
	template <typename T,
			  typename std::enable_if<std::is_same<std::string, T>::value || std::is_same<std::string_view, T>::value>::type* = nullptr>
	T As(T defval = T()) const {
		if (!isValid()) {
			return defval;
		}
		MsgPackTag tag = getTag();
		if (tag != MSGPACK_STRING) {
			throw reindexer::Error(errParseMsgPack, "Impossible to convert type [{}] to string", tag);
		}
		return T(p->via.str.ptr, p->via.str.size);
	}
	template <typename T, typename std::enable_if<std::is_same<T, bool>::value>::type* = nullptr>
	T As(T defval = T()) const {
		if (!isValid()) {
			return defval;
		}
		MsgPackTag tag = getTag();
		if (tag != MSGPACK_BOOLEAN) {
			throw reindexer::Error(errParseMsgPack, "Impossible to convert type [{}] to bool", tag);
		}
		return p->via.boolean;
	}

	bool isValid() const;
	MsgPackTag getTag() const;
	uint32_t size() const;

	const msgpack_object* p;
};

struct MsgPackIterator {
	uint32_t index;
	const MsgPackValue* val;
	MsgPackIterator(uint32_t index, const MsgPackValue* val);
	void operator++();
	bool operator!=(const MsgPackIterator& x) const;
	const MsgPackValue& operator*() const;
	bool isValid() const;

private:
	mutable MsgPackValue elemValue;
};

inline MsgPackIterator begin(const MsgPackValue& w) { return MsgPackIterator{0, &w}; }
inline MsgPackIterator end(const MsgPackValue& w) { return MsgPackIterator{w.size(), &w}; }

class MsgPackParser {
public:
	MsgPackParser();
	~MsgPackParser();

	MsgPackParser(const MsgPackParser&) = delete;
	MsgPackParser(MsgPackParser&&) = delete;
	MsgPackParser& operator=(const MsgPackParser&) = delete;
	MsgPackParser& operator=(MsgPackParser&&) = delete;

	template <typename T>
	MsgPackValue Parse(T data, size_t& offset) {
		prepare();
		msgpack_unpack_return ret = msgpack_unpack_next(&unpacked_, data.data(), data.size(), &offset);
		if (ret != MSGPACK_UNPACK_SUCCESS) {
			return MsgPackValue(nullptr);
		}
		return MsgPackValue(&unpacked_.data);
	}

private:
	void prepare();
	void dispose();

	msgpack_unpacked unpacked_;
	bool inUse_;
};
