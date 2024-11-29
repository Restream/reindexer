#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include "estl/span.h"
#include "tools/jsonstring.h"

namespace gason {

using reindexer::span;

enum JsonTag : uint8_t {
	JSON_STRING = 0,
	JSON_NUMBER,
	JSON_DOUBLE,
	JSON_ARRAY,
	JSON_OBJECT,
	JSON_TRUE,
	JSON_FALSE,
	JSON_NULL = 0xF,
	JSON_EMPTY = 0xFF
};
constexpr inline int format_as(JsonTag v) noexcept { return int(v); }

struct JsonNode;

using Exception = std::runtime_error;

using LargeStringStorageT = std::vector<std::unique_ptr<char[]>>;

struct JsonString {
	JsonString(char* beg, char* end, LargeStringStorageT& largeStrings) {
		ptr = end;
		const uint64_t l = end - beg;
		if (l >= (uint64_t(1) << 31)) {
			throw Exception("JSON string too long. Limit is 2^31 bytes");
		}
		reindexer::json_string::encode(reinterpret_cast<uint8_t*>(end), l, largeStrings);
	}
	JsonString(const char* end = nullptr) : ptr(end) {}

	size_t length() const noexcept {
		assertrx(ptr);
		return reindexer::json_string::length(reinterpret_cast<const uint8_t*>(ptr));
	}
	size_t size() const noexcept { return ptr ? length() : 0; }
	const char* data() const noexcept {
		if (!ptr) {
			return nullptr;
		}

		return reindexer::json_string::to_string_view(reinterpret_cast<const uint8_t*>(ptr)).data();
	}
	explicit operator std::string() const { return ptr ? std::string(data(), length()) : std::string(); }
	operator std::string_view() const noexcept { return ptr ? std::string_view(data(), length()) : std::string_view(); }

	const char* ptr;
};

inline static std::ostream& operator<<(std::ostream& o, const gason::JsonString& sv) {
	o.write(sv.data(), sv.length());
	return o;
}

union JsonValue {
	JsonValue(double x) : fval(x) { u.tag = JSON_DOUBLE; }
	JsonValue(int64_t x) : ival(x) { u.tag = JSON_NUMBER; }
	JsonValue(JsonString x) : sval(x) { u.tag = JSON_STRING; }

	JsonValue(JsonTag tag = JSON_NULL, void* payload = nullptr) {
		u.tag = tag;
		ival = uintptr_t(payload);
	}
	// TODO: Remove NOLINT after pyreindexer update. Issue #1736
	JsonTag getTag() const noexcept { return JsonTag(u.tag); }	// NOLINT(*EnumCastOutOfRange)

	int64_t toNumber() const {
		assertrx(getTag() == JSON_NUMBER || getTag() == JSON_DOUBLE);
		if (getTag() == JSON_NUMBER) {
			return ival;
		}
		return fval;
	}
	double toDouble() const {
		assertrx(getTag() == JSON_NUMBER || getTag() == JSON_DOUBLE);
		if (getTag() == JSON_DOUBLE) {
			return fval;
		}
		return ival;
	}
	std::string_view toString() const {
		assertrx(getTag() == JSON_STRING);
		return sval;
	}
	JsonNode* toNode() const {
		assertrx(getTag() == JSON_ARRAY || getTag() == JSON_OBJECT);
		return node;
	}

	struct {
		uint8_t dummy[8];
		uint8_t tag;
	} u;
	int64_t ival;
	double fval;
	JsonString sval;
	JsonNode* node;
};

struct JsonNode {
	JsonValue value;
	JsonNode* next;
	JsonString key;

	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type* = nullptr>
	T As(T defval = T(), T minv = std::numeric_limits<T>::lowest(), T maxv = std::numeric_limits<T>::max()) const {
		if (empty()) {
			return defval;
		}
		if (value.getTag() != JSON_DOUBLE && value.getTag() != JSON_NUMBER) {
			throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to number");
		}
		T v;
		if (std::is_integral<T>::value) {
			v = value.toNumber();
		} else {
			v = value.toDouble();
		}

		if (v < minv || v > maxv) {
			throw Exception(std::string("Value of '") + std::string(key) + "' - " + std::to_string(v) + " is out of bounds: [" +
							std::to_string(minv) + "," + std::to_string(maxv) + "]");
		}
		return v;
	}
	template <typename T,
			  typename std::enable_if<std::is_same<std::string, T>::value || std::is_same<std::string_view, T>::value>::type* = nullptr>
	T As(T defval = T()) const {
		if (empty()) {
			return defval;
		}
		if (value.getTag() != JSON_STRING) {
			throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to string");
		}
		return T(value.toString());
	}
	template <typename T, typename std::enable_if<std::is_same<T, bool>::value>::type* = nullptr>
	T As(T defval = T()) const {
		if (empty()) {
			return defval;
		}
		switch (value.getTag()) {
			case JSON_TRUE:
				return true;
			case JSON_FALSE:
				return false;
			case JSON_STRING:
			case JSON_NUMBER:
			case JSON_DOUBLE:
			case JSON_ARRAY:
			case JSON_OBJECT:
			case JSON_NULL:
			case JSON_EMPTY:
			default:
				throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to bool");
		}
	}

	const JsonNode& operator[](std::string_view sv) const;
	bool empty() const noexcept { return value.getTag() == JSON_EMPTY; }
	bool isObject() const noexcept { return value.getTag() == JSON_OBJECT; }
	JsonNode* toNode() const;
};

struct JsonIterator {
	JsonNode* p;

	void operator++() noexcept { p = p->next; }
	bool operator!=(const JsonIterator& x) const noexcept { return p != x.p; }
	JsonNode& operator*() const noexcept { return *p; }
	JsonNode* operator->() const noexcept { return p; }
};

inline JsonIterator begin(const JsonValue o) { return JsonIterator{o.toNode()}; }
inline JsonIterator end(JsonValue) noexcept { return JsonIterator{nullptr}; }

struct JsonNodeIterator {
	const JsonNode* p;

	void operator++() noexcept { p = p->next; }
	bool operator!=(const JsonNodeIterator& x) const noexcept { return p != x.p; }
	const JsonNode& operator*() const noexcept { return *p; }
	const JsonNode* operator->() const noexcept { return p; }
};

inline JsonNodeIterator begin(const JsonNode& w) { return JsonNodeIterator{w.toNode()}; }
inline JsonNodeIterator end(const JsonNode&) noexcept { return JsonNodeIterator{nullptr}; }

#define JSON_ERRNO_MAP(XX)                           \
	XX(OK, "ok")                                     \
	XX(BAD_NUMBER, "bad number")                     \
	XX(BAD_STRING, "bad string")                     \
	XX(BAD_IDENTIFIER, "bad identifier")             \
	XX(STACK_OVERFLOW, "stack overflow")             \
	XX(STACK_UNDERFLOW, "stack underflow")           \
	XX(MISMATCH_BRACKET, "mismatch bracket")         \
	XX(UNEXPECTED_CHARACTER, "unexpected character") \
	XX(UNQUOTED_KEY, "unquoted key")                 \
	XX(BREAKING_BAD, "breaking bad")                 \
	XX(ALLOCATION_FAILURE, "allocation failure")

enum JsonErrno {
#define XX(no, str) JSON_##no,
	JSON_ERRNO_MAP(XX)
#undef XX
};

const char* jsonStrError(int err);

class JsonAllocator {
	struct Zone {
		Zone* next;
		size_t used;
	}* head;

public:
	JsonAllocator() : head(nullptr) {}
	JsonAllocator(const JsonAllocator&) = delete;
	JsonAllocator& operator=(const JsonAllocator&) = delete;
	JsonAllocator(JsonAllocator&& x) : head(x.head) { x.head = nullptr; }
	JsonAllocator& operator=(JsonAllocator&& x) noexcept {
		if (this != &x) {
			deallocate();
			head = x.head;
			x.head = nullptr;
		}
		return *this;
	}
	~JsonAllocator() { deallocate(); }
	void* allocate(size_t size);
	void deallocate();
};

bool isHomogeneousArray(const JsonValue& v) noexcept;

// Parser wrapper
class JsonParser {
public:
	JsonParser(LargeStringStorageT* strings = nullptr) : largeStrings_(strings ? strings : &internalLargeStrings_) {}
	// Inplace parse. Buffer pointed by str will be changed
	JsonNode Parse(span<char> str, size_t* length = nullptr) &;
	// Copy str. Buffer pointed by str will be copied
	JsonNode Parse(std::string_view str, size_t* length = nullptr) &;

private:
	JsonAllocator alloc_;
	std::string tmp_;
	LargeStringStorageT internalLargeStrings_;
	LargeStringStorageT* largeStrings_;
};

}  // namespace gason
