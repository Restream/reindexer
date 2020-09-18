#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdexcept>
#include <type_traits>
#include "estl/span.h"
#include "estl/string_view.h"

namespace gason {

using reindexer::string_view;
using reindexer::span;

enum JsonTag : int { JSON_STRING = 0, JSON_NUMBER, JSON_DOUBLE, JSON_ARRAY, JSON_OBJECT, JSON_TRUE, JSON_FALSE, JSON_NULL = 0xF };

struct JsonNode;

using Exception = std::runtime_error;

struct JsonString {
	JsonString(char *beg, char *end) {
		ptr = end;
		size_t l = end - beg;
		if (l >= (1 << 24)) {
			throw Exception("JSON string too long. Limit is 2^24 bytes");
		}
		uint8_t *p = reinterpret_cast<uint8_t *>(end);
		p[0] = l & 0xFF;
		p[1] = (l >> 8) & 0xFF;
		p[2] = (l >> 16) & 0xFF;
	}
	JsonString(const char *end = nullptr) : ptr(end) {}

	size_t length() const {
		assert(ptr);
		const uint8_t *p = reinterpret_cast<const uint8_t *>(ptr);
		return p[0] | (p[1] << 8) | (p[2] << 16);
	}
	size_t size() const { return length(); }
	const char *data() const { return ptr - length(); }
	explicit operator std::string() const { return std::string(data(), length()); }
	operator string_view() const { return string_view(data(), length()); }

	const char *ptr;
};

inline static std::ostream &operator<<(std::ostream &o, const gason::JsonString &sv) {
	o.write(sv.data(), sv.length());
	return o;
}

union JsonValue {
	JsonValue(double x) : fval(x) { u.tag = JSON_DOUBLE; }
	JsonValue(int64_t x) : ival(x) { u.tag = JSON_NUMBER; }
	JsonValue(JsonString x) : sval(x) { u.tag = JSON_STRING; }

	JsonValue(JsonTag tag = JSON_NULL, void *payload = nullptr) {
		u.tag = tag;
		ival = uintptr_t(payload);
	}
	JsonTag getTag() const { return JsonTag(u.tag); }

	int64_t toNumber() const {
		assert(getTag() == JSON_NUMBER || getTag() == JSON_DOUBLE);
		if (getTag() == JSON_NUMBER) return ival;
		return fval;
	}
	double toDouble() const {
		assert(getTag() == JSON_NUMBER || getTag() == JSON_DOUBLE);
		if (getTag() == JSON_DOUBLE) return fval;
		return ival;
	}
	string_view toString() const {
		assert(getTag() == JSON_STRING);
		return sval;
	}
	JsonNode *toNode() const {
		assert(getTag() == JSON_ARRAY || getTag() == JSON_OBJECT);
		return node;
	}

	struct {
		uint8_t dummy[8];
		uint8_t tag;
	} u;
	int64_t ival;
	double fval;
	JsonString sval;
	JsonNode *node;
};

struct JsonNode {
	JsonValue value;
	JsonNode *next;
	JsonString key;

	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type * = nullptr>
	T As(T defval = T(), T minv = std::numeric_limits<T>::min(), T maxv = std::numeric_limits<T>::max()) const {
		if (empty()) return defval;
		if (value.getTag() != JSON_DOUBLE && value.getTag() != JSON_NUMBER)
			throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to number");
		T v;
		if (std::is_integral<T>::value)
			v = value.toNumber();
		else
			v = value.toDouble();

		if (v < minv || v > maxv)
			throw Exception(std::string("Value of '") + std::string(key) + "' - " + std::to_string(v) + " is out of bounds: [" +
							std::to_string(minv) + "," + std::to_string(maxv) + "]");
		return v;
	}
	template <typename T,
			  typename std::enable_if<std::is_same<std::string, T>::value || std::is_same<string_view, T>::value>::type * = nullptr>
	T As(T defval = T()) const {
		if (empty()) return defval;
		if (value.getTag() != JSON_STRING) throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to string");
		return T(value.toString());
	}
	template <typename T, typename std::enable_if<std::is_same<T, bool>::value>::type * = nullptr>
	T As(T defval = T()) const {
		if (empty()) return defval;
		switch (value.getTag()) {
			case JSON_TRUE:
				return true;
			case JSON_FALSE:
				return false;
			default:
				throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to bool");
		}
	}

	const JsonNode &operator[](string_view sv) const;
	bool empty() const;
	JsonNode *toNode() const;
};

struct JsonIterator {
	JsonNode *p;

	void operator++() { p = p->next; }
	bool operator!=(const JsonIterator &x) const { return p != x.p; }
	JsonNode *operator*() const { return p; }
	JsonNode *operator->() const { return p; }
};

inline JsonIterator begin(JsonValue o) { return JsonIterator{o.toNode()}; }
inline JsonIterator end(JsonValue) { return JsonIterator{nullptr}; }

struct JsonNodeIterator {
	const JsonNode *p;

	void operator++() { p = p->next; }
	bool operator!=(const JsonNodeIterator &x) const { return p != x.p; }
	const JsonNode &operator*() const { return *p; }
	const JsonNode *operator->() const { return p; }
};

inline JsonNodeIterator begin(const JsonNode &w) { return JsonNodeIterator{w.toNode()}; }
inline JsonNodeIterator end(const JsonNode &) { return JsonNodeIterator{nullptr}; }

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

const char *jsonStrError(int err);

class JsonAllocator {
	struct Zone {
		Zone *next;
		size_t used;
	} * head;

public:
	JsonAllocator() : head(nullptr) {}
	JsonAllocator(const JsonAllocator &) = delete;
	JsonAllocator &operator=(const JsonAllocator &) = delete;
	JsonAllocator(JsonAllocator &&x) : head(x.head) { x.head = nullptr; }
	JsonAllocator &operator=(JsonAllocator &&x) noexcept {
		if (this != &x) {
			deallocate();
			head = x.head;
			x.head = nullptr;
		}
		return *this;
	}
	~JsonAllocator() { deallocate(); }
	void *allocate(size_t size);
	void deallocate();
};

int jsonParse(span<char> str, char **endptr, JsonValue *value, JsonAllocator &allocator);
bool isHomogeneousArray(const JsonValue &v);

// Parser wrapper
class JsonParser {
public:
	// Inplace parse. Buffer pointed by str will be changed
	JsonNode Parse(span<char> str, size_t *length = nullptr);
	// Copy str. Buffer pointed by str will be copied
	JsonNode Parse(string_view str, size_t *length = nullptr);

private:
	JsonAllocator alloc_;
	std::string tmp_;
};

}  // namespace gason
