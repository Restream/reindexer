#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include "core/enums.h"
#include "tools/assertrx.h"
#include "tools/jsonstring.h"

namespace gason {

using std::span;

// MinGW is unable to declare 'TRUE'/'FALSE' enum members
enum class JsonTag : uint8_t { STRING = 0, NUMBER, DOUBLE, ARRAY, OBJECT, JTRUE, JFALSE, JSON_NULL = 0xF, EMPTY = 0xFF };
constexpr inline int format_as(JsonTag v) noexcept { return int(v); }
[[nodiscard]] std::string_view JsonTagToTypeStr(JsonTag) noexcept;

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
	JsonString(const char* end = nullptr) noexcept : ptr(end) {}

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

struct JsonValue {
	JsonValue(double x) noexcept : fval(x), tag(JsonTag::DOUBLE) {}
	JsonValue(int64_t x, bool neg) noexcept : ival(x), tag(JsonTag::NUMBER), negative(neg) {}
	JsonValue(JsonString x) noexcept : sval(x), tag(JsonTag::STRING) {}

	JsonValue(JsonTag t = JsonTag::JSON_NULL, void* payload = nullptr) noexcept : ival(uintptr_t(payload)), tag(t) {}
	JsonTag getTag() const noexcept { return tag; }
	bool isNegative() const noexcept { return (getTag() == JsonTag::NUMBER && negative) || (getTag() == JsonTag::DOUBLE && fval < 0.0); }

	int64_t toNumber() const {
		switch (getTag()) {
			case JsonTag::NUMBER:
				return ival;
			case JsonTag::DOUBLE:
				return int64_t(fval);
			case JsonTag::JSON_NULL:
			case JsonTag::JTRUE:
			case JsonTag::JFALSE:
			case JsonTag::ARRAY:
			case JsonTag::OBJECT:
			case JsonTag::STRING:
			case JsonTag::EMPTY:
				break;
		}
		throw_as_assert;
	}
	double toDouble() const {
		switch (getTag()) {
			case JsonTag::NUMBER:
				return ival;
			case JsonTag::DOUBLE:
				return fval;
			case JsonTag::JSON_NULL:
			case JsonTag::JTRUE:
			case JsonTag::JFALSE:
			case JsonTag::ARRAY:
			case JsonTag::OBJECT:
			case JsonTag::STRING:
			case JsonTag::EMPTY:
				break;
		}
		throw_as_assert;
	}
	std::string_view toString() const noexcept {
		assertrx(getTag() == JsonTag::STRING);
		return sval;
	}
	JsonNode* toNode() const {
		if (getTag() != JsonTag::OBJECT && getTag() != JsonTag::ARRAY) {
			throw Exception("Can't convert json field to object or array");
		}
		return node;
	}

	union {
		int64_t ival;
		double fval;
		JsonString sval;
		JsonNode* node;
	};
	JsonTag tag;
	bool negative{false};
};

struct JsonNode {
	JsonValue value;
	JsonNode* next;
	JsonString key;
	static JsonNode Empty() { return JsonNode{{JsonTag::EMPTY}, nullptr, {}}; }

	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type* = nullptr>
	T As(T defval = T(), T minv = std::numeric_limits<T>::lowest(), T maxv = std::numeric_limits<T>::max()) const {
		if (empty()) {
			return defval;
		}
		if (value.getTag() != JsonTag::DOUBLE && value.getTag() != JsonTag::NUMBER) {
			throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to number");
		}
		T v;
		if constexpr (std::is_integral<T>::value) {
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
	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type* = nullptr>
	T As(reindexer::CheckUnsigned checkUnsigned, T defval = T(), T minv = std::numeric_limits<T>::lowest(),
		 T maxv = std::numeric_limits<T>::max()) const {
		if (empty()) {
			return defval;
		}
		if (value.getTag() != JsonTag::DOUBLE && value.getTag() != JsonTag::NUMBER) {
			throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to number");
		}
		T v;
		if constexpr (std::is_integral<T>::value) {
			if (std::is_unsigned_v<T> && checkUnsigned && value.isNegative()) {
				if (value.getTag() == JsonTag::NUMBER) {
					throw Exception(std::string("Value of '") + std::string(key) + "' - " + std::to_string(value.ival) +
									" is out of bounds: [" + std::to_string(minv) + "," + std::to_string(maxv) + "]");
				} else {
					throw Exception(std::string("Value of '") + std::string(key) + "' - " + std::to_string(value.fval) +
									" is out of bounds: [" + std::to_string(minv) + "," + std::to_string(maxv) + "]");
				}
			}
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
		if (value.getTag() != JsonTag::STRING) {
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
			case JsonTag::JTRUE:
				return true;
			case JsonTag::JFALSE:
				return false;
			case JsonTag::STRING:
			case JsonTag::NUMBER:
			case JsonTag::DOUBLE:
			case JsonTag::ARRAY:
			case JsonTag::OBJECT:
			case JsonTag::JSON_NULL:
			case JsonTag::EMPTY:
			default:
				throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to bool");
		}
	}

	const JsonNode& operator[](std::string_view sv) const;
	const JsonNode& findCaseInsensitive(std::string_view key) const;
	bool empty() const noexcept { return value.getTag() == JsonTag::EMPTY; }
	bool isObject() const noexcept { return value.getTag() == JsonTag::OBJECT; }
	bool isArray() const noexcept { return value.getTag() == JsonTag::ARRAY; }
	JsonNode* toNode() const;
	static JsonNode EmptyNode() noexcept;
};

struct JsonIterator {
	using difference_type = long;
	using value_type = JsonNode;
	using pointer = JsonNode*;
	using reference = JsonNode&;
	using iterator_category = std::input_iterator_tag;
	JsonNode* p;

	void operator++() noexcept { p = p->next; }
	bool operator==(const JsonIterator& x) const noexcept { return p == x.p; }
	bool operator!=(const JsonIterator& x) const noexcept { return !operator==(x); }
	JsonNode& operator*() const noexcept { return *p; }
	JsonNode* operator->() const noexcept { return p; }
};

inline JsonIterator begin(JsonValue o) { return JsonIterator{o.toNode()}; }
inline JsonIterator end(JsonValue) noexcept { return JsonIterator{nullptr}; }

struct JsonNodeIterator {
	using difference_type = long;
	using value_type = JsonNode;
	using pointer = const JsonNode*;
	using reference = const JsonNode&;
	using iterator_category = std::input_iterator_tag;

	const JsonNode* p;

	void operator++() noexcept { p = p->next; }
	bool operator!=(const JsonNodeIterator& x) const noexcept { return p != x.p; }
	bool operator==(const JsonNodeIterator& x) const noexcept { return p == x.p; }
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

const char* jsonStrError(int err) noexcept;

class JsonAllocator {
	struct Zone {
		Zone* next;
		size_t used;
	}* head;

public:
	JsonAllocator() noexcept : head(nullptr) {}
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
	void* allocate(size_t size) noexcept;
	void deallocate() noexcept;
};

bool isHomogeneousArray(const JsonValue& v) noexcept;

// Parser wrapper
class JsonParser {
public:
	JsonParser(LargeStringStorageT* strings = nullptr) : largeStrings_(strings ? strings : &internalLargeStrings_) {}
	// Inplace parse. Buffer pointed by str will be changed
	JsonNode Parse(std::span<char> str, size_t* length = nullptr) &;
	// Copy str. Buffer pointed by str will be copied
	JsonNode Parse(std::string_view str, size_t* length = nullptr) &;

private:
	JsonAllocator alloc_;
	std::string tmp_;
	LargeStringStorageT internalLargeStrings_;
	LargeStringStorageT* largeStrings_;
};

}  // namespace gason
