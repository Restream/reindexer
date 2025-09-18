#ifndef _MSC_VER
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

#include "gason.h"
#include <stdlib.h>
#include <string>
#include "tools/stringstools.h"
#include "vendor/atoi/atoi.h"
#include "vendor/double-conversion/double-conversion.h"

namespace gason {

#define JSON_ZONE_SIZE 4096
#define JSON_STACK_SIZE 32

using double_conversion::StringToDoubleConverter;

const char* jsonStrError(int err) noexcept {
	switch (err) {
#define XX(no, str) \
	case JSON_##no: \
		return str;
		JSON_ERRNO_MAP(XX)
#undef XX
		default:
			return "unknown";
	}
}

void* JsonAllocator::allocate(size_t size) noexcept {
	size = (size + 7) & ~7;

	if (head && head->used + size <= JSON_ZONE_SIZE) {
		char* p = (char*)head + head->used;
		head->used += size;
		return p;
	}

	size_t allocSize = sizeof(Zone) + size;
	Zone* zone = (Zone*)malloc(allocSize <= JSON_ZONE_SIZE ? JSON_ZONE_SIZE : allocSize);
	if (zone == nullptr) {
		return nullptr;
	}
	zone->used = allocSize;
	if (allocSize <= JSON_ZONE_SIZE || head == nullptr) {
		zone->next = head;
		head = zone;
	} else {
		zone->next = head->next;
		head->next = zone;
	}
	return (char*)zone + sizeof(Zone);
}

void JsonAllocator::deallocate() noexcept {
	while (head) {
		Zone* next = head->next;
		free(head);
		head = next;
	}
	head = nullptr;
}

static inline bool isspace(char c) { return c == ' ' || (c >= '\t' && c <= '\r'); }

static inline bool isdelim(char c) { return c == ',' || c == ':' || c == ']' || c == '}' || isspace(c) || !c; }

static inline bool isdigit(char c) { return c >= '0' && c <= '9'; }

static inline bool isxdigit(char c) { return (c >= '0' && c <= '9') || ((c & ~' ') >= 'A' && (c & ~' ') <= 'F'); }

static inline int char2int(char c) {
	if (c <= '9') {
		return c - '0';
	}
	return (c & ~' ') - 'A' + 10;
}

static bool isDouble(char* s, size_t& size) {
	size = 0;

	if (*s == '-') {
		++s;
		++size;
	}

	while (isdigit(*s)) {
		++s;
		++size;
	}
	bool res = false;

	if (*s == '.') {
		++s;
		++size;
		while (isdigit(*s)) {
			++s;
			++size;
		}
		res = true;
	}
	if (*s == 'e' || *s == 'E') {
		++s;
		++size;
		if (*s == '+' || *s == '-') {
			++s;
			++size;
		}
		while (isdigit(*s)) {
			++s;
			++size;
		}
		res = true;
	}
	return res;
}

static double string2double(char* s, char** endptr, size_t size) {
	StringToDoubleConverter sd(StringToDoubleConverter::NO_FLAGS, 0, 0, "inf", "nan");
	int len;
	double vv = sd.StringToDouble(s, size, &len);
	*endptr = s + len;
	return vv;
}

static inline JsonNode* insertAfter(JsonNode* tail, JsonNode* node) {
	if (!tail) {
		return node->next = node;
	}
	node->next = tail->next;
	tail->next = node;
	return node;
}

static inline JsonValue listToValue(JsonTag tag, JsonNode* tail) {
	if (tail) {
		auto head = tail->next;
		tail->next = nullptr;
		return JsonValue(tag, head);
	}
	return JsonValue(tag, nullptr);
}

static int jsonParse(std::span<char> str, char** endptr, JsonValue* value, JsonAllocator& allocator, LargeStringStorageT& largeStrings) {
	JsonNode* tails[JSON_STACK_SIZE];
	JsonTag tags[JSON_STACK_SIZE];
	JsonString keys[JSON_STACK_SIZE];
	JsonValue o;
	int pos = -1;
	char* s = str.data();
	size_t l = str.size();
	bool separator = true;
	JsonNode* node;
	*endptr = s;
	size_t size = 0;

	while (l) {
		while (l && isspace(*s)) {
			++s, --l;
			if (!l) {
				break;
			}
		}
		if (!l) {
			break;
		}
		*endptr = s++;
		--l;
		bool negative = false;
		switch (**endptr) {
			case '-':
				if (!isdigit(*s) && *s != '.') {
					*endptr = s;
					return JSON_BAD_NUMBER;
				}
				negative = true;
			// fall through
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				if (isDouble(*endptr, size)) {
					o = JsonValue(string2double(*endptr, &s, size));
				} else if (negative) {
					bool tmpb;
					o = JsonValue(jsteemann::atoi<int64_t>(*endptr, *endptr + size, tmpb), true);
					s = s + size - 1;
				} else {
					bool tmpb;
					o = JsonValue(jsteemann::atoi<uint64_t>(*endptr, *endptr + size, tmpb), false);
					s = s + size - 1;
				}

				if (!isdelim(*s)) {
					*endptr = s;
					return JSON_BAD_NUMBER;
				}
				break;
			case '"':
				if (s - str.data() < 2) {
					return JSON_UNEXPECTED_CHARACTER;
				}
				for (char* it = s - 2; l; ++it, ++s, --l) {
					int c = *it = *s;
					if (c == '\\') {
						c = *++s;
						l--;
						switch (c) {
							case '\\':
							case '"':
							case '/':
								*it = c;
								break;
							case 'b':
								*it = '\b';
								break;
							case 'f':
								*it = '\f';
								break;
							case 'n':
								*it = '\n';
								break;
							case 'r':
								*it = '\r';
								break;
							case 't':
								*it = '\t';
								break;
							case 'u':
								c = 0;
								for (int i = 0; i < 4; ++i, --l) {
									if (isxdigit(*++s)) {
										c = c * 16 + char2int(*s);
									} else {
										*endptr = s;
										return JSON_BAD_STRING;
									}
								}
								if (c < 0x80) {
									*it = c;
								} else if (c < 0x800) {
									*it++ = 0xC0 | (c >> 6);
									*it = 0x80 | (c & 0x3F);
								} else {
									*it++ = 0xE0 | (c >> 12);
									*it++ = 0x80 | ((c >> 6) & 0x3F);
									*it = 0x80 | (c & 0x3F);
								}
								break;
							default:
								*endptr = s;
								return JSON_BAD_STRING;
						}
					} else if (c == '"') {
						o = JsonValue(JsonString((*endptr) - 1, it, largeStrings));
						++s, --l;
						break;
					}
				}
				if (!isdelim(*s)) {
					*endptr = s;
					return JSON_BAD_STRING;
				}
				break;
			case 't':
				if (!(s[0] == 'r' && s[1] == 'u' && s[2] == 'e' && isdelim(s[3]))) {
					return JSON_BAD_IDENTIFIER;
				}
				o = JsonValue(JsonTag::JTRUE);
				s += 3, l -= 3;
				break;
			case 'f':
				if (!(s[0] == 'a' && s[1] == 'l' && s[2] == 's' && s[3] == 'e' && isdelim(s[4]))) {
					return JSON_BAD_IDENTIFIER;
				}
				o = JsonValue(JsonTag::JFALSE);
				s += 4, l -= 4;
				break;
			case 'n':
				if (!(s[0] == 'u' && s[1] == 'l' && s[2] == 'l' && isdelim(s[3]))) {
					return JSON_BAD_IDENTIFIER;
				}
				o = JsonValue(JsonTag::JSON_NULL);
				s += 3, l -= 3;
				break;
			case ']':
				if (pos == -1) {
					return JSON_STACK_UNDERFLOW;
				}
				if (tags[pos] != JsonTag::ARRAY) {
					return JSON_MISMATCH_BRACKET;
				}
				o = listToValue(JsonTag::ARRAY, tails[pos--]);
				break;
			case '}':
				if (pos == -1) {
					return JSON_STACK_UNDERFLOW;
				}
				if (tags[pos] != JsonTag::OBJECT) {
					return JSON_MISMATCH_BRACKET;
				}
				if (keys[pos].ptr != nullptr) {
					return JSON_UNEXPECTED_CHARACTER;
				}
				o = listToValue(JsonTag::OBJECT, tails[pos--]);
				break;
			case '[':
				if (++pos == JSON_STACK_SIZE) {
					return JSON_STACK_OVERFLOW;
				}
				tails[pos] = nullptr;
				tags[pos] = JsonTag::ARRAY;
				keys[pos] = JsonString();
				separator = true;
				continue;
			case '{':
				if (++pos == JSON_STACK_SIZE) {
					return JSON_STACK_OVERFLOW;
				}
				tails[pos] = nullptr;
				tags[pos] = JsonTag::OBJECT;
				keys[pos] = JsonString();
				separator = true;
				continue;
			case ':':
				if (separator || keys[pos].ptr == nullptr) {
					return JSON_UNEXPECTED_CHARACTER;
				}
				separator = true;
				continue;
			case ',':
				if (separator || keys[pos].ptr != nullptr) {
					return JSON_UNEXPECTED_CHARACTER;
				}
				separator = true;
				continue;
			case '\0':
				continue;
			default:
				return JSON_UNEXPECTED_CHARACTER;
		}

		separator = false;

		if (pos == -1) {
			*endptr = s;
			*value = o;
			return JSON_OK;
		}

		if (tags[pos] == JsonTag::OBJECT) {
			if (!keys[pos].ptr) {
				if (o.getTag() != JsonTag::STRING) {
					return JSON_UNQUOTED_KEY;
				}
				keys[pos] = o.sval;
				continue;
			}
			if ((node = (JsonNode*)allocator.allocate(sizeof(JsonNode))) == nullptr) {
				return JSON_ALLOCATION_FAILURE;
			}
			tails[pos] = insertAfter(tails[pos], node);
			tails[pos]->key = keys[pos];
			keys[pos] = JsonString();
		} else {
			if ((node = (JsonNode*)allocator.allocate(sizeof(JsonNode))) == nullptr) {
				return JSON_ALLOCATION_FAILURE;
			}
			tails[pos] = insertAfter(tails[pos], node);
			tails[pos]->key = JsonString();
		}
		tails[pos]->value = o;
	}
	return JSON_BREAKING_BAD;
}

JsonNode* JsonNode::toNode() const {
	if (empty() || value.getTag() == JsonTag::JSON_NULL) {
		return nullptr;
	}
	if (value.getTag() != JsonTag::OBJECT && value.getTag() != JsonTag::ARRAY) {
		throw Exception(std::string("Can't convert json field '") + std::string(key) + "' to object or array");
	}
	return value.toNode();
}

const JsonNode& JsonNode::operator[](std::string_view key) const {
	if (value.getTag() != JsonTag::OBJECT && value.getTag() != JsonTag::JSON_NULL) {
		throw Exception(std::string("Can't obtain json field '") + std::string(key) + "' from non-object json node");
	}
	for (auto& v : (*this)) {
		if (std::string_view(v.key) == key) {
			return v;
		}
	}
	static const JsonNode empty_node = JsonNode::Empty();
	return empty_node;
}

const JsonNode& JsonNode::findCaseInsensitive(std::string_view key) const {
	if (value.getTag() != JsonTag::OBJECT && value.getTag() != JsonTag::JSON_NULL) {
		throw Exception(std::string("Can't obtain json field '") + std::string(key) + "' from non-object json node");
	}
	const auto keyLower = reindexer::toLower(key);
	for (auto& v : (*this)) {
		if (reindexer::toLower(v.key) == keyLower) {
			return v;
		}
	}
	static JsonNode empty_node = JsonNode::Empty();
	return empty_node;
}

// TODO: Remove NOLINT after pyreindexer update. Issue #1736
JsonNode JsonNode::EmptyNode() noexcept { return {{JsonTag::EMPTY}, nullptr, {}}; }	 // NOLINT(*EnumCastOutOfRange)

JsonNode JsonParser::Parse(std::span<char> str, size_t* length) & {
	largeStrings_->clear();
	char* endp = nullptr;
	JsonNode val{{}, nullptr, {}};
	const char* begin = str.data();
	int status = jsonParse(str, &endp, &val.value, alloc_, *largeStrings_);
	if (status != JSON_OK) {
		size_t pos = endp - str.data();
		throw Exception(std::string("Error parsing json: ") + jsonStrError(status) + ", pos " + std::to_string(pos));
	}
	if (length) {
		*length = endp - begin;
	}
	return val;
}

JsonNode JsonParser::Parse(std::string_view str, size_t* length) & {
	tmp_.reserve(str.size());
	tmp_.assign(str.begin(), str.end());
	return Parse(std::span<char>(&tmp_[0], tmp_.size()), length);
}

static inline bool haveEqualType(JsonTag lt, JsonTag rt) noexcept {
	return lt == rt || (lt == JsonTag::JTRUE && rt == JsonTag::JFALSE) || (lt == JsonTag::JFALSE && rt == JsonTag::JTRUE);
}

bool isHomogeneousArray(const gason::JsonValue& v) noexcept {
	if (v.getTag() != JsonTag::ARRAY) {
		return false;
	}
	bool hasTag = false;
	gason::JsonTag prevTag;
	try {
		for (const auto& elem : v) {
			if (hasTag) {
				if (!haveEqualType(prevTag, elem.value.getTag())) {
					return false;
				}
			} else {
				hasTag = true;
			}
			prevTag = elem.value.getTag();
		}
		return true;
	} catch (Exception&) {
		return false;
	}
}

std::string_view JsonTagToTypeStr(JsonTag tag) noexcept {
	using namespace std::string_view_literals;
	switch (tag) {
		case JsonTag::STRING:
			return "STRING"sv;
		case JsonTag::NUMBER:
			return "NUMBER"sv;
		case JsonTag::DOUBLE:
			return "DOUBLE"sv;
		case JsonTag::ARRAY:
			return "ARRAY"sv;
		case JsonTag::OBJECT:
			return "OBJECT"sv;
		case JsonTag::JTRUE:
		case JsonTag::JFALSE:
			return "BOOL"sv;
		case JsonTag::JSON_NULL:
			return "NULL"sv;
		case JsonTag::EMPTY:
			return "EMPTY"sv;
	}
	return "<UNKNOWN>"sv;
}

}  // namespace gason
