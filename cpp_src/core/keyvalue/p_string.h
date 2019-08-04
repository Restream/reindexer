#pragma once

#include <assert.h>
#include <string>
#include <vector>
#include "estl/string_view.h"
#include "key_string.h"
#include "tools/customhash.h"
#include "tools/varint.h"

namespace reindexer {

using std::string;

struct l_string_hdr {
	int length;
	char data[1];
};

struct v_string_hdr {
	uint8_t data[1];
};

struct json_string_ftr {
	const char *data;
};

// Dark
struct p_string {
	// ptr points to c null-terminated string
	constexpr static uint64_t tagCstr = 0x0ULL;
	// ptr points to 4 byte string len header, followed by string's char array
	constexpr static uint64_t tagLstr = 0x1ULL;
	// ptr points to c++ std::string object
	constexpr static uint64_t tagCxxstr = 0x2ULL;
	// ptr points to varint len header, followed by string's char array
	constexpr static uint64_t tagVstr = 0x3ULL;
	// ptr points to slice object
	constexpr static uint64_t tagSlice = 0x4ULL;
	// pyr points to key_string payload atomic_rc_wrapper<base_key_string>
	constexpr static uint64_t tagKeyString = 0x5ULL;
	// pyr points to json_string
	constexpr static uint64_t tagJsonStr = 0x6ULL;
	// offset of tag in pointer
	constexpr static uint64_t tagShift = 59ULL;
	constexpr static uint64_t tagMask = 0x7ULL << tagShift;

	explicit p_string(const l_string_hdr *lstr) : v((uintptr_t(lstr) & ~tagMask) | (tagLstr << tagShift)) {}
	explicit p_string(const v_string_hdr *vstr) : v((uintptr_t(vstr) & ~tagMask) | (tagVstr << tagShift)) {}
	explicit p_string(const char *cstr) : v((uintptr_t(cstr) & ~tagMask) | (tagCstr << tagShift)) {}
	explicit p_string(const json_string_ftr jstr) : v((uintptr_t(jstr.data) & ~tagMask) | (tagJsonStr << tagShift)) {}
	explicit p_string(const string *str) : v((uintptr_t(str) & ~tagMask) | (tagCxxstr << tagShift)) {}
	explicit p_string(const key_string &str) : v((uintptr_t(str.get()) & ~tagMask) | (tagKeyString << tagShift)) {}
	explicit p_string(const string_view *ptr) : v((uintptr_t(ptr) & ~tagMask) | (tagSlice << tagShift)) {}
	p_string() : v(0) {}

	operator string_view() const { return string_view(data(), length()); }
	const char *data() const {
		switch (type()) {
			case tagCstr:
				return reinterpret_cast<const char *>(ptr());
			case tagCxxstr:
			case tagKeyString:
				return (reinterpret_cast<const string *>(ptr()))->data();
			case tagSlice:
				return (reinterpret_cast<const string_view *>(ptr()))->data();
			case tagLstr:
				return &(reinterpret_cast<const l_string_hdr *>(ptr()))->data[0];
			case tagVstr: {
				auto p = reinterpret_cast<const uint8_t *>(ptr());
				auto l = scan_varint(10, p);
				return reinterpret_cast<const char *>(p) + l;
			}
			case tagJsonStr: {
				auto p = reinterpret_cast<const uint8_t *>(ptr());
				return reinterpret_cast<const char *>(p) - (p[0] | (p[1] << 8) | (p[2] << 16));
			}
			default:
				abort();
		}
	}
	size_t size() const { return length(); }
	size_t length() const {
		switch (type()) {
			case tagCstr:
				return strlen(reinterpret_cast<const char *>(ptr()));
			case tagCxxstr:
			case tagKeyString:
				return (reinterpret_cast<const string *>(ptr()))->length();
			case tagSlice:
				return (reinterpret_cast<const string_view *>(ptr()))->size();
			case tagLstr:
				return (reinterpret_cast<const l_string_hdr *>(ptr()))->length;
			case tagVstr: {
				auto p = reinterpret_cast<const uint8_t *>(ptr());
				auto l = scan_varint(10, p);
				return parse_uint32(l, p);
			}
			case tagJsonStr: {
				auto p = reinterpret_cast<const uint8_t *>(ptr());
				return p[0] | (p[1] << 8) | (p[2] << 16);
			}
			default:
				abort();
		}
	}
	int compare(p_string other) const {
		int l1 = length();
		int l2 = other.length();
		int res = memcmp(data(), other.data(), std::min(l1, l2));
		return res ? res : ((l1 < l2) ? -1 : (l1 > l2) ? 1 : 0);
	}
	bool operator>(p_string other) const { return compare(other) > 0; }
	bool operator<(p_string other) const { return compare(other) < 0; }
	bool operator==(p_string other) const { return compare(other) == 0; }
	bool operator>=(p_string other) const { return compare(other) >= 0; }
	bool operator<=(p_string other) const { return compare(other) <= 0; }
	const string *getCxxstr() const {
		assert(type() == tagCxxstr || type() == tagKeyString);
		return reinterpret_cast<const string *>(ptr());
	};

	key_string getKeyString() const {
		assert(type() == tagKeyString);
		auto *str = reinterpret_cast<intrusive_atomic_rc_wrapper<base_key_string> *>(const_cast<void *>(ptr()));
		return key_string(str);
	};

	key_string getOrMakeKeyString() const {
		if (type() == tagKeyString) {
			return getKeyString();
		} else {
			const auto sv = operator string_view();
			return make_key_string(sv.data(), sv.size());
		}
	}

	int type() const { return (v & tagMask) >> tagShift; }
	string toString() const { return string(data(), length()); }

protected:
	const void *ptr() const { return v ? reinterpret_cast<const void *>(v & ~tagMask) : ""; }
	uint64_t v;
};

}  // namespace reindexer
namespace std {
template <>
struct hash<reindexer::p_string> {
public:
	size_t operator()(const reindexer::p_string &str) const { return reindexer::_Hash_bytes(str.data(), str.length()); }
};

}  // namespace std
