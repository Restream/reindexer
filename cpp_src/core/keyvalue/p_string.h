#pragma once

#include <assert.h>
#include <string>
#include "tools/customhash.h"
#include "tools/slice.h"
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
	// offset of tag in pointer
	constexpr static uint64_t tagShift = 60ULL;
	constexpr static uint64_t tagMask = 0x3ULL << tagShift;

	explicit p_string(const l_string_hdr *lstr) : v(reinterpret_cast<uint64_t>(lstr) | (tagLstr << tagShift)) {}
	explicit p_string(const v_string_hdr *vstr) : v(reinterpret_cast<uint64_t>(vstr) | (tagVstr << tagShift)) {}
	explicit p_string(const char *cstr) : v(reinterpret_cast<uint64_t>(cstr) | (tagCstr << tagShift)) {}
	explicit p_string(const string *str) : v(reinterpret_cast<uint64_t>(str) | (tagCxxstr << tagShift)) {}
	p_string() : v(0) {}

	operator Slice() const { return Slice(data(), length()); }
	const char *data() const {
		switch (type()) {
			case tagCstr:
				return reinterpret_cast<const char *>(ptr());
			case tagCxxstr:
				return (reinterpret_cast<const string *>(ptr()))->data();
			case tagLstr:
				return &(reinterpret_cast<const l_string_hdr *>(ptr()))->data[0];
			case tagVstr: {
				auto p = reinterpret_cast<const uint8_t *>(ptr());
				auto l = scan_varint(10, p);
				return reinterpret_cast<const char *>(p) + l;
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
				return (reinterpret_cast<const string *>(ptr()))->length();
			case tagLstr:
				return (reinterpret_cast<const l_string_hdr *>(ptr()))->length;
			case tagVstr: {
				auto p = reinterpret_cast<const uint8_t *>(ptr());
				auto l = scan_varint(10, p);
				return parse_uint32(l, p);
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
	const string *getCxxstr() {
		assert(((v & tagMask) >> tagShift) == tagCxxstr);
		return reinterpret_cast<const string *>(ptr());
	};

	int type() const { return (v & tagMask) >> tagShift; }

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
