#pragma once

#include <span>
#include <string>
#include "key_string.h"
#include "tools/assertrx.h"
#include "tools/customhash.h"
#include "tools/jsonstring.h"
#include "tools/varint.h"

namespace reindexer {

struct [[nodiscard]] l_string_hdr {
	uint32_t length;
	char data[1];
};

struct [[nodiscard]] v_string_hdr {
	uint8_t data[1];
};

struct [[nodiscard]] l_msgpack_hdr {
	uint32_t size;
	const char* ptr;
};

struct [[nodiscard]] json_string_ftr {
	const char* data;
};

// Dark
struct [[nodiscard]] p_string {
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
	// ptr points to key_string payload atomic_rc_wrapper<key_string_impl>
	constexpr static uint64_t tagKeyString = 0x5ULL;
	// ptr points to json_string
	constexpr static uint64_t tagJsonStr = 0x6ULL;
	// ptr points to msgpack string
	constexpr static uint64_t tagMsgPackStr = 0x7ULL;
	// offset of tag in pointer
	constexpr static uint64_t tagShift = 59ULL;
	constexpr static uint64_t tagMask = 0x7ULL << tagShift;

	explicit p_string(const l_string_hdr* lstr) noexcept : v((uintptr_t(lstr) & ~tagMask) | (tagLstr << tagShift)) {}
	explicit p_string(const v_string_hdr* vstr) noexcept : v((uintptr_t(vstr) & ~tagMask) | (tagVstr << tagShift)) {}
	explicit p_string(const l_msgpack_hdr* mstr) noexcept : v((uintptr_t(mstr) & ~tagMask) | (tagMsgPackStr << tagShift)) {}
	explicit p_string(const char* cstr) noexcept : v((uintptr_t(cstr) & ~tagMask) | (tagCstr << tagShift)) {}
	explicit p_string(const json_string_ftr jstr) noexcept : v((uintptr_t(jstr.data) & ~tagMask) | (tagJsonStr << tagShift)) {}
	explicit p_string(const std::string* str) noexcept : v((uintptr_t(str) & ~tagMask) | (tagCxxstr << tagShift)) {}
	explicit p_string(const key_string& str) noexcept : v((uintptr_t(str.get()) & ~tagMask) | (tagKeyString << tagShift)) {}
	explicit p_string(const std::string_view* ptr) noexcept : v((uintptr_t(ptr) & ~tagMask) | (tagSlice << tagShift)) {}
	p_string() noexcept = default;

	operator std::string_view() const noexcept {
		switch (type()) {
			case tagCstr: {
				const auto str = reinterpret_cast<const char*>(ptr());
				return std::string_view(str, strlen(str));
			}
			case tagMsgPackStr: {
				const auto& str = *reinterpret_cast<const l_msgpack_hdr*>(ptr());
				return std::string_view(str.ptr, str.size);
			}
			case tagCxxstr:
				return std::string_view(*reinterpret_cast<const std::string*>(ptr()));
			case tagKeyString:
				return std::string_view(*reinterpret_cast<const key_string_impl*>(ptr()));
			case tagSlice:
				return *reinterpret_cast<const std::string_view*>(ptr());
			case tagLstr: {
				const auto& str = *reinterpret_cast<const l_string_hdr*>(ptr());
				return std::string_view(&str.data[0], str.length);
			}
			case tagVstr: {
				auto p = reinterpret_cast<const uint8_t*>(ptr());
				auto l = scan_varint(10, p);
				return std::string_view(reinterpret_cast<const char*>(p) + l, parse_uint32(l, p));
			}
			case tagJsonStr: {
				return json_string::to_string_view(reinterpret_cast<const uint8_t*>(ptr()));
			}
			default:
				abort();
		}
	}
	const char* data() const noexcept {
		switch (type()) {
			case tagCstr:
				return reinterpret_cast<const char*>(ptr());
			case tagCxxstr:
				return (reinterpret_cast<const std::string*>(ptr()))->data();
			case tagKeyString:
				return (reinterpret_cast<const key_string_impl*>(ptr()))->data();
			case tagMsgPackStr:
				return (reinterpret_cast<const l_msgpack_hdr*>(ptr()))->ptr;
			case tagSlice:
				return (reinterpret_cast<const std::string_view*>(ptr()))->data();
			case tagLstr:
				return &(reinterpret_cast<const l_string_hdr*>(ptr()))->data[0];
			case tagVstr: {
				auto p = reinterpret_cast<const uint8_t*>(ptr());
				auto l = scan_varint(10, p);
				return reinterpret_cast<const char*>(p) + l;
			}
			case tagJsonStr: {
				const auto sv = json_string::to_string_view(reinterpret_cast<const uint8_t*>(ptr()));
				return sv.data();
			}
			default:
				abort();
		}
	}
	size_t size() const noexcept { return length(); }
	size_t length() const noexcept {
		if (v) {
			switch (type()) {
				case tagCstr:
					return strlen(reinterpret_cast<const char*>(ptr()));
				case tagCxxstr:
					return (reinterpret_cast<const std::string*>(ptr()))->length();
				case tagKeyString:
					return (reinterpret_cast<const key_string_impl*>(ptr()))->size();
				case tagSlice:
					return (reinterpret_cast<const std::string_view*>(ptr()))->size();
				case tagLstr:
					return (reinterpret_cast<const l_string_hdr*>(ptr()))->length;
				case tagMsgPackStr:
					return (reinterpret_cast<const l_msgpack_hdr*>(ptr()))->size;
				case tagVstr: {
					auto p = reinterpret_cast<const uint8_t*>(ptr());
					auto l = scan_varint(10, p);
					return parse_uint32(l, p);
				}
				case tagJsonStr: {
					return json_string::length(reinterpret_cast<const uint8_t*>(ptr()));
				}
				default:
					abort();
			}
		}
		return 0;
	}
	int compare(p_string other) const noexcept {
		int l1 = length();
		int l2 = other.length();
		int res = memcmp(data(), other.data(), std::min(l1, l2));
		return res ? res : ((l1 < l2) ? -1 : (l1 > l2) ? 1 : 0);
	}
	bool operator>(p_string other) const noexcept { return compare(other) > 0; }
	bool operator<(p_string other) const noexcept { return compare(other) < 0; }
	bool operator==(p_string other) const noexcept { return compare(other) == 0; }
	bool operator>=(p_string other) const noexcept { return compare(other) >= 0; }
	bool operator<=(p_string other) const noexcept { return compare(other) <= 0; }
	const std::string* getCxxstr() const noexcept {
		assertrx(type() == tagCxxstr);
		return reinterpret_cast<const std::string*>(ptr());
	}

	key_string getKeyString() const noexcept {
		if (type() == tagKeyString) {
			auto str = reinterpret_cast<key_string_impl*>(const_cast<void*>(ptr()));
			return key_string(str);
		} else {
			return make_key_string(data(), size());
		}
	}
	const key_string_impl* getBaseKeyString() const noexcept {
		assertrx(type() == tagKeyString);
		return reinterpret_cast<key_string_impl*>(const_cast<void*>(ptr()));
	}

	int type() const noexcept { return (v & tagMask) >> tagShift; }
	std::string toString() const { return std::string(data(), length()); }
	void Dump(std::ostream& os) const;

protected:
	const void* ptr() const noexcept { return v ? reinterpret_cast<const void*>(v & ~tagMask) : ""; }

	uint64_t v = 0;
};

inline std::span<char> giftStr(p_string s) noexcept {
#ifndef _GLIBCXX_USE_CXX11_ABI
	if (s.type() == p_string::tagCxxstr) {
		// Trying to avoid COW-string problems
		auto strPtr = s.getCxxstr();
		return std::span<char>(const_cast<std::string*>(strPtr)->data(), strPtr->size());
	}
#endif	// _GLIBCXX_USE_CXX11_ABI
	return std::span<char>(const_cast<char*>(s.data()), s.size());
}

}  // namespace reindexer
namespace std {
template <>
struct [[nodiscard]] hash<reindexer::p_string> {
public:
	size_t operator()(const reindexer::p_string& str) const noexcept { return reindexer::_Hash_bytes(str.data(), str.length()); }
};

}  // namespace std
