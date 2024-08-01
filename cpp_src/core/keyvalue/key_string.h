#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include "estl/fast_hash_traits.h"
#include "estl/intrusive_ptr.h"

namespace reindexer {

typedef const std::string const_string;

class base_key_string : public std::string {
public:
	base_key_string(std::string_view str) : std::string(str.data(), str.length()) {
		export_hdr_.refcounter.store(0, std::memory_order_release);
		bind();
	}
	template <typename... Args>
	base_key_string(Args&&... args) : std::string(std::forward<Args>(args)...) {
		export_hdr_.refcounter.store(0, std::memory_order_release);
		bind();
	}

	template <typename... Args>
	void assign(Args&&... args) {
		const_string::assign(std::forward<Args>(args)...);
		bind();
	}
	static ptrdiff_t export_hdr_offset() noexcept {
		static base_key_string sample;
		return ptrdiff_t(reinterpret_cast<const char*>(&sample.export_hdr_) - reinterpret_cast<const char*>(&sample));
	}
	size_t heap_size() noexcept {
		// Check for SSO (small string optimization)
		uintptr_t pstart = uintptr_t(this);
		uintptr_t pend = pstart + sizeof(std::string);
		uintptr_t pdata = uintptr_t(data());
		return (pdata >= pstart && pdata < pend) ? 0 : (capacity() + 1);  // +1 for terminating \0
	}

	// delete all modification methods - to be sure, that base_key_string is mutable, and export will not invalidate after construction
	iterator begin() = delete;
	iterator end() = delete;
	char& operator[](int) = delete;
	template <typename... Args>
	void insert(Args&&... args) = delete;
	template <typename... Args>
	void append(Args&&... args) = delete;
	template <typename... Args>
	void copy(Args&&... args) = delete;
	template <typename... Args>
	void replace(Args&&... args) = delete;
	void push_back(char c) = delete;
	template <typename... Args>
	void erase(Args&&... args) = delete;
	template <typename... Args>
	void reserve(Args&&... args) = delete;
	template <typename... Args>
	void resize(Args&&... args) = delete;
	void at(int) = delete;
	void shrink_to_fit() = delete;
	void clear() = delete;

protected:
	friend void intrusive_ptr_add_ref(base_key_string* x) noexcept {
		if (x) {
			x->export_hdr_.refcounter.fetch_add(1, std::memory_order_relaxed);
		}
	}
	friend void intrusive_ptr_release(base_key_string* x) noexcept {
		if (x && x->export_hdr_.refcounter.fetch_sub(1, std::memory_order_acq_rel) == 1) {
			delete x;  // NOLINT(*.NewDelete) False positive
		}
	}
	friend bool intrusive_ptr_is_unique(base_key_string* x) noexcept {
		// std::memory_order_acquire - is essential for COW constructions based on intrusive_ptr
		return !x || (x->export_hdr_.refcounter.load(std::memory_order_acquire) == 1);
	}

	void bind() noexcept {
		export_hdr_.cstr = std::string::c_str();
		export_hdr_.len = length();
	}

	struct export_hdr {
		const void* cstr;
		int32_t len;
		std::atomic<int32_t> refcounter;
	} export_hdr_;
};

static_assert(sizeof(std::atomic<int32_t>) == sizeof(int8_t[4]),
			  "refcounter in cbinding (struct reindexer_string) is reserved via int8_t array. Sizes must be same");

class key_string : public intrusive_ptr<base_key_string> {
public:
	using intrusive_ptr<base_key_string>::intrusive_ptr;
};

template <typename... Args>
key_string make_key_string(Args&&... args) {
	return key_string(new base_key_string(std::forward<Args>(args)...));
}

inline static bool operator==(const key_string& rhs, const key_string& lhs) noexcept { return *rhs == *lhs; }

// Unchecked cast to derived class!
// It assumes, that all strings in payload are intrusive_ptr
inline void key_string_add_ref(std::string* str) noexcept { intrusive_ptr_add_ref(reinterpret_cast<base_key_string*>(str)); }
inline void key_string_release(std::string* str) noexcept { intrusive_ptr_release(reinterpret_cast<base_key_string*>(str)); }

template <>
struct is_recommends_sc_hash_map<key_string> {
	constexpr static bool value = true;
};

}  // namespace reindexer
namespace std {
template <>
struct hash<reindexer::base_key_string> {
public:
	size_t operator()(const reindexer::base_key_string& obj) const { return hash<std::string>()(obj); }
};

template <>
struct hash<reindexer::key_string> {
public:
	size_t operator()(const reindexer::key_string& obj) const { return hash<reindexer::base_key_string>()(*obj); }
};

}  // namespace std
