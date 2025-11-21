#pragma once

#include <atomic>
#include <cstddef>
#include <cstring>
#include <limits>
#include <new>
#include <string_view>
#include "estl/defines.h"
#include "estl/fast_hash_traits.h"

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4200)
#endif

namespace reindexer {

#pragma pack(push, 1)
class [[nodiscard]] key_string_impl {
public:
	using size_type = uint32_t;

	key_string_impl(const key_string_impl&) = delete;
	key_string_impl(key_string_impl&&) = delete;
	key_string_impl& operator=(const key_string_impl&) = delete;
	key_string_impl& operator=(key_string_impl&&) = delete;

	static ptrdiff_t export_hdr_offset() noexcept {
		const static key_string_impl sample(std::string_view{});
		// NOLINTBEGIN (*PointerSub)
		const static ptrdiff_t offset =
			ptrdiff_t(reinterpret_cast<const char*>(&sample.export_hdr_) - reinterpret_cast<const char*>(&sample));
		// NOLINTEND (*PointerSub)
		return offset;
	}
	const char* data() const noexcept { return data_; }
	size_t size() const noexcept { return export_hdr_.len; }
	operator std::string_view() const noexcept { return std::string_view(data_, export_hdr_.len); }

	// Unsafe ref counter methods for direct payload access
	static void addref_unsafe(const key_string_impl* x) noexcept {
		if (x) {
			x->refcounter_.fetch_add(1, std::memory_order_relaxed);
		}
	}
	static void release_unsafe(const key_string_impl* x) noexcept {
		if ((x && x->refcounter_.fetch_sub(1, std::memory_order_acq_rel) == 1)) {
			x->~key_string_impl();
			operator delete(const_cast<key_string_impl*>(x));
		}
	}

private:
	friend class key_string;
	// Only key_string should be able to construct key_string_impl
	explicit key_string_impl(std::string_view str) noexcept : refcounter_{0} {
		export_hdr_.len = str.size();
		std::memcpy(data_, str.data(), str.size());
	}

	mutable std::atomic<int32_t> refcounter_;
	struct [[nodiscard]] export_hdr {
		size_type len;
	} export_hdr_;
	char data_[];
};
#pragma pack(pop)

static_assert(sizeof(key_string_impl) == 8, "Impl class must be a header of fixed size before string data");
static_assert(sizeof(std::atomic<int32_t>) == sizeof(int8_t[4]),
			  "refcounter in cbinding (struct reindexer_string) is reserved via int8_t array. Sizes must be same");

class [[nodiscard]] key_string {
public:
	using const_iterator = const char*;
	using iterator = const_iterator;

	key_string() noexcept : impl_(nullptr) {}
	explicit key_string(std::nullptr_t) noexcept : key_string() {}
	key_string(const key_string_impl* str) noexcept : impl_(str) { key_string_impl::addref_unsafe(impl_); }
	key_string(const key_string_impl* str, bool add_ref) noexcept : impl_(str) {
		if (add_ref) {
			key_string_impl::addref_unsafe(impl_);
		}
	}
	explicit key_string(std::string_view str) {
		if (str.size() > kMaxLen) [[unlikely]] {
			throwMaxLenOverflow(str.size());
		}
		void* impl = operator new(sizeof(key_string_impl) + str.size());
		impl_ = new (impl) key_string_impl(str);
		key_string_impl::addref_unsafe(impl_);
	}
	key_string(const key_string& rhs) noexcept : impl_(rhs.impl_) { key_string_impl::addref_unsafe(impl_); }
	key_string(key_string&& rhs) noexcept : impl_(rhs.impl_) { rhs.impl_ = nullptr; }
	~key_string() { key_string_impl::release_unsafe(impl_); }

	key_string& operator=(key_string&& rhs) noexcept {
		swap(rhs);
		return *this;
	}
	key_string& operator=(const key_string& rhs) noexcept {
		key_string copy(rhs);
		swap(copy);
		return *this;
	}

	const key_string_impl* get() const noexcept { return impl_; }
	size_t size() const noexcept { return impl_ ? impl_->size() : 0; }
	const char* data() const& noexcept { return impl_ ? impl_->data() : nullptr; }
	const char* data() && = delete;

	explicit operator bool() const noexcept { return impl_; }
	operator std::string_view() const noexcept { return impl_ ? std::string_view(*impl_) : std::string_view(); }
	void swap(key_string& rhs) noexcept { std::swap(impl_, rhs.impl_); }
	size_t heap_size() const noexcept { return impl_ ? (sizeof(key_string_impl) + impl_->size()) : 0; }

	iterator begin() const& noexcept { return impl_ ? impl_->data() : nullptr; }
	iterator end() const& noexcept { return impl_ ? (impl_->data() + impl_->size()) : nullptr; }
	const_iterator cbegin() const& noexcept { return begin(); }
	const_iterator cend() const& noexcept { return end(); }
	iterator begin() const&& = delete;
	iterator end() && = delete;
	const_iterator cbegin() && = delete;
	const_iterator cend() && = delete;

private:
	constexpr static size_t kMaxLen = std::numeric_limits<key_string_impl::size_type>::max();

	[[noreturn]] void throwMaxLenOverflow(size_t len);

	const key_string_impl* impl_;
};

template <typename... Args>
key_string make_key_string(Args&&... args) {
	return key_string(std::string_view(std::forward<Args>(args)...));
}

template <typename T>
T& operator<<(T& os, const key_string& k) {
	return os << std::string_view(k);
}

inline static bool operator==(const key_string& lhs, const key_string& rhs) noexcept {
	return std::string_view(rhs) == std::string_view(lhs);
}
inline static bool operator==(const key_string& lhs, std::string_view rhs) noexcept { return std::string_view(lhs) == rhs; }
inline static bool operator==(std::string_view lhs, const key_string& rhs) noexcept { return std::string_view(rhs) == lhs; }

template <>
struct [[nodiscard]] is_recommends_sc_hash_map<key_string> {
	constexpr static bool value = true;
};

}  // namespace reindexer

namespace std {
template <>
struct [[nodiscard]] hash<reindexer::key_string> {
public:
	size_t operator()(const reindexer::key_string& obj) const noexcept { return hash<std::string_view>()(obj); }
};

}  // namespace std

#ifdef _MSC_VER
#pragma warning(pop)
#endif
