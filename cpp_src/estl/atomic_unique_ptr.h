#pragma once
#include <atomic>
#include <memory>

namespace reindexer {

template <class T>
class atomic_unique_ptr {
	using pointer = T*;
	std::atomic<pointer> ptr;

public:
	constexpr atomic_unique_ptr() noexcept : ptr(nullptr) {}
	explicit atomic_unique_ptr(pointer p) noexcept : ptr(p) {}
	atomic_unique_ptr(atomic_unique_ptr&& p) noexcept : ptr(p.release()) {}
	atomic_unique_ptr& operator=(atomic_unique_ptr&& p) noexcept {
		reset(p.release());
		return *this;
	}
	atomic_unique_ptr(std::unique_ptr<T>&& p) noexcept : ptr(p.release()) {}
	atomic_unique_ptr& operator=(std::unique_ptr<T>&& p) noexcept {
		reset(p.release());
		return *this;
	}

	void reset(pointer p = pointer()) {
		auto old = ptr.exchange(p);
		if (old) delete old;
	}
	operator pointer() const { return ptr; }
	pointer operator->() const { return ptr; }
	pointer get() const { return ptr; }
	explicit operator bool() const { return ptr != pointer(); }
	pointer release() { return ptr.exchange(pointer()); }
	~atomic_unique_ptr() { reset(); }
};

template <class T>
class atomic_unique_ptr<T[]>  // for array types
{
	using pointer = T*;
	std::atomic<pointer> ptr;

public:
	constexpr atomic_unique_ptr() noexcept : ptr(nullptr) {}
	explicit atomic_unique_ptr(pointer p) noexcept : ptr(p) {}
	atomic_unique_ptr(atomic_unique_ptr&& p) noexcept : ptr(p.release()) {}
	atomic_unique_ptr& operator=(atomic_unique_ptr&& p) noexcept {
		reset(p.release());
		return *this;
	}
	atomic_unique_ptr(std::unique_ptr<T>&& p) noexcept : ptr(p.release()) {}
	atomic_unique_ptr& operator=(std::unique_ptr<T>&& p) noexcept {
		reset(p.release());
		return *this;
	}

	void reset(pointer p = pointer()) {
		auto old = ptr.exchange(p);
		if (old) delete[] old;
	}
	operator pointer() const { return ptr; }
	pointer operator->() const { return ptr; }
	pointer get() const { return ptr; }
	explicit operator bool() const { return ptr != pointer(); }
	pointer release() { return ptr.exchange(pointer()); }
	~atomic_unique_ptr() { reset(); }
};

}  // namespace reindexer
