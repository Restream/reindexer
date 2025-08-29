#pragma once
#include <atomic>
#include <memory>

namespace reindexer {

template <class T>
class [[nodiscard]] atomic_unique_ptr {
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

	void reset(pointer p = pointer(), std::memory_order order = std::memory_order_seq_cst) {
		auto old = ptr.exchange(p, order);
		if (old) {
			delete old;
		}
	}
	operator pointer() const { return ptr; }
	pointer operator->() const { return ptr; }
	pointer get(std::memory_order order = std::memory_order_seq_cst) const { return ptr.load(order); }
	explicit operator bool() const { return ptr != pointer(); }
	pointer release(std::memory_order order = std::memory_order_seq_cst) { return ptr.exchange(pointer(), order); }
	bool compare_exchange_strong(pointer exp, pointer p, std::memory_order order = std::memory_order_seq_cst) {
		return ptr.compare_exchange_strong(exp, p, order);
	}
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

	void reset(pointer p = pointer(), std::memory_order order = std::memory_order_seq_cst) {
		auto old = ptr.exchange(p, order);
		if (old) {
			delete[] old;
		}
	}
	operator pointer() const { return ptr; }
	pointer operator->() const { return ptr; }
	pointer get(std::memory_order order = std::memory_order_seq_cst) const { return ptr.load(order); }
	explicit operator bool() const { return ptr != pointer(); }
	pointer release(std::memory_order order = std::memory_order_seq_cst) { return ptr.exchange(pointer(), order); }
	bool compare_exchange_strong(pointer exp, pointer p, std::memory_order order = std::memory_order_seq_cst) {
		return ptr.compare_exchange_strong(exp, p, order);
	}
	~atomic_unique_ptr() { reset(); }
};

}  // namespace reindexer
