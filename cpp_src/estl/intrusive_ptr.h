#pragma once

#include <atomic>
#include "tools/assertrx.h"

namespace reindexer {

template <typename T>
class intrusive_ptr {
private:
	typedef intrusive_ptr this_type;

public:
	typedef T element_type;

	constexpr intrusive_ptr() noexcept = default;
	constexpr intrusive_ptr(std::nullptr_t) noexcept {}

	intrusive_ptr(T* p, bool add_ref = true) noexcept : px(p) {
		if (px != 0 && add_ref) {
			intrusive_ptr_add_ref(px);
		}
	}

	template <typename U>
	intrusive_ptr(const intrusive_ptr<U>& rhs) noexcept : px(rhs.get()) {
		if (px != 0) {
			intrusive_ptr_add_ref(px);
		}
	}

	intrusive_ptr(const intrusive_ptr& rhs) noexcept : px(rhs.px) {
		if (px != 0) {
			intrusive_ptr_add_ref(px);
		}
	}

	~intrusive_ptr() {
		if (px != 0) {
			intrusive_ptr_release(px);
		}
	}

	template <typename U>
	intrusive_ptr& operator=(const intrusive_ptr<U>& rhs) noexcept {
		this_type(rhs).swap(*this);
		return *this;
	}

	intrusive_ptr(intrusive_ptr&& rhs) noexcept : px(rhs.px) { rhs.px = 0; }

	intrusive_ptr& operator=(intrusive_ptr&& rhs) noexcept {
		this_type(static_cast<intrusive_ptr&&>(rhs)).swap(*this);
		return *this;
	}
	// NOLINTNEXTLINE(bugprone-unhandled-self-assignment)
	intrusive_ptr& operator=(const intrusive_ptr& rhs) noexcept {
		this_type(rhs).swap(*this);
		return *this;
	}

	intrusive_ptr& operator=(T* rhs) noexcept {
		this_type(rhs).swap(*this);
		return *this;
	}

	void reset() noexcept { this_type().swap(*this); }

	void reset(T* rhs) noexcept { this_type(rhs).swap(*this); }
	bool unique() const noexcept {
		if (px == 0) {
			return true;
		}
		return intrusive_ptr_is_unique(px);
	}

	T* get() const noexcept { return px; }

	T& operator*() const noexcept {
		assertrx(px != 0);
		return *px;
	}

	T* operator->() const noexcept {
		assertrx(px != 0);
		return px;
	}

	typedef T* this_type::*unspecified_bool_type;

	operator unspecified_bool_type() const noexcept { return px == 0 ? 0 : &this_type::px; }

	void swap(intrusive_ptr& rhs) noexcept {
		T* tmp = px;
		px = rhs.px;
		rhs.px = tmp;
	}

private:
	T* px{nullptr};
};

template <class T, class U>
inline bool operator==(const intrusive_ptr<T>& a, const intrusive_ptr<U>& b) noexcept {
	return a.get() == b.get();
}

template <class T, class U>
inline bool operator!=(const intrusive_ptr<T>& a, const intrusive_ptr<U>& b) noexcept {
	return a.get() != b.get();
}

template <class T, class U>
inline bool operator==(const intrusive_ptr<T>& a, U* b) noexcept {
	return a.get() == b;
}

template <class T, class U>
inline bool operator!=(const intrusive_ptr<T>& a, U* b) noexcept {
	return a.get() != b;
}

template <class T, class U>
inline bool operator==(T* a, const intrusive_ptr<U>& b) noexcept {
	return a == b.get();
}

template <class T, class U>
inline bool operator!=(T* a, const intrusive_ptr<U>& b) noexcept {
	return a != b.get();
}

template <class T>
inline bool operator<(const intrusive_ptr<T>& a, const intrusive_ptr<T>& b) noexcept {
	return std::less<T*>()(a.get(), b.get());
}

template <class T>
void swap(intrusive_ptr<T>& lhs, intrusive_ptr<T>& rhs) noexcept {
	lhs.swap(rhs);
}

template <class T>
T* get_pointer(const intrusive_ptr<T>& p) noexcept {
	return p.get();
}

template <class T, class U>
intrusive_ptr<T> static_pointer_cast(const intrusive_ptr<U>& p) noexcept {
	return static_cast<T*>(p.get());
}

template <class T, class U>
intrusive_ptr<T> const_pointer_cast(const intrusive_ptr<U>& p) noexcept {
	return const_cast<T*>(p.get());
}

template <class T, class U>
intrusive_ptr<T> dynamic_pointer_cast(const intrusive_ptr<U>& p) noexcept {
	return dynamic_cast<T*>(p.get());
}

template <typename T>
class intrusive_atomic_rc_wrapper;

template <typename T>
inline void intrusive_ptr_add_ref(intrusive_atomic_rc_wrapper<T>* x) noexcept {
	if (x) {
		x->refcount.fetch_add(1, std::memory_order_relaxed);
	}
}

template <typename T>
inline void intrusive_ptr_release(intrusive_atomic_rc_wrapper<T>* x) noexcept {
	if (x && x->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
		delete x;
	}
}

template <typename T>
inline bool intrusive_ptr_is_unique(intrusive_atomic_rc_wrapper<T>* x) noexcept {
	// std::memory_order_acquire - is essential for COW constructions based on intrusive_ptr
	return !x || (x->refcount.load(std::memory_order_acquire) == 1);
}

template <typename T>
class intrusive_atomic_rc_wrapper : public T {
public:
	template <typename... Args>
	intrusive_atomic_rc_wrapper(Args&&... args) : T(std::forward<Args>(args)...) {}
	intrusive_atomic_rc_wrapper& operator=(const intrusive_atomic_rc_wrapper&) = delete;

protected:
	std::atomic<int> refcount{0};

	friend void intrusive_ptr_add_ref<>(intrusive_atomic_rc_wrapper<T>* x) noexcept;
	friend void intrusive_ptr_release<>(intrusive_atomic_rc_wrapper<T>* x) noexcept;
	friend bool intrusive_ptr_is_unique<>(intrusive_atomic_rc_wrapper<T>* x) noexcept;
};

template <typename T>
class intrusive_rc_wrapper;

template <typename T>
inline void intrusive_ptr_add_ref(intrusive_rc_wrapper<T>* x) noexcept {
	if (x) {
		++x->refcount;
	}
}

template <typename T>
inline void intrusive_ptr_release(intrusive_rc_wrapper<T>* x) noexcept {
	if (x && --x->refcount == 0) {
		delete x;
	}
}

template <typename T>
inline bool intrusive_ptr_is_unique(intrusive_rc_wrapper<T>* x) noexcept {
	return !x || (x->refcount == 1);
}

template <typename T>
class intrusive_rc_wrapper : public T {
public:
	template <typename... Args>
	intrusive_rc_wrapper(Args&&... args) : T(std::forward<Args>(args)...) {}
	intrusive_rc_wrapper& operator=(const intrusive_rc_wrapper&) = delete;

protected:
	int refcount{0};

	friend void intrusive_ptr_add_ref<>(intrusive_rc_wrapper<T>* x) noexcept;
	friend void intrusive_ptr_release<>(intrusive_rc_wrapper<T>* x) noexcept;
	friend bool intrusive_ptr_is_unique<>(intrusive_rc_wrapper<T>* x) noexcept;
};

class intrusive_atomic_rc_base {
public:
	intrusive_atomic_rc_base& operator=(const intrusive_atomic_rc_base&) = delete;
	virtual ~intrusive_atomic_rc_base() = default;

protected:
	std::atomic<int> refcount{0};

	friend void intrusive_ptr_add_ref(intrusive_atomic_rc_base* x) noexcept;
	friend void intrusive_ptr_release(intrusive_atomic_rc_base* x) noexcept;
	friend bool intrusive_ptr_is_unique(intrusive_atomic_rc_base* x) noexcept;
};

inline void intrusive_ptr_add_ref(intrusive_atomic_rc_base* x) noexcept {
	if (x) {
		x->refcount.fetch_add(1, std::memory_order_relaxed);
	}
}

inline void intrusive_ptr_release(intrusive_atomic_rc_base* x) noexcept {
	if (x && x->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
		delete x;
	}
}

inline bool intrusive_ptr_is_unique(intrusive_atomic_rc_base* x) noexcept {
	// std::memory_order_acquire - is essential for COW constructions based on intrusive_ptr
	return !x || (x->refcount.load(std::memory_order_acquire) == 1);
}

class intrusive_rc_base {
public:
	intrusive_rc_base& operator=(const intrusive_rc_base&) = delete;
	virtual ~intrusive_rc_base() = default;

protected:
	int refcount{0};

	friend void intrusive_ptr_add_ref(intrusive_rc_base* x) noexcept;
	friend void intrusive_ptr_release(intrusive_rc_base* x) noexcept;
	friend bool intrusive_ptr_is_unique(intrusive_rc_base* x) noexcept;
};

inline void intrusive_ptr_add_ref(intrusive_rc_base* x) noexcept {
	if (x) {
		++x->refcount;
	}
}

inline void intrusive_ptr_release(intrusive_rc_base* x) noexcept {
	if (x && --x->refcount == 0) {
		delete x;
	}
}

inline bool intrusive_ptr_is_unique(intrusive_rc_base* x) noexcept { return !x || (x->refcount == 1); }

template <typename T, typename... Args>
intrusive_ptr<T> make_intrusive(Args&&... args) {
	return intrusive_ptr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace reindexer

namespace std {

template <typename T>
struct hash<reindexer::intrusive_atomic_rc_wrapper<T>> {
public:
	size_t operator()(const reindexer::intrusive_atomic_rc_wrapper<T>& obj) const { return hash<T>()(obj); }
};

template <typename T>
struct hash<reindexer::intrusive_ptr<T>> {
public:
	size_t operator()(const reindexer::intrusive_ptr<T>& obj) const { return hash<T>()(*obj); }
};

}  // namespace std
