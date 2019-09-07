#pragma once

#include <assert.h>
#include <algorithm>
#include <atomic>

namespace reindexer {

template <typename T>
class intrusive_ptr {
private:
	typedef intrusive_ptr this_type;

public:
	typedef T element_type;

	intrusive_ptr() : px(0) {}

	intrusive_ptr(T *p, bool add_ref = true) : px(p) {
		if (px != 0 && add_ref) intrusive_ptr_add_ref(px);
	}

	template <typename U>
	intrusive_ptr(intrusive_ptr<U> const &rhs)

		: px(rhs.get()) {
		if (px != 0) intrusive_ptr_add_ref(px);
	}

	intrusive_ptr(intrusive_ptr const &rhs) : px(rhs.px) {
		if (px != 0) intrusive_ptr_add_ref(px);
	}

	~intrusive_ptr() {
		if (px != 0) intrusive_ptr_release(px);
	}

	template <typename U>
	intrusive_ptr &operator=(intrusive_ptr<U> const &rhs) {
		this_type(rhs).swap(*this);
		return *this;
	}

	intrusive_ptr(intrusive_ptr &&rhs) : px(rhs.px) { rhs.px = 0; }

	intrusive_ptr &operator=(intrusive_ptr &&rhs) {
		this_type(static_cast<intrusive_ptr &&>(rhs)).swap(*this);
		return *this;
	}
	intrusive_ptr &operator=(intrusive_ptr const &rhs) {
		this_type(rhs).swap(*this);
		return *this;
	}

	intrusive_ptr &operator=(T *rhs) {
		this_type(rhs).swap(*this);
		return *this;
	}

	void reset() { this_type().swap(*this); }

	void reset(T *rhs) { this_type(rhs).swap(*this); }
	bool unique() const {
		if (px == 0) {
			return true;
		}
		return intrusive_ptr_is_unique(px);
	}

	T *get() const { return px; }

	T &operator*() const {
		assert(px != 0);
		return *px;
	}

	T *operator->() const {
		assert(px != 0);
		return px;
	}

	typedef T *this_type::*unspecified_bool_type;

	operator unspecified_bool_type() const { return px == 0 ? 0 : &this_type::px; }

	void swap(intrusive_ptr &rhs) {
		T *tmp = px;
		px = rhs.px;
		rhs.px = tmp;
	}

private:
	T *px;
};

template <class T, class U>
inline bool operator==(intrusive_ptr<T> const &a, intrusive_ptr<U> const &b) {
	return a.get() == b.get();
}

template <class T, class U>
inline bool operator!=(intrusive_ptr<T> const &a, intrusive_ptr<U> const &b) {
	return a.get() != b.get();
}

template <class T, class U>
inline bool operator==(intrusive_ptr<T> const &a, U *b) {
	return a.get() == b;
}

template <class T, class U>
inline bool operator!=(intrusive_ptr<T> const &a, U *b) {
	return a.get() != b;
}

template <class T, class U>
inline bool operator==(T *a, intrusive_ptr<U> const &b) {
	return a == b.get();
}

template <class T, class U>
inline bool operator!=(T *a, intrusive_ptr<U> const &b) {
	return a != b.get();
}

template <class T>
inline bool operator<(intrusive_ptr<T> const &a, intrusive_ptr<T> const &b) {
	return std::less<T *>()(a.get(), b.get());
}

template <class T>
void swap(intrusive_ptr<T> &lhs, intrusive_ptr<T> &rhs) {
	lhs.swap(rhs);
}

template <class T>
T *get_pointer(intrusive_ptr<T> const &p) {
	return p.get();
}

template <class T, class U>
intrusive_ptr<T> static_pointer_cast(intrusive_ptr<U> const &p) {
	return static_cast<T *>(p.get());
}

template <class T, class U>
intrusive_ptr<T> const_pointer_cast(intrusive_ptr<U> const &p) {
	return const_cast<T *>(p.get());
}

template <class T, class U>
intrusive_ptr<T> dynamic_pointer_cast(intrusive_ptr<U> const &p) {
	return dynamic_cast<T *>(p.get());
}

template <typename T>
class intrusive_atomic_rc_wrapper;

template <typename T>
inline static void intrusive_ptr_add_ref(intrusive_atomic_rc_wrapper<T> *x) {
	if (x) {
		x->refcount.fetch_add(1, std::memory_order_relaxed);
	}
}

template <typename T>
inline static void intrusive_ptr_release(intrusive_atomic_rc_wrapper<T> *x) {
	if (x && x->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
		delete x;
	}
}

template <typename T>
inline static bool intrusive_ptr_is_unique(intrusive_atomic_rc_wrapper<T> *x) {
	// std::memory_order_acquire - is essetial for COW constructions based on intrusive_ptr
	return !x || (x->refcount.load(std::memory_order_acquire) == 1);
}

template <typename T>
class intrusive_atomic_rc_wrapper : public T {
public:
	template <typename... Args>
	intrusive_atomic_rc_wrapper(Args &&... args) : T(args...), refcount(0) {}
	intrusive_atomic_rc_wrapper &operator=(const intrusive_atomic_rc_wrapper &) = delete;

protected:
	std::atomic<int> refcount;

	friend void intrusive_ptr_add_ref<>(intrusive_atomic_rc_wrapper<T> *x);
	friend void intrusive_ptr_release<>(intrusive_atomic_rc_wrapper<T> *x);
	friend bool intrusive_ptr_is_unique<>(intrusive_atomic_rc_wrapper<T> *x);
};

template <typename T, typename... Args>
intrusive_ptr<T> make_intrusive(Args &&... args) {
	return intrusive_ptr<T>(new T(args...));
}

}  // namespace reindexer

namespace std {

template <typename T>
struct hash<reindexer::intrusive_atomic_rc_wrapper<T>> {
public:
	size_t operator()(const reindexer::intrusive_atomic_rc_wrapper<T> &obj) const { return hash<T>()(obj); }
};

template <typename T>
struct hash<reindexer::intrusive_ptr<T>> {
public:
	size_t operator()(const reindexer::intrusive_ptr<T> &obj) const { return hash<T>()(*obj); }
};

}  // namespace std
