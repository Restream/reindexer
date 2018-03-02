#pragma once

#include <stdio.h>
#include <memory>

namespace reindexer {

template <typename T>
class shared_cow_ptr {
public:
	explicit shared_cow_ptr(std::shared_ptr<T> ptr) : payload_(ptr) {}
	shared_cow_ptr() {}

	const T* operator->() const { return payload_.get(); }

	const T* get() const { return payload_.get(); }
	T* clone() {
		copy_if_not_owner();
		return payload_.get();
	}
	operator bool() const { return bool(payload_); }
	const T& operator*() const { return *payload_; }

private:
	// If we are not the owner of the payload object, make a private copy of it
	void copy_if_not_owner() {
		if (!payload_.unique()) payload_ = std::make_shared<T>(*payload_);
	}

private:
	std::shared_ptr<T> payload_;
};

}  // namespace reindexer
