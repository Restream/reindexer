#pragma once

#include <list>

namespace reindexer {

template <typename T, size_t kRecycleSize = 20>
class [[nodiscard]] RecyclingList {
public:
	typename std::list<T>::iterator begin() noexcept { return list_.begin(); }
	typename std::list<T>::const_iterator begin() const noexcept { return list_.begin(); }
	typename std::list<T>::iterator end() noexcept { return list_.end(); }
	typename std::list<T>::const_iterator end() const noexcept { return list_.end(); }
	template <typename... Args>
	typename std::list<T>::reference emplace_back(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) {
		if (recycleSize_) {
			auto it = recycle_.begin();
			auto& val = *it;
			val = T(std::forward<Args>(args)...);
			list_.splice(list_.end(), recycle_, it);
			--recycleSize_;
			++size_;
			return val;
		} else {
			auto& val = list_.emplace_back(std::forward<Args>(args)...);
			++size_;
			return val;
		}
	}
	typename std::list<T>::iterator erase(typename std::list<T>::iterator it) noexcept(std::is_nothrow_constructible_v<T>) {
		typename std::list<T>::iterator res;
		if (recycleSize_ < kRecycleSize) {
			auto tmp = it++;
			res = it;
			recycle_.splice(recycle_.end(), list_, tmp);
			*(--recycle_.end()) = T();
			++recycleSize_;
		} else {
			res = list_.erase(it);
		}
		--size_;
		return res;
	}
	void pop_front() noexcept(std::is_nothrow_constructible_v<T>) {
		if (recycleSize_ < kRecycleSize) {
			recycle_.splice(recycle_.end(), list_, list_.begin());
			*(--recycle_.end()) = T();
			++recycleSize_;
		} else {
			list_.pop_front();
		}
		--size_;
	}
	size_t size() const noexcept { return size_; }
	bool empty() const noexcept { return !size_; }
	void clear(bool withRecycled = false) {
		list_.clear();
		if (withRecycled) {
			recycle_.clear();
		}
	}

private:
	std::list<T> list_;
	size_t size_ = 0;  // Explicit size counters to avoid std::list::size() O(n) complexity on centos7
	std::list<T> recycle_;
	size_t recycleSize_ = 0;
};

}  // namespace reindexer
