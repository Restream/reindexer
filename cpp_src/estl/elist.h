#pragma once

#include <list>

namespace reindexer {

// Wrapper for std::list, which holds list's size.
// For some older libstdc++ implemetation, std::list::size() has O(N) complection
// (for example, Centos7 with devtoolsets is affected to this issue)
template <typename T, typename Allocator>
class elist {
public:
	using iterator = typename std::list<T, Allocator>::iterator;
	using const_iterator = typename std::list<T, Allocator>::const_iterator;
	using reference = typename std::list<T, Allocator>::reference;
	using const_reference = typename std::list<T, Allocator>::const_reference;
	using pointer = typename std::list<T, Allocator>::pointer;
	using const_pointer = typename std::list<T, Allocator>::const_pointer;
	using value_type = typename std::list<T, Allocator>::value_type;
	using size_type = typename std::list<T, Allocator>::size_type;

	elist() = default;
	explicit elist(const Allocator& a) noexcept : list_(a) {}

	iterator begin() noexcept { return list_.begin(); }
	const_iterator begin() const noexcept { return list_.begin(); }
	const_iterator cbegin() const noexcept { return list_.cbegin(); }
	iterator end() noexcept { return list_.end(); }
	const_iterator end() const noexcept { return list_.end(); }
	const_iterator cend() const noexcept { return list_.cend(); }
	template <typename... Args>
	reference emplace_back(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) {
		auto& val = list_.emplace_back(std::forward<Args>(args)...);
		++size_;
		return val;
	}
	iterator insert(const_iterator pos, T&& v) {
		auto res = list_.insert(pos, std::move(v));
		++size_;
		return res;
	}
	iterator insert(const_iterator pos, const T& v) {
		auto res = list_.insert(pos, v);
		++size_;
		return res;
	}
	iterator erase(const_iterator it) noexcept {
		auto res = list_.erase(it);
		--size_;
		return res;
	}
	void pop_front() noexcept {
		list_.pop_front();
		--size_;
	}
	size_type size() const noexcept { return size_; }
	bool empty() const noexcept { return !size_; }
	void clear() noexcept { list_.clear(); }
	void swap(elist& r) noexcept {
		list_.swap(r.list_);
		std::swap(size_, r.size_);
	}

private:
	std::list<T, Allocator> list_;
	size_type size_ = 0;
};

}  // namespace reindexer
