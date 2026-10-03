#pragma once

#include <vector>

namespace hnswlib {
template <class T, class Container = std::vector<T>, class Compare = std::less<T>>
class [[nodiscard]] PriorityQueue {
public:
	PriorityQueue() = default;
	explicit PriorityQueue(const Compare& comp) : comp_(comp) {}
	explicit PriorityQueue(const Compare& comp, const Container& cont) : c_(cont), comp_(comp) { make_heap(); }
	explicit PriorityQueue(const Compare& comp, Container&& cont) : c_(std::move(cont)), comp_(comp) { make_heap(); }

	bool empty() const noexcept { return c_.empty(); }
	std::size_t size() const noexcept { return c_.size(); }
	const T& top() const noexcept { return c_.front(); }

	void push(const T& value) {
		c_.push_back(value);
		push_heap();
	}

	void push(T&& value) {
		c_.push_back(std::move(value));
		push_heap();
	}

	template <class... Args>
	void emplace(Args&&... args) {
		c_.emplace_back(std::forward<Args>(args)...);
		push_heap();
	}

	void pop() {
		pop_heap();
		c_.pop_back();
	}

	void replace_top(const T& value) {
		c_.front() = value;
		sift_down(0, c_.size());
	}

	T replace_top(T&& value) {
		auto top = std::move(c_.front());
		c_.front() = std::move(value);
		sift_down(0, c_.size());
		return top;
	}

	template <class... Args>
	T replace_top(Args&&... args) {
		auto top = std::move(c_.front());
		c_.front() = T(std::forward<Args>(args)...);
		sift_down(0, c_.size());
		return top;
	}

	void clear() { c_.clear(); }

	Container& container() & noexcept { return c_; }

	const Container& container() const& noexcept { return c_; }

	auto container() const&& = delete;

private:
	Container c_;
	Compare comp_;

	void make_heap() {
		const std::size_t n = c_.size();
		if (n < 2) {
			return;
		}

		for (std::size_t parent = (n - 2) / 2;; --parent) {
			sift_down(parent, n);
			if (parent == 0) {
				break;
			}
		}
	}

	void push_heap() {
		const std::size_t n = c_.size();
		if (n < 2) {
			return;
		}

		sift_up(n - 1);
	}

	void pop_heap() {
		const std::size_t n = c_.size();
		if (n < 2) {
			return;
		}
		if (n == 2) {
			std::swap(c_[0], c_[1]);
			return;
		}

		std::swap(c_[0], c_[n - 1]);
		sift_down(0, n - 1);
	}

	void sift_up(std::size_t child) {
		auto value = std::move(c_[child]);

		while (child > 0) {
			const std::size_t parent = (child - 1) / 2;

			if (!comp_(c_[parent], value)) {
				break;
			}

			c_[child] = std::move(c_[parent]);
			child = parent;
		}

		c_[child] = std::move(value);
	}

	void sift_down(std::size_t parent, std::size_t heap_size) {
		auto value = std::move(c_[parent]);

		while (true) {
			const std::size_t left = parent * 2 + 1;

			if (left >= heap_size) {
				break;
			}

			std::size_t best_child = left;
			const std::size_t right = left + 1;

			if (right < heap_size && comp_(c_[left], c_[right])) {
				best_child = right;
			}

			if (!comp_(value, c_[best_child])) {
				break;
			}

			c_[parent] = std::move(c_[best_child]);
			parent = best_child;
		}

		c_[parent] = std::move(value);
	}
};
}  // namespace hnswlib
