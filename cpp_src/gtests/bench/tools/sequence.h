#pragma once

#include <atomic>
#include <string>
#include <type_traits>

#include "helpers.h"

namespace internal {

template <typename fromT, typename toT>
struct [[nodiscard]] cast_helper {
	static toT cast(const fromT& value) { return static_cast<toT>(value); }
};

template <typename fromT>
struct [[nodiscard]] cast_helper<fromT, std::string> {
	static std::string cast(const fromT& value);
};
}  // namespace internal

template <typename fromT>
std::string internal::cast_helper<fromT, std::string>::cast(const fromT& value) {
	return std::to_string(value);
}

template <typename counterT = int>
class [[nodiscard]] SequenceBase {
public:
	typedef counterT value_type;
	static_assert(std::is_integral<counterT>::value, "'counterT' must be an integral type");

	SequenceBase(value_type start, value_type count, value_type inc) : start_(start), end_(start + count - 1), inc_(inc) {
		counter_ = start_;
	}

	value_type Next() {
		value_type val;
		if ((val = counter_++) >= end_) {
			return end_;
		}

		return val;
	}

	value_type Start() const { return start_; }
	value_type Count() const { return end_ - start_ + 1; }
	value_type Current() { return counter_.load() - 1; }
	value_type End() const { return end_; }

	std::pair<value_type, value_type> GetRandomIdRange(size_t cnt) {
		auto count = cnt >= static_cast<size_t>(Count()) ? Count() : cnt;
		auto start = random<value_type>(start_, end_ - count - 1);
		return {start, start + count - 1};
	}

	void Reset() { counter_.store(start_); }

	template <typename T>
	T As() {
		return internal::cast_helper<counterT, T>::cast(Current());
	}

private:
	SequenceBase(const SequenceBase&) = delete;
	SequenceBase& operator=(const SequenceBase&) = delete;

private:
	value_type start_;
	value_type end_;
	value_type inc_;

	std::atomic<counterT> counter_;
};

using Sequence = SequenceBase<int>;
