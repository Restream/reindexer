#pragma once

#include <algorithm>
#include <optional>
#include <tuple>
#include <vector>

namespace reindexer::sharding {
template <typename T>
struct Segment {
	explicit Segment(T e) : left(std::move(e)), right(left) {}
	explicit Segment(T e1, T e2) : left(std::move(e1)), right(std::move(e2)) {
		if (left > right) std::swap(left, right);
	}
	T left, right;
	bool isEnabled = true;
};

template <typename T>
std::optional<Segment<T>> getUnion(const Segment<T>& s1, const Segment<T>& s2) {
	const auto& [left, right] = s2.left >= s1.left ? std::tie(s1, s2) : std::tie(s2, s1);

	if (left.right < right.left) return std::nullopt;

	return Segment<T>{left.left, std::max(right.right, left.right)};
}

template <typename T>
std::vector<Segment<T>> getUnion(const std::vector<Segment<T>>& segments) {
	auto result = segments;
	for (size_t i = 0; i < result.size(); ++i) {
		for (size_t j = i + 1; j < result.size(); ++j) {
			if (auto u = getUnion(result[i], result[j])) {
				result[j] = u.value();
				result[i].isEnabled = false;
				break;
			}
		}
	}
	auto it = std::remove_if(result.begin(), result.end(), [](const Segment<T>& x) { return !x.isEnabled; });
	return {result.begin(), it};
}

template <typename T>
bool intersected(const std::vector<Segment<T>>& segments, const Segment<T>& segment) {
	auto intersected = [](const Segment<T>& s1, const Segment<T>& s2) {
		const auto& [left, right] = s2.left >= s1.left ? std::tie(s1, s2) : std::tie(s2, s1);
		return left.right >= right.left;
	};

	for (const auto& s : segments)
		if (intersected(s, segment)) return true;
	return false;
}

}  // namespace reindexer::sharding