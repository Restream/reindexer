#pragma once

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include "tools/assertrx.h"

namespace reindexer {

struct Point {
	double x, y;
};

template <typename T>
T& operator<<(T& os, Point p) {
	return os << '{' << p.x << ", " << p.y << '}';
}

inline bool approxEqual(double lhs, double rhs) noexcept {
	return std::abs(lhs - rhs) <=
		   ((std::abs(lhs) < std::abs(rhs) ? std::abs(rhs) : std::abs(lhs)) * std::numeric_limits<double>::epsilon());
}

inline bool operator==(Point lhs, Point rhs) noexcept { return approxEqual(lhs.x, rhs.x) && approxEqual(lhs.y, rhs.y); }
inline bool operator!=(Point lhs, Point rhs) noexcept { return !(lhs == rhs); }

struct point_strict_equal {
	bool operator()(const Point& lhs, const Point& rhs) const noexcept {
		return std::equal_to<double>()(lhs.x, rhs.x) && std::equal_to<double>()(lhs.y, rhs.y);
	}
};
struct point_strict_less {
	bool operator()(const Point& lhs, const Point& rhs) const noexcept { return lhs.x < rhs.x || lhs.y < rhs.y; }
};

inline bool DWithin(Point lhs, Point rhs, double distance) noexcept {
	return (lhs.x - rhs.x) * (lhs.x - rhs.x) + (lhs.y - rhs.y) * (lhs.y - rhs.y) <= distance * distance;
}

class Rectangle {
public:
	Rectangle() noexcept : left_{}, right_{}, bottom_{}, top_{} {}
	Rectangle(Point a, Point b) noexcept
		: left_{std::min(a.x, b.x)}, right_{std::max(a.x, b.x)}, bottom_{std::min(a.y, b.y)}, top_{std::max(a.y, b.y)} {}
	Rectangle(double l, double r, double b, double t) noexcept
		: left_{std::min(l, r)}, right_{std::max(l, r)}, bottom_{std::min(b, t)}, top_{std::max(b, t)} {}
	double Left() const noexcept { return left_; }
	double Right() const noexcept { return right_; }
	double Bottom() const noexcept { return bottom_; }
	double Top() const noexcept { return top_; }
	double Area() const noexcept { return (right_ - left_) * (top_ - bottom_); }
	bool Contain(Point p) const noexcept { return left_ <= p.x && p.x <= right_ && bottom_ <= p.y && p.y <= top_; }
	bool Contain(const Rectangle& r) const noexcept {
		return left_ <= r.left_ && r.right_ <= right_ && bottom_ <= r.bottom_ && r.top_ <= top_;
	}

private:
	double left_, right_, bottom_, top_;
};

inline bool operator==(const Rectangle& lhs, const Rectangle& rhs) noexcept {
	return approxEqual(lhs.Left(), rhs.Left()) && approxEqual(lhs.Right(), rhs.Right()) && approxEqual(lhs.Bottom(), rhs.Bottom()) &&
		   approxEqual(lhs.Top(), rhs.Top());
}
inline bool operator!=(const Rectangle& lhs, const Rectangle& rhs) noexcept { return !(lhs == rhs); }

inline bool DWithin(const Rectangle& r, Point p, double distance) noexcept {
	return DWithin(Point{r.Left(), r.Bottom()}, p, distance) && DWithin(Point{r.Left(), r.Top()}, p, distance) &&
		   DWithin(Point{r.Right(), r.Bottom()}, p, distance) && DWithin(Point{r.Right(), r.Top()}, p, distance);
}

inline Rectangle boundRect(Point p) noexcept { return {p.x, p.x, p.y, p.y}; }

inline Rectangle boundRect(const Rectangle& r1, const Rectangle& r2) noexcept {
	return {std::min(r1.Left(), r2.Left()), std::max(r1.Right(), r2.Right()), std::min(r1.Bottom(), r2.Bottom()),
			std::max(r1.Top(), r2.Top())};
}

inline Rectangle boundRect(const Rectangle& r, Point p) noexcept {
	return {std::min(r.Left(), p.x), std::max(r.Right(), p.x), std::min(r.Bottom(), p.y), std::max(r.Top(), p.y)};
}

class Circle {
public:
	Circle() noexcept = default;
	Circle(Point c, double r) : center_(c), radius_(r) { assertrx(radius_ >= 0.0); }
	Point Center() const noexcept { return center_; }
	double Radius() const noexcept { return radius_; }

private:
	Point center_;
	double radius_;
};

inline bool intersect(const Rectangle& r, const Circle& c) noexcept {
	if (c.Center().x < r.Left()) {
		const auto diffX = r.Left() - c.Center().x;
		if (c.Center().y < r.Bottom()) {
			const auto diffY = r.Bottom() - c.Center().y;
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else if (c.Center().y > r.Top()) {
			const auto diffY = c.Center().y - r.Top();
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else {
			return diffX <= c.Radius();
		}
	} else if (c.Center().x > r.Right()) {
		const auto diffX = c.Center().x - r.Right();
		if (c.Center().y < r.Bottom()) {
			const auto diffY = r.Bottom() - c.Center().y;
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else if (c.Center().y > r.Top()) {
			const auto diffY = c.Center().y - r.Top();
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else {
			return diffX <= c.Radius();
		}
	} else {
		return c.Center().y + c.Radius() >= r.Bottom() && c.Center().y - c.Radius() <= r.Top();
	}
}

}  // namespace reindexer

namespace std {

template <>
struct hash<reindexer::Point> {
	size_t operator()(reindexer::Point p) const noexcept { return (hash<double>()(p.x) << 1) ^ hash<double>()(p.y); }
};

}  // namespace std
