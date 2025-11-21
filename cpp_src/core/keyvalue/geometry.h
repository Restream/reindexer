#pragma once

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <string_view>
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] Point {
public:
	Point() noexcept : x_(0.0), y_(0.0) {}
	explicit Point(double x, double y) : x_(x), y_(y) {
		validate(x, "x");
		validate(y, "y");
	}
	double X() const noexcept { return x_; }
	double Y() const noexcept { return y_; }

private:
	void validate(double v, std::string_view name) {
		if (std::isinf(v)) [[unlikely]] {
			throwInfError(name);
		}
		if (std::isnan(v)) [[unlikely]] {
			throwNanError(name);
		}
	}
	[[noreturn]] void throwInfError(std::string_view name);
	[[noreturn]] void throwNanError(std::string_view name);

	double x_, y_;
};

template <typename T>
T& operator<<(T& os, Point p) {
	return os << '{' << p.X() << ", " << p.Y() << '}';
}

inline bool approxEqual(double lhs, double rhs) noexcept {
	return std::abs(lhs - rhs) <=
		   ((std::abs(lhs) < std::abs(rhs) ? std::abs(rhs) : std::abs(lhs)) * std::numeric_limits<double>::epsilon());
}

inline bool operator==(Point lhs, Point rhs) noexcept { return approxEqual(lhs.X(), rhs.X()) && approxEqual(lhs.Y(), rhs.Y()); }
inline bool operator!=(Point lhs, Point rhs) noexcept { return !(lhs == rhs); }

struct [[nodiscard]] point_strict_equal {
	bool operator()(const Point& lhs, const Point& rhs) const noexcept {
		return std::equal_to<double>()(lhs.X(), rhs.X()) && std::equal_to<double>()(lhs.Y(), rhs.Y());
	}
};
struct [[nodiscard]] point_strict_less {
	bool operator()(const Point& lhs, const Point& rhs) const noexcept { return lhs.X() < rhs.X() || lhs.Y() < rhs.Y(); }
};

inline bool DWithin(Point lhs, Point rhs, double distance) noexcept {
	return (lhs.X() - rhs.X()) * (lhs.X() - rhs.X()) + (lhs.Y() - rhs.Y()) * (lhs.Y() - rhs.Y()) <= distance * distance;
}

class [[nodiscard]] Rectangle {
public:
	Rectangle() noexcept : left_{}, right_{}, bottom_{}, top_{} {}
	Rectangle(Point a, Point b) noexcept
		: left_{std::min(a.X(), b.X())}, right_{std::max(a.X(), b.X())}, bottom_{std::min(a.Y(), b.Y())}, top_{std::max(a.Y(), b.Y())} {}
	Rectangle(double l, double r, double b, double t) noexcept
		: left_{std::min(l, r)}, right_{std::max(l, r)}, bottom_{std::min(b, t)}, top_{std::max(b, t)} {}
	double Left() const noexcept { return left_; }
	double Right() const noexcept { return right_; }
	double Bottom() const noexcept { return bottom_; }
	double Top() const noexcept { return top_; }
	double Area() const noexcept { return (right_ - left_) * (top_ - bottom_); }
	bool Contain(Point p) const noexcept { return left_ <= p.X() && p.X() <= right_ && bottom_ <= p.Y() && p.Y() <= top_; }
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

inline Rectangle boundRect(Point p) noexcept { return {p.X(), p.X(), p.Y(), p.Y()}; }

inline Rectangle boundRect(const Rectangle& r1, const Rectangle& r2) noexcept {
	return {std::min(r1.Left(), r2.Left()), std::max(r1.Right(), r2.Right()), std::min(r1.Bottom(), r2.Bottom()),
			std::max(r1.Top(), r2.Top())};
}

inline Rectangle boundRect(const Rectangle& r, Point p) noexcept {
	return {std::min(r.Left(), p.X()), std::max(r.Right(), p.X()), std::min(r.Bottom(), p.Y()), std::max(r.Top(), p.Y())};
}

class [[nodiscard]] Circle {
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
	if (c.Center().X() < r.Left()) {
		const auto diffX = r.Left() - c.Center().X();
		if (c.Center().Y() < r.Bottom()) {
			const auto diffY = r.Bottom() - c.Center().Y();
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else if (c.Center().Y() > r.Top()) {
			const auto diffY = c.Center().Y() - r.Top();
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else {
			return diffX <= c.Radius();
		}
	} else if (c.Center().X() > r.Right()) {
		const auto diffX = c.Center().X() - r.Right();
		if (c.Center().Y() < r.Bottom()) {
			const auto diffY = r.Bottom() - c.Center().Y();
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else if (c.Center().Y() > r.Top()) {
			const auto diffY = c.Center().Y() - r.Top();
			return diffX * diffX + diffY * diffY <= c.Radius() * c.Radius();
		} else {
			return diffX <= c.Radius();
		}
	} else {
		return c.Center().Y() + c.Radius() >= r.Bottom() && c.Center().Y() - c.Radius() <= r.Top();
	}
}

}  // namespace reindexer

namespace std {

template <>
struct [[nodiscard]] hash<reindexer::Point> {
	size_t operator()(reindexer::Point p) const noexcept { return (hash<double>()(p.X()) << 1) ^ hash<double>()(p.Y()); }
};

}  // namespace std
