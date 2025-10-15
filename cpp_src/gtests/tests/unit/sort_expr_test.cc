#include "core/nsselecter/joinedselectormock.h"
#include "core/sorting/sortexpression.h"
#include "gtest/gtest.h"

namespace {

using reindexer::Point;
using reindexer::SortExpression;
namespace SortExprFuncs = reindexer::SortExprFuncs;

ArithmeticOpType operation(char ch) {
	switch (ch) {
		case '+':
			return OpPlus;
		case '-':
			return OpMinus;
		case '*':
			return OpMult;
		case '/':
			return OpDiv;
		default:
			abort();
	}
}

struct [[nodiscard]] RankFunction {
} Rank;
struct [[nodiscard]] RankNamed {
	RankNamed(const char* idx, double defaultVal = 0.0) noexcept : index{idx}, defaultValue{defaultVal} {}
	const char* index;
	double defaultValue;
};
struct [[nodiscard]] SortHashFunction {
	std::optional<uint32_t> seed;
};
struct [[nodiscard]] Joined {
	size_t nsIdx;
	const char* column;
};
struct [[nodiscard]] DistanceIndexPoint {
	const char* column;
	Point point;
};
struct [[nodiscard]] DistanceBetweenIndexes {
	const char* column1;
	const char* column2;
};
struct [[nodiscard]] DistanceBetweenJoinedIndexes {
	size_t nsIdx1;
	const char* column1;
	size_t nsIdx2;
	const char* column2;
};
struct [[nodiscard]] DistanceBetweenJoinedIndexesSameNs {
	size_t nsIdx;
	const char* column1;
	const char* column2;
};
struct [[nodiscard]] DistanceBetweenIndexAndJoinedIndex {
	const char* column;
	size_t nsIdx;
	const char* jColumn;
};
struct [[nodiscard]] DistanceJoinedIndexPoint {
	size_t nsIdx;
	const char* column;
	Point point;
};
DistanceBetweenIndexes Distance(const char* c1, const char* c2) noexcept { return {c1, c2}; }
DistanceIndexPoint Distance(const char* c, Point p) noexcept { return {c, p}; }
DistanceJoinedIndexPoint Distance(size_t ns, const char* c, Point p) noexcept { return {ns, c, p}; }
DistanceBetweenJoinedIndexes Distance(size_t ns1, const char* c1, size_t ns2, const char* c2) noexcept { return {ns1, c1, ns2, c2}; }
DistanceBetweenJoinedIndexesSameNs Distance(size_t ns, const char* c1, const char* c2) noexcept { return {ns, c1, c2}; }
DistanceBetweenIndexAndJoinedIndex Distance(const char* c, size_t ns, const char* jc) noexcept { return {c, ns, jc}; }

static void append(SortExpression& se, char op, const char* field) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::Index{field});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, char neg, const char* field) {
	assertrx(neg == '-');
	(void)neg;
	[[maybe_unused]] auto inserted = se.Append({operation(op), true}, SortExprFuncs::Index{field});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, const Joined& join) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::JoinedIndex{join.nsIdx, join.column});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, char neg, const Joined& join) {
	assertrx(neg == '-');
	(void)neg;
	[[maybe_unused]] auto inserted = se.Append({operation(op), true}, SortExprFuncs::JoinedIndex{join.nsIdx, join.column});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, reindexer::concepts::OneOf<int, double> auto value) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::Value(value));
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, RankFunction) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::Rank{});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, RankNamed r) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::RankNamed{r.index, r.defaultValue});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, const SortHashFunction& h) {
	[[maybe_unused]] auto inserted =
		se.Append({operation(op), false}, h.seed.has_value() ? SortExprFuncs::SortHash{h.seed.value()} : SortExprFuncs::SortHash{});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, char neg, RankFunction) {
	assertrx(neg == '-');
	(void)neg;
	[[maybe_unused]] auto inserted = se.Append({operation(op), true}, SortExprFuncs::Rank{});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, char neg, RankNamed r) {
	assertrx(neg == '-');
	(void)neg;
	[[maybe_unused]] auto inserted = se.Append({operation(op), true}, SortExprFuncs::RankNamed{r.index, r.defaultValue});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, DistanceBetweenIndexes d) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenIndexes{d.column1, d.column2});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, DistanceBetweenJoinedIndexes d) {
	[[maybe_unused]] auto inserted =
		se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenJoinedIndexes{d.nsIdx1, d.column1, d.nsIdx2, d.column2});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, DistanceBetweenJoinedIndexesSameNs d) {
	[[maybe_unused]] auto inserted =
		se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenJoinedIndexesSameNs{d.nsIdx, d.column1, d.column2});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, DistanceIndexPoint d) {
	[[maybe_unused]] auto inserted = se.Append({operation(op), false}, SortExprFuncs::DistanceFromPoint{d.column, d.point});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, DistanceJoinedIndexPoint d) {
	[[maybe_unused]] auto inserted =
		se.Append({operation(op), false}, SortExprFuncs::DistanceJoinedIndexFromPoint{d.nsIdx, d.column, d.point});
	assertrx(inserted == 1);
}
static void append(SortExpression& se, char op, DistanceBetweenIndexAndJoinedIndex d) {
	[[maybe_unused]] auto inserted =
		se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenIndexAndJoinedIndex{d.column, d.nsIdx, d.jColumn});
	assertrx(inserted == 1);
}

struct [[nodiscard]] OpenAbs {
} Abs;
struct [[nodiscard]] OpenBracket {
} Open;
struct [[nodiscard]] CloseBracket {
} Close;

template <typename T, typename... Args>
static void append(SortExpression&, char op, T, Args...);

template <typename T, typename... Args>
static void append(SortExpression&, char op, char neg, T, Args...);

static void append(SortExpression& se, CloseBracket) { se.CloseBracket(); }

template <typename... Args>
static void append(SortExpression& se, CloseBracket, Args... args) {
	se.CloseBracket();
	append(se, args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, OpenAbs, Args... args) {
	se.OpenBracket({operation(op), false}, true);
	append(se, '+', args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, char neg, OpenAbs, Args... args) {
	assertrx(neg == '-');
	(void)neg;
	se.OpenBracket({operation(op), true}, true);
	append(se, '+', args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, OpenBracket, Args... args) {
	se.OpenBracket({operation(op), false});
	append(se, '+', args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, OpenBracket, char neg, Args... args) {
	assertrx(neg == '-');
	(void)neg;
	se.OpenBracket({operation(op), false});
	append(se, '+', '-', args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, char neg, OpenBracket, Args... args) {
	assertrx(neg == '-');
	(void)neg;
	se.OpenBracket({operation(op), true});
	append(se, '+', args...);
}

template <typename T, typename... Args>
static void append(SortExpression& se, char op, T a, Args... args) {
	append(se, op, a);
	append(se, args...);
}

template <typename T, typename... Args>
static void append(SortExpression& se, char op, char neg, T a, Args... args) {
	append(se, op, neg, a);
	append(se, args...);
}

template <typename... Args>
static SortExpression makeExpr(Args... args) {
	SortExpression result;
	append(result, '+', args...);
	return result;
}

}  // namespace

TEST(StringFunctions, SortExpressionParse) {
	enum [[nodiscard]] Result { SUCCESS, FAIL };
	struct [[nodiscard]] Case {
		Case(const char* e, std::vector<JoinedNsNameMock> js, SortExpression se)
			: expression{e}, joinedSelectors{std::move(js)}, expected{std::move(se)}, result{SUCCESS} {}
		Case(const char* e, std::vector<JoinedNsNameMock> js, Result r)
			: expression{e}, joinedSelectors{std::move(js)}, expected{}, result{r} {}
		const char* expression;
		std::vector<JoinedNsNameMock> joinedSelectors;
		SortExpression expected;
		Result result;
	} testCases[]{
		{"\"", {}, FAIL},
		{"\"abc", {}, FAIL},
		{"abc\"", {}, FAIL},
		{"abc\"abc", {}, FAIL},
		{"\"\"", {}, FAIL},
		{"\"\"abc", {}, FAIL},
		{"abc\"\"", {}, FAIL},
		{"\"abc\"\"", {}, FAIL},
		{"\"a\"bc", {}, FAIL},
		{"\"a\"bc\"", {}, FAIL},
		{"-1.2E-3", {}, FAIL},
		{"ns.", {"ns"}, FAIL},
		{"rank(", {}, FAIL},
		{"rank(ft, 123", {}, FAIL},
		{"rank(ft, id)", {}, FAIL},
		{"rank(ft 123)", {}, FAIL},
		{"abs()", {}, FAIL},
		{"id", {}, makeExpr("id")},
		{"id+value", {}, makeExpr("id+value")},
		{R"("123")", {}, makeExpr("123")},
		{R"("123abc")", {}, makeExpr("123abc")},
		{R"("123abc123")", {}, makeExpr("123abc123")},
		{R"("123+123")", {}, makeExpr("123+123")},	// No space - field as a name in quotes.
		{R"("123+123abc")", {}, makeExpr("123+123abc")},
		{R"("123+123abc123")", {}, makeExpr("123+123abc123")},
		{R"("123abc+123abc")", {}, makeExpr("123abc+123abc")},
		{R"("123abc123+123abc123")", {}, makeExpr("123abc123+123abc123")},
		{R"("123" + 123)", {}, makeExpr("123", '+', 123.0)},
		{R"(123 + "123")", {}, makeExpr(123.0, '+', "123")},
		{R"(123 + "123" + 123)", {}, makeExpr(246.0, '+', "123")},
		{R"("123" + 123 + "123")", {}, makeExpr("123", '+', 123.0, '+', "123")},
		{R"("123" + "123")", {}, makeExpr("123", '+', "123")},
		{R"("123" + "123abc")", {}, makeExpr("123", '+', "123abc")},
		{"id + value", {}, makeExpr("id", '+', "value")},
		{"id-value", {}, makeExpr("id", '-', "value")},
		{R"("123" - "123")", {}, makeExpr("123", '-', "123")},
		{"ns.id", {"ns"}, makeExpr(Joined{0, "id"})},
		{R"(ns.123)", {"ns"}, makeExpr(Joined{0, "123"})},
		{R"("ns.123")", {"ns"}, makeExpr(Joined{0, "123"})},
		{R"("ns.123" + 123)", {"ns"}, makeExpr(Joined{0, "123"}, '+', 123.0)},
		{R"("ns.123" + "123")", {"ns"}, makeExpr(Joined{0, "123"}, '+', "123")},
		{R"(123 + "ns.123")", {"ns"}, makeExpr(123.0, '+', Joined{0, "123"})},
		{R"("123" + "ns.123")", {"ns"}, makeExpr("123", '+', Joined{0, "123"})},
		{R"(123 + "ns.123" + 123)", {"ns"}, makeExpr(246.0, '+', Joined{0, "123"})},
		{R"("ns.123" * 123 - "ns.567")", {"ns"}, makeExpr(123.0, '*', Joined{0, "123"}, '-', Joined{0, "567"})},
		{R"(ns.123abc)", {"ns"}, makeExpr(Joined{0, "123abc"})},
		{R"(ns.123abc123)", {"ns"}, makeExpr(Joined{0, "123abc123"})},
		{"ns2.id_1", {"ns1"}, makeExpr("ns2.id_1")},
		{R"(ns2.123)", {"ns1"}, makeExpr("ns2.123")},
		{R"("ns2.123")", {"ns1"}, makeExpr("ns2.123")},
		{R"(ns2.123abc)", {"ns1"}, makeExpr("ns2.123abc")},
		{R"(ns2.123abc123)", {"ns1"}, makeExpr("ns2.123abc123")},
		{"-id", {}, makeExpr('-', "id")},
		{R"(-"123")", {}, makeExpr('-', "123")},
		{R"(-"123abc")", {}, makeExpr('-', "123abc")},
		{R"(-"123abc123")", {}, makeExpr('-', "123abc123")},
		{"-ns.group.id", {"ns2", "ns"}, makeExpr('-', Joined{1, "group.id"})},
		{"rank()", {}, makeExpr(Rank)},
		{"-RANK()", {}, makeExpr('-', Rank)},
		{"rank(ft)", {}, makeExpr(RankNamed{"ft"})},
		{"rank(ft, 0.0)", {}, makeExpr(RankNamed{"ft", 0.0})},
		{"- Rank(ft, 1.0e+3)", {}, makeExpr('-', RankNamed{"ft", 1.0e+3})},
		{"-1.2E-3 + id - obj.value + value", {}, makeExpr(-1.2e-3, '+', "id", '-', "obj.value", '+', "value")},
		{"-1.2E-3 + -id - - ns.obj.value + -Rank()", {"ns"}, makeExpr(-1.2e-3, '-', "id", '+', Joined{0, "obj.value"}, '-', Rank)},
		{"-1.2E-3+-id--obj.value +-Rank()", {}, makeExpr(-1.2e-3, '-', "id", '+', "obj.value", '-', Rank)},
		{"id * (value - 25) / obj.value", {}, makeExpr("id", '*', Open, "value", '+', -25.0, Close, '/', "obj.value")},
		{"-id * -(-value - - + - -25) / -obj.value", {}, makeExpr('-', "id", '*', Open, '-', "value", '+', 25.0, Close, '/', "obj.value")},
		{"hash(100)", {}, makeExpr(SortHashFunction{100})},
		{"id * value - 1.2", {}, makeExpr("id", '*', "value", '+', -1.2)},
		{"id + value / 1.2", {}, makeExpr("id", '+', 1 / 1.2, '*', "value")},
		{"id + (value + rank()) / 1.2", {}, makeExpr("id", '+', 1 / 1.2, '*', Open, "value", '+', Rank, Close)},
		{"-id + -(-rank() + -value) / -1.2", {}, makeExpr('-', "id", '+', 1 / 1.2, '*', Open, '-', Rank, '-', "value", Close)},
		{"id + value / 1.2 + 5", {}, makeExpr("id", '+', 1 / 1.2, '*', "value", '+', 5.0)},
		{"-id + -value / -1.2 + -Rank()", {}, makeExpr('-', "id", '+', 1 / 1.2, '*', "value", '-', Rank)},
		{"-id + (-value + -1.2) * -Rank()", {}, makeExpr('-', "id", '-', Open, '-', "value", '+', -1.2, Close, '*', Rank)},
		{"-id + Abs(-value + -1.2) * -Rank()", {}, makeExpr('-', "id", '-', Abs, '-', "value", '+', -1.2, Close, '*', Rank)},
		{"ST_Distance(point, point)", {}, FAIL},
		{"ST_Distance(ns.point, ns.point)", {"ns"}, FAIL},
		{"ST_Distance(point1 point2)", {}, FAIL},
		{"ST_Distance(point1, ", {}, FAIL},
		{"ST_Distance(point1, )", {}, FAIL},
		{"ST_Distance(point1 )", {}, FAIL},
		{"ST_Distance(point1, point2", {}, FAIL},
		{"ST_Distance(point, ST_GeomFromText('point(1.25)'))", {}, FAIL},
		{"ST_Distance(point, ST_GeomFromText(\"point(1.25 -3.5)'))", {}, FAIL},
		{"ST_Distance(point, ST_GeomFromText('point(1.25 -3.5)))", {}, FAIL},
		{"ST_Distance(point, ST_GeomFromText('point(1.25 -3.5)')", {}, FAIL},
		{"ST_Distance(point, ST_GeomFromText('point(1.25, -3.5)'))", {}, FAIL},
		{"ST_Distance(ST_GeomFromText('point(0.5 5.5)'), ST_GeomFromText('point(1.25 -3.5)'))", {}, FAIL},
		{"ST_Distance(point1, point2)", {}, makeExpr(Distance("point1", "point2"))},
		{"ST_Distance(point1, ns.point2)", {"ns"}, makeExpr(Distance("point1", 0, "point2"))},
		{"ST_Distance(ns.point1, point2)", {"ns"}, makeExpr(Distance("point2", 0, "point1"))},
		{"ST_Distance(ns.point1, ns.point2)", {"ns"}, makeExpr(Distance(0, "point1", "point2"))},
		{"ST_Distance(ns1.point1, ns2.point2)", {"ns1", "ns2"}, makeExpr(Distance(0, "point1", 1, "point2"))},
		{"ST_Distance(point, ST_GeomFromText('point(1.25 -3.5)'))", {}, makeExpr(Distance("point", reindexer::Point{1.25, -3.5}))},
		{"ST_Distance(ST_GeomFromText(\"point(1.25 -3.5)\"), ns.point)",
		 {"ns"},
		 makeExpr(Distance(0, "point", reindexer::Point{1.25, -3.5}))},
		{"  ST_Distance ( ST_GeomFromText ( ' point ( 1.25 -3.5 ) ' ) ,  ns.point )  ",
		 {"ns"},
		 makeExpr(Distance(0, "point", reindexer::Point{1.25, -3.5}))},
		{"-1.2E-3 + -id - - ns.obj.value + -ST_Distance(point1, point2) * -ST_Distance(ns1.point1, ns2.point2)",
		 {"ns", "ns1", "ns2"},
		 makeExpr(-1.2e-3, '-', "id", '+', Joined{0, "obj.value"}, '+', Distance("point1", "point2"), '*',
				  Distance(1, "point1", 2, "point2"))},

		{"a + 4 - 9 * 6 / 2 + b - c * f / g * 3 / 4 + ((( d - f * 3 / 4 - 3 / 4 * b - -2 / abs(-4 * 3 / 6) + -3 / 4 * -6) * 4 / 2 + 3 - 3 "
		 "* f - 5 -(f - 4 / 8) - 5 * ( 3 / 1 + 4 - 5) - 3 / 4) * (3 / 4 + 4 - 7 / 2) - a + 4 - x *2 - (3 / 4))",
		 {},
		 makeExpr("a", '+', -79.0 / 4, '+', "b", '+', -3.0 / 4, '*', "c", '*', "f", '/', "g", '+', 5.0 / 4, '*', Open, 2, '*', Open, "d",
				  '+', -3.0 / 4, '*', "f", '+', -3.0 / 4, '*', "b", '+', 11.0 / 2, Close, '+', -49.0 / 4, '+', -3, '*', "f", '-', "f",
				  Close, '-', "a", '+', -2, '*', "x")},

		{"g - 4 * (a - 4 + 5 / (3 / (1 + 2) - - 3)) - (4 - g + 1 * (3 + 8 - 4 *( 2 / 4 + 6)) + b - c * 5 / (a + b - 4) - 2 * (25 * 3 / 5 / "
		 "4 * (2 - 1) - 5 + 4)) + - (3 - 2 / 4 + 0.5) * (-(-(1 - 4 + 2 * abs( 2 -4 ))))",
		 {},
		 makeExpr("g", '+', -4, '*', Open, "a", '+', -11.0 / 4, Close, '+', 27.0 / 2, '+', "g", '-', "b", '+', 5, '*', "c", '/', Open, "a",
				  '+', "b", '+', -4, Close)},

		{"(5 - 3 / 2 *( -a - 4 / 6 + 4 * 3 -(f / 3 - 10 * 4 + 5)) * 8 / 4 + (2 - 4 -(5  -3) / (3 / 3 * 2)  - 4 /1 /8 ))",
		 {},
		 makeExpr(1.5, '+', -3, '*', Open, '-', "a", '+', 139.0 / 3, '+', -1.0 / 3, '*', "f", Close)},

		{"2 * ( x + 3)  + 4 * x * 2 * ( x + 3) + 4  * x",
		 {},
		 makeExpr(2, '*', Open, "x", '+', 3, Close, '+', 8, '*', "x", '*', Open, "x", '+', 3, Close, '+', 4, '*', "x")},

		{"( x + 2)* ( x + 3) - x* 2* (x +2)*(x +3)-x*2",
		 {},
		 makeExpr(Open, "x", '+', 2, Close, '*', Open, "x", '+', 3, Close, '+', -2, '*', "x", '*', Open, "x", '+', 2, Close, '*', Open, "x",
				  '+', 3, Close, '+', -2, '*', "x")},

		{" 3* a* ( 2 *b + 1) - 2* a* b + 5* 3*a*(2*b +1)-2*a*b +5 ",
		 {},
		 makeExpr(3, '*', "a", '*', Open, 2, '*', "b", '+', 1, Close, '+', -2, '*', "a", '*', "b", '+', 15, '*', "a", '*', Open, 2, '*',
				  "b", '+', 1, Close, '+', -2, '*', "a", '*', "b", '+', 5)},

		{"((((2* ( x + 4))/ 2 )))+ 3 *((x))*( 22*(x + 4))+3*x",
		 {},
		 makeExpr(1, '*', Open, "x", '+', 4, Close, '+', 66, '*', "x", '*', Open, "x", '+', 4, Close, '+', 3, '*', "x")},

		{"( y + 5)* ( y - 2) + 3* y - 10* (y +5)*(y-2)+3*y-10",
		 {},
		 makeExpr(Open, "y", '+', 5, Close, '*', Open, "y", '+', -2, Close, '+', 3, '*', "y", '+', -10, '*', Open, "y", '+', 5, Close, '*',
				  Open, "y", '+', -2, Close, '+', 3, '*', "y", '+', -10)},

		{"(6* x* 2 + 12* x)/( 6 *x) + x* 6*x*6*x*2+12*x +x",
		 {},
		 makeExpr(1 / 6.0, '*', Open, 12, '*', "x", '+', 12, '*', "x", Close, '/', "x", '+', 72, '*', "x", '*', "x", '*', "x", '+', 12, '*',
				  "x", '+', "x")},

		{"3* ( x + 4) + 2* ( 5 - x)* 3*(x +4)+2*(5-x)",
		 {},
		 makeExpr(3, '*', Open, "x", '+', 4, Close, '+', 6, '*', Open, 5, '-', "x", Close, '*', Open, "x", '+', 4, Close, '+', 2, '*', Open,
				  5, '-', "x", Close)},

		{"( a + 2)* ( a - 1) - a* (a +2)*(a-1)-a",
		 {},
		 makeExpr(Open, "a", '+', 2, Close, '*', Open, "a", '+', -1, Close, '-', "a", '*', Open, "a", '+', 2, Close, '*', Open, "a", '+',
				  -1, Close, '-', "a")},

		{"6* ( x + 2) /3 + 4* x* 36*(x +2)+4*x",
		 {},
		 makeExpr(2, '*', Open, "x", '+', 2, Close, '+', 144, '*', "x", '*', Open, "x", '+', 2, Close, '+', 4, '*', "x")},

		{"(8* y + 16) /4 - y* 48*y +16-y",
		 {},
		 makeExpr(0.25, '*', Open, 8, '*', "y", '+', 16, Close, '+', -48, '*', "y", '*', "y", '+', 16, '-', "y")},

		{"5 *( 2 *x + 3 *( 4 - y)) + (10* x - 2* ( 3 *y + 1))/ 2 - ( x + y) *5*(2*x +3*(4-y))+210*x-2*(3*y +1)-(x +y)",
		 {},
		 makeExpr(5, '*', Open, 2, '*', "x", '+', 3, '*', Open, 4, '-', "y", Close, Close, '+', 0.5, '*', Open, 10, '*', "x", '+', -2, '*',
				  Open, 3, '*', "y", '+', 1, Close, Close, '+', -5, '*', Open, "x", '+', "y", Close, '*', Open, 2, '*', "x", '+', 3, '*',
				  Open, 4, '-', "y", Close, Close, '+', 210, '*', "x", '+', -2, '*', Open, 3, '*', "y", '+', 1, Close, '-', "x", '-', "y")},

		{"(4* ( 3* a + 2* b) - 2* ( 5* a - b))/ 2 + 3 *( a + 2* b) - (6* a + 12* b)/ 3 *24*(3*a +2*b)-2*(5*a-b)+3*(a +2*b)-36*a +12*b",
		 {},
		 makeExpr(0.5, '*', Open, 4, '*', Open, 3, '*', "a", '+', 2, '*', "b", Close, '+', -2, '*', Open, 5, '*', "a", '-', "b", Close,
				  Close, '+', 3, '*', Open, "a", '+', 2, '*', "b", Close, '+', -8, '*', Open, 6, '*', "a", '+', 12, '*', "b", Close, '*',
				  Open, 3, '*', "a", '+', 2, '*', "b", Close, '+', -2, '*', Open, 5, '*', "a", '-', "b", Close, '+', 3, '*', Open, "a", '+',
				  2, '*', "b", Close, '+', -36, '*', "a", '+', 12, '*', "b")},

		{"2* ( 3* ( x + 2* y) - 4* ( 2 *x - y)) + 5* ( 3* y - x) + (10* x + 20* y )/5* 2*(3*(x +2*y)-4*(2*x-y))+5*(3*y-x)+510*x +20*y",
		 {},
		 makeExpr(2, '*', Open, 3, '*', Open, "x", '+', 2, '*', "y", Close, '+', -4, '*', Open, 2, '*', "x", '-', "y", Close, Close, '+', 5,
				  '*', Open, 3, '*', "y", '-', "x", Close, '+', 0.4, '*', Open, 10, '*', "x", '+', 20, '*', "y", Close, '*', Open, 3, '*',
				  Open, "x", '+', 2, '*', "y", Close, '+', -4, '*', Open, 2, '*', "x", '-', "y", Close, Close, '+', 5, '*', Open, 3, '*',
				  "y", '-', "x", Close, '+', 510, '*', "x", '+', 20, '*', "y")},

		{"(3* ( 2* m + 4* n) + 6* ( m - n))/ 2 - 2* ( 3* n + m) + (8* m - 4* n)/ 4 *23*(2*m +4*n)+6*(m-n)-2*(3*n +m)+48*m-4*n",
		 {},
		 makeExpr(0.5, '*', Open, 3, '*', Open, 2, '*', "m", '+', 4, '*', "n", Close, '+', 6, '*', Open, "m", '-', "n", Close, Close, '+',
				  -2, '*', Open, 3, '*', "n", '+', "m", Close, '+', 5.75, '*', Open, 8, '*', "m", '+', -4, '*', "n", Close, '*', Open, 2,
				  '*', "m", '+', 4, '*', "n", Close, '+', 6, '*', Open, "m", '-', "n", Close, '+', -2, '*', Open, 3, '*', "n", '+', "m",
				  Close, '+', 48, '*', "m", '+', -4, '*', "n")},

		{"4* ( 2* ( p + 3* q) - 5 *q) + 3* ( 4 *q - p) - (6* p + 18* q)/ 3 + 2* ( 3* p + q)* "
		 "4*(2*(p +3*q)-5*q)+3*(4*q-p)-36*p +18*q +2*(3*p +q)",
		 {},
		 makeExpr(4, '*', Open, 2, '*', Open, "p", '+', 3, '*', "q", Close, '+', -5, '*', "q", Close, '+', 3, '*', Open, 4, '*', "q", '-',
				  "p", Close, '+', -1.0 / 3, '*', Open, 6, '*', "p", '+', 18, '*', "q", Close, '+', 8, '*', Open, 3, '*', "p", '+', "q",
				  Close, '*', Open, 2, '*', Open, "p", '+', 3, '*', "q", Close, '+', -5, '*', "q", Close, '+', 3, '*', Open, 4, '*', "q",
				  '-', "p", Close, '+', -36, '*', "p", '+', 18, '*', "q", '+', 2, '*', Open, 3, '*', "p", '+', "q", Close)},

		{"(5* ( 2* k + 3* l) - 3* ( 4 *k - l))/ 2 + 2* ( 3* l + k) - (8* k + 16 *l)/ 4 + 5* l* "
		 "25*(2*k +3*l)-3*(4*k-l)+2*(3*l +k)-48*k +16*l +5*l",
		 {},
		 makeExpr(0.5, '*', Open, 5, '*', Open, 2, '*', "k", '+', 3, '*', "l", Close, '+', -3, '*', Open, 4, '*', "k", '-', "l", Close,
				  Close, '+', 2, '*', Open, 3, '*', "l", '+', "k", Close, '+', -0.25, '*', Open, 8, '*', "k", '+', 16, '*', "l", Close, '+',
				  125, '*', "l", '*', Open, 2, '*', "k", '+', 3, '*', "l", Close, '+', -3, '*', Open, 4, '*', "k", '-', "l", Close, '+', 2,
				  '*', Open, 3, '*', "l", '+', "k", Close, '+', -48, '*', "k", '+', 16, '*', "l", '+', 5, '*', "l")},

		{"3* ( 2* ( a + b) - 4 *( a - b)) + (6* ( 3* a + 2* b) - 4* ( a + 5* b))/ 2 - 2* ( 3* a + b)* "
		 "3*(2*(a +b)-4*(a-b))+26*(3*a +2*b)-4*(a +5*b)-2*(3*a +b)",
		 {},
		 makeExpr(3, '*', Open, 2, '*', Open, "a", '+', "b", Close, '+', -4, '*', Open, "a", '-', "b", Close, Close, '+', 0.5, '*', Open, 6,
				  '*', Open, 3, '*', "a", '+', 2, '*', "b", Close, '+', -4, '*', Open, "a", '+', 5, '*', "b", Close, Close, '+', -6, '*',
				  Open, 3, '*', "a", '+', "b", Close, '*', Open, 2, '*', Open, "a", '+', "b", Close, '+', -4, '*', Open, "a", '-', "b",
				  Close, Close, '+', 26, '*', Open, 3, '*', "a", '+', 2, '*', "b", Close, '+', -4, '*', Open, "a", '+', 5, '*', "b", Close,
				  '+', -2, '*', Open, 3, '*', "a", '+', "b", Close)},

		{"2 *( 3* ( 4* x - 2 *y) + 2 *( x + y)) - 5 *( 2* x - y) + (12* x - 6* y)/ 3 - ( 4* x + 3* y) "
		 "+2*(3*(4*x-2*y)+2*(x +y))-5*(2*x-y)+312*x-6*y-(4*x +3*y)",
		 {},
		 makeExpr(2, '*', Open, 3, '*', Open, 4, '*', "x", '+', -2, '*', "y", Close, '+', 2, '*', Open, "x", '+', "y", Close, Close, '+',
				  -5, '*', Open, 2, '*', "x", '-', "y", Close, '+', 1.0 / 3, '*', Open, 12, '*', "x", '+', -6, '*', "y", Close, '+', -4,
				  '*', "x", '+', -3, '*', "y", '+', 2, '*', Open, 3, '*', Open, 4, '*', "x", '+', -2, '*', "y", Close, '+', 2, '*', Open,
				  "x", '+', "y", Close, Close, '+', -5, '*', Open, 2, '*', "x", '-', "y", Close, '+', 312, '*', "x", '+', -6, '*', "y", '+',
				  -4, '*', "x", '+', -3, '*', "y")},

		{"(4* ( 3* c + 2* d) + 2 *( 5* c - d) )/2 - 3 *( c + 2* d) + (9 *c + 6* d)/ 3 - ( 2* c - d)* "
		 "24*(3*c +2*d)+2*(5*c-d)-3*(c +2*d)+39*c +6*d-(2*c-d)",
		 {},
		 makeExpr(0.5, '*', Open, 4, '*', Open, 3, '*', "c", '+', 2, '*', "d", Close, '+', 2, '*', Open, 5, '*', "c", '-', "d", Close,
				  Close, '+', -3, '*', Open, "c", '+', 2, '*', "d", Close, '+', 1.0 / 3, '*', Open, 9, '*', "c", '+', 6, '*', "d", Close,
				  '+', -24, '*', Open, 2, '*', "c", '-', "d", Close, '*', Open, 3, '*', "c", '+', 2, '*', "d", Close, '+', 2, '*', Open, 5,
				  '*', "c", '-', "d", Close, '+', -3, '*', Open, "c", '+', 2, '*', "d", Close, '+', 39, '*', "c", '+', 6, '*', "d", '+', -2,
				  '*', "c", '+', "d")},

		{"5 *( 2* ( 3* u + v) - 4* ( u - 2* v)) +( 6 *( 2* u + 3* v) - 2* ( 5* u - v))/ 2 - 3* ( 4* v + u) + (8* u + 24* v)/ 4* "
		 "5*(2*(3*u +v)-4*(u-2*v))+26*(2*u +3*v)-2*(5*u-v)-3*(4*v +u)+48*u +24*v",
		 {},
		 makeExpr(5, '*', Open, 2, '*', Open, 3, '*', "u", '+', "v", Close, '+', -4, '*', Open, "u", '+', -2, '*', "v", Close, Close, '+',
				  0.5, '*', Open, 6, '*', Open, 2, '*', "u", '+', 3, '*', "v", Close, '+', -2, '*', Open, 5, '*', "u", '-', "v", Close,
				  Close, '+', -3, '*', Open, 4, '*', "v", '+', "u", Close, '+', 1.25, '*', Open, 8, '*', "u", '+', 24, '*', "v", Close, '*',
				  Open, 2, '*', Open, 3, '*', "u", '+', "v", Close, '+', -4, '*', Open, "u", '+', -2, '*', "v", Close, Close, '+', 26, '*',
				  Open, 2, '*', "u", '+', 3, '*', "v", Close, '+', -2, '*', Open, 5, '*', "u", '-', "v", Close, '+', -3, '*', Open, 4, '*',
				  "v", '+', "u", Close, '+', 48, '*', "u", '+', 24, '*', "v")}};
	for (const auto& tC : testCases) {
		if (tC.result == FAIL) {
			// NOLINTNEXTLINE (bugprone-unused-return-value)
			EXPECT_THROW(SortExpression::Parse(tC.expression, tC.joinedSelectors), reindexer::Error) << tC.expression;
		} else {
			try {
				const auto parsed = SortExpression::Parse(tC.expression, tC.joinedSelectors);
				EXPECT_EQ(parsed, tC.expected) << "Parsed:\n"
											   << parsed.Dump() << "\nExpected:\n"
											   << tC.expected.Dump() << "\nExpression:\n"
											   << tC.expression;
			} catch (const reindexer::Error& err) {
				ADD_FAILURE() << err.what();
			}
		}
	}
}
