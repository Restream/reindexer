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

struct RankFunction {
} Rank;
struct Joined {
	size_t nsIdx;
	const char* column;
};
struct DistanceIndexPoint {
	const char* column;
	Point point;
};
struct DistanceBetweenIndexes {
	const char* column1;
	const char* column2;
};
struct DistanceBetweenJoinedIndexes {
	size_t nsIdx1;
	const char* column1;
	size_t nsIdx2;
	const char* column2;
};
struct DistanceBetweenJoinedIndexesSameNs {
	size_t nsIdx;
	const char* column1;
	const char* column2;
};
struct DistanceBetweenIndexAndJoinedIndex {
	const char* column;
	size_t nsIdx;
	const char* jColumn;
};
struct DistanceJoinedIndexPoint {
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

static void append(SortExpression& se, char op, const char* field) { se.Append({operation(op), false}, SortExprFuncs::Index{field}); }
static void append(SortExpression& se, char op, char neg, const char* field) {
	assertrx(neg == '-');
	(void)neg;
	se.Append({operation(op), true}, SortExprFuncs::Index{field});
}
static void append(SortExpression& se, char op, const Joined& join) {
	se.Append({operation(op), false}, SortExprFuncs::JoinedIndex{join.nsIdx, join.column});
}
static void append(SortExpression& se, char op, char neg, const Joined& join) {
	assertrx(neg == '-');
	(void)neg;
	se.Append({operation(op), true}, SortExprFuncs::JoinedIndex{join.nsIdx, join.column});
}
static void append(SortExpression& se, char op, double value) { se.Append({operation(op), false}, SortExprFuncs::Value{value}); }
static void append(SortExpression& se, char op, RankFunction) { se.Append({operation(op), false}, SortExprFuncs::Rank{}); }
static void append(SortExpression& se, char op, char neg, RankFunction) {
	assertrx(neg == '-');
	(void)neg;
	se.Append({operation(op), true}, SortExprFuncs::Rank{});
}
static void append(SortExpression& se, char op, DistanceBetweenIndexes d) {
	se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenIndexes{d.column1, d.column2});
}
static void append(SortExpression& se, char op, DistanceBetweenJoinedIndexes d) {
	se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenJoinedIndexes{d.nsIdx1, d.column1, d.nsIdx2, d.column2});
}
static void append(SortExpression& se, char op, DistanceBetweenJoinedIndexesSameNs d) {
	se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenJoinedIndexesSameNs{d.nsIdx, d.column1, d.column2});
}
static void append(SortExpression& se, char op, DistanceIndexPoint d) {
	se.Append({operation(op), false}, SortExprFuncs::DistanceFromPoint{d.column, d.point});
}
static void append(SortExpression& se, char op, DistanceJoinedIndexPoint d) {
	se.Append({operation(op), false}, SortExprFuncs::DistanceJoinedIndexFromPoint{d.nsIdx, d.column, d.point});
}
static void append(SortExpression& se, char op, DistanceBetweenIndexAndJoinedIndex d) {
	se.Append({operation(op), false}, SortExprFuncs::DistanceBetweenIndexAndJoinedIndex{d.column, d.nsIdx, d.jColumn});
}
static void append(SortExpression& se, char op, char neg, DistanceBetweenJoinedIndexes d) {
	assertrx(neg == '-');
	(void)neg;
	se.Append({operation(op), true}, SortExprFuncs::DistanceBetweenJoinedIndexes{d.nsIdx1, d.column1, d.nsIdx2, d.column2});
}

struct OpenAbs {
} Abs;
struct OpenBracket {
} Open;
struct CloseBracket {
} Close;

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
static void append(SortExpression& se, char op, OpenBracket, Args... args) {
	se.OpenBracket({operation(op), false});
	append(se, '+', args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, char neg, OpenBracket, Args... args) {
	assertrx(neg == '-');
	(void)neg;
	se.OpenBracket({operation(op), true});
	append(se, '+', args...);
}

template <typename T, typename... Args>
static void append(SortExpression&, char op, char neg, T, Args...);

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
	enum Result { SUCCESS, FAIL };
	struct Case {
		Case(const char* e, std::vector<JoinedNsNameMock> js, SortExpression se)
			: expression{e}, joinedSelectors{std::move(js)}, expected{std::move(se)}, result{SUCCESS} {}
		Case(const char* e, std::vector<JoinedNsNameMock> js, Result r)
			: expression{e}, joinedSelectors{std::move(js)}, expected{}, result{r} {}
		const char* expression;
		std::vector<JoinedNsNameMock> joinedSelectors;
		SortExpression expected;
		Result result;
	} testCases[]{
		{"-1.2E-3", {}, FAIL},
		{"ns.", {"ns"}, FAIL},
		{"rank(", {}, FAIL},
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
		{R"(123 + "123" + 123)", {}, makeExpr(123.0, '+', "123", '+', 123.0)},
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
		{R"(123 + "ns.123" + 123)", {"ns"}, makeExpr(123.0, '+', Joined{0, "123"}, '+', 123.0)},
		{R"("ns.123" * 123 - "ns.567")", {"ns"}, makeExpr(Joined{0, "123"}, '*', 123.0, '-', Joined{0, "567"})},
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
		{"-1.2E-3 + id - obj.value + value", {}, makeExpr(-1.2e-3, '+', "id", '-', "obj.value", '+', "value")},
		{"-1.2E-3 + -id - - ns.obj.value + -Rank()", {"ns"}, makeExpr(-1.2e-3, '-', "id", '+', Joined{0, "obj.value"}, '-', Rank)},
		{"-1.2E-3+-id--obj.value +-Rank()", {}, makeExpr(-1.2e-3, '-', "id", '+', "obj.value", '-', Rank)},
		{"id * (value - 25) / obj.value", {}, makeExpr("id", '*', Open, "value", '-', 25.0, Close, '/', "obj.value")},
		{"-id * -(-value - - + - -25) / -obj.value",
		 {},
		 makeExpr('-', "id", '*', '-', Open, '-', "value", '+', 25.0, Close, '/', '-', "obj.value")},
		{"id * value - 1.2", {}, makeExpr("id", '*', "value", '-', 1.2)},
		{"id + value / 1.2", {}, makeExpr("id", '+', Open, "value", '/', 1.2, Close)},
		{"id + (value + rank()) / 1.2", {}, makeExpr("id", '+', Open, Open, "value", '+', Rank, Close, '/', 1.2, Close)},
		{"-id + -(-rank() + -value) / -1.2", {}, makeExpr('-', "id", '-', Open, Open, '-', Rank, '-', "value", Close, '/', -1.2, Close)},
		{"id + value / 1.2 + 5", {}, makeExpr("id", '+', Open, "value", '/', 1.2, Close, '+', 5.0)},
		{"-id + -value / -1.2 + -Rank()", {}, makeExpr('-', "id", '-', Open, "value", '/', -1.2, Close, '-', Rank)},
		{"-id + (-value + -1.2) * -Rank()", {}, makeExpr('-', "id", '+', Open, Open, '-', "value", '-', 1.2, Close, '*', '-', Rank, Close)},
		{"-id + Abs(-value + -1.2) * -Rank()",
		 {},
		 makeExpr('-', "id", '+', Open, Abs, '-', "value", '-', 1.2, Close, '*', '-', Rank, Close)},
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
		 makeExpr(-1.2e-3, '-', "id", '+', Joined{0, "obj.value"}, '-', Open, Distance("point1", "point2"), '*', '-',
				  Distance(1, "point1", 2, "point2"), Close)},
	};
	for (const auto& tC : testCases) {
		if (tC.result == FAIL) {
			EXPECT_THROW(SortExpression::Parse(tC.expression, tC.joinedSelectors), reindexer::Error) << tC.expression;
		} else {
			try {
				EXPECT_EQ(SortExpression::Parse(tC.expression, tC.joinedSelectors), tC.expected) << tC.expression;
			} catch (const reindexer::Error& err) {
				ADD_FAILURE() << err.what();
			}
		}
	}
}
