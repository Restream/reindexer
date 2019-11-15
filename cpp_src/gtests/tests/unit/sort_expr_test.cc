#include "core/nsselecter/sortexpression.h"
#include "gtest/gtest.h"

namespace {

using reindexer::SortExpression;

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
static void append(SortExpression& se, char op, const char* field) { se.Append({operation(op), false}, {field}); }
static void append(SortExpression& se, char op, char neg, const char* field) {
	assert(neg == '-');
	(void)neg;
	se.Append({operation(op), true}, {field});
}
static void append(SortExpression& se, char op, double value) { se.Append({operation(op), false}, {value}); }
static void append(SortExpression& se, char op, RankFunction) { se.Append({operation(op), false}, {}); }
static void append(SortExpression& se, char op, char neg, RankFunction) {
	assert(neg == '-');
	(void)neg;
	se.Append({operation(op), true}, {});
}

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
static void append(SortExpression& se, char op, OpenBracket, Args... args) {
	se.OpenBracket({operation(op), false});
	append(se, '+', args...);
}

template <typename... Args>
static void append(SortExpression& se, char op, char neg, OpenBracket, Args... args) {
	assert(neg == '-');
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
	struct {
		const char* expression;
		SortExpression expected;
		bool fail;
	} testCases[]{
		{"-1.2E-3", {}, true},
		{"rank(", {}, true},
		{"id", makeExpr("id"), false},
		{"-id", makeExpr('-', "id"), false},
		{"rank()", makeExpr(Rank), false},
		{"-RANK()", makeExpr('-', Rank), false},
		{"-1.2E-3 + id - obj.value + value", makeExpr(-1.2e-3, '+', "id", '-', "obj.value", '+', "value"), false},
		{"-1.2E-3 + -id - - obj.value + -Rank()", makeExpr(-1.2e-3, '-', "id", '+', "obj.value", '-', Rank), false},
		{"id * (value - 25) / obj.value", makeExpr("id", '*', Open, "value", '-', 25.0, Close, '/', "obj.value"), false},
		{"-id * -(-value - -25) / -obj.value", makeExpr('-', "id", '*', '-', Open, '-', "value", '+', 25.0, Close, '/', '-', "obj.value"),
		 false},
		{"id * value - 1.2", makeExpr("id", '*', "value", '-', 1.2), false},
		{"id + value / 1.2", makeExpr("id", '+', Open, "value", '/', 1.2, Close), false},
		{"id + (value + rank()) / 1.2", makeExpr("id", '+', Open, Open, "value", '+', Rank, Close, '/', 1.2, Close), false},
		{"-id + -(-rank() + -value) / -1.2", makeExpr('-', "id", '-', Open, Open, '-', Rank, '-', "value", Close, '/', -1.2, Close), false},
		{"id + value / 1.2 + 5", makeExpr("id", '+', Open, "value", '/', 1.2, Close, '+', 5.0), false},
		{"-id + -value / -1.2 + -Rank()", makeExpr('-', "id", '-', Open, "value", '/', -1.2, Close, '-', Rank), false}};
	for (const auto& tC : testCases) {
		if (tC.fail) {
			EXPECT_ANY_THROW(SortExpression::Parse(tC.expression)) << tC.expression;
		} else {
			EXPECT_EQ(SortExpression::Parse(tC.expression), tC.expected) << tC.expression;
		}
	}
}
