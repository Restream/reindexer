#pragma once

#include <string>
#include <string_view>

namespace reindexer_tests {

// Shared operator-precedence cases for WHERE and UPDATE arithmetic expressions.
// "{v}" is expanded to a literal (WHERE) or field name (UPDATE) that evaluates to 8.
struct [[nodiscard]] ArithmeticPrecedenceCase {
	std::string_view expr;
	double expected;
	bool updateSqlSafe;	 // false: '--' is a SQL comment in UPDATE SET
};

inline std::string ExpandArithmeticPrecedenceExpr(std::string_view expr, std::string_view value) {
	static constexpr std::string_view kPlaceholder = "{v}";
	std::string out;
	out.reserve(expr.size() + value.size());
	for (size_t i = 0; i < expr.size();) {
		if (expr.substr(i, kPlaceholder.size()) == kPlaceholder) {
			out.append(value);
			i += kPlaceholder.size();
		} else {
			out.push_back(expr[i++]);
		}
	}
	return out;
}

inline constexpr ArithmeticPrecedenceCase kArithmeticPrecedenceCases[]{
	{"{v}/4/2", 1, true},			// (8 / 4) / 2
	{"{v}/(4/2)", 4, true},			// 8 / (4 / 2)
	{"{v}-3-2", 3, true},			// (8 - 3) - 2
	{"{v}-(3-2)", 7, true},			// 8 - (3 - 2)
	{"2*3*4", 24, true},			// (2 * 3) * 4
	{"1+2*3", 7, true},				// 1 + (2 * 3)
	{"(1+2)*3", 9, true},			// (1 + 2) * 3
	{"10-2*3", 4, true},			// 10 - (2 * 3)
	{"(10-2)*3", 24, true},			// (10 - 2) * 3
	{"{v}/2+1*3", 7, true},			// (8 / 2) + (1 * 3)
	{"({v}/2+1)*3", 15, true},		// ((8 / 2) + 1) * 3
	{"1+2-3", 0, true},				// (1 + 2) - 3
	{"1-(2-3)", 2, true},			// 1 - (2 - 3)
	{"-{v}/4", -2, true},			// (-8) / 4 and -(8 / 4) are both -2
	{"{v}/-4", -2, true},			// 8 / (-4)
	{"-2*3", -6, true},				// (-2) * 3 and -(2 * 3) are both -6
	{"2*-3", -6, true},				// 2 * (-3)
	{"-(1+2)*3", -9, true},			// (-(1 + 2)) * 3 and -((1 + 2) * 3) are both -9
	{"-((1+2)*3)", -9, true},		// -((1 + 2) * 3)
	{"-2+3", 1, true},				// (-2) + 3, not -(2 + 3)
	{"-{v}+4", -4, true},			// (-8) + 4, not -(8 + 4)
	{"2+-3", -1, true},				// 2 + (-3)
	{"10-(-2)", 12, true},			// 10 - (-2)
	{"10--2", 12, false},			// WHERE Query API: minus + unary minus; SQL UPDATE still treats '--' as comment
	{"((1+2)*3)-1", 8, true},		// ((1 + 2) * 3) - 1
	{"2+3*4-5", 9, true},			// (2 + (3 * 4)) - 5
	{"2*3+4*5", 26, true},			// (2 * 3) + (4 * 5)
	{"100/10/2*3", 15, true},		// ((100 / 10) / 2) * 3
	{"100/(10/(2*3))", 60, true},	// 100 / (10 / (2 * 3))
	{"{v}*2-{v}/2", 12, true},		// (8 * 2) - (8 / 2)
	{"({v}+2)*({v}-6)", 20, true},	// (8 + 2) * (8 - 6)
};

}  // namespace reindexer_tests
