#include "sqlencoder.h"

#include "core/queryresults/aggregationresult.h"
#include "core/type_consts.h"
#include "sql_formatters.h"
#include "tools/serilize/wrserializer.h"

constexpr static std::string_view kOpNames[] = {"-", "OR ", "AND ", "AND NOT "};

namespace reindexer {

SqlFormatterBase::~SqlFormatterBase() {
	if (std::uncaught_exceptions() == 0) {
		assertrx_dbg(indents_.empty());
		if (ser_.Len() == lastPositionInSerializer_) {
			ser_.Reset(prevPositionInSerializer_);
		}
	}
}

void SingleLineSqlFormatter::Comma() const { ser_ << ", "; }
void SingleLineSqlFormatter::Next() {
	if (ser_.Len() == lastPositionInSerializer_) {
		ser_.Reset(prevPositionInSerializer_);
	} else {
		prevPositionInSerializer_ = ser_.Len();
	}
	ser_ << ' ';
	lastPositionInSerializer_ = ser_.Len();
}

SqlFormatterBase::Guard<SingleLineSqlFormatter> SingleLineSqlFormatter::StartBlock() {
	Next();
	Base::startBlock();
	return Guard{*this, Block};
}

void SingleLineSqlFormatter::endBlock() {
	Next();
	Base::endBlock();
}

SqlFormatterBase::Guard<SingleLineSqlFormatter> SingleLineSqlFormatter::OpenParenthesis() {
	ser_ << '(';
	Base::openParenthesis();
	return Guard{*this, Parenthesis};
}

SqlFormatterBase::Guard<SingleLineSqlFormatter> SingleLineSqlFormatter::ConditionallyOpenParenthesis(bool needOpen) {
	if (needOpen) {
		return OpenParenthesis();
	} else {
		return Guard{*this, No};
	}
}

SqlFormatterBase::Guard<SingleLineSqlFormatter> SingleLineSqlFormatter::OpenBracket() {
	ser_ << '[';
	Base::openBracket();
	return Guard{*this, Bracket};
}

SqlFormatterBase::Guard<SingleLineSqlFormatter> SingleLineSqlFormatter::ConditionallyOpenBracket(bool needOpen) {
	if (needOpen) {
		return OpenBracket();
	} else {
		return Guard{*this, No};
	}
}

void SingleLineSqlFormatter::closeParenthesis() {
	if (ser_.Len() == lastPositionInSerializer_) {
		ser_.Reset(prevPositionInSerializer_);
	}
	ser_ << ')';
	prevPositionInSerializer_ = ser_.Len();
	lastPositionInSerializer_ = ser_.Len();
	Base::closeParenthesis();
}

void SingleLineSqlFormatter::closeBracket() {
	if (ser_.Len() == lastPositionInSerializer_) {
		ser_.Reset(prevPositionInSerializer_);
	}
	ser_ << ']';
	prevPositionInSerializer_ = ser_.Len();
	lastPositionInSerializer_ = ser_.Len();
	Base::closeBracket();
}

void SingleLineSqlFormatter::ConditionsFormatter::AddCondition(OpType op, bool /*isNextOr*/) {
	if (first_) {
		if (op == OpNot) {
			baseFormatter_.ser_ << "NOT";
			baseFormatter_.Next();
		}
	} else {
		baseFormatter_.Next();
		baseFormatter_.ser_ << kOpNames[op];
	}
	first_ = false;
}

static constexpr std::string_view indentStr{"  "};

void PrettySqlFormatter::Next() {
	if (ser_.Len() == lastPositionInSerializer_) {
		ser_.Reset(prevPositionInSerializer_);
	} else {
		prevPositionInSerializer_ = ser_.Len();
	}
	ser_ << '\n';
	const auto indents = startIndent + Base::indents();
	for (unsigned i = 0; i < indents; ++i) {
		ser_ << indentStr;
	}
	lastPositionInSerializer_ = ser_.Len();
}

void PrettySqlFormatter::Comma() {
	ser_ << ',';
	Next();
}

SqlFormatterBase::Guard<PrettySqlFormatter> PrettySqlFormatter::StartBlock() {
	Base::startBlock();
	Next();
	return Guard{*this, Block};
}

void PrettySqlFormatter::endBlock() {
	Base::endBlock();
	Next();
}

void PrettySqlFormatter::openParenthesis() {
	ser_ << '(';
	Base::openParenthesis();
	Next();
}

void PrettySqlFormatter::openBracket() {
	ser_ << '[';
	Base::openBracket();
	Next();
}

SqlFormatterBase::Guard<PrettySqlFormatter> PrettySqlFormatter::OpenParenthesis() {
	openParenthesis();
	return Guard{*this, Parenthesis};
}

SqlFormatterBase::Guard<PrettySqlFormatter> PrettySqlFormatter::ConditionallyOpenParenthesis(bool needOpen) {
	if (needOpen) {
		return OpenParenthesis();
	} else {
		return Guard{*this, No};
	}
}

SqlFormatterBase::Guard<PrettySqlFormatter> PrettySqlFormatter::OpenBracket() {
	openBracket();
	return Guard{*this, Bracket};
}

SqlFormatterBase::Guard<PrettySqlFormatter> PrettySqlFormatter::ConditionallyOpenBracket(bool needOpen) {
	if (needOpen) {
		return OpenBracket();
	} else {
		return Guard{*this, No};
	}
}

void PrettySqlFormatter::closeParenthesis() {
	Base::closeParenthesis();
	Next();
	ser_ << ')';
}

void PrettySqlFormatter::closeBracket() {
	Base::closeBracket();
	Next();
	ser_ << ']';
}

void PrettySqlFormatter::ConditionsFormatter::AddCondition(OpType op, bool isNextOr) {
	if (orStarted_ && op != OpOr) {
		assertrx_dbg(!first_);
		baseFormatter_.closeParenthesis();
		orStarted_ = false;
	}
	if (first_) {
		if (op == OpNot) {
			baseFormatter_.ser_ << "NOT ";
		}
	} else {
		baseFormatter_.Next();
		baseFormatter_.ser_ << kOpNames[op];
	}
	if (needEncloseOR_ && isNextOr && !orStarted_) {
		baseFormatter_.openParenthesis();
		orStarted_ = true;
	}
	first_ = false;
}

PrettySqlFormatter::ConditionsFormatter::~ConditionsFormatter() noexcept(false) {
	assertrx_dbg(!first_);
	if (orStarted_ && std::uncaught_exceptions() == 0) {
		baseFormatter_.closeParenthesis();
	}
}

}  // namespace reindexer
