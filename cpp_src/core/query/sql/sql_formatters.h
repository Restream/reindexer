#pragma once

#include <exception>
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "tools/assertrx.h"

/// @namespace reindexer
/// The base namespace
namespace reindexer {

class WrSerializer;

class [[nodiscard]] SqlFormatterBase {
protected:
	enum [[nodiscard]] IndentType : int8_t { Block, Parenthesis, Bracket, No };

	SqlFormatterBase(WrSerializer& ser) noexcept : ser_(ser) {}
	~SqlFormatterBase();

	template <typename Formatter>
	class [[nodiscard]] Guard {
	public:
		Guard(Formatter& f, IndentType indentT) noexcept : formatter_(f), indentType_(indentT) {}
		~Guard() noexcept(false) {
			if (std::uncaught_exceptions() == 0) {
				switch (indentType_) {
					case Block:
						formatter_.endBlock();
						break;
					case Parenthesis:
						formatter_.closeParenthesis();
						break;
					case Bracket:
						formatter_.closeBracket();
						break;
					case No:
						break;
				}
			}
		}

	private:
		Formatter& formatter_;
		const IndentType indentType_;
	};

	void reset() noexcept { indents_ = {}; }
	void startBlock() { indents_.push_back(Block); }
	void openParenthesis() { indents_.push_back(Parenthesis); }
	void openBracket() { indents_.push_back(Bracket); }
	void endBlock() {
		assertrx_dbg(!indents_.empty());
		assertrx_dbg(indents_.back() == Block);
		indents_.pop_back();
	}
	void closeParenthesis() {
		assertrx_dbg(!indents_.empty());
		assertrx_dbg(indents_.back() == Parenthesis);
		indents_.pop_back();
	}
	void closeBracket() {
		assertrx_dbg(!indents_.empty());
		assertrx_dbg(indents_.back() == Bracket);
		indents_.pop_back();
	}

	unsigned indents() const noexcept { return indents_.size(); }

	WrSerializer& ser_;
	size_t prevPositionInSerializer_{0};
	size_t lastPositionInSerializer_{0};

private:
	h_vector<IndentType, 32> indents_;
};

class [[nodiscard]] SingleLineSqlFormatter : private SqlFormatterBase {
	using Base = SqlFormatterBase;
	friend class Base::Guard<SingleLineSqlFormatter>;

	class [[nodiscard]] ConditionsFormatter {
	public:
		ConditionsFormatter(SingleLineSqlFormatter& base) noexcept : baseFormatter_(base) {}
		void AddCondition(OpType, bool isNextOr);

	private:
		SingleLineSqlFormatter& baseFormatter_;
		bool first_{true};
	};

public:
	SingleLineSqlFormatter(WrSerializer& ser) noexcept : Base(ser) {}
	WrSerializer& Serializer() & noexcept { return ser_; }
	void Reset() noexcept { reset(); }
	void Comma() const;
	void Next();
	Guard<SingleLineSqlFormatter> StartBlock();
	Guard<SingleLineSqlFormatter> OpenParenthesis();
	Guard<SingleLineSqlFormatter> ConditionallyOpenParenthesis(bool needOpen);
	Guard<SingleLineSqlFormatter> OpenBracket();
	Guard<SingleLineSqlFormatter> ConditionallyOpenBracket(bool needOpen);
	ConditionsFormatter StartConditions(bool /*needEncloseOR*/) noexcept { return {*this}; }

private:
	void endBlock();
	void closeParenthesis();
	void closeBracket();
};

class [[nodiscard]] PrettySqlFormatter : private SqlFormatterBase {
	using Base = SqlFormatterBase;
	friend class Base::Guard<PrettySqlFormatter>;

	class [[nodiscard]] ConditionsFormatter {
	public:
		ConditionsFormatter(PrettySqlFormatter& base, bool needEncloseOR) noexcept : baseFormatter_(base), needEncloseOR_(needEncloseOR) {}
		~ConditionsFormatter() noexcept(false);
		void AddCondition(OpType, bool isNextOr);

	private:
		PrettySqlFormatter& baseFormatter_;
		bool first_{true};
		bool orStarted_{false};
		const bool needEncloseOR_;
	};

public:
	PrettySqlFormatter(WrSerializer& ser) noexcept : Base(ser) {}
	WrSerializer& Serializer() & noexcept { return ser_; }
	void Reset() noexcept { reset(); }
	void Comma();
	void Next();
	Guard<PrettySqlFormatter> StartBlock();
	Guard<PrettySqlFormatter> OpenParenthesis();
	Guard<PrettySqlFormatter> ConditionallyOpenParenthesis(bool needOpen);
	Guard<PrettySqlFormatter> OpenBracket();
	Guard<PrettySqlFormatter> ConditionallyOpenBracket(bool needOpen);
	ConditionsFormatter StartConditions(bool needEncloseOR) noexcept { return {*this, needEncloseOR}; }

private:
	void endBlock();
	void openParenthesis();
	void closeParenthesis();
	void openBracket();
	void closeBracket();

	unsigned startIndent{0};
};

}  // namespace reindexer
