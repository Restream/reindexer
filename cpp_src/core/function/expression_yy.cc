// NOLINTBEGIN
// A Bison parser, made by GNU Bison 3.8.2.

// Skeleton implementation for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2015, 2018-2021 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

// As a special exception, you may create a larger work that contains
// part or all of the Bison parser skeleton and distribute that work
// under terms of your choice, so long as that work isn't itself a
// parser generator using the skeleton or a modified version thereof
// as a parser skeleton.  Alternatively, if you modify or redistribute
// the parser skeleton itself, you may (at your option) remove this
// special exception, which will cause the skeleton and the resulting
// Bison output files to be licensed under the GNU General Public
// License without this special exception.

// This special exception was added by the Free Software Foundation in
// version 2.2 of Bison.

// DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
// especially those whose name start with YY_ or yy_.  They are
// private implementation details that can be changed or removed.

// "%code top" blocks.
#line 15 "expression.y"

	#if defined(__GNUC__) && !defined(__clang__)
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
	#endif

#line 46 "expression_yy.cc"




#include "expression_yy.hh"


// Unqualified %code blocks.
#line 29 "expression.y"

	#include <utility>
	namespace reindexer::expr_yy {
	Parser::symbol_type yylex(reindexer::ExprParseContext& ctx);
	}  // namespace reindexer::expr_yy

#line 62 "expression_yy.cc"


#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> // FIXME: INFRINGES ON USER NAME SPACE.
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif


// Whether we are compiled with exception support.
#ifndef YY_EXCEPTIONS
# if defined __GNUC__ && !defined __EXCEPTIONS
#  define YY_EXCEPTIONS 0
# else
#  define YY_EXCEPTIONS 1
# endif
#endif



// Enable debugging if requested.
#if YYDEBUG

// A pseudo ostream that takes yydebug_ into account.
# define YYCDEBUG if (yydebug_) (*yycdebug_)

# define YY_SYMBOL_PRINT(Title, Symbol)         \
  do {                                          \
    if (yydebug_)                               \
    {                                           \
      *yycdebug_ << Title << ' ';               \
      yy_print_ (*yycdebug_, Symbol);           \
      *yycdebug_ << '\n';                       \
    }                                           \
  } while (false)

# define YY_REDUCE_PRINT(Rule)          \
  do {                                  \
    if (yydebug_)                       \
      yy_reduce_print_ (Rule);          \
  } while (false)

# define YY_STACK_PRINT()               \
  do {                                  \
    if (yydebug_)                       \
      yy_stack_print_ ();                \
  } while (false)

#else // !YYDEBUG

# define YYCDEBUG if (false) std::cerr
# define YY_SYMBOL_PRINT(Title, Symbol)  YY_USE (Symbol)
# define YY_REDUCE_PRINT(Rule)           static_cast<void> (0)
# define YY_STACK_PRINT()                static_cast<void> (0)

#endif // !YYDEBUG

#define yyerrok         (yyerrstatus_ = 0)
#define yyclearin       (yyla.clear ())

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYRECOVERING()  (!!yyerrstatus_)

#line 6 "expression.y"
namespace reindexer { namespace expr_yy {
#line 136 "expression_yy.cc"

  /// Build a parser object.
  Parser::Parser (reindexer::ExprParseContext& ctx_yyarg)
#if YYDEBUG
    : yydebug_ (false),
      yycdebug_ (&std::cerr),
#else
    :
#endif
      ctx (ctx_yyarg)
  {}

  Parser::~Parser ()
  {}

  Parser::syntax_error::~syntax_error () YY_NOEXCEPT YY_NOTHROW
  {}

  /*---------.
  | symbol.  |
  `---------*/



  // by_state.
  Parser::by_state::by_state () YY_NOEXCEPT
    : state (empty_state)
  {}

  Parser::by_state::by_state (const by_state& that) YY_NOEXCEPT
    : state (that.state)
  {}

  void
  Parser::by_state::clear () YY_NOEXCEPT
  {
    state = empty_state;
  }

  void
  Parser::by_state::move (by_state& that)
  {
    state = that.state;
    that.clear ();
  }

  Parser::by_state::by_state (state_type s) YY_NOEXCEPT
    : state (s)
  {}

  Parser::symbol_kind_type
  Parser::by_state::kind () const YY_NOEXCEPT
  {
    if (state == empty_state)
      return symbol_kind::S_YYEMPTY;
    else
      return YY_CAST (symbol_kind_type, yystos_[+state]);
  }

  Parser::stack_symbol_type::stack_symbol_type ()
  {}

  Parser::stack_symbol_type::stack_symbol_type (YY_RVREF (stack_symbol_type) that)
    : super_type (YY_MOVE (that.state))
  {
    switch (that.kind ())
    {
      case symbol_kind::S_opt_expr_args: // opt_expr_args
      case symbol_kind::S_expr_args: // expr_args
        value.YY_MOVE_OR_COPY< reindexer::ExprNodeArgs > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_input: // input
      case symbol_kind::S_expr: // expr
      case symbol_kind::S_primary: // primary
      case symbol_kind::S_array_lit: // array_lit
        value.YY_MOVE_OR_COPY< reindexer::ExprNodePtr > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_NUMBER: // NUMBER
      case symbol_kind::S_array_elem: // array_elem
        value.YY_MOVE_OR_COPY< reindexer::Variant > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_opt_array_elems: // opt_array_elems
      case symbol_kind::S_array_elems: // array_elems
        value.YY_MOVE_OR_COPY< reindexer::VariantArray > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_NAME: // NAME
      case symbol_kind::S_QUOTED_NAME: // QUOTED_NAME
      case symbol_kind::S_FUNCTION: // FUNCTION
      case symbol_kind::S_STRING: // STRING
        value.YY_MOVE_OR_COPY< std::string > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

#if 201103L <= YY_CPLUSPLUS
    // that is emptied.
    that.state = empty_state;
#endif
  }

  Parser::stack_symbol_type::stack_symbol_type (state_type s, YY_MOVE_REF (symbol_type) that)
    : super_type (s)
  {
    switch (that.kind ())
    {
      case symbol_kind::S_opt_expr_args: // opt_expr_args
      case symbol_kind::S_expr_args: // expr_args
        value.move< reindexer::ExprNodeArgs > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_input: // input
      case symbol_kind::S_expr: // expr
      case symbol_kind::S_primary: // primary
      case symbol_kind::S_array_lit: // array_lit
        value.move< reindexer::ExprNodePtr > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_NUMBER: // NUMBER
      case symbol_kind::S_array_elem: // array_elem
        value.move< reindexer::Variant > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_opt_array_elems: // opt_array_elems
      case symbol_kind::S_array_elems: // array_elems
        value.move< reindexer::VariantArray > (YY_MOVE (that.value));
        break;

      case symbol_kind::S_NAME: // NAME
      case symbol_kind::S_QUOTED_NAME: // QUOTED_NAME
      case symbol_kind::S_FUNCTION: // FUNCTION
      case symbol_kind::S_STRING: // STRING
        value.move< std::string > (YY_MOVE (that.value));
        break;

      default:
        break;
    }

    // that is emptied.
    that.kind_ = symbol_kind::S_YYEMPTY;
  }

#if YY_CPLUSPLUS < 201103L
  Parser::stack_symbol_type&
  Parser::stack_symbol_type::operator= (const stack_symbol_type& that)
  {
    state = that.state;
    switch (that.kind ())
    {
      case symbol_kind::S_opt_expr_args: // opt_expr_args
      case symbol_kind::S_expr_args: // expr_args
        value.copy< reindexer::ExprNodeArgs > (that.value);
        break;

      case symbol_kind::S_input: // input
      case symbol_kind::S_expr: // expr
      case symbol_kind::S_primary: // primary
      case symbol_kind::S_array_lit: // array_lit
        value.copy< reindexer::ExprNodePtr > (that.value);
        break;

      case symbol_kind::S_NUMBER: // NUMBER
      case symbol_kind::S_array_elem: // array_elem
        value.copy< reindexer::Variant > (that.value);
        break;

      case symbol_kind::S_opt_array_elems: // opt_array_elems
      case symbol_kind::S_array_elems: // array_elems
        value.copy< reindexer::VariantArray > (that.value);
        break;

      case symbol_kind::S_NAME: // NAME
      case symbol_kind::S_QUOTED_NAME: // QUOTED_NAME
      case symbol_kind::S_FUNCTION: // FUNCTION
      case symbol_kind::S_STRING: // STRING
        value.copy< std::string > (that.value);
        break;

      default:
        break;
    }

    return *this;
  }

  Parser::stack_symbol_type&
  Parser::stack_symbol_type::operator= (stack_symbol_type& that)
  {
    state = that.state;
    switch (that.kind ())
    {
      case symbol_kind::S_opt_expr_args: // opt_expr_args
      case symbol_kind::S_expr_args: // expr_args
        value.move< reindexer::ExprNodeArgs > (that.value);
        break;

      case symbol_kind::S_input: // input
      case symbol_kind::S_expr: // expr
      case symbol_kind::S_primary: // primary
      case symbol_kind::S_array_lit: // array_lit
        value.move< reindexer::ExprNodePtr > (that.value);
        break;

      case symbol_kind::S_NUMBER: // NUMBER
      case symbol_kind::S_array_elem: // array_elem
        value.move< reindexer::Variant > (that.value);
        break;

      case symbol_kind::S_opt_array_elems: // opt_array_elems
      case symbol_kind::S_array_elems: // array_elems
        value.move< reindexer::VariantArray > (that.value);
        break;

      case symbol_kind::S_NAME: // NAME
      case symbol_kind::S_QUOTED_NAME: // QUOTED_NAME
      case symbol_kind::S_FUNCTION: // FUNCTION
      case symbol_kind::S_STRING: // STRING
        value.move< std::string > (that.value);
        break;

      default:
        break;
    }

    // that is emptied.
    that.state = empty_state;
    return *this;
  }
#endif

  template <typename Base>
  void
  Parser::yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const
  {
    if (yymsg)
      YY_SYMBOL_PRINT (yymsg, yysym);
  }

#if YYDEBUG
  template <typename Base>
  void
  Parser::yy_print_ (std::ostream& yyo, const basic_symbol<Base>& yysym) const
  {
    std::ostream& yyoutput = yyo;
    YY_USE (yyoutput);
    if (yysym.empty ())
      yyo << "empty symbol";
    else
      {
        symbol_kind_type yykind = yysym.kind ();
        yyo << (yykind < YYNTOKENS ? "token" : "nterm")
            << ' ' << yysym.name () << " (";
        YY_USE (yykind);
        yyo << ')';
      }
  }
#endif

  void
  Parser::yypush_ (const char* m, YY_MOVE_REF (stack_symbol_type) sym)
  {
    if (m)
      YY_SYMBOL_PRINT (m, sym);
    yystack_.push (YY_MOVE (sym));
  }

  void
  Parser::yypush_ (const char* m, state_type s, YY_MOVE_REF (symbol_type) sym)
  {
#if 201103L <= YY_CPLUSPLUS
    yypush_ (m, stack_symbol_type (s, std::move (sym)));
#else
    stack_symbol_type ss (s, sym);
    yypush_ (m, ss);
#endif
  }

  void
  Parser::yypop_ (int n) YY_NOEXCEPT
  {
    yystack_.pop (n);
  }

#if YYDEBUG
  std::ostream&
  Parser::debug_stream () const
  {
    return *yycdebug_;
  }

  void
  Parser::set_debug_stream (std::ostream& o)
  {
    yycdebug_ = &o;
  }


  Parser::debug_level_type
  Parser::debug_level () const
  {
    return yydebug_;
  }

  void
  Parser::set_debug_level (debug_level_type l)
  {
    yydebug_ = l;
  }
#endif // YYDEBUG

  Parser::state_type
  Parser::yy_lr_goto_state_ (state_type yystate, int yysym)
  {
    int yyr = yypgoto_[yysym - YYNTOKENS] + yystate;
    if (0 <= yyr && yyr <= yylast_ && yycheck_[yyr] == yystate)
      return yytable_[yyr];
    else
      return yydefgoto_[yysym - YYNTOKENS];
  }

  bool
  Parser::yy_pact_value_is_default_ (int yyvalue) YY_NOEXCEPT
  {
    return yyvalue == yypact_ninf_;
  }

  bool
  Parser::yy_table_value_is_error_ (int yyvalue) YY_NOEXCEPT
  {
    return yyvalue == yytable_ninf_;
  }

  int
  Parser::operator() ()
  {
    return parse ();
  }

  int
  Parser::parse ()
  {
    int yyn;
    /// Length of the RHS of the rule being reduced.
    int yylen = 0;

    // Error handling.
    int yynerrs_ = 0;
    int yyerrstatus_ = 0;

    /// The lookahead symbol.
    symbol_type yyla;

    /// The return value of parse ().
    int yyresult;

#if YY_EXCEPTIONS
    try
#endif // YY_EXCEPTIONS
      {
    YYCDEBUG << "Starting parse\n";


    /* Initialize the stack.  The initial state will be set in
       yynewstate, since the latter expects the semantical and the
       location values to have been already stored, initialize these
       stacks with a primary value.  */
    yystack_.clear ();
    yypush_ (YY_NULLPTR, 0, YY_MOVE (yyla));

  /*-----------------------------------------------.
  | yynewstate -- push a new symbol on the stack.  |
  `-----------------------------------------------*/
  yynewstate:
    YYCDEBUG << "Entering state " << int (yystack_[0].state) << '\n';
    YY_STACK_PRINT ();

    // Accept?
    if (yystack_[0].state == yyfinal_)
      YYACCEPT;

    goto yybackup;


  /*-----------.
  | yybackup.  |
  `-----------*/
  yybackup:
    // Try to take a decision without lookahead.
    yyn = yypact_[+yystack_[0].state];
    if (yy_pact_value_is_default_ (yyn))
      goto yydefault;

    // Read a lookahead token.
    if (yyla.empty ())
      {
        YYCDEBUG << "Reading a token\n";
#if YY_EXCEPTIONS
        try
#endif // YY_EXCEPTIONS
          {
            symbol_type yylookahead (yylex (ctx));
            yyla.move (yylookahead);
          }
#if YY_EXCEPTIONS
        catch (const syntax_error& yyexc)
          {
            YYCDEBUG << "Caught exception: " << yyexc.what() << '\n';
            error (yyexc);
            goto yyerrlab1;
          }
#endif // YY_EXCEPTIONS
      }
    YY_SYMBOL_PRINT ("Next token is", yyla);

    if (yyla.kind () == symbol_kind::S_YYerror)
    {
      // The scanner already issued an error message, process directly
      // to error recovery.  But do not keep the error token as
      // lookahead, it is too special and may lead us to an endless
      // loop in error recovery. */
      yyla.kind_ = symbol_kind::S_YYUNDEF;
      goto yyerrlab1;
    }

    /* If the proper action on seeing token YYLA.TYPE is to reduce or
       to detect an error, take that action.  */
    yyn += yyla.kind ();
    if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yyla.kind ())
      {
        goto yydefault;
      }

    // Reduce or error.
    yyn = yytable_[yyn];
    if (yyn <= 0)
      {
        if (yy_table_value_is_error_ (yyn))
          goto yyerrlab;
        yyn = -yyn;
        goto yyreduce;
      }

    // Count tokens shifted since error; after three, turn off error status.
    if (yyerrstatus_)
      --yyerrstatus_;

    // Shift the lookahead token.
    yypush_ ("Shifting", state_type (yyn), YY_MOVE (yyla));
    goto yynewstate;


  /*-----------------------------------------------------------.
  | yydefault -- do the default action for the current state.  |
  `-----------------------------------------------------------*/
  yydefault:
    yyn = yydefact_[+yystack_[0].state];
    if (yyn == 0)
      goto yyerrlab;
    goto yyreduce;


  /*-----------------------------.
  | yyreduce -- do a reduction.  |
  `-----------------------------*/
  yyreduce:
    yylen = yyr2_[yyn];
    {
      stack_symbol_type yylhs;
      yylhs.state = yy_lr_goto_state_ (yystack_[yylen].state, yyr1_[yyn]);
      /* Variants are always initialized to an empty instance of the
         correct type. The default '$$ = $1' action is NOT applied
         when using variants.  */
      switch (yyr1_[yyn])
    {
      case symbol_kind::S_opt_expr_args: // opt_expr_args
      case symbol_kind::S_expr_args: // expr_args
        yylhs.value.emplace< reindexer::ExprNodeArgs > ();
        break;

      case symbol_kind::S_input: // input
      case symbol_kind::S_expr: // expr
      case symbol_kind::S_primary: // primary
      case symbol_kind::S_array_lit: // array_lit
        yylhs.value.emplace< reindexer::ExprNodePtr > ();
        break;

      case symbol_kind::S_NUMBER: // NUMBER
      case symbol_kind::S_array_elem: // array_elem
        yylhs.value.emplace< reindexer::Variant > ();
        break;

      case symbol_kind::S_opt_array_elems: // opt_array_elems
      case symbol_kind::S_array_elems: // array_elems
        yylhs.value.emplace< reindexer::VariantArray > ();
        break;

      case symbol_kind::S_NAME: // NAME
      case symbol_kind::S_QUOTED_NAME: // QUOTED_NAME
      case symbol_kind::S_FUNCTION: // FUNCTION
      case symbol_kind::S_STRING: // STRING
        yylhs.value.emplace< std::string > ();
        break;

      default:
        break;
    }



      // Perform the reduction.
      YY_REDUCE_PRINT (yyn);
#if YY_EXCEPTIONS
      try
#endif // YY_EXCEPTIONS
        {
          switch (yyn)
            {
  case 2: // input: expr "end of expression"
#line 67 "expression.y"
                   {
		ctx.SetResult(std::move(yystack_[1].value.as < reindexer::ExprNodePtr > ()));
	}
#line 665 "expression_yy.cc"
    break;

  case 3: // expr: expr "+" expr
#line 73 "expression.y"
                         {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Add, std::move(yystack_[2].value.as < reindexer::ExprNodePtr > ()), std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 673 "expression_yy.cc"
    break;

  case 4: // expr: expr "-" expr
#line 76 "expression.y"
                          {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Sub, std::move(yystack_[2].value.as < reindexer::ExprNodePtr > ()), std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 681 "expression_yy.cc"
    break;

  case 5: // expr: expr "*" expr
#line 79 "expression.y"
                        {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Mul, std::move(yystack_[2].value.as < reindexer::ExprNodePtr > ()), std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 689 "expression_yy.cc"
    break;

  case 6: // expr: expr "/" expr
#line 82 "expression.y"
                        {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Div, std::move(yystack_[2].value.as < reindexer::ExprNodePtr > ()), std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 697 "expression_yy.cc"
    break;

  case 7: // expr: expr "||" expr
#line 85 "expression.y"
                         {
		if (ctx.WhereMode()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: '||'");
		}
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprBinary>(reindexer::ExprBinOp::Concat, std::move(yystack_[2].value.as < reindexer::ExprNodePtr > ()), std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 708 "expression_yy.cc"
    break;

  case 8: // expr: "-" expr
#line 91 "expression.y"
                                  {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprUnaryMinus>(std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 716 "expression_yy.cc"
    break;

  case 9: // expr: primary
#line 94 "expression.y"
                  { yylhs.value.as < reindexer::ExprNodePtr > () = std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()); }
#line 722 "expression_yy.cc"
    break;

  case 10: // primary: NUMBER
#line 98 "expression.y"
                 {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprNumber>(std::move(yystack_[0].value.as < reindexer::Variant > ()));
	}
#line 730 "expression_yy.cc"
    break;

  case 11: // primary: STRING
#line 101 "expression.y"
                 {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: string literal");
		}
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{std::move(yystack_[0].value.as < std::string > ())});
	}
#line 741 "expression_yy.cc"
    break;

  case 12: // primary: TRUE
#line 107 "expression.y"
               {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: boolean literal");
		}
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{true});
	}
#line 752 "expression_yy.cc"
    break;

  case 13: // primary: FALSE
#line 113 "expression.y"
                {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: boolean literal");
		}
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{false});
	}
#line 763 "expression_yy.cc"
    break;

  case 14: // primary: NULL_VALUE
#line 119 "expression.y"
                     {
		if (ctx.WhereMode() && ctx.InternFieldNames()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: null literal");
		}
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprNumber>(reindexer::Variant{});
	}
#line 774 "expression_yy.cc"
    break;

  case 15: // primary: NAME
#line 125 "expression.y"
               {
		yylhs.value.as < reindexer::ExprNodePtr > () = ctx.MakeField(std::move(yystack_[0].value.as < std::string > ()));
	}
#line 782 "expression_yy.cc"
    break;

  case 16: // primary: QUOTED_NAME
#line 128 "expression.y"
                      {
		yylhs.value.as < reindexer::ExprNodePtr > () = ctx.MakeField(std::move(yystack_[0].value.as < std::string > ()), true);
	}
#line 790 "expression_yy.cc"
    break;

  case 17: // primary: FUNCTION "(" opt_expr_args ")"
#line 131 "expression.y"
                                               {
		yylhs.value.as < reindexer::ExprNodePtr > () = ctx.MakeFunction(std::move(yystack_[3].value.as < std::string > ()), std::move(yystack_[1].value.as < reindexer::ExprNodeArgs > ()));
	}
#line 798 "expression_yy.cc"
    break;

  case 18: // primary: "(" expr ")"
#line 134 "expression.y"
                             { yylhs.value.as < reindexer::ExprNodePtr > () = std::move(yystack_[1].value.as < reindexer::ExprNodePtr > ()); }
#line 804 "expression_yy.cc"
    break;

  case 19: // primary: array_lit
#line 135 "expression.y"
                    {
		if (ctx.WhereMode()) {
			ctx.ThrowError("Unsupported construct in WHERE arithmetic expression: '['");
		}
		yylhs.value.as < reindexer::ExprNodePtr > () = std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ());
	}
#line 815 "expression_yy.cc"
    break;

  case 20: // array_lit: "[" opt_array_elems "]"
#line 144 "expression.y"
                                        {
		yylhs.value.as < reindexer::ExprNodePtr > () = std::make_unique<reindexer::ExprArrayLiteral>(std::move(yystack_[1].value.as < reindexer::VariantArray > ()));
	}
#line 823 "expression_yy.cc"
    break;

  case 21: // opt_array_elems: %empty
#line 150 "expression.y"
                 { yylhs.value.as < reindexer::VariantArray > () = reindexer::VariantArray{}; }
#line 829 "expression_yy.cc"
    break;

  case 22: // opt_array_elems: array_elems
#line 151 "expression.y"
                      { yylhs.value.as < reindexer::VariantArray > () = std::move(yystack_[0].value.as < reindexer::VariantArray > ()); }
#line 835 "expression_yy.cc"
    break;

  case 23: // array_elems: array_elem
#line 155 "expression.y"
                     {
		yylhs.value.as < reindexer::VariantArray > () = reindexer::VariantArray{};
		yylhs.value.as < reindexer::VariantArray > ().emplace_back(std::move(yystack_[0].value.as < reindexer::Variant > ()));
	}
#line 844 "expression_yy.cc"
    break;

  case 24: // array_elems: array_elems "," array_elem
#line 159 "expression.y"
                                       { yylhs.value.as < reindexer::VariantArray > () = std::move(yystack_[2].value.as < reindexer::VariantArray > ()); yylhs.value.as < reindexer::VariantArray > ().emplace_back(std::move(yystack_[0].value.as < reindexer::Variant > ())); }
#line 850 "expression_yy.cc"
    break;

  case 25: // array_elem: NUMBER
#line 163 "expression.y"
                 { yylhs.value.as < reindexer::Variant > () = std::move(yystack_[0].value.as < reindexer::Variant > ()); }
#line 856 "expression_yy.cc"
    break;

  case 26: // array_elem: "-" NUMBER
#line 164 "expression.y"
                       {
		if (yystack_[0].value.as < reindexer::Variant > ().Type().IsOneOf<reindexer::KeyValueType::Int, reindexer::KeyValueType::Int64>()) {
			yylhs.value.as < reindexer::Variant > () = reindexer::Variant{-yystack_[0].value.as < reindexer::Variant > ().As<int64_t>()};
		} else {
			yylhs.value.as < reindexer::Variant > () = reindexer::Variant{-yystack_[0].value.as < reindexer::Variant > ().As<double>()};
		}
	}
#line 868 "expression_yy.cc"
    break;

  case 27: // array_elem: "+" NUMBER
#line 171 "expression.y"
                      { yylhs.value.as < reindexer::Variant > () = std::move(yystack_[0].value.as < reindexer::Variant > ()); }
#line 874 "expression_yy.cc"
    break;

  case 28: // array_elem: STRING
#line 172 "expression.y"
                 { yylhs.value.as < reindexer::Variant > () = reindexer::Variant{std::move(yystack_[0].value.as < std::string > ())}; }
#line 880 "expression_yy.cc"
    break;

  case 29: // array_elem: TRUE
#line 173 "expression.y"
               { yylhs.value.as < reindexer::Variant > () = reindexer::Variant{true}; }
#line 886 "expression_yy.cc"
    break;

  case 30: // array_elem: FALSE
#line 174 "expression.y"
                { yylhs.value.as < reindexer::Variant > () = reindexer::Variant{false}; }
#line 892 "expression_yy.cc"
    break;

  case 31: // array_elem: NULL_VALUE
#line 175 "expression.y"
                     { yylhs.value.as < reindexer::Variant > () = reindexer::Variant{}; }
#line 898 "expression_yy.cc"
    break;

  case 32: // opt_expr_args: %empty
#line 179 "expression.y"
                 { yylhs.value.as < reindexer::ExprNodeArgs > () = reindexer::ExprNodeArgs{}; }
#line 904 "expression_yy.cc"
    break;

  case 33: // opt_expr_args: expr_args
#line 180 "expression.y"
                    { yylhs.value.as < reindexer::ExprNodeArgs > () = std::move(yystack_[0].value.as < reindexer::ExprNodeArgs > ()); }
#line 910 "expression_yy.cc"
    break;

  case 34: // expr_args: expr
#line 184 "expression.y"
               {
		yylhs.value.as < reindexer::ExprNodeArgs > () = reindexer::ExprNodeArgs{};
		yylhs.value.as < reindexer::ExprNodeArgs > ().emplace_back(std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ()));
	}
#line 919 "expression_yy.cc"
    break;

  case 35: // expr_args: expr_args "," expr
#line 188 "expression.y"
                               { yylhs.value.as < reindexer::ExprNodeArgs > () = std::move(yystack_[2].value.as < reindexer::ExprNodeArgs > ()); yylhs.value.as < reindexer::ExprNodeArgs > ().emplace_back(std::move(yystack_[0].value.as < reindexer::ExprNodePtr > ())); }
#line 925 "expression_yy.cc"
    break;


#line 929 "expression_yy.cc"

            default:
              break;
            }
        }
#if YY_EXCEPTIONS
      catch (const syntax_error& yyexc)
        {
          YYCDEBUG << "Caught exception: " << yyexc.what() << '\n';
          error (yyexc);
          YYERROR;
        }
#endif // YY_EXCEPTIONS
      YY_SYMBOL_PRINT ("-> $$ =", yylhs);
      yypop_ (yylen);
      yylen = 0;

      // Shift the result of the reduction.
      yypush_ (YY_NULLPTR, YY_MOVE (yylhs));
    }
    goto yynewstate;


  /*--------------------------------------.
  | yyerrlab -- here on detecting error.  |
  `--------------------------------------*/
  yyerrlab:
    // If not already recovering from an error, report this error.
    if (!yyerrstatus_)
      {
        ++yynerrs_;
        context yyctx (*this, yyla);
        report_syntax_error (yyctx);
      }


    if (yyerrstatus_ == 3)
      {
        /* If just tried and failed to reuse lookahead token after an
           error, discard it.  */

        // Return failure if at end of input.
        if (yyla.kind () == symbol_kind::S_YYEOF)
          YYABORT;
        else if (!yyla.empty ())
          {
            yy_destroy_ ("Error: discarding", yyla);
            yyla.clear ();
          }
      }

    // Else will try to reuse lookahead token after shifting the error token.
    goto yyerrlab1;


  /*---------------------------------------------------.
  | yyerrorlab -- error raised explicitly by YYERROR.  |
  `---------------------------------------------------*/
  yyerrorlab:
    /* Pacify compilers when the user code never invokes YYERROR and
       the label yyerrorlab therefore never appears in user code.  */
    if (false)
      YYERROR;

    /* Do not reclaim the symbols of the rule whose action triggered
       this YYERROR.  */
    yypop_ (yylen);
    yylen = 0;
    YY_STACK_PRINT ();
    goto yyerrlab1;


  /*-------------------------------------------------------------.
  | yyerrlab1 -- common code for both syntax error and YYERROR.  |
  `-------------------------------------------------------------*/
  yyerrlab1:
    yyerrstatus_ = 3;   // Each real token shifted decrements this.
    // Pop stack until we find a state that shifts the error token.
    for (;;)
      {
        yyn = yypact_[+yystack_[0].state];
        if (!yy_pact_value_is_default_ (yyn))
          {
            yyn += symbol_kind::S_YYerror;
            if (0 <= yyn && yyn <= yylast_
                && yycheck_[yyn] == symbol_kind::S_YYerror)
              {
                yyn = yytable_[yyn];
                if (0 < yyn)
                  break;
              }
          }

        // Pop the current state because it cannot handle the error token.
        if (yystack_.size () == 1)
          YYABORT;

        yy_destroy_ ("Error: popping", yystack_[0]);
        yypop_ ();
        YY_STACK_PRINT ();
      }
    {
      stack_symbol_type error_token;


      // Shift the error token.
      error_token.state = state_type (yyn);
      yypush_ ("Shifting", YY_MOVE (error_token));
    }
    goto yynewstate;


  /*-------------------------------------.
  | yyacceptlab -- YYACCEPT comes here.  |
  `-------------------------------------*/
  yyacceptlab:
    yyresult = 0;
    goto yyreturn;


  /*-----------------------------------.
  | yyabortlab -- YYABORT comes here.  |
  `-----------------------------------*/
  yyabortlab:
    yyresult = 1;
    goto yyreturn;


  /*-----------------------------------------------------.
  | yyreturn -- parsing is finished, return the result.  |
  `-----------------------------------------------------*/
  yyreturn:
    if (!yyla.empty ())
      yy_destroy_ ("Cleanup: discarding lookahead", yyla);

    /* Do not reclaim the symbols of the rule whose action triggered
       this YYABORT or YYACCEPT.  */
    yypop_ (yylen);
    YY_STACK_PRINT ();
    while (1 < yystack_.size ())
      {
        yy_destroy_ ("Cleanup: popping", yystack_[0]);
        yypop_ ();
      }

    return yyresult;
  }
#if YY_EXCEPTIONS
    catch (...)
      {
        YYCDEBUG << "Exception caught: cleaning lookahead and stack\n";
        // Do not try to display the values of the reclaimed symbols,
        // as their printers might throw an exception.
        if (!yyla.empty ())
          yy_destroy_ (YY_NULLPTR, yyla);

        while (1 < yystack_.size ())
          {
            yy_destroy_ (YY_NULLPTR, yystack_[0]);
            yypop_ ();
          }
        throw;
      }
#endif // YY_EXCEPTIONS
  }

  void
  Parser::error (const syntax_error& yyexc)
  {
    error (yyexc.what ());
  }

  const char *
  Parser::symbol_name (symbol_kind_type yysymbol)
  {
    static const char *const yy_sname[] =
    {
    "end of expression", "error", "invalid token", "NUMBER", "NAME",
  "QUOTED_NAME", "FUNCTION", "STRING", "TRUE", "FALSE", "NULL_VALUE", "||",
  "+", "-", "*", "/", "(", ")", "[", "]", ",", "UMINUS", "$accept",
  "input", "expr", "primary", "array_lit", "opt_array_elems",
  "array_elems", "array_elem", "opt_expr_args", "expr_args", YY_NULLPTR
    };
    return yy_sname[yysymbol];
  }



  // Parser::context.
  Parser::context::context (const Parser& yyparser, const symbol_type& yyla)
    : yyparser_ (yyparser)
    , yyla_ (yyla)
  {}

  int
  Parser::context::expected_tokens (symbol_kind_type yyarg[], int yyargn) const
  {
    // Actual number of expected tokens
    int yycount = 0;

    const int yyn = yypact_[+yyparser_.yystack_[0].state];
    if (!yy_pact_value_is_default_ (yyn))
      {
        /* Start YYX at -YYN if negative to avoid negative indexes in
           YYCHECK.  In other words, skip the first -YYN actions for
           this state because they are default actions.  */
        const int yyxbegin = yyn < 0 ? -yyn : 0;
        // Stay within bounds of both yycheck and yytname.
        const int yychecklim = yylast_ - yyn + 1;
        const int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
        for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
          if (yycheck_[yyx + yyn] == yyx && yyx != symbol_kind::S_YYerror
              && !yy_table_value_is_error_ (yytable_[yyx + yyn]))
            {
              if (!yyarg)
                ++yycount;
              else if (yycount == yyargn)
                return 0;
              else
                yyarg[yycount++] = YY_CAST (symbol_kind_type, yyx);
            }
      }

    if (yyarg && yycount == 0 && 0 < yyargn)
      yyarg[0] = symbol_kind::S_YYEMPTY;
    return yycount;
  }








  const signed char Parser::yypact_ninf_ = -15;

  const signed char Parser::yytable_ninf_ = -1;

  const signed char
  Parser::yypact_[] =
  {
      24,   -15,   -15,   -15,   -13,   -15,   -15,   -15,   -15,    24,
      24,    36,     4,     2,   -15,   -15,    24,   -15,    39,   -15,
     -15,   -15,   -15,   -15,     3,    15,   -14,    -1,   -15,   -15,
     -15,    24,    24,    24,    24,    24,    -3,    18,     0,   -15,
     -15,   -15,   -15,    36,   -15,    44,    44,    10,    10,   -15,
      24,   -15,    -3
  };

  const signed char
  Parser::yydefact_[] =
  {
       0,    10,    15,    16,     0,    11,    12,    13,    14,     0,
       0,    21,     0,     0,     9,    19,    32,     8,     0,    25,
      28,    29,    30,    31,     0,     0,     0,    22,    23,     1,
       2,     0,     0,     0,     0,     0,    34,     0,    33,    18,
      27,    26,    20,     0,     7,     3,     4,     5,     6,    17,
       0,    24,    35
  };

  const signed char
  Parser::yypgoto_[] =
  {
     -15,   -15,    -9,   -15,   -15,   -15,   -15,    -7,   -15,   -15
  };

  const signed char
  Parser::yydefgoto_[] =
  {
       0,    12,    13,    14,    15,    26,    27,    28,    37,    38
  };

  const signed char
  Parser::yytable_[] =
  {
      17,    18,    30,    16,    29,    42,    40,    36,    31,    32,
      33,    34,    35,    31,    32,    33,    34,    35,    41,    43,
      50,    31,    44,    45,    46,    47,    48,     1,     2,     3,
       4,     5,     6,     7,     8,    49,    51,     9,     0,    19,
      10,    52,    11,    20,    21,    22,    23,     0,    24,    25,
      31,    32,    33,    34,    35,    31,    39,     0,    34,    35
  };

  const signed char
  Parser::yycheck_[] =
  {
       9,    10,     0,    16,     0,    19,     3,    16,    11,    12,
      13,    14,    15,    11,    12,    13,    14,    15,     3,    20,
      20,    11,    31,    32,    33,    34,    35,     3,     4,     5,
       6,     7,     8,     9,    10,    17,    43,    13,    -1,     3,
      16,    50,    18,     7,     8,     9,    10,    -1,    12,    13,
      11,    12,    13,    14,    15,    11,    17,    -1,    14,    15
  };

  const signed char
  Parser::yystos_[] =
  {
       0,     3,     4,     5,     6,     7,     8,     9,    10,    13,
      16,    18,    23,    24,    25,    26,    16,    24,    24,     3,
       7,     8,     9,    10,    12,    13,    27,    28,    29,     0,
       0,    11,    12,    13,    14,    15,    24,    30,    31,    17,
       3,     3,    19,    20,    24,    24,    24,    24,    24,    17,
      20,    29,    24
  };

  const signed char
  Parser::yyr1_[] =
  {
       0,    22,    23,    24,    24,    24,    24,    24,    24,    24,
      25,    25,    25,    25,    25,    25,    25,    25,    25,    25,
      26,    27,    27,    28,    28,    29,    29,    29,    29,    29,
      29,    29,    30,    30,    31,    31
  };

  const signed char
  Parser::yyr2_[] =
  {
       0,     2,     2,     3,     3,     3,     3,     3,     2,     1,
       1,     1,     1,     1,     1,     1,     1,     4,     3,     1,
       3,     0,     1,     1,     3,     1,     2,     2,     1,     1,
       1,     1,     0,     1,     1,     3
  };




#if YYDEBUG
  const unsigned char
  Parser::yyrline_[] =
  {
       0,    67,    67,    73,    76,    79,    82,    85,    91,    94,
      98,   101,   107,   113,   119,   125,   128,   131,   134,   135,
     144,   150,   151,   155,   159,   163,   164,   171,   172,   173,
     174,   175,   179,   180,   184,   188
  };

  void
  Parser::yy_stack_print_ () const
  {
    *yycdebug_ << "Stack now";
    for (stack_type::const_iterator
           i = yystack_.begin (),
           i_end = yystack_.end ();
         i != i_end; ++i)
      *yycdebug_ << ' ' << int (i->state);
    *yycdebug_ << '\n';
  }

  void
  Parser::yy_reduce_print_ (int yyrule) const
  {
    int yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    // Print the symbols being reduced, and their result.
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1
               << " (line " << yylno << "):\n";
    // The symbols being reduced.
    for (int yyi = 0; yyi < yynrhs; yyi++)
      YY_SYMBOL_PRINT ("   $" << yyi + 1 << " =",
                       yystack_[(yynrhs) - (yyi + 1)]);
  }
#endif // YYDEBUG


#line 6 "expression.y"
} } // reindexer::expr_yy
#line 1297 "expression_yy.cc"

#line 191 "expression.y"


void reindexer::expr_yy::Parser::error(const std::string& msg) {
	ctx.ThrowError(msg);
}

void reindexer::expr_yy::Parser::report_syntax_error(const context& yyctx) const {
	const auto& la = yyctx.lookahead();
	std::string tok;
	switch (la.kind()) {
		case symbol_kind::S_YYEMPTY:
		case symbol_kind::S_YYEOF:
			tok = "end of expression";
			break;
		case symbol_kind::S_NAME:
		case symbol_kind::S_QUOTED_NAME:
		case symbol_kind::S_FUNCTION:
		case symbol_kind::S_STRING:
			tok = la.value.as<std::string>();
			break;
		case symbol_kind::S_NUMBER:
			tok = std::string{la.value.as<reindexer::Variant>().As<std::string>()};
			break;
		default:
			tok = la.name();
			break;
	}
	ctx.ThrowError("Unexpected token in expression: '" + tok + "'");
}

#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ >= 15
#pragma GCC diagnostic pop
#endif
// NOLINTEND
