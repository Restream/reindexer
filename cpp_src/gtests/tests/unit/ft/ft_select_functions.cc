#include <gtest/gtest-param-test.h>
#include <algorithm>
#include "ft_api.h"
#include "gtests/tests/gtest_cout.h"

using namespace std::string_view_literals;

class [[nodiscard]] FTSelectFunctionsApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_select_fn_default_namespace"; }
};

class [[nodiscard]] FTSelectFunctionsApiF : public FTSelectFunctionsApi {
public:
	reindexer::FtFastConfig GetDefaultConfig(size_t fieldsCount = 2) override {
		reindexer::FtFastConfig cfg(fieldsCount);
		cfg.enableNumbersSearch = true;
		cfg.logLevel = 5;
		cfg.mergeLimit = 20000;
		cfg.maxStepSize = 100;
		cfg.optimization = reindexer::FtFastConfig::Optimization::Memory;
		return cfg;
	}
};

TEST_P(FTSelectFunctionsApi, SnippetN) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);
	Add("one two three gg three empty empty empty empty three"sv);

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,'{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Incorrect count of position arguments. Found 5 required 4.");
	}
	{  // check other case, error on not last argument
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,'{','{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token ','.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',pre_delim='}')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Argument already added 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',pre_delim='}',post_delim='!')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Argument already added 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Incorrect count of position arguments. Found 3 required 4.");
	}
	{  // check other case, error on not last argument
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>' , '</b>',5,pre_delim='{',post_delim='')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token ',', expecting positional argument (1 more positional args required)");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',pre_delim='}') g");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected character `g` after closing parenthesis.");
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token ',', expecting positional argument (2 more positional args required)");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token ','.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',,post_delim='}')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token ','.");
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>''n','</b>',5,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token 'n'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>'n,'</b>',5,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token 'n'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>'5,5,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token '5'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5'v',5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token 'v'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,\"pre_delim\"pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim= ='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token '='.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{'8)");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token '8'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim=)");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,not_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unknown argument name 'not_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,not_delim='{',pre_delim='}')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unknown argument name 'not_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: The closing parenthesis is required, but found `5`");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n{('<b>','</b>',5,5}");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: An open parenthesis is required, but found `{`");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<b>','</b>',5,5,"post_delim"="v"})#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token 'v'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n(<>,'</b>',5,5,"post_delim"='v'})#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Unexpected token '<>'");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<>','</b>',5,5,='v'})#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "snippet_n: Argument name is empty.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<>','</b>','5a',5))#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Invalid snippet param before - 5a is not a number");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<>','</b>',5,'5b'))#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_STREQ(err.what(), "Invalid snippet param after - 5b is not a number");
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim=',')");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		reindexer::WrSerializer wrSer;
		auto err = res.begin().GetJSON(wrSer, false);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(wrSer.Slice(), R"S({"ft1":", two <b>three</b> gg <b>three</b> empt ,mpty <b>three</b> "})S");
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>' , 		'</b>'
																											,5	,5 ,       pre_delim=','))S");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		reindexer::WrSerializer wrSer;
		auto err = res.begin().GetJSON(wrSer, false);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(wrSer.Slice(), R"S({"ft1":", two <b>three</b> gg <b>three</b> empt ,mpty <b>three</b> "})S");
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>','</b>',5,5,pre_delim=' g ', post_delim='h'))S");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":" g  two <b>three</b> gg <b>three</b> empth g mpty <b>three</b>h"})S");
		}
	}
	{
		reindexer::Query q("nm1");
		q.Select({"ft1"})
			.Where("ft1", CondEq, "three")
			.AddFunction(R"S(ft1=snippet_n('<b>','</b>','5',5,post_delim='h',pre_delim=' g '))S");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":" g  two <b>three</b> gg <b>three</b> empth g mpty <b>three</b>h"})S");
		}
	}
	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>','</b>',5,5,post_delim='h'))S");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":" two <b>three</b> gg <b>three</b> empthmpty <b>three</b>h"})S");
		}
	}
	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>','</b>',5,5,pre_delim='!'))S");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":"! two <b>three</b> gg <b>three</b> empt !mpty <b>three</b> "})S");
		}
	}
}

TEST_P(FTSelectFunctionsApi, SnippetNOthers) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	std::string_view s1 = "123456 one 789012"sv;
	[[maybe_unused]] auto [ss1, id1] = Add(s1);

	std::string_view s2 = "123456 one 789 one 987654321"sv;
	[[maybe_unused]] auto [ss2, id2] = Add(s2);

	std::string_view s3 = "123456 one two 789 one two 987654321"sv;
	[[maybe_unused]] auto [ss3, id3] = Add(s3);

	std::string_view s4 = "123456 one один два two 789 one один два two 987654321"sv;
	[[maybe_unused]] auto [ss4, id4] = Add(s4);

	auto check = [&](int index, const std::string& find, const std::string& fun, std::string_view answer) {
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, find).Where("id", CondEq, index).AddFunction(fun);
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), answer);
		}
	};
	check(id1, "one", R"S(ft1=snippet_n('<','>',5,5,pre_delim='[',post_delim=']',with_area=0))S", R"S({"ft1":"[3456 <one> 7890]"})S");
	check(id1, "one", R"S(ft1=snippet_n('<','>',5,5,pre_delim='[',post_delim=']'))S", R"S({"ft1":"[3456 <one> 7890]"})S");
	check(id2, "one", R"S(ft1=snippet_n('<','>',5,5,pre_delim='[',post_delim=']'))S", R"S({"ft1":"[3456 <one> 789 <one> 9876]"})S");
	check(id3, R"S("one two")S", R"S(ft1=snippet_n('<','>',2,2,pre_delim='[',post_delim=']'))S",
		  R"S({"ft1":"[6 <one two> 7][9 <one two> 9]"})S");
	check(id3, R"S("one two")S", R"S(ft1=snippet_n('<','>',2,2,pre_delim='[',post_delim=']',with_area=1))S",
		  R"S({"ft1":"[[5,16]6 <one two> 7][[17,28]9 <one two> 9]"})S");
	check(id4, R"S("one один два two")S", R"S(ft1=snippet_n('<','>',2,2,pre_delim='[',post_delim=']'))S",
		  R"S({"ft1":"[6 <one один два two> 7][9 <one один два two> 9]"})S");
	check(id4, R"S("one один два two")S", R"S(ft1=snippet_n('<','>',2,2,with_area=1,pre_delim='[',post_delim=']'))S",
		  R"S({"ft1":"[[5,25]6 <one один два two> 7][[26,46]9 <one один два two> 9]"})S");
}

TEST_P(FTSelectFunctionsApi, SnippetNOffset) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	std::string_view s1 = "one"sv;
	[[maybe_unused]] auto [ss1, id1] = Add(s1);

	std::string_view s2 = "один"sv;
	[[maybe_unused]] auto [ss2, id2] = Add(s2);

	std::string_view s3 = "asd one ghj"sv;
	[[maybe_unused]] auto [ss3, id3] = Add(s3);

	std::string_view s4 = "лмн один опр"sv;
	[[maybe_unused]] auto [ss4, id4] = Add(s4);

	std::string_view s5 = "лмн один опр один лмк"sv;
	[[maybe_unused]] auto [ss5, id5] = Add(s5);

	std::string_view s6 = "лмн опр jkl один"sv;
	[[maybe_unused]] auto [ss6, id6] = Add(s6);

	auto check = [&](int index, const std::string& find, const std::string& fun, std::string_view answer) {
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, find).Where("id", CondEq, index).AddFunction(fun);
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), answer);
		}
	};
	check(id1, "one", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[0,3]one "})S");
	check(id1, "one", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,3]one "})S");
	check(id2, "один", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[0,4]один "})S");
	check(id2, "один", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,4]один "})S");

	check(id3, "one", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[4,7]one "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',1,1,with_area=1))S", R"S({"ft1":"[3,8] one  "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',4,4,with_area=1))S", R"S({"ft1":"[0,11]asd one ghj "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,11]asd one ghj "})S");

	check(id6, "один", R"S(ft1=snippet_n('','',2,0,with_area=1))S", R"S({"ft1":"[10,16]l один "})S");
	check(id6, "один", R"S(ft1=snippet_n('','',2,2,with_area=1))S", R"S({"ft1":"[10,16]l один "})S");

	check(id4, "один", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[4,8]один "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',1,1,with_area=1))S", R"S({"ft1":"[3,9] один  "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',2,2,with_area=1))S", R"S({"ft1":"[2,10]н один о "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',4,4,with_area=1))S", R"S({"ft1":"[0,12]лмн один опр "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,12]лмн один опр "})S");

	check(id5, "один", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[4,8]один [13,17]один "})S");
	check(id5, "один", R"S(ft1=snippet_n('','',2,2,with_area=1))S", R"S({"ft1":"[2,10]н один о [11,19]р один л "})S");

	check(id5, "один", R"S(ft1=snippet_n('','',3,3,with_area=1))S", R"S({"ft1":"[1,20]мн один опр один лм "})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',post_delim='))',with_area=1))S",
		  R"S({"ft1":"(([2,10]н {!один} о))(([11,19]р {!один} л))"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',3,3,pre_delim='(',post_delim=')',with_area=1))S",
		  R"S({"ft1":"([1,20]мн {!один} опр {!один} лм)"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',post_delim='))',with_area=0))S",
		  R"S({"ft1":"((н {!один} о))((р {!один} л))"})S");

	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',with_area=1))S",
		  R"S({"ft1":"(([2,10]н {!один} о (([11,19]р {!один} л "})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',3,3,pre_delim='(',with_area=1))S", R"S({"ft1":"([1,20]мн {!один} опр {!один} лм "})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',with_area=0))S", R"S({"ft1":"((н {!один} о ((р {!один} л "})S");

	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,post_delim='))',with_area=1))S",
		  R"S({"ft1":"[2,10]н {!один} о))[11,19]р {!один} л))"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',3,3,post_delim=')',with_area=1))S", R"S({"ft1":"[1,20]мн {!один} опр {!один} лм)"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,post_delim='))',with_area=0))S", R"S({"ft1":"н {!один} о))р {!один} л))"})S");
}

TEST_P(FTSelectFunctionsApi, SnippetNBounds) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	std::string_view s1 = "one"sv;
	[[maybe_unused]] auto [ss1, id1] = Add(s1);

	std::string_view s3 = "as|d one g!hj"sv;
	[[maybe_unused]] auto [ss3, id3] = Add(s3);

	auto check = [&](int index, const std::string& find, const std::string& fun, std::string_view answer) {
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, find).Where("id", CondEq, index).AddFunction(fun);
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			auto err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), answer);
		}
	};
	check(id1, "one", R"S(ft1=snippet_n('','',0,0,with_area=1,left_bound='|',right_bound='|'))S", R"S({"ft1":"[0,3]one "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1,left_bound='|',right_bound='!'))S", R"S({"ft1":"[3,10]d one g "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',1,1,with_area=1,left_bound='|',right_bound='!'))S", R"S({"ft1":"[4,9] one  "})S");

	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1,right_bound='!'))S", R"S({"ft1":"[0,10]as|d one g "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',6,5,with_area=1,right_bound='!'))S", R"S({"ft1":"[0,10]as|d one g "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',4,5,with_area=1,right_bound='!'))S", R"S({"ft1":"[1,10]s|d one g "})S");

	check(id3, "one", R"S(ft1=snippet_n('','',2,5,with_area=1,left_bound='|'))S", R"S({"ft1":"[3,13]d one g!hj "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',2,6,with_area=1,left_bound='!'))S", R"S({"ft1":"[3,13]d one g!hj "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',2,4,with_area=1,left_bound='!'))S", R"S({"ft1":"[3,12]d one g!h "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1,left_bound='!',right_bound='|'))S", R"S({"ft1":"[0,13]as|d one g!hj "})S");
}

TEST_P(FTSelectFunctionsApi, RankAsField) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);
	Add("one two three gg three empty empty empty empty three"sv);

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").Select({"ft1", "rank()"});
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		reindexer::Expected<std::string> json = res.begin().GetJSON();
		EXPECT_TRUE(json.has_value()) << json.error().what();
		EXPECT_EQ(json.value(), R"#({"ft1":"one two three gg three empty empty empty empty three","rank()":93.0})#");
	}
}

#if !defined(REINDEX_WITH_TSAN)
TEST_F(FTSelectFunctionsApiF, TotalOrVids) {
	const unsigned int kItemCount = 140'000;
	auto ftCfg = GetDefaultConfig();
	ftCfg.mergeLimit = 1'000'000;
	ftCfg.maxAreasInDoc = 100000;
	Init(ftCfg);
	const std::vector<std::string_view> words = {"zero",	 "one",		"two",	   "three",		"four",		"five",	   "six",
												 "seven",	 "eight",	"nine",	   "ten",		"eleven",	"twelve",  "thirteen",
												 "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"};
	const std::vector<std::string_view> testWord = {"test", "mest", "nest", "sest", "vest", "best"};

	auto generateStr = [&](int num, unsigned int posW, unsigned int posT, bool adding) {
		std::string res = fmt::format("{} ", num), resSnippet = res;
		for (unsigned int i = 0; i < words.size(); i++) {
			if (i == posW) {
				res += fmt::format("{} empty ", testWord[posT]);
				resSnippet += fmt::format("!{}! empty ", testWord[posT]);
			} else {
				res.append(words[i]).append(" ");
				resSnippet.append(words[i]).append(" ");
			}
		}
		if (adding) {
			const unsigned addWord = std::rand() % 100;
			for (unsigned i = 0, pos = 0; i < addWord; i++, pos++) {
				pos %= words.size();
				res.append(words[pos]).append(" ");
				resSnippet.append(words[pos]).append(" ");
			}
		}
		const auto& w = testWord[std::rand() % testWord.size()];
		res += fmt::format("empty {} ", w);
		resSnippet += fmt::format("empty !{}! ", w);
		return std::make_pair(res, resSnippet);
	};

	unsigned posW = 0;
	unsigned posT = 0;
	std::set<std::string> resSet, resSelect;
	for (unsigned m = 0; m < kItemCount; m++, posW++, posT++) {
		posT %= testWord.size();
		posW %= words.size();

		auto [str, strSnippet] = generateStr(m, posW, posT, m % 2);
		resSet.insert(std::move(strSnippet));
		Add(str);
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft3", CondEq, "test~").AddFunction(R"(ft1=snippet(!,!,1000000,1000000,,))");
		auto res = rt.Select(q);
		EXPECT_EQ(res.Count(), kItemCount);
		for (auto& r : res) {
			auto item = r.GetItem();
			resSelect.insert(item["ft1"].As<std::string>());
		}
		if (resSelect != resSet) {
			EXPECT_TRUE(false) << "items are not equal";
			std::vector<std::string> diff;
			std::set_difference(resSelect.begin(), resSelect.end(), resSet.begin(), resSet.end(), std::back_inserter(diff));
			TestCout() << "diff count = " << diff.size() << std::endl;
			for (const auto& s : diff) {
				TestCout() << s << std::endl;
			}
		}
	}
}
#endif

INSTANTIATE_TEST_SUITE_P(, FTSelectFunctionsApi,
						 ::testing::Values(reindexer::FtFastConfig::Optimization::Memory, reindexer::FtFastConfig::Optimization::CPU),
						 [](const auto& info) {
							 switch (info.param) {
								 case reindexer::FtFastConfig::Optimization::Memory:
									 return "OptimizationByMemory";
								 case reindexer::FtFastConfig::Optimization::CPU:
									 return "OptimizationByCPU";
								 default:
									 assert(false);
									 std::abort();
							 }
						 });
