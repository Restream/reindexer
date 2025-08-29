#include <gtest/gtest-param-test.h>
#include "ft_api.h"

using namespace std::string_view_literals;

class [[nodiscard]] FTSynonymsApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_synonyms_default_namespace"; }
};

TEST_P(FTSynonymsApi, CompositeRankWithSynonyms) {
	auto cfg = GetDefaultConfig();
	cfg.synonyms = {{{"word"}, {"слово"}}};
	Init(cfg);
	Add("word"sv, "слово"sv);
	Add("world"sv, "world"sv);

	// rank of synonym is higher
	CheckAllPermutations("@", {"ft1^0.5", "ft2^2"}, " word~", {{"!word!", "!слово!"}, {"!world!", "!world!"}}, true, ", ");
}

TEST_P(FTSynonymsApi, SelectWithMultSynonymArea) {
	reindexer::FtFastConfig config = GetDefaultConfig();
	// hyponyms as synonyms
	config.synonyms = {{{"digit", "one", "two", "three", "big number"}, {"digit", "one", "two", "big number"}},
					   {{"animal", "cat", "dog", "lion"}, {"animal", "cat", "dog", "lion"}}};
	config.maxAreasInDoc = 100;
	Init(config);

	Add("digit cat empty one animal"sv);

	CheckResults(R"s("big number animal")s", {}, false, true);	// Result is incorrect. Has to be fixed in issue #1393
	CheckResults(R"s("digit animal")s", {{"!digit cat! empty !one animal!", ""}}, false, true);
	CheckResults(R"s("two lion")s", {{"!digit cat! empty !one animal!", ""}}, false, true);
}

TEST_P(FTSynonymsApi, SelectSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"лыв", "лав"}, {"love"}}, {{"лар", "hate"}, {"rex", "looove"}}};
	Init(ftCfg);

	Add("nm1"sv, "test"sv, "love rex"sv);
	Add("nm1"sv, "test"sv, "no looove"sv);
	Add("nm1"sv, "test"sv, "no match"sv);

	CheckAllPermutations("", {"лыв"}, "", {{"test", "!love! rex"}});
	CheckAllPermutations("", {"hate"}, "", {{"test", "love !rex!"}, {"test", "no !looove!"}});
}

TEST_P(FTSynonymsApi, SelectMultiwordSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"whole world", "UN", "United Nations"},
					   {"UN", "ООН", "целый мир", "планета", "генеральная ассамблея организации объединенных наций"}},
					  {{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1"sv, "whole world"sv, "test"sv);
	Add("nm1"sv, "world whole"sv, "test"sv);
	Add("nm1"sv, "whole"sv, "world"sv);
	Add("nm1"sv, "world"sv, "test"sv);
	Add("nm1"sv, "whole"sv, "test"sv);
	Add("nm1"sv, "целый мир"sv, "test"sv);
	Add("nm1"sv, "целый"sv, "мир"sv);
	Add("nm1"sv, "целый"sv, "test"sv);
	Add("nm1"sv, "мир"sv, "test"sv);
	Add("nm1"sv, "планета"sv, "test"sv);
	Add("nm1"sv, "генеральная ассамблея организации объединенных наций"sv, "test"sv);
	Add("nm1"sv, "ассамблея генеральная наций объединенных организации"sv, "test"sv);
	Add("nm1"sv, "генеральная прегенеральная ассамблея"sv, "организации объединенных свободных наций"sv);
	Add("nm1"sv, "генеральная прегенеральная "sv, "организации объединенных свободных наций"sv);
	Add("nm1"sv, "UN"sv, "UN"sv);

	Add("nm1"sv, "word"sv, "test"sv);
	Add("nm1"sv, "test"sv, "word"sv);
	Add("nm1"sv, "word"sv, "слово"sv);
	Add("nm1"sv, "word"sv, "одно"sv);
	Add("nm1"sv, "слово"sv, "test"sv);
	Add("nm1"sv, "слово всего лишь одно"sv, "test"sv);
	Add("nm1"sv, "одно"sv, "test"sv);
	Add("nm1"sv, "слово"sv, "одно"sv);
	Add("nm1"sv, "слово одно"sv, "test"sv);
	Add("nm1"sv, "одно слово"sv, "word"sv);

	CheckAllPermutations("", {"world"}, "",
						 {{"whole !world!", "test"}, {"!world! whole", "test"}, {"whole", "!world!"}, {"!world!", "test"}});

	CheckAllPermutations("", {"whole", "world"}, "",
						 {{"!whole world!", "test"},
						  {"!world whole!", "test"},
						  {"!whole!", "!world!"},
						  {"!world!", "test"},
						  {"!whole!", "test"},
						  {"!целый мир!", "test"},
						  {"!целый!", "!мир!"},
						  {"!планета!", "test"},
						  {"!генеральная ассамблея организации объединенных наций!", "test"},
						  {"!ассамблея генеральная наций объединенных организации!", "test"},
						  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
						  {"!UN!", "!UN!"}});

	CheckAllPermutations("", {"UN"}, "",
						 {{"!целый мир!", "test"},
						  {"!целый!", "!мир!"},
						  {"!планета!", "test"},
						  {"!генеральная ассамблея организации объединенных наций!", "test"},
						  {"!ассамблея генеральная наций объединенных организации!", "test"},
						  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
						  {"!UN!", "!UN!"}});

	CheckAllPermutations("", {"United", "+Nations"}, "",
						 {{"!целый мир!", "test"},
						  {"!целый!", "!мир!"},
						  {"!планета!", "test"},
						  {"!генеральная ассамблея организации объединенных наций!", "test"},
						  {"!ассамблея генеральная наций объединенных организации!", "test"},
						  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
						  {"!UN!", "!UN!"}});

	CheckAllPermutations("", {"целый", "мир"}, "", {{"!целый мир!", "test"}, {"!целый!", "test"}, {"!мир!", "test"}, {"!целый!", "!мир!"}});

	CheckAllPermutations("", {"ООН"}, "", {});

	CheckAllPermutations("", {"word"}, "",
						 {{"!word!", "test"},
						  {"test", "!word!"},
						  {"!word!", "слово"},
						  {"!word!", "одно"},
						  {"!слово! всего лишь !одно!", "test"},
						  {"!слово!", "!одно!"},
						  {"!слово одно!", "test"},
						  {"!одно слово!", "!word!"}});
}

// issue #715
TEST_P(FTSynonymsApi, SelectMultiwordSynonyms2) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"черный"}, {"серый космос"}}};
	Init(ftCfg);

	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 черный"sv, "SAMSUNG"sv);
	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 серый"sv, "SAMSUNG"sv);
	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 красный"sv, "SAMSUNG"sv);
	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 серый космос"sv, "SAMSUNG"sv);

	// Check all combinations of "+samsung" and "+galaxy" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 черный", "!SAMSUNG!"},
						  {"Смартфон !SAMSUNG Galaxy! S20 серый", "!SAMSUNG!"},
						  {"Смартфон !SAMSUNG Galaxy! S20 красный", "!SAMSUNG!"},
						  {"Смартфон !SAMSUNG Galaxy! S20 серый космос", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy" and "+серый" in request
	CheckAllPermutations(
		"@ft1 ", {"+samsung", "+galaxy", "+серый"}, "",
		{{"Смартфон !SAMSUNG Galaxy! S20 !серый!", "!SAMSUNG!"}, {"Смартфон !SAMSUNG Galaxy! S20 !серый! космос", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+серый" and "+космос" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+серый", "+космос"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy" and "+черный" in request
	CheckAllPermutations(
		"@ft1 ", {"+samsung", "+galaxy", "+черный"}, "",
		{{"Смартфон !SAMSUNG Galaxy! S20 !черный!", "!SAMSUNG!"}, {"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+серый" and "+серый" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+серый"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+черный" and "+космос" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+космос"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+черный" and "+somthing" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+something"}, "", {});
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+черный", "+черный", "+something"}, "", {});
}

TEST_P(FTSynonymsApi, SelectWithMinusWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word", "several lexems"}, {"слово", "сколькото лексем"}}};
	Init(ftCfg);

	Add("nm1"sv, "word"sv, "test"sv);
	Add("nm1"sv, "several lexems"sv, "test"sv);
	Add("nm1"sv, "слово"sv, "test"sv);
	Add("nm1"sv, "сколькото лексем"sv, "test"sv);

	CheckAllPermutations("", {"test", "word"}, "",
						 {{"!word!", "!test!"}, {"several lexems", "!test!"}, {"!слово!", "!test!"}, {"!сколькото лексем!", "!test!"}});
	// Don't use synonyms
	CheckAllPermutations("", {"test", "-word"}, "", {{"several lexems", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
	CheckAllPermutations("", {"test", "several", "lexems"}, "",
						 {{"word", "!test!"}, {"!several lexems!", "!test!"}, {"!слово!", "!test!"}, {"!сколькото лексем!", "!test!"}});
	// Don't use synonyms
	CheckAllPermutations("", {"test", "several", "-lexems"}, "", {{"word", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
	// Don't use synonyms
	CheckAllPermutations("", {"test", "-several", "lexems"}, "", {{"word", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
}

// issue #627
TEST_P(FTSynonymsApi, SelectMultiwordSynonymsWithExtraWords) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {
		{{"бронестекло", "защитное стекло", "бронированное стекло"}, {"бронестекло", "защитное стекло", "бронированное стекло"}}};
	Init(ftCfg);

	Add("nm1"sv, "защитное стекло для экрана samsung galaxy"sv, "test"sv);
	Add("nm1"sv, "защитное стекло для экрана iphone"sv, "test"sv);
	Add("nm1"sv, "бронированное стекло для samsung galaxy"sv, "test"sv);
	Add("nm1"sv, "бронированное стекло для экрана iphone"sv, "test"sv);
	Add("nm1"sv, "бронестекло для экрана samsung galaxy"sv, "test"sv);
	Add("nm1"sv, "бронестекло для экрана iphone"sv, "test"sv);

	CheckAllPermutations("", {"бронестекло"}, "",
						 {{"!защитное стекло! для экрана samsung galaxy", "test"},
						  {"!защитное стекло! для экрана iphone", "test"},
						  {"!бронированное стекло! для samsung galaxy", "test"},
						  {"!бронированное стекло! для экрана iphone", "test"},
						  {"!бронестекло! для экрана samsung galaxy", "test"},
						  {"!бронестекло! для экрана iphone", "test"}});

	CheckAllPermutations("", {"+бронестекло", "+iphone"}, "",
						 {{"!защитное стекло! для экрана !iphone!", "test"},
						  {"!бронированное стекло! для экрана !iphone!", "test"},
						  {"!бронестекло! для экрана !iphone!", "test"}});

	CheckAllPermutations("", {"+galaxy", "+бронестекло", "+samsung"}, "",
						 {{"!защитное стекло! для экрана !samsung galaxy!", "test"},
						  {"!бронированное стекло! для !samsung galaxy!", "test"},
						  {"!бронестекло! для экрана !samsung galaxy!", "test"}});

	CheckAllPermutations("", {"+galaxy", "+бронестекло", "экрана", "+samsung"}, "",
						 {{"!защитное стекло! для !экрана samsung galaxy!", "test"},
						  {"!бронированное стекло! для !samsung galaxy!", "test"},
						  {"!бронестекло! для !экрана samsung galaxy!", "test"}});

	CheckAllPermutations("", {"+galaxy", "+бронестекло", "какоетослово", "+samsung"}, "",
						 {{"!защитное стекло! для экрана !samsung galaxy!", "test"},
						  {"!бронированное стекло! для !samsung galaxy!", "test"},
						  {"!бронестекло! для экрана !samsung galaxy!", "test"}});

	CheckAllPermutations("", {"+бронестекло", "+iphone", "+samsung"}, "", {});
}

TEST_P(FTSynonymsApi, ChangeSynonymsCfg) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	Add("nm1"sv, "UN"sv, "test"sv);
	Add("nm1"sv, "United Nations"sv, "test"sv);
	Add("nm1"sv, "ООН"sv, "test"sv);
	Add("nm1"sv, "организация объединенных наций"sv, "test"sv);

	Add("nm1"sv, "word"sv, "test"sv);
	Add("nm1"sv, "several lexems"sv, "test"sv);
	Add("nm1"sv, "слово"sv, "test"sv);
	Add("nm1"sv, "сколькото лексем"sv, "test"sv);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "", {{"!United Nations!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "", {{"!several lexems!", "test"}});

	// Add synonyms
	ftCfg.synonyms = {{{"UN", "United Nations"}, {"ООН", "организация объединенных наций"}}};
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}, {"!ООН!", "test"}, {"!организация объединенных наций!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "",
						 {{"!United Nations!", "test"}, {"!ООН!", "test"}, {"!организация объединенных наций!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "", {{"!several lexems!", "test"}});

	// Change synonyms
	ftCfg.synonyms = {{{"word", "several lexems"}, {"слово", "сколькото лексем"}}};
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "", {{"!United Nations!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}, {"!слово!", "test"}, {"!сколькото лексем!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "",
						 {{"!several lexems!", "test"}, {"!слово!", "test"}, {"!сколькото лексем!", "test"}});

	// Remove synonyms
	ftCfg.synonyms.clear();
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "", {{"!United Nations!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "", {{"!several lexems!", "test"}});
}

TEST_P(FTSynonymsApi, SelectWithRelevanceBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}, {{"United Nations"}, {"ООН"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, ""sv, "ООН"sv);

	CheckAllPermutations("", {"word^2", "United^0.5", "Nations"}, "", {{"!одно слово!", ""}, {"", "!ООН!"}}, true);
	CheckAllPermutations("", {"word^0.5", "United^2", "Nations^0.5"}, "", {{"", "!ООН!"}, {"!одно слово!", ""}}, true);
}

TEST_P(FTSynonymsApi, SelectWithFieldsBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, "одно"sv, "слово"sv);
	Add("nm1"sv, ""sv, "одно слово"sv);

	CheckAllPermutations("@", {"ft1^2", "ft2^0.5"}, " word", {{"!одно слово!", ""}, {"!одно!", "!слово!"}, {"", "!одно слово!"}}, true,
						 ", ");
	CheckAllPermutations("@", {"ft1^0.5", "ft2^2"}, " word", {{"", "!одно слово!"}, {"!одно!", "!слово!"}, {"!одно слово!", ""}}, true,
						 ", ");
}

TEST_P(FTSynonymsApi, SelectWithFieldsListWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, "одно"sv, "слово"sv);
	Add("nm1"sv, ""sv, "одно слово"sv);

	CheckAllPermutations("", {"word"}, "", {{"!одно слово!", ""}, {"!одно!", "!слово!"}, {"", "!одно слово!"}});
	CheckAllPermutations("@ft1 ", {"word"}, "", {{"!одно слово!", ""}});
	CheckAllPermutations("@ft2 ", {"word"}, "", {{"", "!одно слово!"}});
}

INSTANTIATE_TEST_SUITE_P(, FTSynonymsApi,
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
