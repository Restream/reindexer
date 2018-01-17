

#include <time.h>
#include <iostream>
#include "ft_api.h"
#include "gtest/gtest.h"
#define tab std::setw(22) << std::left

using namespace reindexer;

int main(int argc, char* argv[]) {
	srand(time(NULL));
	allocdebug_init();
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
template <typename T>
std::string tostring(T const& value) {
	stringstream sstr;
	sstr << std::fixed << std::setprecision(2) << value;
	return sstr.str();
}
void Print(std::string first, std::string second, std::string third) {
	std::cout << tab << first << tab << second << tab << third << std::endl;
}

void PrintStat(Statistic text_stat, Statistic ft_stat, std::string name) {
	std::cout << std::endl;
	Print("", "START STATISTIC PRINT " + name, "");
	std::cout << std::endl;
	Print("", "Fast", "Fuzzy");
	Print("Default dtata size  ", tostring(text_stat.data_size) + "kB", tostring(ft_stat.data_size) + "kB");
	Print("Data count  ", tostring(text_stat.data_cnt), tostring(ft_stat.data_cnt));

	Print("Upsert data size  ", tostring(text_stat.after_upsert_size) + "kB", tostring(ft_stat.after_upsert_size) + "kB");
	Print("Upsert allocs  ", tostring(text_stat.upsert_alocs), tostring(ft_stat.upsert_alocs));

	Print("Commit data size  ", tostring(text_stat.after_commit_size) + "kB", tostring(ft_stat.after_commit_size) + "kB");
	Print("Commit coof  ", tostring(text_stat.after_commit_size / text_stat.data_size),
		  tostring(ft_stat.after_commit_size / text_stat.data_size));

	Print("Commit allocs  ", tostring(text_stat.commit_alocs), tostring(ft_stat.commit_alocs));
	std::cout << std::endl;
	Print("Upsert takes  ", tostring(text_stat.upsert_ms) + " mls", tostring(ft_stat.upsert_ms) + " mls");
	Print("Commit takes  ", tostring(text_stat.commit_ms) + " mls", tostring(ft_stat.commit_ms) + " mls");
	Print("Res per request  ", tostring(text_stat.res_per_request_cnt), tostring(ft_stat.res_per_request_cnt));
	Print("rps  ", tostring(text_stat.rps), tostring(ft_stat.rps));
	Print("Midle reqest time  ", tostring(text_stat.middle_request_time) + " micro sec",
		  tostring(ft_stat.middle_request_time) + " micro sec");

	std::cout << std::endl;
	Print("", "END STATISTIC PRINT " + name, "");
	std::cout << std::endl;
}

TEST_F(FTApi, CompareSearchMemorySmall) {
	Reinit("text");
	FillDataForTest(900);
	CalcRps(300);
	auto text_stat = GetStat();
	Reinit("fulltext");
	FillDataForTest(900);
	CalcRps(300);
	auto ft_stat = GetStat();
	PrintStat(text_stat, ft_stat, "Small size");
}
TEST_F(FTApi, CompareSearchMemoryBig) {
	/*  Reinit("text");
	/  FillDataForTest(1000);
	  CalcRps(1000);
	  auto text_stat = GetStat();
	  Reinit("fulltext");
	  FillDataForTest(1000);
	  CalcRps(1000);
	  auto ft_stat = GetStat();
	  PrintStat(text_stat, ft_stat, "Big size");*/
}
