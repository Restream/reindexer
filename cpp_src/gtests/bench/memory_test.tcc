
#include <debug/allocdebug.h>
#include <boost/test/unit_test.hpp>
#include <ctime>
#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <thread>
#include <vector>
#include "algoritm/full_text_search/searchengine.h"
#include "core/reindexer.h"
#include "test_framework.h"
using namespace reindexer;
using std::string;
using std::map;
const string name = "name";
const string index_name = "index";

std::shared_ptr<Reindexer> rei;

double SelectClear(const std::set<std::string>& src_data, const std::vector<std::string>& search_data) {
	auto& test_fr = TestFrameWork::Init();

	search_engine::SearchEngine search;
	map<IdType, string> data;
	IdType counter = 0;
	double result_time = 0;

	for (auto& val : src_data) {
		data[counter] = val;
		counter++;
	}
	search.Rebuild(data);
	size_t limit = 100;
	counter = 0;
	for (int i = 0; i < limit; ++i) {
		auto word = search_data[test_fr.GetRandomNumber() % data.size()];
		result_time += test_fr.CalcMs([&]() { search.Search(word); });
		counter++;
		if (counter == limit) {
			return 1000 / (result_time / (double)limit);
		}
	}
	return 0;
}

double Select(std::vector<std::string>& data, size_t limit, TestContext::Ptr ctx, std::vector<QueryResults>& results) {
	auto& test_fr = TestFrameWork::Init();
	double result_time = 0;
	size_t counter = 0;

	for (int i = 0; i < limit; ++i) {
		auto word = data[test_fr.GetRandomNumber() % data.size()];

		result_time += test_fr.CalcMs([&]() { results.push_back(ctx->Select(word, name, index_name).Exec()); });
		counter++;
		if (counter == limit) {
			return 1000 / (result_time / (double)limit);
		}
	}
	return 0;
}

std::vector<std::string> LoadFromFile() {
	std::ifstream testFile("/home/andrey/RusS.txt", std::ios::binary);
	std::vector<std::string> myLines;
	std::copy(std::istream_iterator<std::string>(testFile), std::istream_iterator<std::string>(), std::back_inserter(myLines));
	return myLines;
}

std::string CreatePerphase(const std::vector<std::string>& words) {
	auto& test_fr = TestFrameWork::Init();
	size_t word_count = test_fr.GetRandomNumber() % 3 + 2;
	std::string result;
	for (int i = 0; i < word_count; ++i) {
		result += words[test_fr.GetRandomNumber() % words.size()] + " ";
	}
	return result;
}

void Commit() { rei->Commit(name); }

struct ptr_compare {
	bool operator()(const shared_ptr<std::string>& lhs, const shared_ptr<std::string>& rhs) const { return *lhs.get() < *rhs.get(); }
};

BOOST_AUTO_TEST_CASE(SIMPLE_TRASLIT_SELECT) {
	try {
		auto& test_fr = TestFrameWork::Init();
		auto cxt = test_fr.CreateEmptyContext();
		cxt->AddIndex(name, index_name, IndexNewFullText, true);
		double data_size;
		std::vector<std::string> words = LoadFromFile();

		std::set<std::string> perphases;

		size_t curent_mem = get_alloc_size();
		size_t perphases_mem = get_alloc_size();
		int counter = 0;
		while (counter < 10000) {
			perphases.insert(CreatePerphase(words));
			perphases_mem = get_alloc_size();
			size_t perphases_mem = get_alloc_size();
			counter++;
		}

		data_size = perphases_mem - curent_mem;

		int i = 0;
		size_t before_upsert = get_alloc_size();
		cxt->StartCommit(name);

		for (auto& perphas : perphases) {
			cxt->Add(perphas, index_name);
			cxt->Upsert();
			++i;
		}
		size_t before_com = get_alloc_size();
		size_t before_com_cnt = get_alloc_cnt();
		size_t before_com_cnt_total = get_alloc_cnt_total();

		size_t time = test_fr.CalcMs([&]() {
			cxt->Commit();
			cxt->Select(words[0], name, index_name).Exec();
		});
		size_t after_com_cnt = get_alloc_cnt();

		size_t after_com_cnt_total = get_alloc_cnt_total();

		size_t after_com = get_alloc_size();
		double mem = after_com - before_com;
		int limit = 10000;
		std::vector<QueryResults> results(limit);
		double select_time = Select(words, limit, cxt, results);
		mem /= (double)1024 * (double)1024;
		data_size /= (double)1024 * (double)1024;
		double cof = mem / data_size;
		double tm = time;
		uint64_t total_count = 0;
		for (auto& val : results) {
			total_count += val.size();
		}
		std::cout.precision(2);
		std::cout << std::fixed;
		// double select_time_clear = SelectClear(perphases, words);
		std::cout << "Source_Data_size " << data_size << "Mb" << std::endl;
		std::cout << "we_use " << mem << "Mb" << std::endl;
		std::cout << "coff " << cof << std::endl;
		std::cout << "commit takes " << tm << " miliseocnds" << std::endl;
		std::cout << "commit use TOTAL " << after_com_cnt_total - before_com_cnt_total << std::endl;
		std::cout << "commit use still " << after_com_cnt - before_com_cnt << std::endl;
		std::cout << "commit use DELETE " << after_com_cnt_total - before_com_cnt_total - (after_com_cnt - before_com_cnt) << std::endl;

		std::cout << "rps by reindexer " << select_time << std::endl;
		std::cout << "total result: " << total_count << std::endl;
		std::cout << "result per reqest : " << total_count / (double)limit << std::endl;

		// std::cout << "rps without REINEXER " << select_time_clear << std::endl;

	} catch (const std::exception& err) {
		std::cout << err.what() << std::endl;
	}
}
