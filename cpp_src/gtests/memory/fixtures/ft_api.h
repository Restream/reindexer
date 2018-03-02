#pragma once
#include <chrono>
#include <exception>
#include <fstream>
#include <limits>
#include "debug/allocdebug.h"
#include "reindexer_api.h"
#include "unordered_map"

using std::ifstream;
using std::unordered_map;
using namespace std;
using namespace reindexer;

struct Statistic {
	double data_size;
	double after_upsert_size;
	double after_commit_size;
	size_t upsert_alocs;
	size_t commit_alocs;
	double commit_ms;
	size_t data_cnt;

	double upsert_ms;
	double res_per_request_cnt;
	double middle_request_time;

	size_t rps;
};
class FTApi : public ReindexerApi {
public:
	void Reinit(std::string type) {
		all_perphase_.clear();
		std::vector<std::string> tmp;
		all_perphase_.swap(tmp);
		reindexer.reset(new Reindexer);

		CreateNamespace(default_namespace);

		DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK()},
												   IndexDeclaration{"ft1", type.c_str(), "string", IndexOpts()},
												   IndexDeclaration{"ft2", type.c_str(), "string", IndexOpts()},
												   IndexDeclaration{"ft1+ft2=target", type.c_str(), "composite", IndexOpts()}});
	}

	FTApi() {
		std::ifstream testFile("dict", std::ios::binary);
		if (!testFile.is_open()) {
			throw std::runtime_error("No dict file - test fails");
		}
		std::copy(std::istream_iterator<std::string>(testFile), std::istream_iterator<std::string>(), std::back_inserter(all_words_));
		std::vector<std::string> tmp;
		tmp.reserve(100001);
		for (int i = 0; i < 100000; ++i) {
			tmp.push_back(all_words_[i]);
		}
		all_words_.swap(tmp);
		testFile.close();
	}

	void FillDataForTest(size_t kb) {
		size_t total_size = 0;
		while (total_size <= kb * KB) {
			all_perphase_.push_back(CreatePerphase());
			total_size += all_perphase_.back().length();
		}
		stat.data_size = total_size / KB;
		size_t before_upsert = get_alloc_size();
		size_t before_upsert_cnt = get_alloc_cnt();
		size_t counter = 0;
		auto start = std::chrono::high_resolution_clock::now();
		for (auto &perphase : all_perphase_) {
			Item item = NewItem(default_namespace);
			item["id"] = int(counter);
			if (counter % 2) {
				item["ft1"] = perphase;
			} else {
				item["ft2"] = perphase;
			}
			Upsert(default_namespace, item);
			counter++;
		}

		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double, std::milli> fp_ms = end - start;
		stat.upsert_ms = fp_ms.count();
		stat.data_cnt = counter;
		stat.after_upsert_size = (get_alloc_size() - before_upsert) / KB;
		stat.upsert_alocs = get_alloc_cnt() - before_upsert_cnt;

		size_t before_commit = get_alloc_size();
		size_t before_commit_cnt = get_alloc_cnt();
		start = std::chrono::high_resolution_clock::now();
		Commit(default_namespace);
		Select(all_perphase_[0]);
		end = std::chrono::high_resolution_clock::now();
		stat.commit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

		stat.after_commit_size = (get_alloc_size() - before_commit) / KB;
		stat.commit_alocs = get_alloc_cnt() - before_commit_cnt;
	}

	void CalcRps(size_t count) {
		QueryResults res;
		auto start = std::chrono::high_resolution_clock::now();
		size_t total_size = 0;
		for (size_t i = 0; i < count; ++i) {
			res = Select(all_perphase_[i % all_perphase_.size()]);
			total_size += res.size();
		}
		auto end = std::chrono::high_resolution_clock::now();
		stat.res_per_request_cnt = total_size / double(count);
		std::chrono::duration<double, std::micro> fp_ms = end - start;
		stat.middle_request_time = fp_ms.count() / double(count);

		stat.rps = (count / (fp_ms.count() / double(1000000)));
	}
	Statistic GetStat() { return stat; }

	QueryResults Select(string word) {
		Query qr = Query(default_namespace).Where("target", CondEq, word);
		QueryResults res;
		reindexer->Select(qr, res);
		return res;
	}

protected:
	std::string CreatePerphase() {
		size_t word_count = rand() % 3 + 2;
		std::string result;
		for (size_t i = 0; i < word_count; ++i) {
			result += all_words_[rand() % all_words_.size()] + " ";
		}
		result.pop_back();
		return result;
	}
	double KB = 1024;

	double Mb = 1024 / 1024;
	std::vector<std::string> all_words_;
	std::vector<std::string> all_perphase_;
	Statistic stat;
};
