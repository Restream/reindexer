#ifndef FULLTEXTDUMPER_H
#define FULLTEXTDUMPER_H
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <thread>
#include "core/ft/ft_fuzzy/dataholder/basebuildedholder.h"
#include "core/query/queryresults.h"

namespace search_engine {
using std::thread;
using std::atomic_bool;
using std::condition_variable;
using std::mutex;
using std::vector;
using std::string;
using std::lock_guard;
using std::deque;

class FullTextDumper {
public:
	static FullTextDumper& Init();

	void Log(const std::string& data);

	void LogFinalData(const reindexer::QueryResults& result);
	void AddResultData(const std::string& reqest);

private:
	void startThread();

	FullTextDumper(const FullTextDumper&) = delete;
	FullTextDumper& operator=(const FullTextDumper&) = delete;
	FullTextDumper() : new_info_(false), stoped_(false) {}

	void writeToFile();

	~FullTextDumper();

	deque<string> buffer_;

	atomic_bool new_info_;
	atomic_bool stoped_;
	std::shared_ptr<thread> writer_;
	condition_variable cv;
	mutex cv_m;

	const size_t write_timeout_seconds = 5;
	const string file_path = "/tmp/reindexer_full_text.log";
	const string env = "LOG_REINDEXER_FULLTEXT";
};

}  // namespace search_engine

#endif  // FULLTEXTDUMPER_H
