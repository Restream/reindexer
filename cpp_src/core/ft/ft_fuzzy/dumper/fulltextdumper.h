#ifndef FULLTEXTDUMPER_H
#define FULLTEXTDUMPER_H
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <thread>
#include "core/queryresults/queryresults.h"

namespace search_engine {

class FullTextDumper {
public:
	static FullTextDumper& Init();

	void Log(const std::string& data);

	void LogFinalData(const reindexer::LocalQueryResults& result);
	void AddResultData(const std::string& reqest);

private:
	void startThread();

	FullTextDumper(const FullTextDumper&) = delete;
	FullTextDumper& operator=(const FullTextDumper&) = delete;
	FullTextDumper() : new_info_(false), stoped_(false) {}

	void writeToFile();

	~FullTextDumper();

	std::deque<std::string> buffer_;

	std::atomic_bool new_info_;
	std::atomic_bool stoped_;
	std::shared_ptr<std::thread> writer_;
	std::condition_variable cv;
	std::mutex cv_m;

	const size_t write_timeout_seconds = 5;
	const std::string file_path = "/tmp/reindexer_full_text.log";
	const std::string env = "LOG_REINDEXER_FULLTEXT";
};

}  // namespace search_engine

#endif	// FULLTEXTDUMPER_H
