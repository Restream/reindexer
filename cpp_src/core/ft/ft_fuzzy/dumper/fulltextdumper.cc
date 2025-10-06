#include "fulltextdumper.h"
#include <chrono>
#include <fstream>
#include <memory>
#include <thread>
#include "estl/lock.h"

namespace search_engine {
using std::chrono::seconds;

FullTextDumper& FullTextDumper::Init() {
	static FullTextDumper dumper;
	return dumper;
}

void FullTextDumper::LogFinalData(const reindexer::LocalQueryResults& result) {
	if (!std::getenv(env.c_str())) {
		return;
	}

	startThread();
	std::vector<std::string> tmp_buffer;
	tmp_buffer.push_back("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	tmp_buffer.push_back("Returned ids: ");
	for (const auto& it : result.Items()) {
		const auto& res = it.GetItemRef();
		tmp_buffer.push_back("id: " + std::to_string(res.Id()) + " | lsn: " + std::to_string(int64_t(res.Value().GetLSN())));
	}
	tmp_buffer.push_back("_______________________________________");

	reindexer::lock_guard lk(cv_m);
	buffer_.insert(buffer_.end(), tmp_buffer.begin(), tmp_buffer.end());
	new_info_ = true;
}
void FullTextDumper::Log(const std::string& data) {
	if (!std::getenv(env.c_str())) {
		return;
	}

	startThread();
	reindexer::lock_guard lk(cv_m);
	buffer_.push_back(data);
	new_info_ = true;
}

void FullTextDumper::AddResultData(const std::string& reqest) {
	if (!std::getenv(env.c_str())) {
		return;
	}

	startThread();
	std::vector<std::string> tmp_buffer;
	tmp_buffer.push_back("_______________________________________");
	tmp_buffer.push_back("New full test reqest: " + reqest);

	tmp_buffer.push_back("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

	reindexer::lock_guard lk(cv_m);
	buffer_.insert(buffer_.end(), tmp_buffer.begin(), tmp_buffer.end());
	new_info_ = true;
}

void FullTextDumper::startThread() {
	if (writer_ && !stoped_) {
		return;
	} else if (writer_ && stoped_) {
		cv.notify_all();
		writer_->join();
		writer_.reset();
		reindexer::lock_guard lk(cv_m);
		buffer_.clear();
	}

	if (!std::getenv(env.c_str())) {
		return;
	}

	stoped_ = false;
	writer_ = std::make_shared<std::thread>(&FullTextDumper::writeToFile, this);
}

void FullTextDumper::writeToFile() {
	while (!stoped_) {
		if (new_info_) {
			size_t size = 1;
			size_t counter = 0;

			std::string data;
			std::ofstream file(file_path, std::ios::app);

			while (size != 0 && file.is_open()) {
				{
					reindexer::lock_guard lk(cv_m);
					data = buffer_.front();
					buffer_.pop_front();
					size = buffer_.size();
				}
				counter++;
				file << data << "\n";
				if (counter % 10 == 0) {
					file.flush();
				}
			}
			file.close();
		}
		if (stoped_ || !std::getenv(env.c_str())) {
			return;
		}

		reindexer::unique_lock lk(cv_m);
		if (cv.wait_for(lk, seconds(write_timeout_seconds), [this] { return stoped_.load(); })) {
			return;
		};
	}
}

FullTextDumper::~FullTextDumper() {
	if (writer_) {
		stoped_ = true;
		cv.notify_all();
		writer_->join();
	}

	// static class destructor - nothing conflicrts here
	if (buffer_.empty()) {
		return;
	}

	std::ofstream file(file_path, std::ios::app);
	if (!file.is_open()) {
		return;
	}
	for (const auto& data : buffer_) {
		file << data << "\n";
	}
}

}  // namespace search_engine
