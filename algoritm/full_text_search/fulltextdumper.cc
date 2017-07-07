#include "fulltextdumper.h"
#include <chrono>
#include <fstream>
#include <memory>
#include <thread>

namespace search_engine {
using std::this_thread::sleep_for;
using std::chrono::seconds;
using std::make_shared;

FullTextDumper& FullTextDumper::Init() {
	static FullTextDumper dumper;
	return dumper;
}

void FullTextDumper::LogFinalData(const reindexer::QueryResults& result) {
	if (!std::getenv(env.c_str())) return;

	startThread();
	vector<string> tmp_buffer;
	tmp_buffer.push_back("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
	tmp_buffer.push_back("Returned ids: ");
	for (auto res : result) {
		tmp_buffer.push_back("id: " + std::to_string(res.id) + " | ver: " + std::to_string(res.version));
	}
	tmp_buffer.push_back("_______________________________________");

	lock_guard<mutex> lk(cv_m);
	buffer_.insert(buffer_.end(), tmp_buffer.begin(), tmp_buffer.end());
	new_info_ = true;
}
void FullTextDumper::Log(const std::string& data) {
	if (!std::getenv(env.c_str())) return;

	startThread();
	lock_guard<mutex> lk(cv_m);
	buffer_.push_back(data);
	new_info_ = true;
}

void FullTextDumper::AddResultData(const string& reqest, BaseHolder::SearchTypePtr result, const IdVirtualizer& id_virtualizer) {
	if (!std::getenv(env.c_str())) return;

	startThread();
	vector<string> tmp_buffer;
	tmp_buffer.push_back("_______________________________________");
	tmp_buffer.push_back("New full test reqest: " + reqest);

	for (auto res : *result.get()) {
		tmp_buffer.push_back("find: " + id_virtualizer.GetDataByVirtualId(res.first));
		tmp_buffer.push_back("procent: " + std::to_string(res.second));
		tmp_buffer.push_back("IDs: ");
		for (auto& val : *id_virtualizer.GetByVirtualId(res.first).get()) {
			tmp_buffer.push_back(std::to_string(val));
		}
	}
	tmp_buffer.push_back("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

	lock_guard<mutex> lk(cv_m);
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
		lock_guard<mutex> lk(cv_m);
		buffer_.clear();
	}

	if (!std::getenv(env.c_str())) {
		return;
	}

	stoped_ = false;
	writer_ = make_shared<thread>(&FullTextDumper::writeToFile, this);
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
					lock_guard<mutex> lk(cv_m);
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
		if (stoped_ || !std::getenv(env.c_str())) return;

		std::unique_lock<std::mutex> lk(cv_m);
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
	if (buffer_.empty()) return;

	std::ofstream file(file_path, std::ios::app);
	if (!file.is_open()) return;
	for (auto data : buffer_) {
		file << data << "\n";
	}
}

}  // namespace search_engine
