#pragma once

#include <fstream>
#include <iostream>

#include "tools/errors.h"
#include "tools/serializer.h"

namespace reindexer_tool {

using std::ofstream;
using std::ostream;
using std::istream;
using std::string;
using std::stringstream;
using std::numeric_limits;
using reindexer::string_view;
using reindexer::Error;
using reindexer::WrSerializer;

class Output {
public:
	Output() : isCout_(true), errState_(0) {}
	Output(const string& filePath) : f_(filePath, std::ios::out | std::ios::trunc), isCout_(false || filePath.empty()) {
		errState_ = (isCout_ || f_.is_open()) ? 0 : errno;
	}

	ostream& operator()() {
		if (!isCout_ && !f_.is_open()) throw Error(errLogic, "%s", strerror(errState_));
		return isCout_ ? std::cout : f_;
	}

	Error Status() const { return errState_ ? Error(errLogic, "%s", strerror(errState_)) : 0; }

private:
	ofstream f_;
	bool isCout_;
	int errState_;
};

class LineParser {
public:
	LineParser(const string& line) : line_(line), cur_(line.data()){};
	string_view NextToken() {
		while (*cur_ == ' ' || *cur_ == '\t') cur_++;

		const char* next = cur_;
		while (*next != ' ' && *next != '\t' && *next) next++;
		string_view ret(cur_, next - cur_);
		cur_ = next;
		while (*cur_ == ' ' || *cur_ == '\t') cur_++;
		return ret;
	}
	bool End() { return *cur_ == 0; }
	string_view CurPtr() { return string_view(cur_, line_.size() - (cur_ - line_.data())); }

protected:
	const string& line_;
	const char* cur_;
};

}  // namespace reindexer_tool
