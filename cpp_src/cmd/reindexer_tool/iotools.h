#pragma once

#include <fstream>
#include <iostream>

#include "tools/errors.h"

namespace reindexer_tool {

using reindexer::Error;

class [[nodiscard]] Output {
public:
	Output() : isCout_(true), errState_(0) {}
	Output(const std::string& filePath) : f_(filePath, std::ios::out | std::ios::trunc), isCout_(filePath.empty()) {
		errState_ = (isCout_ || f_.is_open()) ? 0 : errno;
	}

	std::ostream& operator()() {
		if (!isCout_ && !f_.is_open()) {
			throw Error(errLogic, "{}", strerror(errState_));
		}
		return isCout_ ? std::cout : f_;
	}

	Error Status() const { return errState_ ? Error(errLogic, "{}", strerror(errState_)) : Error{}; }
	bool IsCout() const { return isCout_; }

private:
	std::ofstream f_;
	bool isCout_;
	int errState_;
};

class [[nodiscard]] LineParser {
public:
	LineParser(std::string_view line) : line_(line), cur_(line.data()) {}
	std::string_view NextToken() {
		while (*cur_ == ' ' || *cur_ == '\t') {
			cur_++;
		}

		const char* next = cur_;
		while (*next != ' ' && *next != '\t' && *next) {
			next++;
		}
		std::string_view ret(cur_, next - cur_);
		cur_ = next;
		while (*cur_ == ' ' || *cur_ == '\t') {
			cur_++;
		}
		return ret;
	}
	bool End() { return *cur_ == 0; }
	std::string_view CurPtr() { return std::string_view(cur_, line_.size() - (cur_ - line_.data())); }

protected:
	const std::string_view line_;
	const char* cur_;
};

}  // namespace reindexer_tool
