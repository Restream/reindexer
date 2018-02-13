#pragma once

#include <fstream>
#include <iostream>
#include <limits>

#include "core/query/queryresults.h"
#include "tools/errors.h"
#include "tools/serializer.h"

using std::ofstream;
using std::ostream;
using std::istream;
using std::string;
using std::numeric_limits;
using std::streamsize;
using reindexer::QueryResults;
using reindexer::WrSerializer;
using reindexer::Error;

class Output {
public:
	Output() : isCout_(true), errState_(0) {}
	Output(const string& filePath) : f_(filePath, std::ios::out | std::ios::trunc), isCout_(false | filePath.empty()) { errState_ = errno; }

	Output(const Output&) = default;
	Output& operator=(const Output&) = default;

	ostream& operator()() {
		if (!isCout_ && !f_.is_open()) throw Error(errLogic, strerror(errState_));
		return isCout_ ? std::cout : f_;
	}

	Error Status() const { return errState_ ? Error(errLogic, "%s", strerror(errState_)) : 0; }

private:
	ofstream f_;
	bool isCout_;
	int errState_;
};

class ProgressPrinter {
public:
	constexpr static size_t MaxValue() { return numeric_limits<size_t>::max(); }

	ProgressPrinter(size_t max = MaxValue(), std::ostream& o = std::cout) : max_(max), out_(o) {}

	void Reset(size_t max = MaxValue()) {
		max_ = max;
		current_ = 0;
	}

	void Show(size_t value = MaxValue()) {
		auto percent = static_cast<string::size_type>((value * 100) / max_);
		if (percent == current_ || percent > 100) return;

		current_ = percent;

		std::string bar;
		for (string::size_type i = 0; i < 50; i++) {
			if (i < (current_ / 2)) {
				bar.replace(i, 1, "=");
			} else if (i == (current_ / 2)) {
				bar.replace(i, 1, ">");
			} else {
				bar.replace(i, 1, " ");
			}
		}
		print(bar);
	}

	void Finish() {
		string bar(50, '=');
		current_ = 100;
		print(bar);
		out_ << std::endl;
	}

private:
	void print(string& bar) {
		out_ << "\r"
				"["
			 << bar << "] ";
		out_.width(3);
		out_ << current_ << "%     " << std::flush;
	}

private:
	size_t max_;
	size_t current_ = 0;
	ostream& out_;
};

ostream& operator<<(ostream& o, const QueryResults& r) {
	WrSerializer ser;
	size_t lastIdx = r.size() - 1;
	for (size_t i = 0; i < r.size(); i++) {
		ser.Reset();
		r.GetJSON(i, ser, false);
		char* begin = reinterpret_cast<char*>(ser.Buf());
		o.write(begin, ser.Len());
		o << (i == lastIdx ? "\n" : ",\n");
	}
	return o;
}

size_t GetStreamSize(istream& f) {
	size_t sz = 0;
	if (f) {
		f.ignore(numeric_limits<streamsize>::max());
		sz = f.gcount();
		f.clear();
		f.seekg(0, std::ios::beg);
	}
	return sz;
}

class LineParser {
public:
	LineParser(const string& line) : line_(line), cur_(line.c_str()){};
	string NextToken() {
		while (*cur_ == ' ' || *cur_ == '\t') cur_++;

		const char* next = cur_;
		while (*next != ' ' && *next != '\t' && *next) next++;
		string ret(cur_, next - cur_);
		cur_ = next;
		while (*cur_ == ' ' || *cur_ == '\t') cur_++;
		return ret;
	}
	const char* CurPtr() { return cur_; }

protected:
	const string& line_;
	const char* cur_;
};
