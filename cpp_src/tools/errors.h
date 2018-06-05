#pragma once

#include <string>
#include "core/type_consts.h"

namespace reindexer {

using std::string;

class Error {
public:
	Error(int code = errOK);
	Error(int code, const string &what);
	Error(int code, const char *fmt, ...)
#ifndef _MSC_VER
		__attribute__((format(printf, 3, 4)))
#endif
		;

	const string &what() const;
	int code() const;
	bool ok() const { return code_ == errOK; }

	explicit operator bool() { return code_ != errOK; }

protected:
	int code_;
	string what_;
};

#ifdef NDEBUG
#define assertf(...) ((void)0)
#else
#define assertf(e, fmt, ...)                                                                         \
	if (!(e)) {                                                                                      \
		fprintf(stderr, "%s:%d: failed assertion '%s':\n" fmt, __FILE__, __LINE__, #e, __VA_ARGS__); \
		abort();                                                                                     \
	}
#endif

}  // namespace reindexer
