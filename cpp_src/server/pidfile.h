#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
#include <string>

#include "tools/errors.h"

namespace reindexer_server {

class [[nodiscard]] PidFile {
public:
	PidFile(const std::string& name = std::string(), pid_t pid = -1) : file_(-1) {
		if (!name.empty()) {
			Open(name, pid);
		}
	}
	~PidFile() {
		if (IsOpen()) {
			Close();
		}
	}

	bool IsOpen() const { return file_ != -1; }

	bool Open(const std::string& name, pid_t pid = -1) {
		if (IsOpen() || name.empty()) {
			return false;
		}
		// open file
		int fd = ::open(name.c_str(), O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP);
		if (fd == -1) {
			error_ = reindexer::Error(errLogic, "Could not create PID file `{}`. Reason: {}", name.c_str(), strerror(errno));
			return false;
		}
		// TODO: is it so very necessary?
		if (lockf(fd, F_TLOCK, 0) < 0) {
			error_ = reindexer::Error(errLogic, "Could not create PID file `{}`. Reason: {}", name.c_str(), strerror(errno));
			return false;
		}
		// get PID value and convert it to string
		if (pid == -1) {
			pid = ::getpid();
		}
		std::string buf = fmt::format("{}\n", pid);
		// write PID to file
		size_t rc = static_cast<size_t>(::write(fd, buf.c_str(), buf.size()));
		if (rc != buf.size()) {
			error_ = reindexer::Error(errLogic, "Could not create PID file `{}`. Reason: {}", name.c_str(), strerror(errno));
			::close(fd);
			return false;
		}
		fname_ = name;
		file_ = fd;
		return true;
	}

	void Close() {
		if (file_ != -1) {
			::close(file_);
			file_ = -1;
			::unlink(fname_.c_str());
		}
		if (!fname_.empty()) {
			fname_.clear();
		}
	}

	const char* Name() const { return fname_.c_str(); }

	reindexer::Error Status() { return error_; }

private:
	PidFile(const PidFile&);
	PidFile& operator=(const PidFile&);

private:
	int file_;
	std::string fname_;
	reindexer::Error error_;
};	// class <pidfile>

}  // namespace reindexer_server
