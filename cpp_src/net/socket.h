#pragma once

#include <stdlib.h>
#include "estl/chunk_buf.h"
#include "tools/ssize_t.h"

struct addrinfo;
namespace reindexer {
namespace net {

class socket {
public:
	socket(const socket &other) : fd_(other.fd_) {}
	socket(int fd = -1) : fd_(fd) {}

	int bind(const char *addr);
	int connect(const char *addr);
	socket accept();
	int listen(int backlog);
	ssize_t recv(char *buf, size_t len);
	ssize_t send(const char *buf, size_t len);
	ssize_t send(span<chunk> chunks);
	int close();

	int set_nonblock();
	int set_nodelay();
	int fd() { return fd_; }
	bool valid() { return fd_ >= 0; }

	static int last_error();
	static bool would_block(int error);

protected:
	int create(const char *addr, struct addrinfo **pres);

	int fd_;
};
}  // namespace net
}  // namespace reindexer
