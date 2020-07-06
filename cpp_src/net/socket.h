#pragma once

#include <stdlib.h>
#include "estl/chunk_buf.h"
#include "tools/ssize_t.h"

struct addrinfo;
namespace reindexer {
namespace net {

class socket {
public:
	socket(const socket &other) = default;
	socket &operator=(const socket &other) = default;
	socket(int fd = -1) : fd_(fd) {}

	int bind(string_view addr);
	int connect(string_view addr);
	socket accept();
	int listen(int backlog);
	ssize_t recv(span<char> buf);
	ssize_t send(const span<char> buf);
	ssize_t send(span<chunk> chunks);
	int close();
	std::string addr() const;

	int set_nonblock();
	int set_nodelay();
	int fd() { return fd_; }
	bool valid() { return fd_ >= 0; }

	static int last_error();
	static bool would_block(int error);

protected:
	int create(string_view addr, struct addrinfo **pres);

	int fd_;
};
}  // namespace net
}  // namespace reindexer
