
#include "socket.h"
#include <assert.h>
#include <errno.h>
#include <memory.h>
#include <stdio.h>
#include <memory>
#include "estl/h_vector.h"
#include "tools/oscompat.h"

namespace reindexer {
namespace net {

int socket::bind(string_view addr) {
	struct addrinfo *results = nullptr;
	int ret = create(addr, &results);
	if (!ret) {
		if (::bind(fd_, results->ai_addr, results->ai_addrlen) != 0) {
			perror("bind error");
			close();
		}
	}
	if (results) {
		freeaddrinfo(results);
	}
	return ret;
}

int socket::connect(string_view addr) {
	struct addrinfo *results = nullptr;
	int ret = create(addr, &results);
	if (!ret) {
		if (::connect(fd_, results->ai_addr, results->ai_addrlen) != 0) {
			if (!would_block(last_error())) {
				perror("connect error");
				close();
			}
		}
	}
	if (results) {
		freeaddrinfo(results);
	}
	return ret;
}

int socket::listen(int backlog) {
#ifdef __linux__
	int enable = 1;

	if (setsockopt(fd_, SOL_TCP, TCP_DEFER_ACCEPT, &enable, sizeof(enable)) < 0) {
		perror("setsockopt(TCP_DEFER_ACCEPT) failed");
	}
	if (setsockopt(fd_, SOL_TCP, TCP_QUICKACK, &enable, sizeof(enable)) < 0) {
		perror("setsockopt(TCP_QUICKACK) failed");
	}
#endif
	return ::listen(fd_, backlog);
}

ssize_t socket::recv(span<char> buf) {
	//
	return ::recv(fd_, buf.data(), buf.size(), 0);
}
ssize_t socket::send(span<char> buf) {
	//
	return ::send(fd_, buf.data(), buf.size(), 0);
}

#ifdef _WIN32
ssize_t socket::send(span<chunk> chunks) {
	h_vector<WSABUF, 64> iov;
	iov.resize(chunks.size());

	for (unsigned i = 0; i < chunks.size(); i++) {
		iov[i].buf = reinterpret_cast<CHAR *>(chunks[i].data());
		iov[i].len = chunks[i].size();
	}
	DWORD numberOfBytesSent;
	int res = ::WSASend(SOCKET(fd_), iov.data(), iov.size(), &numberOfBytesSent, 0, NULL, NULL);

	return res == 0 ? numberOfBytesSent : -1;
}
#else
ssize_t socket::send(span<chunk> chunks) {
	h_vector<iovec, 64> iov;
	iov.resize(chunks.size());

	for (unsigned i = 0; i < chunks.size(); i++) {
		iov[i].iov_base = chunks[i].data();
		iov[i].iov_len = chunks[i].size();
	}
	return ::writev(fd_, iov.data(), iov.size());
}
#endif

int socket::close() {
	int fd = fd_;
	fd_ = -1;
#ifndef _WIN32
	return ::close(fd);
#else
	return ::closesocket(fd);
#endif
}

int socket::create(string_view addr, struct addrinfo **presults) {
	assert(!valid());

	struct addrinfo hints, *results = nullptr;
	memset(&hints, 0, sizeof(hints));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	*presults = nullptr;

	std::string saddr(addr);
	char *paddr = &saddr[0];

	char *pport = strchr(paddr, ':');
	if (pport == nullptr) {
		pport = paddr;
		paddr = nullptr;
	} else {
		*pport = 0;
		if (*paddr == 0) paddr = nullptr;
		pport++;
	}

	int ret = ::getaddrinfo(paddr, pport, &hints, &results);
	if (ret != 0) {
		fprintf(stderr, "getaddrinfo failed: %s\n", gai_strerror(ret));
		return -1;
	}
	assert(results != nullptr);
	*presults = results;

	if ((fd_ = ::socket(results->ai_family, results->ai_socktype, results->ai_protocol)) < 0) {
		perror("socket error");
		return -1;
	}
	set_nonblock();

	int enable = 1;
	if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char *>(&enable), sizeof(enable)) < 0) {
		perror("setsockopt(SO_REUSEADDR) failed");
	}

	set_nodelay();

	return 0;
}

std::string socket::addr() const {
	struct sockaddr_storage saddr;
	struct sockaddr *paddr = reinterpret_cast<sockaddr *>(&saddr);
	socklen_t len = sizeof(saddr);
	if (::getpeername(fd_, paddr, &len) != 0) {
		perror("getpeername");
		return {};
	}
	char buf[INET_ADDRSTRLEN] = {};
	getnameinfo(paddr, len, buf, INET_ADDRSTRLEN, NULL, 0, NI_NUMERICHOST);
	return buf;
}

socket socket::accept() {
	struct sockaddr client_addr;
	memset(&client_addr, 0, sizeof(client_addr));
	socklen_t client_len = sizeof(client_addr);

#ifdef __linux__
	socket client = ::accept4(fd_, &client_addr, &client_len, SOCK_NONBLOCK);

#else
	socket client = ::accept(fd_, &client_addr, &client_len);
	if (client.valid()) {
		client.set_nonblock();
	}
#endif
	if (client.valid()) {
		client.set_nodelay();
	}
	return client;
}

int socket::set_nonblock() {
#ifndef _WIN32
	return fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL, 0) | O_NONBLOCK);
#else
	u_long flag = 1;
	return ioctlsocket(fd_, FIONBIO, &flag);
#endif
}

int socket::set_nodelay() {
	int flag = 1;
	return setsockopt(fd_, SOL_TCP, TCP_NODELAY, reinterpret_cast<char *>(&flag), sizeof(flag));
}

int socket::last_error() {
#ifndef _WIN32
	return errno;
#else
	return WSAGetLastError();
#endif
}
bool socket::would_block(int error) {
#ifndef _WIN32
	return error == EAGAIN || error == EWOULDBLOCK || error == EINPROGRESS;
#else
	return error == EAGAIN || error == EWOULDBLOCK || error == WSAEWOULDBLOCK || error == EINPROGRESS;
#endif
}

#ifdef _WIN32
class __windows_ev_init {
public:
	__windows_ev_init() {
		WSADATA wsaData;
		WSAStartup(MAKEWORD(2, 2), &wsaData);
	}
} __windows_ev_init;
#endif

}  // namespace net
}  // namespace reindexer
