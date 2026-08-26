#include "socket.h"
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <memory>
#include <numeric>
#include <string>
#include "estl/h_vector.h"
#include "tools/assertrx.h"
#include "tools/oscompat.h"

namespace reindexer {
namespace net {

#ifdef _WIN32
static int print_not_supported() {
	fprintf(stderr, "reindexer error: unix domain sockets are not supported on windows\n");
	return -1;
}
#endif	// _WIN32

int socket::connect(std::string_view addr, socket_domain t) {
	int ret = 0;
	type_ = t;
	if (domain() == socket_domain::tcp) {
		struct addrinfo* results = nullptr;
		ret = create(addr, &results);
		if (!ret) [[likely]] {
			assertrx(results != nullptr);
			if (::connect(fd_, results->ai_addr, results->ai_addrlen) != 0) [[unlikely]] {	// -V595
				if (!would_block(last_error())) [[unlikely]] {
					perror("connect error");
					std::ignore = close();
				}
				ret = -1;
			}
			if (ssl && ret != -1) {
				openssl::SSL_set_fd(*ssl, fd_);
				if (int res = openssl::SSL_connect(*ssl); res != 1) {
					ret = -1;
					int err = openssl::SSL_get_error(*ssl, res);
					if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
						perror("ssl connect error");
						std::ignore = close();
					}
				}
			}
		}
		if (results) [[likely]] {
			freeaddrinfo(results);
		}
	} else {
#ifdef _WIN32
		return print_not_supported();
#else	// _WIN32
		if (create(addr, nullptr) < 0) [[unlikely]] {
			return -1;
		}

		struct sockaddr_un address;
		address.sun_family = AF_UNIX;
		memcpy(address.sun_path, addr.data(), addr.size());
		address.sun_path[addr.size()] = 0;

		if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) != 0) [[unlikely]] {	// -V595
			if (!would_block(last_error())) [[unlikely]] {
				perror("connect error");
				std::ignore = close();
			}
			return -1;
		}
#endif	// _WIN32
	}
	return ret;
}

ssize_t socket::recv(std::span<char> buf) {
	if (ssl) {
		return reindexer::openssl::SSL_read(*ssl, buf.data(), buf.size());
	} else {
		return ::recv(fd_, buf.data(), buf.size(), 0);
	}
}

ssize_t socket::send(std::span<char> buf) {
	if (ssl) {
		return reindexer::openssl::SSL_write(*ssl, buf.data(), buf.size());
	} else {
		return ::send(fd_, buf.data(), buf.size(), 0);
	}
}

ssize_t socket::ssl_send(std::span<chunk> chunks) {
	constexpr size_t defaultBufCapacity = 0x1000;
	constexpr size_t maxBufSize = 0x4000;

	size_t size = std::accumulate(chunks.begin(), chunks.end(), 0, [](int res, const chunk& ch) { return res + ch.size(); });

	if (ssl_write_buf_.empty()) {
		ssl_write_buf_.reserve(defaultBufCapacity);
	}

	if (size > ssl_write_buf_.size()) {
		ssl_write_buf_.resize(size);
	}

	size_t total_size = 0;
	for (const auto& ch : chunks) {
		memcpy(ssl_write_buf_.data() + total_size, ch.data(), ch.size());
		total_size += ch.size();
	}

	int ret = reindexer::openssl::SSL_write(*ssl, ssl_write_buf_.data(), size);

	if (size > maxBufSize && ret > 0) {
		std::vector<uint8_t> tmp;
		tmp.reserve(defaultBufCapacity);
		std::swap(ssl_write_buf_, tmp);
	}

	return ret;
}

#ifdef _WIN32
ssize_t socket::send(std::span<chunk> chunks) {
	if (ssl) {
		return ssl_send(chunks);
	} else {
		h_vector<WSABUF, 64> iov;
		iov.resize(chunks.size());

		for (unsigned i = 0; i < chunks.size(); i++) {
			iov[i].buf = reinterpret_cast<CHAR*>(chunks[i].data());
			iov[i].len = chunks[i].size();
		}
		DWORD numberOfBytesSent;
		int res = ::WSASend(SOCKET(fd_), iov.data(), iov.size(), &numberOfBytesSent, 0, NULL, NULL);

		return res == 0 ? numberOfBytesSent : -1;
	}
}
#else	// _WIN32
ssize_t socket::send(std::span<chunk> chunks) {
	if (ssl) {
		return ssl_send(chunks);
	} else {
		h_vector<iovec, 64> iov;
		iov.resize(chunks.size());

		for (unsigned i = 0; i < chunks.size(); i++) {
			iov[i].iov_base = chunks[i].data();
			iov[i].iov_len = chunks[i].size();
		}
		return ::writev(fd_, iov.data(), iov.size());
	}
}
#endif	// _WIN32

void socket::setLinger0() {
	if (fd() >= 0) {
		struct linger sl;
		sl.l_onoff = 1;	 /* enable linger */
		sl.l_linger = 0; /* with 0 seconds timeout */
		setsockopt(fd(), SOL_SOCKET, SO_LINGER, reinterpret_cast<const char*>(&sl), sizeof(sl));
	}
}

int socket::create(std::string_view addr, struct addrinfo** presults) {
	assertrx(!valid());

	if (domain() == socket_domain::tcp) {
		struct addrinfo hints, *results = nullptr;
		memset(&hints, 0, sizeof(hints));
		hints.ai_flags = AI_PASSIVE;
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = IPPROTO_TCP;
		*presults = nullptr;

		std::string saddr(addr);
		char* paddr = &saddr[0];

		char* pport = strchr(paddr, ':');
		if (pport == nullptr) {
			pport = paddr;
			paddr = nullptr;
		} else {
			*pport = 0;
			if (*paddr == 0) {
				paddr = nullptr;
			}
			pport++;
		}

		int ret = ::getaddrinfo(paddr, pport, &hints, &results);
		if (ret != 0) [[unlikely]] {
			fprintf(stderr, "reindexer error: getaddrinfo failed: %s\n", gai_strerror(ret));
			return -1;
		}
		assertrx(results != nullptr);
		*presults = results;

		if ((fd_ = ::socket(results->ai_family, results->ai_socktype, results->ai_protocol)) < 0) [[unlikely]] {
			perror("socket error");
			return -1;
		}

		int enable = 1;
		if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&enable), sizeof(enable)) < 0) [[unlikely]] {
			perror("setsockopt(SO_REUSEADDR) failed");
		}
	} else {
#ifdef _WIN32
		return print_not_supported();
#else	// _WIN32
		(void)addr;
		(void)presults;
		assertrx(!presults);

		if ((fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0) [[unlikely]] {
			perror("socket error");
			return -1;
		}
#endif	// _WIN32
	}

	if (set_nodelay() < 0) [[unlikely]] {
		perror("set_nodelay() failed");
	}
	if (set_nonblock() < 0) [[unlikely]] {
		perror("set_nonblock() failed");
	}

	return 0;
}

std::string socket::addr() const {
	if (domain() == socket_domain::tcp) {
		struct sockaddr_storage saddr;
		struct sockaddr* paddr = reinterpret_cast<sockaddr*>(&saddr);
		socklen_t len = sizeof(saddr);
		if (::getpeername(fd_, paddr, &len) == 0) [[likely]] {
			char buf[INET_ADDRSTRLEN] = {};
			auto port = ntohs(reinterpret_cast<sockaddr_in*>(paddr)->sin_port);
			if (getnameinfo(paddr, len, buf, INET_ADDRSTRLEN, NULL, 0, NI_NUMERICHOST) == 0) [[likely]] {
				return std::string(buf) + ':' + std::to_string(port);
			} else {
				perror("getnameinfo error");
			}
		} else {
			perror("getpeername error");
		}
		return std::string();
	} else {
		return std::string("unx_dmn");
	}
}

int socket::set_nonblock() {
#ifndef _WIN32
	return fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL, 0) | O_NONBLOCK);
#else	// _WIN32
	u_long flag = 1;
	return ioctlsocket(fd_, FIONBIO, &flag);
#endif	// _WIN32
}

int socket::set_nodelay() noexcept {
	if (domain() == socket_domain::tcp) {
		int flag = 1;
		return setsockopt(fd_, SOL_TCP, TCP_NODELAY, reinterpret_cast<char*>(&flag), sizeof(flag));
	} else {
		return 0;
	}
}

bool socket::has_pending_data() const noexcept {
	if (!valid()) {
		return false;
	}
#ifndef _WIN32
	int count;
	if (ioctl(fd(), FIONREAD, &count) < 0) {
		perror("ioctl(FIONREAD) error");
		return false;
	}
#else	// _WIN32
	u_long count = -1;
	if (ioctlsocket(fd(), FIONREAD, &count) < 0) {
		perror("ioctlsocket(FIONREAD) error");
		return false;
	}
#endif	// _WIN32
	return count > 0;
}

int socket::last_error() noexcept {
#ifndef _WIN32
	return errno;
#else
	return WSAGetLastError();
#endif
}

bool socket::would_block(int error) noexcept {
#ifndef _WIN32
	return error == EAGAIN || error == EWOULDBLOCK || error == EINPROGRESS;
#else
	return error == EAGAIN || error == EWOULDBLOCK || error == WSAEWOULDBLOCK || error == EINPROGRESS;
#endif
}

int socket::close() {
#ifndef _WIN32
	if (fd_ >= 0) {
		shutdown(fd_, SHUT_RDWR);
	}
	int ret = ::close(fd_);
#else
	int ret = ::closesocket(fd_);
#endif
	if (ret < 0) {
		perror("close() error");
	}
	fd_ = -1;
	ssl.reset();
	ssl_write_buf_ = std::vector<uint8_t>{};
	return ret;
}

int lst_socket::bind(std::string_view addr, socket_domain t) {
	assertrx(!valid());
	int ret = 0;
	sock_.domain(t);
	if (domain() == socket_domain::tcp) {
		struct addrinfo* results = nullptr;
		ret = sock_.create(addr, &results);
		if (!ret) [[unlikely]] {
			assertrx(results != nullptr);
			ret = ::bind(sock_.fd(), results->ai_addr, results->ai_addrlen);
			if (ret != 0) [[unlikely]] {
				perror("bind error");
				std::ignore = close();
			}
		}
		if (results) {
			freeaddrinfo(results);
		}
	} else {
#ifdef _WIN32
		return print_not_supported();
#else	// _WIN32
		if (sock_.create(addr, nullptr) < 0) {
			return -1;
		}

		struct sockaddr_un address;
		address.sun_family = AF_UNIX;
		memcpy(address.sun_path, addr.data(), addr.size());
		address.sun_path[addr.size()] = 0;

		unPath_ = addr;
		unLock_ = unPath_ + ".LOCK";
		assertrx(lockFd_ < 0);
		lockFd_ = ::open(unLock_.c_str(), O_WRONLY | O_CREAT | O_APPEND, S_IRWXU);	// open(unLock_.c_str(), O_RDONLY | O_CREAT, 0600);
		if (lockFd_ < 0) {
			perror("open(lock) error");
			std::ignore = close();
			return -1;
		}

		struct flock lock;
		memset(&lock, 0, sizeof(struct flock));
		lock.l_type = F_WRLCK;
		lock.l_start = 0;
		lock.l_whence = SEEK_SET;
		lock.l_len = 0;
		lock.l_pid = getpid();

		if (fcntl(lockFd_, F_SETLK, &lock) < 0) [[unlikely]] {
			fprintf(stderr, "reindexer error: unable to get LOCK for %s\n", unLock_.c_str());
			perror("fcntl(F_SETLK) error");
			std::ignore = close();
			return -1;
		}
		unlink(unPath_.c_str());

		if (::bind(sock_.fd(), reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) < 0) [[unlikely]] {
			perror("bind() error");
			std::ignore = close();
			return -1;
		}
#endif	// _WIN32
	}
	return ret;
}

int lst_socket::listen(int backlog) noexcept {
	if (domain() == socket_domain::tcp) {
#ifdef __linux__
		int enable = 1;

		if (setsockopt(sock_.fd(), SOL_TCP, TCP_DEFER_ACCEPT, &enable, sizeof(enable)) < 0) {
			perror("setsockopt(TCP_DEFER_ACCEPT) failed");
		}
		if (setsockopt(sock_.fd(), SOL_TCP, TCP_QUICKACK, &enable, sizeof(enable)) < 0) {
			perror("setsockopt(TCP_QUICKACK) failed");
		}
#endif
	}
#ifdef _WIN32
	else {
		return print_not_supported();
	}
#endif	// _WIN32
	return ::listen(sock_.fd(), backlog);
}

int lst_socket::close() {
	int ret = sock_.close();

	if (domain() == socket_domain::unx) {
		if (!unPath_.empty() && (unlink(unPath_.c_str()) != 0)) {
			perror("unix socket unlink error");
		}
		if (::close(lockFd_) != 0) {
			perror("close(lock) error");
			ret = -1;
		} else {
			if (::unlink(unLock_.c_str()) != 0) {
				perror("lock file unlink error");
				ret = -1;
			}
		}
		unPath_.clear();
		unLock_.clear();
		lockFd_ = -1;
	}
	return ret;
}

socket lst_socket::accept() {
	struct sockaddr client_addr;
	memset(&client_addr, 0, sizeof(client_addr));
	socklen_t client_len = sizeof(client_addr);

#ifdef __linux__
	socket client(::accept4(sock_.fd(), &client_addr, &client_len, SOCK_NONBLOCK), domain());

#else	// __linux__
	socket client(::accept(sock_.fd(), &client_addr, &client_len), domain());
	if (client.valid()) {
		if (client.set_nonblock() < 0) [[unlikely]] {
			perror("client.set_nonblock() error");
		}
	}
#endif	// __linux__
	if (client.valid()) [[likely]] {
		if (client.set_nodelay() != 0) [[unlikely]] {
			perror("client.set_nodelay() error");
		}
	}
	return client;
}

#ifdef _WIN32
class [[nodiscard]] __windows_ev_init {
public:
	__windows_ev_init() {
		WSADATA wsaData;
		WSAStartup(MAKEWORD(2, 2), &wsaData);
	}
} __windows_ev_init;
#endif

}  // namespace net
}  // namespace reindexer
