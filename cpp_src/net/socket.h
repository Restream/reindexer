#pragma once

#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include "estl/chunk.h"
#include "estl/defines.h"
#include "tools/ssize_t.h"
#include "tools/tls.h"

struct addrinfo;
namespace reindexer {
namespace net {

enum class [[nodiscard]] socket_domain : bool { tcp, unx };
class lst_socket;

class [[nodiscard]] socket {
public:
	socket() = default;
	socket(const socket&& other) = delete;
	socket(socket&& other) noexcept : ssl(std::move(other.ssl)), fd_(other.fd_), type_(other.type_) { other.fd_ = -1; }
	socket& operator=(const socket& other) = delete;
	socket& operator=(socket&& other) noexcept {
		if (this != &other) [[likely]] {
			if (valid()) {
				close();
			}
			fd_ = other.fd_;
			type_ = other.type_;
			ssl = std::move(other.ssl);
			other.fd_ = -1;
		}
		return *this;
	}
	~socket() {
		if (valid()) {
			close();
		}
	}

	int connect(std::string_view addr, socket_domain t);
	ssize_t recv(std::span<char> buf);
	ssize_t send(const std::span<char> buf);
	ssize_t send(std::span<chunk> chunks);
	void setLinger0();
	int close();
	std::string addr() const;

	int set_nonblock();
	int set_nodelay() noexcept;
	int fd() const noexcept { return fd_; }
	bool valid() const noexcept { return fd_ >= 0; }
	bool has_pending_data() const noexcept;
	socket_domain domain() const noexcept { return type_; }

	static int last_error() noexcept;
	static bool would_block(int error) noexcept;

	openssl::SslPtr ssl;

private:
	friend class lst_socket;

	socket(int fd, socket_domain type) noexcept : fd_(fd), type_(type) {}
	int create(std::string_view addr, struct addrinfo** pres);
	void domain(socket_domain t) noexcept { type_ = t; }

	ssize_t ssl_send(std::span<chunk> chunks);

	int fd_ = -1;
	socket_domain type_ = socket_domain::tcp;

	// When sending big data, SSL_write can return -1, but the write operation can be performed again.
	// SSL_write expects the buffer to be at the same address as in the previous attempt
	std::vector<uint8_t> ssl_write_buf_;
};

class [[nodiscard]] lst_socket {
public:
	lst_socket() = default;
	lst_socket(const lst_socket&& other) = delete;
	lst_socket(lst_socket&& other) noexcept
		: sock_(std::move(other.sock_)), lockFd_(other.lockFd_), unPath_(std::move(other.unPath_)), unLock_(std::move(other.unLock_)) {
		other.lockFd_ = -1;
	}
	lst_socket& operator=(const lst_socket& other) = delete;
	lst_socket& operator=(lst_socket&& other) noexcept {
		if (this != &other) [[likely]] {
			if (valid()) {
				close();
			}
			sock_ = std::move(other.sock_);
			lockFd_ = other.lockFd_;
			unPath_ = std::move(other.unPath_);
			unLock_ = std::move(other.unLock_);
			other.lockFd_ = -1;
		}
		return *this;
	}
	~lst_socket() {
		if (valid()) {
			close();
		}
	}

	int bind(std::string_view addr, socket_domain t);
	socket accept();
	int listen(int backlog) noexcept;
	int close();

	int fd() const noexcept { return sock_.fd(); }
	bool valid() const noexcept { return sock_.valid(); }
	bool owns_lock() const noexcept { return lockFd_ >= 0; }

	static int last_error() noexcept { return socket::last_error(); }
	static bool would_block(int error) noexcept { return socket::would_block(error); }

private:
	socket_domain domain() const noexcept { return sock_.domain(); }

	socket sock_;
	int lockFd_ = -1;
	std::string unPath_;
	std::string unLock_;
};

}  // namespace net
}  // namespace reindexer
