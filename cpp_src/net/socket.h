#pragma once

#include <cstdlib>
#include "estl/chunk_buf.h"
#include "tools/ssize_t.h"

struct addrinfo;
namespace reindexer {
namespace net {

enum class socket_domain : bool { tcp, unx };
class lst_socket;

class socket {
public:
	socket() = default;
	socket(const socket &&other) = delete;
	socket(socket &&other) noexcept : fd_(other.fd_), type_(other.type_) { other.fd_ = -1; }
	socket &operator=(const socket &other) = delete;
	socket &operator=(socket &&other) noexcept {
		if rx_likely (this != &other) {
			if (valid()) {
				close();
			}
			fd_ = other.fd_;
			type_ = other.type_;
			other.fd_ = -1;
		}
		return *this;
	}
	~socket() {
		if (valid()) {
			close();
		}
	}

	[[nodiscard]] int connect(std::string_view addr, socket_domain t);
	ssize_t recv(span<char> buf);
	ssize_t send(const span<char> buf);
	ssize_t send(span<chunk> chunks);
	int close();
	std::string addr() const;

	[[nodiscard]] int set_nonblock();
	[[nodiscard]] int set_nodelay() noexcept;
	int fd() const noexcept { return fd_; }
	bool valid() const noexcept { return fd_ >= 0; }
	bool has_pending_data() const noexcept;
	socket_domain domain() const noexcept { return type_; }

	static int last_error() noexcept;
	static bool would_block(int error) noexcept;

private:
	friend class lst_socket;

	socket(int fd, socket_domain type) noexcept : fd_(fd), type_(type) {}
	int create(std::string_view addr, struct addrinfo **pres);
	void domain(socket_domain t) noexcept { type_ = t; }

	int fd_ = -1;
	socket_domain type_ = socket_domain::tcp;
};

class lst_socket {
public:
	lst_socket() = default;
	lst_socket(const lst_socket &&other) = delete;
	lst_socket(lst_socket &&other) noexcept
		: sock_(std::move(other.sock_)), lockFd_(other.lockFd_), unPath_(std::move(other.unPath_)), unLock_(std::move(other.unLock_)) {
		other.lockFd_ = -1;
	}
	lst_socket &operator=(const lst_socket &other) = delete;
	lst_socket &operator=(lst_socket &&other) noexcept {
		if rx_likely (this != &other) {
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
	[[nodiscard]] int listen(int backlog) noexcept;
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
