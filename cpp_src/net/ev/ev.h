#pragma once

#include <assert.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <functional>
#include <memory>
#include <vector>

// Thank's to windows.h include
#ifdef ERROR
#undef ERROR
#endif

#ifdef _WIN32
#define HAVE_WSA_LOOP 1
#else
#define HAVE_POSIX_LOOP 1
#define HAVE_SELECT_LOOP 1
#ifdef __linux__
#define HAVE_EPOLL_LOOP 1
#elif defined(__APPLE__) || (defined __unix__)
#define HAVE_POLL_LOOP 1
#endif
#endif

namespace reindexer {
namespace net {
namespace ev {
const int READ = 0x01;
const int WRITE = 0x02;
const int HUP = 0x4;
const int ERROR = 0x08;

class dynamic_loop;

#ifdef HAVE_POSIX_LOOP
class loop_posix_base {
public:
	void enable_asyncs();
	void send_async();

protected:
	loop_posix_base();
	~loop_posix_base();
	bool check_async(int fd);

	int async_fds_[2] = {-1, -1};
	dynamic_loop *owner_ = nullptr;
};

class loop_poll_backend_private;
class loop_poll_backend : public loop_posix_base {
public:
	loop_poll_backend();
	~loop_poll_backend();
	void init(dynamic_loop *owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);
	static int capacity();

protected:
	std::unique_ptr<loop_poll_backend_private> private_;
};

class loop_select_backend_private;
class loop_select_backend : public loop_posix_base {
public:
	loop_select_backend();
	~loop_select_backend();
	void init(dynamic_loop *owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);
	static int capacity();

protected:
	std::unique_ptr<loop_select_backend_private> private_;
};

#endif

#ifdef HAVE_EPOLL_LOOP
class loop_epoll_backend_private;
class loop_epoll_backend : public loop_posix_base {
public:
	loop_epoll_backend();
	~loop_epoll_backend();
	void init(dynamic_loop *owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);
	static int capacity();

protected:
	std::unique_ptr<loop_epoll_backend_private> private_;
};
#endif

#ifdef HAVE_WSA_LOOP
class loop_wsa_backend_private;
class loop_wsa_backend {
public:
	loop_wsa_backend();
	~loop_wsa_backend();
	void init(dynamic_loop *owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);
	void enable_asyncs();
	void send_async();

	static int capacity();

protected:
	dynamic_loop *owner_;
	std::unique_ptr<loop_wsa_backend_private> private_;
};
#endif

class io;
class timer;
class async;
class sig;
class dynamic_loop {
	friend class loop_ref;
	friend class loop_epoll_backend;
	friend class loop_poll_backend;
	friend class loop_select_backend;
	friend class loop_wsa_backend;
	friend class loop_posix_base;

public:
	dynamic_loop();
	~dynamic_loop();
	void run();
	void break_loop();

protected:
	void set(int fd, io *watcher, int events);
	void set(timer *watcher, double t);
	void set(async *watcher);
	void set(sig *watcher);
	void stop(int fd);
	void stop(timer *watcher);
	void stop(async *watcher);
	void stop(sig *watcher);
	void send(async *watcher);

	void io_callback(int fd, int events);
	void async_callback();

	struct fd_handler {
		int emask_ = 0;
		int idx = -1;
		io *watcher_ = nullptr;
	};

	std::vector<fd_handler> fds_;
	std::vector<timer *> timers_;
	std::vector<async *> asyncs_;
	std::vector<sig *> sigs_;
	bool break_ = false;
	std::atomic<int> async_sent_;

#ifdef HAVE_EPOLL_LOOP
	loop_epoll_backend backend_;
#elif defined(HAVE_POLL_LOOP)
	loop_poll_backend backend_;
#elif defined(HAVE_SELECT_LOOP)
	loop_select_backend backend_;
#elif defined(HAVE_WSA_LOOP)
	loop_wsa_backend backend_;
#else
#error "There are no compatible loop backend"
#endif
};

class loop_ref {
	friend class io;
	friend class timer;
	friend class sig;
	friend class async;

public:
	void break_loop() {
		if (loop_) loop_->break_loop();
	}
	void run() {
		if (loop_) loop_->run();
	}
	void enable_asyncs() {
		if (loop_) loop_->backend_.enable_asyncs();
	}

protected:
	template <typename... Args>
	void set(Args... args) {
		if (loop_) loop_->set(args...);
	}
	template <typename... Args>
	void stop(Args... args) {
		if (loop_) loop_->stop(args...);
	}
	template <typename... Args>
	void send(Args... args) {
		if (loop_) loop_->send(args...);
	}
	dynamic_loop *loop_ = nullptr;
};

class io {
	friend class dynamic_loop;

public:
	io(){};
	io(const io &) = delete;
	~io() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void set(int events) { loop.set(fd, this, events); }
	void start(int _fd, int events) {
		fd = _fd;
		loop.set(fd, this, events);
	}
	void stop() {
		loop.stop(fd);
		fd = -1;
	}
	void reset() { loop.loop_ = nullptr; }

	template <typename K, void (K::*func)(io &, int events)>
	void set(K *object) {
		func_ = [object](io &watcher, int events) { (static_cast<K *>(object)->*func)(watcher, events); };
	}
	void set(std::function<void(io &watcher, int events)> func) { func_ = func; }

	int fd = -1;
	loop_ref loop;

protected:
	void callback(int events) {
		assert(func_ != nullptr);
		func_(*this, events);
	}
	std::function<void(io &watcher, int events)> func_ = nullptr;
};
class timer {
	friend class dynamic_loop;

public:
	timer(){};
	timer(const timer &) = delete;
	~timer() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void start(double t, double p = 0) {
		period_ = p;
		loop.set(this, t);
	}
	void stop() { loop.stop(this); }
	void reset() { loop.loop_ = nullptr; }

	template <typename K, void (K::*func)(timer &, int t)>
	void set(K *object) {
		func_ = [object](timer &watcher, int t) { (static_cast<K *>(object)->*func)(watcher, t); };
	}
	void set(std::function<void(timer &, int)> func) { func_ = func; }

	loop_ref loop;
	std::chrono::time_point<std::chrono::steady_clock> deadline_;

protected:
	void callback(int tv) {
		assert(func_ != nullptr);
		func_(*this, tv);
		if (period_ > 0.00000001) {
			loop.set(this, period_);
		}
	}

	std::function<void(timer &watcher, int t)> func_ = nullptr;
	double period_ = 0;
};

using periodic = timer;

class sig {
	friend class dynamic_loop;

public:
	sig(){};
	sig(const sig &) = delete;
	~sig() { stop(); }

	void set(dynamic_loop &loop_) { loop.loop_ = &loop_; }
	void start(int signum) {
		signum_ = signum;
		loop.set(this);
	}
	void stop() { loop.stop(this); }
	void reset() { loop.loop_ = nullptr; }

	template <typename K, void (K::*func)(sig &)>
	void set(K *object) {
		func_ = [object](sig &watcher) { (static_cast<K *>(object)->*func)(watcher); };
	}
	void set(std::function<void(sig &)> func) { func_ = func; }

	loop_ref loop;

protected:
	void callback() {
		assert(func_ != nullptr);
		func_(*this);
	}

	std::function<void(sig &watcher)> func_ = nullptr;
#ifndef _WIN32
	struct sigaction old_action_;
#else
	void (*old_handler_)(int);
#endif
	int signum_;
};

class async {
	friend class dynamic_loop;

public:
	async() : sent_(false) {}
	async(const async &) = delete;
	~async() { stop(); }

	void set(dynamic_loop &loop_) {
		loop.loop_ = &loop_;
		loop.enable_asyncs();
	}
	void start() { loop.set(this); }
	void stop() { loop.stop(this); }
	void reset() { loop.loop_ = nullptr; }
	void send() { loop.send(this); }

	template <typename K, void (K::*func)(async &)>
	void set(K *object) {
		func_ = [object](async &watcher) { (static_cast<K *>(object)->*func)(watcher); };
	}
	void set(std::function<void(async &)> func) { func_ = func; }

	loop_ref loop;

protected:
	void callback() {
		assert(func_ != nullptr);
		func_(*this);
	}

	std::function<void(async &watcher)> func_ = nullptr;
	std::atomic<bool> sent_;
};

extern bool gEnableBusyLoop;

}  // namespace ev
}  // namespace net
}  // namespace reindexer
