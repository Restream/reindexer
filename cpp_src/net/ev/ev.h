#pragma once

#include <atomic>
#include <csignal>
#include <memory>
#include <thread>
#include <vector>

#include "coroutine/waitgroup.h"
#include "estl/h_vector.h"
#include "tools/clock.h"

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
#define HAVE_EVENT_FD 1
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
#ifdef HAVE_EVENT_FD
class [[nodiscard]] loop_posix_base {
public:
	void enable_asyncs();
	void send_async();

protected:
	loop_posix_base() = default;
	~loop_posix_base();
	bool check_async(int fd);

	int async_fd_ = -1;
	dynamic_loop* owner_ = nullptr;
};
#else	// HAVE_EVENT_FD
class [[nodiscard]] loop_posix_base {
public:
	void enable_asyncs();
	void send_async();

protected:
	loop_posix_base() = default;
	~loop_posix_base();
	bool check_async(int fd);

	int async_fds_[2] = {-1, -1};
	dynamic_loop* owner_ = nullptr;
};
#endif	// HAVE_EVENT_FD

class loop_poll_backend_private;
class [[nodiscard]] loop_poll_backend : public loop_posix_base {
public:
	loop_poll_backend();
	~loop_poll_backend();
	void init(dynamic_loop* owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);
	static int capacity();

protected:
	std::unique_ptr<loop_poll_backend_private> private_;
};

class loop_select_backend_private;
class [[nodiscard]] loop_select_backend : public loop_posix_base {
public:
	loop_select_backend();
	~loop_select_backend();
	void init(dynamic_loop* owner) noexcept;
	void set(int fd, int events, int oldevents) noexcept;
	void stop(int fd) noexcept;
	int runonce(int64_t tv);
	static int capacity() noexcept;

protected:
	std::unique_ptr<loop_select_backend_private> private_;
};
#endif	// HAVE_POSIX_LOOP

#ifdef HAVE_EPOLL_LOOP
class loop_epoll_backend_private;
class [[nodiscard]] loop_epoll_backend : public loop_posix_base {
public:
	loop_epoll_backend();
	~loop_epoll_backend();
	void init(dynamic_loop* owner);
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
class [[nodiscard]] loop_wsa_backend {
public:
	loop_wsa_backend();
	~loop_wsa_backend();
	void init(dynamic_loop* owner);
	void set(int fd, int events, int oldevents);
	void stop(int fd);
	int runonce(int64_t tv);
	void enable_asyncs();
	void send_async();

	static int capacity();

protected:
	dynamic_loop* owner_;
	std::unique_ptr<loop_wsa_backend_private> private_;
};
#endif

class io;
class timer;
class async;
class sig;
class [[nodiscard]] dynamic_loop {
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
	void break_loop() noexcept { break_ = true; }
	void spawn(std::function<void()> func, size_t stack_size = coroutine::k_default_stack_limit) {
		auto tid = std::this_thread::get_id();
		if (coroTid_ != std::thread::id() && coroTid_ != tid) {
			// Every coroutine has to be spawned from the same thread
			assertrx(false);
		} else {
			coroTid_ = tid;
		}
		auto id = coroutine::create(std::move(func), stack_size);
		new_tasks_.emplace_back(id);
	}
	void spawn(coroutine::wait_group& wg, std::function<void()> func, size_t stack_size = coroutine::k_default_stack_limit) {
		wg.add(1);
		spawn(
			[f = std::move(func), &wg]() {	// NOLINT(*.NewDeleteLeaks) False positive
				coroutine::wait_group_guard wgg(wg);
				f();
			},
			stack_size);
	}
	template <typename Rep, typename Period>
	void sleep(std::chrono::duration<Rep, Period> dur);
	template <typename Rep1, typename Period1, typename Rep2, typename Period2, typename TerminateT>
	void granular_sleep(std::chrono::duration<Rep1, Period1> dur, std::chrono::duration<Rep2, Period2> granularity,
						const TerminateT& terminate);
	void yield() {
		// Yield is allowed for loop-thread only
		assertrx(coroTid_ == std::thread::id() || coroTid_ == std::this_thread::get_id());
		auto id = coroutine::current();
		assertrx(id);
		yielded_tasks_.emplace_back(id);
		coroutine::suspend();
	}

protected:
	void set(int fd, io* watcher, int events);
	void set(timer* watcher, double t);
	void set(async* watcher);
	void set(sig* watcher);
	void stop(int fd) noexcept;
	void stop(timer* watcher) noexcept;
	void stop(async* watcher) noexcept;
	void stop(sig* watcher) noexcept;
	void send(async* watcher) noexcept;
	bool is_active(const timer* watcher) const noexcept;

	void io_callback(int fd, int events);
	void async_callback();

	void set_coro_cb();
	void remove_coro_cb() noexcept;

	struct [[nodiscard]] fd_handler {
		int emask_ = 0;
		int idx = -1;
		io* watcher_ = nullptr;
	};

	std::vector<fd_handler> fds_;
	std::vector<timer*> timers_;
	std::vector<async*> asyncs_;
	std::vector<sig*> sigs_;
	bool break_ = false;
	bool coro_cb_is_set_ = false;
	std::atomic<int> async_sent_;

	using tasks_container = h_vector<coroutine::routine_t, 64>;
	tasks_container new_tasks_;
	tasks_container running_tasks_;
	h_vector<coroutine::routine_t, 5> yielded_tasks_;
	std::thread::id coroTid_;

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

class [[nodiscard]] loop_ref {
	friend class io;
	friend class timer;
	friend class sig;
	friend class async;

public:
	void break_loop() noexcept {
		if (loop_) {
			loop_->break_loop();
		}
	}
	void run() {
		if (loop_) {
			loop_->run();
		}
	}
	void enable_asyncs() {
		if (loop_) {
			loop_->backend_.enable_asyncs();
		}
	}
	bool is_valid() const noexcept { return loop_; }
	void spawn(coroutine::wait_group& wg, std::function<void()> func, size_t stack_size = coroutine::k_default_stack_limit) {
		if (loop_) {
			loop_->spawn(wg, std::move(func), stack_size);
		}
	}
	void spawn(std::function<void()> func, size_t stack_size = coroutine::k_default_stack_limit) {
		if (loop_) {
			loop_->spawn(std::move(func), stack_size);
		}
	}

protected:
	template <typename... Args>
	void set(Args... args) {
		if (loop_) {
			loop_->set(args...);
		}
	}
	template <typename... Args>
	void stop(Args... args) {
		if (loop_) {
			loop_->stop(args...);
		}
	}
	template <typename... Args>
	void send(Args... args) {
		if (loop_) {
			loop_->send(args...);
		}
	}
	template <typename... Args>
	bool is_active(Args... args) const noexcept {
		if (loop_) {
			return loop_->is_active(args...);
		}
		return false;
	}
	dynamic_loop* loop_ = nullptr;
};

class [[nodiscard]] io {
	friend class dynamic_loop;

public:
	io() = default;
	io(const io&) = delete;
	~io() { stop(); }

	void set(dynamic_loop& loop_) noexcept { loop.loop_ = &loop_; }
	void set(int events) { loop.set(fd, this, events); }
	void start(int _fd, int events) {
		fd = _fd;
		loop.set(fd, this, events);
	}
	void stop() {
		loop.stop(fd);
		fd = -1;
	}
	void reset() noexcept { loop.loop_ = nullptr; }

	template <typename K, void (K::*func)(io&, int events)>
	void set(K* object) {
		func_ = [object](io& watcher, int events) { (static_cast<K*>(object)->*func)(watcher, events); };
	}
	void set(std::function<void(io& watcher, int events)> func) noexcept { func_ = std::move(func); }

	int fd = -1;
	loop_ref loop;

protected:
	void callback(int events) {
		assertrx(func_ != nullptr);
		func_(*this, events);
	}
	std::function<void(io& watcher, int events)> func_ = nullptr;
};
class [[nodiscard]] timer {
	friend class dynamic_loop;

public:
	timer() = default;
	timer(const timer&) = delete;
	timer(timer&&) = default;
	timer& operator=(timer&&) = default;
	~timer() { stop(); }

	void set(dynamic_loop& loop_) noexcept { loop.loop_ = &loop_; }
	void start(double t, double p = 0) {
		period_ = p;
		loop.set(this, t);
	}
	void stop() { loop.stop(this); }
	void reset() noexcept { loop.loop_ = nullptr; }

	template <typename K, void (K::*func)(timer&, int t)>
	void set(K* object) {
		func_ = [object](timer& watcher, int t) { (static_cast<K*>(object)->*func)(watcher, t); };
	}
	void set(std::function<void(timer&, int)> func) noexcept { func_ = std::move(func); }

	bool is_active() const noexcept { return loop.is_active(this); }
	bool has_period() const noexcept { return period_ > 0.00000001; }

	loop_ref loop;
	steady_clock_w::time_point deadline_;

protected:
	struct [[nodiscard]] coro_t {};
	timer(coro_t) noexcept : in_coro_storage_(true) {}

	void callback(int tv) {
		assertrx(func_ != nullptr);
		if (in_coro_storage_) {
			auto func = std::move(func_);
			func(*this, tv);  // Timer is deallocated after this call
		} else {
			func_(*this, tv);
			if (has_period()) {
				loop.set(this, period_);
			}
		}
	}

	std::function<void(timer& watcher, int t)> func_ = nullptr;
	double period_ = 0;
	bool in_coro_storage_ = false;
};

using periodic = timer;

template <typename Rep, typename Period>
void dynamic_loop::sleep(std::chrono::duration<Rep, Period> dur) {
	auto id = coroutine::current();
	if (id) {
		timer tm(timer::coro_t{});
		tm.set([id](timer&, int) { std::ignore = coroutine::resume(id); });
		tm.set(*this);
		const double awaitTime = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
		tm.start(awaitTime / 1e6);
		do {
			coroutine::suspend();
		} while (tm.is_active());
		tm.reset();
	} else {
		std::this_thread::sleep_for(dur);
	}
}

template <typename Rep1, typename Period1, typename Rep2, typename Period2, typename TerminateT>
void dynamic_loop::granular_sleep(std::chrono::duration<Rep1, Period1> dur, std::chrono::duration<Rep2, Period2> granularity,
								  const TerminateT& terminate) {
	for (std::chrono::nanoseconds t = dur; t.count() > 0; t -= granularity) {
		if (terminate()) {
			return;
		}
		sleep(std::min(t, std::chrono::duration_cast<std::chrono::nanoseconds>(granularity)));
	}
}

class [[nodiscard]] sig {
	friend class dynamic_loop;

public:
	sig() = default;  // -V730
	sig(const sig&) = delete;
	~sig() { stop(); }

	void set(dynamic_loop& loop_) noexcept { loop.loop_ = &loop_; }
	void start(int signum) {
		signum_ = signum;
		loop.set(this);
	}
	void stop() { loop.stop(this); }
	void reset() noexcept { loop.loop_ = nullptr; }

	template <typename K, void (K::*func)(sig&)>
	void set(K* object) {
		func_ = [object](sig& watcher) { (static_cast<K*>(object)->*func)(watcher); };
	}
	void set(std::function<void(sig&)> func) noexcept { func_ = std::move(func); }

	loop_ref loop;

protected:
	void callback() {
		assertrx(func_ != nullptr);
		func_(*this);
	}

	std::function<void(sig& watcher)> func_ = nullptr;
#ifndef _WIN32
	struct sigaction old_action_;
#else
	void (*old_handler_)(int);
#endif
	int signum_ = 0;
};

class [[nodiscard]] async {
	friend class dynamic_loop;

public:
	async() : sent_(false) {}
	async(const async&) = delete;
	~async() { stop(); }

	void set(dynamic_loop& loop_) {
		loop.loop_ = &loop_;
		loop.enable_asyncs();
	}
	void start() { loop.set(this); }
	void stop() { loop.stop(this); }
	void reset() noexcept { loop.loop_ = nullptr; }
	void send() { loop.send(this); }

	template <typename K, void (K::*func)(async&)>
	void set(K* object) {
		func_ = [object](async& watcher) { (static_cast<K*>(object)->*func)(watcher); };
	}
	void set(std::function<void(async&)> func) noexcept { func_ = std::move(func); }

	loop_ref loop;

protected:
	void callback() {
		assertrx(func_ != nullptr);
		func_(*this);
	}

	std::function<void(async& watcher)> func_ = nullptr;
	std::atomic<bool> sent_;
};

// extern bool gEnableBusyLoop;

}  // namespace ev
}  // namespace net
}  // namespace reindexer
