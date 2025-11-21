#include "ev.h"
#include <stdio.h>
#include <string.h>
#include <sstream>
#include "debug/backtrace.h"
#include "tools/oscompat.h"

#ifdef HAVE_EVENT_FD
#include <sys/eventfd.h>
#endif
#ifdef HAVE_SELECT_LOOP
#include <sys/select.h>
#endif
#ifdef HAVE_POLL_LOOP
#include <sys/poll.h>
#endif
#ifdef HAVE_EPOLL_LOOP
#include <sys/epoll.h>
#endif

namespace reindexer {
namespace net {
namespace ev {

constexpr bool gEnableBusyLoop = false;

#ifdef HAVE_POSIX_LOOP
#ifdef HAVE_EVENT_FD
loop_posix_base::~loop_posix_base() {
	if (async_fd_ >= 0) {
		close(async_fd_);
	}
}

void loop_posix_base::enable_asyncs() {
	if (async_fd_ < 0) {
		async_fd_ = eventfd(0, EFD_NONBLOCK);
		if (async_fd_ < 0) {
			perror("eventfd error");
		}
		owner_->set(async_fd_, nullptr, READ);
	}
}

void loop_posix_base::send_async() {
	int res = eventfd_write(async_fd_, 1);
	(void)res;
}

bool loop_posix_base::check_async(int fd) {
	if (fd == async_fd_) {
		eventfd_t val;
		int res = eventfd_read(fd, &val);
		(void)res;
		owner_->async_callback();
		return true;
	}
	return false;
}
#else	// HAVE_EVENT_FD
loop_posix_base::~loop_posix_base() {
	if (async_fds_[0] >= 0) {
		close(async_fds_[0]);
	}
	if (async_fds_[1] >= 0) {
		close(async_fds_[1]);
	}
}

void loop_posix_base::enable_asyncs() {
	if (async_fds_[0] < 0) {
		if (pipe(async_fds_) < 0) {
			perror("pipe error");
		}
		owner_->set(async_fds_[0], nullptr, READ);
	}
}

void loop_posix_base::send_async() {
	int res = write(async_fds_[1], " ", 1);
	(void)res;
}

bool loop_posix_base::check_async(int fd) {
	if (fd == async_fds_[0]) {
		char tmpBuf[256];
		int res = read(fd, tmpBuf, sizeof(tmpBuf));
		(void)res;
		owner_->async_callback();
		return true;
	}
	return false;
}
#endif	// HAVE_EVENT_FD
#endif	// HAVE_POSIX_LOOP

#ifdef HAVE_SELECT_LOOP
class [[nodiscard]] loop_select_backend_private {
public:
	fd_set rfds_, wfds_;
	int maxfd_;
};

loop_select_backend::loop_select_backend() : private_(new loop_select_backend_private) {}
loop_select_backend::~loop_select_backend() = default;

void loop_select_backend::init(dynamic_loop* owner) noexcept {
	owner_ = owner;
	private_->maxfd_ = -1;
	FD_ZERO(&private_->rfds_);
	FD_ZERO(&private_->wfds_);
}

void loop_select_backend::set(int fd, int events, int /*oldevents*/) noexcept {
	assertrx(fd < capacity());

	if (fd > private_->maxfd_) {
		private_->maxfd_ = fd;
	}

	if (events & READ) {
		FD_SET(fd, &private_->rfds_);
	} else {
		FD_CLR(fd, &private_->rfds_);
	}

	if (events & WRITE) {
		FD_SET(fd, &private_->wfds_);
	} else {
		FD_CLR(fd, &private_->wfds_);
	}
}

void loop_select_backend::stop(int fd) noexcept {
	FD_CLR(fd, &private_->rfds_);
	FD_CLR(fd, &private_->wfds_);

	if (fd == private_->maxfd_) {
		private_->maxfd_--;
	}
}

int loop_select_backend::runonce(int64_t t) {
	timeval tv;
	tv.tv_sec = t / 1000000;
	tv.tv_usec = t % 1000000;
	fd_set rfds, wfds;

	memcpy(&rfds, &private_->rfds_, 1 + (private_->maxfd_ / 8));
	memcpy(&wfds, &private_->wfds_, 1 + (private_->maxfd_ / 8));

	int ret = select(private_->maxfd_ + 1, &rfds, &wfds, nullptr, t != -1 ? &tv : nullptr);
	if (ret < 0) {
		return ret;
	}

	for (int fd = 0; fd < private_->maxfd_ + 1; fd++) {
		int events = (FD_ISSET(fd, &rfds) ? READ : 0) | (FD_ISSET(fd, &wfds) ? WRITE : 0);
		if (events) {
			if (!check_async(fd)) {
				owner_->io_callback(fd, events);
			}
		}
	}
	return ret;
}

int loop_select_backend::capacity() noexcept { return FD_SETSIZE; }

#endif

#ifdef HAVE_POLL_LOOP
class [[nodiscard]] loop_poll_backend_private {
public:
	std::vector<pollfd> fds_;
	bool wasErased_;
};

loop_poll_backend::loop_poll_backend() : private_(new loop_poll_backend_private) {}
loop_poll_backend::~loop_poll_backend() {}

void loop_poll_backend::init(dynamic_loop* owner) {
	owner_ = owner;
	private_->fds_.reserve(2048);
	private_->fds_.resize(0);
}

void loop_poll_backend::set(int fd, int events, int /*oldevents*/) {
	short ev = ((events & READ) ? (POLLRDNORM | POLLIN) : 0) | ((events & WRITE) ? (POLLWRNORM | POLLOUT) : 0);
	int& idx = owner_->fds_[fd].idx;

	if (idx < 0) {
		private_->fds_.push_back({fd, ev, 0});
		idx = private_->fds_.size() - 1;
	} else {
		assertrx(private_->fds_.at(idx).fd == fd);
		private_->fds_.at(idx).events = ev;
		private_->fds_.at(idx).revents = 0;
	}
}

void loop_poll_backend::stop(int fd) {
	int& idx = owner_->fds_[fd].idx;
	assertrx(idx >= 0 && !private_->fds_.empty());

	if (static_cast<size_t>(idx) < private_->fds_.size() - 1) {
		int tmpfd = private_->fds_.back().fd;
		std::swap(private_->fds_.at(idx), private_->fds_.back());
		owner_->fds_[tmpfd].idx = idx;
	}

	private_->fds_.pop_back();
	owner_->fds_.at(fd).idx = -1;
	private_->wasErased_ = true;
}

int loop_poll_backend::runonce(int64_t t) {
	int ret = poll(&private_->fds_[0], private_->fds_.size(), t != -1 ? t / 1000 : -1);
	if (ret < 1) {
		return ret;
	}

	for (size_t i = 0; i < private_->fds_.size();) {
		pollfd& pfd = private_->fds_[i];
		private_->wasErased_ = false;
		if (pfd.revents != 0) {
			int events =
				((pfd.revents & (POLLRDNORM | POLLIN | POLLHUP)) ? READ : 0) | ((pfd.revents & (POLLWRNORM | POLLOUT)) ? WRITE : 0);
			if (events) {
				if (!check_async(pfd.fd)) {
					owner_->io_callback(pfd.fd, events);
				}
				if (!private_->wasErased_) {
					pfd.revents = 0;
				}
			}
		}
		if (!private_->wasErased_) {
			++i;
		}
	}
	return ret;
}

int loop_poll_backend::capacity() { return 500000; }
#endif

#ifdef HAVE_EPOLL_LOOP

class [[nodiscard]] loop_epoll_backend_private {
public:
	int ctlfd_ = -1;
	std::vector<epoll_event> events_;
	std::vector<epoll_event> eventsTmp_;
};

loop_epoll_backend::loop_epoll_backend() : private_(new loop_epoll_backend_private) {}

loop_epoll_backend::~loop_epoll_backend() {
	close(private_->ctlfd_);
	private_->ctlfd_ = -1;
}

void loop_epoll_backend::init(dynamic_loop* owner) {
	owner_ = owner;
	private_->ctlfd_ = epoll_create1(EPOLL_CLOEXEC);
	if (private_->ctlfd_ < 0) {
		perror("epoll_create error");
	}
	private_->events_.reserve(2048);
	private_->events_.resize(1);
	private_->eventsTmp_.reserve(private_->events_.capacity());
}

void loop_epoll_backend::set(int fd, int events, int oldevents) {
	epoll_event ev;
	ev.events = ((events & READ) ? int(EPOLLIN) | int(EPOLLHUP) : 0) | ((events & WRITE) ? int(EPOLLOUT) : 0) /*| EPOLLET*/;
	ev.data.fd = fd;
	if (epoll_ctl(private_->ctlfd_, oldevents == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, fd, &ev) < 0) {
		perror("epoll_ctl EPOLL_CTL_MOD error");
	}
	if (oldevents == 0) {
		private_->events_.emplace_back();
	}
}

void loop_epoll_backend::stop(int fd) {
	epoll_event ev;
	ev.data.fd = fd;
	if (epoll_ctl(private_->ctlfd_, EPOLL_CTL_DEL, fd, &ev) < 0) {
		perror("epoll_ctl EPOLL_CTL_DEL error");
	}
	private_->events_.pop_back();
}

int loop_epoll_backend::runonce(int64_t t) {
	int ret = epoll_wait(private_->ctlfd_, &private_->events_[0], private_->events_.size(), t != -1 ? t / 1000 : -1);

	assertrx(ret < static_cast<int>(private_->events_.size()));
	std::swap(private_->events_, private_->eventsTmp_);
	private_->events_.resize(private_->eventsTmp_.size());
	for (int i = 0; i < ret; i++) {
		auto& eventRef = private_->eventsTmp_[i];
		int events = ((eventRef.events & (EPOLLIN | EPOLLHUP)) ? READ : 0) | ((eventRef.events & EPOLLOUT) ? WRITE : 0);
		int fd = eventRef.data.fd;
		if (!check_async(fd)) {
			owner_->io_callback(fd, events);
		}
	}
	return ret;
}

#endif

#ifdef HAVE_WSA_LOOP
struct [[nodiscard]] win_fd {
	HANDLE hEvent = INVALID_HANDLE_VALUE;
	int fd = -1;
};

class [[nodiscard]] loop_wsa_backend_private {
public:
	std::vector<win_fd> wfds_;
	HANDLE hAsyncEvent = INVALID_HANDLE_VALUE;
};

HANDLE gSigEvent = INVALID_HANDLE_VALUE;

loop_wsa_backend::loop_wsa_backend() : private_(new loop_wsa_backend_private) {
	if (gSigEvent == INVALID_HANDLE_VALUE) {
		gSigEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
	}
}

loop_wsa_backend::~loop_wsa_backend() {
	for (auto& fd : private_->wfds_) {
		CloseHandle(fd.hEvent);
		fd.hEvent = INVALID_HANDLE_VALUE;
	}
	if (private_->hAsyncEvent != INVALID_HANDLE_VALUE) {
		CloseHandle(private_->hAsyncEvent);
	}
}

void loop_wsa_backend::init(dynamic_loop* owner) { owner_ = owner; }

void loop_wsa_backend::set(int fd, int events, int oldevents) {
	auto it = std::find_if(private_->wfds_.begin(), private_->wfds_.end(), [&](const win_fd& wfd) { return wfd.fd == fd; });
	if (it == private_->wfds_.end()) {
		assertrx(int(private_->wfds_.size()) < capacity());
		win_fd new_wfd;
		new_wfd.hEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
		new_wfd.fd = fd;
		it = private_->wfds_.insert(it, new_wfd);
		oldevents = -1;
	}
	if (oldevents != events) {
		int wevents = ((events & READ) ? FD_READ | FD_CONNECT | FD_ACCEPT : 0) | ((events & WRITE) ? FD_WRITE : 0) | FD_CLOSE;
		WSAEventSelect(it->fd, it->hEvent, wevents);
	}
}

void loop_wsa_backend::stop(int fd) {
	auto it = std::find_if(private_->wfds_.begin(), private_->wfds_.end(), [&](const win_fd& wfd) { return wfd.fd == fd; });
	if (it == private_->wfds_.end()) {
		return;
	}
	CloseHandle(it->hEvent);
	private_->wfds_.erase(it);
}

int loop_wsa_backend::runonce(int64_t t) {
	HANDLE objs[WSA_MAXIMUM_WAIT_EVENTS];
	unsigned ecount = private_->wfds_.size();
	for (unsigned i = 0; i < ecount; i++) {
		objs[i] = private_->wfds_[i].hEvent;
	}

	if (private_->hAsyncEvent != INVALID_HANDLE_VALUE) {
		objs[ecount] = private_->hAsyncEvent;
		ecount++;
	}
	if (owner_->sigs_.size()) {
		objs[ecount] = gSigEvent;
		ecount++;
	}

	// t = 200000;
	int ret = WaitForMultipleObjects(ecount, objs, FALSE, t != -1 ? t / 1000 : INFINITE);

	if (ret < 0) {
		perror("WaitForMultipleObjects error");
		return ret;
	}

	if (ret == int(WAIT_TIMEOUT)) {
		return 0;
	}

	if (ret >= int(WAIT_OBJECT_0)) {
		for (unsigned i = ret - WAIT_OBJECT_0; i < private_->wfds_.size(); i++) {
			WSANETWORKEVENTS wevents;
			if (!WSAEnumNetworkEvents(private_->wfds_[i].fd, private_->wfds_[i].hEvent, &wevents)) {
				int events = ((wevents.lNetworkEvents & (FD_READ | FD_CONNECT | FD_ACCEPT | FD_CLOSE)) ? READ : 0) |
							 ((wevents.lNetworkEvents & FD_WRITE) ? WRITE : 0);
				int fd = private_->wfds_[i].fd;
				owner_->io_callback(fd, events);
			}
		}
		if (ret == int(WAIT_OBJECT_0 + private_->wfds_.size())) {
			owner_->async_callback();
		}
	}
	return ret;
}

void loop_wsa_backend::enable_asyncs() {
	if (private_->hAsyncEvent == INVALID_HANDLE_VALUE) {
		private_->hAsyncEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
	}
}
void loop_wsa_backend::send_async() { SetEvent(private_->hAsyncEvent); }

int loop_wsa_backend::capacity() { return WSA_MAXIMUM_WAIT_EVENTS - 2; }

#endif

static std::atomic<int> signalsMask;

static void net_ev_sighandler(int signum) {
	signalsMask |= (1 << signum);
#ifdef _WIN32
	SetEvent(gSigEvent);
#endif
}

dynamic_loop::dynamic_loop() : async_sent_(false) {
	fds_.reserve(2048);
	backend_.init(this);
}

dynamic_loop::~dynamic_loop() {
	if (!running_tasks_.empty() || !new_tasks_.empty()) {
		try {
			run();
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: unexpected exception in ~dynamic_loop: %s\n", e.what());
			assertrx_dbg(false);
		}
	}
	if (coro_cb_is_set_) {
		remove_coro_cb();
	}
}

void dynamic_loop::run() {
	if (coroTid_ != std::thread::id() && coroTid_ != std::this_thread::get_id()) {
		// Loop has coroutines from another thread
		std::stringstream ss;
		reindexer::debug::print_backtrace(ss, nullptr, -1);
		fprintf(stderr, "reindexer error: incorrect coroutine in dynamic loop. Backtrace:\n%s\n", ss.str().c_str());
		assertrx(false);
	}

	break_ = false;
	auto now = steady_clock_w::now();
	int count = 0;

	set_coro_cb();
	bool has_coro_tasks = false;
	while (!break_) {
		while (new_tasks_.size()) {
			has_coro_tasks = true;
			running_tasks_.reserve(running_tasks_.size() + new_tasks_.size());
			for (auto id : new_tasks_) {
				running_tasks_.emplace_back(id);
			}
			tasks_container new_tasks;
			std::swap(new_tasks_, new_tasks);
			for (auto id : new_tasks) {
				int res = coroutine::resume(id);
				assertrx(res == 0);
				if (res != 0) {
					running_tasks_.pop_back();
				}
			}
		}

		if (has_coro_tasks && running_tasks_.empty() && yielded_tasks_.empty()) {
			break;
		}

		bool busy_loop = gEnableBusyLoop || yielded_tasks_.size();
		int tv = busy_loop ? 0 : -1;

		if (!busy_loop && timers_.size()) {
			tv = std::chrono::duration_cast<std::chrono::microseconds>(timers_.front()->deadline_ - now).count();
			if (tv < 0) {
				tv = 0;
			}
		}
		int ret = backend_.runonce(tv);

		if (sigs_.size()) {
			int pendingSignalsMask = signalsMask.exchange(0);
			if (pendingSignalsMask) {
				for (auto sig : sigs_) {
					if ((1 << (sig->signum_)) & pendingSignalsMask) {
						sig->callback();
						pendingSignalsMask &= ~(1 << (sig->signum_));
					}
				}
			}
			if (pendingSignalsMask) {
				fprintf(stderr, "Unexpected signals %08X", pendingSignalsMask);
				assertrx_dbg(false);
			}
		}
		if (ret >= 0 && timers_.size()) {
			if (!busy_loop || !(++count & 0x7F)) {
				now = steady_clock_w::now();
				count = 0;
			}
			while (timers_.size() && now >= timers_.front()->deadline_) {
				auto tim = timers_.front();
				timers_.erase(timers_.begin());
				tim->callback(1);
			}
		}
		h_vector<coroutine::routine_t, 5> yielded_tasks;
		std::swap(yielded_tasks, yielded_tasks_);
		for (auto id : yielded_tasks) {
			std::ignore = coroutine::resume(id);
		}
		yielded_tasks.clear();
	}
	remove_coro_cb();
}

void dynamic_loop::set(int fd, io* watcher, int events) {
	if (fd < 0) {
		return;
	}
	fds_.resize(std::max(fds_.size(), size_t(fd + 1)));
	int oldevents = fds_[fd].emask_;
	fds_[fd].emask_ = events;
	fds_[fd].watcher_ = watcher;
	backend_.set(fd, events, oldevents);
}

void dynamic_loop::stop(int fd) noexcept {
	if (fd < 0 || fd >= int(fds_.size())) {
		return;
	}
	if (!fds_[fd].emask_) {
		return;
	}
	fds_[fd].watcher_ = nullptr;
	fds_[fd].emask_ = 0;
	backend_.stop(fd);
}

void dynamic_loop::set(timer* watcher, double t) {
	auto it = std::find(timers_.begin(), timers_.end(), watcher);
	if (it != timers_.end()) {
		timers_.erase(it);
	}

	watcher->deadline_ = steady_clock_w::now();
	watcher->deadline_ += std::chrono::duration<int64_t, std::ratio<1, 1000000>>(int64_t(t * 1000000));
	it = std::lower_bound(timers_.begin(), timers_.end(), watcher,
						  [](const timer* lhs, const timer* rhs) { return lhs->deadline_ < rhs->deadline_; });
	timers_.insert(it, watcher);
}

void dynamic_loop::stop(timer* watcher) noexcept {
	auto it = std::find(timers_.begin(), timers_.end(), watcher);
	if (it != timers_.end()) {
		timers_.erase(it);
	}
}

void dynamic_loop::set(sig* watcher) {
	auto it = std::find(sigs_.begin(), sigs_.end(), watcher);
	if (it != sigs_.end()) {
		fprintf(stderr, "reindexer warning: sig %d already set\n", watcher->signum_);
		return;
	}
	sigs_.push_back(watcher);
#ifndef _WIN32
	struct sigaction new_action, old_action;
	new_action.sa_handler = net_ev_sighandler;
	sigemptyset(&new_action.sa_mask);
	new_action.sa_flags = 0;

	auto res = sigaction(watcher->signum_, &new_action, &old_action);
	if (res < 0) {
		fprintf(stderr, "reindexer error: sigaction error: %d\n", res);
		return;
	}
	watcher->old_action_ = old_action;
#else
	watcher->old_handler_ = signal(watcher->signum_, net_ev_sighandler);
#endif
}

void dynamic_loop::stop(sig* watcher) noexcept {
	auto it = std::find(sigs_.begin(), sigs_.end(), watcher);
	if (it == sigs_.end()) {
		fprintf(stderr, "reindexer warning: sig %d is not set\n", watcher->signum_);
		return;
	}
	sigs_.erase(it);
#ifndef _WIN32
	auto res = sigaction(watcher->signum_, &(watcher->old_action_), 0);
	if (res < 0) {
		fprintf(stderr, "reindexer error: sigaction error: %d\n", res);
		return;
	}
#else
	signal(watcher->signum_, watcher->old_handler_);
#endif
}

void dynamic_loop::set(async* watcher) {
	auto it = std::find(asyncs_.begin(), asyncs_.end(), watcher);
	if (it != asyncs_.end()) {
		return;
	}
	backend_.enable_asyncs();
	asyncs_.push_back(watcher);
}

void dynamic_loop::stop(async* watcher) noexcept {
	auto it = std::find(asyncs_.begin(), asyncs_.end(), watcher);
	if (it == asyncs_.end()) {
		return;
	}
	asyncs_.erase(it);
}

void dynamic_loop::send(async* watcher) noexcept {
	watcher->sent_ = true;
	bool was = async_sent_.exchange(true);
	if (!was) {
		backend_.send_async();
	}
}

bool dynamic_loop::is_active(const timer* watcher) const noexcept {
	return std::find(timers_.begin(), timers_.end(), watcher) != timers_.end();
}

void dynamic_loop::io_callback(int fd, int events) {
	if ((fd < 0) || (fd > int(fds_.size()))) {
		return;
	}

	if (fds_[fd].watcher_) {
		fds_[fd].watcher_->callback(events);
	}
}

void dynamic_loop::async_callback() {
	async_sent_ = false;
	for (auto async = asyncs_.begin(); async != asyncs_.end();) {
		if ((*async)->sent_) {
			(*async)->sent_ = false;
			(*async)->callback();
			async = asyncs_.begin();
		} else {
			async++;
		}
	}
}

void dynamic_loop::set_coro_cb() {
	[[maybe_unused]] bool res = coroutine::set_loop_completion_callback([this](coroutine::routine_t id) {
		auto found = std::find(running_tasks_.begin(), running_tasks_.end(), id);
		assertrx(found != running_tasks_.end());
		std::ignore = running_tasks_.erase(found);
		if (new_tasks_.empty() && running_tasks_.empty()) {
			coroTid_ = std::thread::id();
			break_loop();
		}
	});
	// if res is false, then callback was set before (probably by another loop)
	assertrx(res);
	coro_cb_is_set_ = true;
}

void dynamic_loop::remove_coro_cb() noexcept {
	[[maybe_unused]] bool res = coroutine::remove_loop_completion_callback();
	assertrx(res);
	coro_cb_is_set_ = false;
}

}  // namespace ev
}  // namespace net
}  // namespace reindexer
