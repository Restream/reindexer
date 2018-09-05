#include "ev.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include "tools/oscompat.h"
#ifdef __linux__
#include <sys/epoll.h>
#endif

namespace reindexer {
namespace net {
namespace ev {

#ifndef _WIN32
loop_posix_base::loop_posix_base() {}
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
			perror("pipe:");
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

class loop_select_backend_private {
public:
	fd_set rfds_, wfds_;
	int maxfd_;
};

loop_select_backend::loop_select_backend() : private_(new loop_select_backend_private) {}
loop_select_backend::~loop_select_backend() {}

void loop_select_backend::init(dynamic_loop *owner) {
	owner_ = owner;
	private_->maxfd_ = -1;
	FD_ZERO(&private_->rfds_);
	FD_ZERO(&private_->wfds_);
}

void loop_select_backend::set(int fd, int events, int /*oldevents*/) {
	assert(fd < capacity());

	if (fd > private_->maxfd_) private_->maxfd_ = fd;

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

void loop_select_backend::stop(int fd) {
	FD_CLR(fd, &private_->rfds_);
	FD_CLR(fd, &private_->wfds_);

	if (fd == private_->maxfd_) private_->maxfd_--;
}

int loop_select_backend::runonce(int64_t t) {
	timeval tv;
	tv.tv_sec = t / 1000000;
	tv.tv_usec = t % 1000000;
	fd_set rfds, wfds;

	memcpy(&rfds, &private_->rfds_, 1 + (private_->maxfd_ / 8));
	memcpy(&wfds, &private_->wfds_, 1 + (private_->maxfd_ / 8));

	int ret = select(private_->maxfd_ + 1, &rfds, &wfds, nullptr, t != -1 ? &tv : nullptr);
	if (ret < 0) return ret;

	for (int fd = 0; fd < private_->maxfd_ + 1; fd++) {
		int events = (FD_ISSET(fd, &rfds) ? READ : 0) | (FD_ISSET(fd, &wfds) ? WRITE : 0);
		if (events) {
			if (!check_async(fd)) owner_->io_callback(fd, events);
		}
	}
	return ret;
}

int loop_select_backend::capacity() { return FD_SETSIZE; }
#endif

#ifdef __linux__

class loop_epoll_backend_private {
public:
	int ctlfd_ = -1;
	std::vector<epoll_event> events_;
};

loop_epoll_backend::loop_epoll_backend() : private_(new loop_epoll_backend_private) {}

loop_epoll_backend::~loop_epoll_backend() {
	close(private_->ctlfd_);
	private_->ctlfd_ = -1;
}

void loop_epoll_backend::init(dynamic_loop *owner) {
	owner_ = owner;
	private_->ctlfd_ = epoll_create1(EPOLL_CLOEXEC);
	if (private_->ctlfd_ < 0) {
		perror("epoll_create");
	}
	private_->events_.reserve(2048);
	private_->events_.resize(1);
}

void loop_epoll_backend::set(int fd, int events, int oldevents) {
	epoll_event ev;
	ev.events = ((events & READ) ? int(EPOLLIN) | int(EPOLLHUP) : 0) | ((events & WRITE) ? int(EPOLLOUT) : 0) /*| EPOLLET*/;
	ev.data.fd = fd;
	if (epoll_ctl(private_->ctlfd_, oldevents == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, fd, &ev) < 0) {
		perror("epoll_ctl EPOLL_CTL_MOD");
	}
	if (oldevents == 0) {
		private_->events_.resize(private_->events_.size() + 1);
	}
}

void loop_epoll_backend::stop(int fd) {
	epoll_event ev;
	ev.data.fd = fd;
	if (epoll_ctl(private_->ctlfd_, EPOLL_CTL_DEL, fd, &ev) < 0) {
		perror("epoll_ctl EPOLL_CTL_DEL");
	}
	private_->events_.pop_back();
}

int loop_epoll_backend::runonce(int64_t t) {
	int ret = epoll_wait(private_->ctlfd_, &private_->events_[0], private_->events_.size(), t != -1 ? t / 1000 : -1);

	assert(ret <= static_cast<int>(private_->events_.size()));

	for (int i = 0; i < ret; i++) {
		int events =
			((private_->events_[i].events & (EPOLLIN | EPOLLHUP)) ? READ : 0) | ((private_->events_[i].events & EPOLLOUT) ? WRITE : 0);
		int fd = private_->events_[i].data.fd;
		if (!check_async(fd)) owner_->io_callback(fd, events);
	}
	return ret;
}

int loop_epoll_backend::capacity() { return 500000; }

#endif

#ifdef _WIN32
struct win_fd {
	HANDLE hEvent = INVALID_HANDLE_VALUE;
	int fd = -1;
};

class loop_wsa_backend_private {
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
	for (auto &fd : private_->wfds_) {
		CloseHandle(fd.hEvent);
		fd.hEvent = INVALID_HANDLE_VALUE;
	}
	if (private_->hAsyncEvent != INVALID_HANDLE_VALUE) {
		CloseHandle(private_->hAsyncEvent);
	}
}

void loop_wsa_backend::init(dynamic_loop *owner) { owner_ = owner; }

void loop_wsa_backend::set(int fd, int events, int oldevents) {
	auto it = std::find_if(private_->wfds_.begin(), private_->wfds_.end(), [&](const win_fd &wfd) { return wfd.fd == fd; });
	if (it == private_->wfds_.end()) {
		assert(int(private_->wfds_.size()) < capacity());
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
	auto it = std::find_if(private_->wfds_.begin(), private_->wfds_.end(), [&](const win_fd &wfd) { return wfd.fd == fd; });
	if (it == private_->wfds_.end()) {
		return;
	}
	CloseHandle(it->hEvent);
	private_->wfds_.erase(it);
}

int loop_wsa_backend::runonce(int64_t t) {
	HANDLE objs[WSA_MAXIMUM_WAIT_EVENTS];
	unsigned ecount = private_->wfds_.size();
	for (unsigned i = 0; i < ecount; i++) objs[i] = private_->wfds_[i].hEvent;

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
		perror("WaitForMultipleObjects");
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

extern "C" void net_ev_sighandler(int signum) {
	signalsMask |= (1 << signum);
#ifdef _WIN32
	SetEvent(gSigEvent);
#endif
}

dynamic_loop::dynamic_loop() {
	fds_.reserve(2048);
	backend_.init(this);
}

dynamic_loop::~dynamic_loop() {}

void dynamic_loop::run() {
	break_ = false;
	auto now = std::chrono::steady_clock::now();
	int count = 0;
	while (!break_) {
		int tv = gEnableBusyLoop ? 0 : -1;

		if (!gEnableBusyLoop && timers_.size()) {
			tv = std::chrono::duration_cast<std::chrono::microseconds>(timers_.front()->deadline_ - now).count();
			if (tv < 0) tv = 0;
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
				printf("Unexpected signals %08X", pendingSignalsMask);
			}
		}
		if (ret >= 0 && timers_.size()) {
			if (!gEnableBusyLoop || !(++count % 100)) {
				now = std::chrono::steady_clock::now();
			}
			while (timers_.size() && now >= timers_.front()->deadline_) {
				auto tim = timers_.front();
				timers_.erase(timers_.begin());
				tim->callback(1);
			}
		}
	}
}

void dynamic_loop::break_loop() {
	//
	break_ = true;
}

void dynamic_loop::set(int fd, io *watcher, int events) {
	if (fd < 0) {
		return;
	}
	fds_.resize(std::max(fds_.size(), size_t(fd + 1)));
	int oldevents = fds_[fd].emask_;
	fds_[fd].emask_ = events;
	fds_[fd].watcher_ = watcher;
	backend_.set(fd, events, oldevents);
}

void dynamic_loop::stop(int fd) {
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

void dynamic_loop::set(timer *watcher, double t) {
	auto it = std::find(timers_.begin(), timers_.end(), watcher);
	if (it != timers_.end()) {
		timers_.erase(it);
	}

	watcher->deadline_ = std::chrono::steady_clock::now();
	watcher->deadline_ += std::chrono::duration<int64_t, std::ratio<1, 1000000>>(int64_t(t * 1000000));
	it = std::lower_bound(timers_.begin(), timers_.end(), watcher,
						  [](const timer *lhs, const timer *rhs) { return lhs->deadline_ < rhs->deadline_; });
	timers_.insert(it, watcher);
}

void dynamic_loop::stop(timer *watcher) {
	auto it = std::find(timers_.begin(), timers_.end(), watcher);
	if (it != timers_.end()) {
		timers_.erase(it);
	}
}

void dynamic_loop::set(sig *watcher) {
	auto it = std::find(sigs_.begin(), sigs_.end(), watcher);
	if (it != sigs_.end()) {
		printf("sig %d already set\n", watcher->signum_);
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
		printf("sigaction error: %d\n", res);
		return;
	}
	watcher->old_action_ = old_action;
#else
	watcher->old_handler_ = signal(watcher->signum_, net_ev_sighandler);
#endif
}

void dynamic_loop::stop(sig *watcher) {
	auto it = std::find(sigs_.begin(), sigs_.end(), watcher);
	if (it == sigs_.end()) {
		printf("sig %d is not set\n", watcher->signum_);
		return;
	}
	sigs_.erase(it);
#ifndef _WIN32
	auto res = sigaction(watcher->signum_, &(watcher->old_action_), 0);
	if (res < 0) {
		printf("sigaction error: %d\n", res);
		return;
	}
#else
	signal(watcher->signum_, watcher->old_handler_);
#endif
}

void dynamic_loop::set(async *watcher) {
	auto it = std::find(asyncs_.begin(), asyncs_.end(), watcher);
	if (it != asyncs_.end()) {
		printf("async is already set\n");
		return;
	}
	backend_.enable_asyncs();
	asyncs_.push_back(watcher);
}

void dynamic_loop::stop(async *watcher) {
	auto it = std::find(asyncs_.begin(), asyncs_.end(), watcher);
	if (it == asyncs_.end()) {
		return;
	}
	asyncs_.erase(it);
}

void dynamic_loop::send(async *watcher) {
	watcher->sent_ = true;
	backend_.send_async();
}

void dynamic_loop::io_callback(int fd, int events) {
	if (fd < 0 || fd > int(fds_.size())) {
		return;
	}

	if (fds_[fd].watcher_) {
		fds_[fd].watcher_->callback(events);
	}
}

void dynamic_loop::async_callback() {
	for (auto async : asyncs_) {
		if (async->sent_) {
			async->callback();
			async->sent_ = false;
		}
	}
}

bool gEnableBusyLoop = false;

}  // namespace ev
}  // namespace net
}  // namespace reindexer
