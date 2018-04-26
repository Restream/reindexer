#include "ev.h"
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <algorithm>
#include <csignal>

namespace reindexer {
namespace net {
namespace ev {

#ifdef __linux__
void loop_epoll_backend::init(dynamic_loop *owner) {
	owner_ = owner;
	ctlfd_ = epoll_create1(EPOLL_CLOEXEC);
	if (ctlfd_ < 0) {
		perror("epoll_create");
	}
	events_.reserve(2048);
	events_.resize(1);
}

void loop_epoll_backend::set(int fd, int events, int oldevents) {
	epoll_event ev;
	ev.events = ((events & READ) ? int(EPOLLIN) | int(EPOLLHUP) : 0) | ((events & WRITE) ? int(EPOLLOUT) : 0) | EPOLLET;
	ev.data.fd = fd;
	if (epoll_ctl(ctlfd_, oldevents == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, fd, &ev) < 0) {
		perror("epoll_ctl EPOLL_CTL_MOD");
	}
	if (oldevents == 0) {
		events_.resize(events_.size() + 1);
	}
}

void loop_epoll_backend::stop(int fd) {
	epoll_event ev;
	ev.data.fd = fd;
	if (epoll_ctl(ctlfd_, EPOLL_CTL_DEL, fd, &ev) < 0) {
		printf("%d->", fd);
		perror("epoll_ctl EPOLL_CTL_DEL");
	}
	events_.pop_back();
}

int loop_epoll_backend::runonce(int64_t t) {
	int ret = epoll_wait(ctlfd_, &events_[0], events_.size(), t != -1 ? t / 1000 : -1);
	if (ret < 0) {
		perror("epoll_wait");
	}
	assert(ret <= static_cast<int>(events_.size()));

	for (int i = 0; i < ret; i++) {
		int events = ((events_[i].events & (EPOLLIN | EPOLLHUP)) ? READ : 0) | ((events_[i].events & EPOLLOUT) ? WRITE : 0);
		int fd = events_[i].data.fd;
		owner_->callback(fd, events);
	}
	return ret;
}
#endif

void loop_select_backend::init(dynamic_loop *owner) {
	owner_ = owner;
	maxfd_ = -1;
	FD_ZERO(&rfds_);
	FD_ZERO(&wfds_);
}

void loop_select_backend::set(int fd, int events, int /*oldevents*/) {
	if (fd > maxfd_) maxfd_ = fd;

	if (events & READ) {
		FD_SET(fd, &rfds_);
	} else {
		FD_CLR(fd, &rfds_);
	}

	if (events & WRITE) {
		FD_SET(fd, &wfds_);
	} else {
		FD_CLR(fd, &wfds_);
	}
}

void loop_select_backend::stop(int fd) {
	FD_CLR(fd, &rfds_);
	FD_CLR(fd, &wfds_);

	if (fd == maxfd_) maxfd_--;
}

int loop_select_backend::runonce(int64_t t) {
	timeval tv;
	tv.tv_sec = t / 1000000;
	tv.tv_usec = t % 1000000;
	fd_set rfds, wfds;
	memcpy(&rfds, &rfds_, 1 + (maxfd_ / 8));
	memcpy(&wfds, &wfds_, 1 + (maxfd_ / 8));

	int ret = select(maxfd_ + 1, &rfds, &wfds, nullptr, t != -1 ? &tv : nullptr);
	if (ret < 0) return ret;

	for (int fd = 0; fd < maxfd_ + 1; fd++) {
		int events = (FD_ISSET(fd, &rfds) ? READ : 0) | (FD_ISSET(fd, &wfds) ? WRITE : 0);
		if (events) {
			owner_->callback(fd, events);
		}
	}
	return ret;
}

static thread_local std::atomic<int> signalsMask;

extern "C" void net_ev_sighandler(int signum) { signalsMask |= (1 << signum); }

dynamic_loop::dynamic_loop() {
	fds_.reserve(2048);
	backend_.init(this);
}

dynamic_loop::~dynamic_loop() {
	if (async_fds_[0] >= 0) {
		stop(async_fds_[0]);
		close(async_fds_[0]);
	}
	if (async_fds_[1] >= 0) {
		close(async_fds_[1]);
	}
}

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
}

void dynamic_loop::stop(sig *watcher) {
	auto it = std::find(sigs_.begin(), sigs_.end(), watcher);
	if (it == sigs_.end()) {
		printf("sig %d is not set\n", watcher->signum_);
		return;
	}
	sigs_.erase(it);

	auto res = sigaction(watcher->signum_, &(watcher->old_action_), 0);
	if (res < 0) {
		printf("sigaction error: %d\n", res);
		return;
	}
}

void dynamic_loop::set(async *watcher) {
	auto it = std::find(asyncs_.begin(), asyncs_.end(), watcher);
	if (it != asyncs_.end()) {
		printf("async is already set\n");
		return;
	}
	if (async_fds_[0] < 0) {
		if (pipe(async_fds_) < 0) {
			perror("pipe:");
		}
		set(async_fds_[0], nullptr, READ);
	}

	asyncs_.push_back(watcher);
}

void dynamic_loop::stop(async *watcher) {
	auto it = std::find(asyncs_.begin(), asyncs_.end(), watcher);
	if (it == asyncs_.end()) {
		printf("async is not set\n");
		return;
	}
	asyncs_.erase(it);
}

void dynamic_loop::send(async *watcher) {
	watcher->sent_ = true;
	int res = write(async_fds_[1], " ", 1);
	(void)res;
}

void dynamic_loop::callback(int fd, int events) {
	if (fd < 0 || fd > int(fds_.size())) {
		return;
	}

	if (fd == async_fds_[0]) {
		char tmpBuf[256];
		int res = read(fd, tmpBuf, sizeof(tmpBuf));
		(void)res;
		for (auto async : asyncs_) {
			if (async->sent_) {
				async->callback();
				async->sent_ = false;
			}
		}
		return;
	}
	if (fds_[fd].watcher_) {
		fds_[fd].watcher_->callback(events);
	}
}
bool gEnableBusyLoop = false;

}  // namespace ev
}  // namespace net
}  // namespace reindexer
