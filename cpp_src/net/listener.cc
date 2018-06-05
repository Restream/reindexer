#include "listener.h"
#include <fcntl.h>
#include <chrono>
#include <thread>
#include "core/type_consts.h"
#include "net/http/serverconnection.h"
#include "tools/logger.h"

#if REINDEX_WITH_GPERFTOOLS
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#else
static void ProfilerRegisterThread(){};
#endif

namespace reindexer {

namespace net {

static atomic<int> counter_;

Listener::Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared) : loop_(loop), shared_(shared), idleConns_(0), id_(counter_++) {
	io_.set<Listener, &Listener::io_accept>(this);
	io_.set(loop);
	timer_.set<Listener, &Listener::timeout_cb>(this);
	timer_.set(loop);
	timer_.start(15., 15.);
	async_.set<Listener, &Listener::async_cb>(this);
	async_.set(loop);
	async_.start();
	std::lock_guard<std::mutex> lck(shared_->lck_);
	shared_->listeners_.push_back(this);
}

Listener::Listener(ev::dynamic_loop &loop, ConnectionFactory connFactory, int maxListeners)
	: Listener(loop, std::make_shared<Shared>(connFactory, maxListeners ? maxListeners : std::thread::hardware_concurrency())) {}

Listener::~Listener() {
	io_.stop();
	std::lock_guard<std::mutex> lck(shared_->lck_);
	auto it = std::find(shared_->listeners_.begin(), shared_->listeners_.end(), this);
	assert(it != shared_->listeners_.end());
	shared_->listeners_.erase(it);
	shared_->count_--;
}

bool Listener::Bind(string addr) {
	if (shared_->sock_.valid()) {
		return false;
	}

	shared_->addr_ = addr;

	if (shared_->sock_.bind(addr.c_str()) < 0) {
		return false;
	}

	if (shared_->sock_.listen(500) < 0) {
		perror("listen error");
		return false;
	}

	io_.start(shared_->sock_.fd(), ev::READ);
	reserveStack();
	return true;
}

void Listener::io_accept(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) {
		perror("got invalid event");
		return;
	}

	auto client = shared_->sock_.accept();

	if (!client.valid()) {
		return;
	}

	if (idleConns_) {
		connectons_[--idleConns_]->Restart(client.fd());
	} else {
		connectons_.push_back(std::unique_ptr<IServerConnection>(shared_->connFactory_(loop_, client.fd())));
	}
	connCount_++;
	shared_->lck_.lock();
	if (shared_->count_ < shared_->maxListeners_) {
		shared_->count_++;
		std::thread th(&Listener::clone, shared_);
		th.detach();
	}
	shared_->lck_.unlock();
}

void Listener::timeout_cb(ev::periodic &, int) {
	assert(idleConns_ <= int(connectons_.size()));
	for (auto it = connectons_.begin() + idleConns_; it != connectons_.end(); it++) {
		if ((*it)->IsFinished()) {
			std::swap(*it, connectons_[idleConns_]);
			++idleConns_;
			connCount_--;
		}
	}
	if (connectons_.size() - idleConns_ != 0) {
		logPrintf(LogInfo, "Listener(%s) %d stats: %d connections, %d idle", shared_->addr_.c_str(), id_,
				  int(connectons_.size() - idleConns_), idleConns_);
	}
}

void Listener::async_cb(ev::async &watcher) {
	logPrintf(LogInfo, "Listener(%s) %d async received", shared_->addr_.c_str(), id_);
	watcher.loop.break_loop();
}

void Listener::Stop() {
	std::lock_guard<std::mutex> lck(shared_->lck_);
	shared_->terminating_ = true;
	for (auto listener : shared_->listeners_) {
		listener->async_.send();
	}
	if (shared_->listeners_.size() && this == shared_->listeners_.front()) {
		while (shared_->listeners_.size() != 1) {
			shared_->lck_.unlock();
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			shared_->lck_.lock();
		}
	}
}

void Listener::Fork(int clones) {
	for (int i = 0; i < clones; i++) {
		std::thread th(&Listener::clone, shared_);
		th.detach();
		shared_->count_++;
	}
}

void Listener::clone(std::shared_ptr<Shared> shared) {
	ev::dynamic_loop loop;
	Listener listener(loop, shared);
	ProfilerRegisterThread();
	listener.io_.start(listener.shared_->sock_.fd(), ev::READ);
	while (!listener.shared_->terminating_) {
		loop.run();
	}
}

void Listener::reserveStack() {
	char placeholder[0x8000];
	for (size_t i = 0; i < sizeof(placeholder); i += 4096) placeholder[i] = i & 0xFF;
}

Listener::Shared::Shared(ConnectionFactory connFactory, int maxListeners)
	: maxListeners_(maxListeners), count_(1), connFactory_(connFactory), terminating_(false) {}

Listener::Shared::~Shared() { sock_.close(); }

}  // namespace net
}  // namespace reindexer
