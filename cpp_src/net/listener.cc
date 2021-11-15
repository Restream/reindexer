#include "listener.h"
#include <fcntl.h>
#include <chrono>
#include <cstdlib>
#include <thread>
#include "core/type_consts.h"
#include "net/http/serverconnection.h"
#include "server/pprof/gperf_profiler.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/flagguard.h"
#include "tools/logger.h"

namespace reindexer {
namespace net {

static std::atomic_uint_fast64_t counter_;

constexpr int kListenCount = 500;

Listener::Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared) : loop_(loop), shared_(shared), id_(counter_++) {
	io_.set<Listener, &Listener::io_accept>(this);
	io_.set(loop);
	timer_.set<Listener, &Listener::timeout_cb>(this);
	timer_.set(loop);
	timer_.start(5., 5.);
	async_.set<Listener, &Listener::async_cb>(this);
	async_.set(loop);
	async_.start();
	std::lock_guard<std::mutex> lck(shared_->lck_);
	shared_->listeners_.push_back(this);
}

Listener::Listener(ev::dynamic_loop &loop, ConnectionFactory connFactory, int maxListeners)
	: Listener(loop, std::make_shared<Shared>(connFactory, (maxListeners ? maxListeners : std::thread::hardware_concurrency()) + 1)) {}

Listener::~Listener() { io_.stop(); }

bool Listener::Bind(string addr) {
	if (shared_->sock_.valid()) {
		return false;
	}

	shared_->addr_ = addr;

	if (shared_->sock_.bind(addr) < 0) {
		return false;
	}

	if (shared_->sock_.listen(kListenCount) < 0) {
		perror("listen error");
		return false;
	}

	if (shared_->count_ == 1) {
		++shared_->count_;
		std::thread th(&Listener::clone, shared_);
		th.detach();
		reserveStack();
	}
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

	if (shared_->terminating_) {
		client.close();
		logPrintf(LogWarning, "Can't accept connection. Listener is terminating!");
		return;
	}

	bool connIsActive = true;
	std::unique_ptr<IServerConnection> conn;
	std::unique_lock<std::mutex> lck(shared_->lck_);
	if (shared_->idle_.size()) {
		conn = std::move(shared_->idle_.back());
		shared_->idle_.pop_back();
		lck.unlock();
		conn->Attach(loop_);
		conn->Restart(client.fd());
	} else {
		lck.unlock();
		conn = std::unique_ptr<IServerConnection>(shared_->connFactory_(loop_, client.fd()));
		connIsActive = !conn->IsFinished();
	}

	if (connIsActive) {
		lck.lock();
		connections_.emplace_back(std::move(conn));
		rebalance();
	}

	int count = shared_->count_.load();
	while (count < shared_->maxListeners_) {
		if (shared_->count_.compare_exchange_strong(count, count + 1)) {
			std::thread th(&Listener::clone, shared_);
			th.detach();
			break;
		}
	}
}

void Listener::timeout_cb(ev::periodic &, int) {
	const bool enableReuseIdle = !std::getenv("REINDEXER_NOREUSEIDLE");

	std::unique_lock<std::mutex> lck(shared_->lck_);
	// Move finished connections to idle connections pool
	for (unsigned i = 0; i < connections_.size();) {
		if (connections_[i]->IsFinished()) {
			connections_[i]->Detach();
			if (enableReuseIdle) {	// -V547
				shared_->idle_.push_back(std::move(connections_[i]));
			} else {
				connections_[i].reset();
			}

			if (i != connections_.size() - 1) connections_[i] = std::move(connections_.back());
			connections_.pop_back();
			shared_->ts_ = std::chrono::steady_clock::now();
		} else {
			i++;
		}
	}

	// Clear all idle connections, after 300 sec
	if (shared_->idle_.size() && std::chrono::steady_clock::now() - shared_->ts_ > std::chrono::seconds(300)) {
		logPrintf(LogInfo, "Cleanup idle connections. %d cleared", shared_->idle_.size());
		shared_->idle_.clear();
	}

	rebalance();
	const size_t curConnCount = connections_.size();
	if (curConnCount != 0) {
		logPrintf(LogTrace, "Listener(%s) %d stats: %d connections", shared_->addr_, id_, curConnCount);
	}
}

void Listener::rebalance() {
	const bool enableRebalance = !std::getenv("REINDEXER_NOREBALANCE");
	if (enableRebalance && this != shared_->listeners_.front()) {
		// Try to rebalance
		for (;;) {
			const size_t curConnCount = connections_.size();
			size_t minConnCount = std::numeric_limits<size_t>::max();
			auto minIt = shared_->listeners_.begin() + 1;  // Rebalancing to the first listener is not allowed

			for (auto it = minIt; it != shared_->listeners_.end(); ++it) {
				const size_t connCount = (*it)->connections_.size();
				if (connCount < minConnCount) {
					minIt = it;
					minConnCount = connCount;
				}
			}

			if (minIt != shared_->listeners_.end() && minConnCount + 1 < curConnCount) {
				logPrintf(LogInfo, "Rebalance connection from listener %d to %d", id_, (*minIt)->id_);
				auto conn = std::move(connections_.back());
				conn->Detach();
				(*minIt)->connections_.push_back(std::move(conn));
				(*minIt)->async_.send();
				connections_.pop_back();
			} else {
				break;
			}
		}
	}
}

void Listener::async_cb(ev::async &watcher) {
	logPrintf(LogInfo, "Listener(%s) %d async received", shared_->addr_, id_);
	std::unique_lock<std::mutex> lck(shared_->lck_);
	for (auto &it : connections_) {
		if (!it->IsFinished()) it->Attach(loop_);
	}
	watcher.loop.break_loop();
}

void Listener::Stop() {
	std::lock_guard<std::mutex> lck(shared_->lck_);
	shared_->terminating_ = true;
	for (auto listener : shared_->listeners_) {
		listener->async_.send();
	}
	assert(this == shared_->listeners_.front());
	while (shared_->count_.load() > 1) {
		shared_->lck_.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		shared_->lck_.lock();
	}
}

void Listener::clone(std::shared_ptr<Shared> shared) {
	std::unique_lock<std::mutex> lck(shared->lck_, std::defer_lock);
	{
		ev::dynamic_loop loop;
		Listener listener(loop, shared);
#if REINDEX_WITH_GPERFTOOLS
		if (alloc_ext::TCMallocIsAvailable()) {
			reindexer_server::pprof::ProfilerRegisterThread();
		}
#endif
		listener.io_.start(listener.shared_->sock_.fd(), ev::READ);
		while (!listener.shared_->terminating_) {
			loop.run();
		}
		lck.lock();
		auto it = std::find(shared->listeners_.begin(), shared->listeners_.end(), &listener);
		assert(it != shared->listeners_.end());
		shared->listeners_.erase(it);
	}
	--shared->count_;
}

void Listener::reserveStack() {
	char placeholder[0x8000];
	for (size_t i = 0; i < sizeof(placeholder); i += 4096) placeholder[i] = i & 0xFF;
}

Listener::Shared::Shared(ConnectionFactory connFactory, int maxListeners)
	: maxListeners_(maxListeners), count_(1), connFactory_(connFactory), terminating_(false) {}

Listener::Shared::~Shared() { sock_.close(); }

ForkedListener::ForkedListener(ev::dynamic_loop &loop, ConnectionFactory connFactory) : connFactory_(connFactory), loop_(loop) {
	io_.set<ForkedListener, &ForkedListener::io_accept>(this);
	io_.set(loop);
	async_.set<ForkedListener, &ForkedListener::async_cb>(this);
	async_.set(loop);
	async_.start();
}

ForkedListener::~ForkedListener() {
	io_.stop();
	if (!terminating_ || runningThreadsCount_) {
		Stop();
	}
	sock_.close();
}

bool ForkedListener::Bind(string addr) {
	if (sock_.valid()) {
		return false;
	}

	addr_ = addr;

	if (sock_.bind(addr) < 0) {
		return false;
	}

	if (sock_.listen(kListenCount) < 0) {
		perror("listen error");
		return false;
	}

	io_.start(sock_.fd(), ev::READ);
	return true;
}

void ForkedListener::io_accept(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) {
		perror("got invalid event");
		return;
	}
	if (terminating_) {
		return;
	}

	auto client = sock_.accept();

	if (!client.valid()) {
		return;
	}

	++runningThreadsCount_;
	if (terminating_) {
		--runningThreadsCount_;
		client.close();
		logPrintf(LogWarning, "Can't accept connection. Listener is terminating!");
		return;
	}

	std::thread th([this, client] {
		try {
#if REINDEX_WITH_GPERFTOOLS
			if (alloc_ext::TCMallocIsAvailable()) {
				reindexer_server::pprof::ProfilerRegisterThread();
			}
#endif
			ev::dynamic_loop loop;
			ev::async async;
			async.set([](ev::async &a) { a.loop.break_loop(); });
			async.set(loop);
			async.start();

			Worker w(std::unique_ptr<IServerConnection>(connFactory_(loop, client.fd())), async);
			auto pc = w.conn.get();
			if (pc->IsFinished()) {	 // Connection may be closed inside Worker construction
				pc->Detach();
			} else {
				std::unique_lock<std::mutex> lck(lck_);
				logPrintf(LogTrace, "Listener (%s) dedicated thread started. %d total", addr_, workers_.size());
				workers_.push_back(std::move(w));
				lck.unlock();
				while (!terminating_) {
					loop.run();
					if (pc->IsFinished()) {
						pc->Detach();
						break;
					}
				}
				lck.lock();
				auto it = std::find_if(workers_.begin(), workers_.end(), [&pc](const Worker &cw) { return cw.conn.get() == pc; });
				assert(it != workers_.end());
				workers_.erase(it);
				logPrintf(LogTrace, "Listener (%s) dedicated thread finished. %d left", addr_, workers_.size());
			}
		} catch (Error &e) {
			logPrintf(LogError, "Unhandled excpetion in listener thread: %s", e.what());
		} catch (std::exception &e) {
			logPrintf(LogError, "Unhandled excpetion in listener thread: %s", e.what());
		} catch (...) {
			logPrintf(LogError, "Unhandled excpetion in listener thread");
		}
		std::unique_lock<std::mutex> lck(lck_);
		--runningThreadsCount_;
	});
	th.detach();
}

void ForkedListener::async_cb(ev::async &watcher) {
	logPrintf(LogInfo, "Listener(%s) async received", addr_);
	watcher.loop.break_loop();
}

void ForkedListener::Stop() {
	terminating_ = true;
	async_.send();
	std::unique_lock lck(lck_);
	for (auto &worker : workers_) {
		worker.async->send();
	}
	while (runningThreadsCount_ || !workers_.empty()) {
		lck.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		lck.lock();
	}
}

}  // namespace net
}  // namespace reindexer
