#include "listener.h"
#include <fcntl.h>
#include <chrono>
#include <cstdlib>
#include <thread>
#include "core/type_consts.h"
#include "net/http/serverconnection.h"
#include "server/pprof/gperf_profiler.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"

namespace reindexer {
namespace net {

static std::atomic_uint_fast64_t counter_;

constexpr int kListenCount = 500;

template <ListenerType LT>
Listener<LT>::Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared)
	: loop_(loop), shared_(std::move(shared)), id_(counter_.fetch_add(1, std::memory_order_relaxed)) {
	io_.set<Listener, &Listener::io_accept>(this);
	io_.set(loop);
	timer_.set<Listener, &Listener::timeout_cb>(this);
	timer_.set(loop);
	timer_.start(5., 5.);
	async_.set<Listener, &Listener::async_cb>(this);
	async_.set(loop);
	async_.start();
	std::lock_guard lck(shared_->mtx_);
	shared_->listeners_.emplace_back(this);
}

template <ListenerType LT>
Listener<LT>::Listener(ev::dynamic_loop &loop, ConnectionFactory &&connFactory, int maxListeners)
	: Listener(loop, std::make_shared<Shared>(std::move(connFactory), (maxListeners ? maxListeners : hardware_concurrency()) + 1)) {}

template <ListenerType LT>
Listener<LT>::~Listener() {
	io_.stop();
}

template <ListenerType LT>
bool Listener<LT>::Bind(std::string addr, socket_domain type) {
	if (shared_->sock_.valid()) {
		return false;
	}

	shared_->addr_ = std::move(addr);

	if (shared_->sock_.bind(shared_->addr_, type) < 0) {
		return false;
	}

	if (shared_->sock_.listen(kListenCount) < 0) {
		perror("listen error");
		return false;
	}

	if constexpr (LT == ListenerType::Mixed) {
		if (shared_->listeners_.size() == 1) {
			std::thread th(&Listener::clone, std::make_unique<ListeningThreadData>(shared_));
			th.detach();
			// Current listener is the only one, which reads the first messages
			io_.start(shared_->sock_.fd(), ev::READ);
			reserve_stack();
		}
	} else {
		if (shared_->listenersCount_ == 1) {
			shared_->listenersCount_.fetch_add(1, std::memory_order_release);
			std::thread th(&Listener::clone, std::make_unique<ListeningThreadData>(shared_));
			th.detach();
		}
	}
	return true;
}

template <ListenerType LT>
void Listener<LT>::io_accept(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) {
		perror("got invalid event");
		return;
	}

	auto client = shared_->sock_.accept();

	if (!client.valid()) {
		return;
	}

	if (shared_->terminating_) {
		logPrintf(LogWarning, "Can't accept connection. Listener is terminating!");
		return;
	}

	bool connIsActive = true;
	std::unique_ptr<IServerConnection> conn;
	std::unique_lock<std::mutex> lck(shared_->mtx_);
	if (shared_->idle_.size()) {
		conn = std::move(shared_->idle_.back());
		shared_->idle_.pop_back();
		lck.unlock();
		conn->Attach(loop_);
		conn->Restart(std::move(client));
	} else {
		lck.unlock();
		conn = std::unique_ptr<IServerConnection>(shared_->connFactory_(loop_, std::move(client), LT == ListenerType::Mixed));
		connIsActive = !conn->IsFinished();
	}

	if (connIsActive) {
		if constexpr (LT == ListenerType::Mixed) {
			const auto balancingType = conn->GetBalancingType();
			switch (balancingType) {
				case IServerConnection::BalancingType::None:
					logPrintf(LogError, "Listener: BalancingType::None. Interpreting as 'shared'");
					[[fallthrough]];
				case IServerConnection::BalancingType::Shared:
					shared_->connCount_.fetch_add(1, std::memory_order_release);
					startup_shared_thread();

					lck.lock();
					connections_.emplace_back(std::move(conn));
					rebalance_from_acceptor();
					return;
				case IServerConnection::BalancingType::Dedicated:
					conn->Detach();
					run_dedicated_thread(std::move(conn));
					break;
				case IServerConnection::BalancingType::NotSet: {
					auto c = conn.get();
					accepted_.emplace(std::move(conn));
					c->SetRebalanceCallback(
						[this](IServerConnection *c, IServerConnection::BalancingType type) { rebalance_conn(c, type); });
					break;
				}
			}
			startup_shared_thread();
		} else {
			shared_->connCount_.fetch_add(1, std::memory_order_release);
			startup_shared_thread();

			lck.lock();
			connections_.emplace_back(std::move(conn));
			rebalance();
			lck.unlock();
		}
	}
}

template <ListenerType LT>
void Listener<LT>::timeout_cb(ev::periodic &, int) {
	const bool enableReuseIdle = !std::getenv("REINDEXER_NOREUSEIDLE");

	std::unique_lock lck(shared_->mtx_);
	// Move finished connections to idle connections pool
	for (unsigned i = 0; i < connections_.size();) {
		if (connections_[i]->IsFinished()) {
			connections_[i]->Detach();
			shared_->connCount_.fetch_sub(1, std::memory_order_release);
			if (enableReuseIdle) {	// -V547
				shared_->idle_.emplace_back(std::move(connections_[i]));
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

template <ListenerType LT>
void Listener<LT>::rebalance() {
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
				(*minIt)->connections_.emplace_back(std::move(conn));
				(*minIt)->async_.send();
				connections_.pop_back();
			} else {
				break;
			}
		}
	}
}

template <ListenerType LT>
void Listener<LT>::rebalance_from_acceptor() {
	if (this != shared_->listeners_.front()) {
		assertrx(false);
		return;
	}
	if (shared_->listeners_.size() < 2) {
		logPrintf(LogError, "Unable to rebalance connections from acceptor: there are no other listeners");
		connections_.clear();
		return;
	}
	// Try to rebalance
	while (connections_.size()) {
		size_t minConnCount = std::numeric_limits<size_t>::max();
		auto minIt = shared_->listeners_.begin() + 1;  // Rebalancing to the first listener (acceptor) is not allowed

		for (auto it = minIt; it != shared_->listeners_.end(); ++it) {
			const size_t connCount = (*it)->connections_.size();
			if (connCount < minConnCount) {
				minIt = it;
				minConnCount = connCount;
			}
		}
		assertrx(minIt != shared_->listeners_.end());
		auto conn = std::move(connections_.back());
		conn->Detach();
		(*minIt)->connections_.emplace_back(std::move(conn));
		(*minIt)->async_.send();
		connections_.pop_back();
	}
}

template <ListenerType LT>
void Listener<LT>::rebalance_conn(IServerConnection *c, IServerConnection::BalancingType type) {
	std::unique_lock lck(shared_->mtx_);
	auto found = accepted_.find(c);
	if (found == accepted_.end()) {
		logPrintf(LogError, "Rebalance was requested for incorrect connection ptr: %08X", uint64_t(c));
		return;
	}
	auto fc = std::move(*found);
	accepted_.erase(found);
	if (type == IServerConnection::BalancingType::Dedicated) {
		lck.unlock();
		fc->Detach();
		run_dedicated_thread(std::move(fc));
	} else {
		connections_.emplace_back(std::move(fc));
		shared_->connCount_.fetch_add(1, std::memory_order_release);
		rebalance_from_acceptor();
		lck.unlock();
		startup_shared_thread();
	}
}

template <ListenerType LT>
void Listener<LT>::run_dedicated_thread(std::unique_ptr<IServerConnection> &&conn) {
	std::thread th([this, conn = std::move(conn)]() mutable {
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

			typename Shared::Worker w(std::move(conn), async);
			auto pc = w.conn.get();
			std::unique_lock lck(shared_->mtx_);
			shared_->dedicatedWorkers_.emplace_back(std::move(w));
			logPrintf(LogTrace, "Listener (%s) dedicated thread started. %d total", shared_->addr_, shared_->dedicatedWorkers_.size());
			lck.unlock();
			pc->Attach(loop);
			pc->HandlePendingData();
			while (!shared_->terminating_) {
				if (pc->IsFinished()) {
					pc->Detach();
					break;
				}
				loop.run();
			}
			lck.lock();
			const auto it = std::find_if(shared_->dedicatedWorkers_.begin(), shared_->dedicatedWorkers_.end(),
										 [&pc](const typename Shared::Worker &cw) { return cw.conn.get() == pc; });
			assertrx(it != shared_->dedicatedWorkers_.end());
			shared_->dedicatedWorkers_.erase(it);
			logPrintf(LogTrace, "Listener (%s) dedicated thread finished. %d left", shared_->addr_, shared_->dedicatedWorkers_.size());
		} catch (Error &e) {
			logPrintf(LogError, "Unhandled excpetion in listener thread: %s", e.what());
		} catch (std::exception &e) {
			logPrintf(LogError, "Unhandled excpetion in listener thread: %s", e.what());
		} catch (...) {
			logPrintf(LogError, "Unhandled excpetion in listener thread");
		}
	});
	th.detach();
}

template <ListenerType LT>
void Listener<LT>::startup_shared_thread() {
	int count = shared_->listenersCount_.load(std::memory_order_relaxed);
	while (count < shared_->maxListeners_ && count <= (shared_->connCount_.load(std::memory_order_acquire) + 1)) {
		if (shared_->listenersCount_.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel)) {
			logPrintf(LogTrace, "Listener (%s). Creating new shared thread (%d total)", shared_->addr_, count);
			std::thread th(&Listener::clone, std::make_unique<ListeningThreadData>(shared_));
			th.detach();
			break;
		}
	}
}

template <ListenerType LT>
void Listener<LT>::async_cb(ev::async &watcher) {
	logPrintf(LogInfo, "Listener(%s) %d async received", shared_->addr_, id_);
	h_vector<IServerConnection *, 32> conns;
	{
		std::lock_guard lck(shared_->mtx_);
		for (auto &it : connections_) {
			if (!it->IsFinished()) {
				if constexpr (LT == ListenerType::Mixed) {
					if (it->HasPendingData()) {
						conns.emplace_back(it.get());
					} else {
						it->Attach(loop_);
					}
				} else {
					it->Attach(loop_);
				}
			}
		}
	}
	if constexpr (LT == ListenerType::Mixed) {
		for (auto &conn : conns) {
			conn->Attach(loop_);
			conn->HandlePendingData();
		}
	}
	watcher.loop.break_loop();
}

template <ListenerType LT>
void Listener<LT>::Stop() {
	std::unique_lock lck(shared_->mtx_);
	shared_->terminating_ = true;
	for (auto &listener : shared_->listeners_) {
		listener->async_.send();
	}
	for (auto &worker : shared_->dedicatedWorkers_) {
		worker.async->send();
	}
	assertrx(this == shared_->listeners_.front());
	while (shared_->listeners_.size() > 1 || shared_->dedicatedWorkers_.size()) {
		lck.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		lck.lock();
	}
}

template <ListenerType LT>
void Listener<LT>::clone(std::unique_ptr<ListeningThreadData> d) noexcept {
	assertrx(d);
	auto &shared = d->GetShared();
	const auto &listener = d->GetListener();

	try {
#if REINDEX_WITH_GPERFTOOLS
		if (alloc_ext::TCMallocIsAvailable()) {
			reindexer_server::pprof::ProfilerRegisterThread();
		}
#endif
		d->Loop();
	} catch (Error &e) {
		logPrintf(LogError, "Unhandled excpetion in listener thread (%s): %s", shared.addr_, e.what());
	} catch (std::exception &e) {
		logPrintf(LogError, "Unhandled excpetion in listener thread (%s): %s", shared.addr_, e.what());
	} catch (...) {
		logPrintf(LogError, "Unhandled excpetion in listener thread (%s): <unknown>", shared.addr_);
	}

	std::lock_guard lck(shared.mtx_);
	auto it = std::find(shared.listeners_.begin(), shared.listeners_.end(), &listener);
	assertrx(it != shared.listeners_.end());
	shared.listeners_.erase(it);
	d.reset();
	shared.listenersCount_.fetch_sub(1, std::memory_order_release);
}

template <ListenerType LT>
void Listener<LT>::reserve_stack() {
	char placeholder[0x8000];
	for (size_t i = 0; i < sizeof(placeholder); i += 4096) placeholder[i] = i & 0xFF;
}

template <ListenerType LT>
Listener<LT>::Shared::Shared(ConnectionFactory &&connFactory, int maxListeners)
	: maxListeners_(maxListeners), listenersCount_(1), connFactory_(std::move(connFactory)), terminating_(false) {}

template <ListenerType LT>
Listener<LT>::Shared::~Shared() {
	if rx_unlikely (sock_.close() != 0) {
		perror("sock_.close() error");
	}
}

ForkedListener::ForkedListener(ev::dynamic_loop &loop, ConnectionFactory &&connFactory)
	: connFactory_(std::move(connFactory)), loop_(loop) {
	io_.set<ForkedListener, &ForkedListener::io_accept>(this);
	io_.set(loop);
	async_.set<ForkedListener, &ForkedListener::async_cb>(this);
	async_.set(loop);
	async_.start();
}

ForkedListener::~ForkedListener() {
	io_.stop();
	if (!terminating_ || runningThreadsCount_) {
		ForkedListener::Stop();
	}
	if rx_unlikely (sock_.close() != 0) {
		perror("sock_.close() error");
	}
}

bool ForkedListener::Bind(std::string addr, socket_domain type) {
	if (sock_.valid()) {
		return false;
	}

	addr_ = std::move(addr);

	if (sock_.bind(addr_, type) < 0) {
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

	if (terminating_) {
		logPrintf(LogWarning, "Can't accept connection. Listener is terminating!");
		return;
	}
	++runningThreadsCount_;

	std::thread th([this, client = std::move(client)]() mutable noexcept {
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

			Worker w(std::unique_ptr<IServerConnection>(connFactory_(loop, std::move(client), false)), async);
			auto pc = w.conn.get();
			if (pc->IsFinished()) {	 // Connection may be closed inside Worker construction
				pc->Detach();
			} else {
				std::unique_lock lck(mtx_);
				workers_.emplace_back(std::move(w));
				logPrintf(LogTrace, "Listener (%s) dedicated thread started. %d total", addr_, workers_.size());
				lck.unlock();
				while (!terminating_) {
					loop.run();
					if (pc->IsFinished()) {
						pc->Detach();
						break;
					}
				}
				lck.lock();
				const auto it = std::find_if(workers_.begin(), workers_.end(), [&pc](const Worker &cw) { return cw.conn.get() == pc; });
				assertrx(it != workers_.end());
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
	std::unique_lock lck(mtx_);
	for (auto &worker : workers_) {
		worker.async->send();
	}
	while (runningThreadsCount_ || !workers_.empty()) {
		lck.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		lck.lock();
	}
}

template class Listener<ListenerType::Mixed>;
template class Listener<ListenerType::Shared>;

}  // namespace net
}  // namespace reindexer
