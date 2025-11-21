#include "listener.h"
#include <fcntl.h>
#include <cstdlib>
#include <thread>
#include "core/type_consts.h"
#include "estl/lock.h"
#include "server/pprof/gperf_profiler.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/crypt.h"
#include "tools/errors.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"

namespace reindexer {
namespace net {

static std::atomic_uint_fast64_t counter_;

constexpr int kListenCount = 500;

template <ListenerType LT>
Listener<LT>::Listener(ev::dynamic_loop& loop, std::shared_ptr<Shared> shared)
	: loop_(loop), shared_(std::move(shared)), id_(counter_.fetch_add(1, std::memory_order_relaxed)) {
	io_.set<Listener, &Listener::io_accept>(this);
	io_.set(loop);
	timer_.set<Listener, &Listener::timeout_cb>(this);
	timer_.set(loop);
	timer_.start(5., 5.);
	async_.set<Listener, &Listener::async_cb>(this);
	async_.set(loop);
	async_.start();

	lock_guard lck(shared_->mtx_);
	if (int(shared_->listeners_.size()) == shared_->maxListeners_) [[unlikely]] {
		// We are going to get this exception few times in case of concurrent listeners creation
		throw Error(errConflict, "Too many shared listeners");
	}
	if (shared_->terminating_) [[unlikely]] {
		throw Error(errConflict, "Listener is terminating");
	}
	logFmt(LogTrace, "Listener ({}). Creating new shared thread ({} total)", shared_->addr_, shared_->listeners_.size() + 1);
	shared_->listeners_.emplace_back(this);
	shared_->sharedListenersCount_.fetch_add(1, std::memory_order_release);
}

template <ListenerType LT>
Listener<LT>::Listener(ev::dynamic_loop& loop, ConnectionFactory&& connFactory, openssl::SslCtxPtr sslCtx, int maxListeners)
	: Listener(loop,
			   std::make_shared<Shared>(std::move(connFactory), (maxListeners ? maxListeners : (double(hardware_concurrency()) * 1.2)) + 1,
										std::move(sslCtx))) {}

template <ListenerType LT>
Listener<LT>::~Listener() {
	io_.stop();
	if (isMainListener_) {
		Listener<LT>::Stop();
	}

	unique_lock lck(shared_->mtx_);
	auto it = std::find(shared_->listeners_.begin(), shared_->listeners_.end(), this);
	assertrx_dbg(it != shared_->listeners_.end());
	if (it != shared_->listeners_.end()) {	// Just in case of some kind system error
		shared_->listeners_.erase(it);
	}
	lck.unlock();

	// Connections have to be removed before sharedListenersCount_ decrement
	connections_.clear();
	accepted_.clear();

	shared_->sharedListenersCount_.fetch_sub(1, std::memory_order_release);
}

template <ListenerType LT>
void Listener<LT>::Bind(std::string addr, socket_domain type) {
	if (shared_->sock_.valid()) {
		throw Error(errSystem, "Invalid listener socket");
	}

	// We have single listener at the beginning
	if (shared_->sharedListenersCount_ != 1) {
		throw Error(errSystem, "Listener '{}' is already running", addr);
	}

	shared_->addr_ = std::move(addr);

	if (shared_->sock_.bind(shared_->addr_, type) < 0) {
		throw Error(errSystem, "Unable to bind socket for '{}': {}", shared_->addr_, strerror(errno));
	}

	if (shared_->sock_.listen(kListenCount) < 0) {
		throw Error(errSystem, "Unable to listen on '{}': {}", shared_->addr_, strerror(errno));
	}

	std::thread th(&Listener::clone, std::make_unique<ListeningThreadData>(shared_));
	th.detach();
	isMainListener_ = true;
	if constexpr (LT == ListenerType::Mixed) {
		// Current listener is the only one, which reads the first messages
		io_.start(shared_->sock_.fd(), ev::READ);
		reserve_stack();
	}
}

static int try_ssl_accept(socket& client, const openssl::SslCtxPtr& ctx) noexcept {
	if (!ctx) {
		return 0;
	}

	try {
		client.ssl = openssl::create_ssl(ctx);
	} catch (Error&) {
		// Error has already been logged in create_ssl already
		return -1;
	}

	auto logSslErr = [] {
		logFmt(LogError, "{}: {}", static_cast<unsigned long>(openssl::ERR_get_error()),
			   static_cast<char*>(openssl::ERR_error_string(openssl::ERR_get_error(), NULL)));
	};

	if (int res = openssl::SSL_set_fd(*client.ssl, client.fd()); res == 0) {
		logSslErr();
		return -1;
	}

	if (int res = openssl::SSL_accept(*client.ssl); res != 1) {
		int err = openssl::SSL_get_error(*client.ssl, res);
		if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
			logSslErr();
			return -1;
		}
	}

	return 0;
}

template <ListenerType LT>
void Listener<LT>::io_accept(ev::io& /*watcher*/, int revents) {
	if (ev::ERROR & revents) {
		perror("got invalid event");
		return;
	}

	auto client = shared_->sock_.accept();

	if (!client.valid()) {
		return;
	}

	if (shared_->terminating_) {
		logFmt(LogWarning, "Can't accept connection. Listener is terminating!");
		return;
	}

	if (try_ssl_accept(client, shared_->sslCtx_) != 0) {
		return;
	}

	bool connIsActive = true;
	std::unique_ptr<IServerConnection> conn;
	unique_lock lck(shared_->mtx_);
	if (shared_->idle_.size()) {
		conn = std::move(shared_->idle_.back());
		shared_->idle_.pop_back();
		lck.unlock();
		conn->Attach(loop_);
		std::ignore = conn->Restart(std::move(client));
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
					logFmt(LogError, "Listener: BalancingType::None. Interpreting as 'shared'");
					[[fallthrough]];
				case IServerConnection::BalancingType::Shared:
					shared_->connsCountOnSharedListeners_.fetch_add(1, std::memory_order_release);
					startup_shared_thread();

					lck.lock();
					connections_.emplace_back(std::move(conn));
					rebalance_from_acceptor();
					return;
				case IServerConnection::BalancingType::Dedicated:
					conn->Detach();
					run_dedicated_thread(std::move(conn));
					return;
				case IServerConnection::BalancingType::NotSet: {
					auto c = conn.get();
					lck.lock();
					accepted_.emplace(std::move(conn));
					lck.unlock();
					c->SetRebalanceCallback(
						[this](IServerConnection* c, IServerConnection::BalancingType type) { rebalance_conn(c, type); });
					break;
				}
			}
			startup_shared_thread();
		} else {
			shared_->connsCountOnSharedListeners_.fetch_add(1, std::memory_order_release);
			startup_shared_thread();

			lck.lock();
			connections_.emplace_back(std::move(conn));
			rebalance();
			lck.unlock();
		}
	}
}

template <ListenerType LT>
void Listener<LT>::timeout_cb(ev::periodic&, int) {
	const bool enableReuseIdle = !std::getenv("REINDEXER_NOREUSEIDLE");

	lock_guard lck(shared_->mtx_);
	// Move finished connections to idle connections pool
	for (unsigned i = 0; i < connections_.size();) {
		if (connections_[i]->IsFinished()) {
			connections_[i]->Detach();
			shared_->connsCountOnSharedListeners_.fetch_sub(1, std::memory_order_release);
			if (enableReuseIdle) {	// -V547
				shared_->idle_.emplace_back(std::move(connections_[i]));
			} else {
				connections_[i].reset();
			}

			if (i != connections_.size() - 1) {
				connections_[i] = std::move(connections_.back());
			}
			connections_.pop_back();
			shared_->ts_ = steady_clock_w::now_coarse();
		} else {
			i++;
		}
	}

	// Clear all idle connections, after 300 sec
	if (shared_->idle_.size() && steady_clock_w::now_coarse() - shared_->ts_ > std::chrono::seconds(300)) {
		logFmt(LogInfo, "Cleanup idle connections. {} cleared", shared_->idle_.size());
		shared_->idle_.clear();
	}

	rebalance();
	const size_t curConnCount = connections_.size();
	if (curConnCount != 0) {
		logFmt(LogTrace, "Listener({}) {} stats: {} connections", shared_->addr_, id_, curConnCount);
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
				logFmt(LogInfo, "Rebalance connection from listener {} to {}", id_, (*minIt)->id_);
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
		logFmt(LogError, "Unable to rebalance connections from acceptor: there are no other listeners");
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
void Listener<LT>::rebalance_conn(IServerConnection* c, IServerConnection::BalancingType type) {
	unique_lock lck(shared_->mtx_);
	auto found = accepted_.find(c);
	if (found == accepted_.end()) {
		logFmt(LogError, "Rebalance was requested for incorrect connection ptr: {:#08x}", uint64_t(c));
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
		shared_->connsCountOnSharedListeners_.fetch_add(1, std::memory_order_release);
		rebalance_from_acceptor();
		lck.unlock();
		startup_shared_thread();
	}
}

template <ListenerType LT>
void Listener<LT>::run_dedicated_thread(std::unique_ptr<IServerConnection> conn) noexcept {
	try {
		auto worker = std::make_unique<typename Shared::Worker>(shared_, std::move(conn));
		std::thread th([worker = std::move(worker)]() mutable {
			try {
#if REINDEX_WITH_GPERFTOOLS
				if (alloc_ext::TCMallocIsAvailable()) {
					reindexer_server::pprof::ProfilerRegisterThread();
				}
#endif
				worker->Run();
			} catch (Error& e) {
				logFmt(LogError, "Unhandled exception in listener thread: {}", e.what());
			} catch (std::exception& e) {
				logFmt(LogError, "Unhandled exception in listener thread: {}", e.what());
			} catch (...) {
				logFmt(LogError, "Unhandled exception in listener thread");
			}
		});
		th.detach();
	} catch (std::exception& e) {
		logFmt(LogError, "Unable to created dedicated worker for connection: {}", e.what());
	}
}

template <ListenerType LT>
void Listener<LT>::startup_shared_thread() {
	int count = shared_->sharedListenersCount_.load(std::memory_order_relaxed);
	while (count < shared_->maxListeners_ && count <= (shared_->connsCountOnSharedListeners_.load(std::memory_order_acquire) + 1)) {
		std::unique_ptr<ListeningThreadData> ldata;
		try {
			ldata = std::make_unique<ListeningThreadData>(shared_);
		} catch (...) {
			// Listeners capacity was reached or termination was called. It's ok during the initialization procedure
			break;
		}
		std::thread th(&Listener::clone, std::move(ldata));
		th.detach();
		break;
	}
}

template <ListenerType LT>
void Listener<LT>::async_cb(ev::async& watcher) {
	logFmt(LogInfo, "Listener({}) {} async received", shared_->addr_, id_);
	h_vector<IServerConnection*, 32> conns;
	{
		lock_guard lck(shared_->mtx_);
		for (auto& it : connections_) {
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
		for (auto& conn : conns) {
			conn->Attach(loop_);
			conn->HandlePendingData();
		}
	}
	watcher.loop.break_loop();
}

template <ListenerType LT>
void Listener<LT>::Stop() noexcept {
	unique_lock lck(shared_->mtx_);
	shared_->terminating_ = true;

	for (auto& listener : shared_->listeners_) {
		listener->async_.send();
	}
	for (auto& worker : shared_->dedicatedWorkers_) {
		worker->SendTerminateAsync();
	}

	assertrx(this == shared_->listeners_.front());
	while (shared_->sharedListenersCount_ > 1 || !shared_->dedicatedWorkers_.empty()) {
		lck.unlock();
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		lck.lock();
	}
}

template <ListenerType LT>
void Listener<LT>::clone(std::unique_ptr<ListeningThreadData> d) noexcept {
	try {
		assertrx(d);
#if REINDEX_WITH_GPERFTOOLS
		if (alloc_ext::TCMallocIsAvailable()) {
			reindexer_server::pprof::ProfilerRegisterThread();
		}
#endif
		d->Loop();
	} catch (Error& e) {
		assertrx(false);
		logFmt(LogError, "Unhandled exception in listener thread ({}): {}", d->GetShared().addr_, e.what());
	} catch (std::exception& e) {
		assertrx(false);
		logFmt(LogError, "Unhandled exception in listener thread ({}): {}", d->GetShared().addr_, e.what());
	} catch (...) {
		assertrx(false);
		logFmt(LogError, "Unhandled exception in listener thread ({}): <unknown>", d->GetShared().addr_);
	}
}

template <ListenerType LT>
void Listener<LT>::reserve_stack() noexcept {
	char placeholder[0x8000];
	for (size_t i = 0; i < sizeof(placeholder); i += 4096) {
		placeholder[i] = i & 0xFF;
	}
}

template <ListenerType LT>
Listener<LT>::Shared::Worker::Worker(std::shared_ptr<Shared> shared, std::unique_ptr<IServerConnection> conn)
	: conn_{std::move(conn)}, shared_{std::move(shared)} {
	assertrx(shared_);
	assertrx(conn_);

	async_.set([](ev::async& a) noexcept { a.loop.break_loop(); });
	async_.set(loop_);
	async_.start();

	lock_guard lck(shared_->mtx_);
	if (shared_->terminating_) [[unlikely]] {
		throw Error(errConflict, "Listener is terminating");
	}
	shared_->dedicatedWorkers_.emplace_back(this);
	logFmt(LogTrace, "Listener ({}) starting new dedicated thread. {} total", shared_->addr_, shared_->dedicatedWorkers_.size());
}

template <ListenerType LT>
Listener<LT>::Shared::Worker::~Worker() {
	conn_->Detach();
	async_.stop();

	// Connection has to be closed before erasure from dedicatedWorkers_
	conn_.reset();

	lock_guard lck(shared_->mtx_);
	const auto it = std::ranges::find_if(shared_->dedicatedWorkers_, [this](const Worker* w) noexcept { return w == this; });
	assertrx(it != shared_->dedicatedWorkers_.end());
	shared_->dedicatedWorkers_.erase(it);
	logFmt(LogTrace, "Listener ({}) dedicated thread finished. {} left", shared_->addr_, shared_->dedicatedWorkers_.size());
}

template <ListenerType LT>
void Listener<LT>::Shared::Worker::Run() {
	conn_->Attach(loop_);
	conn_->HandlePendingData();
	while (!shared_->terminating_) {
		if (conn_->IsFinished()) {
			break;
		}
		loop_.run();
	}
}

template <ListenerType LT>
Listener<LT>::Shared::Shared(ConnectionFactory&& connFactory, int maxListeners, openssl::SslCtxPtr SslCtx)
	: sslCtx_(std::move(SslCtx)), maxListeners_(maxListeners), connFactory_(std::move(connFactory)), terminating_(false) {}

template <ListenerType LT>
Listener<LT>::Shared::~Shared() {
	if (sock_.close() != 0) [[unlikely]] {
		perror("sock_.close() error");
	}
}

ForkedListener::ForkedListener(ev::dynamic_loop& loop, ConnectionFactory&& connFactory, openssl::SslCtxPtr sslCtx)
	: sslCtx_(std::move(sslCtx)), connFactory_(std::move(connFactory)), loop_(loop) {
	io_.set<ForkedListener, &ForkedListener::io_accept>(this);
	io_.set(loop);
	async_.set<ForkedListener, &ForkedListener::async_cb>(this);
	async_.set(loop);
	async_.start();
}

ForkedListener::~ForkedListener() {
	io_.stop();
	ForkedListener::Stop();
	if (sock_.close() != 0) [[unlikely]] {
		perror("sock_.close() error");
	}
}

void ForkedListener::Bind(std::string addr, socket_domain type) {
	if (sock_.valid()) {
		throw Error(errSystem, "Invalid listener socket");
	}

	addr_ = std::move(addr);

	if (sock_.bind(addr_, type) < 0) {
		throw Error(errSystem, "Unable to bind socket for '{}': {}", addr_, strerror(errno));
	}

	if (sock_.listen(kListenCount) < 0) {
		throw Error(errSystem, "Unable to listen on '{}': {}", addr_, strerror(errno));
	}

	io_.start(sock_.fd(), ev::READ);
}

void ForkedListener::io_accept(ev::io& /*watcher*/, int revents) {
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
		logFmt(LogWarning, "Can't accept connection. Listener is terminating!");
		return;
	}

	if (try_ssl_accept(client, sslCtx_) != 0) {
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
			async.set([](ev::async& a) { a.loop.break_loop(); });
			async.set(loop);
			async.start();

			Worker w(std::unique_ptr<IServerConnection>(connFactory_(loop, std::move(client), false)), async);
			auto pc = w.conn.get();
			if (pc->IsFinished()) {	 // Connection may be closed inside Worker construction
				pc->Detach();
			} else {
				unique_lock lck(mtx_);
				workers_.emplace_back(std::move(w));
				logFmt(LogTrace, "Listener ({}) dedicated thread started. {} total", addr_, workers_.size());
				lck.unlock();
				while (!terminating_) {
					loop.run();
					if (pc->IsFinished()) {
						pc->Detach();
						break;
					}
				}
				lck.lock();
				const auto it = std::find_if(workers_.begin(), workers_.end(), [&pc](const Worker& cw) { return cw.conn.get() == pc; });
				assertrx(it != workers_.end());
				workers_.erase(it);
				logFmt(LogTrace, "Listener ({}) dedicated thread finished. {} left", addr_, workers_.size());
			}
		} catch (Error& e) {
			logFmt(LogError, "Unhandled exception in listener thread: {}", e.what());
		} catch (std::exception& e) {
			logFmt(LogError, "Unhandled exception in listener thread: {}", e.what());
		} catch (...) {
			logFmt(LogError, "Unhandled exception in listener thread");
		}
		--runningThreadsCount_;
	});
	th.detach();
}

void ForkedListener::async_cb(ev::async& watcher) {
	logFmt(LogInfo, "Listener({}) async received", addr_);
	watcher.loop.break_loop();
}

void ForkedListener::Stop() noexcept {
	terminating_ = true;
	async_.send();
	unique_lock lck(mtx_);
	for (auto& worker : workers_) {
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
