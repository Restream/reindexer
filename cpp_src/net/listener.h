#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "iserverconnection.h"
#include "net/ev/ev.h"
#include "socket.h"
#include "vendor/sparse-map/sparse_set.h"

namespace reindexer {
namespace net {

class IListener {
public:
	virtual ~IListener() = default;
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind
	/// @return true - if bind successful, false - on bind error
	virtual bool Bind(std::string addr) = 0;
	/// Stop synchroniusly stops listener
	virtual void Stop() = 0;
};

struct ConnPtrEqual {
	using is_transparent = void;

	bool operator()(const IServerConnection *lhs, const std::unique_ptr<IServerConnection> &rhs) const noexcept { return lhs == rhs.get(); }
	bool operator()(const std::unique_ptr<IServerConnection> &lhs, const IServerConnection *rhs) const noexcept { return lhs.get() == rhs; }
	bool operator()(const std::unique_ptr<IServerConnection> &lhs, const std::unique_ptr<IServerConnection> &rhs) const noexcept {
		return lhs == rhs;
	}
};

struct ConnPtrHash {
	using transparent_key_equal = ConnPtrEqual;

	bool operator()(const IServerConnection *ptr) const noexcept {
		std::hash<uintptr_t> h;
		return h(uintptr_t(ptr));
	}
	bool operator()(const std::unique_ptr<IServerConnection> &ptr) const noexcept {
		std::hash<uintptr_t> h;
		return h(uintptr_t(ptr.get()));
	}
};

enum class ListenerType {
	/// In shared mode each listener (except the first one) running in own detached thread and
	/// able to accept/handle connections by himself. Connections may migrate between listeners only
	/// after rebalance() call.
	/// This mode is more effective for short-term connections, but all the connections will be handled by shared thread pool.
	Shared,
	/// In mixed mode only the first listener is able to accept connections.
	/// When connection is accepted, this listener await the first clint messages to check, if client has requested dedicated thread.
	/// If dedicated thread was requestedm new connection will be moved to new thread, otherwise
	/// new connection will be moved to one of the shared listeners.
	/// This mode is optimal for long-term connections and required to break request cycles in synchronous replications, but
	/// is not effective for short-term connections, because first client's message can not be handled in acceptor thread.
	Mixed
};

/// Network listener implementation
template <ListenerType LT>
class Listener : public IListener {
public:
	/// Constructs new listner object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be binded to that loop.
	/// @param connFactory - Connection factory, will create objects with IServerConnection interface implementation.
	/// @param maxListeners - Maximum number of threads, which listener will utilize. std::thread::hardware_concurrency() by default
	Listener(ev::dynamic_loop &loop, ConnectionFactory &&connFactory, int maxListeners = 0);
	~Listener();
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind
	/// @return true - if bind successful, false - on bind error
	bool Bind(std::string addr);
	/// Stop synchroniusly stops listener
	void Stop();

protected:
	void reserve_stack();
	void io_accept(ev::io &watcher, int revents);
	void timeout_cb(ev::periodic &watcher, int);
	void async_cb(ev::async &watcher);
	void rebalance();
	void rebalance_from_acceptor();
	// Locks shared_->mtx_. Should not be calles under external lock.
	void rebalance_conn(IServerConnection *, IServerConnection::BalancingType type);
	void run_dedicated_thread(std::unique_ptr<IServerConnection> &&conn);
	// May lock shared_->mtx_. Should not be calles under external lock.
	void startup_shared_thread();

	struct Shared {
		struct Worker {
			Worker(std::unique_ptr<IServerConnection> &&_conn, ev::async &_async) : conn(std::move(_conn)), async(&_async) {}
			Worker(Worker &&other) noexcept : conn(std::move(other.conn)), async(other.async) {}
			Worker &operator=(Worker &&other) noexcept {
				if (&other != this) {
					conn = std::move(other.conn);
					async = other.async;
				}
				return *this;
			}

			std::unique_ptr<IServerConnection> conn;
			ev::async *async;
		};

		Shared(ConnectionFactory &&connFactory, int maxListeners);
		~Shared();
		socket sock_;
		const int maxListeners_;
		std::atomic<int> listenersCount_ = {0};
		std::atomic<int> connCount_ = {0};
		std::vector<Listener *> listeners_;
		std::mutex mtx_;
		ConnectionFactory connFactory_;
		std::atomic<bool> terminating_;
		std::string addr_;
		std::vector<std::unique_ptr<IServerConnection>> idle_;
		std::chrono::time_point<std::chrono::steady_clock> ts_;
		std::vector<Worker> dedicatedWorkers_;
	};
	class ListeningThreadData {
	public:
		ListeningThreadData(std::shared_ptr<Shared> shared) : listener_(loop_, shared), shared_(std::move(shared)) { assertrx(shared_); }

		void Loop() {
			if constexpr (LT == ListenerType::Shared) {
				listener_.io_.start(shared_->sock_.fd(), ev::READ);
			}
			while (!shared_->terminating_) {
				loop_.run();
			}
		}
		const Listener &GetListener() const noexcept { return listener_; }
		Shared &GetShared() noexcept { return *shared_; }

	private:
		ev::dynamic_loop loop_;
		Listener listener_;
		std::shared_ptr<Shared> shared_;
	};
	// Locks shared_->mtx_. Should not be calles under external lock.
	Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared);
	static void clone(std::unique_ptr<ListeningThreadData> d) noexcept;

	ev::io io_;
	ev::periodic timer_;
	ev::dynamic_loop &loop_;
	ev::async async_;
	std::shared_ptr<Shared> shared_;
	std::vector<std::unique_ptr<IServerConnection>> connections_;
	tsl::sparse_set<std::unique_ptr<IServerConnection>, ConnPtrHash, ConnPtrHash::transparent_key_equal> accepted_;
	uint64_t id_;
};

/// Network listener implementation
class ForkedListener : public IListener {
public:
	/// Constructs new listner object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be binded to that loop.
	/// @param connFactory - Connection factory, will create objects with IServerConnection interface implementation.
	ForkedListener(ev::dynamic_loop &loop, ConnectionFactory &&connFactory);
	~ForkedListener();
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind
	/// @return true - if bind successful, false - on bind error
	bool Bind(std::string addr);
	/// Stop synchroniusly stops listener
	void Stop();

protected:
	void io_accept(ev::io &watcher, int revents);
	void async_cb(ev::async &watcher);

	struct Worker {
		Worker(std::unique_ptr<IServerConnection> &&conn, ev::async &async) : conn(std::move(conn)), async(&async) {}
		Worker(Worker &&other) noexcept : conn(std::move(other.conn)), async(other.async) {}
		Worker &operator=(Worker &&other) noexcept {
			if (&other != this) {
				conn = std::move(other.conn);
				async = other.async;
			}
			return *this;
		}

		std::unique_ptr<IServerConnection> conn;
		ev::async *async;
	};

	socket sock_;
	std::mutex mtx_;
	ConnectionFactory connFactory_;
	std::atomic<bool> terminating_{false};
	std::string addr_;

	ev::io io_;
	ev::dynamic_loop &loop_;
	ev::async async_;
	std::vector<Worker> workers_;
	std::atomic<int> runningThreadsCount_ = {0};
};

}  // namespace net
}  // namespace reindexer
