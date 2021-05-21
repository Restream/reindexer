#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "iserverconnection.h"
#include "net/ev/ev.h"
#include "socket.h"

namespace reindexer {
namespace net {

using std::atomic;
using std::vector;

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

/// Network listener implementation
class Listener : public IListener {
public:
	/// Constructs new listner object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be binded to that loop.
	/// @param connFactory - Connection factory, will create objects with IServerConnection interface implementation.
	/// @param maxListeners - Maximum number of threads, which listener will utilize. std::thread::hardware_concurrency() by default
	Listener(ev::dynamic_loop &loop, ConnectionFactory connFactory, int maxListeners = 0);
	~Listener();
	/// Bind listener to specified host:port
	/// @param addr - tcp host:port for bind
	/// @return true - if bind successful, false - on bind error
	bool Bind(std::string addr);
	/// Stop synchroniusly stops listener
	void Stop();

protected:
	void reserveStack();
	void io_accept(ev::io &watcher, int revents);
	void timeout_cb(ev::periodic &watcher, int);
	void async_cb(ev::async &watcher);
	void rebalance();

	struct Shared {
		Shared(ConnectionFactory connFactory, int maxListeners);
		~Shared();
		socket sock_;
		const int maxListeners_;
		std::atomic<int> count_;
		vector<Listener *> listeners_;
		std::mutex lck_;
		ConnectionFactory connFactory_;
		std::atomic<bool> terminating_;
		std::string addr_;
		vector<std::unique_ptr<IServerConnection>> idle_;
		std::chrono::time_point<std::chrono::steady_clock> ts_;
	};
	Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared);
	static void clone(std::shared_ptr<Shared>);

	ev::io io_;
	ev::periodic timer_;
	ev::dynamic_loop &loop_;
	ev::async async_;
	std::shared_ptr<Shared> shared_;
	vector<std::unique_ptr<IServerConnection>> connections_;
	uint64_t id_;
};

/// Network listener implementation
class ForkedListener : public IListener {
public:
	/// Constructs new listner object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be binded to that loop.
	/// @param connFactory - Connection factory, will create objects with IServerConnection interface implementation.
	ForkedListener(ev::dynamic_loop &loop, ConnectionFactory connFactory);
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
		Worker(Worker &&other) noexcept : conn(std::move(other.conn)), async(std::move(other.async)) {}
		Worker &operator=(Worker &&other) noexcept {
			if (&other != this) {
				conn = std::move(other.conn);
				async = std::move(other.async);
			}
			return *this;
		}

		std::unique_ptr<IServerConnection> conn;
		ev::async *async;
	};

	socket sock_;
	std::mutex lck_;
	ConnectionFactory connFactory_;
	std::atomic<bool> terminating_{false};
	std::string addr_;

	ev::io io_;
	ev::dynamic_loop &loop_;
	ev::async async_;
	vector<Worker> workers_;
};

}  // namespace net
}  // namespace reindexer
