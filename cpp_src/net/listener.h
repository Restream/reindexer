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

/// Network listener implementation
class Listener {
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
	/// Fork preforks additional listener threads
	/// @param clones - Number of threads
	void Fork(int clones);
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
		int maxListeners_;
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
	int id_;
};
}  // namespace net
}  // namespace reindexer
