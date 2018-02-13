#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include "iconnection.h"
#include "net/ev/ev.h"

namespace reindexer {
namespace net {

using std::atomic;
using std::vector;

/// Network listener implementation
class Listener {
public:
	/// Constructs new listner object.
	/// @param loop - ev::loop of caller's thread, listener's socket will be binded to that loop.
	/// @param connFactory - Connection factory, will create objects with IConnection interface implementation.
	/// @param maxListeners - Maximum number of threads, which listener will utilize. std::thread::hardware_concurrency() by default
	Listener(ev::dynamic_loop &loop, ConnectionFactory connFactory, int maxListeners = 0);
	~Listener();
	/// Bind listener to specified port
	/// @param port - tcp port for bind
	/// @return true - if bind successful, false - on bind error
	bool Bind(int port);
	/// Run listener loop. Block execution of current thread until signal interrupt
	void Run();
	/// Fork preforks additional listener threads
	/// @param clones - Number of threads
	void Fork(int clones);

protected:
	void reserveStack();
	void clone();
	void io_accept(ev::io &watcher, int revents);
	void timeout_cb(ev::periodic &watcher, int);

	struct Shared {
		Shared(ConnectionFactory connFactory, int maxListeners);
		~Shared();
		int fd_;
		int maxListeners_;
		int port_ = 0;
		std::atomic<int> count_;
		vector<Listener *> listeners_;
		std::mutex lck_;
		ConnectionFactory connFactory_;
	};
	Listener(ev::dynamic_loop &loop, std::shared_ptr<Shared> shared);

	ev::io io_;
	ev::periodic timer_;
	ev::dynamic_loop &loop_;
	std::shared_ptr<Shared> shared_;
	vector<std::unique_ptr<IConnection>> connectons_;
	std::atomic<int> connCount_;
	int idleConns_;
	int id_;
};
}  // namespace net
}  // namespace reindexer
