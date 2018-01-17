#pragma once

#include <atomic>
#include <list>
#include "ext_ev.h"
#include "router.h"

namespace reindexer_server {

using std::atomic;
using std::list;
using std::vector;
namespace http {

class Connection;

class Listener {
public:
	Listener(ev::dynamic_loop &loop, Router &router, int maxListeners = 0);
	~Listener();
	bool Bind(int port);
	void Run();
	void Fork(int clones);

protected:
	void reserveStack();
	void clone();
	void io_accept(ev::io &watcher, int revents);
	void timeout_cb(ev::periodic &watcher, int);

	ev::io io_;
	ev::periodic timer_;
	ev::dynamic_loop &loop_;
	Router &router_;
	int fd_;
	int maxListeners_;
	int port_;

	static atomic<int> listeners_, counter_;
	Listener *root_ = nullptr;
	vector<Connection *> connectons_;
	int idleConns_;
	int id_;
};
}  // namespace http
}  // namespace reindexer_server
