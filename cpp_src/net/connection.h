#pragma once

#include <string.h>
#include <mutex>
#include "estl/cbuf.h"
#include "estl/chunk_buf.h"
#include "estl/mutex.h"
#include "net/ev/ev.h"
#include "net/socket.h"
#include "tools/ssize_t.h"

namespace reindexer {
namespace net {

using reindexer::cbuf;

const ssize_t kConnReadbufSize = 0x8000;
const ssize_t kConnWriteBufSize = 0x800;

struct ConnectionStat {
	ConnectionStat() {
		startTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	}
	std::atomic_int_fast64_t recvBytes{0};
	std::atomic_int_fast64_t lastRecvTs{0};
	std::atomic_int_fast64_t sentBytes{0};
	std::atomic_int_fast64_t lastSendTs{0};
	std::atomic_int_fast64_t sendBufBytes{0};
	std::atomic_int_fast64_t pendedUpdates{0};
	std::atomic<uint32_t> sendRate{0};
	std::atomic<uint32_t> recvRate{0};
	int64_t startTime{0};
};

template <typename Mutex>
class Connection {
public:
	Connection(int fd, ev::dynamic_loop &loop, bool enableStat, size_t readBufSize = kConnReadbufSize,
			   size_t writeBufSize = kConnWriteBufSize);
	virtual ~Connection();

protected:
	virtual void onRead() = 0;
	virtual void onClose() = 0;

	// Generic callback
	void callback(ev::io &watcher, int revents);
	void write_cb();
	void read_cb();
	void async_cb(ev::async &watcher);
	void timeout_cb(ev::periodic &watcher, int);
	void stats_check_cb(ev::periodic &watcher, int);

	void closeConn();
	void attach(ev::dynamic_loop &loop);
	void detach();
	void restart(int fd);

	ev::io io_;
	ev::timer timeout_;
	ev::async async_;

	socket sock_;
	int curEvents_ = 0;
	bool closeConn_ = false;
	bool attached_ = false;
	bool canWrite_ = true;

	chain_buf<Mutex> wrBuf_;
	cbuf<char> rdBuf_;
	std::string clientAddr_;

	std::shared_ptr<ConnectionStat> stat_;
	ev::timer statsUpdater_;
	int64_t prevSecSentBytes_ = 0;
	int64_t prevSecRecvBytes_ = 0;
};

using ConnectionST = Connection<reindexer::dummy_mutex>;
using ConnectionMT = Connection<std::mutex>;
}  // namespace net
}  // namespace reindexer
