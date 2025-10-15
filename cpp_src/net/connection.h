#pragma once

#include <string.h>
#include "connectinstatscollector.h"
#include "estl/cbuf.h"
#include "estl/chunk_buf.h"
#include "estl/dummy_mutex.h"
#include "estl/mutex.h"
#include "net/socket.h"

namespace reindexer {
namespace net {

constexpr ssize_t kConnReadbufSize = 0x8000;
constexpr ssize_t kConnWriteBufSize = 0x800;

struct [[nodiscard]] ConnectionStat {
	ConnectionStat() noexcept {
		startTime = std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now_coarse().time_since_epoch()).count();
	}
	std::atomic_int_fast64_t recvBytes{0};
	std::atomic_int_fast64_t lastRecvTs{0};
	std::atomic_int_fast64_t sentBytes{0};
	std::atomic_int_fast64_t lastSendTs{0};
	std::atomic_int_fast64_t sendBufBytes{0};
	std::atomic_int_fast64_t pendedUpdates{0};
	std::atomic_int_fast64_t updatesLost{0};
	std::atomic<uint32_t> sendRate{0};
	std::atomic<uint32_t> recvRate{0};
	int64_t startTime{0};
};
using reindexer::cbuf;

template <typename Mutex>
class [[nodiscard]] Connection {
public:
	Connection(socket&& s, ev::dynamic_loop& loop, bool enableStat, size_t readBufSize = kConnReadbufSize,
			   size_t writeBufSize = kConnWriteBufSize, int idleTimeout = -1);
	virtual ~Connection();

protected:
	enum class [[nodiscard]] ReadResT { Default, Rebalanced };

	// @return false if connection was moved into another thread
	virtual ReadResT onRead() = 0;
	virtual void onClose() = 0;

	// Generic callback
	void callback(ev::io& watcher, int revents);
	void update_cur_events(int nevents) noexcept;
	void write_cb();
	ReadResT read_cb();
	void async_cb(ev::async& watcher);
	void timeout_cb(ev::periodic& watcher, int);

	void closeConn();
	void attach(ev::dynamic_loop& loop);
	void detach();
	void restart(socket&& s);

	socket sock_;
	ev::io io_;
	ev::async async_;

	int curEvents_ = 0;
	bool closeConn_ = false;
	bool attached_ = false;
	bool canWrite_ = true;
	int64_t rwCounter_ = 0;
	int64_t lastCheckRWCounter_ = 0;

	chain_buf<Mutex> wrBuf_;
	cbuf<char> rdBuf_;
	std::string clientAddr_;

	std::unique_ptr<connection_stats_collector> stats_;

private:
	void restartIdleCheckTimer() noexcept {
		lastCheckRWCounter_ = rwCounter_ = 0;
		if (kIdleCheckPeriod_ > 0) {
			timeout_.start(kIdleCheckPeriod_, kIdleCheckPeriod_);
		}
	}

	ev::timer timeout_;
	const int kIdleCheckPeriod_;
};

using ConnectionST = Connection<reindexer::DummyMutex>;
using ConnectionMT = Connection<mutex>;

}  // namespace net
}  // namespace reindexer
