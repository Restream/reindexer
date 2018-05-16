#pragma once

#include <string.h>
#include "dispatcher.h"
#include "estl/cbuf.h"
#include "estl/h_vector.h"
#include "net/ev/ev.h"
#include "net/iconnection.h"
#include "net/socket.h"
#include "tools/ssize_t.h"

namespace reindexer {
namespace net {
namespace cproto {

using reindexer::cbuf;
using reindexer::h_vector;

const ssize_t kRPCReadbufSize = 0x20000;
const ssize_t kRPCWriteBufSize = 0x20000;

class Connection : public IConnection, public Writer {
public:
	Connection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher);
	~Connection();

	// IConnection interface implementation
	static ConnectionFactory NewFactory(Dispatcher &dispatcher) {
		return [&dispatcher](ev::dynamic_loop &loop, int fd) { return new Connection(fd, loop, dispatcher); };
	};

	bool IsFinished() override final { return !sock_.valid(); }
	bool Restart(int fd) override final;
	void Reatach(ev::dynamic_loop &loop) override final;

	// Writer iterface implementation
	void WriteRPCReturn(Context &ctx, const Args &args) override final { responceRPC(ctx, errOK, args); }
	void SetClientData(ClientData::Ptr data) override final { clientData_ = data; }
	ClientData::Ptr GetClientData() override final { return clientData_; }

protected:
	// Generic callback
	void callback(ev::io &watcher, int revents);
	void write_cb();
	void read_cb();

	void timeout_cb(ev::periodic &watcher, int);

	void closeConn();
	void handleRPC(Context &ctx);
	void parseRPC();
	void responceRPC(Context &ctx, const Error &error, const Args &args);

	ev::io io_;
	ev::timer timeout_;
	socket sock_;
	int curEvents_ = 0;
	bool closeConn_ = false;
	bool respSent_ = false;

	cbuf<char> wrBuf_, rdBuf_;
	Dispatcher &dispatcher_;
	ClientData::Ptr clientData_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
