

#include "connection.h"
#include <errno.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const auto kCProtoTimeoutSec = 300.;

Connection::Connection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher)
	: sock_(fd), curEvents_(0), wrBuf_(kRPCWriteBufSize), rdBuf_(kRPCReadbufSize), dispatcher_(dispatcher) {
	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	timeout_.set<Connection, &Connection::timeout_cb>(this);
	timeout_.set(loop);

	callback(io_, ev::READ);
	timeout_.start(kCProtoTimeoutSec);
}

Connection::~Connection() {
	if (sock_.valid()) {
		sock_.close();
		io_.stop();
	}
}

bool Connection::Restart(int fd) {
	assert(!sock_.valid());
	sock_ = fd;

	wrBuf_.clear();
	rdBuf_.clear();
	curEvents_ = 0;
	closeConn_ = false;
	respSent_ = false;
	callback(io_, ev::READ);
	timeout_.start(kCProtoTimeoutSec);
	return true;
}

void Connection::Reatach(ev::dynamic_loop &loop) {
	io_.stop();
	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	io_.start(sock_.fd(), curEvents_);
	timeout_.stop();
	timeout_.set<Connection, &Connection::timeout_cb>(this);
	timeout_.set(loop);
	timeout_.start(kCProtoTimeoutSec);
}

// Generic callback
void Connection::callback(ev::io & /*watcher*/, int revents) {
	if (ev::ERROR & revents) return;

	if (revents & ev::READ) {
		read_cb();
		revents |= ev::WRITE;
	}
	if (revents & ev::WRITE) {
		write_cb();
		if (!wrBuf_.size()) wrBuf_.clear();
	}

	int nevents = ev::READ | (wrBuf_.size() ? ev::WRITE : 0);

	if (curEvents_ != nevents && sock_.valid()) {
		(curEvents_) ? io_.set(nevents) : io_.start(sock_.fd(), nevents);
		curEvents_ = nevents;
	}
}

// Socket is writable
void Connection::write_cb() {
	while (wrBuf_.size()) {
		auto it = wrBuf_.tail();

		ssize_t written = sock_.send(it.data, it.len);
		int err = sock_.last_error();

		if (written < 0 && err == EINTR) continue;

		if (written < 0) {
			if (!socket::would_block(err)) {
				closeConn();
			}
			return;
		}

		wrBuf_.erase(written);
		if (written < ssize_t(it.len)) return;
	}
	if (closeConn_) {
		closeConn();
	}
}

// Receive message from client socket
void Connection::read_cb() {
	while (!closeConn_) {
		auto it = rdBuf_.head();
		ssize_t nread = sock_.recv(it.data, it.len);
		int err = sock_.last_error();

		if (nread < 0 && err == EINTR) continue;

		if ((nread < 0 && !socket::would_block(err)) || nread == 0) {
			closeConn();
			return;
		} else if (nread > 0) {
			rdBuf_.advance_head(nread);
			if (!closeConn_) parseRPC();
		}
		if (nread < ssize_t(it.len) || !rdBuf_.available()) return;
	}
}

void Connection::timeout_cb(ev::periodic & /*watcher*/, int /*time*/) { closeConn(); }

void Connection::closeConn() {
	io_.loop.break_loop();

	if (sock_.valid()) {
		io_.stop();
		sock_.close();
	}
	timeout_.stop();
	if (dispatcher_.onClose_) {
		Stat stat;
		Context ctx;
		ctx.call = nullptr;
		ctx.writer = this;
		ctx.stat = stat;

		dispatcher_.onClose_(ctx, errOK);
	}
	clientData_.reset();
}

void Connection::handleRPC(Context &ctx) {
	Error err = dispatcher_.handle(ctx);

	if (!respSent_) {
		responceRPC(ctx, err, Args());
	}
}

void Connection::parseRPC() {
	CProtoHeader hdr;

	while (!closeConn_) {
		Stat stat;
		Context ctx;
		ctx.call = nullptr;
		ctx.writer = this;
		ctx.stat = stat;

		auto len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));

		if (len < sizeof(hdr)) return;
		if (hdr.magic != kCprotoMagic || hdr.version != kCprotoVersion) {
			responceRPC(ctx, Error(errParams, "Invalid cproto header: magic=%08x or version=%08x", hdr.magic, hdr.version), Args());
			closeConn_ = true;
			return;
		}

		if (hdr.len + sizeof(hdr) > kRPCReadbufSize) {
			responceRPC(ctx, Error(errParams, "Too big request: len=%08x", hdr.len), Args());
			closeConn_ = true;
			return;
		}

		if ((rdBuf_.size() - sizeof(hdr)) < hdr.len) return;

		rdBuf_.erase(sizeof(hdr));

		auto it = rdBuf_.tail();
		if (it.len < hdr.len) {
			rdBuf_.unroll();
			it = rdBuf_.tail();
		}
		assert(it.len >= hdr.len);

		RPCCall call;
		ctx.call = &call;
		try {
			call.cmd = CmdCode(hdr.cmd);
			call.seq = hdr.seq;
			Serializer ser(it.data, hdr.len);
			call.args.Unpack(ser);
			handleRPC(ctx);
		} catch (const Error &err) {
			// Execption occurs on unrecoverble error. Send responce, and drop connection
			fprintf(stderr, "drop connect, reason: %s\n", err.what().c_str());
			responceRPC(ctx, err, Args());
			closeConn_ = true;
		}

		respSent_ = false;
		rdBuf_.erase(hdr.len);
		timeout_.start(kCProtoTimeoutSec);
	}
}

void Connection::responceRPC(Context &ctx, const Error &status, const Args &args) {
	if (respSent_) {
		fprintf(stderr, "Warning - RPC responce already sent\n");
		return;
	}
	if (dispatcher_.logger_ != nullptr) {
		dispatcher_.logger_(ctx, status, args);
	}

	WrSerializer ser;
	ser.PutVarUint(status.code());
	ser.PutVString(status.what());

	args.Pack(ser);

	CProtoHeader hdr;
	hdr.len = ser.Len();
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	if (ctx.call != nullptr) {
		hdr.cmd = ctx.call->cmd;
		hdr.seq = ctx.call->seq;
	} else {
		hdr.cmd = 0;
		hdr.seq = 0;
	}
	wrBuf_.write(reinterpret_cast<char *>(&hdr), sizeof(hdr));
	wrBuf_.write(reinterpret_cast<char *>(ser.Buf()), ser.Len());
	respSent_ = true;
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
