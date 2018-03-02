
#include "connection.h"
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

const auto kCProtoTimeoutSec = 300.;

Connection::Connection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher)
	: fd_(fd), curEvents_(0), wrBuf_(kRPCWriteBufSize), rdBuf_(kRPCReadbufSize), dispatcher_(dispatcher) {
	int flag = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &flag, sizeof(flag));

	io_.set<Connection, &Connection::callback>(this);
	io_.set(loop);
	timeout_.set<Connection, &Connection::timeout_cb>(this);
	timeout_.set(loop);

	callback(io_, ev::READ);
	timeout_.start(kCProtoTimeoutSec);
}

Connection::~Connection() {
	if (fd_ >= 0) {
		close(fd_);
		io_.stop();
	}
}

bool Connection::Restart(int fd) {
	assert(fd_ < 0);
	fd_ = fd;
	int flag = 1;
	setsockopt(fd, SOL_TCP, TCP_NODELAY, &flag, sizeof(flag));

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
	io_.start(fd_, curEvents_);
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

	if (curEvents_ != nevents && fd_ >= 0) {
		(curEvents_) ? io_.set(nevents) : io_.start(fd_, nevents);
		curEvents_ = nevents;
	}
}

// Socket is writable
void Connection::write_cb() {
	while (wrBuf_.size()) {
		auto it = wrBuf_.tail();
		ssize_t written = ::send(fd_, it.data, it.len, 0);

		if (written < 0 && errno == EINTR) continue;

		if (written < 0) {
			if (errno != EAGAIN && errno != EWOULDBLOCK) {
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
		ssize_t nread = ::recv(fd_, it.data, it.len, 0);

		if (nread < 0 && errno == EINTR) continue;

		if ((nread < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) || nread == 0) {
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

	if (fd_ >= 0) {
		io_.stop();
		close(fd_);
		fd_ = -1;
	}
	timeout_.stop();
	if (dispatcher_.onClose_) {
		Context ctx{nullptr, this};
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
		Context ctx{nullptr, this};
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
