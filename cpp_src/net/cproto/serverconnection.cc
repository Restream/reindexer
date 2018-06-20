

#include "serverconnection.h"
#include <errno.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const auto kCProtoTimeoutSec = 300.;

ServerConnection::ServerConnection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher)
	: net::ConnectionST(fd, loop), dispatcher_(dispatcher) {
	timeout_.start(kCProtoTimeoutSec);
	callback(io_, ev::READ);
}

bool ServerConnection::Restart(int fd) {
	restart(fd);
	respSent_ = false;
	callback(io_, ev::READ);
	timeout_.start(kCProtoTimeoutSec);
	return true;
}

void ServerConnection::Attach(ev::dynamic_loop &loop) {
	if (!attached_) {
		attach(loop);
		timeout_.start(kCProtoTimeoutSec);
	}
}

void ServerConnection::Detach() {
	if (attached_) detach();
}

void ServerConnection::onClose() {
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

void ServerConnection::handleRPC(Context &ctx) {
	Error err = dispatcher_.handle(ctx);

	if (!respSent_) {
		responceRPC(ctx, err, Args());
	}
}

void ServerConnection::onRead() {
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

		if (hdr.len + sizeof(hdr) > rdBuf_.capacity()) {
			rdBuf_.reserve(hdr.len + sizeof(hdr) + 0x1000);
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

void ServerConnection::responceRPC(Context &ctx, const Error &status, const Args &args) {
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
	if (canWrite_) {
		write_cb();
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
