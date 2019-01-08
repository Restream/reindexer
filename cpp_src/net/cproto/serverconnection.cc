

#include "serverconnection.h"
#include <errno.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const auto kCProtoTimeoutSec = 300.;
const auto kUpdatesResendTimeout = 0.1;

ServerConnection::ServerConnection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher)
	: net::ConnectionST(fd, loop), dispatcher_(dispatcher) {
	timeout_.start(kCProtoTimeoutSec);
	updates_async_.set<ServerConnection, &ServerConnection::async_cb>(this);
	updates_timeout_.set<ServerConnection, &ServerConnection::timeout_cb>(this);
	updates_async_.set(loop);
	updates_timeout_.set(loop);

	updates_timeout_.start(kUpdatesResendTimeout, kUpdatesResendTimeout);
	updates_async_.start();

	callback(io_, ev::READ);
}

bool ServerConnection::Restart(int fd) {
	restart(fd);
	timeout_.start(kCProtoTimeoutSec);
	updates_async_.start();
	callback(io_, ev::READ);
	return true;
}

void ServerConnection::Attach(ev::dynamic_loop &loop) {
	async_.set<ServerConnection, &ServerConnection::async_cb>(this);
	if (!attached_) {
		attach(loop);

		timeout_.start(kCProtoTimeoutSec);
		updates_async_.set(loop);
		updates_async_.start();
		updates_timeout_.set(loop);
		updates_timeout_.start(kUpdatesResendTimeout, kUpdatesResendTimeout);
	}
}

void ServerConnection::Detach() {
	if (attached_) {
		detach();
		updates_async_.stop();
		updates_async_.reset();
		updates_timeout_.stop();
		updates_timeout_.reset();
	}
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
	updates_mtx_.lock();
	updates_.clear();
	updates_mtx_.unlock();
}

void ServerConnection::handleRPC(Context &ctx) {
	Error err = dispatcher_.handle(ctx);

	if (!ctx.respSent_) {
		responceRPC(ctx, err, Args());
	}
}

void ServerConnection::onRead() {
	CProtoHeader hdr;

	while (!closeConn_) {
		Context ctx;
		ctx.call = nullptr;
		ctx.writer = this;
		ctx.respSent_ = false;

		auto len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));

		if (len < sizeof(hdr)) return;
		if (hdr.magic != kCprotoMagic) {
			responceRPC(ctx, Error(errParams, "Invalid cproto magic %08x", int(hdr.magic)), Args());
			closeConn_ = true;
			return;
		}

		if (hdr.version < kCprotoVersion) {
			responceRPC(ctx,
						Error(errParams, "Unsupported cproto version %04x. This server expects reindexer client v1.9.8+", int(hdr.version)),
						Args());
			closeConn_ = true;
			return;
		}

		if (hdr.len + sizeof(hdr) > rdBuf_.capacity()) {
			rdBuf_.reserve(hdr.len + sizeof(hdr) + 0x1000);
		}

		if (hdr.len + sizeof(hdr) > rdBuf_.size()) {
			if (!rdBuf_.size()) rdBuf_.clear();
			return;
		}

		rdBuf_.erase(sizeof(hdr));

		auto it = rdBuf_.tail();
		if (it.size() < hdr.len) {
			rdBuf_.unroll();
			it = rdBuf_.tail();
		}
		assert(it.size() >= hdr.len);

		ctx.call = &call_;
		try {
			ctx.call->cmd = CmdCode(hdr.cmd);
			ctx.call->seq = hdr.seq;
			Serializer ser(it.data(), hdr.len);
			ctx.call->args.Unpack(ser);
			handleRPC(ctx);
		} catch (const Error &err) {
			// Execption occurs on unrecoverble error. Send responce, and drop connection
			fprintf(stderr, "drop connect, reason: %s\n", err.what().c_str());
			responceRPC(ctx, err, Args());
			closeConn_ = true;
		}

		rdBuf_.erase(hdr.len);
		timeout_.start(kCProtoTimeoutSec);
	}
}

static chunk packRPC(chunk chunk, Context &ctx, const Error &status, const Args &args) {
	WrSerializer ser(std::move(chunk));

	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	if (ctx.call != nullptr) {
		hdr.cmd = ctx.call->cmd;
		hdr.seq = ctx.call->seq;
	} else {
		hdr.cmd = 0;
		hdr.seq = 0;
	}

	ser.Write(string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));
	ser.PutVarUint(status.code());
	ser.PutVString(status.what());
	args.Pack(ser);
	reinterpret_cast<CProtoHeader *>(ser.Buf())->len = ser.Len() - sizeof(hdr);
	return ser.DetachChunk();
}

void ServerConnection::responceRPC(Context &ctx, const Error &status, const Args &args) {
	if (ctx.respSent_) {
		fprintf(stderr, "Warning - RPC responce already sent\n");
		return;
	}

	wrBuf_.write(packRPC(wrBuf_.get_chunk(), ctx, status, args));

	ctx.respSent_ = true;
	// if (canWrite_) {
	// 	write_cb();
	// }

	if (dispatcher_.logger_ != nullptr) {
		dispatcher_.logger_(ctx, status, args);
	}
}

void ServerConnection::CallRPC(CmdCode cmd, const Args &args) {
	RPCCall call{cmd, 0, {}};
	cproto::Context ctx{&call, this, {}, false};
	auto packed = packRPC(chunk(), ctx, errOK, args);
	updates_mtx_.lock();
	updates_.emplace_back(std::move(packed));
	updates_mtx_.unlock();
	// async_.send();
}

void ServerConnection::sendUpdates() {
	if (wrBuf_.size() + 10 > wrBuf_.capacity()) {
		return;
	}

	std::vector<chunk> updates;
	updates_mtx_.lock();
	updates.swap(updates_);
	updates_mtx_.unlock();
	if (updates.size() > 2) {
		WrSerializer ser(wrBuf_.get_chunk());
		for (auto &ch : updates) {
			ser.Write(string_view(reinterpret_cast<char *>(ch.data()), ch.size()));
		}
		wrBuf_.write(ser.DetachChunk());
	} else {
		for (auto &ch : updates) {
			wrBuf_.write(std::move(ch));
		}
	}

	callback(io_, ev::WRITE);
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
