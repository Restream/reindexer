

#include "serverconnection.h"
#include <errno.h>
#include <snappy.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const auto kCProtoTimeoutSec = 300.;
const auto kUpdatesResendTimeout = 0.1;
const auto kMaxUpdatesBufSize = 1024 * 1024 * 8;

ServerConnection::ServerConnection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher, bool enableStat)
	: net::ConnectionST(fd, loop, enableStat), dispatcher_(dispatcher) {
	timeout_.start(kCProtoTimeoutSec);
	updates_async_.set<ServerConnection, &ServerConnection::async_cb>(this);
	updates_timeout_.set<ServerConnection, &ServerConnection::timeout_cb>(this);
	updates_async_.set(loop);
	updates_timeout_.set(loop);

	updates_timeout_.start(kUpdatesResendTimeout, kUpdatesResendTimeout);
	updates_async_.start();

	callback(io_, ev::READ);
}

ServerConnection::~ServerConnection() { closeConn(); }

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
		Context ctx{"", nullptr, this, {{}, {}}, false};
		dispatcher_.onClose_(ctx, errOK);
	}
	clientData_.reset();
	std::unique_lock<std::mutex> lck(updates_mtx_);
	updates_.clear();
	if (ConnectionST::stats_) ConnectionST::stats_->update_pended_updates(0);
}

void ServerConnection::handleRPC(Context &ctx) {
	Error err = dispatcher_.handle(ctx);

	if (!ctx.respSent) {
		responceRPC(ctx, err, Args());
	}
}

void ServerConnection::onRead() {
	CProtoHeader hdr;

	while (!closeConn_) {
		Context ctx{clientAddr_, nullptr, this, {{}, {}}, false};
		std::string uncompressed;

		auto len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));
		if (len < sizeof(hdr)) return;

		if (hdr.magic != kCprotoMagic) {
			try {
				responceRPC(ctx, Error(errParams, "Invalid cproto magic %08x", int(hdr.magic)), Args());
			} catch (const Error &err) {
				fprintf(stderr, "responceRPC unexpected error: %s", err.what().c_str());
			}
			closeConn_ = true;
			return;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			try {
				responceRPC(
					ctx,
					Error(errParams, "Unsupported cproto version %04x. This server expects reindexer client v1.9.8+", int(hdr.version)),
					Args());
			} catch (const Error &err) {
				fprintf(stderr, "responceRPC unexpected error: %s", err.what().c_str());
			}
			closeConn_ = true;
			return;
		}
		// Enable compression, only if clients sand compressed data to us
		enableSnappy_ = (hdr.version >= kCprotoMinSnappyVersion) && hdr.compressed;

		if (size_t(hdr.len) + sizeof(hdr) > rdBuf_.capacity()) {
			rdBuf_.reserve(size_t(hdr.len) + sizeof(hdr) + 0x1000);
		}

		if (size_t(hdr.len) + sizeof(hdr) > rdBuf_.size()) {
			if (!rdBuf_.size()) rdBuf_.clear();
			return;
		}

		rdBuf_.erase(sizeof(hdr));

		auto it = rdBuf_.tail();
		if (it.size() < size_t(hdr.len)) {
			rdBuf_.unroll();
			it = rdBuf_.tail();
		}
		assert(it.size() >= size_t(hdr.len));

		ctx.call = &call_;
		try {
			ctx.stat.sizeStat.reqSizeBytes = size_t(hdr.len) + sizeof(hdr);
			ctx.call->cmd = CmdCode(hdr.cmd);
			ctx.call->seq = hdr.seq;
			Serializer ser(it.data(), hdr.len);
			if (hdr.compressed) {
				if (!snappy::Uncompress(it.data(), hdr.len, &uncompressed)) {
					throw Error(errParseBin, "Can't decompress data from peer");
				}

				ser = Serializer(uncompressed);
			}
			ctx.call->execTimeout_ = milliseconds(0);

			ctx.call->args.Unpack(ser);

			if (!ser.Eof()) {
				Args ctxArgs;
				ctxArgs.Unpack(ser);
				if (ctxArgs.size() > 0) {
					ctx.call->execTimeout_ = milliseconds(int64_t(ctxArgs[0]));
				}
			}

			handleRPC(ctx);
		} catch (const Error &err) {
			// Exception occurs on unrecoverable error. Send responce, and drop connection
			fprintf(stderr, "drop connect, reason: %s\n", err.what().c_str());
			try {
				responceRPC(ctx, err, Args());
			} catch (const Error &err) {
				fprintf(stderr, "responceRPC unexpected error: %s", err.what().c_str());
			}
			closeConn_ = true;
		}

		rdBuf_.erase(hdr.len);
		timeout_.start(kCProtoTimeoutSec);
	}
}
static void packRPC(WrSerializer &ser, Context &ctx, const Error &status, const Args &args, bool enableSnappy) {
	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.compressed = enableSnappy;

	if (ctx.call != nullptr) {
		hdr.cmd = ctx.call->cmd;
		hdr.seq = ctx.call->seq;
	} else {
		hdr.cmd = 0;
		hdr.seq = 0;
	}

	size_t savePos = ser.Len();
	ser.Write(string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));

	ser.PutVarUint(status.code());
	ser.PutVString(status.what());
	args.Pack(ser);

	if (hdr.compressed) {
		auto data = ser.Slice().substr(sizeof(hdr) + savePos);
		std::string compressed;
		snappy::Compress(data.data(), data.length(), &compressed);
		ser.Reset(sizeof(hdr) + savePos);
		ser.Write(compressed);
	}
	if (ser.Len() - savePos >= size_t(std::numeric_limits<int32_t>::max())) {
		throw Error(errNetwork, "Too large RPC message(%d), size: %d bytes", hdr.cmd, ser.Len());
	}
	reinterpret_cast<CProtoHeader *>(ser.Buf() + savePos)->len = ser.Len() - savePos - sizeof(hdr);
}

static chunk packRPC(chunk chunk, Context &ctx, const Error &status, const Args &args, bool enableSnappy) {
	WrSerializer ser(std::move(chunk));
	packRPC(ser, ctx, status, args, enableSnappy);
	return ser.DetachChunk();
}

void ServerConnection::responceRPC(Context &ctx, const Error &status, const Args &args) {
	if (ctx.respSent) {
		fprintf(stderr, "Warning - RPC responce already sent\n");
		return;
	}

	auto &&chunk = packRPC(wrBuf_.get_chunk(), ctx, status, args, enableSnappy_);
	auto len = chunk.len_;
	wrBuf_.write(std::move(chunk));
	if (ConnectionST::stats_) ConnectionST::stats_->update_send_buf_size(wrBuf_.data_size());

	if (dispatcher_.onResponse_) {
		ctx.stat.sizeStat.respSizeBytes = len;
		dispatcher_.onResponse_(ctx);
	}

	ctx.respSent = true;
	//	if (canWrite_) {
	//		write_cb();
	//	}

	if (dispatcher_.logger_ != nullptr) {
		dispatcher_.logger_(ctx, status, args);
	}
}

void ServerConnection::CallRPC(const IRPCCall &call) {
	std::unique_lock<std::mutex> lck(updates_mtx_);
	updates_.emplace_back(call);
	if (ConnectionST::stats_) {
		auto stat = ConnectionST::stats_->get_stat();
		if (stat) {
			stat->pended_updates.store(updates_.size(), std::memory_order_relaxed);
		}
	}
}

void ServerConnection::sendUpdates() {
	if (wrBuf_.size() + 10 > wrBuf_.capacity() || wrBuf_.data_size() > kMaxUpdatesBufSize / 2) {
		return;
	}

	std::vector<IRPCCall> updates;
	updates_mtx_.lock();
	updates.swap(updates_);
	updates_mtx_.unlock();
	RPCCall call{kCmdUpdates, 0, {}, milliseconds(0)};
	cproto::Context ctx{"", &call, this, {{}, {}}, false};
	size_t len = 0;
	Args args;
	CmdCode cmd;

	WrSerializer ser(wrBuf_.get_chunk());
	size_t cnt = 0;
	for (cnt = 0; cnt < updates.size() && ser.Len() < kMaxUpdatesBufSize; ++cnt) {
		updates[cnt].Get(&updates[cnt], cmd, args);
		packRPC(ser, ctx, Error(), args, enableSnappy_);
	}

	if (cnt != updates.size()) {
		std::unique_lock<std::mutex> lck(updates_mtx_);
		updates_.insert(updates_.begin(), updates.begin() + cnt, updates.end());
		if (ConnectionST::stats_) stats_->update_pended_updates(updates.size());
	} else if (ConnectionST::stats_) {
		auto stat = ConnectionST::stats_->get_stat();
		if (stat) {
			std::unique_lock<std::mutex> lck(updates_mtx_);
			stat->pended_updates.store(updates_.size(), std::memory_order_relaxed);
		}
	}

	len = ser.Len();
	wrBuf_.write(ser.DetachChunk());
	if (ConnectionST::stats_) ConnectionST::stats_->update_send_buf_size(wrBuf_.data_size());

	if (dispatcher_.onResponse_) {
		ctx.stat.sizeStat.respSizeBytes = len;
		dispatcher_.onResponse_(ctx);
	}

	callback(io_, ev::WRITE);
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
