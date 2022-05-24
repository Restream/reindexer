#include "serverconnection.h"
#include <errno.h>
#include <snappy.h>
#include "coroclientconnection.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const auto kCProtoTimeoutSec = 300.;

ServerConnection::ServerConnection(int fd, ev::dynamic_loop &loop, Dispatcher &dispatcher, bool enableStat)
	: net::ConnectionST(fd, loop, enableStat), dispatcher_(dispatcher) {
	timeout_.start(kCProtoTimeoutSec);
	callback(io_, ev::READ);
}

ServerConnection::~ServerConnection() { closeConn(); }

bool ServerConnection::Restart(int fd) {
	restart(fd);
	timeout_.start(kCProtoTimeoutSec);
	callback(io_, ev::READ);
	return true;
}

void ServerConnection::Attach(ev::dynamic_loop &loop) {
	if (!attached_) {
		attach(loop);
		timeout_.start(kCProtoTimeoutSec);
	}
}

void ServerConnection::Detach() {
	if (attached_) {
		detach();
	}
}

void ServerConnection::onClose() {
	if (dispatcher_.onClose_) {
		Context ctx{"", nullptr, this, {{}, {}}, false};
		dispatcher_.onClose_(ctx, errOK);
	}
	clientData_.reset();
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
		assertrx(it.size() >= size_t(hdr.len));

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
			ctx.call->execTimeout = milliseconds(0);
			ctx.call->lsn = lsn_t();

			ctx.call->args.Unpack(ser);
			ctx.call->emmiterServerId = -1;
			ctx.call->shardId = -1;

			if (!ser.Eof()) {
				Args ctxArgs;
				ctxArgs.Unpack(ser);
				if (ctxArgs.size() > 0) {
					ctx.call->execTimeout = milliseconds(int64_t(ctxArgs[0]));
				}
				if (ctxArgs.size() > 1) {
					ctx.call->lsn = lsn_t(int64_t(ctxArgs[1]));
				}
				if (ctxArgs.size() > 2) {
					ctx.call->emmiterServerId = int64_t(ctxArgs[2]);
				}
				if (ctxArgs.size() > 3) {
					ctx.call->shardId = int(int64_t(ctxArgs[3]) & ~kShardingFlagsMask);
					ctx.call->shardingParallelExecution = (int64_t{ctxArgs[3]} & kShardingParallelExecutionBit) != 0;
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
	ser.Write(std::string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));

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

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
