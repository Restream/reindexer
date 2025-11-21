#include "serverconnection.h"
#include <snappy.h>
#include "coroclientconnection.h"
#include "tools/serializer.h"

namespace reindexer::net::cproto {

const auto kCProtoTimeoutSec = 300.;

ServerConnection::ServerConnection(socket&& s, ev::dynamic_loop& loop, Dispatcher& dispatcher, bool enableStat, bool enableCustomBalancing)
	: net::ConnectionST(std::move(s), loop, enableStat, kConnReadbufSize, kConnWriteBufSize, kCProtoTimeoutSec),
	  maxPendingUpdates_(size_t(kConnWriteBufSize * 0.8f)),
	  dispatcher_(dispatcher),
	  balancingType_(enableCustomBalancing ? BalancingType::NotSet : BalancingType::None) {
	assertrx(maxPendingUpdates_ > 0);
	assertrx(size_t(maxPendingUpdates_) < BaseConnT::wrBuf_.capacity());
	callback(io_, ev::READ);

	updatesAsync_.set<ServerConnection, &ServerConnection::async_cb>(this);
	updatesAsync_.set(loop);
	updatesAsync_.start();
}

ServerConnection::~ServerConnection() { BaseConnT::closeConn(); }

bool ServerConnection::Restart(socket&& s) {
	BaseConnT::restart(std::move(s));
	updatesAsync_.start();
	callback(io_, ev::READ);
	return true;
}

void ServerConnection::Attach(ev::dynamic_loop& loop) {
	if (!BaseConnT::attached_) {
		BaseConnT::attach(loop);
		updatesAsync_.set(loop);
		updatesAsync_.start();
		io_.set<ServerConnection, &ServerConnection::callback>(this);  // Override io-callback
	}
}

void ServerConnection::SendEvent(chunk&& ch) {
	bool notify = false;
	{
		lock_guard lck(updatesMtx_);
		notify = updates_.empty();
		updates_.emplace_back(std::move(ch));
		currentUpdatesCnt_.fetch_add(1, std::memory_order_acq_rel);
	}
	if (notify) {
		updatesAsync_.send();
	}
}

void ServerConnection::Detach() {
	if (BaseConnT::attached_) {
		BaseConnT::detach();
		updatesAsync_.stop();
		updatesAsync_.reset();
	}
}

void ServerConnection::onClose() {
	if (dispatcher_.OnCloseRef()) {
		Context ctx{"", nullptr, this, {{}, {}}, false};
		dispatcher_.OnCloseRef()(ctx, errOK);
	}
	clientData_.reset();
	balancingType_ = BalancingType::NotSet;
	rebalance_ = nullptr;
}

void ServerConnection::handleRPC(Context& ctx) {
	Error err = dispatcher_.Handle(ctx);

	if (!ctx.respSent) {
		responseRPC(ctx, err, Args());
	}
}

ServerConnection::BaseConnT::ReadResT ServerConnection::onRead() {
	CProtoHeader hdr;

	while (!BaseConnT::closeConn_) {
		Context ctx{BaseConnT::clientAddr_, nullptr, this, {{}, {}}, false};
		std::string uncompressed;

		auto len = BaseConnT::rdBuf_.peek(reinterpret_cast<char*>(&hdr), sizeof(hdr));
		if (len < sizeof(hdr)) {
			return BaseConnT::ReadResT::Default;
		}

		if (hdr.magic != kCprotoMagic) {
			try {
				responseRPC(ctx, Error(errParams, "Invalid cproto magic {:08x}", int(hdr.magic)), Args());
			} catch (std::exception& err) {
				fprintf(stderr, "reindexer error: responseRPC unexpected error: %s\n", err.what());
			}
			BaseConnT::closeConn_ = true;
			return BaseConnT::ReadResT::Default;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			try {
				responseRPC(
					ctx,
					Error(errParams, "Unsupported cproto version {:04x}. This server expects reindexer client v1.9.8+", int(hdr.version)),
					Args());
			} catch (std::exception& err) {
				fprintf(stderr, "reindexer error: responseRPC unexpected error: %s\n", err.what());
			}
			BaseConnT::closeConn_ = true;
			return BaseConnT::ReadResT::Default;
		}
		// Enable compression, only if clients sand compressed data to us
#ifdef WIN32
		enableSnappy_ = false;	// Disable compression on Windows #1823
#else
		enableSnappy_ = (hdr.version >= kCprotoMinSnappyVersion) && hdr.compressed;
#endif

		// Rebalance connection, when first message was received
		if (balancingType_ == BalancingType::NotSet) [[unlikely]] {
			balancingType_ = (hdr.dedicatedThread && hdr.version >= kCprotoMinDedicatedThreadsVersion) ? BalancingType::Dedicated
																									   : BalancingType::Shared;
			hasPendingData_ = true;
			if (rebalance_) {
				rebalance_(this, balancingType_);
				// After rebalancing this connection will probably be handled in another thread. Any code here after rebalance_() may lead
				// to data race
				return BaseConnT::ReadResT::Rebalanced;
			}
			return BaseConnT::ReadResT::Default;
		}

		if (size_t(hdr.len) + sizeof(hdr) > BaseConnT::rdBuf_.capacity()) {
			BaseConnT::rdBuf_.reserve(size_t(hdr.len) + sizeof(hdr) + 0x1000);
		}

		if (size_t(hdr.len) + sizeof(hdr) > BaseConnT::rdBuf_.size()) {
			if (!BaseConnT::rdBuf_.size()) {
				BaseConnT::rdBuf_.clear();
			}
			return BaseConnT::ReadResT::Default;
		}

		std::ignore = BaseConnT::rdBuf_.erase(sizeof(hdr));

		auto it = BaseConnT::rdBuf_.tail();
		if (it.size() < size_t(hdr.len)) {
			BaseConnT::rdBuf_.unroll();
			it = BaseConnT::rdBuf_.tail();
		}
		assertrx(it.size() >= size_t(hdr.len));

		ctx.call = &call_;
		try {
			ctx.stat.sizeStat.reqSizeBytes = size_t(hdr.len) + sizeof(hdr);
			ctx.call->cmd = CmdCode(hdr.cmd);
			ctx.call->seq = hdr.seq;
			Serializer ser(it.data(), hdr.len);
			if (hdr.compressed) {
				if (!snappy::Uncompress(it.data(), hdr.len, &uncompressed)) [[unlikely]] {
					throw Error(errParseBin, "Can't decompress data from peer");
				}

				ser = Serializer(uncompressed);
			}

			ctx.call->args.Unpack(ser);
			if (!ser.Eof()) {
				Args ctxArgs;
				ctxArgs.Unpack(ser);
				if (ctxArgs.size() > 0) {
					if (!ctxArgs[0].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'execTimeout' type='{}'", ctxArgs[0].Type().Name());
					}
					ctx.call->execTimeout = milliseconds(int64_t(ctxArgs[0]));
				} else {
					ctx.call->execTimeout = milliseconds(0);
				}
				if (ctxArgs.size() > 1) {
					if (!ctxArgs[1].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'lsn' type='{}'", ctxArgs[1].Type().Name());
					}
					ctx.call->lsn = lsn_t(int64_t(ctxArgs[1]));
				} else {
					ctx.call->lsn = lsn_t();
				}
				if (ctxArgs.size() > 2) {
					if (!ctxArgs[2].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'emitterServerId' type='{}'", ctxArgs[2].Type().Name());
					}
					ctx.call->emitterServerId = int64_t(ctxArgs[2]);
				} else {
					ctx.call->emitterServerId = -1;
				}
				if (ctxArgs.size() > 3) {
					if (!ctxArgs[3].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'shardIdValue' type='{}'", ctxArgs[3].Type().Name());
					}
					const int64_t shardIdValue = int64_t(ctxArgs[3]);
					if (shardIdValue < 0) {
						if (shardIdValue < std::numeric_limits<int>::min()) {
							throw Error(errLogic, "Unexpected shard ID values: {}", shardIdValue);
						}
						ctx.call->shardId = int(shardIdValue);
						ctx.call->shardingParallelExecution = false;
					} else {
						ctx.call->shardId = int(shardIdValue & ~kShardingFlagsMask);
						ctx.call->shardingParallelExecution = (shardIdValue & kShardingParallelExecutionBit);
					}
				} else {
					ctx.call->shardId = -1;
					ctx.call->shardingParallelExecution = false;
				}
			} else {
				ctx.call->execTimeout = milliseconds(0);
				ctx.call->lsn = lsn_t();
				ctx.call->emitterServerId = -1;
				ctx.call->shardId = -1;
				ctx.call->shardingParallelExecution = false;
			}

			handleRPC(ctx);
		} catch (const Error& err) {
			handleException(ctx, err);
		} catch (const std::exception& err) {
			handleException(ctx, Error(errLogic, err.what()));
		} catch (...) {
			handleException(ctx, Error(errLogic, "Unknown exception"));
		}

		std::ignore = BaseConnT::rdBuf_.erase(hdr.len);
	}
	return BaseConnT::ReadResT::Default;
}

static void packRPC(WrSerializer& ser, Context& ctx, const Error& status, const Args& args, bool enableSnappy) {
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
	ser.Write(std::string_view(reinterpret_cast<char*>(&hdr), sizeof(hdr)));

	ser.PutVarUint(status.code());
	ser.PutVString(status.whatStr());
	args.Pack(ser);

	if (hdr.compressed) {
		auto data = ser.Slice().substr(sizeof(hdr) + savePos);
		std::string compressed;
		snappy::Compress(data.data(), data.length(), &compressed);
		ser.Reset(sizeof(hdr) + savePos);
		ser.Write(compressed);
	}
	if (ser.Len() - savePos >= size_t(std::numeric_limits<int32_t>::max())) {
		throw Error(errNetwork, "Too large RPC message({}), size: {} bytes", hdr.cmd, ser.Len());
	}
	reinterpret_cast<CProtoHeader*>(ser.Buf() + savePos)->len = ser.Len() - savePos - sizeof(hdr);
}

static chunk packRPC(chunk chunk, Context& ctx, const Error& status, const Args& args, bool enableSnappy) {
	WrSerializer ser(std::move(chunk));
	packRPC(ser, ctx, status, args, enableSnappy);
	return ser.DetachChunk();
}

void ServerConnection::responseRPC(Context& ctx, const Error& status, const Args& args) {
	if (ctx.respSent) [[unlikely]] {
		fprintf(stderr, "reindexer warning: RPC response already sent\n");
		return;
	}

	auto&& chunk = packRPC(BaseConnT::wrBuf_.get_chunk(), ctx, status, args, enableSnappy_);
	auto len = chunk.len();
	BaseConnT::wrBuf_.write(std::move(chunk));
	if (BaseConnT::stats_) {
		BaseConnT::stats_->update_send_buf_size(BaseConnT::wrBuf_.data_size());
	}

	if (dispatcher_.OnResponseRef()) {
		ctx.stat.sizeStat.respSizeBytes = len;
		dispatcher_.OnResponseRef()(ctx);
	}

	ctx.respSent = true;
	//	if (canWrite_) {
	//		write_cb();
	//	}

	if (dispatcher_.LoggerRef()) {
		dispatcher_.LoggerRef()(ctx, status, args);
	}
}

void ServerConnection::handleException(Context& ctx, const Error& err) noexcept {
	// Exception occurs on unrecoverable error. Send response, and drop connection
	fprintf(stderr, "reindexer error: dropping RPC-connection. Reason: %s\n", err.what());
	try {
		if (!ctx.respSent) {
			responseRPC(ctx, err, Args());
		}
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: responseRPC unexpected error: %s\n", e.what());
	} catch (...) {
		fprintf(stderr, "reindexer error: responseRPC unexpected error (unknown exception)\n");
	}
	BaseConnT::closeConn_ = true;
}

void ServerConnection::sendUpdates() {
	assertrx_dbg(maxPendingUpdates_ >= BaseConnT::wrBuf_.size_atomic() + pendingUpdates());
	std::vector<chunk> updates;
	{
		lock_guard lck(updatesMtx_);
		updates.swap(updates_);
	}

	if (updates.empty()) {
		return;
	}

	RPCCall callUpdate{kCmdUpdates, 0, {}, milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, false};
	cproto::Context ctx{"", &callUpdate, this, {{}, {}}, false};

	Args args;
	size_t cnt = 0;
	size_t len = 0;

	assertrx_dbg(BaseConnT::wrBuf_.capacity() >= BaseConnT::wrBuf_.size_atomic() + updates.size());
	WrSerializer ser(BaseConnT::wrBuf_.get_chunk());
	for (cnt = 0; cnt < updates.size(); ++cnt) {
		args.clear<false>();
		auto& upd = updates[cnt];
		std::string_view updateData(upd);
		args.emplace_back(p_string(&updateData), Variant::noHold);
		packRPC(ser, ctx, Error(), args, enableSnappy_);

		len += ser.Len();
		BaseConnT::wrBuf_.write(ser.DetachChunk());

		if (BaseConnT::wrBuf_.size() + 2 >= BaseConnT::wrBuf_.capacity()) {
			break;
		}

		upd.clear();
		ser = WrSerializer(std::move(upd));
	}

	// Single OnResponse call for multiple updates seems fine
	if (dispatcher_.OnResponseRef()) {
		ctx.stat.sizeStat.respSizeBytes = len;
		dispatcher_.OnResponseRef()(ctx);
	}

	if (cnt == updates.size()) {
		[[maybe_unused]] auto res = currentUpdatesCnt_.fetch_sub(cnt, std::memory_order_acq_rel);
		assertrx_dbg(res >= int64_t(cnt));
	} else {
		lock_guard lck(updatesMtx_);
		updates_.insert(updates_.begin(), std::make_move_iterator(updates.begin() + cnt), std::make_move_iterator(updates.end()));
		currentUpdatesCnt_.store(updates_.size(), std::memory_order_release);
	}
	// Max updates count is very limited on this stage, so we do not collect separate pending updates stats here

	if (BaseConnT::stats_) {
		BaseConnT::stats_->update_send_buf_size(BaseConnT::wrBuf_.data_size());
	}

	callback(BaseConnT::io_, ev::WRITE);
}

}  // namespace reindexer::net::cproto
