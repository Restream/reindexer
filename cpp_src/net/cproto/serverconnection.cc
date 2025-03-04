#include "serverconnection.h"
#include <snappy.h>
#include "coroclientconnection.h"
#include "tools/serializer.h"

#ifdef REINDEX_WITH_V3_FOLLOWERS
#include "tools/logger.h"
#endif

namespace reindexer {
namespace net {
namespace cproto {

const auto kCProtoTimeoutSec = 300.;

#ifdef REINDEX_WITH_V3_FOLLOWERS
constexpr auto kUpdatesResendTimeout = 0.1;

ServerConnection::ServerConnection(socket&& s, ev::dynamic_loop& loop, Dispatcher& dispatcher, bool enableStat, size_t maxUpdatesSize,
								   bool enableCustomBalancing)
	: net::ConnectionST(std::move(s), loop, enableStat, kConnReadbufSize, kConnWriteBufSize, kCProtoTimeoutSec),
	  maxUpdatesSize_(maxUpdatesSize),
	  maxPendingUpdates_(kConnWriteBufSize * 0.8),
	  dispatcher_(dispatcher),
	  balancingType_(enableCustomBalancing ? BalancingType::NotSet : BalancingType::None) {
	assertrx(maxPendingUpdates_ > 0);
	assertrx(size_t(maxPendingUpdates_) < BaseConnT::wrBuf_.capacity());
	callback(io_, ev::READ);

	updatesAsync_.set<ServerConnection, &ServerConnection::async_cb>(this);
	updates_timeout_.set<ServerConnection, &ServerConnection::timeout_cb>(this);
	updatesAsync_.set(loop);
	updates_timeout_.set(loop);

	updates_timeout_.start(kUpdatesResendTimeout, kUpdatesResendTimeout);
	updatesAsync_.start();
}
#else	// REINDEX_WITH_V3_FOLLOWERS
ServerConnection::ServerConnection(socket&& s, ev::dynamic_loop& loop, Dispatcher& dispatcher, bool enableStat, bool enableCustomBalancing)
	: net::ConnectionST(std::move(s), loop, enableStat, kConnReadbufSize, kConnWriteBufSize, kCProtoTimeoutSec),
	  maxPendingUpdates_(kConnWriteBufSize * 0.8),
	  dispatcher_(dispatcher),
	  balancingType_(enableCustomBalancing ? BalancingType::NotSet : BalancingType::None) {
	assertrx(maxPendingUpdates_ > 0);
	assertrx(size_t(maxPendingUpdates_) < BaseConnT::wrBuf_.capacity());
	callback(io_, ev::READ);

	updatesAsync_.set<ServerConnection, &ServerConnection::async_cb>(this);
	updatesAsync_.set(loop);
	updatesAsync_.start();
}
#endif	// REINDEX_WITH_V3_FOLLOWERS

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
#ifdef REINDEX_WITH_V3_FOLLOWERS
		updates_timeout_.set(loop);
		updates_timeout_.start(kUpdatesResendTimeout, kUpdatesResendTimeout);
#endif	// REINDEX_WITH_V3_FOLLOWERS
	}
}

void ServerConnection::CallRPC(const IRPCCall& call) {
#ifdef REINDEX_WITH_V3_FOLLOWERS
	std::lock_guard lck(updatesMtx_);
	updatesV3_.emplace_back(call);
	updatesSize_ += call.data_->size();

	if (updatesSize_ > maxUpdatesSize_) {
		updatesV3_.clear();
		Args args;
		IRPCCall curCall = call;
		CmdCode cmd;
		std::string_view nsName;
		curCall.Get(&curCall, cmd, nsName, args);

		WrSerializer ser;
		ser.PutVString(nsName);
		IRPCCall callLost = {[](IRPCCall* callLost, CmdCode& cmd, std::string_view& ns, Args& args) {
								 Serializer s(callLost->data_->data(), callLost->data_->size());
								 cmd = kCmdUpdates;
								 args = {Arg(std::string(s.GetVString()))};
								 ns = std::string_view(args[0]);
							 },
							 make_intrusive<intrusive_atomic_rc_wrapper<chunk>>(ser.DetachChunk())};
		logPrintf(LogWarning, "Call updates lost clientAddr = %s updatesSize = %d", clientAddr_, updatesSize_);

		updatesSize_ = callLost.data_->size();
		updatesV3_.emplace_back(std::move(callLost));
		updateLostFlag_ = true;

		// No legacy replication stats in this version
		//		if (ConnectionST::stats_) {
		//			if (auto stat = ConnectionST::stats_->get_stat(); stat) {
		//				stat->updates_lost.fetch_add(1, std::memory_order_relaxed);
		//				stat->pended_updates.store(1, std::memory_order_relaxed);
		//			}
		//		}
	}
	// No legacy replication stats in this version
	//	else if (ConnectionST::stats_) {
	//		if (auto stat = ConnectionST::stats_->get_stat(); stat) {
	//			stat->pended_updates.store(updates_.size(), std::memory_order_relaxed);
	//		}
	//	}
#else	// REINDEX_WITH_V3_FOLLOWERS
	(void)call;
#endif	// REINDEX_WITH_V3_FOLLOWERS
}

void ServerConnection::SendEvent(chunk&& ch) {
	bool notify = false;
	{
		std::lock_guard lck(updatesMtx_);
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
#ifdef REINDEX_WITH_V3_FOLLOWERS
		updates_timeout_.stop();
		updates_timeout_.reset();
#endif	// REINDEX_WITH_V3_FOLLOWERS
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
		responceRPC(ctx, err, Args());
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
				responceRPC(ctx, Error(errParams, "Invalid cproto magic %08x", int(hdr.magic)), Args());
			} catch (std::exception& err) {
				fprintf(stderr, "responceRPC unexpected error: %s\n", err.what());
			}
			BaseConnT::closeConn_ = true;
			return BaseConnT::ReadResT::Default;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			try {
				responceRPC(
					ctx,
					Error(errParams, "Unsupported cproto version %04x. This server expects reindexer client v1.9.8+", int(hdr.version)),
					Args());
			} catch (std::exception& err) {
				fprintf(stderr, "responceRPC unexpected error: %s\n", err.what());
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

		// Rebalance connection, when first message was recieved
		if rx_unlikely (balancingType_ == BalancingType::NotSet) {
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

		BaseConnT::rdBuf_.erase(sizeof(hdr));

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
				if rx_unlikely (!snappy::Uncompress(it.data(), hdr.len, &uncompressed)) {
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
						throw Error(errLogic, "Incorrect variant type for 'execTimeout' type='%s'", ctxArgs[0].Type().Name());
					}
					ctx.call->execTimeout = milliseconds(int64_t(ctxArgs[0]));
				} else {
					ctx.call->execTimeout = milliseconds(0);
				}
				if (ctxArgs.size() > 1) {
					if (!ctxArgs[1].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'lsn' type='%s'", ctxArgs[1].Type().Name());
					}
					ctx.call->lsn = lsn_t(int64_t(ctxArgs[1]));
				} else {
					ctx.call->lsn = lsn_t();
				}
				if (ctxArgs.size() > 2) {
					if (!ctxArgs[2].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'emmiterServerId' type='%s'", ctxArgs[2].Type().Name());
					}
					ctx.call->emmiterServerId = int64_t(ctxArgs[2]);
				} else {
					ctx.call->emmiterServerId = -1;
				}
				if (ctxArgs.size() > 3) {
					if (!ctxArgs[3].Type().IsSame(KeyValueType::From<int64_t>())) {
						throw Error(errLogic, "Incorrect variant type for 'shardIdValue' type='%s'", ctxArgs[3].Type().Name());
					}
					const int64_t shardIdValue = int64_t(ctxArgs[3]);
					if (shardIdValue < 0) {
						if (shardIdValue < std::numeric_limits<int>::min()) {
							throw Error(errLogic, "Unexpected shard ID values: %d", shardIdValue);
						}
						ctx.call->shardId = shardIdValue;
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
				ctx.call->emmiterServerId = -1;
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

		BaseConnT::rdBuf_.erase(hdr.len);
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
		throw Error(errNetwork, "Too large RPC message(%d), size: %d bytes", hdr.cmd, ser.Len());
	}
	reinterpret_cast<CProtoHeader*>(ser.Buf() + savePos)->len = ser.Len() - savePos - sizeof(hdr);
}

static chunk packRPC(chunk chunk, Context& ctx, const Error& status, const Args& args, bool enableSnappy) {
	WrSerializer ser(std::move(chunk));
	packRPC(ser, ctx, status, args, enableSnappy);
	return ser.DetachChunk();
}

void ServerConnection::responceRPC(Context& ctx, const Error& status, const Args& args) {
	if rx_unlikely (ctx.respSent) {
		fprintf(stderr, "Warning - RPC responce already sent\n");
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
	// Exception occurs on unrecoverable error. Send responce, and drop connection
	fprintf(stderr, "Dropping RPC-connection. Reason: %s\n", err.what());
	try {
		if (!ctx.respSent) {
			responceRPC(ctx, err, Args());
		}
	} catch (std::exception& e) {
		fprintf(stderr, "responceRPC unexpected error: %s\n", e.what());
	} catch (...) {
		fprintf(stderr, "responceRPC unexpected error (unknow exception)\n");
	}
	BaseConnT::closeConn_ = true;
}

#ifdef REINDEX_WITH_V3_FOLLOWERS

void ServerConnection::sendUpdatesV3() {
	constexpr auto kMaxUpdatesBufSize = 1024 * 1024 * 8;
	if (BaseConnT::wrBuf_.size() + 10 > BaseConnT::wrBuf_.capacity() || BaseConnT::wrBuf_.data_size() > kMaxUpdatesBufSize / 2) {
		return;
	}

	std::vector<IRPCCall> updates;
	size_t updatesSizeCopy;
	{
		std::lock_guard lck(updatesMtx_);
		updates.swap(updatesV3_);
		updatesSizeCopy = updatesSize_;
		updatesSize_ = 0;
		updateLostFlag_ = false;
	}

	if (updates.empty()) {
		return;
	}

	RPCCall callUpdate{kCmdUpdates, 0, {}, milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, false};
	cproto::Context ctx{"", &callUpdate, this, {{}, {}}, false};
	size_t len = 0;
	Args args;
	CmdCode cmd;
	WrSerializer ser(BaseConnT::wrBuf_.get_chunk());
	size_t cnt = 0;
	size_t updatesSizeBuffered = 0;
	for (cnt = 0; cnt < updates.size() && ser.Len() < kMaxUpdatesBufSize; ++cnt) {
		if (updates[cnt].data_) {
			updatesSizeBuffered += updates[cnt].data_->size();
		}
		[[maybe_unused]] std::string_view ns;
		updates[cnt].Get(&updates[cnt], cmd, ns, args);
		packRPC(ser, ctx, Error(), args, enableSnappy_);
	}

	len = ser.Len();
	try {
		BaseConnT::wrBuf_.write(ser.DetachChunk());
	} catch (...) {
		RPCCall callLost{kCmdUpdates, 0, {}, milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, false};
		cproto::Context ctxLost{"", &callLost, this, {{}, {}}, false};
		{
			std::lock_guard lck(updatesMtx_);
			updatesV3_.clear();
			updatesSize_ = 0;
			updateLostFlag_ = false;

			logPrintf(LogWarning, "Call updates lost clientAddr = %s (wrBuf error)", clientAddr_);
			wrBuf_.clear();
			ser.Reset();
			packRPC(ser, ctxLost, Error(), {Arg(std::string(""))}, enableSnappy_);
			len = ser.Len();
			wrBuf_.write(ser.DetachChunk());
			if (BaseConnT::stats_) {
				BaseConnT::stats_->update_send_buf_size(wrBuf_.data_size());
				// No legacy replication stats in this version
				// BaseConnT::stats_->update_pending_updates(0);
			}
		}

		if (dispatcher_.OnResponseRef()) {
			ctx.stat.sizeStat.respSizeBytes = len;
			dispatcher_.OnResponseRef()(ctxLost);
		}

		callback(io_, ev::WRITE);
		return;
	}

	if (cnt != updates.size()) {
		std::lock_guard lck(updatesMtx_);
		if (!updateLostFlag_) {
			updatesV3_.insert(updatesV3_.begin(), updates.begin() + cnt, updates.end());
			updatesSize_ += updatesSizeCopy - updatesSizeBuffered;
		}
		// No legacy replication stats in this version
		// if (BaseConnT::stats_) stats_->update_pending_updates(updates.size());
	}
	// No legacy replication stats in this version
	//	else if (BaseConnT::stats_) {
	//		if (auto stat = BaseConnT::stats_->get_stat(); stat) {
	//			std::lock_guard lck(updates_mtx_);
	//			stat->pended_updates.store(updates_.size(), std::memory_order_relaxed);
	//		}
	//	}

	if (BaseConnT::stats_) {
		BaseConnT::stats_->update_send_buf_size(BaseConnT::wrBuf_.data_size());
	}

	if (dispatcher_.OnResponseRef()) {
		ctx.stat.sizeStat.respSizeBytes = len;
		dispatcher_.OnResponseRef()(ctx);
	}

	callback(BaseConnT::io_, ev::WRITE);
}

#endif	// REINDEX_WITH_V3_FOLLOWERS

void ServerConnection::sendUpdates() {
	assertrx_dbg(maxPendingUpdates_ >= BaseConnT::wrBuf_.size_atomic() + pendingUpdates());
	std::vector<chunk> updates;
	{
		std::lock_guard lck(updatesMtx_);
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
		std::lock_guard lck(updatesMtx_);
		updates_.insert(updates_.begin(), std::make_move_iterator(updates.begin() + cnt), std::make_move_iterator(updates.end()));
		currentUpdatesCnt_.store(updates_.size(), std::memory_order_release);
	}
	// Max updates count is very limited on this stage, so we do not collect separate pending updates stats here

	if (BaseConnT::stats_) {
		BaseConnT::stats_->update_send_buf_size(BaseConnT::wrBuf_.data_size());
	}

	callback(BaseConnT::io_, ev::WRITE);
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
