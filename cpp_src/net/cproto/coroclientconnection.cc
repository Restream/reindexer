#include "coroclientconnection.h"
#include <errno.h>
#include <snappy.h>
#include <functional>
#include "core/rdxcontext.h"
#include "reindexer_version.h"
#include "server/rpcqrwatcher.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace net {
namespace cproto {

constexpr size_t kMaxRecycledChuncks = 512;
constexpr size_t kMaxChunckSizeToRecycle = 8192;
constexpr size_t kMaxParallelRPCCalls = 512;
constexpr auto kCoroSleepGranularity = std::chrono::milliseconds(150);
constexpr auto kDeadlineCheckInterval = std::chrono::milliseconds(100);
constexpr auto kKeepAliveInterval = std::chrono::seconds(30);
constexpr size_t kReadBufReserveSize = 4096;
constexpr size_t kWrChannelSize = 30;
constexpr size_t kCntToSendNow = 30;
constexpr size_t kMaxSerializedSize = 8 * 1024 * 1024;
constexpr size_t kDataToSendNow = 8192;
constexpr BindingCapabilities kClientCaps = BindingCapabilities(kBindingCapabilityQrIdleTimeouts | kBindingCapabilityResultsWithShardIDs |
																kBindingCapabilityIncarnationTags | kBindingCapabilityComplexRank);

CoroClientConnection::CoroClientConnection()
	: rpcCalls_(kMaxParallelRPCCalls), wrCh_(kWrChannelSize), seqNums_(kMaxParallelRPCCalls), conn_(kReadBufReserveSize, false) {
	recycledChuncks_.reserve(kMaxRecycledChuncks);
	errSyncCh_.close();
	seqNums_.close();
}

CoroClientConnection::~CoroClientConnection() {
	try {
		Stop();
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: unexpected exception in ~CoroClientConnection: %s\n", e.what());
	}
}

void CoroClientConnection::Start(ev::dynamic_loop& loop, ConnectData&& connectData) {
	if (!isRunning_) {
		// Don't allow to call Start, while error handling is in progress
		std::ignore = errSyncCh_.pop();

		if (loop_ != &loop) {
			if (loop_) {
				conn_.detach();
			}
			conn_.attach(loop);
			loop_ = &loop;
		}
		conn_.set_connect_timeout(connectData.opts.loginTimeout);

		if (!seqNums_.opened()) {
			seqNums_.reopen();
			loop_->spawn(wg_, [this] {
				for (size_t i = 1; i < seqNums_.capacity(); ++i) {
					// seq num == 0 is reserved for login
					seqNums_.push(i);
				}
			});
		}

		connectData_ = std::move(connectData);
		if (!wrCh_.opened()) {
			wrCh_.reopen();
		}

		loop_->spawn(wg_, [this] { writerRoutine(); });
		loop_->spawn(wg_, [this] { deadlineRoutine(); });
		loop_->spawn(wg_, [this] { pingerRoutine(); });

		isRunning_ = true;
	}
}

void CoroClientConnection::Stop() {
	if (isRunning_) {
		std::ignore = errSyncCh_.pop();
		errSyncCh_.reopen();

		terminate_ = true;
		wrCh_.close();
		conn_.close_conn(k_sock_closed_err);
		const Error err(errNetwork, "Connection closed");
		// Cancel all the system requests
		for (auto& c : rpcCalls_) {
			if (c.used && c.rspCh.opened() && !c.rspCh.full() && c.system) {
				c.rspCh.push(err);
			}
		}
		wg_.wait();
		readWg_.wait();
		terminate_ = false;
		isRunning_ = false;
		handleFatalErrorImpl(err);
	}
}

Error CoroClientConnection::Status(bool forceCheck, milliseconds netTimeout, milliseconds execTimeout, const IRdxCancelContext* ctx) {
	if (!RequiresStatusCheck() && !forceCheck) {
		return errOK;
	}
	return call({kCmdPing, netTimeout, execTimeout, lsn_t(), -1, ShardingKeyType::NotSetShard, ctx, false}, {}).Status();
}

CoroRPCAnswer CoroClientConnection::call(const CommandParams& opts, const Args& args) {
	if (opts.cancelCtx) {
		switch (opts.cancelCtx->GetCancelType()) {
			case CancelType::Explicit:
				return Error(errCanceled, "Canceled by context");
			case CancelType::Timeout:
				return Error(errTimeout, "Canceled by timeout");
			case CancelType::None:
				break;
		}
	}
	if (terminate_ || !isRunning_) {
		return Error(errLogic, "Client is not running");
	}

	auto deadline = opts.netTimeout.count() ? (Now() + opts.netTimeout + kDeadlineCheckInterval) : TimePointT();
	auto seqp = seqNums_.pop();
	if (!seqp.second) {
		return Error(errLogic, "Unable to get seq num");
	}

	// Don't allow to add new requests, while error handling is in progress
	std::ignore = errSyncCh_.pop();

	const uint32_t seq = seqp.first;
	auto& call = rpcCalls_[seq % rpcCalls_.size()];
	call.seq = seq;
	call.used = true;
	call.deadline = deadline;
	call.cancelCtx = opts.cancelCtx;
	call.system = (opts.cmd == kCmdPing || opts.cmd == kCmdLogin);
	CoroRPCAnswer ans;
	try {
		wrCh_.push(packRPC(
			opts.cmd, seq, false, args,
			Args{Arg{int64_t(opts.execTimeout.count())}, Arg{int64_t(opts.lsn)}, Arg{int64_t(opts.serverId)},
				 Arg{opts.shardingParallelExecution ? int64_t{opts.shardId} | kShardingParallelExecutionBit : int64_t{opts.shardId}}},
			opts.requiredLoginTs));
		auto ansp = call.rspCh.pop();
		if (ansp.second) {
			ans = std::move(ansp.first);
		} else {
			ans = CoroRPCAnswer(Error(errLogic, "Response channel is closed"));
		}
	} catch (...) {
		ans = CoroRPCAnswer(Error(errNetwork, "Writing channel is closed"));
	}

	call.used = false;
	seqNums_.push(seq + seqNums_.capacity());
	return ans;
}

Error CoroClientConnection::callNoReply(const CommandParams& opts, uint32_t seq, const Args& args) {
	if (opts.cancelCtx) {
		switch (opts.cancelCtx->GetCancelType()) {
			case CancelType::Explicit:
				return Error(errCanceled, "Canceled by context");
			case CancelType::Timeout:
				return Error(errTimeout, "Canceled by timeout");
			case CancelType::None:
				break;
		}
	}
	if (terminate_ || !isRunning_) {
		return Error(errLogic, "Client is not running");
	}

	// Don't allow to add new requests, while error handling is in progress
	std::ignore = errSyncCh_.pop();

	try {
		wrCh_.push(packRPC(opts.cmd, seq, true, args, Args{Arg{int64_t(opts.execTimeout.count())}}, LoginTs()));
	} catch (...) {
		return Error(errNetwork, "Writing channel is closed");
	}
	return errOK;
}

CoroClientConnection::MarkedChunk CoroClientConnection::packRPC(CmdCode cmd, uint32_t seq, bool noReply, const Args& args,
																const Args& ctxArgs, std::optional<TimePointT> requiredLoginTs) {
	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.compressed = enableCompression_;
	hdr.dedicatedThread = requestDedicatedThread_;
	hdr.cmd = cmd;
	hdr.seq = seq;

	chunk ch = getChunk();
	WrSerializer ser(std::move(ch));

	ser.Write(std::string_view(reinterpret_cast<char*>(&hdr), sizeof(hdr)));
	args.Pack(ser);
	ctxArgs.Pack(ser);
	if (hdr.compressed) {
		auto data = ser.Slice().substr(sizeof(hdr));
		snappy::Compress(data.data(), data.length(), &compressedBuffer_);
		ser.Reset(sizeof(hdr));
		ser.Write(compressedBuffer_);
	}
	assertrx(ser.Len() < size_t(std::numeric_limits<int32_t>::max()));
	reinterpret_cast<CProtoHeader*>(ser.Buf())->len = ser.Len() - sizeof(hdr);

	return {seq, noReply, requiredLoginTs, ser.DetachChunk()};
}

void CoroClientConnection::appendChunck(std::vector<char>& buf, chunk&& ch) {
	auto oldBufSize = buf.size();
	buf.resize(buf.size() + ch.size());
	memcpy(buf.data() + oldBufSize, ch.data(), ch.size());
	recycleChunk(std::move(ch));
}

Error CoroClientConnection::login(std::vector<char>& buf) {
	assertrx(conn_.state() != manual_connection::conn_state::connecting);
	if (conn_.state() == manual_connection::conn_state::init) {
		assertrx(buf.size() == 0);
		readWg_.wait();
		int ret = 0;
		std::string dbName;
		if (auto err = conn_.with_tls(connectData_.uri.scheme() == "cprotos"); !err.ok()) {
			return err;
		}
		if (connectData_.uri.scheme() == "cproto" || connectData_.uri.scheme() == "cprotos") {
			dbName = connectData_.uri.path();
			std::string port = connectData_.uri.port().length() ? connectData_.uri.port() : std::string("6534");
			ret = conn_.async_connect(connectData_.uri.hostname() + ":" + port, socket_domain::tcp);
		} else {
			std::vector<std::string_view> pathParts;
			std::ignore = split(std::string_view(connectData_.uri.path()), ":", true, pathParts);
			if (pathParts.size() >= 2) {
				dbName = pathParts.back();
				ret = conn_.async_connect(connectData_.uri.path().substr(0, connectData_.uri.path().size() - dbName.size() - 1),
										  socket_domain::unx);
			} else {
				ret = conn_.async_connect(connectData_.uri.path(), socket_domain::unx);
			}
		}
		if (ret < 0) {
			// unable to connect
			return Error(errNetwork, "Connect error: {}", strerror(conn_.socket_last_error()));
		}

		std::string userName = connectData_.uri.username();
		std::string password = connectData_.uri.password();
		if (dbName[0] == '/') {
			dbName = dbName.substr(1);
		}
#ifdef WIN32
		enableCompression_ = false;	 // Disable compression on Windows #1823
#else
		enableCompression_ = connectData_.opts.enableCompression;
#endif

		requestDedicatedThread_ = connectData_.opts.requestDedicatedThread;
		Args args = {Arg{p_string(&userName)},
					 Arg{p_string(&password)},
					 Arg{p_string(&dbName)},
					 Arg{connectData_.opts.createDB},
					 Arg{connectData_.opts.hasExpectedClusterID},
					 Arg{connectData_.opts.expectedClusterID},
					 Arg{p_string(REINDEX_VERSION)},
					 Arg{p_string(&connectData_.opts.appName)},
					 Arg{kClientCaps.caps},
					 Arg{connectData_.opts.replToken}};
		constexpr uint32_t seq = 0;	 // login's seq num is always 0
		assertrx(buf.size() == 0);
		appendChunck(
			buf, packRPC(kCmdLogin, seq, false, args, Args{Arg{int64_t(0)}, Arg{int64_t(lsn_t())}, Arg{int64_t(-1)}}, std::nullopt).data);
		int err = 0;
		auto written = conn_.async_write(buf, err);
		auto toWrite = buf.size();
		buf.clear();
		if (err != 0) {
			// TODO: handle reconnects
			if (err > 0) {
				return Error(errNetwork, "Connection error: {}", strerror(err));
			} else if (err == k_connect_ssl_err) {
				return Error(errConnectSSL, "SSL handshake/connection error");
			} else {
				return Error(errNetwork, "Unable to write login cmd: connection closed");
			}
		}
		assertrx(written == toWrite);
		(void)written;
		(void)toWrite;

		loop_->spawn(readWg_, [this] { readerRoutine(); });
	}
	return errOK;
}

void CoroClientConnection::handleFatalErrorFromReader(const Error& err) noexcept {
	if (errSyncCh_.opened() || terminate_) {
		return;
	} else {
		errSyncCh_.reopen();
	}
	conn_.close_conn(k_sock_closed_err);
	handleFatalErrorImpl(err);
}

// NOLINTNEXTLINE(bugprone-exception-escape) No good ways to recover
void CoroClientConnection::handleFatalErrorImpl(const Error& err) noexcept {
	setLoggedIn(false);
	for (auto& c : rpcCalls_) {
		if (c.used && c.rspCh.opened() && !c.rspCh.full()) {
			c.rspCh.push(err);
		}
	}
	if (connectionStateHandler_) {
		connectionStateHandler_(err);
	}
	rxVersion_ = std::nullopt;
	errSyncCh_.close();
}

// NOLINTNEXTLINE(bugprone-exception-escape) No good ways to recover
void CoroClientConnection::handleFatalErrorFromWriter(const Error& err) noexcept {
	if (!terminate_) {
		if (errSyncCh_.opened()) {
			std::ignore = errSyncCh_.pop();
			return;
		} else {
			errSyncCh_.reopen();
		}
		conn_.close_conn(k_sock_closed_err);
		readWg_.wait();
		handleFatalErrorImpl(err);
	}
}

chunk CoroClientConnection::getChunk() noexcept {
	chunk ch;
	if (recycledChuncks_.size()) {
		ch = std::move(recycledChuncks_.back());
		ch.clear();
		recycledChuncks_.pop_back();
	}
	return ch;
}

void CoroClientConnection::recycleChunk(chunk&& ch) noexcept {
	if (ch.capacity() <= kMaxChunckSizeToRecycle && recycledChuncks_.size() < kMaxRecycledChuncks) {
		recycledChuncks_.emplace_back(std::move(ch));
	}
}

void CoroClientConnection::writerRoutine() {
	std::vector<char> buf;
	buf.reserve(0x800);

	while (!terminate_) {
		size_t cnt = 0;
		do {
			auto mch = wrCh_.pop();
			if (!mch.second) {
				// channel is closed
				return;
			}
			if (mch.first.requiredLoginTs.has_value() && mch.first.requiredLoginTs != LoginTs()) {
				recycleChunk(std::move(mch.first.data));
				auto& c = rpcCalls_[mch.first.seq % rpcCalls_.size()];
				if (c.used && c.rspCh.opened() && !c.rspCh.full()) {
					c.rspCh.push(Error(
						errNetwork, "Connection was broken and all associated snapshots, queryresults and transaction were invalidated"));
				}
				continue;
			}
			if (mch.first.noReply && !loggedIn_) {
				// Skip noreply requests (cmdClose) for disconnected client
				recycleChunk(std::move(mch.first.data));
				continue;
			}
			auto status = login(buf);
			if (!status.ok()) {
				recycleChunk(std::move(mch.first.data));
				handleFatalErrorFromWriter(status);
				continue;
			}
			const auto& rpcData = rpcCalls_[mch.first.seq % rpcCalls_.size()];
			if ((rpcData.used && rpcData.seq == mch.first.seq) || mch.first.noReply) {
				appendChunck(buf, std::move(mch.first.data));
				++cnt;
			} else {
				// Skip handled requests (those request probably were dropped with errors, but still exist in buffered channel)
				recycleChunk(std::move(mch.first.data));
			}
		} while (wrCh_.size() && buf.size() < kMaxSerializedSize);
		int err = 0;
		const bool sendNow = cnt == kCntToSendNow || buf.size() >= kDataToSendNow;
		auto written = conn_.async_write(buf, err, sendNow);
		if (err) {
			// disconnected
			handleFatalErrorFromWriter(Error(errNetwork, "Write error: {}", err > 0 ? strerror(err) : "Connection closed"));
			buf.clear();
			continue;
		}
		assertrx(written == buf.size());
		(void)written;
		buf.clear();
	}
}

void CoroClientConnection::readerRoutine() {
	CProtoHeader hdr;
	std::vector<char> buf;
	buf.reserve(kReadBufReserveSize);
	std::string uncompressed;
	do {
		buf.resize(sizeof(CProtoHeader));
		int err = 0;
		auto read = conn_.async_read(buf, sizeof(CProtoHeader), err);
		if (err) {
			// disconnected
			handleFatalErrorFromReader(err > 0 ? Error(errNetwork, "Read error: {}", strerror(err))
											   : Error(errNetwork, "Connection closed"));
			break;
		}
		assertrx(read == sizeof(hdr));
		(void)read;
		memcpy(&hdr, buf.data(), sizeof(hdr));

		if (hdr.magic != kCprotoMagic) {
			// disconnect
			handleFatalErrorFromReader(Error(errNetwork, "Invalid cproto magic={:08x}", hdr.magic));
			break;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			// disconnect
			handleFatalErrorFromReader(
				Error(errParams, "Unsupported cproto version {:04x}. This client expects reindexer server v1.9.8+", int(hdr.version)));
			break;
		}
		if (hdr.version < kCprotoMinSnappyVersion) {
			enableCompression_ = false;
		}

		buf.resize(hdr.len);
		read = conn_.async_read(buf, size_t(hdr.len), err);
		if (err) {
			// disconnected
			handleFatalErrorFromReader(err > 0 ? Error(errNetwork, "Read error: {}", strerror(err))
											   : Error(errNetwork, "Connection closed"));
			break;
		}
		assertrx(read == hdr.len);
		(void)read;

		CoroRPCAnswer ans;
		try {
			Serializer ser(buf.data(), hdr.len);
			if (hdr.compressed) {
				uncompressed.reserve(kReadBufReserveSize);
				if (!snappy::Uncompress(buf.data(), hdr.len, &uncompressed)) {
					throw Error(errParseBin, "Can't decompress data from peer");
				}
				ser = Serializer(uncompressed);
			}

			const int errCode = ser.GetVarUInt();
			std::string_view errMsg = ser.GetVString();
			if (errCode != errOK) {
				ans.status_ = Error(static_cast<ErrorCode>(errCode), errMsg);
			}
			ans.data_ = std::span<const uint8_t>(ser.Buf() + ser.Pos(), ser.Len() - ser.Pos());
		} catch (const Error& err) {
			// disconnect
			handleFatalErrorFromReader(err);
			break;
		}

		if (hdr.cmd == kCmdLogin) {
			if (ans.Status().ok()) {
				if (!rxVersion_) {
					rxVersion_ = ans.GetArgs(2)[0].As<std::string>();
				}
				setLoggedIn(true);
				if (connectionStateHandler_) {
					connectionStateHandler_(Error());
				}
			} else {
				// disconnect
				handleFatalErrorFromReader(ans.Status());
			}
		} else if (hdr.cmd != kCmdUpdates) {
			auto& rpcData = rpcCalls_[hdr.seq % rpcCalls_.size()];
			if (!rpcData.used || rpcData.seq != hdr.seq) {
				const auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "reindexer error: unexpected RPC answer seq=%d cmd=%u(%.*s)\n", int(hdr.seq), hdr.cmd, int(cmdSv.size()),
						cmdSv.data());
				sendCloseResults(hdr, ans);
				continue;
			}
			assertrx(rpcData.rspCh.opened());
			if (!rpcData.rspCh.readers()) {
				// In this case read buffer will be invalidated, before coroutine switch
				ans.EnsureHold(getChunk());
			}
			rpcData.rspCh.push(std::move(ans));
		} else {
			fprintf(stderr, "reindexer error: unexpected updates response");
		}
	} while (loggedIn_ && !terminate_);
}

void CoroClientConnection::sendCloseResults(const CProtoHeader& hdr, const CoroRPCAnswer& ans) {
	if (!ans.Status().ok()) {
		return;
	}
	switch (hdr.cmd) {
		case kCmdCommitTx:
		case kCmdModifyItem:
		case kCmdDeleteQuery:
		case kCmdUpdateQuery:
		case kCmdSelect:
		case kCmdExecSQL:
		case kCmdFetchResults: {
			Serializer ser{ans.data_.data(), ans.data_.size()};
			Args args;
			args.Unpack(ser);
			if (args.size() > 1) {
				Error err;
				if (args.size() > 2) {
					err = callNoReply({kCmdCloseResults, connectData_.opts.keepAliveTimeout, milliseconds(0), lsn_t(), -1,
									   ShardingKeyType::NotSetShard, nullptr, false},
									  hdr.seq, {Arg{args[1].As<int>()}, Arg{args[2].As<int64_t>()}, Arg{true}});
				} else {
					err = callNoReply({kCmdCloseResults, connectData_.opts.keepAliveTimeout, milliseconds(0), lsn_t(), -1,
									   ShardingKeyType::NotSetShard, nullptr, false},
									  hdr.seq, {Arg{args[1].As<int>()}, Arg{reindexer_server::RPCQrWatcher::kDisabled}, Arg{true}});
				}
				if (!err.ok()) {
					fprintf(stderr, "reindexer error: unable to send 'CloseResults' command: %s\n", err.what());
				}
			} else {
#ifdef RX_WITH_STDLIB_DEBUG
				// Usually this is timeout handling. We should not print message into stderr in regular build
				auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "reindexer error: unexpected RPC answer seq=%d cmd=%u(%.*s); do not have reqId\n", int(hdr.seq), hdr.cmd,
						int(cmdSv.size()), cmdSv.data());
#endif	// RX_WITH_STDLIB_DEBUG
			}
		} break;
		default:
			break;
	}
}

void CoroClientConnection::deadlineRoutine() {
	while (!terminate_) {
		loop_->granular_sleep(kDeadlineCheckInterval, kCoroSleepGranularity, [this] { return terminate_; });
		now_ += kDeadlineCheckInterval;

		for (auto& c : rpcCalls_) {
			if (!c.used) {
				continue;
			}
			bool expired = (c.deadline.time_since_epoch().count() && c.deadline <= now_);
			bool canceled = (c.cancelCtx && c.cancelCtx->IsCancelable() && (c.cancelCtx->GetCancelType() == CancelType::Explicit));
			if (expired || canceled) {
				if (c.rspCh.opened() && !c.rspCh.full()) {
					c.rspCh.push(Error(expired ? errTimeout : errCanceled, expired ? "Request deadline exceeded" : "Canceled"));
				}
			}
		}
	}
}

void CoroClientConnection::pingerRoutine() {
	const std::chrono::milliseconds timeout =
		connectData_.opts.keepAliveTimeout.count() > 0 ? connectData_.opts.keepAliveTimeout : kKeepAliveInterval;
	while (!terminate_) {
		loop_->granular_sleep(kKeepAliveInterval, kCoroSleepGranularity, [this] { return terminate_; });
		if (loggedIn_) {
			std::ignore = call({kCmdPing, timeout, milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard, nullptr, false}, {});
		}
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
