#include "coroclientconnection.h"
#include <errno.h>
#include <snappy.h>
#include "core/rdxcontext.h"
#include "coroclientconnection.h"
#include "reindexer_version.h"
#include "server/rpcqrwatcher.h"
#include "tools/serializer.h"

#include <functional>

namespace reindexer {
namespace net {
namespace cproto {

constexpr size_t kMaxRecycledChuncks = 1500;
constexpr size_t kMaxChunckSizeToRecycle = 2048;
constexpr size_t kMaxParallelRPCCalls = 512;
constexpr auto kCoroSleepGranularity = std::chrono::milliseconds(150);
constexpr auto kDeadlineCheckInterval = std::chrono::seconds(1);
constexpr auto kKeepAliveInterval = std::chrono::seconds(30);
constexpr size_t kUpdatesChannelSize = 128;
constexpr size_t kReadBufReserveSize = 0x1000;
constexpr size_t kWrChannelSize = 30;
constexpr size_t kCntToSendNow = 30;
constexpr size_t kMaxSerializedSize = 20 * 1024 * 1024;
constexpr size_t kDataToSendNow = 8192;

CoroClientConnection::CoroClientConnection()
	: now_(0),
	  rpcCalls_(kMaxParallelRPCCalls),
	  wrCh_(kWrChannelSize),
	  seqNums_(kMaxParallelRPCCalls),
	  updatesCh_(kUpdatesChannelSize),
	  conn_(-1, kReadBufReserveSize, false) {
	recycledChuncks_.reserve(kMaxRecycledChuncks);
	errSyncCh_.close();
	seqNums_.close();
}

CoroClientConnection::~CoroClientConnection() { Stop(); }

void CoroClientConnection::Start(ev::dynamic_loop &loop, ConnectData &&connectData) {
	if (!isRunning_) {
		// Don't allow to call Start, while error handling is in progress
		errSyncCh_.pop();

		if (loop_ != &loop) {
			if (loop_) {
				conn_.detach();
			}
			conn_.attach(loop);
			loop_ = &loop;
		}

		if (!seqNums_.opened()) {
			wg_.add(1);
			seqNums_.reopen();
			loop_->spawn([this] {
				coroutine::wait_group_guard wgg(wg_);
				for (size_t i = 1; i < seqNums_.capacity(); ++i) {
					// seq num == 0 is reserved for login
					seqNums_.push(i);
				}
			});
		}

		connectData_ = std::move(connectData);
		if (!updatesCh_.opened()) {
			updatesCh_.reopen();
		}
		if (!wrCh_.opened()) {
			wrCh_.reopen();
		}

		wg_.add(4);
		loop_->spawn([this] {
			coroutine::wait_group_guard wgg(wg_);
			writerRoutine();
		});
		loop_->spawn([this] {
			coroutine::wait_group_guard wgg(wg_);
			deadlineRoutine();
		});
		loop_->spawn([this] {
			coroutine::wait_group_guard wgg(wg_);
			pingerRoutine();
		});
		loop_->spawn([this] {
			coroutine::wait_group_guard wgg(wg_);
			updatesRoutine();
		});

		isRunning_ = true;
	}
}

void CoroClientConnection::Stop() {
	if (isRunning_) {
		terminate_ = true;
		updatesCh_.close();
		wrCh_.close();
		conn_.close_conn(k_sock_closed_err);
		wg_.wait();
		readWg_.wait();
		terminate_ = false;
		isRunning_ = false;
		handleFatalError(Error(errNetwork, "Connection closed"));
	}
}

Error CoroClientConnection::Status(std::chrono::seconds netTimeout, std::chrono::milliseconds execTimeout, const IRdxCancelContext *ctx) {
	if (loggedIn_) {
		return errOK;
	}
	return call({kCmdPing, netTimeout, execTimeout, ctx}, {}).Status();
}

CoroRPCAnswer CoroClientConnection::call(const CommandParams &opts, const Args &args) {
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

	auto deadline = opts.netTimeout.count() ? Now() + opts.netTimeout + kDeadlineCheckInterval : seconds(0);
	auto seqp = seqNums_.pop();
	if (!seqp.second) {
		return Error(errLogic, "Unable to get seq num");
	}

	// Don't allow to add new requests, while error handling is in progress
	errSyncCh_.pop();

	uint32_t seq = seqp.first;
	auto &call = rpcCalls_[seq % rpcCalls_.size()];
	call.seq = seq;
	call.used = true;
	call.deadline = deadline;
	call.cancelCtx = opts.cancelCtx;
	CoroRPCAnswer ans;
	try {
		wrCh_.push(packRPC(opts.cmd, seq, args, Args{Arg{int64_t(opts.execTimeout.count())}}));
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

Error CoroClientConnection::callNoReply(const CommandParams &opts, uint32_t seq, const Args &args) {
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
	errSyncCh_.pop();

	try {
		wrCh_.push(packRPC(opts.cmd, seq, args, Args{Arg{int64_t(opts.execTimeout.count())}}));
	} catch (...) {
		return Error(errNetwork, "Writing channel is closed");
	}
	return errOK;
}

CoroClientConnection::MarkedChunk CoroClientConnection::packRPC(CmdCode cmd, uint32_t seq, const Args &args, const Args &ctxArgs) {
	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.compressed = enableSnappy_;
	hdr.dedicatedThread = requestDedicatedThread_;
	hdr.cmd = cmd;
	hdr.seq = seq;

	chunk ch = getChunk();
	WrSerializer ser(std::move(ch));

	ser.Write(std::string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));
	args.Pack(ser);
	ctxArgs.Pack(ser);
	if (hdr.compressed) {
		auto data = ser.Slice().substr(sizeof(hdr));
		std::string compressed;
		snappy::Compress(data.data(), data.length(), &compressed);
		ser.Reset(sizeof(hdr));
		ser.Write(compressed);
	}
	assertrx(ser.Len() < size_t(std::numeric_limits<int32_t>::max()));
	reinterpret_cast<CProtoHeader *>(ser.Buf())->len = ser.Len() - sizeof(hdr);

	return {seq, ser.DetachChunk()};
}

void CoroClientConnection::appendChunck(std::vector<char> &buf, chunk &&ch) {
	auto oldBufSize = buf.size();
	buf.resize(buf.size() + ch.size());
	memcpy(buf.data() + oldBufSize, ch.data(), ch.size());
	recycleChunk(std::move(ch));
}

Error CoroClientConnection::login(std::vector<char> &buf) {
	assertrx(conn_.state() != manual_connection::conn_state::connecting);
	if (conn_.state() == manual_connection::conn_state::init) {
		readWg_.wait();
		lastError_ = errOK;
		std::string port = connectData_.uri.port().length() ? connectData_.uri.port() : std::string("6534");
		int ret = conn_.async_connect(connectData_.uri.hostname() + ":" + port);
		if (ret < 0) {
			// unable to connect
			return Error(errNetwork, "Connect error");
		}

		std::string dbName = connectData_.uri.path();
		std::string userName = connectData_.uri.username();
		std::string password = connectData_.uri.password();
		if (dbName[0] == '/') dbName = dbName.substr(1);
		enableCompression_ = connectData_.opts.enableCompression;
		requestDedicatedThread_ = connectData_.opts.requestDedicatedThread;
		Args args = {Arg{p_string(&userName)},
					 Arg{p_string(&password)},
					 Arg{p_string(&dbName)},
					 Arg{connectData_.opts.createDB},
					 Arg{connectData_.opts.hasExpectedClusterID},
					 Arg{connectData_.opts.expectedClusterID},
					 Arg{p_string(REINDEX_VERSION)},
					 Arg{p_string(&connectData_.opts.appName)}};
		constexpr uint32_t seq = 0;	 // login's seq num is always 0
		assertrx(buf.size() == 0);
		appendChunck(buf, packRPC(kCmdLogin, seq, args, Args{Arg{int64_t(0)}}).data);
		int err = 0;
		auto written = conn_.async_write(buf, err);
		auto toWrite = buf.size();
		buf.clear();
		if (err) {
			// TODO: handle reconnects
			return err > 0 ? Error(errNetwork, "Connection error: %s", strerror(err))
						   : (lastError_.ok() ? Error(errNetwork, "Unable to write login cmd: connection closed") : lastError_);
		}
		assertrx(written == toWrite);
		(void)written;
		(void)toWrite;

		readWg_.add(1);
		loop_->spawn([this] {
			coroutine::wait_group_guard readWgg(readWg_);
			readerRoutine();
		});
	}
	return errOK;
}

void CoroClientConnection::closeConn(const Error &err) noexcept {
	errSyncCh_.reopen();
	lastError_ = err;
	conn_.close_conn(k_sock_closed_err);
	handleFatalError(err);
}

void CoroClientConnection::handleFatalError(const Error &err) noexcept {
	if (!errSyncCh_.opened()) {
		errSyncCh_.reopen();
	}
	loggedIn_ = false;
	for (auto &c : rpcCalls_) {
		if (c.used && c.rspCh.opened() && !c.rspCh.full()) {
			c.rspCh.push(err);
		}
	}
	if (fatalErrorHandler_) {
		fatalErrorHandler_(err);
	}
	errSyncCh_.close();
}

chunk CoroClientConnection::getChunk() noexcept {
	chunk ch;
	if (recycledChuncks_.size()) {
		ch = std::move(recycledChuncks_.back());
		ch.len_ = 0;
		ch.offset_ = 0;
		recycledChuncks_.pop_back();
	}
	return ch;
}

void CoroClientConnection::recycleChunk(chunk &&ch) noexcept {
	if (ch.cap_ <= kMaxChunckSizeToRecycle && recycledChuncks_.size() < kMaxRecycledChuncks) {
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
				// channels is closed
				return;
			}
			auto status = login(buf);
			if (!status.ok()) {
				recycleChunk(std::move(mch.first.data));
				handleFatalError(status);
				continue;
			}
			appendChunck(buf, std::move(mch.first.data));
			++cnt;
		} while (wrCh_.size() && buf.size() < kMaxSerializedSize);
		int err = 0;
		bool sendNow = cnt == kCntToSendNow || buf.size() >= kDataToSendNow;
		auto written = conn_.async_write(buf, err, sendNow);
		if (err) {
			// disconnected
			buf.clear();
			if (lastError_.ok()) {
				handleFatalError(Error(errNetwork, "Write error: %s", err > 0 ? strerror(err) : "Connection closed"));
			}
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
			if (lastError_.ok()) {
				handleFatalError(err > 0 ? Error(errNetwork, "Read error: %s", strerror(err)) : Error(errNetwork, "Connection closed"));
			}
			break;
		}
		assertrx(read == sizeof(hdr));
		(void)read;
		memcpy(&hdr, buf.data(), sizeof(hdr));

		if (hdr.magic != kCprotoMagic) {
			// disconnect
			closeConn(Error(errNetwork, "Invalid cproto magic=%08x", hdr.magic));
			break;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			// disconnect
			closeConn(Error(errParams, "Unsupported cproto version %04x. This client expects reindexer server v1.9.8+", int(hdr.version)));
			break;
		}

		buf.resize(hdr.len);
		read = conn_.async_read(buf, size_t(hdr.len), err);
		if (err) {
			// disconnected
			if (lastError_.ok()) {
				handleFatalError(err > 0 ? Error(errNetwork, "Read error: %s", strerror(err)) : Error(errNetwork, "Connection closed"));
			}
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

			const int errCode = ser.GetVarUint();
			std::string_view errMsg = ser.GetVString();
			if (errCode != errOK) {
				ans.status_ = Error(static_cast<ErrorCode>(errCode), std::string{errMsg});
			}
			ans.data_ = span<uint8_t>(ser.Buf() + ser.Pos(), ser.Len() - ser.Pos());
		} catch (const Error &err) {
			// disconnect
			closeConn(err);
			break;
		}

		if (hdr.cmd == kCmdUpdates) {
			if (updatesHandler_) {
				ans.EnsureHold(getChunk());
				updatesCh_.push(std::move(ans));
			}
		} else if (hdr.cmd == kCmdLogin) {
			if (ans.Status().ok()) {
				loggedIn_ = true;
			} else {
				// disconnect
				closeConn(ans.Status());
			}
		} else {
			auto &rpcData = rpcCalls_[hdr.seq % rpcCalls_.size()];
			if (!rpcData.used || rpcData.seq != hdr.seq) {
				auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "Unexpected RPC answer seq=%d cmd=%d(%.*s)\n", int(hdr.seq), hdr.cmd, int(cmdSv.size()), cmdSv.data());
				sendCloseResults(hdr, ans);
				continue;
			}
			assertrx(rpcData.rspCh.opened());
			if (!rpcData.rspCh.readers()) {
				// In this case read buffer will be invalidated, before coroutine switch
				ans.EnsureHold(getChunk());
			}
			rpcData.rspCh.push(std::move(ans));
		}
	} while (loggedIn_ && !terminate_);
}

void CoroClientConnection::sendCloseResults(CProtoHeader const &hdr, CoroRPCAnswer const &ans) {
	if (!ans.Status().ok()) {
		return;
	}
	switch (hdr.cmd) {
		case kCmdCommitTx:
		case kCmdModifyItem:
		case kCmdDeleteQuery:
		case kCmdUpdateQuery:
		case kCmdSelect:
		case kCmdSelectSQL:
		case kCmdFetchResults: {
			Serializer ser{ans.data_.data(), ans.data_.size()};
			Args args;
			args.Unpack(ser);
			if (args.size() > 1) {
				if (args.size() > 2) {
					callNoReply({kCmdCloseResults, connectData_.opts.keepAliveTimeout, milliseconds(0), nullptr}, hdr.seq,
								{Arg{args[1].As<int>()}, Arg{args[2].As<int64_t>()}, Arg{true}});
				} else {
					callNoReply({kCmdCloseResults, connectData_.opts.keepAliveTimeout, milliseconds(0), nullptr}, hdr.seq,
								{Arg{args[1].As<int>()}, Arg{reindexer_server::RPCQrWatcher::kDisabled}, Arg{true}});
				}
			} else {
				auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "Unexpected RPC answer seq=%d cmd=%d(%.*s); do not have reqId\n", int(hdr.seq), hdr.cmd, int(cmdSv.size()),
						cmdSv.data());
			}
		} break;
		default:
			break;
	}
}

void CoroClientConnection::deadlineRoutine() {
	while (!terminate_) {
		static_assert(std::chrono::duration_cast<std::chrono::seconds>(kDeadlineCheckInterval).count() >= 1,
					  "kDeadlineCheckInterval shoul be 1 second or more");
		loop_->granular_sleep(kDeadlineCheckInterval, kCoroSleepGranularity, [this] { return terminate_; });
		now_ += std::chrono::duration_cast<std::chrono::seconds>(kDeadlineCheckInterval).count();

		for (auto &c : rpcCalls_) {
			if (!c.used) continue;
			bool expired = (c.deadline.count() && c.deadline.count() <= now_);
			bool canceled = (c.cancelCtx && c.cancelCtx->IsCancelable() && (c.cancelCtx->GetCancelType() == CancelType::Explicit));
			if (expired || canceled) {
				if (c.rspCh.opened()) {
					c.rspCh.push(Error(expired ? errTimeout : errCanceled, expired ? "Request deadline exceeded" : "Canceled"));
				}
			}
		}
	}
}

void CoroClientConnection::pingerRoutine() {
	while (!terminate_) {
		loop_->granular_sleep(kKeepAliveInterval, kCoroSleepGranularity, [this] { return terminate_; });
		if (conn_.state() != manual_connection::conn_state::init) {
			call({kCmdPing, connectData_.opts.keepAliveTimeout, milliseconds(0), nullptr}, {});
		}
	}
}

void CoroClientConnection::updatesRoutine() {
	while (!terminate_) {
		auto ansp = updatesCh_.pop();
		if (!ansp.second) {
			// Channel is closed
			break;
		}
		auto handler = updatesHandler_;
		auto storage = std::move(ansp.first.storage_);
		if (handler) {
			handler(ansp.first);
		}
		recycleChunk(std::move(storage));
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
