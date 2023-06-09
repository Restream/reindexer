

#include "clientconnection.h"
#include <errno.h>
#include <snappy.h>
#include "core/rdxcontext.h"
#include "reindexer_version.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const int kMaxCompletions = 512;
const int kKeepAliveInterval = 30;
const int kDeadlineCheckInterval = 1;

bool ClientConnection::ConnectData::ThereAreReconnectOptions() const { return entries.size() > 1; }
bool ClientConnection::ConnectData::CurrDsnFailed(int failedDsnIdx) const { return failedDsnIdx == validEntryIdx; }
int ClientConnection::ConnectData::GetNextDsnIndex() const { return (validEntryIdx.load(std::memory_order_acquire) + 1) % entries.size(); }

ClientConnection::ClientConnection(ev::dynamic_loop &loop, ConnectData *connectData, ConnectionFailCallback connectionFailCallback)
	: ConnectionMT(-1, loop, false),
	  state_(ConnInit),
	  completions_(kMaxCompletions),
	  seq_(0),
	  bufWait_(0),
	  now_(0),
	  terminate_(false),
	  onConnectionFailed_(std::move(connectionFailCallback)),
	  connectData_(connectData),
	  currDsnIdx_(connectData->validEntryIdx.load(std::memory_order_acquire)),
	  actualDsnIdx_(currDsnIdx_) {
	connect_async_.set<ClientConnection, &ClientConnection::connect_async_cb>(this);
	connect_async_.set(loop);
	connect_async_.start();
	keep_alive_.set<ClientConnection, &ClientConnection::keep_alive_cb>(this);
	keep_alive_.set(loop);
	deadlineTimer_.set<ClientConnection, &ClientConnection::deadline_check_cb>(this);
	deadlineTimer_.set(loop);
	reconnect_.set(loop);
	reconnect_.set([this](ev::async &sig) {
		disconnect();
		sig.loop.break_loop();
	});
	reconnect_.start();
	loopThreadID_ = std::this_thread::get_id();
}

ClientConnection::~ClientConnection() { assertrx(!PendingCompletions()); }

void ClientConnection::connectInternal() noexcept {
	mtx_.lock();
	if (state_ == ConnConnecting || state_ == ConnConnected) {
		mtx_.unlock();
		return;
	}
	actualDsnIdx_ = connectData_->validEntryIdx;
	assertrx(!sock_.valid());
	assertrx(wrBuf_.size() == 0);
	rdBuf_.clear();
	enableSnappy_ = false;
	state_ = ConnConnecting;
	lastError_ = errOK;

	mtx_.unlock();

	assertrx(connectData_->validEntryIdx < int(connectData_->entries.size()));
	ConnectData::Entry &connectEntry = connectData_->entries[actualDsnIdx_];
	std::string port = connectEntry.uri.port().length() ? connectEntry.uri.port() : std::string("6534");
	std::string dbName = connectEntry.uri.path();
	std::string userName = connectEntry.uri.username();
	std::string password = connectEntry.uri.password();
	if (dbName[0] == '/') dbName = dbName.substr(1);
	enableCompression_ = connectEntry.opts.enableCompression;

	auto completion = [this](const RPCAnswer &ans, ClientConnection *) {
		std::unique_lock<std::mutex> lck(mtx_);
		lastError_ = ans.Status();
		state_ = ans.Status().ok() ? ConnConnected : ConnFailed;
		wrBuf_.clear();
		connectCond_.notify_all();
		currDsnIdx_ = actualDsnIdx_;
		if (!lastError_.ok()) {
			lck.unlock();
			closeConn();
		}
	};
	sock_.connect((connectEntry.uri.hostname() + ":" + port));
	if (!sock_.valid()) {
		completion(RPCAnswer(Error(errNetwork, "Socket connect error: %d", sock_.last_error())), this);
	} else {
		io_.start(sock_.fd(), ev::WRITE);
		curEvents_ = ev::WRITE;
		async_.start();
		keep_alive_.start(kKeepAliveInterval, kKeepAliveInterval);
		deadlineTimer_.start(kDeadlineCheckInterval, kDeadlineCheckInterval);

		call(completion, {kCmdLogin, connectEntry.opts.loginTimeout, milliseconds(0), nullptr},
			 {Arg{p_string(&userName)}, Arg{p_string(&password)}, Arg{p_string(&dbName)}, Arg{connectEntry.opts.createDB},
			  Arg{connectEntry.opts.hasExpectedClusterID}, Arg{connectEntry.opts.expectedClusterID}, Arg{p_string(REINDEX_VERSION)},
			  Arg{p_string(&connectEntry.opts.appName)}});
	}
}

void ClientConnection::failInternal(const Error &error) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (lastError_.ok()) lastError_ = error;
	closeConn_ = true;
}

int ClientConnection::PendingCompletions() {
	int ret = 0;
	for (auto &c : completions_) {
		for (RPCCompletion *cc = &c; cc; cc = cc->next.get()) {
			if (cc->used) {
				ret++;
			}
		}
	}
	return ret;
}

Error ClientConnection::CheckConnection() {
	assertrx(loopThreadID_ != std::this_thread::get_id());
	std::unique_lock<std::mutex> lck(mtx_);
	switch (state_) {
		case ConnConnected:
			return errOK;
		case ConnInit:
			connect_async_.send();
			// fall through
		case ConnConnecting:
			connectCond_.wait(lck);
			if (state_ == ConnFailed) {
				return lastError_;
			}
			return errOK;
		case ConnClosing:
		case ConnFailed:
			return lastError_;
		default:
			std::abort();
	}
	return lastError_;
}

void ClientConnection::keep_alive_cb(ev::periodic &, int) {
	if (terminate_.load(std::memory_order_acquire)) return;
	call(
		[this](RPCAnswer &&ans, ClientConnection *) {
			if (!ans.Status().ok()) {
				failInternal(ans.Status());
				closeConn();
			}
		},
		{kCmdPing, connectData_->entries[connectData_->validEntryIdx].opts.keepAliveTimeout, milliseconds(0), nullptr}, {});
	callback(io_, ev::WRITE);
}

void ClientConnection::deadline_check_cb(ev::timer &, int) {
	now_ += kDeadlineCheckInterval;
	for (auto &c : completions_) {
		for (RPCCompletion *cc = &c; cc; cc = cc->next.get()) {
			if (!cc->used) continue;
			bool expired = (cc->deadline.count() && cc->deadline.count() <= now_);
			if (expired || (cc->cancelCtx && cc->cancelCtx->IsCancelable() && (cc->cancelCtx->GetCancelType() == CancelType::Explicit))) {
				Error err(expired ? errTimeout : errCanceled, expired ? "Request deadline exceeded" : "Canceled");
				cc->cmpl(RPCAnswer(err), this);
				if (state_ == ConnFailed || state_ == ConnClosing) {
					return;
				}
				if (bufWait_) {
					std::unique_lock<std::mutex> lck(mtx_);
					cc->used = false;
					bufCond_.notify_all();
				} else {
					cc->used = false;
				}
				io_.loop.break_loop();
			}
		}
	}
}

void ClientConnection::Reconnect() { reconnect_.send(); }

void ClientConnection::disconnect() {
	assertrx(loopThreadID_ == std::this_thread::get_id());
	std::unique_lock<std::mutex> lck(mtx_);
	State prevState = state_;
	actualDsnIdx_ = connectData_->validEntryIdx.load(std::memory_order_acquire);
	if (state_ != ConnFailed && state_ != ConnClosing) {
		state_ = ConnClosing;
		lck.unlock();
		closeConn();
		if (prevState == ConnConnecting) {
			currDsnIdx_ = actualDsnIdx_;
			connectCond_.notify_all();
		}
	} else {
		state_ = ConnFailed;
		closingCond_.notify_all();
	}
}

void ClientConnection::onClose() {
	bool needToReconnect = false;
	{
		// we need to make sure tmpCompletions is destructed
		// before we switch to ConnFailed state
		std::vector<RPCCompletion> tmpCompletions(kMaxCompletions);

		mtx_.lock();
		wrBuf_.clear();
		if (lastError_.ok())
			lastError_ = Error(errNetwork, "Socket connection to %s closed",
							   connectData_ && actualDsnIdx_ < int(connectData_->entries.size())
								   ? connectData_->entries[actualDsnIdx_].uri.hostname()
								   : "");
		closeConn_ = false;
		State prevState = state_;
		state_ = ConnClosing;
		completions_.swap(tmpCompletions);
		mtx_.unlock();

		keep_alive_.stop();
		deadlineTimer_.stop();

		for (auto &c : tmpCompletions) {
			for (RPCCompletion *cc = &c; cc; cc = cc->next.get())
				if (cc->used && cc->cmd != kCmdLogin && cc->cmd != kCmdPing) {
					cc->cmpl(RPCAnswer(lastError_), this);
				}
		}

		std::unique_ptr<Completion> tmpUpdatesHandler(updatesHandler_.release(std::memory_order_acq_rel));

		if (tmpUpdatesHandler) (*tmpUpdatesHandler)(RPCAnswer(lastError_), this);
		mtx_.lock();
		bufCond_.notify_all();
		mtx_.unlock();

		if (prevState == ConnConnecting) {
			currDsnIdx_ = actualDsnIdx_;
			connectCond_.notify_all();
		} else if (connectData_ && onConnectionFailed_) {
			needToReconnect = onConnectionFailed_(currDsnIdx_);
		}
	}

	if (!needToReconnect) {
		std::unique_lock<std::mutex> lck(mtx_);
		state_ = ConnFailed;
		closingCond_.notify_all();
	}
}

ClientConnection::ReadResT ClientConnection::onRead() {
	CProtoHeader hdr;
	std::string uncompressed;

	while (!closeConn_) {
		auto len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));
		if (len < sizeof(hdr)) return ReadResT::Default;

		if (hdr.magic != kCprotoMagic) {
			failInternal(Error(errNetwork, "Invalid cproto magic=%08x", hdr.magic));
			return ReadResT::Default;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			failInternal(
				Error(errParams, "Unsupported cproto version %04x. This client expects reindexer server v1.9.8+", int(hdr.version)));
			return ReadResT::Default;
		}
		enableSnappy_ = (hdr.version >= kCprotoMinSnappyVersion) && enableCompression_;

		if (size_t(hdr.len) + sizeof(hdr) > rdBuf_.capacity()) {
			rdBuf_.reserve(size_t(hdr.len) + sizeof(hdr) + 0x1000);
		}

		if ((rdBuf_.size() - sizeof(hdr)) < size_t(hdr.len)) return ReadResT::Default;

		rdBuf_.erase(sizeof(hdr));

		auto it = rdBuf_.tail();
		if (it.size() < size_t(hdr.len)) {
			rdBuf_.unroll();
			it = rdBuf_.tail();
		}
		assertrx(it.size() >= size_t(hdr.len));

		RPCAnswer ans;

		try {
			Serializer ser(it.data(), hdr.len);
			if (hdr.compressed) {
				if (!snappy::Uncompress(it.data(), hdr.len, &uncompressed)) {
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
			failInternal(err);
			return ReadResT::Default;
		}
		rdBuf_.erase(hdr.len);
		if (hdr.cmd == kCmdUpdates) {
			auto handler = updatesHandler_.release(std::memory_order_acq_rel);
			if (handler) {
				(*handler)(std::move(ans), this);
				Completion *expected = nullptr;
				if (!updatesHandler_.compare_exchange_strong(expected, handler, std::memory_order_acq_rel)) {
					delete handler;
				}
			}
		} else {
			auto complPtr = completions_.data();
			RPCCompletion *completion = &completions_[hdr.seq % completions_.size()];

			for (; completion; completion = completion->next.get()) {
				if (!completion->used || completion->seq != hdr.seq) {
					continue;
				}
				if (CmdCode(hdr.cmd) != completion->cmd) {
					ans.status_ =
						Error(errParams, "Invalid cmdCode %d, expected %d for seq = %d", int(completion->cmd), int(hdr.cmd), int(hdr.seq));
				}
				completion->cmpl(std::move(ans), this);
				if (completions_.data() != complPtr) {
					rdBuf_.clear();
					return ReadResT::Default;
				}
				if (bufWait_) {
					std::unique_lock<std::mutex> lck(mtx_);
					completion->used = false;
					bufCond_.notify_all();
				} else {
					completion->used = false;
				}

				if (terminate_.load(std::memory_order_acquire) && !PendingCompletions()) {
					closeConn();
				} else {
					io_.loop.break_loop();
				}
				break;
			}
			if (!completion) {
				auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "Unexpected RPC answer seq=%d cmd=%d(%.*s)\n", int(hdr.seq), hdr.cmd, int(cmdSv.size()), cmdSv.data());
			}
		}
	}
	return ReadResT::Default;
}

Args RPCAnswer::GetArgs(int minArgs) const {
	cproto::Args ret;
	Serializer ser(data_.data(), data_.size());
	ret.Unpack(ser);
	if (int(ret.size()) < minArgs) {
		throw Error(errParams, "Server returned %d args, but expected %d", int(ret.size()), minArgs);
	}

	return ret;
}

Error RPCAnswer::Status() const { return status_; }

chunk ClientConnection::packRPC(CmdCode cmd, uint32_t seq, const Args &args, const Args &ctxArgs) {
	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.compressed = enableSnappy_;
	hdr.dedicatedThread = 0;
	hdr.cmd = cmd;
	hdr.seq = seq;

	WrSerializer ser(wrBuf_.get_chunk());

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

	return ser.DetachChunk();
}

void ClientConnection::call(const Completion &cmpl, const CommandParams &opts, const Args &args) {
	if (opts.cancelCtx) {
		switch (opts.cancelCtx->GetCancelType()) {
			case CancelType::Explicit:
				cmpl(RPCAnswer(Error(errCanceled, "Canceled by context")), this);
				return;
			case CancelType::Timeout:
				cmpl(RPCAnswer(Error(errTimeout, "Canceled by timeout")), this);
				return;
			case CancelType::None:
				break;
		}
	}

	uint32_t seq = seq_++;
	chunk data = packRPC(opts.cmd, seq, args, Args{Arg{int64_t(opts.execTimeout.count())}});
	bool inLoopThread = loopThreadID_ == std::this_thread::get_id();

	std::unique_lock<std::mutex> lck(mtx_);
	auto completion = &completions_[seq % completions_.size()];
	auto deadline = opts.netTimeout.count() ? Now() + opts.netTimeout : seconds(0);
	if (!inLoopThread) {
		while (state_ != ConnConnected || completion->used) {
			switch (state_) {
				case ConnConnected:
					break;
				case ConnInit:
				case ConnFailed:
					connect_async_.send();
					state_ = ConnInit;
					// fall through
				case ConnConnecting:
					while (state_ != ConnConnected && state_ != ConnFailed) {
						if (state_ == ConnClosing) {
							while (state_ != ConnFailed) {
								closingCond_.wait(lck);
							}
							completion = &completions_[seq % completions_.size()];
						} else {
							connectCond_.wait(lck);
							if (deadline.count() && deadline <= Now()) {
								lck.unlock();
								cmpl(RPCAnswer(Error(errTimeout, "Connection deadline exceeded")), this);
								return;
							}
							completion = &completions_[seq % completions_.size()];
						}
						if (state_ == ConnFailed) {
							auto err = lastError_;
							lck.unlock();
							cmpl(RPCAnswer(err), this);
							return;
						}
					}
					break;
				case ConnClosing:
					while (state_ != ConnFailed) {
						closingCond_.wait(lck);
					}
					completion = &completions_[seq % completions_.size()];
					break;
				default:
					std::abort();
			}
			if (completion->used) {
				bufWait_++;
				struct {
					uint32_t seq;
					RPCCompletion *cmpl;
				} arg = {seq, completion};

				bufCond_.wait(lck, [this, &arg]() {
					arg.cmpl = &completions_[arg.seq % completions_.size()];
					return !arg.cmpl->used.load();
				});
				completion = arg.cmpl;
				bufWait_--;
			}
		}
	} else {
		if (state_ == ConnInit || state_ == ConnFailed) {
			lck.unlock();
			connectInternal();
			lck.lock();
			if (state_ == ConnFailed) {
				lck.unlock();
				cmpl(RPCAnswer(lastError_), this);
				return;
			}
		}
		while (completion->used) {
			if (!completion->next) completion->next.reset(new RPCCompletion);
			completion = completion->next.get();
		}
	}

	completion->cmpl = cmpl;
	completion->seq = seq;
	completion->cmd = opts.cmd;
	completion->deadline = deadline;
	completion->cancelCtx = opts.cancelCtx;
	completion->used = true;

	try {
		wrBuf_.write(std::move(data));
	} catch (Error &e) {
		completion->used = false;
		cmpl(RPCAnswer(e), this);
		return;
	}

	lck.unlock();

	if (inLoopThread) {
		if (state_ == ConnConnected) {
			callback(io_, ev::WRITE);
		}
	} else {
		async_.send();
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
