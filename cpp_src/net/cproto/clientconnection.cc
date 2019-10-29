

#include "clientconnection.h"
#include <errno.h>
#include "core/rdxcontext.h"
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

const int kMaxCompletions = 512;
const int kKeepAliveInterval = 30;
const int kDeadlineCheckInterval = 1;

ClientConnection::ClientConnection(ev::dynamic_loop &loop, const httpparser::UrlParser *uri, seconds loginTimeout, seconds requestTimeout)
	: ConnectionMT(-1, loop),
	  state_(ConnInit),
	  completions_(kMaxCompletions),
	  seq_(0),
	  bufWait_(0),
	  uri_(uri),
	  now_(0),
	  loginTimeout_(loginTimeout),
	  keepAliveTimeout_(requestTimeout),
	  terminate_(false) {
	connect_async_.set<ClientConnection, &ClientConnection::connect_async_cb>(this);
	connect_async_.set(loop);
	connect_async_.start();
	keep_alive_.set<ClientConnection, &ClientConnection::keep_alive_cb>(this);
	keep_alive_.set(loop);
	deadlineTimer_.set<ClientConnection, &ClientConnection::deadline_check_cb>(this);
	deadlineTimer_.set(loop);
	loopThreadID_ = std::this_thread::get_id();
}  // namespace cproto

ClientConnection::~ClientConnection() { assert(!PendingCompletions()); }

void ClientConnection::connectInternal() {
	mtx_.lock();
	if (state_ == ConnConnecting || state_ == ConnConnected) {
		mtx_.unlock();
		return;
	}
	assert(!sock_.valid());
	assert(wrBuf_.size() == 0);
	state_ = ConnConnecting;
	lastError_ = errOK;

	mtx_.unlock();

	string port = uri_->port().length() ? uri_->port() : string("6534");
	string dbName = uri_->path();
	string userName = uri_->username();
	string password = uri_->password();
	if (dbName[0] == '/') dbName = dbName.substr(1);

	auto completion = [this](const RPCAnswer &ans, ClientConnection *) {
		std::unique_lock<std::mutex> lck(mtx_);
		lastError_ = ans.Status();
		state_ = ans.Status().ok() ? ConnConnected : ConnFailed;
		wrBuf_.clear();
		connectCond_.notify_all();
		if (!lastError_.ok()) {
			lck.unlock();
			closeConn();
		}
	};

	sock_.connect((uri_->hostname() + ":" + port));
	if (!sock_.valid()) {
		completion(RPCAnswer(Error(errNetwork, "Socket connect error: %d", sock_.last_error())), this);
	} else {
		io_.start(sock_.fd(), ev::WRITE);
		curEvents_ = ev::WRITE;
		async_.start();
		keep_alive_.start(kKeepAliveInterval, kKeepAliveInterval);
		deadlineTimer_.start(kDeadlineCheckInterval, kDeadlineCheckInterval);

		call(completion, {kCmdLogin, loginTimeout_, milliseconds(0)},
			 {Arg{p_string(&userName)}, Arg{p_string(&password)}, Arg{p_string(&dbName)}});
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

void ClientConnection::deadline_check_cb(ev::timer &, int) {
	now_ += kDeadlineCheckInterval;
	for (auto &c : completions_) {
		for (RPCCompletion *cc = &c; cc; cc = cc->next.get()) {
			if (!cc->used) continue;
			bool expired = (cc->deadline.count() && cc->deadline.count() <= now_);
			if (expired || (cc->cancelCtx && cc->cancelCtx->IsCancelable() && (cc->cancelCtx->GetCancelType() == CancelType::Explicit))) {
				Error err(expired ? errTimeout : errCanceled, expired ? "Request deadline exceeded" : "Canceled");
				cc->cmpl(RPCAnswer(err), this);
				if (state_ == ConnFailed) {
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

void ClientConnection::onClose() {
	vector<RPCCompletion> tmpCompletions(kMaxCompletions);
	mtx_.lock();
	wrBuf_.clear();
	if (lastError_.ok()) lastError_ = Error(errNetwork, "Socket connection closed");
	closeConn_ = false;
	state_ = ConnFailed;
	completions_.swap(tmpCompletions);
	mtx_.unlock();
	keep_alive_.stop();
	deadlineTimer_.stop();

	for (auto &c : tmpCompletions) {
		for (RPCCompletion *cc = &c; cc; cc = cc->next.get())
			if (cc->used) cc->cmpl(RPCAnswer(lastError_), this);
	}
	std::unique_ptr<Completion> tmpUpdatesHandler(updatesHandler_.release(std::memory_order_acq_rel));

	if (tmpUpdatesHandler) (*tmpUpdatesHandler)(RPCAnswer(lastError_), this);
	bufCond_.notify_all();
}

void ClientConnection::onRead() {
	CProtoHeader hdr;

	while (!closeConn_) {
		auto len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));

		if (len < sizeof(hdr)) return;
		if (hdr.magic != kCprotoMagic) {
			failInternal(Error(errNetwork, "Invalid cproto magic=%08x", hdr.magic));
			return;
		}

		if (hdr.version < kCprotoMinCompatVersion) {
			failInternal(
				Error(errParams, "Unsupported cproto version %04x. This client expects reindexer server v1.9.8+", int(hdr.version)));
			return;
		}

		len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));

		if (hdr.len + sizeof(hdr) > rdBuf_.capacity()) {
			rdBuf_.reserve(hdr.len + sizeof(hdr) + 0x1000);
		}

		if ((rdBuf_.size() - sizeof(hdr)) < hdr.len) return;

		rdBuf_.erase(sizeof(hdr));

		auto it = rdBuf_.tail();
		if (it.size() < hdr.len) {
			rdBuf_.unroll();
			it = rdBuf_.tail();
		}
		assert(it.size() >= hdr.len);

		RPCAnswer ans;

		int errCode = 0;
		try {
			Serializer ser(it.data(), hdr.len);
			errCode = ser.GetVarUint();
			string_view errMsg = ser.GetVString();
			if (errCode != errOK) {
				ans.status_ = Error(errCode, errMsg);
			}
			ans.data_ = {reinterpret_cast<uint8_t *>(it.data()) + ser.Pos(), hdr.len - ser.Pos()};
		} catch (const Error &err) {
			failInternal(err);
			return;
		}

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
				if (bufWait_) {
					std::unique_lock<std::mutex> lck(mtx_);
					completion->used = false;
					bufCond_.notify_all();
				} else {
					completion->used = false;
				}
				io_.loop.break_loop();
				break;
			}
			if (!completion) {
				auto cmdSv = CmdName(hdr.cmd);
				fprintf(stderr, "Unexpected RPC answer seq=%d cmd=%d(%.*s)\n", int(hdr.seq), hdr.cmd, int(cmdSv.size()), cmdSv.data());
			}
		}
		rdBuf_.erase(hdr.len);
	}
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
	hdr.cmd = cmd;
	hdr.seq = seq;

	WrSerializer ser(wrBuf_.get_chunk());

	ser.Write(string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));

	args.Pack(ser);
	ctxArgs.Pack(ser);

	reinterpret_cast<CProtoHeader *>(ser.Buf())->len = ser.Len() - sizeof(hdr);

	return ser.DetachChunk();
}

void ClientConnection::call(Completion cmpl, const CommandParams &opts, const Args &args) {
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
						connectCond_.wait(lck);
						if (deadline.count() && deadline <= Now()) {
							lck.unlock();
							cmpl(RPCAnswer(Error(errTimeout, "Connection deadline exceeded")), this);
							return;
						}
						if (state_ == ConnFailed) {
							auto err = lastError_;
							lck.unlock();
							cmpl(RPCAnswer(err), this);
							return;
						}
					}
					break;
				default:
					std::abort();
			}
			if (completion->used) {
				bufWait_++;
				while (completion->used) {
					bufCond_.wait(lck);
				}
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

	wrBuf_.write(std::move(data));
	lck.unlock();
	if (!inLoopThread) async_.send();
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
