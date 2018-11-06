

#include "clientconnection.h"
#include <errno.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

ClientConnection::ClientConnection(ev::dynamic_loop &loop, const httpparser::UrlParser *uri)
	: ConnectionMT(-1, loop), state_(ConnInit), uri_(uri) {
	connect_async_.set<ClientConnection, &ClientConnection::connect_async_cb>(this);
	connect_async_.set(loop);
	connect_async_.start();
	waiters_.resize(1024);
}

void ClientConnection::Connect() {
	std::unique_lock<std::mutex> lck(mtx_);
	switch (state_) {
		case ConnConnected:
			return;
		case ConnInit:
		case ConnFailed:
			connect_async_.send();
			break;
		default:
			break;
	}
	connectCond_.wait(lck);
	assert(state_ == ConnConnected || state_ == ConnFailed);
}

void ClientConnection::connectInternal() {
	assert(!sock_.valid());
	assert(wrBuf_.size() == 0);

	string port = uri_->port().length() ? uri_->port() : string("6534");
	string dbName = uri_->path();
	if (dbName[0] == '/') dbName = dbName.substr(1);

	mtx_.lock();
	state_ = ConnConnecting;
	lastError_ = errOK;
	mtx_.unlock();

	auto completion = [this](const RPCAnswer &ans) {
		std::unique_lock<std::mutex> lck(mtx_);
		lastError_ = ans.Status();
		state_ = ans.Status().ok() ? ConnConnected : ConnFailed;
		wrBuf_.clear();
		connectCond_.notify_all();
	};

	sock_.connect((uri_->hostname() + ":" + port).c_str());
	if (!sock_.valid()) {
		completion(RPCAnswer(Error(errNetwork, "Socket connect error: %d", sock_.last_error())));
	} else {
		io_.start(sock_.fd(), ev::READ | ev::WRITE);
		async_.start();
		Args args{Arg(uri_->username()), Arg(uri_->password()), Arg(dbName)};
		call(completion, kCmdLogin, args);
	}
}

void ClientConnection::failInternal(const Error &error) {
	if (lastError_.ok()) lastError_ = error;
	closeConn_ = true;
	state_ = ConnFailed;
}

void ClientConnection::onClose() {
	mtx_.lock();
	wrBuf_.clear();
	if (lastError_.ok()) lastError_ = Error(errNetwork, "Socket connection closed");
	closeConn_ = false;
	state_ = ConnFailed;
	auto waiters = std::move(waiters_);
	waiters_.resize(1024);
	mtx_.unlock();

	for (auto w : waiters)
		if (w.cmpl) w.cmpl(RPCAnswer(lastError_));
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

		if (hdr.version != kCprotoVersion) {
			failInternal(
				Error(errParams, "Unsupported cproto version %04x. This client expects reindexer server v1.9.8+", int(hdr.version)));
			return;
		}

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
			string errMsg = ser.GetVString().ToString();
			ans.status_ = Error(errCode, errMsg);
			ans.data_ = {reinterpret_cast<uint8_t *>(it.data()) + ser.Pos(), hdr.len - ser.Pos()};
		} catch (const Error &err) {
			failInternal(err);
			return;
		}

		mtx_.lock();
		RPCWaiter *waiter = &waiters_[hdr.seq % waiters_.size()];

		Completion cmpl;
		if (waiter->seq == hdr.seq) {
			if (CmdCode(hdr.cmd) != waiter->cmd) {
				ans.status_ =
					Error(errParams, "Invalid cmdCode %d, expected %d for seq = %d", int(waiter->cmd), int(hdr.cmd), int(hdr.seq));
			}
			cmpl = waiter->cmpl;
			waiter->cmpl = nullptr;
		} else {
			fprintf(stderr, "Unexpected RPC answer seq=%d cmd=%d", int(hdr.cmd), int(hdr.seq));
		}
		mtx_.unlock();
		if (cmpl)
			cmpl(ans);
		else {
			fprintf(stderr, "RPC answer w/o completion routine seq=%d cmd=%d", int(hdr.cmd), int(hdr.seq));
			std::abort();
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

void ClientConnection::callRPC(CmdCode cmd, uint32_t seq, const Args &args) {
	CProtoHeader hdr;
	hdr.len = 0;
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.cmd = cmd;
	hdr.seq = seq;

	WrSerializer ser(wrBuf_.get_chunk());
	ser.Write(string_view(reinterpret_cast<char *>(&hdr), sizeof(hdr)));
	args.Pack(ser);
	reinterpret_cast<CProtoHeader *>(ser.Buf())->len = ser.Len() - sizeof(hdr);

	wrBuf_.write(ser.DetachChunk());
}

void ClientConnection::call(Completion cmpl, CmdCode cmd, const Args &args) {
	std::unique_lock<std::mutex> lck(mtx_);
	if (state_ == ConnFailed) {
		lck.unlock();
		cmpl(RPCAnswer(lastError_));
		return;
	}

	uint32_t seq = seq_++;
	uint32_t iseq = seq % waiters_.size();
	while (waiters_[seq % waiters_.size()].cmpl) {
		seq = seq_++;
		if (seq % waiters_.size() == iseq % waiters_.size()) {
			lck.unlock();
			cmpl(RPCAnswer(Error(errParams, "RPC commands buffer overflow")));
			return;
		}
	}

	callRPC(cmd, seq, args);
	waiters_[seq % waiters_.size()] = RPCWaiter(cmd, seq, cmpl);
	if (state_ == ConnConnected) async_.send();
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
