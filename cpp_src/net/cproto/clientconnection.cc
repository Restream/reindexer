

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
}

void ClientConnection::Connect() {
	std::unique_lock<mutex> lck(wrBufLock_);
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
	answersCond_.wait(lck);
}

bool ClientConnection::connectInternal() {
	std::unique_lock<mutex> lck(wrBufLock_);
	assert(!sock_.valid());
	assert(wrBuf_.size() == 0);

	string port = uri_->port().length() ? uri_->port() : string("6534");
	string dbName = uri_->path();
	if (dbName[0] == '/') dbName = dbName.substr(1);

	state_ = ConnConnecting;
	lastError_ = errOK;
	sock_.connect((uri_->hostname() + ":" + port).c_str());
	if (!sock_.valid()) {
		state_ = ConnFailed;
		lastError_ = Error(errNetwork, "Socket connect error %d", sock_.last_error());
		lck.unlock();
		answersCond_.notify_all();
		return false;
	}

	io_.start(sock_.fd(), ev::READ | ev::WRITE);
	async_.start();
	Args args{Arg(uri_->username()), Arg(uri_->password()), Arg(dbName)};
	callRPC(kCmdLogin, seq_, args);
	return true;
}

void ClientConnection::failInternal(const Error &error) {
	if (lastError_.ok()) lastError_ = error;
	closeConn_ = true;
	state_ = ConnFailed;
}

void ClientConnection::onClose() {
	{
		std::unique_lock<mutex> lck(wrBufLock_);
		wrBuf_.clear();
		if (lastError_.ok()) lastError_ = Error(errNetwork, "Socket connection closed");
		closeConn_ = false;
		state_ = ConnFailed;
	}
	answersCond_.notify_all();
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
		if (it.len < hdr.len) {
			rdBuf_.unroll();
			it = rdBuf_.tail();
		}
		assert(it.len >= hdr.len);

		RPCRawAnswer ans;
		ans.cmd = CmdCode(hdr.cmd);
		ans.seq = hdr.seq;

		int errCode = 0;
		try {
			Serializer ser(it.data, hdr.len);
			errCode = ser.GetVarUint();
			string errMsg = ser.GetVString().ToString();
			ans.ans.status_ = Error(errCode, errMsg);
			ans.ans.data_.assign(reinterpret_cast<uint8_t *>(it.data) + ser.Pos(), reinterpret_cast<uint8_t *>(it.data) + hdr.len);
		} catch (const Error &err) {
			failInternal(err);
			return;
		}

		wrBufLock_.lock();
		if (ans.cmd == cproto::kCmdLogin) {
			if (errCode == errOK) {
				state_ = ConnConnected;
			}
		} else {
			answers_.push_back(std::move(ans));
		}
		answersCond_.notify_all();
		wrBufLock_.unlock();

		rdBuf_.erase(hdr.len);
	}
}

RPCAnswer ClientConnection::getAnswer(CmdCode cmd, uint32_t seq) {
	RPCAnswer ret;
	std::unique_lock<mutex> lck(wrBufLock_);
	for (;;) {
		if (state_ == ConnFailed) return RPCAnswer(lastError_);

		auto it = find_if(answers_.begin(), answers_.end(), [seq](const RPCRawAnswer &a) { return a.seq == seq; });
		if (it != answers_.end()) {
			if (cmd != it->cmd) {
				ret.status_ = Error(errParams, "Invalid cmdCode %d, expected %d for seq = %d", it->cmd, cmd, seq);
			} else {
				ret = std::move(it->ans);
			}
			std::swap(*it, *std::prev(answers_.end(), 1));
			answers_.pop_back();
			return ret;
		}
		answersCond_.wait(lck);
	}
}

Args RPCAnswer::GetArgs(int minArgs) {
	cproto::Args ret;
	Serializer ser(data_.data(), data_.size());
	ret.Unpack(ser);
	if (int(ret.size()) < minArgs) {
		throw Error(errParams, "Server returned %d args, but expected %d", int(ret.size()), minArgs);
	}

	return ret;
}

Error RPCAnswer::Status() { return status_; }

void ClientConnection::callRPC(CmdCode cmd, uint32_t seq, const Args &args) {
	WrSerializer ser;

	args.Pack(ser);

	CProtoHeader hdr;
	hdr.len = ser.Len();
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.cmd = cmd;
	hdr.seq = seq;

	wrBuf_.write(reinterpret_cast<char *>(&hdr), sizeof(hdr));
	wrBuf_.write(reinterpret_cast<char *>(ser.Buf()), ser.Len());
}

RPCAnswer ClientConnection::call(CmdCode cmd, const Args &args) {
	uint32_t seq;
	{
		std::unique_lock<mutex> lck(wrBufLock_);
		if (state_ == ConnFailed) return RPCAnswer(lastError_);
		seq = seq_++;
		callRPC(cmd, seq, args);
	}
	async_.send();
	return getAnswer(cmd, seq);
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
