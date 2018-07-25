

#include "clientconnection.h"
#include <errno.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

ClientConnection::ClientConnection(ev::dynamic_loop &loop) : ConnectionMT(-1, loop), state_(ConnInit) {}

bool ClientConnection::Connect(string_view addr, string_view username, string_view password, string_view dbName) {
	assert(!sock_.valid());
	assert(wrBuf_.size() == 0);

	std::unique_lock<mutex> lck(wrBufLock_);
	state_ = ConnConnecting;
	sock_.connect(addr.data());
	if (!sock_.valid()) {
		state_ = ConnFailed;
		return false;
	}

	io_.start(sock_.fd(), ev::READ | ev::WRITE);
	async_.start();
	Args args{Arg(p_string(&username)), Arg(p_string(&password)), Arg(p_string(&dbName))};
	callRPC(kCmdLogin, seq_, args);
	return true;
}

void ClientConnection::onClose() {
	{
		std::unique_lock<mutex> lck(wrBufLock_);
		state_ = ConnFailed;
		wrBuf_.clear();
	}
	answersCond_.notify_all();
}

void ClientConnection::onRead() {
	CProtoHeader hdr;

	while (!closeConn_) {
		auto len = rdBuf_.peek(reinterpret_cast<char *>(&hdr), sizeof(hdr));

		if (len < sizeof(hdr)) return;
		if (hdr.magic != kCprotoMagic || hdr.version != kCprotoVersion) {
			// responceRPC(ctx, Error(errParams, "Invalid cproto header: magic=%08x or version=%08x", hdr.magic, hdr.version), Args());
			closeConn_ = true;
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

		Serializer ser(it.data, hdr.len);
		int errCode = ser.GetVarUint();
		string errMsg = ser.GetVString().ToString();
		ans.ans.status_ = Error(errCode, errMsg);
		assert(ser.Pos() <= hdr.len);
		ans.ans.data_.assign(reinterpret_cast<uint8_t *>(it.data) + ser.Pos(), reinterpret_cast<uint8_t *>(it.data) + hdr.len);

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
		if (state_ == ConnFailed) {
			ret.status_ = Error(errNetwork, "Connection to server was dropped");
			return ret;
		}
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

Args RPCAnswer::GetArgs() {
	cproto::Args ret;
	Serializer ser(data_.data(), data_.size());
	ret.Unpack(ser);
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
	// WrSerializer ser;
	// ser.Printf("%s ", cproto::CmdName(cmd));
	// args.Dump(ser);
	// ser.PutChar(0);

	// ser.Printf(" -> %s", err.ok() ? "OK" : err.what().c_str());
	// if (ret.size()) {
	// 	ser.PutChars(" ");
	// 	ret.Dump(ser);
	// }
	// printf("%s\n", ser.Buf());

	wrBufLock_.lock();
	uint32_t seq = seq_++;
	callRPC(cmd, seq, args);
	wrBufLock_.unlock();
	async_.send();
	return getAnswer(cmd, seq);
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
