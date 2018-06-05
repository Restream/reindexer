

#include "clientconnection.h"
#include <errno.h>
#include "tools/serializer.h"

namespace reindexer {
namespace net {
namespace cproto {

ClientConnection::ClientConnection(ev::dynamic_loop &loop) : ConnectionMT(-1, loop) {}

bool ClientConnection::Connect(const char *addr) {
	assert(!sock_.valid());

	sock_.connect(addr);
	if (!sock_.valid()) {
		return false;
	}
	io_.start(sock_.fd(), ev::READ | ev::WRITE);
	async_.start();
	return true;
}

void ClientConnection::onClose() {}

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

		answersLock_.lock();
		answers_.resize(answers_.size() + 1);
		RPCAnswer &ans = answers_.back();
		ans.cmd = CmdCode(hdr.cmd);
		ans.seq = hdr.seq;
		ans.data.assign(reinterpret_cast<uint8_t *>(it.data), reinterpret_cast<uint8_t *>(it.data) + hdr.len);
		answersLock_.unlock();

		rdBuf_.erase(hdr.len);
	}
}
bool ClientConnection::getAnswer(CmdCode cmd, uint32_t seq, Args &args, Error &error) {
	std::unique_lock<mutex> lck(answersLock_);
	for (auto it = answers_.begin(); it != answers_.end(); it++) {
		if (it->seq == seq) {
			if (cmd != it->cmd) {
				error = Error(errParams, "Invalid cmdCode %d, expected %d for seq = %d", it->cmd, cmd, seq);
			} else {
				Serializer ser(it->data.data(), it->data.size());
				int errCode = ser.GetVarUint();
				string errMsg = ser.GetVString().ToString();
				error = Error(errCode, errMsg);
				args.Unpack(ser);
			}
			std::swap(*it, *std::prev(answers_.end(), 1));
			answers_.pop_back();
			return true;
		}
	}
	if (!sock_.valid()) {
		error = Error(errParams, "Connection to server was dropped");
		return true;
	}

	return false;
}

void ClientConnection::callRPC(CmdCode cmd, uint32_t seq, const Args &args) {
	WrSerializer ser;

	args.Pack(ser);

	CProtoHeader hdr;
	hdr.len = ser.Len();
	hdr.magic = kCprotoMagic;
	hdr.version = kCprotoVersion;
	hdr.cmd = cmd;
	hdr.seq = seq;

	wrBufLock_.lock();
	wrBuf_.write(reinterpret_cast<char *>(&hdr), sizeof(hdr));
	wrBuf_.write(reinterpret_cast<char *>(ser.Buf()), ser.Len());
	wrBufLock_.unlock();
	async_.send();
}

ClientConnection::RPCRet ClientConnection::call(CmdCode cmd, const Args &args) {
	uint32_t seq = 3;
	callRPC(cmd, seq, args);
	Args ret;
	Error err;
	getAnswer(cmd, seq, ret, err);
	return {ret, err};
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
