#pragma once

#include <atomic>
#include <condition_variable>
#include <vector>
#include "args.h"
#include "cproto.h"
#include "estl/h_vector.h"
#include "net/connection.h"

namespace reindexer {
namespace net {
namespace cproto {

using std::vector;

class ClientConnection;

class RPCAnswer {
public:
	Error Status();
	Args GetArgs();

protected:
	Error status_;
	h_vector<uint8_t, 32> data_;
	friend class ClientConnection;
};

class ClientConnection : public ConnectionMT {
public:
	ClientConnection(ev::dynamic_loop &loop);

	template <typename... Argss>
	RPCAnswer Call(CmdCode cmd, Argss... argss) {
		Args args;
		args.reserve(sizeof...(argss));
		return call(cmd, args, argss...);
	}

	bool Connect(string_view addr, string_view username, string_view password, string_view dbName);
	// bool IsValid() { return sock_.valid(); }

	bool IsValid() {
		std::unique_lock<mutex> lck(wrBufLock_);
		return state_ == ConnConnected || state_ == ConnConnecting;
	}

protected:
	template <typename... Argss>
	inline RPCAnswer call(CmdCode cmd, Args &args, const string_view &val, Argss... argss) {
		args.push_back(KeyRef(p_string(&val)));
		return call(cmd, args, argss...);
	}
	template <typename... Argss>
	inline RPCAnswer call(CmdCode cmd, Args &args, const string &val, Argss... argss) {
		args.push_back(KeyRef(p_string(&val)));
		return call(cmd, args, argss...);
	}
	template <typename T, typename... Argss>
	inline RPCAnswer call(CmdCode cmd, Args &args, const T &val, Argss... argss) {
		args.push_back(KeyRef(val));
		return call(cmd, args, argss...);
	}

	RPCAnswer call(CmdCode cmd, const Args &args);

	void callRPC(CmdCode cmd, uint32_t seq, const Args &args);
	RPCAnswer getAnswer(CmdCode cmd, uint32_t seq);
	void onRead() override;
	void onClose() override;

	struct RPCRawAnswer {
		CmdCode cmd;
		uint32_t seq = 0;
		RPCAnswer ans;
	};
	enum State { ConnInit, ConnConnecting, ConnConnected, ConnFailed };

	State state_;
	vector<RPCRawAnswer> answers_;

	std::atomic<uint32_t> seq_;
	std::condition_variable answersCond_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
