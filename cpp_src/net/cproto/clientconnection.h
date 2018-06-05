#pragma once

#include <vector>
#include "args.h"
#include "cproto.h"
#include "net/connection.h"

namespace reindexer {
namespace net {
namespace cproto {

using std::vector;

class ClientConnection : public ConnectionMT {
public:
	ClientConnection(ev::dynamic_loop &loop);

	struct RPCRet {
		Args args;
		Error err;
	};

	template <typename... Argss>
	RPCRet Call(CmdCode cmd, Argss... argss) {
		Args args;
		args.reserve(sizeof...(argss));
		return call(cmd, args, argss...);
	}

	bool Connect(const char *addr);

protected:
	template <typename... Argss>
	inline RPCRet call(CmdCode cmd, Args &args, const string_view &val, Argss... argss) {
		args.push_back(KeyRef(p_string(&val)));
		return call(cmd, args, argss...);
	}
	template <typename... Argss>
	inline RPCRet call(CmdCode cmd, Args &args, const string &val, Argss... argss) {
		args.push_back(KeyRef(p_string(&val)));
		return call(cmd, args, argss...);
	}
	template <typename T, typename... Argss>
	inline RPCRet call(CmdCode cmd, Args &args, const T &val, Argss... argss) {
		args.push_back(KeyRef(val));
		return call(cmd, args, argss...);
	}

	RPCRet call(CmdCode cmd, const Args &args);

	void callRPC(CmdCode cmd, uint32_t seq, const Args &args);
	bool getAnswer(CmdCode cmd, uint32_t seq, Args &args, Error &error);
	void onRead() override;
	void onClose() override;

	struct RPCAnswer {
		CmdCode cmd;
		uint32_t seq = 0;
		vector<uint8_t> data;
		Error connErr_;
	};

	vector<RPCAnswer> answers_;

	bool respSent_ = false;

	mutex answersLock_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
