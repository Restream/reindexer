#pragma once

#include <atomic>
#include <condition_variable>
#include <vector>
#include "args.h"
#include "cproto.h"
#include "estl/h_vector.h"
#include "net/connection.h"
#include "urlparser/urlparser.h"
namespace reindexer {
namespace net {
namespace cproto {

using std::vector;

class ClientConnection;

class RPCAnswer {
public:
	Error Status() const;
	Args GetArgs(int minArgs = 0) const;
	RPCAnswer(const Error &error) : status_(error) {}
	bool IsSet() const { return status_.code() != 0 || data_.data() != nullptr; }

protected:
	RPCAnswer() {}
	Error status_;
	span<uint8_t> data_;
	friend class ClientConnection;
};

class ClientConnection : public ConnectionMT {
public:
	ClientConnection(ev::dynamic_loop &loop, const httpparser::UrlParser *uri);

	typedef std::function<void(const RPCAnswer &ans)> Completion;

	template <typename... Argss>
	void Call(const Completion &cmpl, CmdCode cmd, Argss... argss) {
		Args args;
		args.reserve(sizeof...(argss));
		call(cmpl, cmd, args, argss...);
	}

	template <typename... Argss>
	RPCAnswer Call(CmdCode cmd, Argss... argss) {
		Args args;
		args.reserve(sizeof...(argss));
		std::condition_variable cond;

		RPCAnswer ret;
		call(
			[&cond, &ret, this](const RPCAnswer &ans) {
				std::unique_lock<std::mutex> lck(mtx_);
				ret = ans;
				cond.notify_one();
			},
			cmd, args, argss...);
		std::unique_lock<std::mutex> lck(mtx_);
		if (!ret.IsSet()) cond.wait(lck);
		return ret;
	}

	void Connect();

protected:
	void connect_async_cb(ev::async &) { connectInternal(); }

	void connectInternal();
	void failInternal(const Error &error);

	template <typename... Argss>
	inline void call(const Completion &cmpl, CmdCode cmd, Args &args, const string_view &val, Argss... argss) {
		args.push_back(Variant(p_string(&val)));
		return call(cmpl, cmd, args, argss...);
	}
	template <typename... Argss>
	inline void call(const Completion &cmpl, CmdCode cmd, Args &args, const string &val, Argss... argss) {
		args.push_back(Variant(p_string(&val)));
		return call(cmpl, cmd, args, argss...);
	}
	template <typename T, typename... Argss>
	inline void call(const Completion &cmpl, CmdCode cmd, Args &args, const T &val, Argss... argss) {
		args.push_back(Variant(val));
		return call(cmpl, cmd, args, argss...);
	}

	void call(Completion cmpl, CmdCode cmd, const Args &args);

	void callRPC(CmdCode cmd, uint32_t seq, const Args &args);

	void onRead() override;
	void onClose() override;

	struct RPCWaiter {
		RPCWaiter(CmdCode _cmd = kCmdPing, uint32_t _seq = 0, Completion _cmpl = nullptr) : cmd(_cmd), seq(_seq), cmpl(_cmpl) {}
		CmdCode cmd;
		uint32_t seq;
		Completion cmpl;
	};
	enum State { ConnInit, ConnConnecting, ConnConnected, ConnFailed };

	State state_;
	vector<RPCWaiter> waiters_;
	std::condition_variable connectCond_;
	uint32_t seq_;
	std::mutex mtx_;
	Error lastError_;
	const httpparser::UrlParser *uri_;
	ev::async connect_async_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
