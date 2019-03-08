#pragma once

#include <atomic>
#include <condition_variable>
#include <thread>
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
	~RPCAnswer() {
		if (hold_) delete[] data_.data();
	}
	RPCAnswer(const RPCAnswer &other) = delete;
	RPCAnswer(RPCAnswer &&other) : status_(std::move(other.status_)), data_(std::move(other.data_)), hold_(std::move(other.hold_)) {
		other.hold_ = false;
	};
	RPCAnswer &operator=(RPCAnswer &&other) {
		if (this != &other) {
			if (hold_) delete[] data_.data();
			status_ = std::move(other.status_);
			data_ = std::move(other.data_);
			hold_ = std::move(other.hold_);
			other.hold_ = false;
		}
		return *this;
	}
	RPCAnswer &operator=(const RPCAnswer &other) = delete;
	void EnsureHold() {
		if (!hold_) {
			uint8_t *data = new uint8_t[data_.size()];
			memcpy(data, data_.data(), data_.size());
			data_ = {data, data_.size()};
			hold_ = true;
		}
	}

protected:
	RPCAnswer() {}
	Error status_;
	span<uint8_t> data_;
	bool hold_ = false;
	friend class ClientConnection;
};

class ClientConnection : public ConnectionMT {
public:
	ClientConnection(ev::dynamic_loop &loop, const httpparser::UrlParser *uri);
	~ClientConnection();
	typedef std::function<void(RPCAnswer &&ans, ClientConnection *conn)> Completion;

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

		RPCAnswer ret;
		std::atomic_bool set(false);
		call(
			[&ret, &set](RPCAnswer &&ans, ClientConnection * /*conn*/) {
				ret = std::move(ans);
				ret.EnsureHold();
				set = true;
			},
			cmd, args, argss...);
		std::unique_lock<std::mutex> lck(mtx_);
		bufWait_++;
		while (!set) {
			bufCond_.wait(lck);
		}
		bufWait_--;

		return ret;
	}
	int PendingCompletions();
	void SetUpdatesHandler(Completion handler) { updatesHandler_ = handler; }

protected:
	void connect_async_cb(ev::async &) { connectInternal(); }
	void keep_alive_cb(ev::periodic &, int) {
		call([](RPCAnswer &&, ClientConnection *) {}, kCmdPing, {});
		callback(io_, ev::WRITE);
	}

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

	chunk packRPC(CmdCode cmd, uint32_t seq, const Args &args);

	void onRead() override;
	void onClose() override;

	struct RPCCompletion {
		RPCCompletion() : next_(0), used(false) {}
		~RPCCompletion() { delete next(); }
		RPCCompletion *next() { return reinterpret_cast<RPCCompletion *>(next_.load()); }
		CmdCode cmd;
		uint32_t seq;
		Completion cmpl;
		std::atomic<uintptr_t> next_;
		std::atomic<bool> used;
	};

	enum State { ConnInit, ConnConnecting, ConnConnected, ConnFailed };

	State state_;
	// Lock free map seq -> completion
	vector<RPCCompletion> completions_;
	std::condition_variable connectCond_, bufCond_;
	std::atomic<uint32_t> seq_;
	std::atomic<int32_t> bufWait_;
	std::mutex mtx_;
	std::thread::id loopThreadID_;
	Error lastError_;
	const httpparser::UrlParser *uri_;
	ev::async connect_async_;
	Completion updatesHandler_;
	ev::periodic keep_alive_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
