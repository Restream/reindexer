#pragma once

#include <atomic>
#include <condition_variable>
#include <thread>
#include <vector>
#include "args.h"
#include "cproto.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/h_vector.h"
#include "net/connection.h"
#include "urlparser/urlparser.h"

namespace reindexer {

struct IRdxCancelContext;

namespace net {
namespace cproto {

using std::vector;
using std::chrono::seconds;
using std::chrono::milliseconds;

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
	}
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
	struct Options {
		Options() : loginTimeout(0), keepAliveTimeout(0), createDB(false), hasExpectedClusterID(false), expectedClusterID(-1) {}
		Options(seconds _loginTimeout, seconds _keepAliveTimeout, bool _createDB, bool _hasExpectedClusterID, int _expectedClusterID)
			: loginTimeout(_loginTimeout),
			  keepAliveTimeout(_keepAliveTimeout),
			  createDB(_createDB),
			  hasExpectedClusterID(_hasExpectedClusterID),
			  expectedClusterID(_expectedClusterID) {}

		seconds loginTimeout;
		seconds keepAliveTimeout;
		bool createDB;
		bool hasExpectedClusterID;
		int expectedClusterID;
	};

	ClientConnection(ev::dynamic_loop &loop, const httpparser::UrlParser *uri, const Options &options = Options());
	~ClientConnection();
	typedef std::function<void(RPCAnswer &&ans, ClientConnection *conn)> Completion;

	struct CommandParams {
		CommandParams(CmdCode c, seconds n, milliseconds e, const IRdxCancelContext *ctx = nullptr)
			: cmd(c), netTimeout(n), execTimeout(e), cancelCtx(ctx) {}
		CmdCode cmd;
		seconds netTimeout;
		milliseconds execTimeout;
		const IRdxCancelContext *cancelCtx;
	};

	template <typename... Argss>
	void Call(const Completion &cmpl, const CommandParams &opts, Argss... argss) {
		Args args;
		args.reserve(sizeof...(argss));
		call(cmpl, opts, args, argss...);
	}

	template <typename... Argss>
	RPCAnswer Call(const CommandParams &opts, Argss... argss) {
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
			opts, args, argss...);
		std::unique_lock<std::mutex> lck(mtx_);
		bufWait_++;
		bufCond_.wait(lck, [&set]() { return set.load();});
		bufWait_--;

		return ret;
	}

	int PendingCompletions();
	void SetTerminateFlag() noexcept { terminate_.store(true, std::memory_order_release); }
	void SetUpdatesHandler(Completion handler) {
		auto cur = updatesHandler_.get(std::memory_order_acquire);
		auto handlerPtr = new Completion(std::move(handler));
		while (!updatesHandler_.compare_exchange_strong(cur, handlerPtr, std::memory_order_acq_rel))
			;
		delete cur;
	}
	seconds Now() const { return seconds(now_); }
	Error CheckConnection();

protected:
	void connect_async_cb(ev::async &) { connectInternal(); }
	void keep_alive_cb(ev::periodic &, int) {
		if (!terminate_.load(std::memory_order_acquire)) {
			call([](RPCAnswer &&, ClientConnection *) {}, {kCmdPing, options_.keepAliveTimeout, milliseconds(0)}, {});
			callback(io_, ev::WRITE);
		}
	}
	void deadline_check_cb(ev::timer &, int);

	void connectInternal();
	void failInternal(const Error &error);

	template <typename... Argss>
	inline void call(const Completion &cmpl, const CommandParams &opts, Args &args, const string_view &val, Argss... argss) {
		args.push_back(Variant(p_string(&val)));
		return call(cmpl, opts, args, argss...);
	}
	template <typename... Argss>
	inline void call(const Completion &cmpl, const CommandParams &opts, Args &args, const string &val, Argss... argss) {
		args.push_back(Variant(p_string(&val)));
		return call(cmpl, opts, args, argss...);
	}
	template <typename T, typename... Argss>
	inline void call(const Completion &cmpl, const CommandParams &opts, Args &args, const T &val, Argss... argss) {
		args.push_back(Variant(val));
		return call(cmpl, opts, args, argss...);
	}

	void call(Completion cmpl, const CommandParams &opts, const Args &args);

	chunk packRPC(CmdCode cmd, uint32_t seq, const Args &args, const Args &ctxArgs);

	void onRead() override;
	void onClose() override;

	struct RPCCompletion {
		RPCCompletion() : cmd(kCmdPing), seq(0), next(nullptr), used(false), deadline(0), cancelCtx(nullptr) {}
		CmdCode cmd;
		uint32_t seq;
		Completion cmpl;
		atomic_unique_ptr<RPCCompletion> next;
		std::atomic<bool> used;
		seconds deadline;
		const reindexer::IRdxCancelContext *cancelCtx;
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
	atomic_unique_ptr<Completion> updatesHandler_;
	ev::periodic keep_alive_;
	ev::periodic deadlineTimer_;
	std::atomic<uint32_t> now_;
	std::atomic<bool> terminate_;
	const Options options_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
