#pragma once

#include <atomic>
#include <condition_variable>
#include <thread>
#include <vector>
#include "args.h"
#include "core/keyvalue/p_string.h"
#include "cproto.h"
#include "estl/atomic_unique_ptr.h"
#include "net/connection.h"
#include "tools/flagguard.h"
#include "urlparser/urlparser.h"

namespace reindexer {

struct IRdxCancelContext;

namespace net {
namespace cproto {

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
	RPCAnswer(RPCAnswer &&other) : status_(std::move(other.status_)), data_(std::move(other.data_)), hold_(other.hold_) {
		other.hold_ = false;
	}
	RPCAnswer &operator=(RPCAnswer &&other) {
		if (this != &other) {
			if (hold_) delete[] data_.data();
			status_ = std::move(other.status_);
			data_ = std::move(other.data_);
			hold_ = other.hold_;
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
	span<const uint8_t> data_;
	bool hold_ = false;
	friend class ClientConnection;
};

struct CommandParams {
	CommandParams(CmdCode c, seconds n, milliseconds e, const IRdxCancelContext *ctx)
		: cmd(c), netTimeout(n), execTimeout(e), cancelCtx(ctx) {}
	CmdCode cmd;
	seconds netTimeout;
	milliseconds execTimeout;
	const IRdxCancelContext *cancelCtx;
};

class ClientConnection : public ConnectionMT {
public:
	typedef std::function<void(RPCAnswer &&ans, ClientConnection *conn)> Completion;
	typedef std::function<bool(int)> ConnectionFailCallback;
	struct Options {
		Options()
			: loginTimeout(0),
			  keepAliveTimeout(0),
			  createDB(false),
			  hasExpectedClusterID(false),
			  expectedClusterID(-1),
			  reconnectAttempts(),
			  enableCompression(false) {}
		Options(seconds _loginTimeout, seconds _keepAliveTimeout, bool _createDB, bool _hasExpectedClusterID, int _expectedClusterID,
				int _reconnectAttempts, bool _enableCompression, std::string _appName)
			: loginTimeout(_loginTimeout),
			  keepAliveTimeout(_keepAliveTimeout),
			  createDB(_createDB),
			  hasExpectedClusterID(_hasExpectedClusterID),
			  expectedClusterID(_expectedClusterID),
			  reconnectAttempts(_reconnectAttempts),
			  enableCompression(_enableCompression),
			  appName(std::move(_appName)) {}

		seconds loginTimeout;
		seconds keepAliveTimeout;
		bool createDB;
		bool hasExpectedClusterID;
		int expectedClusterID;
		int reconnectAttempts;
		bool enableCompression;
		std::string appName;
	};
	struct ConnectData {
		struct Entry {
			httpparser::UrlParser uri;
			Options opts;
			std::atomic<int> connectAttempts = {0};
		};
		bool ThereAreReconnectOptions() const;
		bool CurrDsnFailed(int failedDsnIdx) const;
		int GetNextDsnIndex() const;
		std::vector<Entry> entries;
		std::atomic<int> validEntryIdx = {0};
		int lastFailedEntryIdx = {-1};
	};

	ClientConnection(ev::dynamic_loop &loop, ConnectData *connectData,
					 ConnectionFailCallback connectionFailCallback = ConnectionFailCallback());
	~ClientConnection();

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
		auto callRet = call(
			[&ret, &set](RPCAnswer &&ans, ClientConnection * /*conn*/) {
				ret = std::move(ans);
				ret.EnsureHold();
				set = true;
			},
			opts, args, argss...);
		const auto until = opts.netTimeout.count() ? (system_clock_w::now_coarse() + 2 * opts.netTimeout) : system_clock_w::time_point();
		std::unique_lock<std::mutex> lck(mtx_);
		CounterGuardIR32 cg(bufWait_);
		if (opts.netTimeout.count()) {
			if (!bufCond_.wait_until(lck, until, [&set]() { return set.load(); })) {
				// Fallback timeout to avoid stucking on wait.
				// Usually this should never happen, however, we've seen it once on the production environment
				assertrx_dbg(false);
				if (callRet.cmpl && callRet.cmpl == &completions_[callRet.seq % completions_.size()]) {
					callRet.cmpl->used = false;
				}
				fprintf(stderr, "RxClientConnection: unexpected RPC client timeout (cmd: %d, network timeout: %d secs)\n", int(opts.cmd),
						int(opts.netTimeout.count()));
				ret = RPCAnswer(Error(errTimeout, "Fallback network timeout"));
			}
		} else {
			bufCond_.wait(lck, [&set]() { return set.load(); });
		}
		cg.Reset();

		return ret;
	}

	int PendingCompletions();
	void SetTerminateFlag() noexcept { terminate_.store(true, std::memory_order_release); }
	void SetUpdatesHandler(Completion handler) {
		auto cur = updatesHandler_.get(std::memory_order_acquire);
		auto handlerPtr = new Completion(std::move(handler));
		while (!updatesHandler_.compare_exchange_strong(cur, handlerPtr, std::memory_order_acq_rel));
		delete cur;
	}
	seconds Now() const { return seconds(now_); }
	Error CheckConnection();

	void Reconnect();

protected:
	enum State { ConnInit, ConnConnecting, ConnConnected, ConnFailed, ConnClosing };

	void connect_async_cb(ev::async &) { connectInternal(); }
	void keep_alive_cb(ev::periodic &, int);
	void deadline_check_cb(ev::timer &, int);

	void connectInternal() noexcept;
	void failInternal(const Error &error);
	void disconnect();

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
	struct CallReturn {
		explicit CallReturn(RPCCompletion *c = nullptr, uint32_t s = 0) noexcept : cmpl(c), seq(s) {}

		RPCCompletion *cmpl;
		uint32_t seq;
	};

	template <typename... Argss>
	inline CallReturn call(const Completion &cmpl, const CommandParams &opts, Args &args, const std::string_view &val, Argss... argss) {
		args.emplace_back(p_string(&val));
		return call(cmpl, opts, args, argss...);
	}
	template <typename... Argss>
	inline CallReturn call(const Completion &cmpl, const CommandParams &opts, Args &args, const std::string &val, Argss... argss) {
		args.emplace_back(p_string(&val));
		return call(cmpl, opts, args, argss...);
	}
	template <typename T, typename... Argss>
	inline CallReturn call(const Completion &cmpl, const CommandParams &opts, Args &args, const T &val, Argss... argss) {
		args.emplace_back(val);
		return call(cmpl, opts, args, argss...);
	}
	CallReturn call(const Completion &cmpl, const CommandParams &opts, const Args &args);
	chunk packRPC(CmdCode cmd, uint32_t seq, const Args &args, const Args &ctxArgs);

	ReadResT onRead() override;
	void onClose() override;

	State state_;
	// Lock free map seq -> completion
	std::vector<RPCCompletion> completions_;
	std::condition_variable connectCond_, bufCond_, closingCond_;
	std::atomic<uint32_t> seq_;
	int32_t bufWait_;
	std::mutex mtx_;
	std::thread::id loopThreadID_;
	Error lastError_;
	ev::async connect_async_;
	atomic_unique_ptr<Completion> updatesHandler_;
	ev::periodic keep_alive_;
	ev::periodic deadlineTimer_;
	std::atomic<uint32_t> now_;
	std::atomic<bool> terminate_;
	ConnectionFailCallback onConnectionFailed_;
	ConnectData *connectData_;
	int currDsnIdx_, actualDsnIdx_;
	ev::async reconnect_;
	std::atomic<bool> enableSnappy_;
	std::atomic<bool> enableCompression_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
