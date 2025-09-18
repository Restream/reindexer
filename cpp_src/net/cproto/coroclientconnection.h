#pragma once

#include "args.h"
#include "core/keyvalue/p_string.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "cproto.h"
#include "net/manualconnection.h"
#include "tools/lsn.h"
#include "tools/serializer.h"
#include "urlparser/urlparser.h"

namespace reindexer {

struct IRdxCancelContext;

namespace net {
namespace cproto {

using std::chrono::seconds;
using std::chrono::milliseconds;

class [[nodiscard]] CoroRPCAnswer {
public:
	const Error& Status() const noexcept { return status_; }
	Args GetArgs(int minArgs = 0) const {
		cproto::Args ret;
		if (!data_.empty()) {
			Serializer ser(data_.data(), data_.size());
			ret.Unpack(ser);
		}
		if (int(ret.size()) < minArgs) {
			throw Error(errParams, "Server returned {} args, but expected {}", int(ret.size()), minArgs);
		}

		return ret;
	}
	CoroRPCAnswer() = default;
	CoroRPCAnswer(const Error& error) : status_(error) {}
	CoroRPCAnswer(const CoroRPCAnswer& other) = delete;
	CoroRPCAnswer(CoroRPCAnswer&& other) = default;
	CoroRPCAnswer& operator=(CoroRPCAnswer&& other) = default;
	CoroRPCAnswer& operator=(const CoroRPCAnswer& other) = delete;

	void EnsureHold(chunk&& ch) {
		ch.append(std::string_view(reinterpret_cast<const char*>(data_.data()), data_.size()));
		storage_ = std::move(ch);
		data_ = {storage_.data(), storage_.size()};
	}

protected:
	Error status_;
	std::span<const uint8_t> data_;
	chunk storage_;
	friend class CoroClientConnection;
};

constexpr int64_t kShardingParallelExecutionBit = int64_t{1} << 62;
constexpr int64_t kShardingFlagsMask = int64_t{0x7FFFFFFF} << 32;

struct CommandParams;

class [[nodiscard]] CoroClientConnection {
public:
	using UpdatesHandlerT = std::function<void(const CoroRPCAnswer& ans)>;
	using FatalErrorHandlerT = std::function<void(Error err)>;
	using ClockT = steady_clock_w;
	using TimePointT = ClockT::time_point;
	using ConnectionStateHandlerT = std::function<void(const Error&)>;

	struct [[nodiscard]] Options {
		Options() noexcept
			: loginTimeout(0),
			  keepAliveTimeout(0),
			  createDB(false),
			  hasExpectedClusterID(false),
			  expectedClusterID(-1),
			  reconnectAttempts(),
			  enableCompression(false),
			  requestDedicatedThread(false) {}
		Options(milliseconds _loginTimeout, milliseconds _keepAliveTimeout, bool _createDB, bool _hasExpectedClusterID,
				int _expectedClusterID, int _reconnectAttempts, bool _enableCompression, bool _requestDedicatedThread, std::string _appName,
				std::string _replToken) noexcept
			: loginTimeout(_loginTimeout),
			  keepAliveTimeout(_keepAliveTimeout),
			  createDB(_createDB),
			  hasExpectedClusterID(_hasExpectedClusterID),
			  expectedClusterID(_expectedClusterID),
			  reconnectAttempts(_reconnectAttempts),
			  enableCompression(_enableCompression),
			  requestDedicatedThread(_requestDedicatedThread),
			  appName(std::move(_appName)),
			  replToken(std::move(_replToken)) {}

		milliseconds loginTimeout;
		milliseconds keepAliveTimeout;
		bool createDB;
		bool hasExpectedClusterID;
		int expectedClusterID;
		int reconnectAttempts;
		bool enableCompression;
		bool requestDedicatedThread;
		std::string appName;
		std::string replToken;
	};
	struct [[nodiscard]] ConnectData {
		httpparser::UrlParser uri;
		Options opts;
	};

	CoroClientConnection();
	~CoroClientConnection();

	void Start(ev::dynamic_loop& loop, ConnectData&& connectData);
	void Stop();
	bool IsRunning() const noexcept { return isRunning_; }
	Error Status(bool forceCheck, milliseconds netTimeout, milliseconds execTimeout, const IRdxCancelContext* ctx);
	bool RequiresStatusCheck() const noexcept { return !loggedIn_; }
	TimePointT Now() const noexcept { return now_; }
	std::optional<TimePointT> LoginTs() const noexcept {
		return (isRunning_ && loggedIn_) ? std::optional<TimePointT>{loginTs_} : std::nullopt;
	}
	void SetConnectionStateHandler(ConnectionStateHandlerT handler) noexcept { connectionStateHandler_ = std::move(handler); }

	template <typename... Argss>
	CoroRPCAnswer Call(const CommandParams& opts, const Argss&... argss) {
		Args args;
		args.reserve(sizeof...(argss));
		return call(opts, args, argss...);
	}

	std::optional<std::string> RxServerVersion() const noexcept { return rxVersion_; }

private:
	struct [[nodiscard]] RPCData {
		RPCData() noexcept : seq(0), used(false), system(false), cancelCtx(nullptr), rspCh(1) {}
		uint32_t seq;
		bool used;
		bool system;
		TimePointT deadline;
		const reindexer::IRdxCancelContext* cancelCtx;
		coroutine::channel<CoroRPCAnswer> rspCh;
	};

	struct [[nodiscard]] MarkedChunk {
		uint32_t seq;
		bool noReply;
		std::optional<TimePointT> requiredLoginTs;
		chunk data;
	};

	template <typename... Argss>
	RX_ALWAYS_INLINE CoroRPCAnswer call(const CommandParams& opts, Args& args, std::string_view val, const Argss&... argss) {
		args.emplace_back(p_string(&val));
		return call(opts, args, argss...);
	}
	template <typename... Argss>
	RX_ALWAYS_INLINE CoroRPCAnswer call(const CommandParams& opts, Args& args, const std::string& val, const Argss&... argss) {
		args.emplace_back(p_string(&val));
		return call(opts, args, argss...);
	}
	template <typename T, typename... Argss>
	RX_ALWAYS_INLINE CoroRPCAnswer call(const CommandParams& opts, Args& args, const T& val, const Argss&... argss) {
		args.emplace_back(val);
		return call(opts, args, argss...);
	}

	CoroRPCAnswer call(const CommandParams& opts, const Args& args);
	Error callNoReply(const CommandParams& opts, uint32_t seq, const Args& args);

	MarkedChunk packRPC(CmdCode cmd, uint32_t seq, bool noReply, const Args& args, const Args& ctxArgs,
						std::optional<TimePointT> requiredLoginTs);
	void appendChunck(std::vector<char>& buf, chunk&& ch);
	Error login(std::vector<char>& buf);
	void handleFatalErrorFromReader(const Error& err) noexcept;
	void handleFatalErrorImpl(const Error& err) noexcept;
	void handleFatalErrorFromWriter(const Error& err) noexcept;
	chunk getChunk() noexcept;
	void recycleChunk(chunk&&) noexcept;
	void sendCloseResults(const CProtoHeader&, const CoroRPCAnswer&);

	void writerRoutine();
	void readerRoutine();
	void deadlineRoutine();
	void pingerRoutine();
	void setLoggedIn(bool val) noexcept {
		loggedIn_ = val;
		if (val) {
			loginTs_ = ClockT::now();
		}
	}

	TimePointT now_;
	bool terminate_ = false;
	bool isRunning_ = false;
	ev::dynamic_loop* loop_ = nullptr;

	// seq -> rpc data
	std::vector<RPCData> rpcCalls_;

	bool requestDedicatedThread_ = false;
	bool enableCompression_ = false;
	std::vector<chunk> recycledChuncks_;
	coroutine::channel<MarkedChunk> wrCh_;
	coroutine::channel<uint32_t> seqNums_;
	ConnectData connectData_;
	ConnectionStateHandlerT connectionStateHandler_;
	coroutine::wait_group wg_;
	coroutine::wait_group readWg_;
	bool loggedIn_ = false;
	coroutine::channel<bool> errSyncCh_;
	manual_connection conn_;
	TimePointT loginTs_;
	std::string compressedBuffer_;
	std::optional<std::string> rxVersion_;
};

struct [[nodiscard]] CommandParams {
	CommandParams(CmdCode c, milliseconds n, milliseconds e, lsn_t l, int sId, int shId, const IRdxCancelContext* ctx,
				  bool parallel) noexcept
		: cmd(c),
		  netTimeout(n),
		  execTimeout(e),
		  lsn(l),
		  serverId(sId),
		  shardId(shId),
		  cancelCtx(ctx),
		  shardingParallelExecution(parallel) {}
	CommandParams(CmdCode c, milliseconds n, milliseconds e, lsn_t l, int sId, int shId, const IRdxCancelContext* ctx, bool parallel,
				  CoroClientConnection::TimePointT loginTs) noexcept
		: CommandParams(c, n, e, l, sId, shId, ctx, parallel) {
		requiredLoginTs.emplace(loginTs);
	}
	CmdCode cmd;
	milliseconds netTimeout;
	milliseconds execTimeout;
	lsn_t lsn;
	int serverId;
	int shardId;
	const IRdxCancelContext* cancelCtx;
	bool shardingParallelExecution;
	std::optional<CoroClientConnection::TimePointT> requiredLoginTs;
};

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
