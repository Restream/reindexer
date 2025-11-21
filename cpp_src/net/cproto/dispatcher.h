#pragma once

#include "args.h"
#include "cproto.h"
#include "estl/chunk.h"
#include "net/connectinstatscollector.h"
#include "net/stat.h"
#include "tools/errors.h"
#include "tools/lsn.h"

namespace reindexer {
namespace net {
namespace cproto {

using std::chrono::milliseconds;

struct [[nodiscard]] RPCCall {
	RPCCall(CmdCode cmd_, uint32_t seq_, Args args_, milliseconds execTimeout_, lsn_t lsn_, int emitterServerId_, int shardId_,
			bool shardingParallelExecution_)
		: cmd{cmd_},
		  seq{seq_},
		  args{args_},
		  execTimeout{execTimeout_},
		  lsn{lsn_},
		  emitterServerId{emitterServerId_},
		  shardId{shardId_},
		  shardingParallelExecution{shardingParallelExecution_} {}

	CmdCode cmd;
	uint32_t seq;
	Args args;
	milliseconds execTimeout;
	lsn_t lsn;
	int emitterServerId;
	int shardId;
	bool shardingParallelExecution;
};

struct [[nodiscard]] ClientData {
	virtual ~ClientData() = default;
};

struct Context;

class [[nodiscard]] Writer {
public:
	virtual ~Writer() = default;
	virtual size_t AvailableEventsSpace() noexcept = 0;
	virtual void SendEvent(chunk&& ch) = 0;
	virtual void WriteRPCReturn(Context& ctx, const Args& args, const Error& status) = 0;
	virtual void SetClientData(std::unique_ptr<ClientData>&& data) noexcept = 0;
	virtual ClientData* GetClientData() noexcept = 0;
	virtual std::shared_ptr<reindexer::net::connection_stat> GetConnectionStat() noexcept = 0;
};

struct [[nodiscard]] Context {
	void Return(const Args& args, const Error& status = Error()) { writer->WriteRPCReturn(*this, args, status); }
	void SetClientData(std::unique_ptr<ClientData>&& data) noexcept { writer->SetClientData(std::move(data)); }
	ClientData* GetClientData() noexcept { return writer->GetClientData(); }

	std::string_view clientAddr;
	RPCCall* call;
	Writer* writer;
	Stat stat;
	bool respSent;
};

/// Reindexer cproto RPC dispatcher implementation.
class [[nodiscard]] Dispatcher {
public:
	/// Add handler for command.
	/// @param cmd - Command code
	/// @param object - handler class object
	/// @param func - handler
	template <class K, typename... Args>
	void Register(CmdCode cmd, K* object, Error (K::*func)(Context&, Args... args)) {
		handlers_[cmd] = FuncWrapper<K, Args...>{object, func};
	}

	/// Add middleware for commands
	/// @param object - handler class object
	/// @param func - handler
	template <class K>
	void Middleware(K* object, Error (K::*func)(Context&)) {
		middlewares_.push_back(FuncWrapper<K>{object, func});
	}

	/// Set logger for commands
	/// @param object - logger class object
	/// @param func - logger
	template <class K>
	void Logger(K* object, void (K::*func)(Context& ctx, const Error& err, const Args& ret)) {
		logger_ = [=](Context& ctx, const Error& err, const Args& ret) { (static_cast<K*>(object)->*func)(ctx, err, ret); };
	}

	/// Set closer notifier
	/// @param object close class object
	/// @param func function, to be called on connection close
	template <class K>
	void OnClose(K* object, void (K::*func)(Context& ctx, const Error& err)) {
		onClose_ = [=](Context& ctx, const Error& err) { (static_cast<K*>(object)->*func)(ctx, err); };
	}

	/// Set response sent notifier
	/// @param object class object
	/// @param func function, to be called on response sent
	template <class K>
	void OnResponse(K* object, void (K::*func)(Context& ctx)) {
		onResponse_ = [=](Context& ctx) { (static_cast<K*>(object)->*func)(ctx); };
	}

	/// Get reference to the current logger functor
	/// @return Log handler reference
	const std::function<void(Context& ctx, const Error& err, const Args& args)>& LoggerRef() const noexcept { return logger_; }
	/// Get reference to the current OnClose() functor
	/// @return OnClose callback reference
	const std::function<void(Context& ctx, const Error& err)>& OnCloseRef() const noexcept { return onClose_; }
	/// Get reference to the current OnResponse() functor
	/// @return OnResponse callback reference
	const std::function<void(Context& ctx)>& OnResponseRef() const noexcept { return onResponse_; }

	/// Handle RPC from the context
	/// @param ctx - RPC context
	Error Handle(Context& ctx) {
		if (uint32_t(ctx.call->cmd) < uint32_t(handlers_.size())) [[likely]] {
			for (auto& middleware : middlewares_) {
				auto ret = middleware(ctx);
				if (!ret.ok()) {
					return ret;
				}
			}
			auto handler = handlers_[ctx.call->cmd];
			if (handler) [[likely]] {
				return handler(ctx);
			}
		}
		return Error(errParams, "Invalid RPC call. CmdCode {:#08x}\n", int(ctx.call->cmd));
	}

private:
	template <typename>
	struct [[nodiscard]] is_optional : public std::false_type {};

	template <typename T>
	struct [[nodiscard]] is_optional<std::optional<T>> : public std::true_type {};

	template <typename T, std::enable_if_t<!is_optional<T>::value, int> = 0>
	static T get_arg(const Args& args, size_t index, const Context& ctx) {
		if (index >= args.size()) [[unlikely]] {
			throw Error(errParams, "Invalid args of {} call; argument {} is not submitted", CmdName(ctx.call->cmd),
						static_cast<int>(index));
		}
		if (!args[index].Type().IsSame(KeyValueType::From<T>())) [[unlikely]] {
			throw Error(errLogic, "Incorrect variant type of {} call, argument index {}, type '{}', expected type '{}'",
						CmdName(ctx.call->cmd), static_cast<int>(index), args[index].Type().Name(), KeyValueType::From<T>().Name());
		}
		return T(args[index]);
	}
	template <typename T, std::enable_if_t<is_optional<T>::value, int> = 0>
	static T get_arg(const Args& args, size_t index, const Context& ctx) {
		if (index >= args.size()) [[unlikely]] {
#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ == 9
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
			return T();
#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ == 9
#pragma GCC diagnostic pop
#endif
		}
		if (!args[index].Type().IsSame(KeyValueType::From<typename T::value_type>())) [[unlikely]] {
			throw Error(errLogic, "Incorrect variant type of {} call, argument index {}, type '{}', optional expected type '{}'",
						CmdName(ctx.call->cmd), static_cast<int>(index), args[index].Type().Name(),
						KeyValueType::From<typename T::value_type>().Name());
		}
		return T(typename T::value_type(args[index]));
	}

	template <typename K, typename... Args>
	class [[nodiscard]] FuncWrapper {
	public:
		FuncWrapper(K* o, Error (K::*f)(Context&, Args...)) noexcept : obj_{o}, func_{f} {}
		Error operator()(Context& ctx) const { return impl(ctx, std::index_sequence_for<Args...>{}); }

	private:
		template <size_t... I>
		Error impl(Context& ctx, std::index_sequence<I...>) const {
			return (obj_->*func_)(ctx, get_arg<Args>(ctx.call->args, I, ctx)...);
		}
		K* obj_;
		Error (K::*func_)(Context&, Args...);
	};

	using Handler = std::function<Error(Context&)>;

	std::array<Handler, kCmdCodeMax> handlers_;
	h_vector<Handler, 1> middlewares_;

	std::function<void(Context& ctx, const Error& err, const Args& args)> logger_;
	std::function<void(Context& ctx, const Error& err)> onClose_;
	// This should be called from the connection thread only to prevent access to other connection's ClientData
	std::function<void(Context& ctx)> onResponse_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
