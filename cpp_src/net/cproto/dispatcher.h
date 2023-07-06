#pragma once

#include <climits>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include "args.h"
#include "core/keyvalue/p_string.h"
#include "cproto.h"
#include "net/connection.h"
#include "net/stat.h"
#include "tools/errors.h"

namespace reindexer {
namespace net {
namespace cproto {

using std::chrono::milliseconds;

struct RPCCall {
	CmdCode cmd;
	uint32_t seq;
	Args args;
	milliseconds execTimeout_;
};

struct ClientData {
	virtual ~ClientData() = default;
};

struct Context;
struct IRPCCall {
	void (*Get)(IRPCCall *, CmdCode &, Args &);
	intrusive_ptr<intrusive_atomic_rc_wrapper<chunk>> data_;
};

class Writer {
public:
	virtual ~Writer() = default;
	virtual void WriteRPCReturn(Context &ctx, const Args &args, const Error &status) = 0;
	virtual void CallRPC(const IRPCCall &call) = 0;
	virtual void SetClientData(std::unique_ptr<ClientData> &&data) noexcept = 0;
	virtual ClientData *GetClientData() noexcept = 0;
	virtual std::shared_ptr<reindexer::net::connection_stat> GetConnectionStat() noexcept = 0;
};

struct Context {
	void Return(const Args &args, const Error &status = errOK) { writer->WriteRPCReturn(*this, args, status); }
	void SetClientData(std::unique_ptr<ClientData> &&data) noexcept { writer->SetClientData(std::move(data)); }
	ClientData *GetClientData() noexcept { return writer->GetClientData(); }

	std::string_view clientAddr;
	RPCCall *call;
	Writer *writer;
	Stat stat;
	bool respSent;
};

class ServerConnection;

/// Reindexer cproto RPC dispatcher implementation.
class Dispatcher {
	friend class ServerConnection;

public:
	Dispatcher() : handlers_(kCmdCodeMax, nullptr) {}

	/// Add handler for command.
	/// @param cmd - Command code
	/// @param object - handler class object
	/// @param func - handler
	/// @param hasOptionalArgs - has to be true if func has optional args
	template <class K, typename... Args>
	void Register(CmdCode cmd, K *object, Error (K::*func)(Context &, Args... args)) {
		handlers_[cmd] = FuncWrapper<K, Args...>{object, func};
	}

	/// Add middleware for commands
	/// @param object - handler class object
	/// @param func - handler
	template <class K>
	void Middleware(K *object, Error (K::*func)(Context &)) {
		middlewares_.push_back(FuncWrapper<K>{object, func});
	}

	/// Set logger for commands
	/// @param object - logger class object
	/// @param func - logger
	template <class K>
	void Logger(K *object, void (K::*func)(Context &ctx, const Error &err, const Args &ret)) {
		logger_ = [=](Context &ctx, const Error &err, const Args &ret) { (static_cast<K *>(object)->*func)(ctx, err, ret); };
	}

	/// Set closer notifier
	/// @param object close class object
	/// @param func function, to be called on connecion close
	template <class K>
	void OnClose(K *object, void (K::*func)(Context &ctx, const Error &err)) {
		onClose_ = [=](Context &ctx, const Error &err) { (static_cast<K *>(object)->*func)(ctx, err); };
	}

	/// Set response sent notifier
	/// @param object class object
	/// @param func function, to be called on response sent
	template <class K>
	void OnResponse(K *object, void (K::*func)(Context &ctx)) {
		onResponse_ = [=](Context &ctx) { (static_cast<K *>(object)->*func)(ctx); };
	}

protected:
	Error handle(Context &ctx);

	template <typename>
	struct is_optional : public std::false_type {};

	template <typename T>
	struct is_optional<std::optional<T>> : public std::true_type {};

	template <typename T, std::enable_if_t<!is_optional<T>::value, int> = 0>
	static T get_arg(const Args &args, size_t index, const Context &ctx) {
		if (index >= args.size()) {
			throw Error(errParams, "Invalid args of %s call; argument %d is not submited", CmdName(ctx.call->cmd), static_cast<int>(index));
		}
		return T(args[index]);
	}
	template <typename T, std::enable_if_t<is_optional<T>::value, int> = 0>
	static T get_arg(const Args &args, size_t index, const Context &) {
		return index < args.size() ? T(typename T::value_type(args[index])) : T();
	}

	template <typename K, typename... Args>
	class FuncWrapper {
	public:
		FuncWrapper(K *o, Error (K::*f)(Context &, Args...)) noexcept : obj_{o}, func_{f} {}
		Error operator()(Context &ctx) const { return impl(ctx, std::index_sequence_for<Args...>{}); }

	private:
		template <size_t... I>
		Error impl(Context &ctx, std::index_sequence<I...>) const {
			return (obj_->*func_)(ctx, get_arg<Args>(ctx.call->args, I, ctx)...);
		}
		K *obj_;
		Error (K::*func_)(Context &, Args...);
	};

	using Handler = std::function<Error(Context &)>;

	std::vector<Handler> handlers_;
	std::vector<Handler> middlewares_;

	std::function<void(Context &ctx, const Error &err, const Args &args)> logger_;
	std::function<void(Context &ctx, const Error &err)> onClose_;
	// This should be called from the connection thread only to prevet access to other connection's ClientData
	std::function<void(Context &ctx)> onResponse_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
