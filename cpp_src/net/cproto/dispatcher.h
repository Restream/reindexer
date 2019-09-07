#pragma once

#include <climits>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "args.h"
#include "core/keyvalue/p_string.h"
#include "cproto.h"
#include "estl/string_view.h"
#include "net/stat.h"
#include "tools/errors.h"

namespace reindexer {
namespace net {
namespace cproto {

using std::string;
using std::chrono::milliseconds;

struct RPCCall {
	CmdCode cmd;
	uint32_t seq;
	Args args;
	milliseconds execTimeout_;
};

class ClientData {
public:
	typedef std::shared_ptr<ClientData> Ptr;
	virtual ~ClientData() = default;
};

struct Context;
class Writer {
public:
	virtual ~Writer() = default;
	virtual void WriteRPCReturn(Context &ctx, const Args &args) = 0;
	virtual void CallRPC(CmdCode cmd, const Args &args) = 0;
	virtual void SetClientData(ClientData::Ptr data) = 0;
	virtual ClientData::Ptr GetClientData() = 0;
};

struct Context {
	void Return(const Args &args) { writer->WriteRPCReturn(*this, args); }
	void SetClientData(ClientData::Ptr data) { writer->SetClientData(data); }
	ClientData::Ptr GetClientData() { return writer->GetClientData(); }

	string_view clientAddr;
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
	Dispatcher() : handlers_(kCmdCodeMax, {nullptr, nullptr}) {}

	/// Add handler for command.
	/// @param cmd - Command code
	/// @param object - handler class object
	/// @param func - handler
	template <class K, typename... Args>
	void Register(CmdCode cmd, K *object, Error (K::*func)(Context &, Args... args)) {
		auto wrapper = [func](void *obj, Context &ctx) {
			if (sizeof...(Args) > ctx.call->args.size())
				return Error(errParams, "Invalid args of %s call expected %d, got %d", CmdName(ctx.call->cmd), int(sizeof...(Args)),
							 int(ctx.call->args.size()));
			return func_wrapper(obj, func, ctx);
		};
		handlers_[cmd] = {wrapper, object};
	}

	/// Add middleware for commands
	/// @param object - handler class object
	/// @param func - handler
	template <class K>
	void Middleware(K *object, Error (K::*func)(Context &)) {
		auto wrapper = [func](void *obj, Context &ctx) { return func_wrapper(obj, func, ctx); };
		middlewares_.push_back({wrapper, object});
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

	template <class K>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx);
	}
	template <class K, typename T1>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]));
	}
	template <class K, typename T1, typename T2>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1, T2), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]), T2(ctx.call->args[1]));
	}
	template <class K, typename T1, typename T2, typename T3>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1, T2, T3), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]), T2(ctx.call->args[1]), T3(ctx.call->args[2]));
	}
	template <class K, typename T1, typename T2, typename T3, typename T4>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1, T2, T3, T4), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]), T2(ctx.call->args[1]), T3(ctx.call->args[2]),
											  T4(ctx.call->args[3]));
	}
	template <class K, typename T1, typename T2, typename T3, typename T4, typename T5>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1, T2, T3, T4, T5), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]), T2(ctx.call->args[1]), T3(ctx.call->args[2]),
											  T4(ctx.call->args[3]), T5(ctx.call->args[4]));
	}
	template <class K, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1, T2, T3, T4, T5, T6), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]), T2(ctx.call->args[1]), T3(ctx.call->args[2]),
											  T4(ctx.call->args[3]), T5(ctx.call->args[4]), T6(ctx.call->args[5]));
	}
	template <class K, typename T1, typename T2, typename T3, typename T4, typename T5, typename T6, typename T7>
	static Error func_wrapper(void *obj, Error (K::*func)(Context &ctx, T1, T2, T3, T4, T5, T6, T7), Context &ctx) {
		return (static_cast<K *>(obj)->*func)(ctx, T1(ctx.call->args[0]), T2(ctx.call->args[1]), T3(ctx.call->args[2]),
											  T4(ctx.call->args[3]), T5(ctx.call->args[4]), T6(ctx.call->args[5]), T7(ctx.call->args[6]));
	}

	struct Handler {
		std::function<Error(void *obj, Context &ctx)> func_;
		void *object_;
	};

	std::vector<Handler> handlers_;
	std::vector<Handler> middlewares_;

	std::function<void(Context &ctx, const Error &err, const Args &args)> logger_;
	std::function<void(Context &ctx, const Error &err)> onClose_;
	std::function<void(Context &ctx)> onResponse_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
