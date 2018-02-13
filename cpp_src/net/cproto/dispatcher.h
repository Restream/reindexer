#pragma once

#include <climits>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "args.h"
#include "cproto.h"
#include "tools/errors.h"
#include "tools/slice.h"

namespace reindexer {
namespace net {
namespace cproto {

using std::string;

struct RPCCall {
	CmdCode cmd;
	uint32_t seq;
	Args args;
};

class ClientData {
public:
	typedef std::shared_ptr<ClientData> Ptr;
	virtual ~ClientData() = default;
};

class Writer {
public:
	virtual ~Writer() = default;
	virtual void WriteRPCReturn(RPCCall *call, const Args &args) = 0;
	virtual void SetClientData(ClientData::Ptr data) = 0;
	virtual ClientData::Ptr GetClientData() = 0;
};

struct Context {
	void Return(const Args &args) { writer->WriteRPCReturn(call, args); }
	void SetClientData(ClientData::Ptr data) { writer->SetClientData(data); };
	ClientData::Ptr GetClientData() { return writer->GetClientData(); }

	RPCCall *call;
	Writer *writer;
};

class Connection;

/// Reindexer cproto RPC dispatcher implementation.
class Dispatcher {
	friend class Connection;

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
				return Error(errParams, "Invalid args of RPC call expected %d, got %d", int(sizeof...(Args)), int(ctx.call->args.size()));
			return func_wrapper(obj, func, ctx);
		};
		handlers_[cmd] = {wrapper, object};
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

	struct Handler {
		std::function<Error(void *obj, Context &ctx)> func_;
		void *object_;
	};

	std::vector<Handler> handlers_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
