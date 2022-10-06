#include "dispatcher.h"
#include <cstdarg>
#include <unordered_map>
#include "debug/allocdebug.h"
#include "tools/fsops.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace net {
namespace cproto {

Error Dispatcher::handle(Context &ctx) {
	if (uint32_t(ctx.call->cmd) < uint32_t(handlers_.size())) {
		for (auto &middleware : middlewares_) {
			auto ret = middleware(ctx);
			if (!ret.ok()) {
				return ret;
			}
		}
		auto handler = handlers_[ctx.call->cmd];
		if (handler) {
			return handler(ctx);
		}
	}
	return Error(errParams, "Invalid RPC call. CmdCode %08X\n", int(ctx.call->cmd));
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
