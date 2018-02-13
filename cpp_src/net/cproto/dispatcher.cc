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
	if (ctx.call->cmd < handlers_.size()) {
		auto handler = handlers_[ctx.call->cmd];
		if (handler.func_) {
			return handler.func_(handler.object_, ctx);
		}
	}
	return Error(errParams, "Invalid RPC call. CmdCode %08X\n", ctx.call->cmd);
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
