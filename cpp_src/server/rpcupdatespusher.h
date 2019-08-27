
#include <atomic>
#include "net/cproto/dispatcher.h"
#include "replicator/updatesobserver.h"

namespace reindexer {
namespace net {
namespace cproto {

class Args;
class Writer;
class RPCUpdatesPusher : public reindexer::IUpdatesObserver {
public:
	RPCUpdatesPusher();
	void SetWriter(Writer *writer) { writer_ = writer; }
	void OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &walRec) override final;
	void OnConnectionState(const Error &err) override final;

protected:
	Writer *writer_;
	std::atomic<uint32_t> seq_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer