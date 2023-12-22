#ifdef REINDEX_WITH_V3_FOLLOWERS

#include <atomic>
#include <functional>
#include "net/cproto/dispatcher.h"
#include "replv3/updatesobserver.h"

namespace reindexer {
namespace net {
namespace cproto {

class Args;
class Writer;
class RPCUpdatesPusher : public reindexer::IUpdatesObserver {
public:
	RPCUpdatesPusher();
	void SetWriter(Writer *writer) { writer_ = writer; }
	void OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord &walRec) override final;
	void OnUpdatesLost(std::string_view nsName) override final;
	void OnConnectionState(const Error &err) override final;
	void SetFilter(std::function<bool(WALRecord &)> filter) { filter_ = std::move(filter); }

protected:
	Writer *writer_;
	std::atomic<uint32_t> seq_;
	std::function<bool(WALRecord &)> filter_;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer

#endif // REINDEX_WITH_V3_FOLLOWERS
