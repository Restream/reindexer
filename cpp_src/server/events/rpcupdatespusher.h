#include "events/iexternal_listener.h"
#include "net/cproto/dispatcher.h"

#ifdef REINDEX_WITH_V3_FOLLOWERS

#include <atomic>
#include <functional>
#include "events/observer.h"

namespace reindexer {
namespace net {
namespace cproto {

class Args;
class Writer;
class RPCUpdatesPusherV3 : public reindexer::IUpdatesObserverV3 {
public:
	RPCUpdatesPusherV3();
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

#endif	// REINDEX_WITH_V3_FOLLOWERS

namespace reindexer {
namespace net {
namespace cproto {
class Args;
class Writer;
class RPCEventsPusher : public reindexer::IEventsObserver {
public:
	void SetWriter(Writer *writer) noexcept { writer_ = writer; }

	size_t AvailableEventsSpace() noexcept override final {
		assertrx_dbg(writer_);
		return writer_ ? writer_->AvailableEventsSpace() : 0;
	}
	void SendEvent(uint32_t streamsMask, const EventsSerializationOpts &opts, const EventRecord &rec) override final;

protected:
	Writer *writer_ = nullptr;
};
}  // namespace cproto
}  // namespace net
}  // namespace reindexer
