#include "events/iexternal_listener.h"
#include "net/cproto/dispatcher.h"

namespace reindexer::net::cproto {

class Args;
class Writer;
class [[nodiscard]] RPCEventsPusher : public reindexer::IEventsObserver {
public:
	void SetWriter(Writer* writer) noexcept { writer_ = writer; }

	size_t AvailableEventsSpace() noexcept override final {
		assertrx_dbg(writer_);
		return writer_ ? writer_->AvailableEventsSpace() : 0;
	}
	void SendEvent(uint32_t streamsMask, const EventsSerializationOpts& opts, const EventRecord& rec) override final;

protected:
	Writer* writer_ = nullptr;
};

}  // namespace reindexer::net::cproto
