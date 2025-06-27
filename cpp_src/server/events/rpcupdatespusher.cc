
#include "rpcupdatespusher.h"
#include "events/serializer.h"

namespace reindexer::net::cproto {

void RPCEventsPusher::SendEvent(uint32_t streamsMask, const EventsSerializationOpts& opts, const EventRecord& rec) {
	assertrx_dbg(writer_);
	if (!writer_) {
		return;
	}

	constexpr static size_t kChunkCapacity = 512;
	WrSerializer wser(chunk{kChunkCapacity});
	UpdateSerializer user(wser);

	writer_->SendEvent(user.Serialize(streamsMask, opts, rec));
}

}  // namespace reindexer::net::cproto
