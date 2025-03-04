
#include "rpcupdatespusher.h"
#include "events/serializer.h"
#include "net/cproto/args.h"

#ifdef REINDEX_WITH_V3_FOLLOWERS

#include "net/cproto/dispatcher.h"

namespace reindexer {
namespace net {
namespace cproto {

RPCUpdatesPusherV3::RPCUpdatesPusherV3() : writer_(nullptr), seq_(0) {}

void RPCUpdatesPusherV3::OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord& walRec) {
	SharedWALRecord pwalRec;
	if (filter_) {
		WALRecord rec = walRec;
		(void)walRec;  // walRec should not be used after this moment
		if (filter_(rec)) {
			return;
		}
		pwalRec = rec.GetShared(int64_t(LSNs.upstreamLSN_), int64_t(LSNs.originLSN_), nsName);
	} else {
		pwalRec = walRec.GetShared(int64_t(LSNs.upstreamLSN_), int64_t(LSNs.originLSN_), nsName);
	}

	writer_->CallRPC({[](IRPCCall* self, CmdCode& cmd, std::string_view& ns, Args& args) {
						  auto unpacked = SharedWALRecord(self->data_).Unpack();
						  cmd = kCmdUpdates;
						  args = {Arg(unpacked.upstreamLSN), Arg(unpacked.nsName), Arg(unpacked.pwalRec), Arg(unpacked.originLSN)};
						  ns = std::string_view(args[1]);
					  },
					  pwalRec.packed_

	});
}

void RPCUpdatesPusherV3::OnUpdatesLost(std::string_view) {}

void RPCUpdatesPusherV3::OnConnectionState(const Error&) {}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer

#endif	// REINDEX_WITH_V3_FOLLOWERS

namespace reindexer {
namespace net {
namespace cproto {

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

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
