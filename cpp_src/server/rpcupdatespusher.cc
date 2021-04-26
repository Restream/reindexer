#include "rpcupdatespusher.h"
#include "net/cproto/args.h"
#include "net/cproto/dispatcher.h"

namespace reindexer {
namespace net {
namespace cproto {

RPCUpdatesPusher::RPCUpdatesPusher() : writer_(nullptr), seq_(0) {}

void RPCUpdatesPusher::OnWALUpdate(LSNPair LSNs, std::string_view nsName, const WALRecord &walRec) {
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

	writer_->CallRPC({[](IRPCCall *self, CmdCode &cmd, Args &args) {
						  auto unpacked = SharedWALRecord(self->data_).Unpack();
						  cmd = kCmdUpdates;
						  args = {Arg(unpacked.upstreamLSN), Arg(unpacked.nsName), Arg(unpacked.pwalRec), Arg(unpacked.originLSN)};
					  },
					  pwalRec.packed_

	});
}

void RPCUpdatesPusher::OnUpdatesLost(std::string_view) {}

void RPCUpdatesPusher::OnConnectionState(const Error &) {}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
