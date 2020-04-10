#include "rpcupdatespusher.h"
#include "net/cproto/args.h"
#include "net/cproto/dispatcher.h"

namespace reindexer {
namespace net {
namespace cproto {

RPCUpdatesPusher::RPCUpdatesPusher() : writer_(nullptr), seq_(0) {}

void RPCUpdatesPusher::OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &walRec) {
	SharedWALRecord pwalRec;
	if (filter_) {
		WALRecord rec = walRec;
		(void)walRec;  // walRec should not be used after this moment
		if (filter_(rec)) {
			return;
		}
		pwalRec = rec.GetShared(lsn, nsName);
	} else {
		pwalRec = walRec.GetShared(lsn, nsName);
	}

	writer_->CallRPC({[](IRPCCall *self, CmdCode &cmd, Args &args) {
						  auto unpacked = SharedWALRecord(self->data_).Unpack();
						  cmd = kCmdUpdates;
						  args = {Arg(unpacked.lsn), Arg(unpacked.nsName), Arg(unpacked.pwalRec)};
					  },
					  pwalRec.packed_

	});
}

void RPCUpdatesPusher::OnConnectionState(const Error &) {}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
