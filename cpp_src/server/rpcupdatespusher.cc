#include "rpcupdatespusher.h"
#include "net/cproto/args.h"
#include "net/cproto/dispatcher.h"

namespace reindexer {
namespace net {
namespace cproto {

RPCUpdatesPusher::RPCUpdatesPusher() : writer_(nullptr), seq_(0) {}

void RPCUpdatesPusher::OnWALUpdate(int64_t lsn, string_view nsName, const WALRecord &walRec) {
	PackedWALRecord pwalRec;
	if (filter_) {
		WALRecord rec = walRec;
		(void)walRec;  // walRec should not be used after this moment
		if (filter_(rec)) {
			return;
		}
		pwalRec.Pack(rec);
	} else {
		pwalRec.Pack(walRec);
	}
	string_view pwal(reinterpret_cast<char *>(pwalRec.data()), pwalRec.size());

	writer_->CallRPC(kCmdUpdates, {Arg(lsn), Arg(p_string(&nsName)), Arg(p_string(&pwal))});
}

void RPCUpdatesPusher::OnConnectionState(const Error &) {}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
