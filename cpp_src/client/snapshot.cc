#include "snapshot.h"

namespace reindexer {
namespace client {

Snapshot::Snapshot(Snapshot&& o)
	: data_(std::move(o.data_)),
	  id_(o.id_),
	  count_(o.count_),
	  rawCount_(o.rawCount_),
	  nsVersion_(o.nsVersion_),
	  conn_(o.conn_),
	  requestTimeout_(o.requestTimeout_) {
	o.count_ = 0;
	o.rawCount_ = 0;
	o.nsVersion_ = lsn_t();
	o.conn_ = nullptr;
}

Snapshot& Snapshot::operator=(Snapshot&& o) {
	data_ = std::move(o.data_);
	id_ = o.id_;
	count_ = o.count_;
	rawCount_ = o.rawCount_;
	nsVersion_ = o.nsVersion_;
	conn_ = o.conn_;
	requestTimeout_ = o.requestTimeout_;
	o.count_ = 0;
	o.rawCount_ = 0;
	o.nsVersion_ = lsn_t();
	o.conn_ = nullptr;
	return *this;
}

Snapshot::~Snapshot() {
	if (conn_ && id_ > 0) {
		// Just close snapshot on server side
		conn_->Call(
			{net::cproto::kCmdFetchSnapshot, requestTimeout_, std::chrono::milliseconds(0), lsn_t(), -1, IndexValueType::NotSet, nullptr},
			id_, int64_t(-1));
	}
}

Snapshot::Snapshot(net::cproto::CoroClientConnection* conn, int id, int64_t count, int64_t rawCount, lsn_t nsVersion, std::string_view data,
				   std::chrono::milliseconds timeout)
	: id_(id), count_(count), rawCount_(rawCount), nsVersion_(nsVersion), conn_(conn), requestTimeout_(timeout) {
	if (id < 0) {
		throw Error(errLogic, "Unexpectd snapshot id: %d", id);
	}
	if (count < 0) {
		throw Error(errLogic, "Unexpectd snapshot size: %d", count);
	}

	if (count_ > 0) {
		parseFrom(data);
		if (count_ == 1) {
			id_ = -1;
		}
	}
}

void Snapshot::fetchNext(size_t idx) {
	if (!conn_) {
		throw Error(errLogic, "Snapshot: connection is nullptr");
	}
	auto ret = conn_->Call(
		{net::cproto::kCmdFetchSnapshot, requestTimeout_, std::chrono::milliseconds(0), lsn_t(), -1, IndexValueType::NotSet, nullptr}, id_,
		int64_t(idx));
	if (!ret.Status().ok()) {
		if (ret.Status().code() == errNetwork) {
			conn_ = nullptr;
		}
		throw ret.Status();
	}
	auto args = ret.GetArgs(1);
	parseFrom(p_string(args[0]));

	if (idx + 1 == count_) {
		id_ = -1;
	}
}

void Snapshot::parseFrom(std::string_view data) {
	SnapshotChunk ch;
	Serializer ser(data);
	ch.Deserialize(ser);
	data_ = std::move(ch);
}

}  // namespace client
}  // namespace reindexer
