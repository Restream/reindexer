#include "snapshot.h"

namespace reindexer {
namespace client {

Snapshot::Snapshot(Snapshot&& o) noexcept : i_(std::move(o.i_)) {
	o.i_.count_ = 0;
	o.i_.rawCount_ = 0;
	o.i_.nsVersion_ = lsn_t();
	o.setClosed();
}

Snapshot& Snapshot::operator=(Snapshot&& o) noexcept {
	if (&o != this) {
		i_ = std::move(o.i_);
		o.i_.count_ = 0;
		o.i_.rawCount_ = 0;
		o.i_.nsVersion_ = lsn_t();
		o.setClosed();
	}
	return *this;
}

Snapshot::~Snapshot() {
	if (holdsRemoteData()) {
		try {
			// Just close snapshot on server side
			std::ignore = i_.conn_->Call({net::cproto::kCmdFetchSnapshot, i_.requestTimeout_, std::chrono::milliseconds(0), lsn_t(), -1,
										  ShardingKeyType::NotSetShard, nullptr, false, i_.sessionTs_},
										 i_.id_, int64_t(-1));
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: unexpected exception in ~Snapshot: %s\n", e.what());
			assertrx_dbg(false);
		}
	}
}

Snapshot::Snapshot(net::cproto::CoroClientConnection* conn, int id, int64_t count, int64_t rawCount, lsn_t nsVersion, std::string_view data,
				   std::chrono::milliseconds timeout)
	: i_(conn, id, count, rawCount, nsVersion, timeout) {
	if (id < 0) {
		throw Error(errLogic, "Unexpectd snapshot id: {}", id);
	}
	if (count < 0) {
		throw Error(errLogic, "Unexpectd snapshot size: {}", count);
	}

	if (i_.count_ > 0) {
		parseFrom(data);
		if (i_.count_ == 1) {
			i_.id_ = -1;
		}
	}
}

void Snapshot::fetchNext(size_t idx) {
	if (!holdsRemoteData()) {
		throw Error(errLogic, "Snapshot: client snapshot does not owns any data");
	}
	auto ret = i_.conn_->Call({net::cproto::kCmdFetchSnapshot, i_.requestTimeout_, std::chrono::milliseconds(0), lsn_t(), -1,
							   ShardingKeyType::NotSetShard, nullptr, false, i_.sessionTs_},
							  i_.id_, int64_t(idx));
	if (!ret.Status().ok()) {
		if (ret.Status().code() == errNetwork) {
			setClosed();
		}
		throw ret.Status();
	}
	auto args = ret.GetArgs(1);
	parseFrom(p_string(args[0]));

	if (idx + 1 == i_.count_) {
		setClosed();
	}
}

void Snapshot::parseFrom(std::string_view data) {
	SnapshotChunk ch;
	Serializer ser(data);
	ch.Deserialize(ser);
	i_.data_ = std::move(ch);
}

Snapshot::Impl::Impl(net::cproto::CoroClientConnection* conn, int id, int64_t count, int64_t rawCount, lsn_t nsVersion,
					 std::chrono::milliseconds timeout) noexcept
	: id_(id), count_(count), rawCount_(rawCount), nsVersion_(nsVersion), conn_(conn), requestTimeout_(timeout) {
	assert(conn_);
	const auto sessionTs = conn_->LoginTs();
	if (sessionTs.has_value()) {
		sessionTs_ = sessionTs.value();
	}
}

}  // namespace client
}  // namespace reindexer
