#pragma once

#include "core/namespace/snapshot/snapshotrecord.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer {
namespace client {

class Snapshot {
public:
	Snapshot() = default;
	Snapshot(const Snapshot &) = delete;
	Snapshot(Snapshot &&);
	Snapshot &operator=(const Snapshot &) = delete;
	Snapshot &operator=(Snapshot &&);
	~Snapshot();

	class Iterator {
	public:
		Iterator(const Snapshot *sn, size_t idx) : sn_(sn), idx_(idx) {}
		const SnapshotChunk &Chunk() const { return sn_->data_; }
		Iterator &operator++() {
			++idx_;
			if (idx_ < sn_->count_) {
				const_cast<Snapshot *>(sn_)->fetchNext(idx_);
			} else {
				idx_ = sn_->count_;
			}
			return *this;
		}
		bool operator!=(const Iterator &other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator &other) const noexcept { return idx_ == other.idx_; }
		Iterator &operator*() { return *this; }

	private:
		const Snapshot *sn_;
		size_t idx_;
	};
	Iterator begin() { return Iterator{this, 0}; }
	Iterator end() { return Iterator{this, count_}; }
	size_t Size() const noexcept { return count_; }
	bool HasRawData() const noexcept { return rawCount_; }

private:
	friend class CoroRPCClient;
	Snapshot(net::cproto::CoroClientConnection *conn, int id, int64_t count, int64_t rawCount, lsn_t nsVersion, std::string_view data,
			 std::chrono::milliseconds timeout);
	void fetchNext(size_t idx);
	void parseFrom(std::string_view data);

	SnapshotChunk data_;
	int id_ = -1;
	size_t count_ = 0;
	size_t rawCount_ = 0;
	lsn_t nsVersion_;
	net::cproto::CoroClientConnection *conn_;
	std::chrono::milliseconds requestTimeout_;
	friend class Iterator;
};

}  // namespace client
}  // namespace reindexer
