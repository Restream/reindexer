#pragma once

#include "core/namespace/namespacestat.h"
#include "core/namespace/snapshot/snapshotrecord.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer {
namespace client {

class [[nodiscard]] Snapshot {
public:
	Snapshot() = default;
	Snapshot(const Snapshot&) = delete;
	Snapshot(Snapshot&&) noexcept;
	Snapshot& operator=(const Snapshot&) = delete;
	Snapshot& operator=(Snapshot&&) noexcept;
	~Snapshot();

	class [[nodiscard]] Iterator {
	public:
		Iterator(const Snapshot* sn, size_t idx) noexcept : sn_(sn), idx_(idx) {}
		const SnapshotChunk& Chunk() const noexcept { return sn_->i_.data_; }
		Iterator& operator++() {
			++idx_;
			if (idx_ < sn_->i_.count_) {
				const_cast<Snapshot*>(sn_)->fetchNext(idx_);
			} else {
				idx_ = sn_->i_.count_;
			}
			return *this;
		}
		bool operator!=(const Iterator& other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator& other) const noexcept { return idx_ == other.idx_; }
		Iterator& operator*() { return *this; }

	private:
		const Snapshot* sn_;
		size_t idx_;
	};
	Iterator begin() noexcept { return Iterator{this, 0}; }
	Iterator end() noexcept { return Iterator{this, i_.count_}; }
	size_t Size() const noexcept { return i_.count_; }
	bool HasRawData() const noexcept { return i_.rawCount_; }
	void ClusterOperationStat(ClusterOperationStatus&& clusterStatus) noexcept { i_.clusterStatus_ = std::move(clusterStatus); }
	std::optional<ClusterOperationStatus> ClusterOperationStat() const noexcept { return i_.clusterStatus_; }

private:
	friend class RPCClient;
	Snapshot(net::cproto::CoroClientConnection* conn, int id, int64_t count, int64_t rawCount, lsn_t nsVersion, std::string_view data,
			 std::chrono::milliseconds timeout);
	void fetchNext(size_t idx);
	void parseFrom(std::string_view data);
	bool holdsRemoteData() const noexcept { return i_.conn_ && i_.id_ >= 0; }
	void setClosed() noexcept {
		i_.conn_ = nullptr;
		i_.id_ = -1;
	}

	struct [[nodiscard]] Impl {
		Impl() = default;
		Impl(net::cproto::CoroClientConnection* conn, int id, int64_t count, int64_t rawCount, lsn_t nsVersion,
			 std::chrono::milliseconds timeout) noexcept;

		SnapshotChunk data_;
		int id_ = -1;
		size_t count_ = 0;
		size_t rawCount_ = 0;
		lsn_t nsVersion_;
		net::cproto::CoroClientConnection* conn_;
		std::chrono::milliseconds requestTimeout_;
		steady_clock_w::time_point sessionTs_;
		std::optional<ClusterOperationStatus> clusterStatus_;
	};

	Impl i_;

	friend class Iterator;
};

}  // namespace client
}  // namespace reindexer
