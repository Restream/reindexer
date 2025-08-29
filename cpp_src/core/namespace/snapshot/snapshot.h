#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/namespace/namespacestat.h"
#include "core/queryresults/localqueryresults.h"
#include "snapshotrecord.h"

namespace reindexer {

class [[nodiscard]] Snapshot {
public:
	Snapshot() = default;
	Snapshot(TagsMatcher tm, lsn_t nsVersion, uint64_t expectedDataHash, uint64_t expectedDataCount, ClusterOperationStatus clusterStatus);
	Snapshot(PayloadType pt, TagsMatcher tm, lsn_t nsVersion, lsn_t lastLsn, uint64_t expectedDataHash, uint64_t expectedDataCount,
			 ClusterOperationStatus clusterStatus, LocalQueryResults&& wal, LocalQueryResults&& raw = LocalQueryResults());
	Snapshot(const Snapshot&) = delete;
	Snapshot(Snapshot&&) = default;
	Snapshot& operator=(const Snapshot&) = delete;
	Snapshot& operator=(Snapshot&&) noexcept;
	~Snapshot();

	class [[nodiscard]] Iterator {
	public:
		Iterator(Iterator&&) = default;
		Iterator(const Iterator& it) noexcept : sn_(it.sn_), idx_(it.idx_) {}
		Iterator(const Snapshot* sn, size_t idx) : sn_(sn), idx_(idx) {}
		Iterator& operator=(const Iterator& it) noexcept {
			sn_ = it.sn_;
			idx_ = it.idx_;
			return *this;
		}
		Iterator& operator=(Iterator&& it) = default;
		SnapshotChunk Chunk() const;
		Iterator& operator++() noexcept;
		Iterator& operator+(size_t delta) noexcept;
		bool operator!=(const Iterator& other) const noexcept { return idx_ != other.idx_; }
		bool operator==(const Iterator& other) const noexcept { return idx_ == other.idx_; }
		Iterator& operator*() { return *this; }

	private:
		const Snapshot* sn_;
		size_t idx_;
		mutable WrSerializer ser_;
	};
	Iterator begin() const { return Iterator{this, 0}; }
	Iterator end() const { return Iterator{this, rawData_.Size() + walData_.Size()}; }
	size_t Size() const noexcept { return rawData_.Size() + walData_.Size(); }
	size_t RawDataSize() const noexcept { return rawData_.Size(); }
	bool HasRawData() const noexcept { return rawData_.Size(); }
	uint64_t ExpectedDataHash() const noexcept { return expectedDataHash_; }
	uint64_t ExpectedDataCount() const noexcept { return expectedDataCount_; }
	ClusterOperationStatus ClusterOperationStat() const noexcept { return clusterOperationStatus_; }
	lsn_t LastLSN() const noexcept { return lastLsn_; }
	lsn_t NsVersion() const noexcept { return nsVersion_; }
	std::string Dump();

private:
	struct [[nodiscard]] Chunk {
		bool txChunk = false;
		std::vector<ItemRef> items;
	};

	class [[nodiscard]] ItemsContainer {
	public:
		void AddItem(ItemRef&& item);
		void SetVectorsHolder(FloatVectorsHolderMap&& map) noexcept { vectorsHolder_ = std::move(map); }
		size_t Size() const noexcept { return data_.size(); }
		size_t ItemsCount() const noexcept { return itemsCount_; }
		const std::vector<Chunk>& Data() const noexcept { return data_; }
		void LockItems(const PayloadType& pt, bool lock);

	private:
		void lockItem(const PayloadType& pt, ItemRef& itemref, bool lock) noexcept;

		std::vector<Chunk> data_;
		size_t itemsCount_ = 0;
		FloatVectorsHolderMap vectorsHolder_;
	};

	void addRawData(LocalQueryResults&&);
	void addWalData(LocalQueryResults&&);
	void appendQr(ItemsContainer& container, LocalQueryResults&& qr);
	void lockItems(bool lock);
	PayloadValue createTmItem() const;

	PayloadType pt_;
	TagsMatcher tm_;
	ItemsContainer rawData_;
	ItemsContainer walData_;
	uint64_t expectedDataHash_ = 0;
	uint64_t expectedDataCount_ = 0;
	ClusterOperationStatus clusterOperationStatus_;
	lsn_t lastLsn_;
	lsn_t nsVersion_;
	friend class Iterator;
};

}  // namespace reindexer
