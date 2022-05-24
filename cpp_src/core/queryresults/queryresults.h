#pragma once

#include "client/synccoroqueryresults.h"
#include "core/itemimplrawdata.h"
#include "localqueryresults.h"

namespace reindexer {

class Query;

/// QueryResults is an interface for iterating over documents, returned by Query from Reindexer.<br>
/// QueryResults may contain LocalQueryResults (from local rx node), RemoteQueryResults (from remote node) or distributed results from
/// multiple nodes.
/// QueryResults contains current iterators state, so you can iterate over it forward only once.
/// Random access is supported for LocalQueryResults only.
/// QueryResults cannot be externaly changed or deleted even in case of changing origin data in DB.<br>

class QueryResults {
public:
	enum class Type { None, Local, SingleRemote, MultipleRemote, Mixed };

	QueryResults(int flags = 0);
	~QueryResults();
	QueryResults(QueryResults &&);
	QueryResults(const QueryResults &) = delete;
	QueryResults &operator=(QueryResults &&qr);
	QueryResults &operator=(const QueryResults &qr) = delete;

	void AddQr(LocalQueryResults &&local, int shardID = ShardingKeyType::ProxyOff, bool rebuildMergedData = false);
	void AddQr(client::SyncCoroQueryResults &&remote, int shardID = ShardingKeyType::ProxyOff, bool rebuildMergedData = false);
	void RebuildMergedData();
	size_t Count() const {
		size_t cnt = 0;
		if (local_.has_value()) {
			cnt += local_->qr.Count();
		}
		for (const auto &qrp : remote_) {
			cnt += qrp.qr.Count();
		}
		return cnt;
	}
	size_t TotalCount() const {
		size_t cnt = 0;
		if (local_.has_value()) {
			cnt += local_->qr.TotalCount();
		}
		for (const auto &qrp : remote_) {
			if (qrp.qr.TotalCount() > 0) {
				cnt += size_t(qrp.qr.TotalCount());
			}
		}
		return cnt;
	}

	void Clear();
	Type GetType() const noexcept { return type_; }
	LocalQueryResults &ToLocalQr(bool allowInit) {
		if (allowInit && type_ == Type::None) {
			AddQr(LocalQueryResults());
		}
		if (!IsLocal()) {
			throw Error(errLogic, "QueryResults are not local");
		}
		return local_->qr;
	}
	const LocalQueryResults &ToLocalQr() const {
		if (!IsLocal()) {
			throw Error(errLogic, "QueryResults are not local");
		}
		return local_->qr;
	}
	int Flags() const noexcept { return flags_; }
	const std::string &GetExplainResults() const {
		switch (type_) {
			case Type::Local:
				return local_->qr.GetExplainResults();
			case Type::SingleRemote:
				return remote_[0].qr.GetExplainResults();
			case Type::Mixed:
				if (local_->qr.explainResults.size()) {
					throw Error(errForbidden, "Explain is not supported for distribute queries");
				}
				[[fallthrough]];
			case Type::MultipleRemote:
				for (auto &qrp : remote_) {
					if (qrp.qr.GetExplainResults().size()) {
						throw Error(errForbidden, "Explain is not supported for distribute queries");
					}
				}
				[[fallthrough]];
			default: {
				static std::string kEmpty;
				return kEmpty;
			}
		}
	}
	int GetMergedNSCount() const noexcept;
	const std::vector<AggregationResult> &GetAggregationResults() const;
	h_vector<std::string_view, 1> GetNamespaces() const;
	bool IsCacheEnabled() const noexcept;
	int64_t GetShardingConfigVersion() const noexcept { return shardingConfigVersion_; }
	bool IsLocal() const noexcept { return type_ == Type::Local; }
	bool HasProxiedResults() const noexcept { return type_ == Type::SingleRemote || type_ == Type::Mixed || type_ == Type::MultipleRemote; }
	bool IsDistributed() const noexcept { return type_ == Type::Mixed || type_ == Type::MultipleRemote; }
	bool HaveShardIDs() const noexcept;
	int GetCommonShardID() const;
	PayloadType GetPayloadType(int nsid) const;
	TagsMatcher GetTagsMatcher(int nsid) const;
	// For local qr only
	const FieldsSet &GetFieldsFilter(int nsid) const {
		if (type_ == Type::Local) {
			return local_->qr.getFieldsFilter(nsid);
		}
		static const FieldsSet kEmpty;
		return kEmpty;
	}
	std::shared_ptr<const Schema> GetSchema(int nsid) const {
		if (type_ == Type::Local) {
			return local_->qr.getSchema(nsid);
		}
		return std::shared_ptr<const Schema>();
	}
	bool HaveRank() const;
	bool NeedOutputRank() const;
	bool NeedOutputShardId() const noexcept { return flags_ & kResultsNeedOutputShardId; }
	bool HaveJoined() const;
	void SetQuery(const Query *q);
	bool IsWalQuery() const noexcept { return qData_.has_value() && qData_->isWalQuery; }
	uint32_t GetJoinedField(int parentNsId) const;

	class Iterator {
	public:
		struct ItemRefCache {
			ItemRefCache() = default;
			ItemRefCache(IdType id, uint16_t proc, uint16_t nsid, ItemImpl &&i, bool raw);
			void Clear() noexcept {}

			ItemImplRawData itemImpl;
			WrSerializer wser;
			ItemRef ref;
		};

		Iterator() = default;
		Iterator(const QueryResults *qr, int64_t idx, std::optional<LocalQueryResults::Iterator> localIt)
			: qr_(qr), idx_(idx), localIt_(localIt) {}

		Error GetJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetCJSON(WrSerializer &wrser, bool withHdrLen = true);
		Error GetMsgPack(WrSerializer &wrser, bool withHdrLen = true);
		Error GetProtobuf(WrSerializer &wrser, bool withHdrLen = true);
		// use enableHold = false only if you are sure that the item will be destroyed before the LocalQueryResults
		Item GetItem(bool enableHold = true);
		joins::ItemIterator GetJoined();
		ItemRef GetItemRef(std::vector<ItemRefCache> *storage = nullptr);
		int GetNsID() const {
			auto vit = getVariantIt();
			if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
				return std::get<LocalQueryResults::Iterator>(vit).GetItemRef().Nsid();
			}
			return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetNSID();
		}
		lsn_t GetLSN() const {
			auto vit = getVariantIt();
			if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
				return std::get<LocalQueryResults::Iterator>(vit).GetLSN();
			}
			return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetLSN();
		}
		int GetShardId() const {
			switch (qr_->type_) {
				case Type::None:
					return ShardingKeyType::ProxyOff;
				case Type::Local:
					return qr_->local_->shardID;
				default:;
			}
			validateProxiedIterator();
			if (qr_->curQrId_ < 0) {
				return qr_->local_->shardID;
			}
			return qr_->remote_[size_t(qr_->curQrId_)].shardID;
		}
		bool IsRaw() const {
			auto vit = getVariantIt();
			if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
				return std::get<LocalQueryResults::Iterator>(vit).IsRaw();
			}
			return std::get<client::SyncCoroQueryResults::Iterator>(vit).IsRaw();
		}
		std::string_view GetRaw() const {
			auto vit = getVariantIt();
			if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
				return std::get<LocalQueryResults::Iterator>(vit).GetRaw();
			}
			return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetRaw();
		}
		Iterator &operator++() {
			switch (qr_->type_) {
				case Type::None:
					*this = qr_->end();
					return *this;
				case Type::Local:
					++(*localIt_);
					return *this;
				default:;
			}

			if (idx_ < qr_->lastSeenIdx_) {
				++idx_;	 // This iterator is not valid yet, so simply increment index and do not touch qr's internals
				return *this;
			}

			auto *qr = const_cast<QueryResults *>(qr_);
			bool qrIdWasChanged = false;
			if (qr->curQrId_ < 0) {
				++qr->local_->it;
				++qr->lastSeenIdx_;
				++idx_;
				if (qr->local_->it == qr->local_->qr.end()) {
					qr->curQrId_ = 0;
					qrIdWasChanged = true;
					assertrx(qr_->remote_.size());
				}
			} else if (size_t(qr->curQrId_) < qr_->remote_.size()) {
				auto &remoteQrp = qr->remote_[size_t(qr_->curQrId_)];
				++remoteQrp.it;
				++qr->lastSeenIdx_;
				++idx_;
				if (remoteQrp.it == remoteQrp.qr.end()) {
					++qr->curQrId_;
					qrIdWasChanged = true;
				}
			}
			// Find next qr with items
			while (qrIdWasChanged && size_t(qr_->curQrId_) < qr->remote_.size() &&
				   qr->remote_[size_t(qr_->curQrId_)].it == qr->remote_[size_t(qr_->curQrId_)].qr.end()) {
				++qr->curQrId_;
			}
			return *this;
		}
		Iterator &operator+(uint32_t delta) {
			switch (qr_->type_) {
				case Type::None:
					*this = qr_->end();
					return *this;
				case Type::Local:
					localIt_ = *localIt_ + delta;
					return *this;
				default:;
			}

			if (idx_ < qr_->lastSeenIdx_) {
				const auto readItemsDiff = qr_->lastSeenIdx_ - idx_;
				if (readItemsDiff < delta) {
					delta -= readItemsDiff;
					idx_ = qr_->lastSeenIdx_;
				} else {
					idx_ += delta;
					return *this;
				}
			}

			for (uint32_t i = 0; i < delta; ++i) {
				++(*this);
			}
			return *this;
		}
		Error Status() const {
			switch (qr_->type_) {
				case Type::None:
					return Error();
				case Type::Local:
					return localIt_->Status();
				default:;
			}
			if (qr_->lastSeenIdx_ != idx_) return Error(errNotValid, "Iterator is not valid, it points to already removed data");
			if (qr_->curQrId_ < 0) return Error();
			return qr_->remote_[qr_->curQrId_].it.Status();
		}
		bool operator!=(const Iterator &other) const noexcept { return !(*this == other); }
		bool operator==(const Iterator &other) const noexcept {
			switch (qr_->type_) {
				case Type::None:
					return qr_ == other.qr_;
				case Type::Local:
					return qr_ == other.qr_ && localIt_ == other.localIt_;
				default:;
			}
			return qr_ == other.qr_ && idx_ == other.idx_;
		}
		Iterator &operator*() noexcept { return *this; }

		const QueryResults *qr_;
		int64_t idx_;

	private:
		struct JoinResStorage;
		template <typename DataT>
		struct ItemDataStorage;

		std::variant<LocalQueryResults::Iterator, client::SyncCoroQueryResults::Iterator> getVariantIt() const {
			switch (qr_->type_) {
				case Type::None:
					throw Error(errLogic, "QueryResults are empty");
				case Type::Local:
					return *localIt_;
				default:;
			}
			validateProxiedIterator();

			auto *qr = const_cast<QueryResults *>(qr_);
			if (qr_->curQrId_ < 0) {
				return qr->local_->it;
			}
			if (size_t(qr_->curQrId_) < qr->remote_.size()) {
				return qr->remote_[size_t(qr_->curQrId_)].it;
			}
			throw Error(errNotValid, "Iterator is not valid");
		}
		template <typename QrItT>
		Item getItem(QrItT &, std::unique_ptr<ItemImpl> &&itemImpl, bool convertViaJSON);
		template <typename QrItT>
		Error fillItemImpl(QrItT &, ItemImpl &itemImpl, bool convertViaJSON);
		template <typename QrItT>
		Error getCJSONviaJSON(WrSerializer &wrser, bool withHdrLen, QrItT &);
		void validateProxiedIterator() const {
			if (qr_->lastSeenIdx_ != idx_) {
				throw Error(errLogic, "Distributed and remote query results are 'one pass'. Qr index missmatch");
			}
		}
		template <typename DataT>
		bool checkIfStorageHasSameIdx(const std::shared_ptr<ItemDataStorage<DataT>> &storagePtr) const noexcept {
			return storagePtr.get() && idx_ == storagePtr->idx;
		}
		template <typename DataT>
		void resetStorageData(std::shared_ptr<ItemDataStorage<DataT>> &storagePtr, DataT &&newD) {
			if (storagePtr) {
				*storagePtr = ItemDataStorage<DataT>(idx_, std::move(newD));
			} else {
				storagePtr = std::make_shared<ItemDataStorage<DataT>>(idx_, std::move(newD));
			}
		}
		template <typename DataT>
		void resetStorageData(std::shared_ptr<ItemDataStorage<DataT>> &storagePtr) {
			if (storagePtr) {
				storagePtr->Clear();
				storagePtr->idx = idx_;
			} else {
				storagePtr = std::make_shared<ItemDataStorage<DataT>>(idx_);
			}
		}

		// Iterator for Qr with Type::Local. It may be used to iterate in any direction independantly from main query results
		std::optional<LocalQueryResults::Iterator> localIt_;
		std::shared_ptr<ItemDataStorage<ItemRefCache>> itemRefData_;
		std::shared_ptr<ItemDataStorage<JoinResStorage>> nsJoinRes_;
	};
	using ProxiedRefsStorage = std::vector<Iterator::ItemRefCache>;

	Iterator begin() const {
		switch (type_) {
			case Type::Local:
				return Iterator{this, 0, {local_->qr.begin()}};
			default:
				return Iterator{this, 0, std::nullopt};
		}
	}
	Iterator end() const {
		switch (type_) {
			case Type::None:
				return Iterator{this, 0, std::nullopt};
			case Type::Local:
				return Iterator(this, int64_t(Count()), {local_->qr.end()});
			default:
				return Iterator{this, int64_t(Count()), std::nullopt};
		}
	}

private:
	struct MergedData;
	template <typename QrT>
	struct QrMetaData {
		QrMetaData(QrT &&_qr = QrT()) : qr(std::move(_qr)), it(qr.begin()) {}
		QrMetaData(const QrMetaData &) = delete;
		QrMetaData(QrMetaData &&o) : qr(std::move(o.qr)), it(std::move(o.it)), hasCompatibleTm(o.hasCompatibleTm), shardID(o.shardID) {
			it.qr_ = &qr;
		}
		QrMetaData &operator=(const QrMetaData &) = delete;
		QrMetaData &operator=(QrMetaData &&o) {
			if (this != &o) {
				qr = std::move(o.qr);
				it = std::move(o.it);
				it.qr_ = &qr;
				hasCompatibleTm = o.hasCompatibleTm;
				shardID = o.shardID;
			}
			return *this;
		}

		QrT qr;
		typename QrT::Iterator it;
		bool hasCompatibleTm = false;
		int shardID = ShardingKeyType::ProxyOff;
	};
	const MergedData &getMergedData() const;
	int findFirstQrWithItems() const noexcept;

	struct QueryData {
		bool isWalQuery = false;
		uint16_t joinedSize = 0;
		h_vector<uint16_t, 8> mergedJoinedSizes;
	};

	int64_t shardingConfigVersion_ = -1;
	std::unique_ptr<MergedData> mergedData_;  // Merged data of distributed query results
	std::optional<QrMetaData<LocalQueryResults>> local_;
	std::deque<QrMetaData<client::SyncCoroQueryResults>> remote_;
	int64_t lastSeenIdx_ = 0;
	int curQrId_ = -1;
	Type type_ = Type::None;
	int flags_ = 0;
	std::optional<QueryData> qData_;
};

}  // namespace reindexer
