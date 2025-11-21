#pragma once

#include <climits>
#include <set>
#include "client/queryresults.h"
#include "core/itemimplrawdata.h"
#include "core/namespace/incarnationtags.h"
#include "fields_filter.h"
#include "localqueryresults.h"

namespace reindexer_server {
class RPCQrWatcher;
}

namespace reindexer {

class Query;

const std::string_view kWALParamLsn = "lsn";
const std::string_view kWALParamItem = "item";

/// QueryResults is an interface for iterating over documents, returned by Query from Reindexer.<br>
/// QueryResults may contain LocalQueryResults (from local rx node), RemoteQueryResults (from remote node) or distributed results from
/// multiple nodes.
/// QueryResults contains current iterators state, so you can iterate over it forward only once.
/// Random access is supported for LocalQueryResults only.
/// QueryResults cannot be externally changed or deleted even in case of changing origin data in DB.<br>

class [[nodiscard]] QueryResults {
	template <typename DataT>
	struct ItemDataStorage;
	template <typename QrItT>
	static Error fillItemImpl(QrItT&, ItemImpl& itemImpl, bool convertViaJSON);

public:
	enum class [[nodiscard]] Type { None, Local, SingleRemote, MultipleRemote, Mixed };
	struct [[nodiscard]] ItemRefCache {
		ItemRefCache() = default;
		ItemRefCache(IdType id, RankT, uint16_t nsid, ItemImpl&& i, bool raw);
		ItemRefCache(IdType id, uint16_t nsid, ItemImpl&& i, bool raw);
		void Clear() noexcept {}

		ItemImplRawData itemImpl;
		WrSerializer wser;
		ItemRefVariant ref;
	};

	struct JoinResStorage;

private:
	template <typename QrT>
	class [[nodiscard]] QrMetaData {
	public:
		QrMetaData(QrT&& _qr, int shardID);
		QrMetaData(const QrMetaData&) = delete;
		QrMetaData(QrMetaData&& o) noexcept;
		QrMetaData& operator=(const QrMetaData&) = delete;
		QrMetaData& operator=(QrMetaData&& o) noexcept;

		QrT qr;
		typename QrT::Iterator it;
		bool hasCompatibleTm = false;
		void ResetItemRefCache(int64_t idx, ItemRefCache&& newD) const;
		ItemDataStorage<ItemRefCache>& ItemRefData(int64_t idx);
		const std::unique_ptr<ItemDataStorage<ItemRefCache>>& ItemRefData() const noexcept { return itemRefData_; }
		bool CheckIfItemRefStorageHasSameIdx(int64_t idx) const noexcept;
		void ResetJoinStorage(int64_t idx) const;
		const std::unique_ptr<ItemDataStorage<JoinResStorage>>& NsJoinRes() const noexcept { return nsJoinRes_; }
		bool CheckIfNsJoinStorageHasSameIdx(int64_t idx) const noexcept;
		int ShardID() const noexcept { return shardID_; }
		uint32_t ShardIDHash() const noexcept { return shardIDHash_; }

	private:
		int shardID_ = ShardingKeyType::ProxyOff;
		uint32_t shardIDHash_ = 0;
		mutable std::unique_ptr<ItemDataStorage<ItemRefCache>> itemRefData_;
		mutable std::unique_ptr<ItemDataStorage<JoinResStorage>> nsJoinRes_;
	};

	size_t count() const {
		size_t cnt = 0;
		if (local_) {
			cnt += local_->qr.Count();
		}
		for (const auto& qrp : remote_) {
			cnt += qrp->qr.Count();
		}
		return cnt;
	}

public:
	using NamespaceImplPtr = intrusive_ptr<NamespaceImpl>;

	explicit QueryResults(int flags = 0);
	~QueryResults();
	QueryResults(QueryResults&&) noexcept;
	QueryResults(const QueryResults&) = delete;
	QueryResults& operator=(QueryResults&& qr) noexcept;
	QueryResults& operator=(const QueryResults& qr) = delete;

	void AddQr(LocalQueryResults&& local, int shardID = ShardingKeyType::ProxyOff, bool rebuildMergedData = false);
	void AddQr(client::QueryResults&& remote, int shardID = ShardingKeyType::ProxyOff, bool rebuildMergedData = false);
	void RebuildMergedData();
	size_t Count() const {
		const auto cnt = count();
		return std::min<size_t>(limit, offset < cnt ? cnt - offset : 0);
	}
	size_t TotalCount() const {
		size_t cnt = 0;
		if (local_) {
			cnt += local_->qr.TotalCount();
		}
		for (const auto& qrp : remote_) {
			if (qrp->qr.TotalCount() > 0) {
				cnt += size_t(qrp->qr.TotalCount());
			}
		}
		return cnt;
	}

	void Clear();
	Type GetType() const noexcept { return type_; }
	LocalQueryResults& ToLocalQr(bool allowInit) {
		if (allowInit && type_ == Type::None) {
			AddQr(LocalQueryResults());
		}
		if (!IsLocal()) {
			throw Error(errLogic, "QueryResults are not local");
		}
		return local_->qr;
	}
	const LocalQueryResults& ToLocalQr() const {
		if (!IsLocal()) {
			throw Error(errLogic, "QueryResults are not local");
		}
		return local_->qr;
	}
	int Flags() const noexcept { return flags_; }
	const std::string& GetExplainResults() & {
		switch (type_) {
			case Type::Local:
				return local_->qr.GetExplainResults();
			case Type::SingleRemote:
				return remote_[0]->qr.GetExplainResults();
			case Type::Mixed:
				if (local_->qr.explainResults.size()) {
					throw Error(errForbidden, "Explain is not supported for distribute queries");
				}
				[[fallthrough]];
			case Type::MultipleRemote:
				for (auto& qrp : remote_) {
					if (qrp->qr.GetExplainResults().size()) {
						throw Error(errForbidden, "Explain is not supported for distribute queries");
					}
				}
				[[fallthrough]];
			case Type::None:
			default: {
				static std::string kEmpty;
				return kEmpty;
			}
		}
	}
	const std::string& GetExplainResults() && = delete;
	int GetMergedNSCount() const noexcept {
		switch (type_) {
			case Type::None: {
				return 0;
			}
			case Type::Local: {
				return local_->qr.getMergedNSCount();
			}
			case Type::SingleRemote: {
				return remote_[0]->qr.GetMergedNSCount();
			}
			case Type::MultipleRemote:
			case Type::Mixed:
			default:
				return 1;  // No joined/merged nss in distributed qr
		}
	}
	const std::vector<AggregationResult>& GetAggregationResults() &;
	const std::vector<AggregationResult>& GetAggregationResults() && = delete;
	h_vector<std::string_view, 1> GetNamespaces() const;
	NsShardsIncarnationTags GetIncarnationTags() const {
		NsShardsIncarnationTags ret;
		switch (type_) {
			case Type::None:
				return ret;
			case Type::Local: {
				auto localTags = local_->qr.GetIncarnationTags();
				if (localTags.empty()) {
					return ret;
				}
				if (localTags.size() != 1) {
					throw Error(errLogic, "Unexpected shards count in the local query results");
				}
				localTags[0].shardId = local_->ShardID();
				ret.emplace_back(std::move(localTags[0]));
				return ret;
			}
			case Type::SingleRemote: {
				auto& remote = *remote_[0];
				auto& remoteTags = remote.qr.GetIncarnationTags();
				if (remoteTags.empty()) {
					return ret;
				}
				if (remoteTags.size() != 1) {
					throw Error(errLogic, "Unexpected shards count in the remote query results");
				}
				auto& tags = ret.emplace_back(remoteTags[0]);
				tags.shardId = remote.ShardID();
				return ret;
			}
			case Type::Mixed: {
				auto localTags = local_->qr.GetIncarnationTags();
				if (!localTags.empty()) {
					if (localTags.size() != 1) {
						throw Error(errLogic, "Unexpected shards count in the local query results");
					}
					localTags[0].shardId = local_->ShardID();
					ret.emplace_back(std::move(localTags[0]));
				}
			}
				[[fallthrough]];
			case Type::MultipleRemote:
				for (auto& r : remote_) {
					auto& remoteTags = r->qr.GetIncarnationTags();
					if (remoteTags.empty()) {
						continue;
					}
					if (remoteTags.size() != 1) {
						throw Error(errLogic, "Unexpected shards count in the remote query results");
					}
					auto& tags = ret.emplace_back(remoteTags[0]);
					tags.shardId = r->ShardID();
				}
				return ret;
		}
		throw Error(errLogic, "Unknown query results type");
	}
	bool IsCacheEnabled() const noexcept;
	int64_t GetShardingConfigVersion() const noexcept { return shardingConfigVersion_; }
	void SetShardingConfigVersion(int64_t v) noexcept {
		assertrx_dbg(shardingConfigVersion_ == ShardingSourceId::NotSet);  // Do not set version multiple times
		shardingConfigVersion_ = v;
	}
	bool IsLocal() const noexcept { return type_ == Type::Local; }
	bool HasProxiedResults() const noexcept { return type_ == Type::SingleRemote || type_ == Type::Mixed || type_ == Type::MultipleRemote; }
	bool IsDistributed() const noexcept { return type_ == Type::Mixed || type_ == Type::MultipleRemote; }
	bool HaveShardIDs() const noexcept;
	int GetCommonShardID() const;
	PayloadType GetPayloadType(int nsid) const noexcept;
	TagsMatcher GetTagsMatcher(int nsid) const noexcept;
	// For local qr only
	const FieldsFilter& GetFieldsFilter(int nsid) const noexcept {
		if (type_ == Type::Local) {
			return local_->qr.getFieldsFilter(nsid);
		}
		static const FieldsFilter kEmpty;
		return kEmpty;
	}
	std::shared_ptr<const Schema> GetSchema(int nsid) const noexcept {
		if (type_ == Type::Local) {
			return local_->qr.getSchema(nsid);
		}
		return std::shared_ptr<const Schema>();
	}
	bool HaveRank() const noexcept;
	bool NeedOutputRank() const noexcept;
	bool NeedOutputShardId() const noexcept { return flags_ & kResultsNeedOutputShardId; }
	bool HaveJoined() const noexcept;
	void SetQuery(const Query* q);
	bool IsWALQuery() const noexcept { return qData_.has_value() && qData_->isWalQuery; }
	uint32_t GetJoinedField(int parentNsId) const noexcept;
	bool IsRawProxiedBufferAvailable(int flags) const noexcept {
		if (type_ != Type::SingleRemote || !remote_[0]->qr.IsInLazyMode()) {
			return false;
		}

		auto& remote = *remote_[0];
		const auto qrFlags =
			remote.qr.GetFlags() ? (remote.qr.GetFlags() & ~kResultsWithPayloadTypes & ~kResultsWithShardId) : kResultsCJson;
		const auto qrFormat = qrFlags & kResultsFormatMask;
		const auto reqFlags = flags ? (flags & ~kResultsWithPayloadTypes & ~kResultsWithShardId) : kResultsCJson;
		const auto reqFormat = reqFlags & kResultsFormatMask;
		return qrFormat == reqFormat && (qrFlags & reqFlags) == reqFlags;
	}
	bool GetRawProxiedBuffer(client::ParsedQrRawBuffer& out) { return remote_[0]->qr.GetRawBuffer(out); }
	void FetchRawBuffer(int flgs, int off, int lim) {
		if (!IsRawProxiedBufferAvailable(flgs)) {
			throw Error(errLogic, "Raw buffer is not available");
		}
		remote_[0]->qr.FetchNextResults(flgs, off, lim);
	}
	void SetFlags(int flags) {
		if (GetType() != Type::None) {
			throw Error(errLogic, "Unable to set flags for non-empty query results");
		}
		flags_ = flags;
	}

	class [[nodiscard]] Iterator {
	public:
		Iterator(const QueryResults* qr, int64_t idx, std::optional<LocalQueryResults::ConstIterator> localIt)
			: qr_(qr), idx_(idx), localIt_(std::move(localIt)) {}

		Error GetJSON(WrSerializer& wrser, bool withHdrLen = true);
		Expected<std::string> GetJSON();
		Error GetCJSON(WrSerializer& wrser, bool withHdrLen = true) noexcept;
		Error GetMsgPack(WrSerializer& wrser, bool withHdrLen = true) noexcept;
		Error GetProtobuf(WrSerializer& wrser) noexcept;
		Error GetCSV(WrSerializer& wrser, CsvOrdering& ordering) noexcept;

		// use enableHold = false only if you are sure that the item will be destroyed before the LocalQueryResults
		Item GetItem(bool enableHold = true);
		joins::ItemIterator GetJoined(std::vector<ItemRefCache>* storage = nullptr);
		ItemRef GetItemRef(std::vector<ItemRefCache>* storage = nullptr);
		ItemRefRanked GetItemRefRanked(std::vector<ItemRefCache>* storage = nullptr);
		bool IsRanked() const noexcept { return qr_->HaveRank(); }
		int GetNsID() const {
			struct {
				int operator()(LocalQueryResults::ConstIterator&& it) const noexcept { return it.GetItemRef().Nsid(); }
				int operator()(client::QueryResults::Iterator&& it) const { return it.GetNSID(); }
			} constexpr static nsIdGetter;
			return std::visit(nsIdGetter, getVariantIt());
		}
		lsn_t GetLSN() const {
			struct {
				lsn_t operator()(LocalQueryResults::ConstIterator&& it) const noexcept { return it.GetLSN(); }
				lsn_t operator()(client::QueryResults::Iterator&& it) const { return it.GetLSN(); }
			} constexpr static lsnGetter;
			return std::visit(lsnGetter, getVariantIt());
		}
		int GetShardId() const {
			switch (qr_->type_) {
				case Type::None:
					return ShardingKeyType::ProxyOff;
				case Type::Local:
					return qr_->local_->ShardID();
				case Type::SingleRemote:
				case Type::MultipleRemote:
				case Type::Mixed:
					break;
			}
			validateProxiedIterator();
			if (qr_->curQrId_ < 0) {
				return qr_->local_->ShardID();
			}
			return qr_->remote_[size_t(qr_->curQrId_)]->ShardID();
		}
		bool IsRaw() const {
			struct {
				bool operator()(LocalQueryResults::ConstIterator&& it) const noexcept { return it.IsRaw(); }
				bool operator()(client::QueryResults::Iterator&& it) const { return it.IsRaw(); }
			} constexpr static rawTester;
			return std::visit(rawTester, getVariantIt());
		}
		std::string_view GetRaw() const {
			struct {
				std::string_view operator()(LocalQueryResults::ConstIterator&& it) const noexcept { return it.GetRaw(); }
				std::string_view operator()(client::QueryResults::Iterator&& it) const { return it.GetRaw(); }
			} constexpr static rawGetter;
			return std::visit(rawGetter, getVariantIt());
		}
		size_t GetJoinedField() const { return qr_->GetJoinedField(GetNsID()); }
		Iterator& operator++();
		Iterator& operator+(uint32_t delta) {
			switch (qr_->type_) {
				case Type::None:
					*this = qr_->end();
					return *this;
				case Type::Local:
					localIt_ = *localIt_ + delta;
					return *this;
				case Type::SingleRemote:
				case Type::MultipleRemote:
				case Type::Mixed:
					break;
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
				case Type::SingleRemote:
				case Type::MultipleRemote:
				case Type::Mixed:
					break;
			}
			if (qr_->lastSeenIdx_ != idx_) {
				return Error(errNotValid, "Iterator is not valid, it points to already removed data");
			}
			if (qr_->curQrId_ < 0) {
				return Error();
			}
			return qr_->remote_[qr_->curQrId_]->it.Status();
		}
		bool operator!=(const Iterator& other) const noexcept { return !(*this == other); }
		bool operator==(const Iterator& other) const noexcept {
			switch (qr_->type_) {
				case Type::None:
					return qr_ == other.qr_;
				case Type::Local:
					return qr_ == other.qr_ && localIt_ == other.localIt_;
				case Type::SingleRemote:
				case Type::MultipleRemote:
				case Type::Mixed:
					break;
			}
			return qr_ == other.qr_ && idx_ == other.idx_;
		}
		Iterator& operator*() noexcept { return *this; }

	private:
		template <bool isRanked>
		auto getItemRef(std::vector<ItemRefCache>* storage);

		std::variant<QrMetaData<LocalQueryResults>*, QrMetaData<client::QueryResults>*> getVariantResult() const {
			switch (qr_->type_) {
				case Type::None:
					throw Error(errLogic, "QueryResults are empty");
				case Type::Local:
					throw Error(errLogic, "QueryResults are local");
				case Type::SingleRemote:
				case Type::MultipleRemote:
				case Type::Mixed:
					break;
			}
			validateProxiedIterator();

			auto* qr = const_cast<QueryResults*>(qr_);
			if (qr_->curQrId_ < 0) {
				return &(*qr->local_);
			}
			if (size_t(qr_->curQrId_) < qr->remote_.size()) {
				return qr->remote_[size_t(qr_->curQrId_)].get();
			}
			throw Error(errNotValid, "Iterator is not valid");
		}

		std::variant<LocalQueryResults::ConstIterator, client::QueryResults::Iterator> getVariantIt() const {
			switch (qr_->type_) {
				case Type::None:
					throw Error(errLogic, "QueryResults are empty");
				case Type::Local:
					assertrx_throw(localIt_.has_value());
					return *localIt_;
				case Type::SingleRemote:
				case Type::MultipleRemote:
				case Type::Mixed:
					break;
			}
			validateProxiedIterator();

			auto* qr = const_cast<QueryResults*>(qr_);
			if (qr_->curQrId_ < 0) {
				return qr->local_->it;
			}
			if (size_t(qr_->curQrId_) < qr->remote_.size()) {
				return qr->remote_[size_t(qr_->curQrId_)]->it;
			}
			throw Error(errNotValid, "Iterator is not valid");
		}
		template <typename QrItT>
		Item getItem(QrItT&, std::unique_ptr<ItemImpl>&& itemImpl, bool convertViaJSON);
		template <typename QrItT>
		Item getItem(QrItT&, std::unique_ptr<ItemImpl>&& itemImpl, const FieldsFilter&, bool convertViaJSON);
		template <typename QrItT>
		Error getCJSONviaJSON(WrSerializer& wrser, bool withHdrLen, QrItT&);
		void validateProxiedIterator() const {
			if (qr_->lastSeenIdx_ != idx_) {
				throw Error(errLogic, "Distributed and remote query results are 'one pass'. Qr index missmatch");
			}
		}

		const QueryResults* qr_{nullptr};
		int64_t idx_{0};
		// An iterator for Qr of Type::Local. Can be used to iterate in either direction regardless of the results of the underlying query
		std::optional<LocalQueryResults::ConstIterator> localIt_;
	};
	using ProxiedRefsStorage = std::vector<ItemRefCache>;

	Iterator begin() const {
		if (!begin_.it) {
			beginImpl();
		}
		return *begin_.it;	// -V1007
	}
	Iterator end() const {
		if (type_ == Type::None) {
			return Iterator{this, 0, std::nullopt};
		} else {
			const int64_t n = std::min<size_t>(count(), limit < UINT_MAX ? limit + offset : UINT_MAX);
			if (type_ == Type::Local) {
				return Iterator{this, n, {local_->qr.begin() + n}};
			} else {
				return Iterator{this, n, std::nullopt};
			}
		}
	}
	void SetOrdering(const Query&, const NamespaceImpl&, const RdxContext&);

private:
	struct MergedData;
	class Comparator;
	class CompositeFieldForceComparator;
	const MergedData& getMergedData() const;
	int findFirstQrWithItems(int minShardId = std::numeric_limits<int>().min());
	bool ordering() const noexcept;
	void beginImpl() const;
	void setFlags(int flags) noexcept { flags_ = flags; }

	struct [[nodiscard]] QueryData {
		bool isWalQuery = false;
		uint16_t joinedSize = 0;
		h_vector<uint16_t, 8> mergedJoinedSizes;
	};

	int64_t shardingConfigVersion_ = ShardingSourceId::NotSet;
	std::unique_ptr<MergedData> mergedData_;  // Merged data of distributed query results
	std::optional<QrMetaData<LocalQueryResults>> local_;
	// We could use std::deque to make QrMetaData non-movable, but deque's default constructor performs allocation in GCC's implementation
	std::vector<std::unique_ptr<QrMetaData<client::QueryResults>>> remote_;
	int64_t lastSeenIdx_ = 0;
	int curQrId_ = -1;
	Type type_ = Type::None;
	int flags_ = 0;
	std::optional<QueryData> qData_;
	std::unique_ptr<std::set<int, Comparator>> orderedQrs_;
	struct [[nodiscard]] BeginContainer {
		BeginContainer() = default;
		BeginContainer(BeginContainer&&) noexcept {}
		BeginContainer& operator=(BeginContainer&&) noexcept {
			it = std::nullopt;
			return *this;
		}
		std::optional<Iterator> it;
	};
	mutable BeginContainer begin_;
	unsigned offset{0};
	unsigned limit{UINT_MAX};
	friend InternalRdxContext;
	friend class reindexer_server::RPCQrWatcher;
	std::optional<RdxActivityContext> activityCtx_;
};

}  // namespace reindexer
