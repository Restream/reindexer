#include "queryresults.h"
#include "core/nsselecter/joinedselector.h"
#include "core/query/query.h"
#include "core/sorting/sortexpression.h"
#include "core/type_consts.h"
#include "joinresults.h"
#include "tools/catch_and_return.h"
#include "tools/float_comparison.h"

namespace reindexer {

struct [[nodiscard]] QueryResults::MergedData {
	MergedData(const std::string& ns, bool _haveRank, bool _needOutputRank)
		: pt(ns, {PayloadFieldType(KeyValueType::String{}, "-tuple", {}, IsArray_False)}),
		  haveRank(_haveRank),
		  needOutputRank(_needOutputRank) {}

	std::string nsName;
	PayloadType pt;
	TagsMatcher tm;	 // Merged tagsmatcher currently does not have PayloadType and can not convert tags to indexes
	std::vector<AggregationResult> aggregationResults;
	bool haveRank = false;
	bool needOutputRank = false;
};

struct [[nodiscard]] QueryResults::JoinResStorage {
	void Clear() {
		jr.Clear();
		joinedRawData.clear();
	}

	joins::NamespaceResults jr;
	h_vector<ItemImplRawData, 1> joinedRawData;
};

template <typename DataT>
struct [[nodiscard]] QueryResults::ItemDataStorage {
	ItemDataStorage(int64_t newIdx, DataT&& d = DataT()) : idx(newIdx), data(std::move(d)) {}
	void Clear() {
		data.Clear();
		idx = -1;
	}

	int64_t idx;
	DataT data;
};

QueryResults::QueryResults(int flags) : flags_(flags) {}

QueryResults::~QueryResults() = default;
QueryResults::QueryResults(QueryResults&&) noexcept = default;

QueryResults& QueryResults::operator=(QueryResults&& qr) noexcept {
	if (this != &qr) {
		shardingConfigVersion_ = qr.shardingConfigVersion_;
		mergedData_ = std::move(qr.mergedData_);
		local_ = std::move(qr.local_);
		remote_ = std::move(qr.remote_);
		lastSeenIdx_ = qr.lastSeenIdx_;
		curQrId_ = qr.curQrId_;
		type_ = qr.type_;
		flags_ = qr.flags_;
		qData_ = std::move(qr.qData_);
		orderedQrs_ = std::move(qr.orderedQrs_);
		begin_.it = std::nullopt;
		offset = qr.offset;
		limit = qr.limit;

		activityCtx_.reset();
		if (qr.activityCtx_) {
			activityCtx_.emplace(std::move(*qr.activityCtx_));
			qr.activityCtx_.reset();
		}
	}
	return *this;
}

void QueryResults::AddQr(LocalQueryResults&& local, int shardID, bool buildMergedData) {
	if (local_) {
		throw Error(errLogic, "Query results already have encapsulated local query results");
	}
	if (lastSeenIdx_ > 0) {
		throw Error(
			errLogic,
			"Unable to add new local query results to general query results, because it was already read by someone (last seen idx: {})",
			lastSeenIdx_);
	}
	if (type_ == Type::None || local.Count() != 0 || local.TotalCount() != 0 || !local.GetAggregationResults().empty()) {
		begin_.it = std::nullopt;
		if (NeedOutputShardId()) {
			local.SetOutputShardId(shardID);
		}
		local_.emplace(std::move(local), shardID);
		switch (type_) {
			case Type::None:
				type_ = Type::Local;
				local_->hasCompatibleTm = true;
				break;
			case Type::SingleRemote:
			case Type::MultipleRemote:
				type_ = Type::Mixed;
				break;
			case Type::Local:
			case Type::Mixed:
				break;
		}
	}
	if (buildMergedData) {
		RebuildMergedData();
	}
	curQrId_ = findFirstQrWithItems();
}

void QueryResults::AddQr(client::QueryResults&& remote, int shardID, bool buildMergedData) {
	if (lastSeenIdx_ > 0) {
		throw Error(
			errLogic,
			"Unable to add new remote query results to general query results, because it was already read by someone (last seen idx: {})",
			lastSeenIdx_);
	}
	if (type_ == Type::None || remote.Count() != 0 || remote.TotalCount() != 0 || !remote.GetAggregationResults().empty()) {
		begin_.it = std::nullopt;
		if (remote_.empty()) {
			remote_.reserve(16u);
		}
		remote_.emplace_back(std::make_unique<QrMetaData<client::QueryResults>>(std::move(remote), shardID));
		switch (type_) {
			case Type::None:
				type_ = Type::SingleRemote;
				remote_[0]->hasCompatibleTm = true;
				break;
			case Type::SingleRemote:
				type_ = Type::MultipleRemote;
				remote_[0]->hasCompatibleTm = false;
				break;
			case Type::Local:
				type_ = Type::Mixed;
				assertrx_dbg(local_);
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				local_->hasCompatibleTm = false;
				break;
			case Type::MultipleRemote:
			case Type::Mixed:
				break;
		}
	}
	if (buildMergedData) {
		RebuildMergedData();
	}
	curQrId_ = findFirstQrWithItems();
}

void QueryResults::RebuildMergedData() {
	try {
		mergedData_.reset();
		if (type_ == Type::Mixed) {
			assertrx(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			const auto nss = local_->qr.GetNamespaces();
			if (nss.size() > 1) {
				throw Error(errLogic, "Local query result has {} namespaces, but distributed query results may have only 1", nss.size());
			}
			mergedData_ = std::make_unique<MergedData>(std::string(nss[0]), local_->qr.haveRank, local_->qr.needOutputRank);
			const auto& agg = local_->qr.GetAggregationResults();
			for (const auto& a : agg) {
				if (a.GetType() == AggAvg || a.GetType() == AggFacet || a.GetType() == AggDistinct || a.GetType() == AggUnknown) {
					throw Error(errLogic, "Local query result (within distributed results) has unsupported aggregations");
				}
			}
			mergedData_->aggregationResults = agg;
		} else if (type_ != Type::MultipleRemote) {
			return;
		}

		assertrx(remote_.size());
		for (auto& qrp : remote_) {
			const auto nss = qrp->qr.GetNamespaces();
			const auto& agg = qrp->qr.GetAggregationResults();
			if (mergedData_) {
				if (!iequals(mergedData_->pt.Name(), nss[0])) {
					throw Error(errLogic, "Query results in distributed query have different ns names: '{}' vs '{}'",
								mergedData_->pt.Name(), nss[0]);
				}
				if (mergedData_->haveRank != qrp->qr.HaveRank() || mergedData_->needOutputRank != qrp->qr.NeedOutputRank()) {
					throw Error(errLogic, "Rank options are incompatible between query results inside distributed query results");
				}
				if (mergedData_->aggregationResults.size() != agg.size()) {
					throw Error(errLogic, "Aggregations are incompatible between query results inside distributed query results");
				}
				for (size_t i = 0, s = agg.size(); i < s; ++i) {
					auto& mergedAgg = mergedData_->aggregationResults[i];
					const auto& newAgg = agg[i];
					if (newAgg.GetType() != mergedAgg.GetType()) {
						throw Error(errLogic, "Aggregations are incompatible between query results inside distributed query results");
					}

					auto newValue = newAgg.GetValue();
					if (!newValue) {
						continue;
					}

					auto value = mergedAgg.GetValue();

					switch (newAgg.GetType()) {
						case AggMin:
							if (!value || *value > *newValue) {
								mergedAgg.UpdateValue(*newValue);
							}
							break;
						case AggMax:
							if (!value || *value < *newValue) {
								mergedAgg.UpdateValue(*newValue);
							}
							break;
						case AggSum:
						case AggCount:
						case AggCountCached:
							mergedAgg.UpdateValue(*newValue + mergedAgg.GetValueOrZero());
							break;
						case AggUnknown:
						case AggAvg:
						case AggFacet:
						case AggDistinct:
							throw Error(errLogic, "Remote query result (within distributed results) has unsupported aggregations");
					}
				}
			} else {
				mergedData_ = std::make_unique<MergedData>(std::string(nss[0]), qrp->qr.HaveRank(), qrp->qr.NeedOutputRank());
				for (const auto& a : agg) {
					if (a.GetType() == AggAvg || a.GetType() == AggFacet || a.GetType() == AggDistinct || a.GetType() == AggUnknown) {
						throw Error(errLogic, "Remote query result (within distributed results) has unsupported aggregations");
					}
				}
				mergedData_->aggregationResults = agg;
			}
		}

		assertrx(mergedData_);
		std::vector<TagsMatcher> tmList;
		tmList.reserve(remote_.size() + (local_ ? 1 : 0));
		if (local_) {
			tmList.emplace_back(local_->qr.getTagsMatcher(0));
			mergedData_->pt = local_->qr.getPayloadType(0);
		}
		for (auto& qrp : remote_) {
			tmList.emplace_back(qrp->qr.GetTagsMatcher(0));
		}
		mergedData_->tm = TagsMatcher::CreateMergedTagsMatcher(tmList);

		if (local_) {
			local_->hasCompatibleTm = local_->qr.getTagsMatcher(0).IsSubsetOf(mergedData_->tm);
		}
		for (auto& qrp : remote_) {
			qrp->hasCompatibleTm = qrp->qr.GetTagsMatcher(0).IsSubsetOf(mergedData_->tm);
		}
	} catch (...) {
		mergedData_.reset();
		throw;
	}
}

void QueryResults::Clear() { *this = QueryResults(); }

const std::vector<AggregationResult>& QueryResults::GetAggregationResults() & {
	switch (type_) {
		case Type::None: {
			static std::vector<AggregationResult> kEmpty;
			return kEmpty;
		}
		case Type::Local: {
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.GetAggregationResults();
		}
		case Type::SingleRemote: {
			return remote_[0]->qr.GetAggregationResults();
		}
		case Type::MultipleRemote:
		case Type::Mixed:
		default:
			return getMergedData().aggregationResults;
	}
}

h_vector<std::string_view, 1> QueryResults::GetNamespaces() const {
	switch (type_) {
		case Type::None:
			return h_vector<std::string_view, 1>();
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.GetNamespaces();
		case Type::SingleRemote:
			return remote_[0]->qr.GetNamespaces();
		case Type::MultipleRemote:
		case Type::Mixed:
		default:
			return h_vector<std::string_view, 1>{getMergedData().pt.Name()};
	}
}

bool QueryResults::IsCacheEnabled() const noexcept {
	switch (type_) {
		case Type::None:
			return true;
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.IsCacheEnabled();
		case Type::SingleRemote:
		case Type::MultipleRemote:
		case Type::Mixed:
		default: {
			if (local_ && !local_->qr.IsCacheEnabled()) {
				return false;
			}
			for (auto& qrp : remote_) {
				if (!qrp->qr.IsCacheEnabled()) {
					return false;
				}
			}
			return true;
		}
	}
}

bool QueryResults::HaveShardIDs() const noexcept {
	if (local_ && local_->ShardID() != ShardingKeyType::ProxyOff) {
		return true;
	}
	for (auto& qrp : remote_) {
		if (qrp->ShardID() != ShardingKeyType::ProxyOff) {
			return true;
		}
	}
	return false;
}

int QueryResults::GetCommonShardID() const {
	switch (type_) {
		case Type::None:
			return -1;
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->ShardID();
		case Type::SingleRemote:
			return remote_[0]->ShardID();
		case Type::MultipleRemote:
		case Type::Mixed:
			break;
	}
	std::optional<int> shardId;
	if (local_) {
		shardId = local_->ShardID();
	}
	for (auto& qrp : remote_) {
		if (shardId.has_value()) {
			if (qrp->ShardID() != *shardId) {
				throw Error(errLogic, "Distributed query results does not have common shard id ({} vs {})", qrp->ShardID(), *shardId);
			}
		} else {
			shardId = qrp->ShardID();
		}
	}
	return shardId.has_value() ? *shardId : ShardingKeyType::ProxyOff;
}

PayloadType QueryResults::GetPayloadType(int nsid) const noexcept {
	switch (type_) {
		case Type::None:
			return PayloadType();
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.getPayloadType(nsid);
		case Type::SingleRemote:
			return remote_[0]->qr.GetPayloadType(nsid);
		case Type::MultipleRemote:
		case Type::Mixed:
		default:
			return getMergedData().pt;
	}
}

TagsMatcher QueryResults::GetTagsMatcher(int nsid) const noexcept {
	switch (type_) {
		case Type::None:
			return TagsMatcher();
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.getTagsMatcher(nsid);
		case Type::SingleRemote:
			return remote_[0]->qr.GetTagsMatcher(nsid);
		case Type::MultipleRemote:
		case Type::Mixed:
		default:
			return getMergedData().tm;
	}
}

bool QueryResults::HaveRank() const noexcept {
	switch (type_) {
		case Type::None:
			return false;
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.haveRank;
		case Type::SingleRemote:
			return remote_[0]->qr.HaveRank();
		case Type::MultipleRemote:
		case Type::Mixed:
			break;
	}
	return mergedData_ ? mergedData_->haveRank : false;
}

bool QueryResults::NeedOutputRank() const noexcept {
	switch (type_) {
		case Type::None:
			return false;
		case Type::Local:
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return local_->qr.needOutputRank;
		case Type::SingleRemote:
			return remote_[0]->qr.NeedOutputRank();
		case Type::MultipleRemote:
		case Type::Mixed:
			break;
	}
	return mergedData_ ? mergedData_->needOutputRank : false;
}

bool QueryResults::HaveJoined() const noexcept {
	switch (type_) {
		case Type::None:
			return false;
		case Type::Local: {
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			auto& joined = local_->qr.joined_;
			return !joined.empty() && std::any_of(joined.begin(), joined.end(), [](const auto& j) noexcept { return j.TotalItems(); });
		}
		case Type::SingleRemote:
			return remote_[0]->qr.HaveJoined();
		case Type::MultipleRemote:
		case Type::Mixed:
			break;
	}
	return false;
}

void QueryResults::SetQuery(const Query* q) {
	if (q) {
		QueryData data;
		data.isWalQuery = q->IsWALQuery();
		data.joinedSize = uint16_t(q->GetJoinQueries().size());
		data.mergedJoinedSizes.reserve(q->GetMergeQueries().size());
		for (const auto& mq : q->GetMergeQueries()) {
			data.mergedJoinedSizes.emplace_back(mq.GetJoinQueries().size());
		}
		qData_.emplace(std::move(data));
	} else {
		qData_.reset();
	}
}

uint32_t QueryResults::GetJoinedField(int parentNsId) const noexcept {
	uint32_t joinedField = 1;
	if (qData_.has_value()) {
		joinedField += qData_->mergedJoinedSizes.size();
		int mergedNsIdx = parentNsId;
		if (mergedNsIdx > 0) {
			joinedField += qData_->joinedSize;
			--mergedNsIdx;
		}
		for (int ns = 0; ns < mergedNsIdx; ++ns) {
			assertrx(size_t(ns) < qData_->mergedJoinedSizes.size());
			joinedField += qData_->mergedJoinedSizes[ns];
		}
	} else if (type_ == Type::Local) {
		assertrx_dbg(local_);
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		joinedField = local_->qr.joined_.size();
		for (int ns = 0; ns < parentNsId; ++ns) {
			assertrx_dbg(local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			joinedField += local_->qr.joined_[size_t(ns)].GetJoinedSelectorsCount();
		}
	}
	return joinedField;
}

QueryResults::ItemRefCache::ItemRefCache(IdType id, RankT rank, uint16_t nsid, ItemImpl&& i, bool raw)
	: itemImpl(std::move(i)), ref{ItemRefRanked{rank, id, itemImpl.payloadValue_, nsid, raw}} {}

QueryResults::ItemRefCache::ItemRefCache(IdType id, uint16_t nsid, ItemImpl&& i, bool raw)
	: itemImpl(std::move(i)), ref{ItemRef{id, itemImpl.payloadValue_, nsid, raw}} {}

Error QueryResults::Iterator::GetJSON(WrSerializer& wrser, bool withHdrLen) {
	try {
		return std::visit([&wrser, withHdrLen](auto&& it) { return it.GetJSON(wrser, withHdrLen); }, getVariantIt());
	} catch (Error& e) {
		return e;
	}
}

Expected<std::string> QueryResults::Iterator::GetJSON() {
	WrSerializer wrser;
	Error err = GetJSON(wrser, false);
	if (!err.ok()) {
		return Unexpected(std::move(err));
	}
	return std::string(wrser.Slice());
}

Error QueryResults::Iterator::GetCJSON(WrSerializer& wrser, bool withHdrLen) noexcept {
	try {
		switch (qr_->type_) {
			case Type::None:
				return Error(errLogic, "QueryResults are empty");
			case Type::Local:
				assertrx_dbg(qr_->local_);
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				return localIt_->GetCJSON(wrser, withHdrLen);
			case Type::SingleRemote:
			case Type::MultipleRemote:
			case Type::Mixed:
				break;
		}

		return std::visit(overloaded{[&](LocalQueryResults::ConstIterator it) {
										 assertrx_dbg(qr_->local_);
										 // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
										 if (qr_->local_->hasCompatibleTm) {
											 return it.GetCJSON(wrser, withHdrLen);
										 }
										 return getCJSONviaJSON(wrser, withHdrLen, it);
									 },
									 [&](client::QueryResults::Iterator it) {
										 if (qr_->type_ == Type::SingleRemote || qr_->remote_[size_t(qr_->curQrId_)]->hasCompatibleTm) {
											 return it.GetCJSON(wrser, withHdrLen);
										 }
										 return getCJSONviaJSON(wrser, withHdrLen, it);
									 }},
						  getVariantIt());
	}
	CATCH_AND_RETURN;
}

Error QueryResults::Iterator::GetMsgPack(WrSerializer& wrser, bool withHdrLen) noexcept {
	try {
		return std::visit([&wrser, withHdrLen](auto&& it) { return it.GetMsgPack(wrser, withHdrLen); }, getVariantIt());
	}
	CATCH_AND_RETURN;
}

Error QueryResults::Iterator::GetProtobuf(WrSerializer& wrser) noexcept {
	try {
		return std::visit(overloaded{[&wrser](LocalQueryResults::ConstIterator it) { return it.GetProtobuf(wrser); },
									 [](const client::QueryResults::Iterator&) {
										 // TODO: May be implemented on request some day
										 return Error(errParams, "Protobuf is not supported for distributed and proxied queries");
									 }},
						  getVariantIt());
	}
	CATCH_AND_RETURN;
}

Item QueryResults::Iterator::GetItem(bool enableHold) {
	try {
		switch (qr_->type_) {
			case Type::None:
				return Item();
			case Type::Local:
				assertrx_dbg(qr_->local_);
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				return localIt_->GetItem(enableHold);
			case Type::SingleRemote:
			case Type::MultipleRemote:
			case Type::Mixed:
				break;
		}

		auto vit = getVariantIt();
		std::unique_ptr<ItemImpl> itemImpl;
		if (qr_->type_ == Type::Mixed || qr_->type_ == Type::MultipleRemote) {
			auto& mData = qr_->getMergedData();
			itemImpl.reset(new ItemImpl(mData.pt, mData.tm));
		} else {
			auto& remoteQr = qr_->remote_[size_t(qr_->curQrId_)]->qr;
			const int nsId = std::get<client::QueryResults::Iterator>(vit).GetNSID();
			itemImpl.reset(new ItemImpl(remoteQr.GetPayloadType(nsId), remoteQr.GetTagsMatcher(nsId)));
		}

		Item item =
			std::visit(overloaded{[&](LocalQueryResults::ConstIterator& it) {
									  assertrx_dbg(qr_->local_);
									  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
									  auto item = getItem(it, std::move(itemImpl), it.GetFieldsFilter(), !qr_->local_->hasCompatibleTm);
									  item.setID(it.GetItemRef().Id());
									  item.setLSN(it.GetItemRef().Value().GetLSN());
									  item.setShardID(qr_->local_->ShardID());
									  return item;
								  },
								  [&](client::QueryResults::Iterator& it) {
									  auto& remoteQr = *qr_->remote_[size_t(qr_->curQrId_)];
									  auto item = getItem(it, std::move(itemImpl), !remoteQr.hasCompatibleTm || !remoteQr.qr.IsCJSON());
									  item.setID(it.GetID());
									  assertrx(!it.GetLSN().isEmpty());
									  item.setLSN(it.GetLSN());
									  item.setShardID(it.GetShardID());
									  return item;
								  }},
					   vit);
		return item;
	} catch (Error& e) {
		return Item(e);
	}
}

template <typename QrT>
void QueryResults::QrMetaData<QrT>::ResetJoinStorage(int64_t idx) const {
	if (nsJoinRes_) {
		nsJoinRes_->Clear();
		nsJoinRes_->idx = idx;
	} else {
		nsJoinRes_ = std::make_unique<ItemDataStorage<JoinResStorage>>(idx);
	}
}

Error QueryResults::Iterator::GetCSV(WrSerializer& ser, CsvOrdering& ordering) noexcept {
	try {
		return std::visit(overloaded{[&ser, &ordering](auto it) { return it.GetCSV(ser, ordering); }}, getVariantIt());
	}
	CATCH_AND_RETURN
}

joins::ItemIterator QueryResults::Iterator::GetJoined(std::vector<ItemRefCache>* storage) {
	if (qr_->type_ == Type::Local) {
		assertrx_dbg(localIt_);
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		return localIt_->GetJoined();
	} else if (qr_->type_ == Type::SingleRemote) {
		validateProxiedIterator();

		auto rit = qr_->remote_[0]->it;
		const auto& joinedData = rit.GetJoined();
		if (!joinedData.size()) {
			return joins::ItemIterator::CreateEmpty();
		}
		if (!qr_->qData_.has_value()) {
			throw Error(errLogic, "Unable to init joined data without initial query");
		}

		auto& rqr = *qr_->remote_[0];
		const auto& ritItemParams = rit.GetItemParams();
		if (storage || !rqr.CheckIfNsJoinStorageHasSameIdx(idx_)) {
			try {
				rqr.ResetJoinStorage(idx_);
				const auto& qData = qr_->qData_;
				if (ritItemParams.nsid >= int(qData->joinedSize)) {
					return reindexer::joins::ItemIterator::CreateEmpty();
				}
				rqr.NsJoinRes()->data.jr.SetJoinedSelectorsCount(qData->joinedSize);

				auto jField = qr_->GetJoinedField(ritItemParams.nsid);
				for (size_t i = 0; i < joinedData.size(); ++i, ++jField) {
					LocalQueryResults qrJoined;
					const auto& joinedItems = joinedData[i];
					for (const auto& itemData : joinedItems) {
						ItemImpl itemimpl(rqr.qr.GetPayloadType(jField), rqr.qr.GetTagsMatcher(jField));
						itemimpl.FromCJSON(itemData.data);

						if (qrJoined.haveRank) {
							qrJoined.AddItemRef(itemData.rank, itemData.id, itemimpl.Value(), itemData.nsid, true);
						} else {
							qrJoined.AddItemRef(itemData.id, itemimpl.Value(), itemData.nsid, true);
						}
						if (!storage) {
							rqr.NsJoinRes()->data.joinedRawData.emplace_back(std::move(itemimpl));
						} else {
							if (qrJoined.haveRank) {
								storage->emplace_back(itemData.id, RankT{}, itemData.nsid, std::move(itemimpl), true);
							} else {
								storage->emplace_back(itemData.id, itemData.nsid, std::move(itemimpl), true);
							}
						}
					}
					rqr.NsJoinRes()->data.jr.Insert(ritItemParams.id, i, std::move(qrJoined));
				}
			} catch (...) {
				if (rqr.NsJoinRes()) {
					rqr.NsJoinRes()->idx = -1;
				}
				throw;
			}
		}

		return joins::ItemIterator(&(rqr.NsJoinRes()->data.jr), ritItemParams.id);
	}
	// Distributed queries can not have joins
	return reindexer::joins::ItemIterator::CreateEmpty();
}

template <>
QueryResults::ItemDataStorage<QueryResults::ItemRefCache>& QueryResults::QrMetaData<client::QueryResults>::ItemRefData(int64_t idx) {
	ItemImpl itemimpl(qr.GetPayloadType(0), qr.GetTagsMatcher(0));
	const bool convertViaJSON = !hasCompatibleTm || !qr.IsCJSON();
	Error err = fillItemImpl(it, itemimpl, convertViaJSON);
	if (!err.ok()) {
		throw err;
	}
	if (it.IsRanked()) {
		ResetItemRefCache(idx, ItemRefCache(it.GetID(), it.GetRank(), it.GetNSID(), std::move(itemimpl), it.IsRaw()));
	} else {
		ResetItemRefCache(idx, ItemRefCache(it.GetID(), it.GetNSID(), std::move(itemimpl), it.IsRaw()));
	}
	return *itemRefData_;
}

class [[nodiscard]] SortExpressionComparator {
public:
	SortExpressionComparator(SortExpression&& se, const NamespaceImpl& ns)
		: localExpression_{std::move(se)}, proxiedExpression_{localExpression_, ns} {}
	ComparationResult Compare(const ItemRefVariant& litem, const ItemRefVariant& ritem, const PayloadType& lpt, const PayloadType& rpt,
							  TagsMatcher& ltm, TagsMatcher& rtm, bool lLocal, bool rLocal, int lShardId, int rShardId) const {
		assertrx_throw(litem.AsVariant().index() == ritem.AsVariant().index());
		return std::visit(overloaded{[&](const ItemRef& lref) {
										 return Compare(lref, ritem.NotRanked(), RankT{}, RankT{}, lpt, rpt, ltm, rtm, lLocal, rLocal,
														lShardId, rShardId);
									 },
									 [&](const ItemRefRanked& lref) {
										 return Compare(lref.NotRanked(), ritem.NotRanked(), lref.Rank(), ritem.Rank(), lpt, rpt, ltm, rtm,
														lLocal, rLocal, lShardId, rShardId);
									 }},
						  litem.AsVariant());
	}
	ComparationResult Compare(const ItemRef& litem, const ItemRef& ritem, RankT lrank, RankT rrank, const PayloadType& lpt,
							  const PayloadType& rpt, TagsMatcher& ltm, TagsMatcher& rtm, bool lLocal, bool rLocal, int lShardId,
							  int rShardId) const {
		const auto lhv = lLocal ? localExpression_.Calculate(litem.Id(), {lpt, litem.Value()}, {}, {}, lrank, ltm, lShardId)
								: proxiedExpression_.Calculate(litem.Id(), {lpt, litem.Value()}, lrank, ltm, lShardId);
		const auto rhv = rLocal ? localExpression_.Calculate(ritem.Id(), {rpt, ritem.Value()}, {}, {}, rrank, rtm, rShardId)
								: proxiedExpression_.Calculate(ritem.Id(), {rpt, ritem.Value()}, rrank, rtm, rShardId);
		if (fp::ExactlyEqual(lhv, rhv)) {
			return ComparationResult::Eq;
		}
		return lhv < rhv ? ComparationResult::Gt : ComparationResult::Lt;
	}

private:
	SortExpression localExpression_;
	ProxiedSortExpression proxiedExpression_;
};

class [[nodiscard]] FieldComparator {
	struct [[nodiscard]] RelaxedCompare {
		bool operator()(const Variant& lhs, const Variant& rhs) const {
			return lhs.RelaxCompare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rhs) == ComparationResult::Eq;
		}
	};

public:
	FieldComparator(std::string fName, int idx, const NamespaceImpl& ns, const VariantArray& forcedValues)
		: fieldName_{std::move(fName)}, fieldIdx_{idx} {
		if (fieldIdx_ != IndexValueType::SetByJsonPath) {
			const auto& jsonPaths = ns.payloadType_.Field(fieldIdx_).JsonPaths();
			assertrx(jsonPaths.size() == 1);
			fieldName_ = jsonPaths[0];
			collateOpts_ = ns.indexes_[fieldIdx_]->Opts().collateOpts_;
			if (ns.indexes_[fieldIdx_]->Opts().IsSparse()) {
				fieldIdx_ = IndexValueType::SetByJsonPath;
			}
		}
		for (size_t i = 0; i < forcedValues.size(); ++i) {
			forcedValues_.emplace(forcedValues[i], i);
		}
	}
	ComparationResult Compare(const ItemRefVariant& litem, const ItemRefVariant& ritem, const PayloadType& lpt, const PayloadType& rpt,
							  TagsMatcher& ltm, TagsMatcher& rtm, bool lLocal, bool rLocal, int rShardId, int lShardId) const {
		return Compare(litem.NotRanked(), ritem.NotRanked(), lpt, rpt, ltm, rtm, lLocal, rLocal, rShardId, lShardId);
	}
	ComparationResult Compare(const ItemRef& litem, const ItemRef& ritem, const PayloadType& lpt, const PayloadType& rpt, TagsMatcher& ltm,
							  TagsMatcher& rtm, bool lLocal, bool rLocal, [[maybe_unused]] int rShardId,
							  [[maybe_unused]] int lShardId) const {
		ConstPayload lpv{lpt, litem.Value()};
		ConstPayload rpv{rpt, ritem.Value()};
		if (!forcedValues_.empty()) {
			VariantArray lValues;
			if (lLocal && fieldIdx_ != IndexValueType::SetByJsonPath) {
				lpv.Get(fieldIdx_, lValues);
			} else {
				lpv.GetByJsonPath(fieldName_, ltm, lValues, KeyValueType::Undefined{});
			}
			VariantArray rValues;
			if (rLocal && fieldIdx_ != IndexValueType::SetByJsonPath) {
				rpv.Get(fieldIdx_, rValues);
			} else {
				rpv.GetByJsonPath(fieldName_, rtm, rValues, KeyValueType::Undefined{});
			}
			if (lValues.size() != 1 || rValues.size() != 1) {
				throw Error(errLogic, "Cannot sort by array field");
			}
			const auto lIt = forcedValues_.find(lValues[0]);
			const auto rIt = forcedValues_.find(rValues[0]);
			const auto end = forcedValues_.end();
			if (lIt != end) {
				if (rIt != end) {
					return lIt->second == rIt->second ? ComparationResult::Eq
													  : (lIt->second < rIt->second ? ComparationResult::Gt : ComparationResult::Lt);
				} else {
					return ComparationResult::Gt;
				}
			} else if (rIt != end) {
				return ComparationResult::Lt;
			}
		}
		return -lpv.RelaxCompare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rpv, fieldName_, fieldIdx_, collateOpts_, ltm,
																							  rtm, !lLocal, !rLocal);
	}

private:
	std::string fieldName_;
	int fieldIdx_;
	CollateOpts collateOpts_;
	fast_hash_map<Variant, size_t, std::hash<Variant>, RelaxedCompare> forcedValues_;
};

class [[nodiscard]] QueryResults::CompositeFieldForceComparator {
	struct [[nodiscard]] RelaxedCompare {
		bool operator()(const Variant& lhs, const Variant& rhs) const {
			return lhs.RelaxCompare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rhs) == ComparationResult::Eq;
		}
	};

public:
	CompositeFieldForceComparator(int index, const VariantArray& forcedSortOrder, const NamespaceImpl& ns) {
		fields_.reserve(ns.indexes_[index]->Fields().size());
		const FieldsSet& fields = ns.indexes_[index]->Fields();
		size_t jsonPathsIndex = 0;
		for (size_t j = 0, s = fields.size(); j < s; ++j) {
			const auto f = fields[j];
			if (f == IndexValueType::SetByJsonPath) {
				fields_.emplace_back(ValuesByField{fields.getJsonPath(jsonPathsIndex++), f, {}});
			} else {
				assertrx(f < ns.indexes_.firstCompositePos());
				fields_.emplace_back(ValuesByField{ns.indexes_[f]->Name(), f, {}});
			}
		}
		assertrx(fields_.size() > 1);
		for (size_t i = 0, size = forcedSortOrder.size(); i < size; ++i) {
			auto va = forcedSortOrder[i].getCompositeValues();
			assertrx(va.size() == fields_.size());
			for (size_t j = 0, s = va.size(); j < s; ++j) {
				fields_[j].values[va[j]].push_back(i);
			}
		}
	}
	ComparationResult Compare(const ItemRefVariant& litem, const ItemRefVariant& ritem, const PayloadType& lpt, const PayloadType& rpt,
							  TagsMatcher& ltm, TagsMatcher& rtm, bool lLocal, bool rLocal, int rShardId, int lShardId) const {
		return Compare(litem.NotRanked(), ritem.NotRanked(), lpt, rpt, ltm, rtm, lLocal, rLocal, rShardId, lShardId);
	}
	ComparationResult Compare(const ItemRef& litem, const ItemRef& ritem, const PayloadType& lpt, const PayloadType& rpt, TagsMatcher& ltm,
							  TagsMatcher& rtm, bool lLocal, bool rLocal, [[maybe_unused]] int rShardId,
							  [[maybe_unused]] int lShardId) const {
		ConstPayload lpv{lpt, litem.Value()};
		ConstPayload rpv{rpt, ritem.Value()};
		h_vector<size_t, 4> positions1, positions2;
		size_t i = 0;
		const size_t size = fields_.size();
		for (; i < size; ++i) {
			VariantArray lValues;
			if (lLocal && fields_[i].fieldIdx != IndexValueType::SetByJsonPath) {
				lpv.Get(fields_[i].fieldIdx, lValues);
			} else {
				lpv.GetByJsonPath(fields_[i].fieldName, ltm, lValues, KeyValueType::Undefined{});
			}
			if (lValues.size() != 1) {
				throw Error(errLogic, "Cannot sort by array field");
			}
			const auto it = fields_[i].values.find(lValues[0]);
			if (it == fields_[i].values.end()) {
				if (i % 2 == 0) {
					positions1.clear();
				} else {
					positions2.clear();
				}
				break;
			}
			if (i == 0) {
				positions1 = it->second;
				positions2.resize(positions1.size());
			} else if (i % 2 == 0) {
				positions1.resize(
					std::set_intersection(positions2.begin(), positions2.end(), it->second.begin(), it->second.end(), positions1.begin()) -
					positions1.begin());
				if (positions1.empty()) {
					break;
				}
			} else {
				positions2.resize(
					std::set_intersection(positions1.begin(), positions1.end(), it->second.begin(), it->second.end(), positions2.begin()) -
					positions2.begin());
				if (positions2.empty()) {
					break;
				}
			}
		}
		const h_vector<size_t, 4>* positions = ((i == size) != (i % 2 == 0)) ? &positions1 : &positions2;
		size_t lPos;
		if (positions->size() > 1) {
			throw Error(errLogic, "Several forced sort values are equal");
		} else if (positions->empty()) {
			lPos = -1;
		} else {
			lPos = (*positions)[0];
		}
		for (i = 0; i < size; ++i) {
			VariantArray rValues;
			if (rLocal && fields_[i].fieldIdx != IndexValueType::SetByJsonPath) {
				rpv.Get(fields_[i].fieldIdx, rValues);
			} else {
				rpv.GetByJsonPath(fields_[i].fieldName, rtm, rValues, KeyValueType::Undefined{});
			}
			if (rValues.size() != 1) {
				throw Error(errLogic, "Cannot sort by array field");
			}
			const auto it = fields_[i].values.find(rValues[0]);
			if (it == fields_[i].values.end()) {
				if (i % 2 == 0) {
					positions1.clear();
				} else {
					positions2.clear();
				}
				break;
			}
			if (i == 0) {
				positions1 = it->second;
				positions2.clear();
				positions2.resize(positions1.size());
			} else if (i % 2 == 0) {
				positions1.resize(
					std::set_intersection(positions2.begin(), positions2.end(), it->second.begin(), it->second.end(), positions1.begin()) -
					positions1.begin());
				if (positions1.empty()) {
					break;
				}
			} else {
				positions2.resize(
					std::set_intersection(positions1.begin(), positions1.end(), it->second.begin(), it->second.end(), positions2.begin()) -
					positions2.begin());
				if (positions2.empty()) {
					break;
				}
			}
		}
		positions = ((i == size) != (i % 2 == 0)) ? &positions1 : &positions2;
		if (positions->size() > 1) {
			throw Error(errLogic, "Several forced sort values are equal");
		}
		if (lPos == static_cast<size_t>(-1)) {
			return (positions->empty()) ? ComparationResult::Eq : ComparationResult::Lt;
		}
		if (positions->empty()) {
			return ComparationResult::Gt;
		} else if ((*positions)[0] == lPos) {
			return ComparationResult::Eq;
		}
		return lPos < (*positions)[0] ? ComparationResult::Gt : ComparationResult::Lt;
	}

private:
	struct [[nodiscard]] ValuesByField {
		std::string fieldName;
		int fieldIdx = IndexValueType::SetByJsonPath;
		fast_hash_map<Variant, h_vector<size_t, 4>, std::hash<Variant>, RelaxedCompare> values;
	};
	h_vector<ValuesByField, 4> fields_;
};

class [[nodiscard]] QueryResults::Comparator {
public:
	Comparator(QueryResults& qr, const Query& q, const NamespaceImpl& ns) : qr_{qr} {
		assertrx(q.GetSortingEntries().size() > 0);
		comparators_.reserve(q.GetSortingEntries().size());
		for (size_t i = 0; i < q.GetSortingEntries().size(); ++i) {
			const auto& se = q.GetSortingEntries()[i];
			auto expr = SortExpression::Parse<JoinedSelector>(se.expression, {});
			if (expr.ByField()) {
				int index = IndexValueType::SetByJsonPath;
				std::string field;
				if (ns.tryGetIndexByName(se.expression, index) && index < ns.indexes_.firstCompositePos() &&
					ns.indexes_[index]->Opts().IsSparse()) {
					const auto& fields = ns.indexes_[index]->Fields();
					assertrx(fields.getJsonPathsLength() == 1);
					field = fields.getJsonPath(0);
					index = IndexValueType::SetByJsonPath;
				} else {
					field = se.expression;
				}
				if (index == IndexValueType::SetByJsonPath || index < ns.indexes_.firstCompositePos()) {
					if (i == 0 && !q.ForcedSortOrder().empty()) {
						comparators_.emplace_back(FieldComparator{std::move(field), index, ns, q.ForcedSortOrder()}, se.desc);
					} else {
						comparators_.emplace_back(FieldComparator{std::move(field), index, ns, {}}, se.desc);
					}
				} else {
					if (i == 0 && !q.ForcedSortOrder().empty()) {
						comparators_.emplace_back(CompositeFieldForceComparator{index, q.ForcedSortOrder(), ns}, se.desc);
					}
					const auto& fields = ns.indexes_[index]->Fields();
					size_t jsonPathsIndex = 0;
					for (size_t j = 0, s = fields.size(); j < s; ++j) {
						const auto f = fields[j];
						if (f == IndexValueType::SetByJsonPath) {
							comparators_.emplace_back(FieldComparator{fields.getJsonPath(jsonPathsIndex++), f, ns, {}}, se.desc);
						} else {
							assertrx(f < ns.indexes_.firstCompositePos());
							comparators_.emplace_back(FieldComparator{ns.indexes_[f]->Name(), f, ns, {}}, se.desc);
						}
					}
				}
			} else {
				expr.PrepareIndexes(ns);
				comparators_.emplace_back(SortExpressionComparator{std::move(expr), ns}, se.desc);
			}
		}
	}
	bool operator()(int lhs, int rhs) const {
		if (lhs == rhs) {
			return false;
		}
		TagsMatcher ltm, rtm;
		PayloadType lpt, rpt;
		ItemRefVariant liref, riref;
		int lShardId, rShardId;
		uint32_t lShardIdHash = 0, rShardIdHash = 0;

		if (lhs < 0) {
			assertrx_dbg(qr_.local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			const auto& lqr = *qr_.local_;
			liref = lqr.it.GetItemRefVariant();
			ltm = lqr.qr.getTagsMatcher(0);
			lpt = lqr.qr.getPayloadType(0);
			lShardId = lqr.ShardID();
			lShardIdHash = lqr.ShardIDHash();
		} else {
			assertrx_throw(static_cast<size_t>(lhs) < qr_.remote_.size());
			auto& rqr = *qr_.remote_[lhs];
			liref = rqr.ItemRefData(qr_.curQrId_).data.ref;
			ltm = rqr.qr.GetTagsMatcher(0);
			lpt = rqr.qr.GetPayloadType(0);
			lShardId = rqr.ShardID();
			lShardIdHash = rqr.ShardIDHash();
		}
		if (rhs < 0) {
			assertrx_dbg(qr_.local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			const auto& lqr = *qr_.local_;
			riref = lqr.it.GetItemRefVariant();
			rtm = lqr.qr.getTagsMatcher(0);
			rpt = lqr.qr.getPayloadType(0);
			rShardId = lqr.ShardID();
			rShardIdHash = lqr.ShardIDHash();
		} else {
			assertrx_throw(static_cast<size_t>(rhs) < qr_.remote_.size());
			auto& rqr = *qr_.remote_[rhs];
			riref = rqr.ItemRefData(qr_.curQrId_).data.ref;
			rtm = rqr.qr.GetTagsMatcher(0);
			rpt = rqr.qr.GetPayloadType(0);
			rShardId = rqr.ShardID();
			rShardIdHash = rqr.ShardIDHash();
		}
		for (const auto& comp : comparators_) {
			const auto res = std::visit(
				[&](const auto& c) { return c.Compare(liref, riref, lpt, rpt, ltm, rtm, lhs < 0, rhs < 0, lShardIdHash, rShardIdHash); },
				comp.first);
			if (res != ComparationResult::Eq) {
				return comp.second ? res == ComparationResult::Lt : res == ComparationResult::Gt;
			}
		}
		return lShardId < rShardId;
	}

private:
	const QueryResults& qr_;
	h_vector<std::pair<std::variant<SortExpressionComparator, FieldComparator, CompositeFieldForceComparator>, bool>, 1> comparators_;
};

void QueryResults::SetOrdering(const Query& q, const NamespaceImpl& ns, const RdxContext& ctx) {
	assertrx(!orderedQrs_);
	if (!q.GetSortingEntries().empty()) {
		auto lock = ns.rLock(ctx);
		Comparator comparator{*this, q, ns};
		lock.unlock();
		orderedQrs_ = std::make_unique<std::set<int, Comparator>>(std::move(comparator));
		limit = q.Limit();
		offset = q.Offset();
	}
}

void QueryResults::beginImpl() const {
	if (type_ == Type::Local) {
		assertrx_dbg(local_);
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		begin_.it = Iterator{this, 0, {local_->qr.begin() + std::min<size_t>(offset, count())}};
	} else {
		begin_.it = Iterator{this, 0, std::nullopt};
		if (orderedQrs_ && offset > 0 && type_ != Type::None) {
			for (size_t i = 0, c = count(); i < offset && i < c; ++i) {
				++(*begin_.it);
			}
		}
	}
}
// if function hash() is used, the records will not be sorted, since the ordering in qr's occurs according to a different seed in hash()
QueryResults::Iterator& QueryResults::Iterator::operator++() {
	switch (qr_->type_) {
		case Type::None:
			*this = qr_->end();
			return *this;
		case Type::Local:
			assertrx_dbg(localIt_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			++(*localIt_);
			return *this;
		case Type::SingleRemote:
		case Type::MultipleRemote:
		case Type::Mixed:
			break;
	}

	if (idx_ < qr_->lastSeenIdx_) {
		++idx_;	 // This iterator is not valid yet, so simply increment index and do not touch qr's internals
		return *this;
	}

	auto* qr = const_cast<QueryResults*>(qr_);
	if (!qr_->orderedQrs_ || qr_->type_ == Type::SingleRemote) {
		if (qr->curQrId_ < 0) {
			assertrx_dbg(qr->local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			++qr->local_->it;
			++qr->lastSeenIdx_;
			++idx_;
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			if (qr->local_->it == qr->local_->qr.end()) {
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				qr->curQrId_ = qr->findFirstQrWithItems(qr->local_->ShardID());
			}
		} else if (size_t(qr->curQrId_) < qr_->remote_.size()) {
			auto& remoteQrp = *qr->remote_[size_t(qr_->curQrId_)];
			++remoteQrp.it;
			++qr->lastSeenIdx_;
			++idx_;
			if (remoteQrp.it == remoteQrp.qr.end()) {
				qr->curQrId_ = qr->findFirstQrWithItems(remoteQrp.ShardID());
			}
		}
	} else if (!qr->orderedQrs_->empty()) {
		++qr->lastSeenIdx_;
		++idx_;
		const auto qrId = qr->curQrId_;
		assertrx(*qr->orderedQrs_->begin() == qrId);
		auto oNode = qr->orderedQrs_->extract(qr->orderedQrs_->begin());
		if (qrId < 0) {
			assertrx_dbg(qr->local_);
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			auto& local = *qr->local_;
			++local.it;
			if (local.it != local.qr.end()) {
				oNode.value() = -1;
				qr->orderedQrs_->insert(std::move(oNode));
			}
		} else {
			assertrx(static_cast<size_t>(qrId) < qr_->remote_.size());
			auto& remoteQrp = *qr->remote_[qrId];
			++remoteQrp.it;
			if (remoteQrp.it != remoteQrp.qr.end()) {
				oNode.value() = qrId;
				qr->orderedQrs_->insert(std::move(oNode));
			}
		}
		if (qr->orderedQrs_->begin() != qr->orderedQrs_->end()) {
			qr->curQrId_ = *qr->orderedQrs_->begin();
		} else {
			*this = qr->end();
		}
	}
	return *this;
}

template <typename QrT>
QueryResults::QrMetaData<QrT>::QrMetaData(QrT&& _qr, int shardID) : qr{std::move(_qr)}, it{qr.begin()}, shardID_{shardID} {
	MurmurHash3_x86_32(&shardID_, sizeof(shardID_), 0, &shardIDHash_);
}

template <typename QrT>
QueryResults::QrMetaData<QrT>::QrMetaData(QrMetaData&& o) noexcept
	: qr(std::move(o.qr)),
	  it(QrT::Iterator::SwitchQueryResultsPtrUnsafe(std::move(o.it), qr)),
	  hasCompatibleTm(o.hasCompatibleTm),
	  shardID_(o.shardID_),
	  shardIDHash_(o.shardIDHash_),
	  itemRefData_(std::move(o.itemRefData_)),
	  nsJoinRes_(std::move(o.nsJoinRes_)) {}

template <typename QrT>
QueryResults::QrMetaData<QrT>& QueryResults::QrMetaData<QrT>::operator=(QrMetaData&& o) noexcept {
	if (this != &o) {
		qr = std::move(o.qr);
		// SwitchQueryResultsPtrUnsafe is not implemented for client query results - iterator contains to many different pointers
		// and it is unsafe to move it
		it = QrT::Iterator::SwitchQueryResultsPtrUnsafe(std::move(o.it), qr);
		hasCompatibleTm = o.hasCompatibleTm;
		shardID_ = o.shardID_;
		shardIDHash_ = o.shardIDHash_;
		itemRefData_ = std::move(o.itemRefData_);
		nsJoinRes_ = std::move(o.nsJoinRes_);
	}
	return *this;
}

template <typename QrT>
void QueryResults::QrMetaData<QrT>::ResetItemRefCache(int64_t idx, ItemRefCache&& newD) const {
	if (itemRefData_) {
		*itemRefData_ = ItemDataStorage<ItemRefCache>(idx, std::move(newD));
	} else {
		itemRefData_ = std::make_unique<ItemDataStorage<ItemRefCache>>(idx, std::move(newD));
	}
}

template <typename QrT>
bool QueryResults::QrMetaData<QrT>::CheckIfItemRefStorageHasSameIdx(int64_t idx) const noexcept {
	return itemRefData_ && idx == itemRefData_->idx;
}

template <typename QrT>
bool QueryResults::QrMetaData<QrT>::CheckIfNsJoinStorageHasSameIdx(int64_t idx) const noexcept {
	return nsJoinRes_ && idx == nsJoinRes_->idx;
}

template <bool isRanked>
auto QueryResults::Iterator::getItemRef(ProxiedRefsStorage* storage) {
	switch (qr_->type_) {
		case Type::None:
			if constexpr (isRanked) {
				return ItemRefRanked(RankT{});
			} else {
				return ItemRef();
			}
		case Type::Local:
			assertrx_dbg(localIt_);
			if constexpr (isRanked) {
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				return localIt_->GetItemRefRanked();
			} else {
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				return localIt_->GetItemRef();
			}
		case Type::SingleRemote:
		case Type::MultipleRemote:
		case Type::Mixed:
			break;
	}
	return std::visit(overloaded{[](QrMetaData<LocalQueryResults>* qr) {
									 if constexpr (isRanked) {
										 return qr->it.GetItemRefRanked();
									 } else {
										 return qr->it.GetItemRef();
									 }
								 },
								 [&](QrMetaData<client::QueryResults>* qr) {
									 if (!qr->CheckIfItemRefStorageHasSameIdx(idx_) || storage) {
										 auto& remoteQr = *qr_->remote_[size_t(qr_->curQrId_)];
										 ItemImpl itemimpl(qr_->GetPayloadType(0), qr_->GetTagsMatcher(0));
										 const bool convertViaJSON = !remoteQr.hasCompatibleTm || !remoteQr.qr.IsCJSON();
										 Error err = fillItemImpl(qr->it, itemimpl, convertViaJSON);
										 if (!err.ok()) {
											 throw err;
										 }

										 if (!storage) {
											 if (qr_->HaveRank()) {
												 qr->ResetItemRefCache(idx_, ItemRefCache(qr->it.GetID(), qr->it.GetRank(),
																						  qr->it.GetNSID(), std::move(itemimpl), IsRaw()));
											 } else {
												 qr->ResetItemRefCache(
													 idx_, ItemRefCache(qr->it.GetID(), qr->it.GetNSID(), std::move(itemimpl), IsRaw()));
											 }
											 if constexpr (isRanked) {
												 return qr->ItemRefData()->data.ref.Ranked();
											 } else {
												 return qr->ItemRefData()->data.ref.NotRanked();
											 }
										 } else {
											 if (qr_->HaveRank()) {
												 storage->emplace_back(qr->it.GetID(), qr->it.GetRank(), qr->it.GetNSID(),
																	   std::move(itemimpl), IsRaw());
											 } else {
												 storage->emplace_back(qr->it.GetID(), qr->it.GetNSID(), std::move(itemimpl), IsRaw());
											 }
											 if constexpr (isRanked) {
												 return storage->back().ref.Ranked();
											 } else {
												 return storage->back().ref.NotRanked();
											 }
										 }
									 }
									 if constexpr (isRanked) {
										 return qr->ItemRefData()->data.ref.Ranked();
									 } else {
										 return qr->ItemRefData()->data.ref.NotRanked();
									 }
								 }},
					  getVariantResult());
}

ItemRef QueryResults::Iterator::GetItemRef(std::vector<ItemRefCache>* storage) { return getItemRef<false>(storage); }
ItemRefRanked QueryResults::Iterator::GetItemRefRanked(std::vector<ItemRefCache>* storage) { return getItemRef<true>(storage); }

const QueryResults::MergedData& QueryResults::getMergedData() const {
	if (!mergedData_) {
		throw Error(errLogic, "Distributed query results are incomplete. Merged data is empty");
	}
	return *mergedData_;
}

bool QueryResults::ordering() const noexcept { return orderedQrs_ && (type_ == Type::Mixed || type_ == Type::MultipleRemote); }

int QueryResults::findFirstQrWithItems(int minShardId) {
	if (ordering()) {
		if (local_ && local_->qr.Count()) {
			assertrx(local_->it == local_->qr.begin());
			orderedQrs_->emplace(-1);
		}
		for (int i = 0, size = remote_.size(); i < size; ++i) {
			auto& remote = *remote_[i];
			if (remote.qr.Count()) {
				assertrx(remote.it == remote.qr.begin());
				orderedQrs_->emplace(i);
			}
		}
		if (orderedQrs_->empty()) {
			return remote_.size();
		} else {
			return *orderedQrs_->begin();
		}
	} else {
		int foundPos = remote_.size();
		int foundShardId = std::numeric_limits<int>::max();
		if (local_ && local_->qr.Count() && local_->ShardID() > minShardId) {
			foundPos = -1;
			foundShardId = local_->ShardID();
		}
		for (int i = 0, size = remote_.size(); i < size; ++i) {
			auto& remote = *remote_[i];
			if (remote.qr.Count() && remote.ShardID() < foundShardId && remote.ShardID() > minShardId) {
				foundPos = i;
				foundShardId = remote.ShardID();
			}
		}
		return foundPos;
	}
}

template <typename QrItT>
Item QueryResults::Iterator::getItem(QrItT& it, std::unique_ptr<ItemImpl>&& itemImpl, bool convertViaJSON) {
	auto err = fillItemImpl(it, *itemImpl, convertViaJSON);
	if (!err.ok()) {
		return Item(err);
	}
	return Item(itemImpl.release());
}

template <typename QrItT>
Item QueryResults::Iterator::getItem(QrItT& it, std::unique_ptr<ItemImpl>&& itemImpl, const FieldsFilter& fieldsFilter,
									 bool convertViaJSON) {
	auto err = fillItemImpl(it, *itemImpl, convertViaJSON);
	if (!err.ok()) {
		return Item(err);
	}
	return Item(itemImpl.release(), fieldsFilter);
}

template <typename QrItT>
Error QueryResults::fillItemImpl(QrItT& it, ItemImpl& itemImpl, bool convertViaJSON) {
	WrSerializer wrser;
	Error err;
	if (!convertViaJSON) {
		err = it.GetCJSON(wrser, false);
		itemImpl.FromCJSON(wrser.Slice());
	} else {
		err = it.GetJSON(wrser, false);
		if (err.ok()) {
			err = itemImpl.FromJSON(wrser.Slice());
		}
	}
	if (err.ok()) {
		itemImpl.Value().SetLSN(it.GetLSN());
	}
	return err;
}

template <typename QrItT>
Error QueryResults::Iterator::getCJSONviaJSON(WrSerializer& wrser, bool withHdrLen, QrItT& it) {
	auto& mData = qr_->getMergedData();
	ItemImpl itemImpl(mData.pt, mData.tm);
	itemImpl.Unsafe(true);
	WrSerializer tmpWrser;
	Error err = it.GetJSON(tmpWrser, false);
	if (err.ok()) {
		err = itemImpl.FromJSON(tmpWrser.Slice());
	}
	if (err.ok()) {
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			std::ignore = itemImpl.GetCJSON(wrser);
		} else {
			std::ignore = itemImpl.GetCJSON(wrser);
		}
	}
	return err;
}

}  // namespace reindexer
