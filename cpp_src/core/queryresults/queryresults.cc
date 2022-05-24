#include "queryresults.h"
#include "core/query/query.h"
#include "joinresults.h"

namespace reindexer {

struct QueryResults::MergedData {
	MergedData(const std::string& ns, bool _haveRank, bool _needOutputRank)
		: pt(ns, {PayloadFieldType(KeyValueString, "-tuple", {}, false)}), haveRank(_haveRank), needOutputRank(_needOutputRank) {}

	std::string nsName;
	PayloadType pt;
	TagsMatcher tm;
	std::vector<AggregationResult> aggregationResults;
	bool haveRank = false;
	bool needOutputRank = false;
};

struct QueryResults::Iterator::JoinResStorage {
	void Clear() {
		jr.Clear();
		joinedRawData.clear();
	}

	joins::NamespaceResults jr;
	h_vector<ItemImplRawData, 1> joinedRawData;
};

template <typename DataT>
struct QueryResults::Iterator::ItemDataStorage {
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
QueryResults::QueryResults(QueryResults&&) = default;
QueryResults& QueryResults::operator=(QueryResults&& qr) = default;

void QueryResults::AddQr(LocalQueryResults&& local, int shardID, bool buildMergedData) {
	if (local_.has_value()) {
		throw Error(errLogic, "Query results already have incapsulated local query results");
	}
	if (lastSeenIdx_ > 0) {
		throw Error(
			errLogic,
			"Unable to add new local query results to general query results, because it was already read by someone (last seen idx: %d)",
			lastSeenIdx_);
	}
	if (NeedOutputShardId()) {
		local.SetOutputShardId(shardID);
	}
	local_.emplace(std::move(local));
	local_->shardID = shardID;
	switch (type_) {
		case Type::None:
			type_ = Type::Local;
			local_->hasCompatibleTm = true;
			break;
		case Type::SingleRemote:
		case Type::MultipleRemote:
			type_ = Type::Mixed;
			break;
		default:;
	}
	if (buildMergedData) {
		RebuildMergedData();
	}
	curQrId_ = findFirstQrWithItems();
}

void QueryResults::AddQr(client::SyncCoroQueryResults&& remote, int shardID, bool buildMergedData) {
	if (lastSeenIdx_ > 0) {
		throw Error(
			errLogic,
			"Unable to add new remote query results to general query results, because it was already read by someone (last seen idx: %d)",
			lastSeenIdx_);
	}
	remote_.emplace_back(std::move(remote));
	remote_.back().shardID = shardID;
	switch (type_) {
		case Type::None:
			type_ = Type::SingleRemote;
			remote_[0].hasCompatibleTm = true;
			break;
		case Type::SingleRemote:
			type_ = Type::MultipleRemote;
			remote_[0].hasCompatibleTm = false;
			break;
		case Type::Local:
			type_ = Type::Mixed;
			local_->hasCompatibleTm = false;
			break;
		default:;
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
			assertrx(local_.has_value());
			const auto nss = local_->qr.GetNamespaces();
			if (nss.size() > 1) {
				throw Error(errLogic, "Local query result has %d namespaces, but distributed query results may have only 1", nss.size());
			}
			mergedData_ = std::make_unique<MergedData>(std::string(nss[0]), local_->qr.haveRank, local_->qr.needOutputRank);
			auto& agg = local_->qr.GetAggregationResults();
			if (agg.size() > 1 || (agg.size() && agg[0].type != AggCount && agg[0].type != AggCountCached)) {
				throw Error(errLogic, "Local query result (within distributed results) has unsopported aggregations");
			}
			mergedData_->aggregationResults = agg;
		} else if (type_ != Type::MultipleRemote) {
			return;
		}

		assertrx(remote_.size());
		for (auto& qrp : remote_) {
			const auto nss = qrp.qr.GetNamespaces();
			if (nss.size() > 1) {
				throw Error(errLogic, "Remote query result has %d namespaces, but distributed query results may have only 1", nss.size());
			}
			auto& agg = qrp.qr.GetAggregationResults();
			if (agg.size() > 1 || (agg.size() && agg[0].type != AggCount && agg[0].type != AggCountCached)) {
				throw Error(errLogic, "Remote query result (within distributed results) has unsopported aggregations");
			}
			if (mergedData_) {
				if (mergedData_->pt.Name() != nss[0]) {
					auto mrName = mergedData_->pt.Name();
					throw Error(errLogic, "Query results in distributed query have different ns names: '%s' vs '%s'",
								mergedData_->pt.Name(), nss[0]);
				}
				if (mergedData_->aggregationResults.size() != agg.size() ||
					(agg.size() && mergedData_->aggregationResults[0].type != agg[0].type)) {
					throw Error(errLogic, "Aggregations are incompatible between query results inside distributed query results");
				}
				if (mergedData_->haveRank != qrp.qr.HaveRank() || mergedData_->needOutputRank != qrp.qr.NeedOutputRank()) {
					throw Error(errLogic, "Rank options are incompatible between query results inside distributed query results");
				}
				if (agg.size()) {
					assertrx(agg.size() == 1);
					assertf(agg[0].type == AggCount || agg[0].type == AggCountCached, "Actual type: %d", agg[0].type);
					mergedData_->aggregationResults[0].value += agg[0].value;
				}
			} else {
				mergedData_ = std::make_unique<MergedData>(std::string(nss[0]), qrp.qr.HaveRank(), qrp.qr.NeedOutputRank());
				mergedData_->aggregationResults = agg;
			}
		}

		assertrx(mergedData_);
		std::vector<TagsMatcher> tmList;
		tmList.reserve(remote_.size() + (local_.has_value() ? 1 : 0));
		if (local_.has_value()) {
			tmList.emplace_back(local_->qr.getTagsMatcher(0));
		}
		for (auto& qrp : remote_) {
			tmList.emplace_back(qrp.qr.GetTagsMatcher(0));
		}
		mergedData_->tm = TagsMatcher::CreateMergedTagsMatcher(mergedData_->pt, tmList);

		if (local_.has_value()) {
			local_->hasCompatibleTm = local_->qr.getTagsMatcher(0).IsSubsetOf(mergedData_->tm);
		}
		for (auto& qrp : remote_) {
			qrp.hasCompatibleTm = qrp.qr.GetTagsMatcher(0).IsSubsetOf(mergedData_->tm);
		}
	} catch (...) {
		mergedData_.reset();
		throw;
	}
}

void QueryResults::Clear() { *this = QueryResults(); }

int QueryResults::GetMergedNSCount() const noexcept {
	switch (type_) {
		case Type::None: {
			return 0;
		}
		case Type::Local: {
			return local_->qr.getMergedNSCount();
		}
		case Type::SingleRemote: {
			return remote_[0].qr.GetMergedNSCount();
		}
		default:
			return 1;  // No joined/merged nss in distributed qr
	}
}

const std::vector<AggregationResult>& QueryResults::GetAggregationResults() const {
	switch (type_) {
		case Type::None: {
			static std::vector<AggregationResult> kEmpty;
			return kEmpty;
		}
		case Type::Local: {
			return local_->qr.GetAggregationResults();
		}
		case Type::SingleRemote: {
			return remote_[0].qr.GetAggregationResults();
		}
		default:
			return getMergedData().aggregationResults;
	}
}

h_vector<std::string_view, 1> QueryResults::GetNamespaces() const {
	switch (type_) {
		case Type::None:
			return h_vector<std::string_view, 1>();
		case Type::Local:
			return local_->qr.GetNamespaces();
		case Type::SingleRemote:
			return remote_[0].qr.GetNamespaces();
		default:
			return h_vector<std::string_view, 1>{getMergedData().pt.Name()};
	}
}

bool QueryResults::IsCacheEnabled() const noexcept {
	switch (type_) {
		case Type::None:
			return true;
		case Type::Local:
			return local_->qr.IsCacheEnabled();
		default: {
			bool res = true;
			if (local_.has_value()) {
				res = res && local_->qr.IsCacheEnabled();
			}
			for (auto& qrp : remote_) {
				res = res && qrp.qr.IsCacheEnabled();
			}
			return res;
		}
	}
}

bool QueryResults::HaveShardIDs() const noexcept {
	if (local_.has_value() && local_->shardID != ShardingKeyType::ProxyOff) {
		return true;
	}
	for (auto& qrp : remote_) {
		if (qrp.shardID != ShardingKeyType::ProxyOff) {
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
			return local_->shardID;
		case Type::SingleRemote:
			return remote_[0].shardID;
		default:;
	}
	std::optional<int> shardId;
	if (local_.has_value()) {
		shardId = local_->shardID;
	}
	for (auto& qrp : remote_) {
		if (shardId.has_value() && qrp.shardID != *shardId) {
			throw Error(errLogic, "Distributed query results does not have common shard id (%d vs %d)", qrp.shardID, *shardId);
		}
	}
	return shardId.has_value() ? *shardId : ShardingKeyType::ProxyOff;
}

PayloadType QueryResults::GetPayloadType(int nsid) const {
	switch (type_) {
		case Type::None:
			return PayloadType();
		case Type::Local:
			return local_->qr.getPayloadType(nsid);
		case Type::SingleRemote:
			return remote_[0].qr.GetPayloadType(nsid);
		default:
			return getMergedData().pt;
	}
}

TagsMatcher QueryResults::GetTagsMatcher(int nsid) const {
	switch (type_) {
		case Type::None:
			return TagsMatcher();
		case Type::Local:
			return local_->qr.getTagsMatcher(nsid);
		case Type::SingleRemote:
			return remote_[0].qr.GetTagsMatcher(nsid);
		default:
			return getMergedData().tm;
	}
}

bool QueryResults::HaveRank() const {
	switch (type_) {
		case Type::None:
			return false;
		case Type::Local:
			return local_->qr.haveRank;
		case Type::SingleRemote:
			return remote_[0].qr.HaveRank();
		default:;
	}
	return getMergedData().haveRank;
}

bool QueryResults::NeedOutputRank() const {
	switch (type_) {
		case Type::None:
			return false;
		case Type::Local:
			return local_->qr.needOutputRank;
		case Type::SingleRemote:
			return remote_[0].qr.NeedOutputRank();
		default:;
	}
	return getMergedData().needOutputRank;
}

bool QueryResults::HaveJoined() const {
	switch (type_) {
		case Type::None:
			return false;
		case Type::Local:
			return local_->qr.joined_.size();
		case Type::SingleRemote:
			return remote_[0].qr.HaveJoined();
		default:;
	}
	return false;
}

void QueryResults::SetQuery(const Query* q) {
	if (q) {
		QueryData data;
		data.isWalQuery = q->IsWALQuery();
		data.joinedSize = uint16_t(q->joinQueries_.size());
		data.mergedJoinedSizes.reserve(q->mergeQueries_.size());
		for (const auto& mq : q->mergeQueries_) {
			data.mergedJoinedSizes.emplace_back(mq.joinQueries_.size());
		}
		qData_.emplace(std::move(data));
	} else {
		qData_.reset();
	}
}

uint32_t QueryResults::GetJoinedField(int parentNsId) const {
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
		joinedField = local_->qr.joined_.size();
		for (int ns = 0; ns < parentNsId; ++ns) {
			joinedField += local_->qr.joined_[size_t(ns)].GetJoinedSelectorsCount();
		}
	}
	return joinedField;
}

QueryResults::Iterator::ItemRefCache::ItemRefCache(IdType id, uint16_t proc, uint16_t nsid, ItemImpl&& i, bool raw)
	: itemImpl(std::move(i)), ref(id, itemImpl.payloadValue_, proc, nsid, raw) {}

Error QueryResults::Iterator::GetJSON(WrSerializer& wrser, bool withHdrLen) {
	try {
		auto vit = getVariantIt();
		if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
			return std::get<LocalQueryResults::Iterator>(vit).GetJSON(wrser, withHdrLen);
		}
		return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetJSON(wrser, withHdrLen);
	} catch (Error& e) {
		return e;
	}
}

Error QueryResults::Iterator::GetCJSON(WrSerializer& wrser, bool withHdrLen) {
	try {
		switch (qr_->type_) {
			case Type::None:
				return Error(errLogic, "QueryResults are empty");
			case Type::Local:
				return localIt_->GetCJSON(wrser, withHdrLen);
			default:;
		}

		auto vit = getVariantIt();
		if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
			if (qr_->local_->hasCompatibleTm) {
				return std::get<LocalQueryResults::Iterator>(vit).GetCJSON(wrser, withHdrLen);
			}
			return getCJSONviaJSON(wrser, withHdrLen, std::get<LocalQueryResults::Iterator>(vit));
		} else if (qr_->type_ == Type::SingleRemote) {
			return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetCJSON(wrser, withHdrLen);
		}

		if (qr_->remote_[size_t(qr_->curQrId_)].hasCompatibleTm) {
			return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetCJSON(wrser, withHdrLen);
		}
		return getCJSONviaJSON(wrser, withHdrLen, std::get<client::SyncCoroQueryResults::Iterator>(vit));
	} catch (Error& e) {
		return e;
	}
}

Error QueryResults::Iterator::GetMsgPack(WrSerializer& wrser, bool withHdrLen) {
	try {
		auto vit = getVariantIt();
		if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
			return std::get<LocalQueryResults::Iterator>(vit).GetMsgPack(wrser, withHdrLen);
		}
		return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetMsgPack(wrser, withHdrLen);
	} catch (Error& e) {
		return e;
	}
}

Error QueryResults::Iterator::GetProtobuf(WrSerializer& wrser, bool withHdrLen) {
	try {
		auto vit = getVariantIt();
		if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
			return std::get<LocalQueryResults::Iterator>(vit).GetProtobuf(wrser, withHdrLen);
		}
		return Error(errParams, "Protobuf is not supported for distributed and proxied queries");
		// return std::get<client::SyncCoroQueryResults::Iterator>(vit).GetProtobuf(wrser, withHdrLen);
	} catch (Error& e) {
		return e;
	}
}

Item QueryResults::Iterator::GetItem(bool enableHold) {
	try {
		switch (qr_->type_) {
			case Type::None:
				return Item();
			case Type::Local:
				return localIt_->GetItem(enableHold);
			default:;
		}

		auto vit = getVariantIt();
		std::unique_ptr<ItemImpl> itemImpl;
		if (qr_->type_ == Type::Mixed || qr_->type_ == Type::MultipleRemote) {
			auto& mData = qr_->getMergedData();
			itemImpl.reset(new ItemImpl(mData.pt, mData.tm));
		} else {
			auto& remoteQr = qr_->remote_[size_t(qr_->curQrId_)].qr;
			const int nsId = std::get<client::SyncCoroQueryResults::Iterator>(vit).GetNSID();
			itemImpl.reset(new ItemImpl(remoteQr.GetPayloadType(nsId), remoteQr.GetTagsMatcher(nsId)));
		}

		if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
			auto& lit = std::get<LocalQueryResults::Iterator>(vit);
			auto item = getItem(lit, std::move(itemImpl), !qr_->local_->hasCompatibleTm);
			item.setID(lit.GetItemRef().Id());
			item.setLSN(lit.GetItemRef().Value().GetLSN());
			item.setShardID(qr_->local_->shardID);
			return item;
		}
		auto& rit = std::get<client::SyncCoroQueryResults::Iterator>(vit);
		auto item = getItem(rit, std::move(itemImpl),
							!qr_->remote_[size_t(qr_->curQrId_)].hasCompatibleTm || !qr_->remote_[size_t(qr_->curQrId_)].qr.IsCJSON());
		item.setID(rit.GetID());
		assertrx(!rit.GetLSN().isEmpty());
		item.setLSN(rit.GetLSN());
		item.setShardID(rit.GetShardID());
		return item;
	} catch (Error& e) {
		return Item(e);
	}
}

joins::ItemIterator QueryResults::Iterator::GetJoined() {
	if (qr_->type_ == Type::Local) {
		return localIt_->GetJoined();
	} else if (qr_->type_ == Type::SingleRemote) {
		validateProxiedIterator();

		auto rit = qr_->remote_[0].it;
		const auto& joinedData = rit.GetJoined();
		if (!joinedData.size()) {
			return joins::ItemIterator::CreateEmpty();
		}
		if (!qr_->qData_.has_value()) {
			throw Error(errLogic, "Unable to init joined data without initial query");
		}

		if (!checkIfStorageHasSameIdx(nsJoinRes_)) {
			try {
				resetStorageData(nsJoinRes_);

				const auto& qData = qr_->qData_;
				if (rit.itemParams_.nsid >= int(qData->joinedSize)) {
					return reindexer::joins::ItemIterator::CreateEmpty();
				}
				nsJoinRes_->data.jr.SetJoinedSelectorsCount(qData->joinedSize);

				auto jField = qr_->GetJoinedField(rit.itemParams_.nsid);
				for (size_t i = 0; i < joinedData.size(); ++i, ++jField) {
					LocalQueryResults qrJoined;
					const auto& joinedItems = joinedData[i];
					for (const auto& itemData : joinedItems) {
						ItemImpl itemimpl(qr_->remote_[0].qr.GetPayloadType(jField), qr_->remote_[0].qr.GetTagsMatcher(jField));
						Error err = itemimpl.FromCJSON(itemData.data);
						if (!err.ok()) {
							throw err;
						}

						qrJoined.Add(ItemRef(itemData.id, itemimpl.Value(), itemData.proc, itemData.nsid, true));
						nsJoinRes_->data.joinedRawData.emplace_back(std::move(itemimpl));
					}
					nsJoinRes_->data.jr.Insert(rit.itemParams_.id, i, std::move(qrJoined));
				}
			} catch (...) {
				if (nsJoinRes_) {
					nsJoinRes_->idx = -1;
				}
				throw;
			}
		}

		return joins::ItemIterator(&(nsJoinRes_->data.jr), rit.itemParams_.id);
	}
	// Distributed queries can not have joins
	return reindexer::joins::ItemIterator::CreateEmpty();
}

ItemRef QueryResults::Iterator::GetItemRef(ProxiedRefsStorage* storage) {
	switch (qr_->type_) {
		case Type::None:
			return ItemRef();
		case Type::Local:
			return localIt_->GetItemRef();
		default:;
	}
	auto vit = getVariantIt();
	if (std::holds_alternative<LocalQueryResults::Iterator>(vit)) {
		return std::get<LocalQueryResults::Iterator>(vit).GetItemRef();
	}
	if (!checkIfStorageHasSameIdx(itemRefData_) || storage) {
		ItemImpl itemimpl(qr_->GetPayloadType(0), qr_->GetTagsMatcher(0));
		const bool converViaJSON =
			!qr_->remote_[size_t(qr_->curQrId_)].hasCompatibleTm || !qr_->remote_[size_t(qr_->curQrId_)].qr.IsCJSON();
		auto rit = std::get<client::SyncCoroQueryResults::Iterator>(vit);
		Error err = fillItemImpl(std::get<client::SyncCoroQueryResults::Iterator>(vit), itemimpl, converViaJSON);
		if (!err.ok()) {
			throw err;
		}

		if (!storage) {
			resetStorageData(itemRefData_, ItemRefCache(rit.GetID(), rit.GetRank(), rit.GetNSID(), std::move(itemimpl), IsRaw()));
			return itemRefData_->data.ref;
		} else {
			storage->emplace_back(rit.GetID(), rit.GetRank(), rit.GetNSID(), std::move(itemimpl), IsRaw());
			return storage->back().ref;
		}
	}
	return itemRefData_->data.ref;
}

const QueryResults::MergedData& QueryResults::getMergedData() const {
	if (!mergedData_) {
		throw Error(errLogic, "Distributed query results are incomplete. Merged data is empty");
	}
	return *mergedData_;
}

int QueryResults::findFirstQrWithItems() const noexcept {
	if (local_.has_value() && local_->qr.Count()) {
		return -1;
	}
	int ret = 0;
	for (auto& qrp : remote_) {
		if (qrp.qr.Count()) {
			return ret;
		}
		++ret;
	}
	return ret;
}

template <typename QrItT>
Item QueryResults::Iterator::getItem(QrItT& it, std::unique_ptr<ItemImpl>&& itemImpl, bool convertViaJSON) {
	auto err = fillItemImpl(it, *itemImpl, convertViaJSON);
	if (!err.ok()) return Item(err);
	return Item(itemImpl.release());
}

template <typename QrItT>
Error QueryResults::Iterator::fillItemImpl(QrItT& it, ItemImpl& itemImpl, bool convertViaJSON) {
	WrSerializer wrser;
	Error err;
	if (!convertViaJSON) {
		err = it.GetCJSON(wrser, false);
		if (err.ok()) err = itemImpl.FromCJSON(wrser.Slice());
	} else {
		err = it.GetJSON(wrser, false);
		if (err.ok()) err = itemImpl.FromJSON(wrser.Slice());
	}
	if (err.ok()) itemImpl.Value().SetLSN(it.GetLSN());
	return err;
}

template <typename QrItT>
Error QueryResults::Iterator::getCJSONviaJSON(WrSerializer& wrser, bool withHdrLen, QrItT& it) {
	auto& mData = qr_->getMergedData();
	ItemImpl itemImpl(mData.pt, mData.tm);
	itemImpl.Unsafe(true);
	WrSerializer tmpWrser;
	Error err = it.GetJSON(tmpWrser, false);
	if (err.ok()) err = itemImpl.FromJSON(tmpWrser.Slice());
	if (err.ok()) {
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			itemImpl.GetCJSON(wrser);
		} else {
			itemImpl.GetCJSON(wrser);
		}
	}
	return err;
}

}  // namespace reindexer
