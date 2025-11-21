#include "localqueryresults.h"
#include "additionaldatasource.h"
#include "cluster/sharding/sharding.h"
#include "core/cbinding/resultserializer.h"
#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/csvbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/itemimpl.h"
#include "core/namespace/namespace.h"
#include "estl/gift_str.h"
#include "joinresults.h"
#include "server/outputparameters.h"
#include "tools/catch_and_return.h"
#include "vendor/gason/gason.h"

namespace reindexer {

void LocalQueryResults::AddNamespace(NamespaceImplPtr ns, [[maybe_unused]] bool noLock) {
	assertrx(noLock);
	const NamespaceImpl* nsPtr = ns.get();
	auto strHolder = ns->strHolder();
	const auto it = std::find_if(nsData_.cbegin(), nsData_.cend(), [nsPtr](const NsDataHolder& nsData) { return nsData.ns == nsPtr; });
	if (it != nsData_.cend()) {
		assertrx(it->StrHolderPtr() == strHolder.get());
		return;
	}
	nsData_.emplace_back(std::move(ns), std::move(strHolder));
}

void LocalQueryResults::AddNamespace(NamespaceImpl* ns, [[maybe_unused]] bool noLock) {
	assertrx(noLock);
	auto strHolder = ns->strHolder();
	const auto it = std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder& nsData) { return nsData.ns == ns; });
	if (it != nsData_.cend()) {
		assertrx(it->StrHolderPtr() == strHolder.get());
		return;
	}
	nsData_.emplace_back(ns, std::move(strHolder));
}

void LocalQueryResults::RemoveNamespace(const NamespaceImpl* ns) {
	const auto it = std::find_if(nsData_.begin(), nsData_.end(), [ns](const NsDataHolder& nsData) { return nsData.ns == ns; });
	assertrx(it != nsData_.end());
	nsData_.erase(it);
}

struct [[nodiscard]] LocalQueryResults::Context {
	Context() = default;
	Context(PayloadType type, TagsMatcher tagsMatcher, FieldsFilter fieldsFilter, std::shared_ptr<const Schema> schema,
			lsn_t nsIncarnationTag)
		: type_(std::move(type)),
		  tagsMatcher_(std::move(tagsMatcher)),
		  fieldsFilter_(std::move(fieldsFilter)),
		  schema_(std::move(schema)),
		  nsIncarnationTag_(nsIncarnationTag) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsFilter fieldsFilter_;
	std::shared_ptr<const Schema> schema_;
	lsn_t nsIncarnationTag_;
};

#ifndef REINDEX_DEBUG_CONTAINERS
static_assert(LocalQueryResults::kSizeofContext >= sizeof(LocalQueryResults::Context),
			  "LocalQueryResults::kSizeofContext should >= sizeof(LocalQueryResults::Context)");
#endif	// REINDEX_DEBUG_CONTAINERS

LocalQueryResults::LocalQueryResults() = default;
LocalQueryResults::LocalQueryResults(LocalQueryResults&& obj) noexcept = default;

LocalQueryResults::LocalQueryResults(const ItemRefVector::ConstIterator& begin, const ItemRefVector::ConstIterator& end)
	: items_(begin, end) {}

LocalQueryResults& LocalQueryResults::operator=(LocalQueryResults&& obj) noexcept = default;

LocalQueryResults::~LocalQueryResults() = default;

void LocalQueryResults::Clear() { *this = LocalQueryResults(); }

// Used to save strings when converting the client result to the server.
// The server item is created, inserted into the result and deleted
// so that the rows are not deleted, they are saved in the results.
void LocalQueryResults::SaveRawData(ItemImplRawData&& rawData) { rawDataHolder_.emplace_back(std::move(rawData)); }

std::string LocalQueryResults::Dump() const {
	std::string buf;
	for (size_t i = 0; i < items_.Size(); ++i) {
		if (i != 0) {
			buf += ",";
		}
		buf += std::to_string(items_.GetItemRef(i).Id());
		if (joined_.empty()) {
			continue;
		}
		ConstIterator itemIt{*this, i};
		auto joinIt = itemIt.GetJoined();
		if (joinIt.getJoinedItemsCount() > 0) {
			buf += "[";
			for (auto fieldIt = joinIt.begin(); fieldIt != joinIt.end(); ++fieldIt) {
				if (fieldIt != joinIt.begin()) {
					buf += ";";
				}
				for (int j = 0; j < fieldIt.ItemsCount(); ++j) {
					if (j != 0) {
						buf += ",";
					}
					buf += std::to_string(fieldIt[j].Id());
				}
			}
			buf += "]";
		}
	}
	return buf;
}

h_vector<std::string_view, 1> LocalQueryResults::GetNamespaces() const {
	h_vector<std::string_view, 1> ret;
	ret.reserve(ctxs.size());
	for (auto& ctx : ctxs) {
		ret.push_back(ctx.type_.Name());
	}
	return ret;
}

NsShardsIncarnationTags LocalQueryResults::GetIncarnationTags() const {
	NsShardsIncarnationTags ret;
	auto& shardTags = ret.emplace_back();
	shardTags.tags.reserve(ctxs.size());
	for (auto& ctx : ctxs) {
		shardTags.tags.emplace_back(ctx.nsIncarnationTag_);
	}
	return ret;
}

int LocalQueryResults::GetJoinedNsCtxIndex(int nsid) const noexcept {
	int ctxIndex = joined_.size();
	for (int ns = 0; ns < nsid; ++ns) {
		ctxIndex += joined_[ns].GetJoinedSelectorsCount();
	}
	return ctxIndex;
}

class [[nodiscard]] LocalQueryResults::EncoderDatasourceWithJoins final : public IEncoderDatasourceWithJoins {
public:
	EncoderDatasourceWithJoins(const joins::ItemIterator& joinedItemIt, const ContextsVector& ctxs,
							   ConstIterator::NsNamesCache& nsNamesCache, int ctxIdx, size_t nsid, size_t joinedCount)
		: joinedItemIt_(joinedItemIt), ctxs_(ctxs), nsNamesCache_(nsNamesCache), ctxId_(ctxIdx), nsid_{nsid} {
		if (nsNamesCache.size() <= nsid_) {
			nsNamesCache.resize(nsid_ + 1);
		}
		if (nsNamesCache[nsid_].size() < joinedCount) {
			nsNamesCache[nsid_].clear();
			nsNamesCache[nsid_].reserve(joinedCount);
			fast_hash_map<std::string_view, int> namesCounters;
			assertrx_dbg(ctxs_.size() >= ctxId_ + joinedCount);
			for (size_t i = ctxId_, end = ctxId_ + joinedCount; i < end; ++i) {
				const std::string& n = ctxs_[i].type_.Name();
				if (auto [it, emplaced] = namesCounters.emplace(n, -1); !emplaced) {
					--it->second;
				}
			}
			for (size_t i = ctxId_, end = ctxId_ + joinedCount; i < end; ++i) {
				const std::string& n = ctxs_[i].type_.Name();
				int& count = namesCounters[n];
				if (count < 0) {
					if (count == -1) {
						nsNamesCache[nsid_].emplace_back(n);
					} else {
						count = 1;
						nsNamesCache[nsid_].emplace_back("1_" + n);
					}
				} else {
					nsNamesCache[nsid_].emplace_back(std::to_string(++count) + '_' + n);
				}
			}
		}
	}

	size_t GetJoinedRowsCount() const noexcept override { return joinedItemIt_.getJoinedFieldsCount(); }
	size_t GetJoinedRowItemsCount(size_t rowId) const override final {
		auto fieldIt = joinedItemIt_.at(rowId);
		return fieldIt.ItemsCount();
	}
	ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) override {
		auto fieldIt = joinedItemIt_.at(rowid);
		const ItemRef& itemRef = fieldIt[plIndex];
		const Context& ctx = ctxs_[ctxId_ + rowid];
		return ConstPayload(ctx.type_, itemRef.Value());
	}
	const TagsMatcher& GetJoinedItemTagsMatcher(size_t rowid) & noexcept override {
		const Context& ctx = ctxs_[ctxId_ + rowid];
		return ctx.tagsMatcher_;
	}
	const FieldsFilter& GetJoinedItemFieldsFilter(size_t rowid) & noexcept override {
		const Context& ctx = ctxs_[ctxId_ + rowid];
		return ctx.fieldsFilter_;
	}
	const std::string& GetJoinedItemNamespace(size_t rowid) & noexcept override { return nsNamesCache_[nsid_][rowid]; }

	auto GetJoinedItemNamespace(size_t) && = delete;
	auto GetJoinedItemTagsMatcher(size_t) && = delete;
	auto GetJoinedItemFieldsFilter(size_t) && = delete;

private:
	const joins::ItemIterator& joinedItemIt_;
	const ContextsVector& ctxs_;
	const ConstIterator::NsNamesCache& nsNamesCache_;
	const int ctxId_;
	const size_t nsid_;
};

void LocalQueryResults::encodeJSON(int idx, WrSerializer& ser, ConstIterator::NsNamesCache& nsNamesCache) const {
	auto& itemRef = items_.GetItemRef(idx);
	if (ctxs.size() <= itemRef.Nsid()) [[unlikely]] {
		assertrx_dbg(ctxs.size() > itemRef.Nsid());	 // This code should be unreachable in normal conditions
		throw Error(errAssert, "Do not have corresponding context for nsid: {} in LocalQueryResults; {} contextes total", itemRef.Nsid(),
					ctxs.size());
	}
	auto& ctx = ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		ser << "{}";
		return;
	}
	ConstPayload pl(ctx.type_, itemRef.Value());
	JsonEncoder encoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);
	JsonBuilder builder(ser, ObjType::TypePlain);
	if (!joined_.empty()) {
		joins::ItemIterator itemIt = (begin() + idx).GetJoined();
		if (itemIt.getJoinedItemsCount() > 0) {
			EncoderDatasourceWithJoins joinsDs(itemIt, ctxs, nsNamesCache, GetJoinedNsCtxIndex(itemRef.Nsid()), itemRef.Nsid(),
											   joined_[itemRef.Nsid()].GetJoinedSelectorsCount());
			h_vector<IAdditionalDatasource<JsonBuilder>*, 2> dss;
			AdditionalDatasource ds =
				needOutputRank ? AdditionalDatasource(items_.GetItemRefRanked(idx).Rank(), &joinsDs) : AdditionalDatasource(&joinsDs);
			dss.push_back(&ds);
			AdditionalDatasourceShardId dsShardId(outputShardId);
			if (outputShardId >= 0) {
				dss.push_back(&dsShardId);
			}
			encoder.Encode(pl, builder, dss);

			return;
		}
	}

	h_vector<IAdditionalDatasource<JsonBuilder>*, 2> dss;

	std::optional<AdditionalDatasource> ds;
	if (needOutputRank) {
		ds = AdditionalDatasource{items_.GetItemRefRanked(idx).Rank(), nullptr};
		dss.push_back(&*ds);
	}
	AdditionalDatasourceShardId dsShardId(outputShardId);
	if (outputShardId >= 0) {
		dss.push_back(&dsShardId);
	}

	encoder.Encode(pl, builder, dss);
}

template <typename QR>
joins::ItemIterator LocalQueryResults::IteratorImpl<QR>::GetJoined() {
	return reindexer::joins::ItemIterator::CreateFrom(*this);
}

template <typename QR>
Error LocalQueryResults::IteratorImpl<QR>::GetMsgPack(WrSerializer& wrser, bool withHdrLen) noexcept {
	try {
		auto& itemRef = qr_->items_.GetItemRef(idx_);
		assertrx(qr_->ctxs.size() > itemRef.Nsid());
		const auto& ctx = qr_->ctxs[itemRef.Nsid()];

		if (itemRef.Value().IsFree()) {
			MsgPackBuilder msgpackBuilder(wrser, ObjType::TypePlain, 0);
			if (withHdrLen) {
				auto slicePosSaver = wrser.StartSlice();
				std::ignore = msgpackBuilder.Object(TagName::Empty(), 0);
			} else {
				std::ignore = msgpackBuilder.Object(TagName::Empty(), 0);
			}
			return {};
		}

		ConstPayload pl(ctx.type_, itemRef.Value());
		MsgPackEncoder msgpackEncoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);
		const TagsLengths& tagsLengths = msgpackEncoder.GetTagsMeasures(pl);
		int startTag = 0;
		MsgPackBuilder msgpackBuilder(wrser, &tagsLengths, &startTag, ObjType::TypePlain, const_cast<TagsMatcher*>(&ctx.tagsMatcher_));
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			msgpackEncoder.Encode(pl, msgpackBuilder);
		} else {
			msgpackEncoder.Encode(pl, msgpackBuilder);
		}
	} catch (std::exception& err) {
		err_ = std::move(err);
		return err_;
	}
	return {};
}

template <typename QR>
Error LocalQueryResults::IteratorImpl<QR>::GetProtobuf(WrSerializer& wrser) noexcept {
	try {
		auto& itemRef = qr_->items_.GetItemRef(idx_);
		assertrx(qr_->ctxs.size() > itemRef.Nsid());
		const auto& ctx = qr_->ctxs[itemRef.Nsid()];
		if (!ctx.schema_) {
			return Error(errParams, "The schema was not found for Protobuf builder");
		}

		ProtobufEncoder encoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);
		ProtobufBuilder builder(&wrser, ObjType::TypePlain, ctx.schema_.get(), const_cast<TagsMatcher*>(&ctx.tagsMatcher_));

		auto item = builder.Object(kProtoQueryResultsFields.at(kParamItems));
		auto ItemImpl = item.Object(TagName(ctx.schema_->GetProtobufNsNumber() + 1));

		if (itemRef.Value().IsFree()) {
			return {};
		}

		ConstPayload pl(ctx.type_, itemRef.Value());
		encoder.Encode(pl, builder);
	} catch (std::exception& err) {
		err_ = std::move(err);
		return err_;
	}
	return {};
}

template <typename QR>
Error LocalQueryResults::IteratorImpl<QR>::GetJSON(WrSerializer& ser, bool withHdrLen) noexcept {
	try {
		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			qr_->encodeJSON(idx_, ser, nsNamesCache);
		} else {
			qr_->encodeJSON(idx_, ser, nsNamesCache);
		}
	} catch (std::exception& err) {
		err_ = std::move(err);
		return err_;
	}
	return {};
}

CsvOrdering LocalQueryResults::MakeCSVTagOrdering(unsigned limit, unsigned offset) const {
	const auto& fieldsFilter = ctxs[0].fieldsFilter_;
	const auto* regularFieldsFilter = fieldsFilter.TryRegularFields();
	const auto* vectorFieldsFilter = fieldsFilter.TryVectorFields();
	if (regularFieldsFilter && vectorFieldsFilter) {
		std::vector<TagName> ordering;
		ordering.reserve(regularFieldsFilter->size() + vectorFieldsFilter->size());
		for (const auto& tag : *regularFieldsFilter) {
			if (tag > 0) {
				ordering.emplace_back(tag);
			}
		}
		for (const auto& tag : *vectorFieldsFilter) {
			if (tag > 0) {
				ordering.emplace_back(tag);
			}
		}
		return ordering;
	}

	std::vector<TagName> ordering;
	ordering.reserve(128);
	fast_hash_set<TagName, TagName::Hash> fieldsTmIds;
	WrSerializer ser;
	const auto& tm = getTagsMatcher(0);
	Iterator::NsNamesCache nsNamesCache;
	for (size_t i = offset; i < items_.Size() && i < offset + limit; ++i) {
		ser.Reset();
		encodeJSON(i, ser, nsNamesCache);

		gason::JsonParser parser;
		auto jsonNode = parser.Parse(giftStr(ser.Slice()));

		for (const auto& child : jsonNode) {
			auto [it, inserted] = fieldsTmIds.insert(tm.name2tag(child.key));
			if (inserted && !it->IsEmpty()) {
				ordering.emplace_back(*it);
			}
		}
	}
	return ordering;
}

template <typename QR>
Error LocalQueryResults::IteratorImpl<QR>::GetCSV(WrSerializer& ser, CsvOrdering& ordering) noexcept {
	try {
		auto& itemRef = qr_->items_.GetItemRef(idx_);
		assertrx(qr_->ctxs.size() > itemRef.Nsid());
		auto& ctx = qr_->ctxs[itemRef.Nsid()];

		if (itemRef.Value().IsFree()) {
			return Error(errNotFound, "Item not found");
		}

		ConstPayload pl(ctx.type_, itemRef.Value());
		CsvBuilder builder(ser, ordering);
		CsvEncoder encoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

		if (!qr_->joined_.empty()) {
			joins::ItemIterator itemIt = (qr_->begin() + idx_).GetJoined();
			if (itemIt.getJoinedItemsCount() > 0) {
				EncoderDatasourceWithJoins joinsDs(itemIt, qr_->ctxs, nsNamesCache, qr_->GetJoinedNsCtxIndex(itemRef.Nsid()),
												   itemRef.Nsid(), qr_->joined_[itemRef.Nsid()].GetJoinedSelectorsCount());
				h_vector<IAdditionalDatasource<CsvBuilder>*, 2> dss;
				AdditionalDatasourceCSV ds(&joinsDs);
				dss.push_back(&ds);
				encoder.Encode(pl, builder, dss);
				return errOK;
			}
		}

		encoder.Encode(pl, builder);
	}
	CATCH_AND_RETURN
	return errOK;
}

template <typename QR>
Error LocalQueryResults::IteratorImpl<QR>::GetCJSON(WrSerializer& ser, bool withHdrLen) noexcept {
	try {
		auto& itemRef = qr_->items_.GetItemRef(idx_);
		const auto nsid = itemRef.Nsid();
		assertrx(qr_->ctxs.size() > nsid);
		const auto& ctx = qr_->ctxs[nsid];

		if (itemRef.Value().IsFree()) {
			return Error(errNotFound, "Item not found");
		}

		ConstPayload pl(ctx.type_, itemRef.Value());
		CJsonBuilder builder(ser, ObjType::TypePlain);
		CJsonEncoder cjsonEncoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			cjsonEncoder.Encode(pl, builder);
		} else {
			cjsonEncoder.Encode(pl, builder);
		}
	} catch (std::exception& err) {
		err_ = std::move(err);
		return err_;
	}
	return {};
}

template <typename QR>
Item LocalQueryResults::IteratorImpl<QR>::GetItem(bool enableHold) {
	auto& itemRef = qr_->items_.GetItemRef(idx_);

	if (qr_->ctxs.size() <= itemRef.Nsid()) {
		return Item(Error(errNotFound, "QueryResults does not have namespace context and unable to create Item object"));
	}
	const auto& ctx = qr_->ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		return Item(Error(errNotFound, "Item not found"));
	}

	auto item = Item(ctx.type_, itemRef.Value(), ctx.tagsMatcher_, ctx.schema_, qr_->getFieldsFilter(itemRef.Nsid()));
	item.impl_->payloadValue_.Clone();
	if (enableHold) {
		if (!item.impl_->holder_) {
			item.impl_->holder_ = std::make_unique<ItemImplRawData::HolderT>();
		}
		Payload{ctx.type_, item.impl_->payloadValue_}.CopyStrings(*(item.impl_->holder_));
	}

	item.setID(itemRef.Id());
	return item;
}

void LocalQueryResults::AddItemNoHold(Item& item, lsn_t nsIncarnationTag, bool withData) {
	if (item.GetID() != -1) {
		auto ritem = item.impl_;
		if (ctxs.empty()) {
			ctxs.emplace_back(ritem->Type(), ritem->tagsMatcher(), FieldsFilter(), ritem->GetSchema(), nsIncarnationTag);
		}
		if (withData) {
			auto& value = ritem->RealValue().IsFree() ? ritem->Value() : ritem->RealValue();
			AddItemRef(item.GetID(), value);
		} else {
			AddItemRef(item.GetID(), PayloadValue());
		}
	}
}

const TagsMatcher& LocalQueryResults::getTagsMatcher(int nsid) const& noexcept { return ctxs[nsid].tagsMatcher_; }

const PayloadType& LocalQueryResults::getPayloadType(int nsid) const& noexcept { return ctxs[nsid].type_; }

const FieldsFilter& LocalQueryResults::getFieldsFilter(int nsid) const& noexcept {
	assertrx(size_t(nsid) < ctxs.size());
	return ctxs[nsid].fieldsFilter_;
}

TagsMatcher& LocalQueryResults::getTagsMatcher(int nsid) & noexcept { return ctxs[nsid].tagsMatcher_; }

PayloadType& LocalQueryResults::getPayloadType(int nsid) & noexcept { return ctxs[nsid].type_; }

std::shared_ptr<const Schema> LocalQueryResults::getSchema(int nsid) const noexcept { return ctxs[nsid].schema_; }

int LocalQueryResults::getNsNumber(int nsid) const noexcept {
	assertrx(ctxs[nsid].schema_);
	return ctxs[nsid].schema_->GetProtobufNsNumber();
}

int LocalQueryResults::getMergedNSCount() const noexcept { return ctxs.size(); }

void LocalQueryResults::addNSContext(const PayloadType& type, const TagsMatcher& tagsMatcher, const FieldsFilter& filter,
									 std::shared_ptr<const Schema> schema, lsn_t nsIncarnationTag) {
	nonCacheableData = nonCacheableData || filter.HasTagsPaths();

	ctxs.emplace_back(type, tagsMatcher, filter, std::move(schema), std::move(nsIncarnationTag));
}

void LocalQueryResults::addNSContext(const QueryResults& baseQr, size_t nsid, lsn_t nsIncarnationTag) {
	addNSContext(baseQr.GetPayloadType(nsid), baseQr.GetTagsMatcher(nsid), baseQr.GetFieldsFilter(nsid), baseQr.GetSchema(nsid),
				 std::move(nsIncarnationTag));
}

LocalQueryResults::NsDataHolder::NsDataHolder(LocalQueryResults::NamespaceImplPtr&& _ns, StringsHolderPtr&& strHldr) noexcept
	: ns(_ns.get()), nsPtr_{std::move(_ns)}, strHolder_{std::move(strHldr)} {}

LocalQueryResults::NsDataHolder::NsDataHolder(NamespaceImpl* _ns, StringsHolderPtr&& strHldr) noexcept
	: ns(_ns), strHolder_(std::move(strHldr)) {}

template class LocalQueryResults::IteratorImpl<const LocalQueryResults>;
template class LocalQueryResults::IteratorImpl<LocalQueryResults>;

}  // namespace reindexer
