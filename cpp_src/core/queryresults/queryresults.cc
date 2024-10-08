#include "core/queryresults/queryresults.h"
#include "core/cbinding/resultserializer.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/csvbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/itemimpl.h"
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "joinresults.h"
#include "server/outputparameters.h"
#include "tools/catch_and_return.h"

namespace reindexer {

void QueryResults::AddNamespace(NamespaceImplPtr ns, [[maybe_unused]] bool noLock) {
	assertrx(noLock);
	const NamespaceImpl* nsPtr = ns.get();
	auto strHolder = ns->strHolder();
	const auto it = std::find_if(nsData_.cbegin(), nsData_.cend(), [nsPtr](const NsDataHolder& nsData) { return nsData.ns == nsPtr; });
	if (it != nsData_.cend()) {
		assertrx(it->strHolder.get() == strHolder.get());
		return;
	}
	nsData_.emplace_back(std::move(ns), std::move(strHolder));
}

void QueryResults::AddNamespace(NamespaceImpl* ns, [[maybe_unused]] bool noLock) {
	assertrx(noLock);
	auto strHolder = ns->strHolder();
	const auto it = std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder& nsData) { return nsData.ns == ns; });
	if (it != nsData_.cend()) {
		assertrx(it->strHolder.get() == strHolder.get());
		return;
	}
	nsData_.emplace_back(ns, std::move(strHolder));
}

void QueryResults::RemoveNamespace(const NamespaceImpl* ns) {
	const auto it = std::find_if(nsData_.begin(), nsData_.end(), [ns](const NsDataHolder& nsData) { return nsData.ns == ns; });
	assertrx(it != nsData_.end());
	nsData_.erase(it);
}

struct QueryResults::Context {
	Context() = default;
	Context(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet& fieldsFilter, std::shared_ptr<const Schema> schema)
		: type_(std::move(type)), tagsMatcher_(std::move(tagsMatcher)), fieldsFilter_(fieldsFilter), schema_(std::move(schema)) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsSet fieldsFilter_;
	std::shared_ptr<const Schema> schema_;
};

static_assert(QueryResults::kSizeofContext >= sizeof(QueryResults::Context),
			  "QueryResults::kSizeofContext should >= sizeof(QueryResults::Context)");

QueryResults::QueryResults(std::initializer_list<ItemRef> l) : items_(l) {}
QueryResults::QueryResults(int /*flags*/) {}
QueryResults::QueryResults(QueryResults&& obj) noexcept
	: joined_(std::move(obj.joined_)),
	  aggregationResults(std::move(obj.aggregationResults)),
	  totalCount(obj.totalCount),
	  haveRank(obj.haveRank),
	  nonCacheableData(obj.nonCacheableData),
	  needOutputRank(obj.needOutputRank),
	  ctxs(std::move(obj.ctxs)),
	  explainResults(std::move(obj.explainResults)),
	  items_(std::move(obj.items_)),
	  activityCtx_(std::move(obj.activityCtx_)),
	  isWalQuery_(obj.isWalQuery_),
	  nsData_(std::move(obj.nsData_)),
	  stringsHolder_(std::move(obj.stringsHolder_)) {
	obj.isWalQuery_ = false;
}

QueryResults::QueryResults(const ItemRefVector::const_iterator& begin, const ItemRefVector::const_iterator& end) : items_(begin, end) {}

QueryResults& QueryResults::operator=(QueryResults&& obj) noexcept {
	if (this != &obj) {
		items_ = std::move(obj.items_);
		assertrx(!obj.items_.size());
		joined_ = std::move(obj.joined_);
		aggregationResults = std::move(obj.aggregationResults);
		totalCount = obj.totalCount;
		haveRank = obj.haveRank;
		needOutputRank = obj.needOutputRank;
		ctxs = std::move(obj.ctxs);
		nonCacheableData = obj.nonCacheableData;
		explainResults = std::move(obj.explainResults);
		nsData_ = std::move(obj.nsData_);
		stringsHolder_ = std::move(obj.stringsHolder_);
		activityCtx_.reset();
		if (obj.activityCtx_) {
			activityCtx_.emplace(std::move(*obj.activityCtx_));
			obj.activityCtx_.reset();
		}
		isWalQuery_ = obj.isWalQuery_;
		obj.isWalQuery_ = false;
	}
	return *this;
}

QueryResults::~QueryResults() = default;

void QueryResults::Clear() { *this = QueryResults(); }

void QueryResults::Erase(ItemRefVector::iterator start, ItemRefVector::iterator finish) { items_.erase(start, finish); }

void QueryResults::Add(const ItemRef& i) { items_.push_back(i); }

std::string QueryResults::Dump() const {
	std::string buf;
	for (size_t i = 0; i < items_.size(); ++i) {
		if (&items_[i] != &*items_.begin()) {
			buf += ",";
		}
		buf += std::to_string(items_[i].Id());
		if (joined_.empty()) {
			continue;
		}
		Iterator itemIt{this, int(i), errOK, {}};
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

h_vector<std::string_view, 1> QueryResults::GetNamespaces() const {
	h_vector<std::string_view, 1> ret;
	ret.reserve(ctxs.size());
	for (auto& ctx : ctxs) {
		ret.push_back(ctx.type_.Name());
	}
	return ret;
}

int QueryResults::GetJoinedNsCtxIndex(int nsid) const noexcept {
	int ctxIndex = joined_.size();
	for (int ns = 0; ns < nsid; ++ns) {
		ctxIndex += joined_[ns].GetJoinedSelectorsCount();
	}
	return ctxIndex;
}

class QueryResults::EncoderDatasourceWithJoins final : public IEncoderDatasourceWithJoins {
public:
	EncoderDatasourceWithJoins(const joins::ItemIterator& joinedItemIt, const ContextsVector& ctxs, Iterator::NsNamesCache& nsNamesCache,
							   int ctxIdx, size_t nsid, size_t joinedCount) noexcept
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
	~EncoderDatasourceWithJoins() override = default;

	size_t GetJoinedRowsCount() const noexcept override final { return joinedItemIt_.getJoinedFieldsCount(); }
	size_t GetJoinedRowItemsCount(size_t rowId) const override final {
		auto fieldIt = joinedItemIt_.at(rowId);
		return fieldIt.ItemsCount();
	}
	ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) const override final {
		auto fieldIt = joinedItemIt_.at(rowid);
		const ItemRef& itemRef = fieldIt[plIndex];
		const Context& ctx = ctxs_[ctxId_ + rowid];
		return ConstPayload(ctx.type_, itemRef.Value());
	}
	const TagsMatcher& GetJoinedItemTagsMatcher(size_t rowid) noexcept override final {
		const Context& ctx = ctxs_[ctxId_ + rowid];
		return ctx.tagsMatcher_;
	}
	virtual const FieldsSet& GetJoinedItemFieldsFilter(size_t rowid) noexcept override final {
		const Context& ctx = ctxs_[ctxId_ + rowid];
		return ctx.fieldsFilter_;
	}
	const std::string& GetJoinedItemNamespace(size_t rowid) const noexcept override final { return nsNamesCache_[nsid_][rowid]; }

private:
	const joins::ItemIterator& joinedItemIt_;
	const ContextsVector& ctxs_;
	const Iterator::NsNamesCache& nsNamesCache_;
	const int ctxId_;
	const size_t nsid_;
};

class AdditionalDatasource : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalDatasource(double r, IEncoderDatasourceWithJoins* jds) noexcept : joinsDs_(jds), withRank_(true), rank_(r) {}
	AdditionalDatasource(IEncoderDatasourceWithJoins* jds) noexcept : joinsDs_(jds), withRank_(false), rank_(0.0) {}
	void PutAdditionalFields(JsonBuilder& builder) const override final {
		if (withRank_) {
			builder.Put("rank()", rank_);
		}
	}
	IEncoderDatasourceWithJoins* GetJoinsDatasource() noexcept override final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins* joinsDs_;
	bool withRank_;
	double rank_;
};

class AdditionalDatasourceCSV : public IAdditionalDatasource<CsvBuilder> {
public:
	AdditionalDatasourceCSV(IEncoderDatasourceWithJoins* jds) noexcept : joinsDs_(jds) {}
	void PutAdditionalFields(CsvBuilder&) const override final {}
	IEncoderDatasourceWithJoins* GetJoinsDatasource() noexcept override final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins* joinsDs_;
};

void QueryResults::encodeJSON(int idx, WrSerializer& ser, Iterator::NsNamesCache& nsNamesCache) const {
	auto& itemRef = items_[idx];
	assertrx(ctxs.size() > itemRef.Nsid());
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
			if (needOutputRank) {
				AdditionalDatasource ds(itemRef.Proc(), &joinsDs);
				encoder.Encode(pl, builder, &ds);
			} else {
				AdditionalDatasource ds(&joinsDs);
				encoder.Encode(pl, builder, &ds);
			}
			return;
		}
	}
	if (needOutputRank) {
		AdditionalDatasource ds(itemRef.Proc(), nullptr);
		encoder.Encode(pl, builder, &ds);
	} else {
		encoder.Encode(pl, builder);
	}
}

joins::ItemIterator QueryResults::Iterator::GetJoined() { return reindexer::joins::ItemIterator::CreateFrom(*this); }

Error QueryResults::Iterator::GetMsgPack(WrSerializer& wrser, bool withHdrLen) {
	auto& itemRef = qr_->items_[idx_];
	assertrx(qr_->ctxs.size() > itemRef.Nsid());
	auto& ctx = qr_->ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		return Error(errNotFound, "Item not found");
	}

	int startTag = 0;
	ConstPayload pl(ctx.type_, itemRef.Value());
	MsgPackEncoder msgpackEncoder(&ctx.tagsMatcher_);
	const TagsLengths& tagsLengths = msgpackEncoder.GetTagsMeasures(pl);
	MsgPackBuilder msgpackBuilder(wrser, &tagsLengths, &startTag, ObjType::TypePlain, const_cast<TagsMatcher*>(&ctx.tagsMatcher_));
	if (withHdrLen) {
		auto slicePosSaver = wrser.StartSlice();
		msgpackEncoder.Encode(pl, msgpackBuilder);
	} else {
		msgpackEncoder.Encode(pl, msgpackBuilder);
	}
	return errOK;
}

Error QueryResults::Iterator::GetProtobuf(WrSerializer& wrser, bool withHdrLen) {
	auto& itemRef = qr_->items_[idx_];
	assertrx(qr_->ctxs.size() > itemRef.Nsid());
	auto& ctx = qr_->ctxs[itemRef.Nsid()];
	if (!ctx.schema_) {
		return Error(errParams, "The schema was not found for Protobuf builder");
	}

	if (itemRef.Value().IsFree()) {
		return Error(errNotFound, "Item not found");
	}

	ConstPayload pl(ctx.type_, itemRef.Value());
	ProtobufEncoder encoder(&ctx.tagsMatcher_);
	ProtobufBuilder builder(&wrser, ObjType::TypePlain, ctx.schema_.get(), const_cast<TagsMatcher*>(&ctx.tagsMatcher_));

	auto item = builder.Object(kProtoQueryResultsFields.at(kParamItems));
	auto ItemImpl = item.Object(ctx.schema_->GetProtobufNsNumber() + 1);

	if (withHdrLen) {
		auto slicePosSaver = wrser.StartSlice();
		encoder.Encode(pl, builder);
	} else {
		encoder.Encode(pl, builder);
	}

	return errOK;
}

Error QueryResults::Iterator::GetJSON(WrSerializer& ser, bool withHdrLen) {
	try {
		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			qr_->encodeJSON(idx_, ser, nsNamesCache);
		} else {
			qr_->encodeJSON(idx_, ser, nsNamesCache);
		}
	} catch (const Error& err) {
		err_ = err;
		return err;
	}
	return errOK;
}

CsvOrdering QueryResults::MakeCSVTagOrdering(unsigned limit, unsigned offset) const {
	if (!ctxs[0].fieldsFilter_.empty()) {
		std::vector<int> ordering;
		ordering.reserve(ctxs[0].fieldsFilter_.size());
		for (const auto& tag : ctxs[0].fieldsFilter_) {
			ordering.emplace_back(tag);
		}
		return ordering;
	}

	std::vector<int> ordering;
	ordering.reserve(128);
	fast_hash_set<int> fieldsTmIds;
	WrSerializer ser;
	const auto& tm = getTagsMatcher(0);
	Iterator::NsNamesCache nsNamesCache;
	for (size_t i = offset; i < items_.size() && i < offset + limit; ++i) {
		ser.Reset();
		encodeJSON(i, ser, nsNamesCache);

		gason::JsonParser parser;
		auto jsonNode = parser.Parse(giftStr(ser.Slice()));

		for (const auto& child : jsonNode) {
			auto [it, inserted] = fieldsTmIds.insert(tm.name2tag(child.key));
			if (inserted && *it > 0) {
				ordering.emplace_back(*it);
			}
		}
	}
	return ordering;
}

Error QueryResults::Iterator::GetCSV(WrSerializer& ser, CsvOrdering& ordering) noexcept {
	try {
		auto& itemRef = qr_->items_[idx_];
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
				AdditionalDatasourceCSV ds(&joinsDs);
				encoder.Encode(pl, builder, &ds);
				return errOK;
			}
		}

		encoder.Encode(pl, builder);
	}
	CATCH_AND_RETURN
	return errOK;
}

Error QueryResults::Iterator::GetCJSON(WrSerializer& ser, bool withHdrLen) {
	try {
		auto& itemRef = qr_->items_[idx_];
		assertrx(qr_->ctxs.size() > itemRef.Nsid());
		auto& ctx = qr_->ctxs[itemRef.Nsid()];

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
	} catch (const Error& err) {
		err_ = err;
		return err;
	}
	return errOK;
}

Item QueryResults::Iterator::GetItem(bool enableHold) {
	auto& itemRef = qr_->items_[idx_];

	assertrx(qr_->ctxs.size() > itemRef.Nsid());
	auto& ctx = qr_->ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		return Item(Error(errNotFound, "Item not found"));
	}

	auto item = Item(new ItemImpl(ctx.type_, itemRef.Value(), ctx.tagsMatcher_, ctx.schema_));
	item.impl_->payloadValue_.Clone();
	if (enableHold) {
		if (!item.impl_->keyStringsHolder_) {
			item.impl_->keyStringsHolder_.reset(new std::vector<key_string>);
		}
		Payload{ctx.type_, item.impl_->payloadValue_}.CopyStrings(*(item.impl_->keyStringsHolder_));
	}

	item.setID(itemRef.Id());
	return item;
}

void QueryResults::AddItem(Item& item, bool withData, bool enableHold) {
	auto ritem = item.impl_;
	if (item.GetID() != -1) {
		if (ctxs.empty()) {
			ctxs.push_back(Context(ritem->Type(), ritem->tagsMatcher(), FieldsSet(), ritem->GetSchema()));
		}
		Add(ItemRef(item.GetID(), withData ? (ritem->RealValue().IsFree() ? ritem->Value() : ritem->RealValue()) : PayloadValue()));
		if (withData && enableHold) {
			if (auto ns{ritem->GetNamespace()}; ns) {
				Payload{ns->ns_->payloadType_, items_.back().Value()}.CopyStrings(stringsHolder_);
			} else {
				assertrx(ctxs.size() == 1);
				Payload{ctxs.back().type_, items_.back().Value()}.CopyStrings(stringsHolder_);
			}
		}
	}
}

const TagsMatcher& QueryResults::getTagsMatcher(int nsid) const noexcept { return ctxs[nsid].tagsMatcher_; }

const PayloadType& QueryResults::getPayloadType(int nsid) const noexcept { return ctxs[nsid].type_; }

const FieldsSet& QueryResults::getFieldsFilter(int nsid) const noexcept { return ctxs[nsid].fieldsFilter_; }

TagsMatcher& QueryResults::getTagsMatcher(int nsid) noexcept { return ctxs[nsid].tagsMatcher_; }

PayloadType& QueryResults::getPayloadType(int nsid) noexcept { return ctxs[nsid].type_; }

std::shared_ptr<const Schema> QueryResults::getSchema(int nsid) const noexcept { return ctxs[nsid].schema_; }

int QueryResults::getNsNumber(int nsid) const noexcept {
	assertrx(ctxs[nsid].schema_);
	return ctxs[nsid].schema_->GetProtobufNsNumber();
}

void QueryResults::addNSContext(const PayloadType& type, const TagsMatcher& tagsMatcher, const FieldsSet& filter,
								std::shared_ptr<const Schema> schema) {
	if (filter.getTagsPathsLength()) {
		nonCacheableData = true;
	}

	ctxs.push_back(Context(type, tagsMatcher, filter, std::move(schema)));
}

QueryResults::NsDataHolder::NsDataHolder(QueryResults::NamespaceImplPtr&& _ns, StringsHolderPtr&& strHldr) noexcept
	: nsPtr_{std::move(_ns)}, ns(nsPtr_.get()), strHolder{std::move(strHldr)} {}

QueryResults::NsDataHolder::NsDataHolder(NamespaceImpl* _ns, StringsHolderPtr&& strHldr) noexcept
	: ns(_ns), strHolder(std::move(strHldr)) {}

}  // namespace reindexer
