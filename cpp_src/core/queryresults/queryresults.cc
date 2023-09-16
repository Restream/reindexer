#include "core/queryresults/queryresults.h"
#include "core/cbinding/resultserializer.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/csvbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/itemimpl.h"
#include "core/namespace/namespace.h"
#include "joinresults.h"
#include "tools/catch_and_return.h"
#include "tools/logger.h"

namespace reindexer {

void QueryResults::AddNamespace(NamespaceImplPtr ns, [[maybe_unused]] bool noLock) {
	assertrx(noLock);
	const NamespaceImpl *nsPtr = ns.get();
	auto strHolder = ns->strHolder();
	const auto it = std::find_if(nsData_.cbegin(), nsData_.cend(), [nsPtr](const NsDataHolder &nsData) { return nsData.ns == nsPtr; });
	if (it != nsData_.cend()) {
		assertrx(it->strHolder.get() == strHolder.get());
		return;
	}
	nsData_.emplace_back(std::move(ns), std::move(strHolder));
}

void QueryResults::AddNamespace(NamespaceImpl *ns, [[maybe_unused]] bool noLock) {
	assertrx(noLock);
	auto strHolder = ns->strHolder();
	const auto it = std::find_if(nsData_.cbegin(), nsData_.cend(), [ns](const NsDataHolder &nsData) { return nsData.ns == ns; });
	if (it != nsData_.cend()) {
		assertrx(it->strHolder.get() == strHolder.get());
		return;
	}
	nsData_.emplace_back(ns, std::move(strHolder));
}

void QueryResults::RemoveNamespace(const NamespaceImpl *ns) {
	const auto it = std::find_if(nsData_.begin(), nsData_.end(), [ns](const NsDataHolder &nsData) { return nsData.ns == ns; });
	assertrx(it != nsData_.end());
	nsData_.erase(it);
}

struct QueryResults::Context {
	Context() = default;
	Context(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet &fieldsFilter, std::shared_ptr<const Schema> schema)
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
QueryResults::QueryResults(QueryResults &&obj) noexcept
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

QueryResults::QueryResults(const ItemRefVector::const_iterator &begin, const ItemRefVector::const_iterator &end) : items_(begin, end) {}

QueryResults &QueryResults::operator=(QueryResults &&obj) noexcept {
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

void QueryResults::Add(const ItemRef &i) { items_.push_back(i); }

std::string QueryResults::Dump() const {
	std::string buf;
	for (size_t i = 0; i < items_.size(); ++i) {
		if (&items_[i] != &*items_.begin()) buf += ",";
		buf += std::to_string(items_[i].Id());
		if (joined_.empty()) continue;
		Iterator itemIt{this, int(i), errOK};
		auto joinIt = itemIt.GetJoined();
		if (joinIt.getJoinedItemsCount() > 0) {
			buf += "[";
			for (auto fieldIt = joinIt.begin(); fieldIt != joinIt.end(); ++fieldIt) {
				if (fieldIt != joinIt.begin()) buf += ";";
				for (int j = 0; j < fieldIt.ItemsCount(); ++j) {
					if (j != 0) buf += ",";
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
	for (auto &ctx : ctxs) ret.push_back(ctx.type_.Name());
	return ret;
}

int QueryResults::GetJoinedNsCtxIndex(int nsid) const {
	int ctxIndex = joined_.size();
	for (int ns = 0; ns < nsid; ++ns) {
		ctxIndex += joined_[ns].GetJoinedSelectorsCount();
	}
	return ctxIndex;
}

class QueryResults::EncoderDatasourceWithJoins : public IEncoderDatasourceWithJoins {
public:
	EncoderDatasourceWithJoins(const joins::ItemIterator &joinedItemIt, const ContextsVector &ctxs, int ctxIdx)
		: joinedItemIt_(joinedItemIt), ctxs_(ctxs), ctxId_(ctxIdx) {}
	~EncoderDatasourceWithJoins() override = default;

	size_t GetJoinedRowsCount() const final { return joinedItemIt_.getJoinedFieldsCount(); }
	size_t GetJoinedRowItemsCount(size_t rowId) const final {
		auto fieldIt = joinedItemIt_.at(rowId);
		return fieldIt.ItemsCount();
	}
	ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) const final {
		auto fieldIt = joinedItemIt_.at(rowid);
		const ItemRef &itemRef = fieldIt[plIndex];
		const Context &ctx = ctxs_[ctxId_ + rowid];
		return ConstPayload(ctx.type_, itemRef.Value());
	}
	const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) final {
		const Context &ctx = ctxs_[ctxId_ + rowid];
		return ctx.tagsMatcher_;
	}
	virtual const FieldsSet &GetJoinedItemFieldsFilter(size_t rowid) final {
		const Context &ctx = ctxs_[ctxId_ + rowid];
		return ctx.fieldsFilter_;
	}

	const std::string &GetJoinedItemNamespace(size_t rowid) final {
		const Context &ctx = ctxs_[ctxId_ + rowid];
		return ctx.type_->Name();
	}

private:
	const joins::ItemIterator &joinedItemIt_;
	const ContextsVector &ctxs_;
	const int ctxId_;
};

class AdditionalDatasource : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalDatasource(double r, IEncoderDatasourceWithJoins *jds) : joinsDs_(jds), withRank_(true), rank_(r) {}
	AdditionalDatasource(IEncoderDatasourceWithJoins *jds) : joinsDs_(jds), withRank_(false), rank_(0.0) {}
	void PutAdditionalFields(JsonBuilder &builder) const final {
		if (withRank_) builder.Put("rank()", rank_);
	}
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins *joinsDs_;
	bool withRank_;
	double rank_;
};

class AdditionalDatasourceCSV : public IAdditionalDatasource<CsvBuilder> {
public:
	AdditionalDatasourceCSV(IEncoderDatasourceWithJoins *jds) : joinsDs_(jds) {}
	void PutAdditionalFields(CsvBuilder &) const final {}
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins *joinsDs_;
};

void QueryResults::encodeJSON(int idx, WrSerializer &ser) const {
	auto &itemRef = items_[idx];
	assertrx(ctxs.size() > itemRef.Nsid());
	auto &ctx = ctxs[itemRef.Nsid()];

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
			EncoderDatasourceWithJoins joinsDs(itemIt, ctxs, GetJoinedNsCtxIndex(itemRef.Nsid()));
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

Error QueryResults::Iterator::GetMsgPack(WrSerializer &wrser, bool withHdrLen) {
	auto &itemRef = qr_->items_[idx_];
	assertrx(qr_->ctxs.size() > itemRef.Nsid());
	auto &ctx = qr_->ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		return Error(errNotFound, "Item not found");
	}

	int startTag = 0;
	ConstPayload pl(ctx.type_, itemRef.Value());
	MsgPackEncoder msgpackEncoder(&ctx.tagsMatcher_);
	const TagsLengths &tagsLengths = msgpackEncoder.GetTagsMeasures(pl);
	MsgPackBuilder msgpackBuilder(wrser, &tagsLengths, &startTag, ObjType::TypePlain, const_cast<TagsMatcher *>(&ctx.tagsMatcher_));
	if (withHdrLen) {
		auto slicePosSaver = wrser.StartSlice();
		msgpackEncoder.Encode(pl, msgpackBuilder);
	} else {
		msgpackEncoder.Encode(pl, msgpackBuilder);
	}
	return errOK;
}

Error QueryResults::Iterator::GetProtobuf(WrSerializer &wrser, bool withHdrLen) {
	auto &itemRef = qr_->items_[idx_];
	assertrx(qr_->ctxs.size() > itemRef.Nsid());
	auto &ctx = qr_->ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		return Error(errNotFound, "Item not found");
	}

	ConstPayload pl(ctx.type_, itemRef.Value());
	ProtobufEncoder encoder(&ctx.tagsMatcher_);
	ProtobufBuilder builder(&wrser, ObjType::TypePlain, ctx.schema_.get(), const_cast<TagsMatcher *>(&ctx.tagsMatcher_));
	if (withHdrLen) {
		auto slicePosSaver = wrser.StartSlice();
		encoder.Encode(pl, builder);
	} else {
		encoder.Encode(pl, builder);
	}

	return errOK;
}

Error QueryResults::Iterator::GetJSON(WrSerializer &ser, bool withHdrLen) {
	try {
		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			qr_->encodeJSON(idx_, ser);
		} else {
			qr_->encodeJSON(idx_, ser);
		}
	} catch (const Error &err) {
		err_ = err;
		return err;
	}
	return errOK;
}

CsvOrdering QueryResults::MakeCSVTagOrdering(unsigned limit, unsigned offset) const {
	if (!ctxs[0].fieldsFilter_.empty()) {
		std::vector<int> ordering;
		ordering.reserve(ctxs[0].fieldsFilter_.size());
		for (const auto &tag : ctxs[0].fieldsFilter_) {
			ordering.emplace_back(tag);
		}
		return ordering;
	}

	std::vector<int> ordering;
	ordering.reserve(128);
	fast_hash_set<int> fieldsTmIds;
	WrSerializer ser;
	const auto &tm = getTagsMatcher(0);
	for (size_t i = offset; i < items_.size() && i < offset + limit; ++i) {
		ser.Reset();
		encodeJSON(i, ser);

		gason::JsonParser parser;
		auto jsonNode = parser.Parse(giftStr(ser.Slice()));

		for (const auto &child : jsonNode) {
			auto [it, inserted] = fieldsTmIds.insert(tm.name2tag(child.key));
			if (inserted && *it > 0) {
				ordering.emplace_back(*it);
			}
		}
	}
	return ordering;
}

[[nodiscard]] Error QueryResults::Iterator::GetCSV(WrSerializer &ser, CsvOrdering &ordering) noexcept {
	try {
		auto &itemRef = qr_->items_[idx_];
		assertrx(qr_->ctxs.size() > itemRef.Nsid());
		auto &ctx = qr_->ctxs[itemRef.Nsid()];

		if (itemRef.Value().IsFree()) {
			return Error(errNotFound, "Item not found");
		}

		ConstPayload pl(ctx.type_, itemRef.Value());
		CsvBuilder builder(ser, ordering);
		CsvEncoder encoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

		if (!qr_->joined_.empty()) {
			joins::ItemIterator itemIt = (qr_->begin() + idx_).GetJoined();
			if (itemIt.getJoinedItemsCount() > 0) {
				EncoderDatasourceWithJoins joinsDs(itemIt, qr_->ctxs, qr_->GetJoinedNsCtxIndex(itemRef.Nsid()));
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

Error QueryResults::Iterator::GetCJSON(WrSerializer &ser, bool withHdrLen) {
	try {
		auto &itemRef = qr_->items_[idx_];
		assertrx(qr_->ctxs.size() > itemRef.Nsid());
		auto &ctx = qr_->ctxs[itemRef.Nsid()];

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
	} catch (const Error &err) {
		err_ = err;
		return err;
	}
	return errOK;
}

bool QueryResults::Iterator::IsRaw() const {
	auto &itemRef = qr_->items_[idx_];
	return itemRef.Raw();
}
std::string_view QueryResults::Iterator::GetRaw() const {
	auto &itemRef = qr_->items_[idx_];
	assertrx(itemRef.Raw());
	return std::string_view(reinterpret_cast<char *>(itemRef.Value().Ptr()), itemRef.Value().GetCapacity());
}

Item QueryResults::Iterator::GetItem(bool enableHold) {
	auto &itemRef = qr_->items_[idx_];

	assertrx(qr_->ctxs.size() > itemRef.Nsid());
	auto &ctx = qr_->ctxs[itemRef.Nsid()];

	if (itemRef.Value().IsFree()) {
		return Item(Error(errNotFound, "Item not found"));
	}

	auto item = Item(new ItemImpl(ctx.type_, itemRef.Value(), ctx.tagsMatcher_, ctx.schema_));
	item.impl_->payloadValue_.Clone();
	if (enableHold) {
		if (!item.impl_->keyStringsHolder_) item.impl_->keyStringsHolder_.reset(new std::vector<key_string>);
		Payload{ctx.type_, item.impl_->payloadValue_}.CopyStrings(*(item.impl_->keyStringsHolder_));
	}

	item.setID(itemRef.Id());
	return item;
}

QueryResults::Iterator &QueryResults::Iterator::operator++() {
	idx_++;
	return *this;
}
QueryResults::Iterator &QueryResults::Iterator::operator+(int val) {
	idx_ += val;
	return *this;
}

bool QueryResults::Iterator::operator!=(const Iterator &other) const { return idx_ != other.idx_; }
bool QueryResults::Iterator::operator==(const Iterator &other) const { return idx_ == other.idx_; }

void QueryResults::AddItem(Item &item, bool withData, bool enableHold) {
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

const TagsMatcher &QueryResults::getTagsMatcher(int nsid) const { return ctxs[nsid].tagsMatcher_; }

const PayloadType &QueryResults::getPayloadType(int nsid) const { return ctxs[nsid].type_; }

const FieldsSet &QueryResults::getFieldsFilter(int nsid) const { return ctxs[nsid].fieldsFilter_; }

TagsMatcher &QueryResults::getTagsMatcher(int nsid) { return ctxs[nsid].tagsMatcher_; }

PayloadType &QueryResults::getPayloadType(int nsid) { return ctxs[nsid].type_; }

std::shared_ptr<const Schema> QueryResults::getSchema(int nsid) const { return ctxs[nsid].schema_; }

int QueryResults::getNsNumber(int nsid) const {
	assertrx(ctxs[nsid].schema_);
	return ctxs[nsid].schema_->GetProtobufNsNumber();
}

int QueryResults::getMergedNSCount() const { return ctxs.size(); }

void QueryResults::addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const FieldsSet &filter,
								std::shared_ptr<const Schema> schema) {
	if (filter.getTagsPathsLength()) nonCacheableData = true;

	ctxs.push_back(Context(type, tagsMatcher, filter, std::move(schema)));
}

QueryResults::NsDataHolder::NsDataHolder(QueryResults::NamespaceImplPtr &&_ns, StringsHolderPtr &&strHldr) noexcept
	: nsPtr_{std::move(_ns)}, ns(nsPtr_.get()), strHolder{std::move(strHldr)} {}

QueryResults::NsDataHolder::NsDataHolder(NamespaceImpl *_ns, StringsHolderPtr &&strHldr) noexcept
	: ns(_ns), strHolder(std::move(strHldr)) {}

}  // namespace reindexer
