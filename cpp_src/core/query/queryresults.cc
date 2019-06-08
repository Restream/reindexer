#include "core/query/queryresults.h"
#include "core/cjson/baseencoder.h"
#include "core/itemimpl.h"
#include "tools/logger.h"

namespace reindexer {

struct QueryResults::Context {
	Context() {}
	Context(PayloadType type, TagsMatcher tagsMatcher, const FieldsSet &fieldsFilter)
		: type_(type), tagsMatcher_(tagsMatcher), fieldsFilter_(fieldsFilter) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsSet fieldsFilter_;
};

static_assert(sizeof(QueryResults::Context) < QueryResults::kSizeofContext,
			  "QueryResults::kSizeofContext should >=  sizeof(QueryResults::Context)");

QueryResults::QueryResults(std::initializer_list<ItemRef> l) : items_(l) {}
QueryResults::QueryResults(int /*flags*/){};
QueryResults::QueryResults(QueryResults &&) = default;
QueryResults &QueryResults::operator=(QueryResults &&obj) noexcept {
	if (this != &obj) {
		unlockResults();
		items_ = std::move(obj.items_);
		assert(!obj.items_.size());
		joined_ = std::move(obj.joined_);
		aggregationResults = std::move(obj.aggregationResults);
		totalCount = std::move(obj.totalCount);
		haveProcent = std::move(obj.haveProcent);
		ctxs = std::move(obj.ctxs);
		nonCacheableData = std::move(obj.nonCacheableData);
		lockedResults_ = std::move(obj.lockedResults_);
		explainResults = std::move(obj.explainResults);
		aggregationResults = std::move(obj.aggregationResults);
		obj.lockedResults_ = false;
	}
	return *this;
}

QueryResults::~QueryResults() { unlockResults(); }

void QueryResults::Clear() { *this = QueryResults(); }

void QueryResults::Erase(ItemRefVector::iterator start, ItemRefVector::iterator finish) {
	assert(!lockedResults_);
	items_.erase(start, finish);
}

void QueryResults::lockResults() {
	assert(!lockedResults_);
	for (auto &itemRef : items_) {
		if (!itemRef.value.IsFree() && !itemRef.raw) {
			assert(ctxs.size() > itemRef.nsid);
			Payload(ctxs[itemRef.nsid].type_, itemRef.value).AddRefStrings();
		}
	}
	for (auto &joinded : joined_) {
		for (auto &jr : joinded) {
			for (auto &jqr : jr.second) {
				jqr.lockResults();
			}
		}
	}
	lockedResults_ = true;
}

void QueryResults::unlockResults() {
	if (!lockedResults_) return;
	for (auto &itemRef : items_) {
		if (!itemRef.value.IsFree() && !itemRef.raw) {
			assert(ctxs.size() > itemRef.nsid);
			Payload(ctxs[itemRef.nsid].type_, itemRef.value).ReleaseStrings();
		}
	}
	lockedResults_ = false;
}

void QueryResults::Add(const ItemRef &i) {
	items_.push_back(i);

	if (!lockedResults_) return;

	if (!i.value.IsFree() && !i.raw) {
		assert(ctxs.size() > items_.back().nsid);
		Payload(ctxs[items_.back().nsid].type_, items_.back().value).AddRefStrings();
	}
}

void QueryResults::Dump() const {
	string buf;
	for (auto &r : items_) {
		if (&r != &*items_.begin()) buf += ",";
		buf += std::to_string(r.id);
		for (const auto &joined : joined_) {
			auto it = joined.find(r.id);
			if (it != joined.end()) {
				buf += "[";
				for (auto &ra : it->second) {
					if (&ra != &*it->second.begin()) buf += ";";
					for (auto &rr : ra.items_) {
						if (&rr != &*ra.items_.begin()) buf += ",";
						buf += std::to_string(rr.id);
					}
				}
				buf += "]";
			}
		}
	}
	logPrintf(LogInfo, "Query returned: [%s]; total=%d", buf, this->totalCount);
}

h_vector<string_view, 1> QueryResults::GetNamespaces() const {
	h_vector<string_view, 1> ret;
	ret.reserve(ctxs.size());
	for (auto &ctx : ctxs) ret.push_back(ctx.type_.Name());
	return ret;
}

class QueryResults::EncoderDatasourceWithJoins : public IEncoderDatasourceWithJoins {
public:
	EncoderDatasourceWithJoins(const QRVector &joined, const ContextsVector &ctxs) : joined_(joined), ctxs_(ctxs) {}
	~EncoderDatasourceWithJoins() {}

	size_t GetJoinedRowsCount() final { return joined_.size(); }
	size_t GetJoinedRowItemsCount(size_t rowId) final {
		const QueryResults &queryRes(joined_[rowId]);
		return queryRes.Count();
	}
	ConstPayload GetJoinedItemPayload(size_t rowid, size_t plIndex) final {
		const QueryResults &queryRes(joined_[rowid]);
		const ItemRef &itemRef = queryRes.items_[plIndex];
		const Context &ctx = ctxs_[rowid + 1];
		return ConstPayload(ctx.type_, itemRef.value);
	}
	const TagsMatcher &GetJoinedItemTagsMatcher(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.tagsMatcher_;
	}
	virtual const FieldsSet &GetJoinedItemFieldsFilter(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.fieldsFilter_;
	}

	const string &GetJoinedItemNamespace(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.type_->Name();
	}

private:
	const QRVector &joined_;
	const ContextsVector &ctxs_;
};

void QueryResults::encodeJSON(int idx, WrSerializer &ser) const {
	auto &itemRef = items_[idx];
	assert(ctxs.size() > itemRef.nsid);
	auto &ctx = ctxs[itemRef.nsid];

	if (itemRef.value.IsFree()) {
		ser << "{}";
		return;
	}
	ConstPayload pl(ctx.type_, itemRef.value);
	JsonEncoder encoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

	JsonBuilder builder(ser, JsonBuilder::TypePlain);

	const QRVector &itJoined = (begin() + idx).GetJoined();

	if (!itJoined.empty()) {
		EncoderDatasourceWithJoins ds(itJoined, ctxs);
		encoder.Encode(&pl, builder, &ds);
		return;
	}
	encoder.Encode(&pl, builder);
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

Error QueryResults::Iterator::GetCJSON(WrSerializer &ser, bool withHdrLen) {
	try {
		auto &itemRef = qr_->items_[idx_];
		assert(qr_->ctxs.size() > itemRef.nsid);
		auto &ctx = qr_->ctxs[itemRef.nsid];

		if (itemRef.value.IsFree()) {
			return Error(errNotFound, "Item not found");
		}

		ConstPayload pl(ctx.type_, itemRef.value);
		CJsonBuilder builder(ser, CJsonBuilder::TypePlain);
		CJsonEncoder cjsonEncoder(&ctx.tagsMatcher_, &ctx.fieldsFilter_);

		if (withHdrLen) {
			auto slicePosSaver = ser.StartSlice();
			cjsonEncoder.Encode(&pl, builder);
		} else {
			cjsonEncoder.Encode(&pl, builder);
		}
	} catch (const Error &err) {
		err_ = err;
		return err;
	}
	return errOK;
}

bool QueryResults::Iterator::IsRaw() const {
	auto &itemRef = qr_->items_[idx_];
	return itemRef.raw;
}
string_view QueryResults::Iterator::GetRaw() const {
	auto &itemRef = qr_->items_[idx_];
	assert(itemRef.raw);
	return string_view(reinterpret_cast<char *>(itemRef.value.Ptr()), itemRef.value.GetCapacity());
}

Item QueryResults::Iterator::GetItem() {
	auto &itemRef = qr_->items_[idx_];

	assert(qr_->ctxs.size() > itemRef.nsid);
	auto &ctx = qr_->ctxs[itemRef.nsid];

	if (itemRef.value.IsFree()) {
		return Item(Error(errNotFound, "Item not found"));
	}

	PayloadValue v(itemRef.value);

	auto item = Item(new ItemImpl(ctx.type_, v, ctx.tagsMatcher_));
	item.setID(itemRef.id);
	return item;
}

const QRVector &QueryResults::Iterator::GetJoined() {
	static QRVector ret;
	if (qr_->joined_.empty()) {
		return ret;
	}

	auto &itemRef = qr_->items_[idx_];
	auto it = qr_->joined_[itemRef.nsid].find(itemRef.id);
	if (it == qr_->joined_[itemRef.nsid].end()) {
		return ret;
	}
	return it->second;
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

void QueryResults::AddItem(Item &item, bool withData) {
	auto ritem = item.impl_;
	if (item.GetID() != -1) {
		if (ctxs.empty()) ctxs.push_back(Context(ritem->Type(), ritem->tagsMatcher(), FieldsSet()));
		Add(ItemRef(item.GetID(), withData ? ritem->RealValue() : PayloadValue()));
		if (withData) {
			lockResults();
		}
	}
}

const TagsMatcher &QueryResults::getTagsMatcher(int nsid) const {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].tagsMatcher_;
}

const PayloadType &QueryResults::getPayloadType(int nsid) const {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].type_;
}
TagsMatcher &QueryResults::getTagsMatcher(int nsid) {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].tagsMatcher_;
}

PayloadType &QueryResults::getPayloadType(int nsid) {
	assert(nsid < int(ctxs.size()));
	return ctxs[nsid].type_;
}
int QueryResults::getMergedNSCount() const { return ctxs.size(); }

void QueryResults::addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const FieldsSet &filter) {
	if (filter.getTagsPathsLength()) nonCacheableData = true;

	ctxs.push_back(Context(type, tagsMatcher, filter));
}

}  // namespace reindexer
