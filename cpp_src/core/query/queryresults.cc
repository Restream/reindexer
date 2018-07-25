#include "core/query/queryresults.h"
#include "core/cjson/cjsonencoder.h"
#include "core/cjson/jsonencoder.h"
#include "core/cjson/jsonprintfilter.h"
#include "tools/logger.h"

namespace reindexer {

struct QueryResults::Context {
	Context() {}
	Context(PayloadType type, TagsMatcher tagsMatcher, JsonPrintFilter jsonFilter)
		: type_(type), tagsMatcher_(tagsMatcher), jsonFilter_(jsonFilter) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	JsonPrintFilter jsonFilter_;
};

static_assert(sizeof(QueryResults::Context) < QueryResults::kSizeofContext,
			  "QueryResults::kSizeofContext should >=  sizeof(QueryResults::Context)");

QueryResults::QueryResults(std::initializer_list<ItemRef> l) : items_(l) {}
QueryResults::QueryResults() = default;
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
		obj.lockedResults_ = false;
	}
	return *this;
}

QueryResults::~QueryResults() { unlockResults(); }

void QueryResults::Erase(ItemRefVector::iterator start, ItemRefVector::iterator finish) {
	assert(!lockedResults_);
	items_.erase(start, finish);
}

void QueryResults::lockResults() {
	assert(!lockedResults_);
	for (auto &itemRef : items_) {
		if (!itemRef.value.IsFree()) {
			assert(ctxs.size() > itemRef.nsid);
			Payload(ctxs[itemRef.nsid].type_, itemRef.value).AddRefStrings();
		}
	}
	if (joined_) {
		for (auto &jr : *joined_) {
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
		if (!itemRef.value.IsFree()) {
			assert(ctxs.size() > itemRef.nsid);
			Payload(ctxs[itemRef.nsid].type_, itemRef.value).ReleaseStrings();
		}
	}
	lockedResults_ = false;
}

void QueryResults::Add(const ItemRef &i) {
	items_.push_back(i);

	if (!lockedResults_) return;

	if (!i.value.IsFree()) {
		assert(ctxs.size() > items_.back().nsid);
		Payload(ctxs[items_.back().nsid].type_, items_.back().value).AddRefStrings();
	}
}

void QueryResults::Dump() const {
	string buf;
	for (auto &r : items_) {
		if (&r != &*items_.begin()) buf += ",";
		buf += std::to_string(r.id);
		if (joined_) {
			auto it = joined_->find(r.id);
			if (it != joined_->end()) {
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
	logPrintf(LogInfo, "Query returned: [%s]; total=%d", buf.c_str(), this->totalCount);
}

class QueryResults::JsonEncoderDatasourceWithJoins : public IJsonEncoderDatasourceWithJoins {
public:
	JsonEncoderDatasourceWithJoins(const QRVector &joined, const ContextsVector &ctxs) : joined_(joined), ctxs_(ctxs) {}
	~JsonEncoderDatasourceWithJoins() {}

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
	virtual const JsonPrintFilter &GetJoinedItemJsonFilter(size_t rowid) final {
		const Context &ctx = ctxs_[rowid + 1];
		return ctx.jsonFilter_;
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

	ConstPayload pl(ctx.type_, itemRef.value);
	JsonEncoder jsonEncoder(ctx.tagsMatcher_, ctx.jsonFilter_);

	if (joined_) {
		auto itJoined(joined_->find(itemRef.id));
		bool withJoins((itJoined != joined_->end()) && !itJoined->second.empty());
		if (withJoins) {
			JsonEncoderDatasourceWithJoins ds(itJoined->second, ctxs);
			jsonEncoder.Encode(&pl, ser, ds);
			return;
		}
	}
	jsonEncoder.Encode(&pl, ser);
}

void QueryResults::Iterator::GetJSON(WrSerializer &ser, bool withHdrLen) {
	if (withHdrLen) {
		// reserve place for size
		uint32_t saveLen = ser.Len();
		ser.PutUInt32(0);

		qr_->encodeJSON(idx_, ser);

		// put real json size
		int realSize = ser.Len() - saveLen - sizeof(saveLen);
		memcpy(ser.Buf() + saveLen, &realSize, sizeof(saveLen));
	} else {
		qr_->encodeJSON(idx_, ser);
	}
}

void QueryResults::Iterator::GetCJSON(WrSerializer &ser, bool withHdrLen) {
	auto &itemRef = qr_->items_[idx_];
	assert(qr_->ctxs.size() > itemRef.nsid);
	auto &ctx = qr_->ctxs[itemRef.nsid];

	ConstPayload pl(ctx.type_, itemRef.value);
	CJsonEncoder cjsonEncoder(ctx.tagsMatcher_, ctx.jsonFilter_);

	if (withHdrLen) {
		// reserve place for size
		uint32_t saveLen = ser.Len();
		ser.PutUInt32(0);

		cjsonEncoder.Encode(&pl, ser);

		// put real json size
		int realSize = ser.Len() - saveLen - sizeof(saveLen);
		memcpy(ser.Buf() + saveLen, &realSize, sizeof(saveLen));
	} else {
		cjsonEncoder.Encode(&pl, ser);
	}
}

Item QueryResults::Iterator::GetItem() {
	auto &itemRef = qr_->items_[idx_];

	assert(qr_->ctxs.size() > itemRef.nsid);
	auto &ctx = qr_->ctxs[itemRef.nsid];

	PayloadValue v(itemRef.value);

	auto item = Item(new ItemImpl(ctx.type_, v, ctx.tagsMatcher_));
	item.setID(itemRef.id, itemRef.version);
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

void QueryResults::AddItem(Item &item) {
	if (item.GetID() != -1) {
		auto ritem = item.impl_;
		ctxs.push_back(Context(ritem->Type(), ritem->tagsMatcher(), JsonPrintFilter()));
		Add(ItemRef(item.GetID(), item.GetVersion()));
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

void QueryResults::addNSContext(const PayloadType &type, const TagsMatcher &tagsMatcher, const JsonPrintFilter &jsonFilter) {
	ctxs.push_back(Context(type, tagsMatcher, jsonFilter));
}

}  // namespace reindexer
