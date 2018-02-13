#include "core/query/queryresults.h"
#include "tools/logger.h"

namespace reindexer {

QueryResults::JsonEncoderDatasourceWithJoins::JsonEncoderDatasourceWithJoins(const vector<QueryResults> &joined,
																			 const h_vector<Context, 1> &ctxs)
	: joined_(joined), ctxs_(ctxs) {}

QueryResults::JsonEncoderDatasourceWithJoins::~JsonEncoderDatasourceWithJoins() {}

size_t QueryResults::JsonEncoderDatasourceWithJoins::GetJoinedRowItemsCount(size_t rowid) {
	const QueryResults &queryRes(joined_[rowid]);
	return queryRes.size();
}

size_t QueryResults::JsonEncoderDatasourceWithJoins::GetJoinedRowsCount() { return joined_.size(); }

ConstPayload QueryResults::JsonEncoderDatasourceWithJoins::GetJoinedItemPayload(size_t rowid, size_t plIndex) {
	const QueryResults &queryRes(joined_[rowid]);
	const ItemRef &itemRef = queryRes[plIndex];
	const Context &ctx = ctxs_[rowid + 1];
	return ConstPayload(ctx.type_, itemRef.value);
}

const string &QueryResults::JsonEncoderDatasourceWithJoins::GetJoinedItemNamespace(size_t rowid) {
	const Context &ctx = ctxs_[rowid + 1];
	return ctx.type_->Name();
}

QueryResults::~QueryResults() {
	for (auto &itemRef : *this) {
		if (!itemRef.value.IsFree()) {
			assert(ctxs.size() > back().nsid);
			Payload(ctxs[itemRef.nsid].type_, itemRef.value).ReleaseStrings();
		}
	}
}

void QueryResults::Add(const ItemRef &i) {
	push_back(i);

	auto &itemRef = back();
	if (!itemRef.value.IsFree()) {
		assert(ctxs.size() > back().nsid);
		Payload(ctxs[itemRef.nsid].type_, itemRef.value).AddRefStrings();
	}
}

void QueryResults::Dump() const {
	string buf;
	for (auto &r : *this) {
		if (&r != &*(*this).begin()) buf += ",";
		buf += std::to_string(r.id);
		auto it = joined_.find(r.id);
		if (it != joined_.end()) {
			buf += "[";
			for (auto &ra : it->second) {
				if (&ra != &*it->second.begin()) buf += ";";
				for (auto &rr : ra) {
					if (&rr != &*ra.begin()) buf += ",";
					buf += std::to_string(rr.id);
				}
			}
			buf += "]";
		}
	}
	logPrintf(LogInfo, "Query returned: [%s]; total=%d", buf.c_str(), this->totalCount);
}

void QueryResults::EncodeJSON(int idx, WrSerializer &ser) const {
	auto &itemRef = at(idx);
	assert(ctxs.size() > itemRef.nsid);
	auto &ctx = ctxs[itemRef.nsid];

	auto itJoined(joined_.find(itemRef.id));
	bool withJoins((itJoined != joined_.end()) && !itJoined->second.empty());

	ConstPayload pl(ctx.type_, itemRef.value);
	JsonEncoder jsonEncoder(ctx.tagsMatcher_, ctx.jsonFilter_);

	if (withJoins) {
		JsonEncoderDatasourceWithJoins ds(itJoined->second, ctxs);
		jsonEncoder.Encode(&pl, ser, ds);
	} else {
		jsonEncoder.Encode(&pl, ser);
	}
}

void QueryResults::GetJSON(int idx, WrSerializer &ser, bool withHdrLen) const {
	if (withHdrLen) {
		// reserve place for size
		uint32_t saveLen = ser.Len();
		ser.PutUInt32(0);

		EncodeJSON(idx, ser);

		// put real json size
		int realSize = ser.Len() - saveLen - sizeof(saveLen);
		memcpy(ser.Buf() + saveLen, &realSize, sizeof(saveLen));
	} else {
		EncodeJSON(idx, ser);
	}
}

void QueryResults::GetCJSON(int idx, WrSerializer &ser, bool withHdrLen) const {
	auto &itemRef = at(idx);
	assert(ctxs.size() > itemRef.nsid);
	auto &ctx = ctxs[itemRef.nsid];

	ConstPayload pl(ctx.type_, itemRef.value);
	CJsonEncoder cjsonEncoder(ctx.tagsMatcher_);

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

Item *QueryResults::GetItem(int idx) const {
	auto &itemRef = at(idx);

	assert(ctxs.size() > itemRef.nsid);
	auto &ctx = ctxs[itemRef.nsid];

	PayloadValue v(itemRef.value);
	Payload pl(ctx.type_, v);

	auto ritem = new ItemImpl(pl, ctx.tagsMatcher_);
	ritem->SetID(itemRef.id, itemRef.version);
	return ritem;
}

}  // namespace reindexer
