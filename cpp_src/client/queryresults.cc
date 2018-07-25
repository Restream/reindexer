#include "client/queryresults.h"
#include "core/cjson/cjsonencoder.h"
#include "core/cjson/jsonencoder.h"
#include "net/cproto/clientconnection.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

QueryResults::QueryResults() = default;
QueryResults::QueryResults(QueryResults &&) = default;
QueryResults &QueryResults::operator=(QueryResults &&obj) noexcept {
	if (this != &obj) {
		rawResult_ = std::move(obj.rawResult_);
		conn_ = std::move(obj.conn_);
		nsArray_ = std::move(obj.nsArray_);
		queryParams_ = std::move(obj.queryParams_);
		fetchOffset_ = std::move(obj.fetchOffset_);
		queryID_ = std::move(obj.queryID_);
	}
	return *this;
}

QueryResults::QueryResults(net::cproto::ClientConnection *conn, const NSArray &nsArray, string_view rawResult, int queryID)
	: conn_(conn), nsArray_(nsArray), queryID_(queryID), fetchOffset_(0) {
	ResultSerializer ser(rawResult);

	queryParams_ = ser.GetRawQueryParams([&](int nsIdx) {
		uint32_t cacheToken = ser.GetVarUint();
		int version = ser.GetVarUint();

		std::unique_lock<std::mutex> lck(nsArray[nsIdx]->lck_);

		bool skip = nsArray[nsIdx]->tagsMatcher_.version() >= version && nsArray[nsIdx]->tagsMatcher_.cacheToken() == cacheToken;
		if (skip) {
			TagsMatcher().deserialize(ser);
			PayloadType("tmp").clone()->deserialize(ser);
		} else {
			nsArray[nsIdx]->tagsMatcher_.deserialize(ser, version, cacheToken);
			nsArray[nsIdx]->payloadType_.clone()->deserialize(ser);
			nsArray[nsIdx]->tagsMatcher_.updatePayloadType(nsArray[nsIdx]->payloadType_);
		}
	});

	rawResult = rawResult.substr(ser.Pos());
	rawResult_ = string(rawResult.data(), rawResult.size());
}

Error QueryResults::fetchNextResults() {
	auto ret = conn_->Call(cproto::kCmdFetchResults, queryID_, kResultsWithCJson, queryParams_.count + fetchOffset_, 100, int64_t(-1));
	if (!ret.Status().ok()) {
		return ret.Status();
	}
	if (ret.GetArgs().size() < 2) {
		return Error(errParams, "Server returned %d args, but expected %d", int(ret.GetArgs().size()), 2);
	}

	fetchOffset_ += queryParams_.count;

	string_view rawResult = p_string(ret.GetArgs()[0]);
	ResultSerializer ser(rawResult);

	queryParams_ = ser.GetRawQueryParams(nullptr);

	rawResult = rawResult.substr(ser.Pos());
	rawResult_ = string(rawResult.data(), rawResult.size());
	return errOK;
}

QueryResults::~QueryResults() {}

void QueryResults::Iterator::GetJSON(WrSerializer &wrser, bool withHdrLen) {
	string_view rawResult = qr_->rawResult_;

	ResultSerializer ser(rawResult.substr(pos_));

	auto itemParams = ser.GetItemParams();
	int joinedCnt = ser.GetVarUint();

	JsonEncoder enc(qr_->nsArray_[itemParams.nsid]->tagsMatcher_, JsonPrintFilter());

	if (withHdrLen) {
		// reserve place for size
		uint32_t saveLen = wrser.Len();
		wrser.PutUInt32(0);

		enc.Encode(itemParams.data, wrser);

		// put real json size
		uint32_t realSize = wrser.Len() - saveLen - sizeof(saveLen);
		memcpy(wrser.Buf() + saveLen, &realSize, sizeof(realSize));
	} else {
		enc.Encode(itemParams.data, wrser);
	}
	nextPos_ = pos_ + ser.Pos();
	(void)joinedCnt;
}

void QueryResults::Iterator::GetCJSON(WrSerializer &wrser, bool withHdrLen) {
	(void)wrser;
	(void)withHdrLen;
	// TODO: implement
	abort();
}

Item QueryResults::Iterator::GetItem() {
	// TODO: implement
	abort();
	// return Item();
}

QueryResults::Iterator &QueryResults::Iterator::operator++() {
	string_view rawResult = qr_->rawResult_;
	ResultSerializer ser(rawResult.substr(pos_));

	if (nextPos_ == 0) {
		ser.GetItemParams();
		int joinedCnt = ser.GetVarUint();
		idx_++;
		pos_ += ser.Pos();
		(void)joinedCnt;
	} else {
		pos_ = nextPos_;
	}
	nextPos_ = 0;

	if (idx_ != qr_->queryParams_.qcount && idx_ == qr_->queryParams_.count + qr_->fetchOffset_) {
		err_ = const_cast<QueryResults *>(qr_)->fetchNextResults();
		pos_ = 0;
	}

	return *this;
}
bool QueryResults::Iterator::operator!=(const Iterator &other) const { return idx_ != other.idx_; }
bool QueryResults::Iterator::operator==(const Iterator &other) const { return idx_ == other.idx_; }

}  // namespace client
}  // namespace reindexer
