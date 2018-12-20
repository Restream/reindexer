#include "client/queryresults.h"
#include "client/namespace.h"
#include "core/cjson/baseencoder.h"
#include "core/keyvalue/p_string.h"
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
		status_ = std::move(obj.status_);
		cmpl_ = std::move(obj.cmpl_);
	}
	return *this;
}

QueryResults::QueryResults(net::cproto::ClientConnection *conn, NSArray &&nsArray, Completion cmpl)
	: conn_(conn), nsArray_(std::move(nsArray)), fetchOffset_(0), cmpl_(std::move(cmpl)) {}

QueryResults::QueryResults(net::cproto::ClientConnection *conn, NSArray &&nsArray, Completion cmpl, string_view rawResult, int queryID)
	: QueryResults(conn, std::move(nsArray), cmpl) {
	Bind(rawResult, queryID);
}

void QueryResults::Bind(string_view rawResult, int queryID) {
	queryID_ = queryID;
	ResultSerializer ser(rawResult);

	try {
		ser.GetRawQueryParams(queryParams_, [&ser, this](int nsIdx) {
			uint32_t stateToken = ser.GetVarUint();
			int version = ser.GetVarUint();

			std::unique_lock<shared_timed_mutex> lck(nsArray_[nsIdx]->lck_);

			bool skip = nsArray_[nsIdx]->tagsMatcher_.version() >= version && nsArray_[nsIdx]->tagsMatcher_.stateToken() == stateToken;
			if (skip) {
				TagsMatcher().deserialize(ser);
				// PayloadType("tmp").clone()->deserialize(ser);
			} else {
				nsArray_[nsIdx]->tagsMatcher_.deserialize(ser, version, stateToken);
				// nsArray[nsIdx]->payloadType_.clone()->deserialize(ser);
				// nsArray[nsIdx]->tagsMatcher_.updatePayloadType(nsArray[nsIdx]->payloadType_, false);
			}
			PayloadType("tmp").clone()->deserialize(ser);
		});
	} catch (const Error &err) {
		status_ = err;
	}

	rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());
}

void QueryResults::fetchNextResults() {
	auto ret = conn_->Call(cproto::kCmdFetchResults, queryID_, kResultsCJson, queryParams_.count + fetchOffset_, 100);
	if (!ret.Status().ok()) {
		throw ret.Status();
	}

	auto args = ret.GetArgs();
	if (args.size() < 2) {
		throw Error(errParams, "Server returned %d args, but expected %d", int(args.size()), 2);
	}

	fetchOffset_ += queryParams_.count;

	string_view rawResult = p_string(args[0]);
	ResultSerializer ser(rawResult);

	ser.GetRawQueryParams(queryParams_, nullptr);

	rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());
}

QueryResults::~QueryResults() {}

h_vector<string_view, 1> QueryResults::GetNamespaces() const {
	h_vector<string_view, 1> ret;
	ret.reserve(nsArray_.size());
	for (auto &ns : nsArray_) ret.push_back(ns->name_);
	return ret;
}

Error QueryResults::Iterator::GetJSON(WrSerializer &wrser, bool withHdrLen) {
	try {
		string_view rawResult(qr_->rawResult_.data(), qr_->rawResult_.size());

		ResultSerializer ser(rawResult.substr(pos_));

		auto itemParams = ser.GetItemParams(qr_->queryParams_.flags);
		if (qr_->queryParams_.flags & kResultsWithJoined) {
			int joinedCnt = ser.GetVarUint();
			(void)joinedCnt;
		}
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson: {
				JsonEncoder enc(&qr_->nsArray_[itemParams.nsid]->tagsMatcher_);
				JsonBuilder builder(wrser, JsonBuilder::TypePlain);

				if (withHdrLen) {
					auto slicePosSaver = wrser.StartSlice();
					enc.Encode(itemParams.data, builder);
				} else {
					enc.Encode(itemParams.data, builder);
				}
				break;
			}
			case kResultsJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams.data);
				} else {
					wrser.Write(itemParams.data);
				}
				break;
			default:
				return Error(errParseBin, "Server returned data in unknown format %d", int(qr_->queryParams_.flags & kResultsFormatMask));
		}
		nextPos_ = pos_ + ser.Pos();
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error QueryResults::Iterator::GetCJSON(WrSerializer &wrser, bool withHdrLen) {
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
	string_view rawResult(qr_->rawResult_.data(), qr_->rawResult_.size());

	ResultSerializer ser(rawResult.substr(pos_));

	try {
		if (nextPos_ == 0) {
			ser.GetItemParams(qr_->queryParams_.flags);
			if (qr_->queryParams_.flags & kResultsWithJoined) {
				int joinedCnt = ser.GetVarUint();
				(void)joinedCnt;
			}
			pos_ += ser.Pos();
			idx_++;
		} else {
			pos_ = nextPos_;
		}
		nextPos_ = 0;

		if (idx_ != qr_->queryParams_.qcount && idx_ == qr_->queryParams_.count + qr_->fetchOffset_) {
			const_cast<QueryResults *>(qr_)->fetchNextResults();
			pos_ = 0;
		}
	} catch (const Error &err) {
		const_cast<QueryResults *>(qr_)->status_ = err;
	}

	return *this;
}
bool QueryResults::Iterator::operator!=(const Iterator &other) const { return idx_ != other.idx_; }
bool QueryResults::Iterator::operator==(const Iterator &other) const { return idx_ == other.idx_; }

}  // namespace client
}  // namespace reindexer
