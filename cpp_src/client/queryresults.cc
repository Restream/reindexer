#include "client/queryresults.h"
#include "client/namespace.h"
#include "core/cjson/baseencoder.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/clientconnection.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

QueryResults::QueryResults(int fetchFlags)
	: conn_(nullptr), queryID_(0), fetchOffset_(0), fetchFlags_(fetchFlags), fetchAmount_(0), requestTimeout_(0) {}

QueryResults::QueryResults(QueryResults &&obj) noexcept
	: conn_(std::move(obj.conn_)),
	  nsArray_(std::move(obj.nsArray_)),
	  rawResult_(std::move(obj.rawResult_)),
	  queryID_(std::move(obj.queryID_)),
	  fetchOffset_(std::move(obj.fetchOffset_)),
	  fetchFlags_(std::move(obj.fetchFlags_)),
	  fetchAmount_(std::move(obj.fetchAmount_)),
	  requestTimeout_(obj.requestTimeout_),
	  queryParams_(std::move(obj.queryParams_)),
	  status_(std::move(obj.status_)),
	  cmpl_(std::move(obj.cmpl_)) {}

QueryResults &QueryResults::operator=(QueryResults &&obj) noexcept {
	if (this != &obj) {
		rawResult_ = std::move(obj.rawResult_);
		conn_ = std::move(obj.conn_);
		nsArray_ = std::move(obj.nsArray_);
		queryParams_ = std::move(obj.queryParams_);
		fetchOffset_ = std::move(obj.fetchOffset_);
		fetchFlags_ = std::move(obj.fetchFlags_);
		fetchAmount_ = std::move(obj.fetchAmount_);
		queryID_ = std::move(obj.queryID_);
		status_ = std::move(obj.status_);
		cmpl_ = std::move(obj.cmpl_);
		requestTimeout_ = obj.requestTimeout_;
	}
	return *this;
}

QueryResults::QueryResults(net::cproto::ClientConnection *conn, NSArray &&nsArray, Completion cmpl, int fetchFlags, int fetchAmount,
						   seconds timeout)
	: conn_(conn),
	  nsArray_(std::move(nsArray)),
	  queryID_(0),
	  fetchOffset_(0),
	  fetchFlags_(fetchFlags),
	  fetchAmount_(fetchAmount),
	  requestTimeout_(timeout),
	  cmpl_(std::move(cmpl)) {}

QueryResults::QueryResults(net::cproto::ClientConnection *conn, NSArray &&nsArray, Completion cmpl, string_view rawResult, int queryID,
						   int fetchFlags, int fetchAmount, seconds timeout)
	: QueryResults(conn, std::move(nsArray), cmpl, fetchFlags, fetchAmount, timeout) {
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
				nsArray_[nsIdx]->tagsMatcher_ = TagsMatcher();
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
	using std::chrono::seconds;
	int flags = fetchFlags_ ? (fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	auto ret = conn_->Call({cproto::kCmdFetchResults, requestTimeout_, milliseconds(0)}, queryID_, flags, queryParams_.count + fetchOffset_,
						   fetchAmount_);
	if (!ret.Status().ok()) {
		throw ret.Status();
	}

	auto args = ret.GetArgs(2);

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

TagsMatcher QueryResults::getTagsMatcher(int nsid) const {
	shared_lock<shared_timed_mutex> lck(nsArray_[nsid]->lck_);
	return nsArray_[nsid]->tagsMatcher_;
}

class AdditionalRank : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalRank(double r) : rank_(r) {}
	void PutAdditionalFields(JsonBuilder &builder) const final { builder.Put("rank()", rank_); }
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return nullptr; }

private:
	double rank_;
};

void QueryResults::Iterator::getJSONFromCJSON(string_view cjson, WrSerializer &wrser, bool withHdrLen) {
	auto tm = qr_->getTagsMatcher(itemParams_.nsid);
	JsonEncoder enc(&tm);
	JsonBuilder builder(wrser, ObjType::TypePlain);
	if (qr_->NeedOutputRank()) {
		AdditionalRank additionalRank(itemParams_.proc);
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			enc.Encode(cjson, builder, &additionalRank);
		} else {
			enc.Encode(cjson, builder, &additionalRank);
		}
	} else {
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
			enc.Encode(cjson, builder, nullptr);
		} else {
			enc.Encode(cjson, builder, nullptr);
		}
	}
}

Error QueryResults::Iterator::GetMsgPack(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	int type = qr_->queryParams_.flags & kResultsFormatMask;
	if (type == kResultsMsgPack) {
		if (withHdrLen) {
			wrser.PutSlice(itemParams_.data);
		} else {
			wrser.Write(itemParams_.data);
		}
	} else {
		return Error(errParseBin, "Impossible to get data in MsgPack because of a different format: %d", type);
	}
	return errOK;
}

Error QueryResults::Iterator::GetJSON(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	try {
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson: {
				getJSONFromCJSON(itemParams_.data, wrser, withHdrLen);
				break;
			}
			case kResultsJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams_.data);
				} else {
					wrser.Write(itemParams_.data);
				}
				break;
			default:
				return Error(errParseBin, "Server returned data in unknown format %d", qr_->queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Error QueryResults::Iterator::GetCJSON(WrSerializer &wrser, bool withHdrLen) {
	readNext();
	try {
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson:
				if (withHdrLen) {
					wrser.PutSlice(itemParams_.data);
				} else {
					wrser.Write(itemParams_.data);
				}
				break;
			case kResultsMsgPack:
			case kResultsJson:
				return Error(errParseBin, "Server returned data in json format, can't process");
			default:
				return Error(errParseBin, "Server returned data in unknown format %d", qr_->queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

Item QueryResults::Iterator::GetItem() {
	readNext();
	try {
		Error err;
		Item item;
		{
			shared_lock<shared_timed_mutex> lck(qr_->nsArray_[itemParams_.nsid]->lck_);
			item = qr_->nsArray_[itemParams_.nsid]->NewItem();
		}
		switch (qr_->queryParams_.flags & kResultsFormatMask) {
			case kResultsMsgPack: {
				size_t offset = 0;
				err = item.FromMsgPack(itemParams_.data, offset);
				break;
			}
			case kResultsCJson: {
				err = item.FromCJSON(itemParams_.data);
				break;
			}
			case kResultsJson: {
				char *endp = nullptr;
				err = item.FromJSON(itemParams_.data, &endp);
				break;
			}
			default:
				return Item();
		}
		if (err.ok()) {
			return item;
		}
	} catch (const Error &) {
	}
	return Item();
}

int64_t QueryResults::Iterator::GetLSN() {
	readNext();
	return itemParams_.lsn;
}

bool QueryResults::Iterator::IsRaw() {
	readNext();
	return itemParams_.raw;
}

string_view QueryResults::Iterator::GetRaw() {
	readNext();
	assert(itemParams_.raw);
	return itemParams_.data;
}

void QueryResults::Iterator::readNext() {
	if (nextPos_ != 0) return;

	string_view rawResult(qr_->rawResult_.data(), qr_->rawResult_.size());

	ResultSerializer ser(rawResult.substr(pos_));

	try {
		itemParams_ = ser.GetItemParams(qr_->queryParams_.flags);
		if (qr_->queryParams_.flags & kResultsWithJoined) {
			int joinedCnt = ser.GetVarUint();
			(void)joinedCnt;
		}
		nextPos_ = pos_ + ser.Pos();
	} catch (const Error &err) {
		const_cast<QueryResults *>(qr_)->status_ = err;
	}
}

QueryResults::Iterator &QueryResults::Iterator::operator++() {
	try {
		readNext();
		idx_++;
		pos_ = nextPos_;
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
