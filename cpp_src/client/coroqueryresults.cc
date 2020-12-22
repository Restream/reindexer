#include "client/coroqueryresults.h"
#include "client/namespace.h"
#include "core/cjson/baseencoder.h"
#include "core/keyvalue/p_string.h"
#include "net/cproto/coroclientconnection.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

using namespace reindexer::net;

CoroQueryResults::CoroQueryResults(int fetchFlags)
	: conn_(nullptr), queryID_(0), fetchOffset_(0), fetchFlags_(fetchFlags), fetchAmount_(0), requestTimeout_(0) {}

CoroQueryResults::CoroQueryResults(net::cproto::CoroClientConnection *conn, NSArray &&nsArray, int fetchFlags, int fetchAmount,
								   seconds timeout)
	: conn_(conn),
	  nsArray_(std::move(nsArray)),
	  queryID_(0),
	  fetchOffset_(0),
	  fetchFlags_(fetchFlags),
	  fetchAmount_(fetchAmount),
	  requestTimeout_(timeout) {}

CoroQueryResults::CoroQueryResults(net::cproto::CoroClientConnection *conn, NSArray &&nsArray, string_view rawResult, int queryID,
								   int fetchFlags, int fetchAmount, seconds timeout)
	: CoroQueryResults(conn, std::move(nsArray), fetchFlags, fetchAmount, timeout) {
	Bind(rawResult, queryID);
}

void CoroQueryResults::Bind(string_view rawResult, int queryID) {
	queryID_ = queryID;
	ResultSerializer ser(rawResult);

	try {
		ser.GetRawQueryParams(queryParams_, [&ser, this](int nsIdx) {
			uint32_t stateToken = ser.GetVarUint();
			int version = ser.GetVarUint();

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
		assert(false);
		status_ = err;
	}

	rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());
}

void CoroQueryResults::fetchNextResults() {
	using std::chrono::seconds;
	int flags = fetchFlags_ ? (fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	auto ret = conn_->Call({cproto::kCmdFetchResults, requestTimeout_, milliseconds(0), nullptr}, queryID_, flags,
						   queryParams_.count + fetchOffset_, fetchAmount_);
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

h_vector<string_view, 1> CoroQueryResults::GetNamespaces() const {
	h_vector<string_view, 1> ret;
	ret.reserve(nsArray_.size());
	for (auto &ns : nsArray_) ret.push_back(ns->name_);
	return ret;
}

TagsMatcher CoroQueryResults::getTagsMatcher(int nsid) const { return nsArray_[nsid]->tagsMatcher_; }

class AdditionalRank : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalRank(double r) : rank_(r) {}
	void PutAdditionalFields(JsonBuilder &builder) const final { builder.Put("rank()", rank_); }
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return nullptr; }

private:
	double rank_;
};

void CoroQueryResults::Iterator::getJSONFromCJSON(string_view cjson, WrSerializer &wrser, bool withHdrLen) {
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

Error CoroQueryResults::Iterator::GetMsgPack(WrSerializer &wrser, bool withHdrLen) {
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

Error CoroQueryResults::Iterator::GetJSON(WrSerializer &wrser, bool withHdrLen) {
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

Error CoroQueryResults::Iterator::GetCJSON(WrSerializer &wrser, bool withHdrLen) {
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
				return Error(errParseBin, "Server returned data in msgpack format, can't process");
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

Item CoroQueryResults::Iterator::GetItem() {
	readNext();
	try {
		Error err;
		Item item = qr_->nsArray_[itemParams_.nsid]->NewItem();
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

int64_t CoroQueryResults::Iterator::GetLSN() {
	readNext();
	return itemParams_.lsn;
}

bool CoroQueryResults::Iterator::IsRaw() {
	readNext();
	return itemParams_.raw;
}

string_view CoroQueryResults::Iterator::GetRaw() {
	readNext();
	assert(itemParams_.raw);
	return itemParams_.data;
}

void CoroQueryResults::Iterator::readNext() {
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
		const_cast<CoroQueryResults *>(qr_)->status_ = err;
	}
}

CoroQueryResults::Iterator &CoroQueryResults::Iterator::operator++() {
	try {
		readNext();
		idx_++;
		pos_ = nextPos_;
		nextPos_ = 0;

		if (idx_ != qr_->queryParams_.qcount && idx_ == qr_->queryParams_.count + qr_->fetchOffset_) {
			const_cast<CoroQueryResults *>(qr_)->fetchNextResults();
			pos_ = 0;
		}
	} catch (const Error &err) {
		const_cast<CoroQueryResults *>(qr_)->status_ = err;
	}

	return *this;
}

}  // namespace client
}  // namespace reindexer
