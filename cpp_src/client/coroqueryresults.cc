#include "client/coroqueryresults.h"
#include "client/itemimpl.h"
#include "client/namespace.h"
#include "core/cjson/csvbuilder.h"
#include "core/cjson/encoderdatasources.h"
#include "core/cjson/jsonbuilder.h"
#include "core/keyvalue/p_string.h"
#include "core/nsselecter/joins/query_joins_table.h"
#include "core/query/query_impl.h"
#include "core/queryresults/fields_filter.h"
#include "net/cproto/coroclientconnection.h"
#include "tools/catch_and_return.h"

#include <forward_list>
#include <limits>

namespace reindexer::client {

using namespace reindexer::net;

CoroQueryResults::~CoroQueryResults() {
	if (holdsRemoteData()) {
		try {
			std::ignore = i_.conn_->Call({cproto::kCmdCloseResults, i_.requestTimeout_, milliseconds(0), lsn_t(), -1,
										  ShardingKeyType::NotSetShard, nullptr, false, i_.sessionTs_},
										 i_.queryID_.main, i_.queryID_.uid);
		} catch (std::exception& e) {
			fprintf(stderr, "reindexer error: unexpected exception in ~CoroQueryResults: %s\n", e.what());
			assertrx_dbg(false);
		}
	}
}

const std::string& CoroQueryResults::GetExplainResults() {
	if (!i_.queryParams_.explainResults.has_value()) {
		parseExtraData();
		if (!i_.queryParams_.explainResults.has_value()) {
			throw Error(errLogic, "Lazy explain in QueryResults was not initialized");
		}
	}
	return i_.queryParams_.explainResults.value();
}

const std::vector<AggregationResult>& CoroQueryResults::GetAggregationResults() {
	if (!i_.queryParams_.aggResults.has_value()) {
		parseExtraData();
		if (!i_.queryParams_.aggResults.has_value()) {
			throw Error(errLogic, "Lazy aggregations in QueryResults were not initialized");
		}
	}
	return i_.queryParams_.aggResults.value();
}

CoroQueryResults::CoroQueryResults(NsArray&& nsArray, Item& item) : i_(std::move(nsArray)) {
	i_.queryParams_.totalcount = 0;
	i_.queryParams_.qcount = 1;
	i_.queryParams_.count = 1;
	i_.queryParams_.flags = kResultsCJson;
	std::string_view itemData = item.GetCJSON();
	i_.rawResult_.resize(itemData.size() + sizeof(uint32_t));
	uint32_t dataSize = itemData.size();
	memcpy(&i_.rawResult_[0], &dataSize, sizeof(uint32_t));
	memcpy(&i_.rawResult_[0] + sizeof(uint32_t), itemData.data(), itemData.size());
}

void CoroQueryResults::Bind(std::string_view rawResult, RPCQrId id, const Query* q) {
	i_.queryID_ = id;
	i_.isBound_ = true;

	if (q && reindexer::Impl(*q).HasJoinQueries()) {
		i_.joinsTable_ = std::make_unique<joins::QueryJoinsTable>(reindexer::Impl(*q));
	} else {
		i_.joinsTable_.reset();
	}
	i_.queryFormat_ = i_.conn_ ? i_.conn_->GetBindingCapabilities().GetQueryFormat() : QueryFormatV1;

	ResultSerializer ser(rawResult);
	try {
		const auto opts = i_.lazyMode_ ? ResultSerializer::Options{ResultSerializer::ClearAggregations | ResultSerializer::LazyMode}
									   : ResultSerializer::Options{ResultSerializer::ClearAggregations};
		ser.GetRawQueryParams(
			i_.queryParams_,
			[&ser, this](int nsIdx) {
				const uint32_t stateToken = ser.GetVarUInt();
				const int version = ser.GetVarUInt();
				TagsMatcher newTm;
				newTm.deserialize(ser, version, stateToken);
				i_.nsArray_[nsIdx]->TryReplaceTagsMatcher(std::move(newTm));
				// nsArray[nsIdx]->payloadType_.clone()->deserialize(ser);
				// nsArray[nsIdx]->tagsMatcher_.updatePayloadType(nsArray[nsIdx]->payloadType_, false);
				PayloadType("tmp").clone()->deserialize(ser);
			},
			opts, i_.parsingData_, i_.queryFormat_);

		const auto copyStart = i_.lazyMode_ ? rawResult.begin() : (rawResult.begin() + ser.Pos());
		if (const auto rawResLen = std::distance(copyStart, rawResult.end()); rawResLen > int64_t(QrRawBuffer::max_size())) [[unlikely]] {
			throw Error(
				errLogic,
				"client::QueryResults::Bind: rawResult buffer overflow. Max size if {} bytes, but {} bytes requested. Try to reduce "
				"fetch limit (current limit is {})",
				QrRawBuffer::max_size(), rawResLen, i_.fetchAmount_);
		}

		i_.rawResult_.assign(copyStart, rawResult.end());
	} catch (const Error& err) {
		i_.status_ = err;
	}
}

void CoroQueryResults::fetchNextResults() {
	using std::chrono::seconds;
	const int flags = i_.fetchFlags_ ? (i_.fetchFlags_ & ~kResultsWithPayloadTypes) : kResultsCJson;
	auto ret = i_.conn_->Call({cproto::kCmdFetchResults, i_.requestTimeout_, milliseconds(0), lsn_t(), -1, ShardingKeyType::NotSetShard,
							   nullptr, false, i_.sessionTs_},
							  i_.queryID_.main, flags, i_.queryParams_.count + i_.fetchOffset_, i_.fetchAmount_, i_.queryID_.uid);
	if (!ret.Status().ok()) {
		throw ret.Status();
	}

	handleFetchedBuf(ret);
}

void CoroQueryResults::handleFetchedBuf(net::cproto::CoroRPCAnswer& ans) {
	auto args = ans.GetArgs(2);

	i_.fetchOffset_ += i_.queryParams_.count;

	std::string_view rawResult = p_string(args[0]);
	ResultSerializer ser(rawResult);

	ser.GetRawQueryParams(i_.queryParams_, nullptr, ResultSerializer::Options{}, i_.parsingData_, i_.queryFormat_);
	const auto copyStart = i_.lazyMode_ ? rawResult.begin() : (rawResult.begin() + ser.Pos());
	if (const auto rawResLen = std::distance(copyStart, rawResult.end()); rawResLen > int64_t(QrRawBuffer::max_size())) [[unlikely]] {
		throw Error(errLogic,
					"client::QueryResults::fetchNextResults: rawResult buffer overflow. Max size if {} bytes, but {} bytes requested. Try "
					"to reduce fetch limit (current limit is {})",
					QrRawBuffer::max_size(), rawResLen, i_.fetchAmount_);
	}
	i_.rawResult_.assign(copyStart, rawResult.end());
	i_.status_ = Error();
}

void CoroQueryResults::parseExtraData() {
	if (i_.lazyMode_) {
		if (hadFetchedRemoteData()) {
			throw Error(errLogic, "Lazy data (aggregations/explain) unavailable. They must be read before the first results fetching");
		}
		std::string_view rawResult(i_.rawResult_.data() + i_.parsingData_.extraData.begin,
								   i_.parsingData_.extraData.end - i_.parsingData_.extraData.begin);
		ResultSerializer ser(rawResult);
		i_.queryParams_.aggResults.emplace();
		i_.queryParams_.explainResults.emplace();
		i_.queryParams_.nsIncarnationTags.clear<false>();
		ser.GetExtraParams(i_.queryParams_, ResultSerializer::Options{});
	} else {
		throw Error(errLogic, "Unable to parse clients explain or aggregation results (lazy parsing mode was expected)");
	}
}

h_vector<std::string_view, 1> CoroQueryResults::GetNamespaces() const {
	h_vector<std::string_view, 1> ret;
	ret.reserve(i_.nsArray_.size());
	for (auto& ns : i_.nsArray_) {
		ret.emplace_back(ns->name);
	}
	return ret;
}

TagsMatcher CoroQueryResults::GetTagsMatcher(int nsid) const noexcept {
	assert(nsid < int(i_.nsArray_.size()));
	return i_.nsArray_[nsid]->GetTagsMatcher();
}

TagsMatcher CoroQueryResults::GetTagsMatcher(std::string_view nsName) const noexcept {
	const auto it = std::find_if(i_.nsArray_.begin(), i_.nsArray_.end(),
								 [&nsName](const std::shared_ptr<Namespace>& ns) { return (std::string_view(ns->name) == nsName); });

	return (it != i_.nsArray_.end()) ? (*it)->GetTagsMatcher() : TagsMatcher();
}

PayloadType CoroQueryResults::GetPayloadType(int nsid) const noexcept {
	assert(nsid < int(i_.nsArray_.size()));
	return i_.nsArray_[nsid]->payloadType;
}

PayloadType CoroQueryResults::GetPayloadType(std::string_view nsName) const noexcept {
	const auto it = std::find_if(i_.nsArray_.begin(), i_.nsArray_.end(),
								 [&nsName](const std::shared_ptr<Namespace>& ns) { return (std::string_view(ns->name) == nsName); });
	return (it != i_.nsArray_.end()) ? (*it)->payloadType : PayloadType();
}

const std::string& CoroQueryResults::GetNsName(int nsid) const noexcept {
	assert(nsid < int(i_.nsArray_.size()));
	return i_.nsArray_[nsid]->payloadType.Name();
}

template <typename Builder>
class [[nodiscard]] EncoderDatasourceWithJoins final : public IJoinsDatasource<Builder> {
public:
	EncoderDatasourceWithJoins(int nsid, const ResultSerializer::JoinedData& joinedData, const CoroQueryResults& qr)
		: nsid_(nsid), joinedData_(joinedData), qr_(qr), tm_(TagsMatcher::unsafe_empty_t()) {
		assertrx_throw(nsid_ >= 0);
	}
	~EncoderDatasourceWithJoins() override = default;

	EncoderContext<Builder> BuildFieldJoinsDatasourceContext(size_t joinedField, size_t itemIdx) override {
		if (hasJoinedDataForField(joinedField)) {
			const auto& fieldData{joinedData_[joinedField]};
			if (itemIdx < fieldData.size() && !fieldData[itemIdx].joined.empty()) {
				const auto& itemData{fieldData[itemIdx]};
				datasourcesWithJoins_.emplace_front(itemData.nsid, itemData.joined, qr_);
				EncoderContext<Builder> ctx;
				ctx.joins = &datasourcesWithJoins_.front();
				if (qr_.NeedOutputRank()) {
					ctx.fields.rank = itemData.rank;
				}
				return ctx;
			}
		}
		return {};
	}
	size_t GetFieldsCount() const noexcept override { return joinedData_.size(); }
	size_t GetRowItemsCount(size_t rowId) const override {
		const auto& fieldIt = joinedData_.at(rowId);
		return fieldIt.size();
	}
	ConstPayload GetItemPayload(size_t joinedField, size_t itemIdx) override {
		auto& fieldIt = joinedData_.at(joinedField);
		auto& dataIt = fieldIt.at(itemIdx);
		const auto joinedNsId = getJoinedNsID(nsid_, joinedField);
		itemimpl_ =
			ItemImpl<RPCClient>(qr_.GetPayloadType(joinedNsId), qr_.GetTagsMatcher(joinedNsId), nullptr, std::chrono::milliseconds());
		itemimpl_.Unsafe(true);
		itemimpl_.FromCJSON(dataIt.data);
		return itemimpl_.GetConstPayload();
	}
	const TagsMatcher& GetItemTagsMatcher(size_t joinedField) & noexcept override {
		if (!hasJoinedDataForField(joinedField)) {
			static const TagsMatcher kEmptyTm;
			return kEmptyTm;
		}
		tm_ = qr_.GetTagsMatcher(getJoinedNsID(nsid_, joinedField));
		return tm_;
	}
	const FieldsFilter& GetItemFieldsFilter(size_t /*joinedField*/) & noexcept override { return FieldsFilter::Empty(); }
	const std::string& GetItemNamespace(size_t joinedField) & noexcept override {
		if (!hasJoinedDataForField(joinedField)) {
			static const std::string empty;
			return empty;
		}
		return qr_.GetNsName(getJoinedNsID(nsid_, joinedField));
	}

private:
	bool hasJoinedDataForField(size_t joinedField) const noexcept {
		return (joinedField < joinedData_.size()) && !joinedData_[joinedField].empty();
	}

	uint32_t getJoinedNsID(int parentNsId, size_t joinedField) noexcept {
		if (cachedParentNsId_ == int64_t(parentNsId) && cachedJoinedField_ == joinedField) {
			return cachedJoinedNsId_;
		}
		const auto* joinsTable{qr_.GetJoinsTable()};
		assertrx(joinsTable);
		if (joinsTable) {
			cachedParentNsId_ = parentNsId;
			cachedJoinedField_ = joinedField;
			cachedJoinedNsId_ = joinsTable->GetJoinedNsId(parentNsId, joinedField);
		}
		return cachedJoinedNsId_;
	}

	int nsid_;
	const ResultSerializer::JoinedData& joinedData_;
	const CoroQueryResults& qr_;
	ItemImpl<RPCClient> itemimpl_;
	int64_t cachedParentNsId_ = -1;
	size_t cachedJoinedField_ = std::numeric_limits<size_t>::max();
	uint32_t cachedJoinedNsId_ = 0;
	TagsMatcher tm_;
	std::forward_list<EncoderDatasourceWithJoins<Builder>> datasourcesWithJoins_;
};

void CoroQueryResults::Iterator::getJSONFromCJSON(std::string_view cjson, WrSerializer& wrser, bool withHdrLen) const {
	auto tm = qr_->GetTagsMatcher(itemParams_.nsid);
	JsonEncoder enc(&tm, nullptr);
	JsonBuilder builder(wrser, ObjType::TypePlain);
	EncoderContext<JsonBuilder> ctx;
	int shardId = (const_cast<Iterator*>(this))->GetShardID();
	if (qr_->NeedOutputShardId() && shardId >= 0) {
		ctx.fields.shardId = shardId;
	}
	if (qr_->HaveJoined() && itemParams_.joined.size()) {
		EncoderDatasourceWithJoins<JsonBuilder> joinsDs(itemParams_.nsid, itemParams_.joined, *qr_);
		ctx.joins = &joinsDs;
		if (qr_->NeedOutputRank()) {
			ctx.fields.rank = itemParams_.rank;
		}
		if (withHdrLen) {
			auto slicePosSaver = wrser.StartSlice();
		}
		enc.Encode(cjson, builder, ctx);
		return;
	}

	if (qr_->NeedOutputRank()) {
		ctx.fields.rank = itemParams_.rank;
	}
	if (withHdrLen) {
		auto slicePosSaver = wrser.StartSlice();
	}
	enc.Encode(cjson, builder, ctx);
}

void CoroQueryResults::Iterator::checkIdx() const {
	if (!isAvailable()) {
		throw Error(errNotValid, "QueryResults iterator refers to unavailable item index ({}). Current fetch offset is {}", idx_,
					qr_->i_.fetchOffset_);
	}
}

Error CoroQueryResults::Iterator::unavailableIdxError() const {
	return Error(errNotValid, "Requested item's index [{}] in not available in this QueryResults. Available indexes: [{}, {})", idx_,
				 qr_->i_.fetchOffset_, qr_->i_.queryParams_.qcount);
}

Error CoroQueryResults::Iterator::GetMsgPack(WrSerializer& wrser, bool withHdrLen) {
	try {
		checkIdx();
		readNext();
		int type = qr_->i_.queryParams_.flags & kResultsFormatMask;
		if (type == kResultsMsgPack) {
			if (withHdrLen) {
				wrser.PutSlice(itemParams_.data);
			} else {
				wrser.Write(itemParams_.data);
			}
		} else {
			return Error(errParseBin, "Impossible to get data in MsgPack because of a different format: {}", type);
		}
	} catch (const Error& err) {
		return err;
	}

	return {};
}

void CoroQueryResults::Iterator::getCSVFromCJSON(std::string_view cjson, WrSerializer& wrser, CsvOrdering& ordering) const {
	auto tm = qr_->GetTagsMatcher(itemParams_.nsid);
	CsvBuilder builder(wrser, ordering);
	CsvEncoder encoder(&tm, nullptr);

	if (qr_->HaveJoined() && itemParams_.joined.size()) {
		EncoderDatasourceWithJoins<CsvBuilder> joinsDs(itemParams_.nsid, itemParams_.joined, *qr_);
		EncoderContext<CsvBuilder> ctx;
		ctx.joins = &joinsDs;
		encoder.Encode(cjson, builder, ctx);
		return;
	}

	encoder.Encode(cjson, builder);
}

Error CoroQueryResults::Iterator::GetCSV(WrSerializer& wrser, CsvOrdering& ordering) noexcept {
	try {
		checkIdx();
		readNext();
		switch (qr_->i_.queryParams_.flags & kResultsFormatMask) {
			case kResultsCJson: {
				getCSVFromCJSON(itemParams_.data, wrser, ordering);
				return {};
			}
			default:
				return Error(errParseBin, "Server returned data in unexpected format {}", qr_->i_.queryParams_.flags & kResultsFormatMask);
		}
	} CATCH_AND_RETURN
	return {};
}

Error CoroQueryResults::Iterator::GetJSON(WrSerializer& wrser, bool withHdrLen) {
	try {
		checkIdx();
		readNext();
		switch (qr_->i_.queryParams_.flags & kResultsFormatMask) {
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
			case kResultsPure: {
				constexpr std::string_view kEmptyJSON("{}");
				if (withHdrLen) {
					wrser.PutSlice(kEmptyJSON);
				} else {
					wrser.Write(kEmptyJSON);
				}
				break;
			}
			default:
				return Error(errParseBin, "Server returned data in unknown format {}", qr_->i_.queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error& err) {
		return err;
	}
	return {};
}

Error CoroQueryResults::Iterator::GetCJSON(WrSerializer& wrser, bool withHdrLen) {
	try {
		checkIdx();
		readNext();
		switch (qr_->i_.queryParams_.flags & kResultsFormatMask) {
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
				return Error(errParseBin, "Server returned data in unknown format {}", qr_->i_.queryParams_.flags & kResultsFormatMask);
		}
	} catch (const Error& err) {
		return err;
	}
	return {};
}

Item CoroQueryResults::Iterator::GetItem() {
	Error err;
	try {
		checkIdx();
		readNext();
		Item item = qr_->i_.nsArray_[itemParams_.nsid]->NewItem();
		item.setID(itemParams_.id);
		item.setLSN(itemParams_.lsn);
		item.setShardID(itemParams_.shardId);

		switch (qr_->i_.queryParams_.flags & kResultsFormatMask) {
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
				char* endp = nullptr;
				err = item.FromJSON(itemParams_.data, &endp);
				break;
			}
			case kResultsPure:
				break;
			default:
				return Item();
		}
		if (err.ok()) {
			return item;
		}
	} catch (const Error& err) {
		return Item(err);
	}
	return Item(std::move(err));
}

lsn_t CoroQueryResults::Iterator::GetLSN() {
	readNext();
	return itemParams_.lsn;
}

int CoroQueryResults::Iterator::GetNSID() {
	readNext();
	return itemParams_.nsid;
}

IdType CoroQueryResults::Iterator::GetID() {
	readNext();
	return itemParams_.id;
}

int CoroQueryResults::Iterator::GetShardID() {
	readNext();
	if (qr_->i_.queryParams_.flags & kResultsWithShardId) {
		if (qr_->i_.queryParams_.shardId != ShardingKeyType::ProxyOff) {
			return qr_->i_.queryParams_.shardId;
		} else {
			return itemParams_.shardId;
		}
	}
	return ShardingKeyType::ProxyOff;
}

RankT CoroQueryResults::Iterator::GetRank() {
	readNext();
	return itemParams_.rank;
}

bool CoroQueryResults::Iterator::IsRanked() noexcept { return qr_->HaveRank(); }

bool CoroQueryResults::Iterator::IsRaw() {
	readNext();
	return itemParams_.raw;
}

std::string_view CoroQueryResults::Iterator::GetRaw() {
	readNext();
	assertrx(itemParams_.raw);
	return itemParams_.data;
}

const ResultSerializer::JoinedData& CoroQueryResults::Iterator::GetJoined() {
	readNext();
	return itemParams_.joined;
}

void CoroQueryResults::Iterator::readNext() {
	if (nextPos_ != 0 || !Status().ok()) {
		return;
	}

	std::string_view rawResult;
	if (qr_->i_.lazyMode_) {
		rawResult = std::string_view(qr_->i_.rawResult_.data() + qr_->i_.parsingData_.itemsPos,
									 qr_->i_.rawResult_.size() - qr_->i_.parsingData_.itemsPos);
	} else {
		rawResult = std::string_view(qr_->i_.rawResult_.data(), qr_->i_.rawResult_.size());
	}

	ResultSerializer ser(rawResult.substr(pos_));
	try {
		int format{qr_->i_.queryParams_.flags & kResultsFormatMask};
		itemParams_ = qr_->i_.queryFormat_ == QueryFormatV2 ? readItem(ser, format) : readItemV1(ser, format);
		nextPos_ = pos_ + ser.Pos();
	} catch (const Error& err) {
		const_cast<CoroQueryResults*>(qr_)->i_.status_ = err;
	}
}

ResultSerializer::ItemParams CoroQueryResults::Iterator::readItemV1(ResultSerializer& ser, int format) {
	ResultSerializer::ItemParams itemParams{ser.GetItemData(qr_->i_.queryParams_.flags, qr_->i_.queryParams_.shardId)};
	if (qr_->i_.queryParams_.flags & kResultsWithJoined) {
		std::ignore = format;
		assertrx_throw(format == kResultsCJson);
		auto joinedFields{ser.GetVarUInt()};
		for (uint64_t i = 0; i < joinedFields; ++i) {
			auto itemsCount{ser.GetVarUInt()};
			auto& joined{itemParams.joined.emplace_back(ResultSerializer::JoinedFieldData(itemsCount))};
			for (uint64_t j = 0; j < itemsCount; ++j) {
				joined[j] = ser.GetItemData(qr_->i_.queryParams_.flags, qr_->i_.queryParams_.shardId);
			}
		}
	}
	return itemParams;
}

ResultSerializer::ItemParams CoroQueryResults::Iterator::readItem(ResultSerializer& ser, int format) {
	ResultSerializer::ItemParams itemParams{ser.GetItemData(qr_->i_.queryParams_.flags, qr_->i_.queryParams_.shardId)};
	if (qr_->i_.queryParams_.flags & kResultsWithJoined) {
		assertrx_throw(format == kResultsCJson);
		auto joinedFields{ser.GetVarUInt()};
		for (uint64_t i = 0; i < joinedFields; ++i) {
			auto itemsCount{ser.GetVarUInt()};
			auto& joined{itemParams.joined.emplace_back(ResultSerializer::JoinedFieldData(itemsCount))};
			for (uint64_t j = 0; j < itemsCount; ++j) {
				joined[j] = readItem(ser, format);
			}
		}
	}
	return itemParams;
}

CoroQueryResults::Iterator& CoroQueryResults::Iterator::operator++() {
	try {
		readNext();
		idx_++;
		pos_ = nextPos_;
		nextPos_ = 0;
		if (idx_ != qr_->i_.queryParams_.qcount && idx_ == qr_->i_.queryParams_.count + qr_->i_.fetchOffset_) {
			const_cast<CoroQueryResults*>(qr_)->fetchNextResults();
			pos_ = 0;
		}
	} catch (const Error& err) {
		const_cast<CoroQueryResults*>(qr_)->i_.status_ = err;
	}

	return *this;
}

CoroQueryResults::Impl::Impl(cproto::CoroClientConnection* conn, CoroQueryResults::NsArray&& nsArray, int fetchFlags, int fetchAmount,
							 std::chrono::milliseconds timeout, bool lazyMode)
	: conn_(conn),
	  nsArray_(std::move(nsArray)),
	  fetchFlags_(fetchFlags),
	  fetchAmount_(fetchAmount),
	  requestTimeout_(timeout),
	  lazyMode_(lazyMode) {
	assert(conn_);
	const auto sessionTs = conn_->LoginTs();
	if (sessionTs.has_value()) {
		sessionTs_ = *sessionTs;
	}
	InitLazyData();
}

CoroQueryResults::Impl::Impl(int fetchFlags, int fetchAmount, bool lazyMode) noexcept
	: fetchFlags_(fetchFlags), fetchAmount_(fetchAmount), lazyMode_(lazyMode) {
	InitLazyData();
}

CoroQueryResults::Impl::Impl(NsArray&& nsArray) noexcept : nsArray_(std::move(nsArray)) { InitLazyData(); }

CoroQueryResults::Impl::Impl(Impl&&) noexcept = default;
CoroQueryResults::Impl& CoroQueryResults::Impl::operator=(Impl&&) noexcept = default;
CoroQueryResults::Impl::~Impl() = default;
}  // namespace reindexer::client
