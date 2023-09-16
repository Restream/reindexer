#include "itemimpl.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"

namespace reindexer {
namespace client {

ItemImpl &ItemImpl::operator=(ItemImpl &&other) noexcept {
	if (&other != this) {
		payloadType_ = std::move(other.payloadType_);
		payloadValue_ = std::move(other.payloadValue_);
		tagsMatcher_ = std::move(other.tagsMatcher_);
		ser_ = std::move(other.ser_);
		tupleData_ = std::move(other.tupleData_);
		precepts_ = std::move(other.precepts_);
		unsafe_ = other.unsafe_;
		holder_ = std::move(other.holder_);
	}
	return *this;
}

// Construct item from compressed json
void ItemImpl::FromCJSON(std::string_view slice) {
	GetPayload().Reset();
	std::string_view data = slice;
	if (!unsafe_) {
		holder_.push_back(std::string(slice));
		data = holder_.back();
	}

	Serializer rdser(data);
	// check tags matcher update
	const ctag tag = rdser.GetCTag();
	uint32_t tmOffset = 0;
	if (tag == kCTagEnd) {
		tmOffset = rdser.GetUInt32();
		// read tags matcher update
		Serializer tser(slice.substr(tmOffset));
		tagsMatcher_.deserialize(tser);
		tagsMatcher_.setUpdated();
	} else {
		rdser.SetPos(0);
	}

	Payload pl = GetPayload();
	CJsonDecoder decoder(tagsMatcher_);
	ser_.Reset();
	decoder.Decode(pl, rdser, ser_);

	if (!rdser.Eof() && rdser.Pos() != tmOffset) {
		throw Error(errParseJson, "Internal error - left unparsed data %d", rdser.Pos());
	}
	tupleData_.assign(ser_.Slice().data(), ser_.Slice().size());
	pl.Set(0, Variant(p_string(&tupleData_)));
}

Error ItemImpl::FromJSON(std::string_view slice, char **endp, bool /*pkOnly*/) {
	std::string_view data = slice;
	if (!unsafe_ && endp == nullptr) {
		holder_.emplace_back(slice);
		data = holder_.back();
	}

	payloadValue_.Clone();

	size_t len;
	gason::JsonNode node;
	gason::JsonParser parser(&largeJSONStrings_);
	try {
		node = parser.Parse(giftStr(data), &len);
		if (node.value.getTag() != gason::JSON_OBJECT) return Error(errParseJson, "Expected json object");
		if (unsafe_ && endp) {
			*endp = const_cast<char *>(data.data()) + len;
		}
	} catch (gason::Exception &e) {
		return Error(errParseJson, "Error parsing json: '%s', pos: %d", e.what(), len);
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_);
	Payload pl = GetPayload();
	ser_.Reset();
	auto err = decoder.Decode(pl, ser_, node.value);

	if (err.ok()) {
		// Put tuple to field[0]
		tupleData_.assign(ser_.Slice().data(), ser_.Slice().size());
		pl.Set(0, Variant(p_string(&tupleData_)));
		ser_ = WrSerializer();
	}
	return err;
}

Error ItemImpl::FromMsgPack(std::string_view buf, size_t &offset) {
	Payload pl = GetPayload();
	MsgPackDecoder decoder(tagsMatcher_);

	ser_.Reset();
	Error err = decoder.Decode(buf, pl, ser_, offset);
	if (err.ok()) {
		tupleData_.assign(ser_.Slice().data(), ser_.Slice().size());
		pl.Set(0, Variant(p_string(&tupleData_)));
	}
	return err;
}

void ItemImpl::FromCJSON(ItemImpl *other) {
	auto cjson = other->GetCJSON();
	FromCJSON(cjson);
}

std::string_view ItemImpl::GetMsgPack() {
	int startTag = 0;
	ConstPayload pl = GetConstPayload();

	MsgPackEncoder msgpackEncoder(&tagsMatcher_);
	const TagsLengths &tagsLengths = msgpackEncoder.GetTagsMeasures(pl);

	ser_.Reset();
	MsgPackBuilder msgpackBuilder(ser_, &tagsLengths, &startTag, ObjType::TypePlain, &tagsMatcher_);
	msgpackEncoder.Encode(pl, msgpackBuilder);

	return ser_.Slice();
}

std::string_view ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	JsonBuilder builder(ser_, ObjType::TypePlain);
	JsonEncoder encoder(&tagsMatcher_);

	ser_.Reset();
	encoder.Encode(pl, builder);

	return ser_.Slice();
}

std::string_view ItemImpl::GetCJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	CJsonBuilder builder(ser_, ObjType::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_);

	ser_.Reset();
	ser_.PutCTag(kCTagEnd);
	int pos = ser_.Len();
	ser_.PutUInt32(0);
	encoder.Encode(pl, builder);

	if (tagsMatcher_.isUpdated()) {
		uint32_t tmOffset = ser_.Len();
		memcpy(ser_.Buf() + pos, &tmOffset, sizeof(tmOffset));
		tagsMatcher_.serialize(ser_);
		return ser_.Slice();
	}

	return ser_.Slice().substr(sizeof(uint32_t) + 1);
}

}  // namespace client
}  // namespace reindexer
