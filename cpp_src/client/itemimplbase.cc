#include "client/itemimplbase.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "estl/gift_str.h"

namespace reindexer {
namespace client {

// Construct item from compressed json
void ItemImplBase::FromCJSON(std::string_view slice) {
	GetPayload().Reset();
	std::string_view data = slice;
	if (!unsafe_) {
		holder_.clear();
		data = holder_.emplace_back(data);
	}

	Serializer rdser(data);
	uint32_t tmOffset = 0;
	const bool hasBundledTm = ReadBundledTmTag(rdser);
	if (hasBundledTm) {
		tmOffset = rdser.GetUInt32();
		// read tags matcher update
		Serializer tser(slice.substr(tmOffset));
		tagsMatcher_.deserialize(tser);
		tagsMatcher_.setUpdated();
	} else {
		rdser.SetPos(0);
	}

	Payload pl = GetPayload();
	ScalarIndexesSetT objectScalarIndexes;
	CJsonDecoder decoder(pl, rdser, ser_, tagsMatcher_, holder_, floatVectorsHolder_, objectScalarIndexes);
	ser_.Reset();
	try {
		decoder.Decode(CJsonDecoder::DefaultFilter{nullptr});
	} catch (const Error& e) {
		if (!hasBundledTm) {
			const auto err = tryToUpdateTagsMatcher();
			if (!err.ok()) {
				throw Error(errParseJson, "Error parsing CJSON: [{}]; [{}]", e.what(), err.what());
			}
			ser_.Reset();
			rdser.SetPos(0);
			CJsonDecoder decoderSecondAttempt(pl, rdser, ser_, tagsMatcher_, holder_, floatVectorsHolder_, objectScalarIndexes);
			decoderSecondAttempt.Decode(CJsonDecoder::DefaultFilter{nullptr});
		}
	}

	if (!rdser.Eof() && rdser.Pos() != tmOffset) {
		throw Error(errParseJson, "Internal error - left unparsed data {}", rdser.Pos());
	}

	const auto tupleSize = ser_.Len();
	tupleHolder_ = ser_.DetachBuf();
	tupleData_ = std::string_view(reinterpret_cast<char*>(tupleHolder_.get()), tupleSize);
	pl.Set(0, Variant{p_string(&tupleData_), Variant::noHold});
}

Error ItemImplBase::FromJSON(std::string_view slice, char** endp, bool /*pkOnly*/) {
	std::string_view data = slice;
	if (!unsafe_ && endp == nullptr) {
		holder_.clear();
		data = holder_.emplace_back(data);
	}

	payloadValue_.Clone();

	size_t len = 0;
	gason::JsonNode node;
	gason::JsonParser parser(&largeJSONStrings_);
	try {
		node = parser.Parse(giftStr(data), &len);
		if (node.value.getTag() != gason::JsonTag::OBJECT) {
			return Error(errParseJson, "Expected json object");
		}
		if (unsafe_ && endp) {
			*endp = const_cast<char*>(data.data()) + len;
		}
	} catch (gason::Exception& e) {
		return Error(errParseJson, "Error parsing json: '{}', pos: {}", e.what(), len);
	}

	ScalarIndexesSetT objectScalarIndexes;
	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_, floatVectorsHolder_, objectScalarIndexes);
	Payload pl = GetPayload();
	ser_.Reset();

	auto err = decoder.Decode(pl, ser_, node.value);

	if (err.ok()) {
		// Put tuple to field[0]
		const auto tupleSize = ser_.Len();
		tupleHolder_ = ser_.DetachBuf();
		tupleData_ = std::string_view(reinterpret_cast<char*>(tupleHolder_.get()), tupleSize);
		pl.Set(0, Variant(p_string(&tupleData_), Variant::noHold));
	}
	return err;
}

Error ItemImplBase::FromMsgPack(std::string_view buf, size_t& offset) {
	Payload pl = GetPayload();

	std::string_view data = buf;
	if (!unsafe_) {
		holder_.clear();
		data = holder_.emplace_back(data);
	}

	ser_.Reset();
	ScalarIndexesSetT objectScalarIndexes;
	MsgPackDecoder decoder(data, pl, ser_, tagsMatcher_, offset, floatVectorsHolder_, objectScalarIndexes);
	Error err = decoder.Decode();
	if (err.ok()) {
		const auto tupleSize = ser_.Len();
		tupleHolder_ = ser_.DetachBuf();
		tupleData_ = std::string_view(reinterpret_cast<char*>(tupleHolder_.get()), tupleSize);
		pl.Set(0, Variant(p_string(&tupleData_), Variant::noHold));
	}
	return err;
}

void ItemImplBase::FromCJSON(ItemImplBase* other) {
	auto cjson = other->GetCJSON();
	FromCJSON(cjson);
}

std::string_view ItemImplBase::GetMsgPack() {
	int startTag = 0;
	ConstPayload pl = GetConstPayload();

	MsgPackEncoder msgpackEncoder(&tagsMatcher_, nullptr);
	const TagsLengths& tagsLengths = msgpackEncoder.GetTagsMeasures(pl);

	ser_.Reset();
	MsgPackBuilder msgpackBuilder(ser_, &tagsLengths, &startTag, ObjType::TypePlain, &tagsMatcher_);
	msgpackEncoder.Encode(pl, msgpackBuilder);

	return ser_.Slice();
}

std::string_view ItemImplBase::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	JsonBuilder builder(ser_, ObjType::TypePlain);
	JsonEncoder encoder(&tagsMatcher_, nullptr);

	ser_.Reset();
	encoder.Encode(pl, builder);

	return ser_.Slice();
}

std::string_view ItemImplBase::GetCJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	CJsonBuilder builder(ser_, ObjType::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_, nullptr);

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

void ItemImplBase::GetPrecepts(WrSerializer& ser) {
	if (precepts_.size()) {
		ser.PutVarUint(precepts_.size());
		for (auto& p : precepts_) {
			ser.PutVString(p);
		}
	}
}

}  // namespace client
}  // namespace reindexer
