#include "itemimpl.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/jsondecoder.h"

using std::move;

namespace reindexer {
namespace client {

ItemImpl &ItemImpl::operator=(ItemImpl &&other) noexcept {
	if (&other != this) {
		payloadType_ = std::move(other.payloadType_);
		payloadValue_ = std::move(other.payloadValue_);
		tagsMatcher_ = std::move(other.tagsMatcher_);
		jsonAllocator_ = std::move(other.jsonAllocator_);
		ser_ = std::move(other.ser_);
		tupleData_ = std::move(other.tupleData_);
		precepts_ = std::move(other.precepts_);
		unsafe_ = other.unsafe_;
		holder_ = std::move(other.holder_);
	}
	return *this;
}

// Construct item from compressed json
Error ItemImpl::FromCJSON(const string_view &slice) {
	GetPayload().Reset();
	string_view data = slice;
	if (!unsafe_) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	Serializer rdser(data);
	// check tags matcher update
	int tag = rdser.GetVarUint();
	uint32_t tmOffset = 0;
	if (tag == TAG_END) {
		tmOffset = rdser.GetUInt32();
		// read tags matcher update
		Serializer tser(slice.substr(tmOffset));
		tagsMatcher_.deserialize(tser);
		tagsMatcher_.setUpdated();
	} else
		rdser.SetPos(0);

	Payload pl = GetPayload();
	CJsonDecoder decoder(tagsMatcher_);
	auto err = decoder.Decode(&pl, rdser, ser_);

	if (err.ok() && !rdser.Eof() && rdser.Pos() != tmOffset)
		return Error(errParseJson, "Internal error - left unparsed data %d", int(rdser.Pos()));
	tupleData_ = make_key_string(ser_.Slice());
	pl.Set(0, {Variant(tupleData_)});

	return err;
}

Error ItemImpl::FromJSON(const string_view &slice, char **endp, bool /*pkOnly*/) {
	string_view data = slice;
	if (!unsafe_ && endp == nullptr) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	const char *json = data.data();
	payloadValue_.Clone();
	char *endptr = nullptr;
	JsonValue value;
	int status = jsonParse(const_cast<char *>(json), &endptr, &value, jsonAllocator_);
	if (status != JSON_OK) return Error(errLogic, "Error parsing json - status %d\n", status);
	if (endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_);
	Payload pl = GetPayload();
	auto err = decoder.Decode(&pl, ser_, value);

	if (err.ok()) {
		// Put tuple to field[0]
		tupleData_ = make_key_string(ser_.Slice());
		pl.Set(0, {Variant(tupleData_)});
	}
	return err;
}

Error ItemImpl::FromCJSON(ItemImpl *other) {
	auto cjson = other->GetCJSON();
	auto err = FromCJSON(cjson);
	assert(err.ok());
	return err;
}

string_view ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	JsonBuilder builder(ser_, JsonBuilder::TypePlain);
	JsonEncoder encoder(&tagsMatcher_);

	ser_.Reset();
	encoder.Encode(&pl, builder);

	return ser_.Slice();
}

string_view ItemImpl::GetCJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	CJsonBuilder builder(ser_, CJsonBuilder::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_);

	ser_.Reset();
	ser_.PutVarUint(TAG_END);
	int pos = ser_.Len();
	ser_.PutUInt32(0);
	encoder.Encode(&pl, builder);

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
