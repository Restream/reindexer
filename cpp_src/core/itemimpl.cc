#include "core/itemimpl.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsonencoder.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/jsonencoder.h"
#include "tools/logger.h"

using std::move;

namespace reindexer {
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

void ItemImpl::SetField(int field, const KeyRefs &krs) {
	payloadValue_.AllocOrClone(0);
	if (!unsafe_ && !krs.empty() && krs[0].Type() == KeyValueString) {
		KeyRefs krsCopy;
		krsCopy.reserve(krs.size());
		for (auto &kr : krs) {
			holder_.push_back(kr.As<string>());
			krsCopy.push_back(KeyRef(p_string(&holder_.back())));
		}

		GetPayload().Set(field, krsCopy, false);

	} else {
		GetPayload().Set(field, krs, false);
	}
}

KeyRef ItemImpl::GetField(int field) {
	KeyRefs kr;
	GetPayload().Get(field, kr);
	return kr[0];
}

// Construct item from compressed json
Error ItemImpl::FromCJSON(const string_view &slice, bool pkOnly) {
	GetPayload().Reset();
	string_view data = slice;
	if (!unsafe_) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	Serializer rdser(data);
	// check tags matcher update
	uint32_t tmOffset = rdser.GetUInt32();
	uint32_t cacheToken = 0;

	if (tmOffset) {
		// read tags matcher update
		Serializer tser(slice.substr(tmOffset));
		cacheToken = tser.GetVarUint();
		if (cacheToken != tagsMatcher_.cacheToken()) {
			return Error(errParams, "cacheToken mismatch:  %08X, need %08X. Can't process item\n", cacheToken, tagsMatcher_.cacheToken());
		}
		if (!tser.Eof()) {
			tagsMatcher_.deserialize(tser);
			tagsMatcher_.setUpdated();
		}
	}

	Payload pl = GetPayload();
	CJsonDecoder decoder(tagsMatcher_, pkOnly ? pkFields_ : FieldsSet{});
	auto err = decoder.Decode(&pl, rdser, ser_);

	if (err.ok() && !rdser.Eof() && rdser.Pos() != tmOffset)
		return Error(errParseJson, "Internal error - left unparsed data %d", int(rdser.Pos()));

	tupleData_ = make_key_string(ser_.Slice());
	pl.Set(0, {KeyRef(tupleData_)});
	return err;
}

Error ItemImpl::FromJSON(const string_view &slice, char **endp, bool pkOnly) {
	string_view data = slice;

	if (!unsafe_ && endp == nullptr) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	payloadValue_.AllocOrClone(0);
	char *endptr = nullptr;
	JsonValue value;
	int status = jsonParse(const_cast<char *>(data.data()), &endptr, &value, jsonAllocator_);
	if (status != JSON_OK) return Error(errParseJson, "Error parsing json: '%s'", jsonStrError(status));
	if (value.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected json object");
	if (endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_, pkOnly ? pkFields_ : FieldsSet{});
	Payload pl = GetPayload();

	auto err = decoder.Decode(&pl, ser_, value);

	// Put tuple to field[0]
	tupleData_ = make_key_string(ser_.Slice());
	pl.Set(0, {KeyRef(tupleData_)});
	return err;
}

Error ItemImpl::FromCJSON(ItemImpl *other) {
	auto cjson = other->GetCJSON();
	return FromCJSON(cjson);
}

string_view ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	JsonPrintFilter filter;
	JsonEncoder encoder(tagsMatcher_, filter);

	ser_.Reset();
	encoder.Encode(&pl, ser_);

	return ser_.Slice();
}

string_view ItemImpl::GetCJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	JsonPrintFilter filter;
	CJsonEncoder encoder(tagsMatcher_, filter);

	ser_.Reset();
	ser_.PutUInt32(0);
	encoder.Encode(&pl, ser_);

	return ser_.Slice();
}

KeyRefs ItemImpl::GetValueByJSONPath(const string &jsonPath) {
	ConstPayload pl(payloadType_, payloadValue_);
	KeyRefs krefs;
	return pl.GetByJsonPath(jsonPath, tagsMatcher_, krefs);
}

}  // namespace reindexer
