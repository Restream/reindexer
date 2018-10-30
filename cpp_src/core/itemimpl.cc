#include "core/itemimpl.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/jsondecoder.h"
#include "core/keyvalue/p_string.h"
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
		cjson_ = std::move(other.cjson_);
		ns_ = std::move(other.ns_);
	}
	return *this;
}

void ItemImpl::SetField(int field, const VariantArray &krs) {
	cjson_ = string_view();
	payloadValue_.Clone();
	if (!unsafe_ && !krs.empty() && krs[0].Type() == KeyValueString) {
		VariantArray krsCopy;
		krsCopy.reserve(krs.size());
		for (auto &kr : krs) {
			holder_.push_back(kr.As<string>());
			krsCopy.push_back(Variant(p_string(&holder_.back())));
		}

		GetPayload().Set(field, krsCopy, false);

	} else {
		GetPayload().Set(field, krs, false);
	}
}

Variant ItemImpl::GetField(int field) { return GetPayload().Get(field, 0); }

// Construct item from compressed json
Error ItemImpl::FromCJSON(const string_view &slice, bool pkOnly) {
	GetPayload().Reset();
	string_view data = slice;
	if (!unsafe_) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	// check tags matcher update

	uint32_t tmOffset = 0;
	try {
		Serializer rdser(data);
		int tag = rdser.GetVarUint();
		if (tag == TAG_END) {
			tmOffset = rdser.GetUInt32();
			// read tags matcher update
			Serializer tser(slice.substr(tmOffset));
			tagsMatcher_.deserialize(tser);
			tagsMatcher_.setUpdated();
			data = data.substr(1 + sizeof(uint32_t), tmOffset - 5);
		}
	} catch (const Error &err) {
		return err;
	}
	cjson_ = data;
	Serializer rdser(data);

	Payload pl = GetPayload();
	CJsonDecoder decoder(tagsMatcher_, pkOnly ? &pkFields_ : nullptr);
	auto err = decoder.Decode(&pl, rdser, ser_);

	if (err.ok() && !rdser.Eof()) return Error(errParseJson, "Internal error - left unparsed data %d", int(rdser.Pos()));

	tupleData_.assign(ser_.Slice().data(), ser_.Slice().size());
	pl.Set(0, {Variant(p_string(&tupleData_))});
	return err;
}

Error ItemImpl::FromJSON(const string_view &slice, char **endp, bool pkOnly) {
	string_view data = slice;
	cjson_ = string_view();

	if (!unsafe_ && endp == nullptr) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	payloadValue_.Clone();
	char *endptr = nullptr;
	JsonValue value;
	int status = jsonParse(const_cast<char *>(data.data()), &endptr, &value, jsonAllocator_);
	if (status != JSON_OK) return Error(errParseJson, "Error parsing json: '%s'", jsonStrError(status));
	if (value.getTag() != JSON_OBJECT) return Error(errParseJson, "Expected json object");
	if (endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_, pkOnly ? &pkFields_ : nullptr);
	Payload pl = GetPayload();

	auto err = decoder.Decode(&pl, ser_, value);

	// Put tuple to field[0]
	tupleData_.assign(ser_.Slice().data(), ser_.Slice().size());
	pl.Set(0, {Variant(p_string(&tupleData_))});
	return err;
}

Error ItemImpl::FromCJSON(ItemImpl *other) {
	auto ret = FromCJSON(other->GetCJSON());
	cjson_ = string_view();
	return ret;
}

string_view ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);

	JsonEncoder encoder(&tagsMatcher_);
	JsonBuilder builder(ser_, JsonBuilder::TypePlain);

	ser_.Reset();
	encoder.Encode(&pl, builder);

	return ser_.Slice();
}

string_view ItemImpl::GetCJSON() {
	if (cjson_.size()) return cjson_;
	ser_.Reset();
	return GetCJSON(ser_);
}

string_view ItemImpl::GetCJSON(WrSerializer &ser) {
	if (cjson_.size()) {
		ser.Write(cjson_);
		return ser.Slice();
	}

	ConstPayload pl(payloadType_, payloadValue_);

	CJsonBuilder builder(ser, CJsonBuilder::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_);

	encoder.Encode(&pl, builder);

	return ser.Slice();
}

VariantArray ItemImpl::GetValueByJSONPath(const string &jsonPath) {
	ConstPayload pl(payloadType_, payloadValue_);
	VariantArray krefs;
	return pl.GetByJsonPath(jsonPath, tagsMatcher_, krefs, KeyValueUndefined);
}

}  // namespace reindexer
