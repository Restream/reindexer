#include "core/itemimpl.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsonmodifier.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/jsondecoder.h"
#include "core/keyvalue/p_string.h"
#include "tools/logger.h"

using std::move;

namespace reindexer {

ItemImplRawData &ItemImplRawData::operator=(ItemImplRawData &&other) noexcept {
	if (&other != this) {
		payloadValue_ = std::move(other.payloadValue_);
		tupleData_ = std::move(other.tupleData_);
		sourceData_ = std::move(other.sourceData_);
		precepts_ = std::move(other.precepts_);
		holder_ = std::move(other.holder_);
	}
	return *this;
}
ItemImplRawData::ItemImplRawData(ItemImplRawData &&other) noexcept
	: payloadValue_(std::move(other.payloadValue_)),
	  tupleData_(std::move(other.tupleData_)),
	  sourceData_(std::move(other.sourceData_)),
	  precepts_(std::move(other.precepts_)),
	  holder_(std::move(other.holder_)) {}

ItemImpl &ItemImpl::operator=(ItemImpl &&other) noexcept {
	if (&other != this) {
		ItemImplRawData::operator=(std::move(other));
		payloadType_ = std::move(other.payloadType_);
		tagsMatcher_ = std::move(other.tagsMatcher_);
		ser_ = std::move(other.ser_);
		unsafe_ = other.unsafe_;
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
		if (!holder_) holder_.reset(new std::deque<std::string>);
		for (auto &kr : krs) {
			holder_->push_back(kr.As<string>());
			krsCopy.push_back(Variant(p_string(&holder_->back())));
		}

		GetPayload().Set(field, krsCopy, false);

	} else {
		GetPayload().Set(field, krs, false);
	}
}

void ItemImpl::ModifyField(string_view jsonPath, const VariantArray &keys, FieldModifyMode mode) {
	Payload pl = GetPayload();

	ser_.Reset();
	ser_.PutUInt32(0);
	WrSerializer generatedCjson;
	string_view cjson(pl.Get(0, 0));
	if (cjson.empty()) {
		buildPayloadTuple(&pl, &tagsMatcher_, generatedCjson);
		cjson = generatedCjson.Slice();
	}

	Error err;
	CJsonModifier cjsonModifier(tagsMatcher_, payloadType_);
	switch (mode) {
		case FieldModeSet:
			err = cjsonModifier.SetFieldValue(cjson, tagsMatcher_.path2tag(jsonPath), keys, ser_);
			break;
		case FieldModeDrop:
			err = cjsonModifier.RemoveFieldValue(cjson, tagsMatcher_.path2tag(jsonPath), ser_);
			break;
		default:
			std::abort();
	}
	if (!err.ok()) throw Error(errLogic, "Error modifying field value: '%s'", err.what());

	tupleData_ = ser_.DetachBuf();
	pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
}

void ItemImpl::SetField(string_view jsonPath, const VariantArray &keys) { ModifyField(jsonPath, keys, FieldModeSet); }
void ItemImpl::DropField(string_view jsonPath) { ModifyField(jsonPath, {}, FieldModeDrop); }
Variant ItemImpl::GetField(int field) { return GetPayload().Get(field, 0); }

// Construct item from compressed json
Error ItemImpl::FromCJSON(const string_view &slice, bool pkOnly) {
	GetPayload().Reset();
	string_view data = slice;
	if (!unsafe_) {
		sourceData_.reset(new char[slice.size()]);
		std::copy(data.begin(), data.end(), sourceData_.get());
		data = string_view(sourceData_.get(), data.size());
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
	ser_.Reset();
	ser_.PutUInt32(0);
	auto err = decoder.Decode(&pl, rdser, ser_);

	if (err.ok() && !rdser.Eof()) return Error(errParseJson, "Internal error - left unparsed data %d", rdser.Pos());

	tupleData_ = ser_.DetachBuf();
	pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
	return err;
}

Error ItemImpl::FromJSON(const string_view &slice, char **endp, bool pkOnly) {
	string_view data = slice;
	cjson_ = string_view();

	if (!unsafe_ && endp == nullptr) {
		sourceData_.reset(new char[slice.size()]);
		std::copy(data.begin(), data.end(), sourceData_.get());
		data = string_view(sourceData_.get(), data.size());
	}

	payloadValue_.Clone();
	char *endptr = nullptr;
	gason::JsonValue value;
	gason::JsonAllocator allocator;
	int status = jsonParse(giftStr(data), &endptr, &value, allocator);
	if (status != gason::JSON_OK) return Error(errParseJson, "Error parsing json: '%s'", gason::jsonStrError(status));
	if (value.getTag() != gason::JSON_OBJECT) return Error(errParseJson, "Expected json object");
	if (endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_, pkOnly ? &pkFields_ : nullptr);
	Payload pl = GetPayload();

	ser_.Reset();
	ser_.PutUInt32(0);
	auto err = decoder.Decode(&pl, ser_, value);

	// Put tuple to field[0]
	tupleData_ = ser_.DetachBuf();
	pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
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

string_view ItemImpl::GetCJSON(bool withTagsMatcher) {
	withTagsMatcher = withTagsMatcher && tagsMatcher_.isUpdated();

	if (cjson_.size() && !withTagsMatcher) return cjson_;
	ser_.Reset();
	return GetCJSON(ser_, withTagsMatcher);
}

string_view ItemImpl::GetCJSON(WrSerializer &ser, bool withTagsMatcher) {
	withTagsMatcher = withTagsMatcher && tagsMatcher_.isUpdated();

	if (cjson_.size() && !withTagsMatcher) {
		ser.Write(cjson_);
		return ser.Slice();
	}

	ConstPayload pl(payloadType_, payloadValue_);

	CJsonBuilder builder(ser, CJsonBuilder::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_);

	if (withTagsMatcher) {
		ser.PutVarUint(TAG_END);
		int pos = ser.Len();
		ser.PutUInt32(0);
		encoder.Encode(&pl, builder);
		uint32_t tmOffset = ser.Len();
		memcpy(ser.Buf() + pos, &tmOffset, sizeof(tmOffset));
		tagsMatcher_.serialize(ser);
	} else {
		encoder.Encode(&pl, builder);
	}

	return ser.Slice();
}

VariantArray ItemImpl::GetValueByJSONPath(string_view jsonPath) {
	ConstPayload pl(payloadType_, payloadValue_);
	VariantArray krefs;
	return pl.GetByJsonPath(jsonPath, tagsMatcher_, krefs, KeyValueUndefined);
}

}  // namespace reindexer
