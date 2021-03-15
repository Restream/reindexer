#include "core/itemimpl.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsonmodifier.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/msgpackdecoder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/cjson/protobufdecoder.h"
#include "core/keyvalue/p_string.h"
#include "core/namespace/namespace.h"
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
		msgPackDecoder_ = std::move(other.msgPackDecoder_);
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

void ItemImpl::ModifyField(string_view jsonPath, const VariantArray &keys, std::function<VariantArray(string_view)> ev,
						   FieldModifyMode mode) {
	return ModifyField(tagsMatcher_.path2indexedtag(jsonPath, ev, mode != FieldModeDrop), keys, mode);
}

void ItemImpl::ModifyField(const IndexedTagsPath &tagsPath, const VariantArray &keys, FieldModifyMode mode) {
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
			err = cjsonModifier.SetFieldValue(cjson, tagsPath, keys, ser_);
			break;
		case FieldModeSetJson:
			err = cjsonModifier.SetObject(cjson, tagsPath, keys, ser_, &pl);
			break;
		case FieldModeDrop:
			err = cjsonModifier.RemoveField(cjson, tagsPath, ser_);
			break;
		default:
			throw Error(errLogic, "Update mode is not supported: %d", mode);
	}
	if (!err.ok()) throw Error(errLogic, "Error modifying field value: '%s'", err.what());

	tupleData_ = ser_.DetachLStr();
	pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
}

void ItemImpl::SetField(string_view jsonPath, const VariantArray &keys, IndexExpressionEvaluator ev) {
	ModifyField(jsonPath, keys, ev, FieldModeSet);
}
void ItemImpl::DropField(string_view jsonPath, IndexExpressionEvaluator ev) { ModifyField(jsonPath, {}, ev, FieldModeDrop); }
Variant ItemImpl::GetField(int field) { return GetPayload().Get(field, 0); }
void ItemImpl::GetField(int field, VariantArray &values) { GetPayload().Get(field, values); }

Error ItemImpl::FromMsgPack(string_view buf, size_t &offset) {
	Payload pl = GetPayload();
	if (!msgPackDecoder_) {
		msgPackDecoder_.reset(new MsgPackDecoder(&tagsMatcher_));
	}

	ser_.Reset();
	ser_.PutUInt32(0);
	Error err = msgPackDecoder_->Decode(buf, &pl, ser_, offset);
	if (err.ok()) {
		tupleData_ = ser_.DetachLStr();
		pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
	}
	return err;
}

Error ItemImpl::FromProtobuf(string_view buf) {
	assert(ns_);
	Payload pl = GetPayload();
	ProtobufDecoder decoder(tagsMatcher_, schema_);

	ser_.Reset();
	ser_.PutUInt32(0);
	Error err = decoder.Decode(buf, &pl, ser_);
	if (err.ok()) {
		tupleData_ = ser_.DetachLStr();
		pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
	}
	return err;
}

Error ItemImpl::GetMsgPack(WrSerializer &wrser) {
	int startTag = 0;
	ConstPayload pl = GetConstPayload();

	MsgPackEncoder msgpackEncoder(&tagsMatcher_);
	const TagsLengths &tagsLengths = msgpackEncoder.GetTagsMeasures(&pl);

	MsgPackBuilder msgpackBuilder(wrser, &tagsLengths, &startTag, ObjType::TypePlain, &tagsMatcher_);
	msgpackEncoder.Encode(&pl, msgpackBuilder);
	return errOK;
}

Error ItemImpl::GetProtobuf(WrSerializer &wrser) {
	assert(ns_);
	ConstPayload pl = GetConstPayload();
	ProtobufBuilder protobufBuilder(&wrser, ObjType::TypePlain, schema_.get(), &tagsMatcher_);
	ProtobufEncoder protobufEncoder(&tagsMatcher_);
	protobufEncoder.Encode(&pl, protobufBuilder);
	return errOK;
}

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

	tupleData_ = ser_.DetachLStr();
	pl.Set(0, {Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())))});
	return err;
}

Error ItemImpl::FromJSON(string_view slice, char **endp, bool pkOnly) {
	string_view data = slice;
	cjson_ = string_view();

	if (!unsafe_) {
		if (endp) {
			size_t len = 0;
			try {
				gason::JsonParser parser;
				parser.Parse(data, &len);
				*endp = const_cast<char *>(data.data()) + len;
				sourceData_.reset(new char[len]);
				std::copy(data.begin(), data.begin() + len, sourceData_.get());
				data = string_view(sourceData_.get(), len);
			} catch (const gason::Exception &e) {
				return Error(errParseJson, "Error parsing json: '%s'", e.what());
			}
		} else {
			sourceData_.reset(new char[slice.size()]);
			std::copy(data.begin(), data.end(), sourceData_.get());
			data = string_view(sourceData_.get(), data.size());
		}
	}

	payloadValue_.Clone();
	gason::JsonValue value;
	gason::JsonAllocator allocator;
	char *endptr = nullptr;
	int status = jsonParse(data, &endptr, &value, allocator);
	if (status != gason::JSON_OK) return Error(errParseJson, "Error parsing json: '%s'", gason::jsonStrError(status));
	if (value.getTag() != gason::JSON_OBJECT) return Error(errParseJson, "Expected json object");
	if (unsafe_ && endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_, pkOnly ? &pkFields_ : nullptr);
	Payload pl = GetPayload();

	ser_.Reset();
	ser_.PutUInt32(0);
	auto err = decoder.Decode(&pl, ser_, value);

	// Put tuple to field[0]
	tupleData_ = ser_.DetachLStr();
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
	JsonBuilder builder(ser_, ObjType::TypePlain);

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

	CJsonBuilder builder(ser, ObjType::TypePlain);
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
