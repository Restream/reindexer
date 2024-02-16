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

namespace reindexer {

void ItemImpl::SetField(int field, const VariantArray &krs) {
	cjson_ = std::string_view();
	payloadValue_.Clone();
	if (!unsafe_ && !krs.empty() && krs[0].Type().Is<KeyValueType::String>() &&
		!payloadType_.Field(field).Type().Is<KeyValueType::Uuid>()) {
		VariantArray krsCopy;
		krsCopy.reserve(krs.size());
		if (!holder_) holder_ = std::make_unique<std::deque<std::string>>();
		for (auto &kr : krs) {
			holder_->push_back(kr.As<std::string>());
			krsCopy.emplace_back(p_string{&holder_->back()});
		}

		GetPayload().Set(field, krsCopy, false);

	} else {
		GetPayload().Set(field, krs, false);
	}
}

void ItemImpl::ModifyField(std::string_view jsonPath, const VariantArray &keys, const IndexExpressionEvaluator &ev, FieldModifyMode mode) {
	ModifyField(tagsMatcher_.path2indexedtag(jsonPath, ev, mode != FieldModeDrop), keys, mode);
}

void ItemImpl::ModifyField(const IndexedTagsPath &tagsPath, const VariantArray &keys, FieldModifyMode mode) {
	Payload pl = GetPayload();

	ser_.Reset();
	ser_.PutUInt32(0);
	WrSerializer generatedCjson;
	const auto cjsonV = pl.Get(0, 0);
	std::string_view cjson(cjsonV);
	if (cjson.empty()) {
		buildPayloadTuple(pl, &tagsMatcher_, generatedCjson);
		cjson = generatedCjson.Slice();
	}

	CJsonModifier cjsonModifier(tagsMatcher_, payloadType_);
	try {
		switch (mode) {
			case FieldModeSet:
				cjsonModifier.SetFieldValue(cjson, tagsPath, keys, ser_, pl);
				break;
			case FieldModeSetJson:
				cjsonModifier.SetObject(cjson, tagsPath, keys, ser_, pl);
				break;
			case FieldModeDrop:
				cjsonModifier.RemoveField(cjson, tagsPath, ser_);
				break;
			case FieldModeArrayPushBack:
			case FieldModeArrayPushFront:
				throw Error(errLogic, "Update mode is not supported: %d", mode);
		}
	} catch (const Error &e) {
		throw Error(e.code(), "Error modifying field value: '%s'", e.what());
	} catch (std::exception &e) {
		throw Error(errLogic, "Error modifying field value: '%s'", e.what());
	}

	tupleData_ = ser_.DetachLStr();
	pl.Set(0, Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())), Variant::no_hold_t{}));
}

void ItemImpl::SetField(std::string_view jsonPath, const VariantArray &keys, const IndexExpressionEvaluator &ev) {
	ModifyField(jsonPath, keys, ev, FieldModeSet);
}
void ItemImpl::DropField(std::string_view jsonPath, const IndexExpressionEvaluator &ev) { ModifyField(jsonPath, {}, ev, FieldModeDrop); }
Variant ItemImpl::GetField(int field) { return GetPayload().Get(field, 0); }
void ItemImpl::GetField(int field, VariantArray &values) { GetPayload().Get(field, values); }

Error ItemImpl::FromMsgPack(std::string_view buf, size_t &offset) {
	Payload pl = GetPayload();
	if (!msgPackDecoder_) {
		msgPackDecoder_.reset(new MsgPackDecoder(tagsMatcher_));
	}

	ser_.Reset();
	ser_.PutUInt32(0);
	Error err = msgPackDecoder_->Decode(buf, pl, ser_, offset);
	if (err.ok()) {
		tupleData_ = ser_.DetachLStr();
		pl.Set(0, Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())), Variant::no_hold_t{}));
	}
	return err;
}

Error ItemImpl::FromProtobuf(std::string_view buf) {
	assertrx(ns_);
	Payload pl = GetPayload();
	ProtobufDecoder decoder(tagsMatcher_, schema_);

	ser_.Reset();
	ser_.PutUInt32(0);
	Error err = decoder.Decode(buf, pl, ser_);
	if (err.ok()) {
		tupleData_ = ser_.DetachLStr();
		pl.Set(0, Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())), Variant::no_hold_t{}));
	}
	return err;
}

Error ItemImpl::GetMsgPack(WrSerializer &wrser) {
	int startTag = 0;
	ConstPayload pl = GetConstPayload();

	MsgPackEncoder msgpackEncoder(&tagsMatcher_);
	const TagsLengths &tagsLengths = msgpackEncoder.GetTagsMeasures(pl);

	MsgPackBuilder msgpackBuilder(wrser, &tagsLengths, &startTag, ObjType::TypePlain, &tagsMatcher_);
	msgpackEncoder.Encode(pl, msgpackBuilder);
	return Error();
}

Error ItemImpl::GetProtobuf(WrSerializer &wrser) {
	assertrx(ns_);
	ConstPayload pl = GetConstPayload();
	ProtobufBuilder protobufBuilder(&wrser, ObjType::TypePlain, schema_.get(), &tagsMatcher_);
	ProtobufEncoder protobufEncoder(&tagsMatcher_);
	protobufEncoder.Encode(pl, protobufBuilder);
	return Error();
}

// Construct item from compressed json
void ItemImpl::FromCJSON(std::string_view slice, bool pkOnly, Recoder *recoder) {
	GetPayload().Reset();
	std::string_view data = slice;
	if (!unsafe_) {
		sourceData_.reset(new char[slice.size()]);
		std::copy(data.begin(), data.end(), sourceData_.get());
		data = std::string_view(sourceData_.get(), data.size());
	}

	// check tags matcher update
	if (Serializer rdser(data); rdser.GetCTag() == kCTagEnd) {
		const auto tmOffset = rdser.GetUInt32();
		// read tags matcher update
		Serializer tser(slice.substr(tmOffset));
		tagsMatcher_.deserialize(tser);
		tagsMatcher_.setUpdated();
		data = data.substr(1 + sizeof(uint32_t), tmOffset - 5);
	}
	cjson_ = data;
	Serializer rdser(data);

	Payload pl = GetPayload();
	if (!holder_) holder_ = std::make_unique<std::deque<std::string>>();
	CJsonDecoder decoder(tagsMatcher_, *holder_);

	ser_.Reset();
	ser_.PutUInt32(0);
	if (pkOnly && !pkFields_.empty()) {
		if rx_unlikely (recoder) {
			throw Error(errParams, "ItemImpl::FromCJSON: pkOnly mode is not compatible with non-null recoder");
		}
		decoder.Decode(pl, rdser, ser_, CJsonDecoder::RestrictingFilter(pkFields_));
	} else {
		if (recoder) {
			decoder.Decode(pl, rdser, ser_, CJsonDecoder::DummyFilter(), CJsonDecoder::DefaultRecoder(*recoder));
		} else {
			decoder.Decode<>(pl, rdser, ser_);
		}
	}

	if (!rdser.Eof()) throw Error(errParseJson, "Internal error - left unparsed data %d", rdser.Pos());

	tupleData_ = ser_.DetachLStr();
	pl.Set(0, Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())), Variant::no_hold_t{}));
}

Error ItemImpl::FromJSON(std::string_view slice, char **endp, bool pkOnly) {
	std::string_view data = slice;
	cjson_ = std::string_view();

	if (!unsafe_) {
		if (endp) {
			size_t len = 0;
			try {
				gason::JsonParser parser(nullptr);
				parser.Parse(data, &len);
				*endp = const_cast<char *>(data.data()) + len;
				sourceData_.reset(new char[len]);
				std::copy(data.begin(), data.begin() + len, sourceData_.get());
				data = std::string_view(sourceData_.get(), len);
			} catch (const gason::Exception &e) {
				return Error(errParseJson, "Error parsing json: '%s'", e.what());
			}
		} else {
			sourceData_.reset(new char[slice.size()]);
			std::copy(data.begin(), data.end(), sourceData_.get());
			data = std::string_view(sourceData_.get(), data.size());
		}
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
	JsonDecoder decoder(tagsMatcher_, pkOnly && !pkFields_.empty() ? &pkFields_ : nullptr);
	Payload pl = GetPayload();

	ser_.Reset();
	ser_.PutUInt32(0);
	auto err = decoder.Decode(pl, ser_, node.value);

	// Put tuple to field[0]
	tupleData_ = ser_.DetachLStr();
	pl.Set(0, Variant(p_string(reinterpret_cast<l_string_hdr *>(tupleData_.get())), Variant::no_hold_t{}));
	return err;
}

void ItemImpl::FromCJSON(ItemImpl *other, Recoder *recoder) {
	FromCJSON(other->GetCJSON(), false, recoder);
	cjson_ = std::string_view();
}

std::string_view ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);

	JsonEncoder encoder(&tagsMatcher_);
	JsonBuilder builder(ser_, ObjType::TypePlain);

	ser_.Reset();
	encoder.Encode(pl, builder);

	return ser_.Slice();
}

std::string_view ItemImpl::GetCJSON(bool withTagsMatcher) {
	withTagsMatcher = withTagsMatcher && tagsMatcher_.isUpdated();

	if (cjson_.size() && !withTagsMatcher) return cjson_;
	ser_.Reset();
	return GetCJSON(ser_, withTagsMatcher);
}

std::string_view ItemImpl::GetCJSON(WrSerializer &ser, bool withTagsMatcher) {
	withTagsMatcher = withTagsMatcher && tagsMatcher_.isUpdated();

	if (cjson_.size() && !withTagsMatcher) {
		ser.Write(cjson_);
		return ser.Slice();
	}

	ConstPayload pl(payloadType_, payloadValue_);

	CJsonBuilder builder(ser, ObjType::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_);

	if (withTagsMatcher) {
		ser.PutCTag(kCTagEnd);
		int pos = ser.Len();
		ser.PutUInt32(0);
		encoder.Encode(pl, builder);
		uint32_t tmOffset = ser.Len();
		memcpy(ser.Buf() + pos, &tmOffset, sizeof(tmOffset));
		tagsMatcher_.serialize(ser);
	} else {
		encoder.Encode(pl, builder);
	}

	return ser.Slice();
}

std::string_view ItemImpl::GetCJSONWithTm() {
	ser_.Reset();
	return GetCJSONWithTm(ser_);
}

std::string_view ItemImpl::GetCJSONWithTm(WrSerializer &ser) {
	ConstPayload pl(payloadType_, payloadValue_);
	CJsonBuilder builder(ser, ObjType::TypePlain);
	CJsonEncoder encoder(&tagsMatcher_);

	ser.PutCTag(kCTagEnd);
	int pos = ser.Len();
	ser.PutUInt32(0);
	encoder.Encode(pl, builder);
	uint32_t tmOffset = ser.Len();
	memcpy(ser.Buf() + pos, &tmOffset, sizeof(tmOffset));
	tagsMatcher_.serialize(ser);

	return ser.Slice();
}

VariantArray ItemImpl::GetValueByJSONPath(std::string_view jsonPath) {
	ConstPayload pl(payloadType_, payloadValue_);
	VariantArray krefs;
	pl.GetByJsonPath(jsonPath, tagsMatcher_, krefs, KeyValueType::Undefined{});
	return krefs;
}

}  // namespace reindexer
